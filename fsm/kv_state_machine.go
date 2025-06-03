package fsm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// 操作类型
const (
	OpPut    = "put"
	OpDelete = "delete"
	OpGet    = "get"
)

// 事务状态
const (
	TxnPrepare = "prepare"
	TxnCommit  = "commit"
	TxnAbort   = "abort"
)

// Command 表示一个键值操作命令
type Command struct {
	Op    string `json:"op"`    // 操作类型：put/delete/get
	Key   string `json:"key"`   // 键
	Value string `json:"value"` // 值（对于 delete 操作可以为空）
}

// Transaction 表示一个事务
type Transaction struct {
	ID       uint64    `json:"id"`       // 事务ID
	State    string    `json:"state"`    // 事务状态
	Index    uint64    `json:"index"`    // Raft日志索引
	Commands []Command `json:"commands"` // 事务包含的命令
}

// ReadIndexContext 表示一致性读的上下文
type ReadIndexContext struct {
	RequiredIndex uint64        // 要求的最小Raft日志索引
	ReadResult    chan struct{} // 读取完成信号
}

// SnapshotData 表示状态机快照数据
type SnapshotData struct {
	Data         map[string]string `json:"data"`
	AppliedIndex uint64            `json:"applied_index"`
}

// KVStateMachine 是一个基于 BadgerDB 的键值状态机
type KVStateMachine struct {
	db           *badger.DB
	mu           sync.RWMutex
	appliedIndex uint64                  // 已应用的最大Raft日志索引
	txns         map[uint64]*Transaction // 活跃事务表
	txnMu        sync.RWMutex
	readIndexes  sync.Map // 跟踪ReadIndex请求
	indexCond    *sync.Cond
}

// NewKVStateMachine 创建一个新的键值状态机
func NewKVStateMachine(db *badger.DB) *KVStateMachine {
	sm := &KVStateMachine{
		db:   db,
		txns: make(map[uint64]*Transaction),
	}
	sm.indexCond = sync.NewCond(&sm.mu)
	return sm
}

// Apply 应用一个 Raft 日志条目到状态机
func (sm *KVStateMachine) Apply(entry raftpb.Entry) error {
	// 如果是配置变更，直接更新索引并返回
	if entry.Type == raftpb.EntryConfChange {
		// 更新应用索引但不执行其他操作
		sm.mu.Lock()
		if entry.Index > sm.appliedIndex {
			sm.appliedIndex = entry.Index
			sm.indexCond.Broadcast() // 通知等待的条件变量
		}
		sm.mu.Unlock()
		return nil
	}

	// 解析命令
	var cmd Command
	if err := json.Unmarshal(entry.Data, &cmd); err != nil {
		return fmt.Errorf("解析命令失败: %v", err)
	}

	fmt.Printf("状态机应用%s命令: 键=%s, 值=%s, 索引=%d\n", cmd.Op, cmd.Key, cmd.Value, entry.Index)

	// 执行命令
	switch cmd.Op {
	case OpPut:
		if err := sm.Put(cmd.Key, cmd.Value); err != nil {
			return fmt.Errorf("执行PUT操作失败: %v", err)
		}
		fmt.Printf("状态机执行PUT操作: 键=%s, 值=%s\n", cmd.Key, cmd.Value)
	case OpDelete:
		if err := sm.Delete(cmd.Key); err != nil {
			return fmt.Errorf("执行DELETE操作失败: %v", err)
		}
		fmt.Printf("状态机执行DELETE操作: 键=%s\n", cmd.Key)
	default:
		return fmt.Errorf("未知的命令类型: %s", cmd.Op)
	}

	// 更新最新应用的索引
	sm.mu.Lock()
	// 确保索引单调递增
	if entry.Index > sm.appliedIndex {
		sm.appliedIndex = entry.Index
		// 通知所有等待的条件变量
		sm.indexCond.Broadcast()
	}
	sm.mu.Unlock()

	// 通知等待的ReadIndex请求
	sm.notifyReadIndex(entry.Index)

	return nil
}

// WaitForIndex 等待状态机应用到指定索引
func (kv *KVStateMachine) WaitForIndex(index uint64) error {
	// 快速路径：如果已经应用到索引，立即返回
	if atomic.LoadUint64(&kv.appliedIndex) >= index {
		return nil
	}

	// 使用通道和条件变量等待索引应用
	done := make(chan struct{})

	go func() {
		defer close(done)

		kv.mu.Lock()
		for kv.appliedIndex < index {
			kv.indexCond.Wait() // 这会自动释放锁并在被唤醒时重新获取锁
		}
		kv.mu.Unlock()
	}()

	// 等待条件满足或超时
	select {
	case <-done:
		return nil
	case <-time.After(5 * time.Second):
		return fmt.Errorf("等待索引 %d 超时，当前索引 %d", index, atomic.LoadUint64(&kv.appliedIndex))
	}
}

// notifyReadIndex 通知等待指定索引的ReadIndex请求
func (kv *KVStateMachine) notifyReadIndex(index uint64) {
	kv.readIndexes.Range(func(key, value interface{}) bool {
		if waitIndex := key.(uint64); waitIndex <= index {
			if ch, ok := value.(chan struct{}); ok {
				close(ch)
			}
			kv.readIndexes.Delete(key)
		}
		return true
	})
}

// ConsistentGet 执行一致性读
func (kv *KVStateMachine) ConsistentGet(key string, ctx *ReadIndexContext) (string, error) {
	if ctx == nil || ctx.RequiredIndex == 0 {
		return "", fmt.Errorf("无效的读取上下文")
	}

	// 等待状态机追上要求的索引
	if err := kv.WaitForIndex(ctx.RequiredIndex); err != nil {
		return "", err
	}

	// 加读锁确保一致性
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	// 再次检查应用索引，确保在获取锁后仍满足条件
	if atomic.LoadUint64(&kv.appliedIndex) < ctx.RequiredIndex {
		return "", fmt.Errorf("应用索引不满足要求")
	}

	fmt.Printf("执行一致性读: 键=%s, 要求索引=%d, 当前索引=%d\n",
		key, ctx.RequiredIndex, atomic.LoadUint64(&kv.appliedIndex))

	// 执行读取
	var value string
	err := kv.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err == badger.ErrKeyNotFound {
			fmt.Printf("一致性读: 键 %s 不存在\n", key)
			return nil
		}
		if err != nil {
			fmt.Printf("一致性读: 读取键 %s 出错: %v\n", key, err)
			return err
		}
		return item.Value(func(val []byte) error {
			value = string(val)
			fmt.Printf("一致性读: 成功读取键 %s = %s\n", key, value)
			return nil
		})
	})

	return value, err
}

// Get 获取键对应的值
func (kv *KVStateMachine) Get(key string) (string, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	fmt.Printf("状态机尝试读取键: %s\n", key)

	var value string
	err := kv.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err == badger.ErrKeyNotFound {
			fmt.Printf("键 %s 不存在\n", key)
			return nil
		}
		if err != nil {
			fmt.Printf("读取键 %s 出错: %v\n", key, err)
			return err
		}

		return item.Value(func(val []byte) error {
			value = string(val)
			fmt.Printf("成功读取键 %s = %s\n", key, value)
			return nil
		})
	})

	return value, err
}

// Put 添加或更新键值对
func (kv *KVStateMachine) Put(key, value string) error {
	fmt.Printf("状态机执行PUT操作: 键=%s, 值=%s\n", key, value)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 打印调试信息
	if kv.db == nil {
		return fmt.Errorf("数据库未初始化")
	}

	// 直接写入键值对
	err := kv.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), []byte(value))
	})

	if err != nil {
		fmt.Printf("PUT操作失败: 键=%s, 错误=%v\n", key, err)
		return err
	}

	fmt.Printf("PUT操作成功: 键=%s, 值=%s\n", key, value)
	return nil
}

// Delete 删除键值对
func (kv *KVStateMachine) Delete(key string) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 开始事务
	txn := kv.db.NewTransaction(true)
	defer txn.Discard()

	// 检查键是否存在
	_, err := txn.Get([]byte(key))
	if err == badger.ErrKeyNotFound {
		return nil // 键不存在，视为成功
	}
	if err != nil {
		return err
	}

	// 删除键
	if err := txn.Delete([]byte(key)); err != nil {
		return err
	}

	// 提交事务
	if err := txn.Commit(); err != nil {
		return err
	}

	return nil
}

// SaveSnapshot 保存状态机的快照
func (kv *KVStateMachine) SaveSnapshot() ([]byte, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	snapshot := &SnapshotData{
		Data:         make(map[string]string),
		AppliedIndex: atomic.LoadUint64(&kv.appliedIndex),
	}

	// 遍历所有键值对
	err := kv.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := string(item.Key())

			// 跳过系统键
			if bytes.HasPrefix(item.Key(), []byte("log/")) ||
				bytes.Equal(item.Key(), []byte("hardstate")) ||
				bytes.Equal(item.Key(), []byte("confstate")) ||
				bytes.Equal(item.Key(), []byte("snapshot")) {
				continue
			}

			err := item.Value(func(v []byte) error {
				snapshot.Data[k] = string(v)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return json.Marshal(snapshot)
}

// RestoreSnapshot 从快照恢复状态机
func (kv *KVStateMachine) RestoreSnapshot(snapshot []byte) error {
	if len(snapshot) == 0 {
		return nil
	}

	// 解析快照数据
	var snapshotData SnapshotData
	if err := json.Unmarshal(snapshot, &snapshotData); err != nil {
		return fmt.Errorf("解析快照数据失败: %v", err)
	}

	fmt.Printf("恢复快照，索引: %d, 键值对数量: %d\n",
		snapshotData.AppliedIndex, len(snapshotData.Data))

	// 清空当前状态
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 删除所有现有键值对
	deleted := 0
	err := kv.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()

			// 跳过系统键
			if bytes.HasPrefix(key, []byte("log/")) ||
				bytes.Equal(key, []byte("hardstate")) ||
				bytes.Equal(key, []byte("confstate")) ||
				bytes.Equal(key, []byte("snapshot")) ||
				bytes.Equal(key, []byte("firstindex")) {
				continue
			}

			if err := txn.Delete(key); err != nil {
				return err
			}
			deleted++
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("清空状态失败: %v", err)
	}

	// 恢复快照中的键值对
	err = kv.db.Update(func(txn *badger.Txn) error {
		for k, v := range snapshotData.Data {
			if err := txn.Set([]byte(k), []byte(v)); err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("恢复快照数据失败: %v", err)
	}

	// 确保应用索引正确更新
	if snapshotData.AppliedIndex > kv.appliedIndex {
		fmt.Printf("更新应用索引: %d -> %d\n", kv.appliedIndex, snapshotData.AppliedIndex)
		kv.appliedIndex = snapshotData.AppliedIndex
	} else {
		fmt.Printf("保持当前应用索引: %d (快照索引: %d)\n", kv.appliedIndex, snapshotData.AppliedIndex)
	}

	// 通知等待的ReadIndex请求
	kv.indexCond.Broadcast()
	kv.notifyReadIndex(kv.appliedIndex)

	fmt.Printf("快照恢复完成，当前应用索引: %d\n", kv.appliedIndex)
	return nil
}

// GetAppliedIndex 获取当前已应用的最大日志索引
func (kv *KVStateMachine) GetAppliedIndex() uint64 {
	return atomic.LoadUint64(&kv.appliedIndex)
}

// prepareTxn 准备事务
func (kv *KVStateMachine) prepareTxn(txn *Transaction) error {
	kv.txnMu.Lock()
	defer kv.txnMu.Unlock()

	// 检查事务是否已存在
	if _, exists := kv.txns[txn.ID]; exists {
		return fmt.Errorf("事务 %d 已存在", txn.ID)
	}

	// 检查时间戳冲突
	if txn.Index < atomic.LoadUint64(&kv.appliedIndex) {
		return fmt.Errorf("事务 %d 时间戳冲突", txn.ID)
	}

	// 验证所有命令
	dbTxn := kv.db.NewTransaction(true)
	defer dbTxn.Discard()

	for _, cmd := range txn.Commands {
		switch cmd.Op {
		case OpPut:
			// 检查键是否被锁定
			item, err := dbTxn.Get([]byte(cmd.Key))
			if err != nil && err != badger.ErrKeyNotFound {
				return err
			}
			if item != nil {
				// 检查版本时间戳
				if item.Version() >= txn.Index {
					return fmt.Errorf("键 %s 已被更新", cmd.Key)
				}
			}
		case OpDelete:
			// 类似的检查
			item, err := dbTxn.Get([]byte(cmd.Key))
			if err != nil && err != badger.ErrKeyNotFound {
				return err
			}
			if item != nil && item.Version() >= txn.Index {
				return fmt.Errorf("键 %s 已被更新", cmd.Key)
			}
		}
	}

	// 记录事务
	kv.txns[txn.ID] = txn
	return nil
}

// commitTxn 提交事务
func (kv *KVStateMachine) commitTxn(txnID uint64) error {
	kv.txnMu.Lock()
	defer kv.txnMu.Unlock()

	// 获取事务
	txn, exists := kv.txns[txnID]
	if !exists {
		return fmt.Errorf("事务 %d 不存在", txnID)
	}

	// 执行所有命令
	dbTxn := kv.db.NewTransaction(true)
	defer dbTxn.Discard()

	for _, cmd := range txn.Commands {
		switch cmd.Op {
		case OpPut:
			if err := dbTxn.Set([]byte(cmd.Key), []byte(cmd.Value)); err != nil {
				return err
			}
		case OpDelete:
			if err := dbTxn.Delete([]byte(cmd.Key)); err != nil {
				return err
			}
		}
	}

	// 提交事务
	if err := dbTxn.Commit(); err != nil {
		return err
	}

	// 清理事务记录
	delete(kv.txns, txnID)
	return nil
}

// abortTxn 中止事务
func (kv *KVStateMachine) abortTxn(txnID uint64) error {
	kv.txnMu.Lock()
	defer kv.txnMu.Unlock()

	// 检查事务是否存在
	if _, exists := kv.txns[txnID]; !exists {
		return fmt.Errorf("事务 %d 不存在", txnID)
	}

	// 清理事务记录
	delete(kv.txns, txnID)
	return nil
}

// ApplySnapshot 应用快照到状态机
func (kv *KVStateMachine) ApplySnapshot(snapshot raftpb.Snapshot) error {
	// 检查快照是否为空
	if snapshot.Metadata.Index == 0 {
		return nil
	}

	fmt.Printf("状态机应用快照，索引: %d, 任期: %d\n",
		snapshot.Metadata.Index, snapshot.Metadata.Term)

	// 调用RestoreSnapshot恢复状态
	return kv.RestoreSnapshot(snapshot.Data)
}

// CreateSnapshot 创建状态机的快照
func (kv *KVStateMachine) CreateSnapshot() (raftpb.Snapshot, error) {
	// 获取当前应用的索引
	appliedIndex := kv.GetAppliedIndex()

	// 获取状态机数据
	data, err := kv.SaveSnapshot()
	if err != nil {
		return raftpb.Snapshot{}, fmt.Errorf("创建状态机快照失败: %v", err)
	}

	// 创建Raft快照
	snap := raftpb.Snapshot{
		Data: data,
		Metadata: raftpb.SnapshotMetadata{
			Index: appliedIndex,
			// 注意：这里没有设置Term和ConfState，
			// 这些应该由调用者设置
		},
	}

	return snap, nil
}
