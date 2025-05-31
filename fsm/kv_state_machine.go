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

// KVStateMachine 实现键值存储状态机
type KVStateMachine struct {
	db           *badger.DB
	mu           sync.RWMutex
	appliedIndex uint64                  // 已应用的最大Raft日志索引
	txns         map[uint64]*Transaction // 活跃事务表
	txnMu        sync.RWMutex
	readIndexes  sync.Map // 跟踪ReadIndex请求
}

// NewKVStateMachine 创建新的键值状态机
func NewKVStateMachine(db *badger.DB) *KVStateMachine {
	return &KVStateMachine{
		db:   db,
		txns: make(map[uint64]*Transaction),
	}
}

// Apply 应用 Raft 日志条目到状态机
func (kv *KVStateMachine) Apply(entry raftpb.Entry) error {
	if len(entry.Data) == 0 {
		return nil
	}

	// 更新已应用的日志索引
	atomic.StoreUint64(&kv.appliedIndex, entry.Index)

	// 通知所有等待的ReadIndex请求
	kv.notifyReadIndex(entry.Index)

	// 尝试解析为事务
	var txn Transaction
	if err := json.Unmarshal(entry.Data, &txn); err == nil && txn.State != "" {
		txn.Index = entry.Index // 设置日志索引
		switch txn.State {
		case TxnPrepare:
			return kv.prepareTxn(&txn)
		case TxnCommit:
			return kv.commitTxn(txn.ID)
		case TxnAbort:
			return kv.abortTxn(txn.ID)
		default:
			return fmt.Errorf("未知事务状态: %s", txn.State)
		}
	}

	// 不是事务，尝试解析为普通命令
	var cmd Command
	if err := json.Unmarshal(entry.Data, &cmd); err != nil {
		return fmt.Errorf("解析命令失败: %v", err)
	}

	// 执行普通命令
	switch cmd.Op {
	case OpPut:
		return kv.put(cmd.Key, cmd.Value)
	case OpDelete:
		return kv.delete(cmd.Key)
	case OpGet:
		return nil
	default:
		return fmt.Errorf("未知操作类型: %s", cmd.Op)
	}
}

// WaitForIndex 等待状态机应用到指定索引
func (kv *KVStateMachine) WaitForIndex(index uint64) error {
	if atomic.LoadUint64(&kv.appliedIndex) >= index {
		return nil
	}

	done := make(chan struct{})
	kv.readIndexes.Store(index, done)
	defer kv.readIndexes.Delete(index)

	select {
	case <-done:
		return nil
	case <-time.After(3 * time.Second):
		return fmt.Errorf("等待索引 %d 超时", index)
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

	// 执行读取
	var value string
	err := kv.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			value = string(val)
			return nil
		})
	})

	return value, err
}

// Get 获取键对应的值
func (kv *KVStateMachine) Get(key string) (string, error) {
	var value string
	err := kv.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			value = string(val)
			return nil
		})
	})
	return value, err
}

// put 添加或更新键值对
func (kv *KVStateMachine) put(key, value string) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 开始事务
	txn := kv.db.NewTransaction(true)
	defer txn.Discard()

	// 检查键是否存在
	_, err := txn.Get([]byte(key))
	if err != nil && err != badger.ErrKeyNotFound {
		return err
	}

	// 设置新值
	if err := txn.Set([]byte(key), []byte(value)); err != nil {
		return err
	}

	// 提交事务
	if err := txn.Commit(); err != nil {
		return err
	}

	return nil
}

// delete 删除键值对
func (kv *KVStateMachine) delete(key string) error {
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
func (kv *KVStateMachine) RestoreSnapshot(data []byte) error {
	fmt.Println("开始解析快照数据...")
	var snapshot SnapshotData
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return fmt.Errorf("解析快照数据失败: %v", err)
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 开始批量写入
	fmt.Println("开始批量写入操作...")
	wb := kv.db.NewWriteBatch()
	defer wb.Cancel()

	// 删除现有的键值对
	fmt.Println("删除现有的键值对...")
	deleted := 0
	err := kv.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			key := it.Item().Key()
			// 只删除用户数据键，保留系统键
			if !bytes.HasPrefix(key, []byte("log/")) &&
				!bytes.Equal(key, []byte("hardstate")) &&
				!bytes.Equal(key, []byte("confstate")) &&
				!bytes.Equal(key, []byte("snapshot")) &&
				!bytes.Equal(key, []byte("firstindex")) {
				if err := wb.Delete(key); err != nil {
					return err
				}
				deleted++
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("删除现有数据失败: %v", err)
	}
	fmt.Printf("删除 %d 个现有键值对...\n", deleted)

	// 写入新的键值对
	fmt.Printf("恢复 %d 个键值对...\n", len(snapshot.Data))
	for k, v := range snapshot.Data {
		if err := wb.Set([]byte(k), []byte(v)); err != nil {
			return fmt.Errorf("写入键值对失败: %v", err)
		}
	}

	// 提交批量写入
	fmt.Println("提交批量写入...")
	if err := wb.Flush(); err != nil {
		return fmt.Errorf("提交批量写入失败: %v", err)
	}

	// 恢复应用的日志索引
	atomic.StoreUint64(&kv.appliedIndex, snapshot.AppliedIndex)

	fmt.Println("成功恢复状态机状态")
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
