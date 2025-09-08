package fsm

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/raft/v3/raftpb"

	"qklzl/internal/repository"
)

// RepositoryStateMachine 基于仓库模式的状态机实现
type RepositoryStateMachine struct {
	kvRepo       repository.KVRepository
	snapshotRepo repository.SnapshotRepository
	mu           sync.RWMutex
	appliedIndex uint64
	txns         map[uint64]*Transaction
	txnMu        sync.RWMutex
	readIndexes  sync.Map
	indexCond    *sync.Cond
}

// NewRepositoryStateMachine 创建基于仓库的状态机
func NewRepositoryStateMachine(factory repository.RepositoryFactory) *RepositoryStateMachine {
	sm := &RepositoryStateMachine{
		kvRepo:       factory.GetKVRepository(),
		snapshotRepo: factory.GetSnapshotRepository(),
		txns:         make(map[uint64]*Transaction),
	}
	sm.indexCond = sync.NewCond(&sm.mu)
	return sm
}

// Apply 应用一个 Raft 日志条目到状态机
func (sm *RepositoryStateMachine) Apply(entry raftpb.Entry) error {
	// 如果是配置变更，直接更新索引并返回
	if entry.Type == raftpb.EntryConfChange {
		sm.mu.Lock()
		if entry.Index > sm.appliedIndex {
			sm.appliedIndex = entry.Index
			sm.indexCond.Broadcast()
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
	ctx := context.Background()
	switch cmd.Op {
	case OpPut:
		if err := sm.kvRepo.Put(ctx, cmd.Key, cmd.Value); err != nil {
			return fmt.Errorf("执行PUT操作失败: %v", err)
		}
		fmt.Printf("状态机执行PUT操作: 键=%s, 值=%s\n", cmd.Key, cmd.Value)
	case OpDelete:
		if err := sm.kvRepo.Delete(ctx, cmd.Key); err != nil {
			return fmt.Errorf("执行DELETE操作失败: %v", err)
		}
		fmt.Printf("状态机执行DELETE操作: 键=%s\n", cmd.Key)
	default:
		return fmt.Errorf("未知的命令类型: %s", cmd.Op)
	}

	// 更新最新应用的索引
	sm.mu.Lock()
	if entry.Index > sm.appliedIndex {
		sm.appliedIndex = entry.Index
		sm.indexCond.Broadcast()
	}
	sm.mu.Unlock()

	// 通知等待的ReadIndex请求
	sm.notifyReadIndex(entry.Index)

	return nil
}

// ApplySnapshot 应用快照到状态机
func (sm *RepositoryStateMachine) ApplySnapshot(snapshot raftpb.Snapshot) error {
	if len(snapshot.Data) == 0 {
		return nil
	}

	// 解析快照数据
	var snapshotData SnapshotData
	if err := json.Unmarshal(snapshot.Data, &snapshotData); err != nil {
		return fmt.Errorf("解析快照数据失败: %v", err)
	}

	fmt.Printf("恢复快照，索引: %d, 键值对数量: %d\n",
		snapshotData.AppliedIndex, len(snapshotData.Data))

	// 使用仓库恢复数据
	ctx := context.Background()
	if err := sm.kvRepo.RestoreFromSnapshot(ctx, snapshotData.Data); err != nil {
		return fmt.Errorf("恢复快照失败: %v", err)
	}

	// 更新应用索引
	sm.mu.Lock()
	sm.appliedIndex = snapshotData.AppliedIndex
	sm.indexCond.Broadcast()
	sm.mu.Unlock()

	return nil
}

// CreateSnapshot 创建状态机的快照
func (sm *RepositoryStateMachine) CreateSnapshot() (raftpb.Snapshot, error) {
	sm.mu.RLock()
	currentIndex := sm.appliedIndex
	sm.mu.RUnlock()

	// 获取所有数据
	ctx := context.Background()
	data, err := sm.kvRepo.GetAll(ctx)
	if err != nil {
		return raftpb.Snapshot{}, fmt.Errorf("获取数据失败: %v", err)
	}

	// 创建快照数据
	snapshotData := &SnapshotData{
		Data:         data,
		AppliedIndex: currentIndex,
	}

	// 序列化快照数据
	snapshotBytes, err := json.Marshal(snapshotData)
	if err != nil {
		return raftpb.Snapshot{}, fmt.Errorf("序列化快照失败: %v", err)
	}

	// 创建快照
	snapshot := raftpb.Snapshot{
		Data: snapshotBytes,
		Metadata: raftpb.SnapshotMetadata{
			Index: currentIndex,
			Term:  0, // 需要从外部设置
		},
	}

	return snapshot, nil
}

// Get 获取键对应的值
func (sm *RepositoryStateMachine) Get(key string) (string, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	ctx := context.Background()
	return sm.kvRepo.Get(ctx, key)
}

// WaitForIndex 等待状态机应用到指定索引
func (sm *RepositoryStateMachine) WaitForIndex(index uint64) error {
	// 如果已经应用到索引，立即返回
	if atomic.LoadUint64(&sm.appliedIndex) >= index {
		return nil
	}

	currentIndex := atomic.LoadUint64(&sm.appliedIndex)
	if index-currentIndex <= 5 {
		return nil
	}

	// 使用通道和条件变量等待索引应用
	done := make(chan struct{})

	go func() {
		defer close(done)

		sm.mu.Lock()
		for sm.appliedIndex < index {
			sm.indexCond.Wait()
		}
		sm.mu.Unlock()
	}()

	// 等待条件满足或超时
	select {
	case <-done:
		return nil
	case <-time.After(100 * time.Millisecond):
		fmt.Printf("等待索引 %d 超时（100ms），当前索引 %d，允许读取\n",
			index, atomic.LoadUint64(&sm.appliedIndex))
		return fmt.Errorf("等待索引 %d 超时", index)
	}
}

// ConsistentGet 执行一致性读
func (sm *RepositoryStateMachine) ConsistentGet(key string, ctx *ReadIndexContext) (string, error) {
	if ctx == nil || ctx.RequiredIndex == 0 {
		return "", fmt.Errorf("无效的读取上下文")
	}

	// 等待状态机追上要求的索引
	if err := sm.WaitForIndex(ctx.RequiredIndex); err != nil {
		return "", err
	}

	// 加读锁确保一致性
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	// 再次检查应用索引
	if atomic.LoadUint64(&sm.appliedIndex) < ctx.RequiredIndex {
		return "", fmt.Errorf("应用索引不满足要求")
	}

	// 执行读取
	ctx2 := context.Background()
	return sm.kvRepo.Get(ctx2, key)
}

// Put 设置键值对（内部方法）
func (sm *RepositoryStateMachine) Put(key, value string) error {
	fmt.Printf("状态机执行PUT操作: 键=%s, 值=%s\n", key, value)

	sm.mu.Lock()
	defer sm.mu.Unlock()

	ctx := context.Background()
	if err := sm.kvRepo.Put(ctx, key, value); err != nil {
		fmt.Printf("PUT操作失败: 键=%s, 错误=%v\n", key, err)
		return err
	}

	fmt.Printf("PUT操作成功: 键=%s, 值=%s\n", key, value)
	return nil
}

// Delete 删除键值对（内部方法）
func (sm *RepositoryStateMachine) Delete(key string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	ctx := context.Background()
	return sm.kvRepo.Delete(ctx, key)
}

// notifyReadIndex 通知等待指定索引的ReadIndex请求
func (sm *RepositoryStateMachine) notifyReadIndex(index uint64) {
	sm.readIndexes.Range(func(key, value interface{}) bool {
		if waitIndex := key.(uint64); waitIndex <= index {
			if ch, ok := value.(chan struct{}); ok {
				close(ch)
			}
			sm.readIndexes.Delete(key)
		}
		return true
	})
}

// GetAppliedIndex 获取当前应用的索引
func (sm *RepositoryStateMachine) GetAppliedIndex() uint64 {
	return atomic.LoadUint64(&sm.appliedIndex)
}

// Close 关闭状态机
func (sm *RepositoryStateMachine) Close() error {
	// 状态机本身不需要特殊的关闭逻辑
	// 仓库的关闭由工厂负责
	return nil
}