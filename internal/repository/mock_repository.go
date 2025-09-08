package repository

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// MockRepository 模拟仓库实现，用于单元测试
type MockRepository struct {
	// 存储数据
	logEntries   map[uint64]raftpb.Entry
	hardState    raftpb.HardState
	confState    raftpb.ConfState
	snapshot     raftpb.Snapshot
	kvData       map[string]string
	transactions map[uint64]string
	firstIndex   uint64
	lastIndex    uint64

	// 控制行为
	errorOnGet    bool
	errorOnPut    bool
	errorOnDelete bool
	errorOnBatch  bool

	// 并发控制
	mu sync.RWMutex
}

// NewMockRepository 创建模拟仓库
func NewMockRepository() *MockRepository {
	// Create a dummy entry at index 0 with term 0, similar to etcd's MemoryStorage
	logEntries := make(map[uint64]raftpb.Entry)
	dummyEntry := raftpb.Entry{
		Index: 0,
		Term:  0,
		Type:  raftpb.EntryNormal,
		Data:  nil,
	}
	logEntries[0] = dummyEntry
	
	// Initialize with empty HardState
	hardState := raftpb.HardState{
		Term:   0,
		Vote:   0,
		Commit: 0,
	}
	
	return &MockRepository{
		logEntries:   logEntries,
		kvData:       make(map[string]string),
		transactions: make(map[uint64]string),
		firstIndex:   1, // First available log entry index
		lastIndex:    0, // Last log entry index (dummy entry doesn't count)
		hardState:    hardState,
	}
}

// RaftLogRepository 实现

// GetEntry 获取日志条目
func (m *MockRepository) GetEntry(index uint64) (raftpb.Entry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entry, exists := m.logEntries[index]
	if !exists {
		return raftpb.Entry{}, raft.ErrUnavailable
	}
	return entry, nil
}

// GetEntries 获取日志条目范围
func (m *MockRepository) GetEntries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var entries []raftpb.Entry
	var totalSize uint64

	for i := lo; i < hi; i++ {
		if entry, exists := m.logEntries[i]; exists {
			if maxSize > 0 {
				entrySize := uint64(entry.Size())
				if totalSize+entrySize > maxSize {
					break
				}
				totalSize += entrySize
			}
			entries = append(entries, entry)
		}
	}

	return entries, nil
}

// StoreEntries 存储日志条目
func (m *MockRepository) StoreEntries(entries []raftpb.Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, entry := range entries {
		m.logEntries[entry.Index] = entry
		if entry.Index > m.lastIndex {
			m.lastIndex = entry.Index
		}
		// 如果这是第一个真实的日志条目（索引>0），更新firstIndex
		if entry.Index > 0 && (m.firstIndex == 1 && m.lastIndex == 0) {
			// 这是第一个真实条目，firstIndex保持为1
		} else if entry.Index > 0 && entry.Index < m.firstIndex {
			m.firstIndex = entry.Index
		}
	}
	return nil
}

// FirstIndex 获取第一个日志索引
func (m *MockRepository) FirstIndex() (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.firstIndex, nil
}

// LastIndex 获取最后一个日志索引
func (m *MockRepository) LastIndex() (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lastIndex, nil
}

// Term 获取指定索引的任期
func (m *MockRepository) Term(index uint64) (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	entry, exists := m.logEntries[index]
	if !exists {
		return 0, ErrEntryNotFound
	}
	return entry.Term, nil
}

// Compact 压缩日志到指定索引
func (m *MockRepository) Compact(compactIndex uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i := m.firstIndex; i < compactIndex; i++ {
		delete(m.logEntries, i)
	}
	m.firstIndex = compactIndex
	return nil
}

// RaftStateRepository 实现

// GetHardState 获取硬状态
func (m *MockRepository) GetHardState() (raftpb.HardState, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.hardState, nil
}

// StoreHardState 存储硬状态
func (m *MockRepository) StoreHardState(st raftpb.HardState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hardState = st
	return nil
}

// GetConfState 获取配置状态
func (m *MockRepository) GetConfState() (raftpb.ConfState, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.confState, nil
}

// StoreConfState 存储配置状态
func (m *MockRepository) StoreConfState(cs raftpb.ConfState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.confState = cs
	return nil
}

// SnapshotRepository 实现

// GetSnapshot 获取快照
func (m *MockRepository) GetSnapshot() (raftpb.Snapshot, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.snapshot.Data == nil {
		return raftpb.Snapshot{}, raft.ErrSnapshotTemporarilyUnavailable
	}
	return m.snapshot, nil
}

// StoreSnapshot 存储快照
func (m *MockRepository) StoreSnapshot(snapshot raftpb.Snapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.snapshot = snapshot
	return nil
}

// ApplySnapshot 应用快照
func (m *MockRepository) ApplySnapshot(snapshot raftpb.Snapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 清空现有数据
	m.kvData = make(map[string]string)

	// 这里应该解析快照数据并恢复，简化实现
	m.snapshot = snapshot
	return nil
}

// CreateSnapshot 创建快照
func (m *MockRepository) CreateSnapshot(ctx context.Context) (raftpb.Snapshot, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// 简化实现，实际应该序列化所有数据
	snapshot := raftpb.Snapshot{
		Data: []byte("mock snapshot data"),
		Metadata: raftpb.SnapshotMetadata{
			Index: m.lastIndex,
			Term:  m.hardState.Term,
		},
	}
	return snapshot, nil
}

// KVRepository 实现

// Get 获取值
func (m *MockRepository) Get(ctx context.Context, key string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.errorOnGet {
		return "", errors.New("模拟Get错误")
	}

	value, exists := m.kvData[key]
	if !exists {
		return "", errors.New("key not found")
	}
	return value, nil
}

// Put 设置值
func (m *MockRepository) Put(ctx context.Context, key, value string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.errorOnPut {
		return errors.New("模拟Put错误")
	}

	m.kvData[key] = value
	return nil
}

// Delete 删除键
func (m *MockRepository) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.errorOnDelete {
		return errors.New("模拟Delete错误")
	}

	delete(m.kvData, key)
	return nil
}

// Batch 批量操作
func (m *MockRepository) Batch(ctx context.Context, ops []KVOperation) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.errorOnBatch {
		return errors.New("模拟Batch错误")
	}

	for _, op := range ops {
		switch op.Type {
		case "put":
			m.kvData[op.Key] = op.Value
		case "delete":
			delete(m.kvData, op.Key)
		default:
			return fmt.Errorf("不支持的操作类型: %s", op.Type)
		}
	}
	return nil
}

// GetAll 获取所有键值对
func (m *MockRepository) GetAll(ctx context.Context) (map[string]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]string)
	for k, v := range m.kvData {
		result[k] = v
	}
	return result, nil
}

// RestoreFromSnapshot 从快照恢复数据
func (m *MockRepository) RestoreFromSnapshot(ctx context.Context, data map[string]string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.kvData = make(map[string]string)
	for k, v := range data {
		m.kvData[k] = v
	}
	return nil
}

// TransactionRepository 实现

// BeginTransaction 开始事务
func (m *MockRepository) BeginTransaction(ctx context.Context, txnID uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.transactions[txnID] = "prepare"
	return nil
}

// CommitTransaction 提交事务
func (m *MockRepository) CommitTransaction(ctx context.Context, txnID uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.transactions[txnID] = "commit"
	return nil
}

// RollbackTransaction 回滚事务
func (m *MockRepository) RollbackTransaction(ctx context.Context, txnID uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.transactions, txnID)
	return nil
}

// GetTransactionState 获取事务状态
func (m *MockRepository) GetTransactionState(ctx context.Context, txnID uint64) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	state, exists := m.transactions[txnID]
	if !exists {
		return "", errors.New("transaction not found")
	}
	return state, nil
}

// AddTransactionOperation 添加事务操作
func (m *MockRepository) AddTransactionOperation(ctx context.Context, txnID uint64, op KVOperation) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// 简化实现，实际应该记录操作
	return nil
}

// Close 关闭仓库
func (m *MockRepository) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// 清空所有数据
	m.logEntries = make(map[uint64]raftpb.Entry)
	m.kvData = make(map[string]string)
	m.transactions = make(map[uint64]string)
	return nil
}

// HealthCheck 健康检查
func (m *MockRepository) HealthCheck(ctx context.Context) error {
	return nil // 模拟仓库总是健康的
}

// 测试辅助方法

// SetErrorOnGet 设置Get操作是否返回错误
func (m *MockRepository) SetErrorOnGet(err bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.errorOnGet = err
}

// SetErrorOnPut 设置Put操作是否返回错误
func (m *MockRepository) SetErrorOnPut(err bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.errorOnPut = err
}

// SetErrorOnDelete 设置Delete操作是否返回错误
func (m *MockRepository) SetErrorOnDelete(err bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.errorOnDelete = err
}

// SetErrorOnBatch 设置Batch操作是否返回错误
func (m *MockRepository) SetErrorOnBatch(err bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.errorOnBatch = err
}

// GetKVDataSize 获取KV数据大小
func (m *MockRepository) GetKVDataSize() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.kvData)
}

// GetLogEntriesSize 获取日志条目数量（不包括虚拟条目）
func (m *MockRepository) GetLogEntriesSize() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	count := 0
	for index := range m.logEntries {
		if index > 0 { // 排除虚拟条目（索引0）
			count++
		}
	}
	return count
}

// Clear 清空所有数据
func (m *MockRepository) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	// 重新创建日志条目map并添加虚拟条目
	m.logEntries = make(map[uint64]raftpb.Entry)
	dummyEntry := raftpb.Entry{
		Index: 0,
		Term:  0,
		Type:  raftpb.EntryNormal,
		Data:  nil,
	}
	m.logEntries[0] = dummyEntry
	
	m.kvData = make(map[string]string)
	m.transactions = make(map[uint64]string)
	m.hardState = raftpb.HardState{}
	m.confState = raftpb.ConfState{}
	m.snapshot = raftpb.Snapshot{}
	m.firstIndex = 1
	m.lastIndex = 0
}