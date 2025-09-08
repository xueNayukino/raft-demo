package repository

import (
	"context"
	"errors"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// 错误定义
var (
	ErrEntryNotFound = errors.New("entry not found")
)

// RaftLogRepository 定义Raft日志存储的接口
type RaftLogRepository interface {
	// 获取日志条目
	GetEntry(index uint64) (raftpb.Entry, error)
	// 获取日志条目范围
	GetEntries(lo, hi, maxSize uint64) ([]raftpb.Entry, error)
	// 存储日志条目
	StoreEntries(entries []raftpb.Entry) error
	// 获取第一个日志索引
	FirstIndex() (uint64, error)
	// 获取最后一个日志索引
	LastIndex() (uint64, error)
	// 获取指定索引的任期
	Term(index uint64) (uint64, error)
	// 压缩日志到指定索引
	Compact(compactIndex uint64) error
}

// RaftStateRepository 定义Raft状态存储的接口
type RaftStateRepository interface {
	// 获取硬状态
	GetHardState() (raftpb.HardState, error)
	// 存储硬状态
	StoreHardState(st raftpb.HardState) error
	// 获取配置状态
	GetConfState() (raftpb.ConfState, error)
	// 存储配置状态
	StoreConfState(cs raftpb.ConfState) error
}

// SnapshotRepository 定义快照存储的接口
type SnapshotRepository interface {
	// 获取快照
	GetSnapshot() (raftpb.Snapshot, error)
	// 存储快照
	StoreSnapshot(snapshot raftpb.Snapshot) error
	// 应用快照
	ApplySnapshot(snapshot raftpb.Snapshot) error
	// 创建快照
	CreateSnapshot(ctx context.Context) (raftpb.Snapshot, error)
}

// KVRepository 定义键值存储的接口
type KVRepository interface {
	// 获取值
	Get(ctx context.Context, key string) (string, error)
	// 设置值
	Put(ctx context.Context, key, value string) error
	// 删除键
	Delete(ctx context.Context, key string) error
	// 批量操作
	Batch(ctx context.Context, ops []KVOperation) error
	// 获取所有键值对（用于快照）
	GetAll(ctx context.Context) (map[string]string, error)
	// 从快照恢复数据
	RestoreFromSnapshot(ctx context.Context, data map[string]string) error
	// 健康检查
	HealthCheck(ctx context.Context) error
}

// KVOperation 定义键值操作
type KVOperation struct {
	Type  string // "put" or "delete"
	Key   string
	Value string // 对于delete操作可以为空
}

// TransactionRepository 定义事务存储的接口
type TransactionRepository interface {
	// 开始事务
	BeginTransaction(ctx context.Context, txnID uint64) error
	// 提交事务
	CommitTransaction(ctx context.Context, txnID uint64) error
	// 回滚事务
	RollbackTransaction(ctx context.Context, txnID uint64) error
	// 获取事务状态
	GetTransactionState(ctx context.Context, txnID uint64) (string, error)
	// 添加事务操作
	AddTransactionOperation(ctx context.Context, txnID uint64, op KVOperation) error
}

// StorageRepository 组合所有存储接口
type StorageRepository interface {
	RaftLogRepository
	RaftStateRepository
	SnapshotRepository
	KVRepository
	TransactionRepository
	
	// 生命周期管理
	Close() error
	HealthCheck(ctx context.Context) error
}

// RepositoryFactory 定义仓库工厂接口
type RepositoryFactory interface {
	// 获取存储仓库
	GetStorageRepository() StorageRepository
	// 获取Raft日志仓库
	GetRaftLogRepository() RaftLogRepository
	// 获取Raft状态仓库
	GetRaftStateRepository() RaftStateRepository
	// 获取快照仓库
	GetSnapshotRepository() SnapshotRepository
	// 获取KV仓库
	GetKVRepository() KVRepository
	// 获取事务仓库
	GetTransactionRepository() TransactionRepository
	
	// 生命周期管理
	Close() error
	HealthCheck() error
}