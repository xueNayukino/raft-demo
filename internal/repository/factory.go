package repository

import (
	"fmt"
	"sync"
)

// RepositoryFactoryImpl 仓库工厂实现
type RepositoryFactoryImpl struct {
	badgerRepo *BadgerRepository
	mu         sync.RWMutex
}

// NewRepositoryFactory 创建仓库工厂
func NewRepositoryFactory(dataDir string) (*RepositoryFactoryImpl, error) {
	badgerRepo, err := NewBadgerRepository(dataDir)
	if err != nil {
		return nil, fmt.Errorf("创建BadgerRepository失败: %v", err)
	}

	return &RepositoryFactoryImpl{
		badgerRepo: badgerRepo,
	}, nil
}

// GetStorageRepository 获取存储仓库
func (f *RepositoryFactoryImpl) GetStorageRepository() StorageRepository {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.badgerRepo
}

// GetRaftLogRepository 获取Raft日志仓库
func (f *RepositoryFactoryImpl) GetRaftLogRepository() RaftLogRepository {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.badgerRepo
}

// GetRaftStateRepository 获取Raft状态仓库
func (f *RepositoryFactoryImpl) GetRaftStateRepository() RaftStateRepository {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.badgerRepo
}

// GetSnapshotRepository 获取快照仓库
func (f *RepositoryFactoryImpl) GetSnapshotRepository() SnapshotRepository {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.badgerRepo
}

// GetKVRepository 获取KV仓库
func (f *RepositoryFactoryImpl) GetKVRepository() KVRepository {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.badgerRepo
}

// GetTransactionRepository 获取事务仓库
func (f *RepositoryFactoryImpl) GetTransactionRepository() TransactionRepository {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.badgerRepo
}

// Close 关闭工厂和所有仓库
func (f *RepositoryFactoryImpl) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.badgerRepo != nil {
		return f.badgerRepo.Close()
	}
	return nil
}

// HealthCheck 健康检查
func (f *RepositoryFactoryImpl) HealthCheck() error {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.badgerRepo != nil {
		return f.badgerRepo.HealthCheck(nil)
	}
	return fmt.Errorf("BadgerRepository未初始化")
}