package repository

import (
	"sync"
)

// MockRepositoryFactory 模拟仓库工厂
type MockRepositoryFactory struct {
	mockRepo *MockRepository
	mu       sync.RWMutex
}

// NewMockRepositoryFactory 创建模拟仓库工厂
func NewMockRepositoryFactory() *MockRepositoryFactory {
	return &MockRepositoryFactory{
		mockRepo: NewMockRepository(),
	}
}

// GetStorageRepository 获取存储仓库
func (f *MockRepositoryFactory) GetStorageRepository() StorageRepository {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.mockRepo
}

// GetRaftLogRepository 获取Raft日志仓库
func (f *MockRepositoryFactory) GetRaftLogRepository() RaftLogRepository {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.mockRepo
}

// GetRaftStateRepository 获取Raft状态仓库
func (f *MockRepositoryFactory) GetRaftStateRepository() RaftStateRepository {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.mockRepo
}

// GetSnapshotRepository 获取快照仓库
func (f *MockRepositoryFactory) GetSnapshotRepository() SnapshotRepository {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.mockRepo
}

// GetKVRepository 获取KV仓库
func (f *MockRepositoryFactory) GetKVRepository() KVRepository {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.mockRepo
}

// GetTransactionRepository 获取事务仓库
func (f *MockRepositoryFactory) GetTransactionRepository() TransactionRepository {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.mockRepo
}

// Close 关闭工厂和所有仓库
func (f *MockRepositoryFactory) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.mockRepo != nil {
		return f.mockRepo.Close()
	}
	return nil
}

// HealthCheck 健康检查
func (f *MockRepositoryFactory) HealthCheck() error {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.mockRepo != nil {
		return f.mockRepo.HealthCheck(nil)
	}
	return nil
}

// GetMockRepository 获取底层的模拟仓库（用于测试）
func (f *MockRepositoryFactory) GetMockRepository() *MockRepository {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.mockRepo
}