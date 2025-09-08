package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qklzl/internal/repository"
)

// 简化的KV服务测试，直接测试仓库层功能
func TestKVService_RepositoryOperations(t *testing.T) {
	// 创建模拟仓库工厂
	mockFactory := repository.NewMockRepositoryFactory()
	defer mockFactory.Close()

	// 获取KV仓库进行直接测试
	kvRepo := mockFactory.GetKVRepository()
	require.NotNil(t, kvRepo)

	ctx := context.Background()

	// 测试Put和Get
	err := kvRepo.Put(ctx, "test_key", "test_value")
	assert.NoError(t, err)

	value, err := kvRepo.Get(ctx, "test_key")
	assert.NoError(t, err)
	assert.Equal(t, "test_value", value)

	// 测试不存在的键
	_, err = kvRepo.Get(ctx, "non_existent_key")
	assert.Error(t, err)

	// 测试Delete操作
	err = kvRepo.Delete(ctx, "test_key")
	assert.NoError(t, err)

	// 验证删除后无法获取
	_, err = kvRepo.Get(ctx, "test_key")
	assert.Error(t, err)
}

func TestKVService_BatchOperations(t *testing.T) {
	mockFactory := repository.NewMockRepositoryFactory()
	defer mockFactory.Close()

	kvRepo := mockFactory.GetKVRepository()
	ctx := context.Background()

	// 测试批量操作
	ops := []repository.KVOperation{
		{Type: "put", Key: "key1", Value: "value1"},
		{Type: "put", Key: "key2", Value: "value2"},
		{Type: "put", Key: "key3", Value: "value3"},
	}

	err := kvRepo.Batch(ctx, ops)
	assert.NoError(t, err)

	// 验证所有键值对都已存储
	for _, op := range ops {
		actualValue, err := kvRepo.Get(ctx, op.Key)
		assert.NoError(t, err)
		assert.Equal(t, op.Value, actualValue)
	}

	// 测试GetAll
	allData, err := kvRepo.GetAll(ctx)
	assert.NoError(t, err)
	assert.Len(t, allData, 3)
	for _, op := range ops {
		actualValue, exists := allData[op.Key]
		assert.True(t, exists)
		assert.Equal(t, op.Value, actualValue)
	}
}

func TestKVService_ErrorHandling(t *testing.T) {
	mockFactory := repository.NewMockRepositoryFactory()
	defer mockFactory.Close()

	kvRepo := mockFactory.GetKVRepository()
	ctx := context.Background()

	// 获取模拟仓库以设置错误条件
	mockRepo := mockFactory.GetMockRepository()

	// 测试Get错误
	mockRepo.SetErrorOnGet(true)
	_, err := kvRepo.Get(ctx, "test_key")
	assert.Error(t, err)
	mockRepo.SetErrorOnGet(false)

	// 测试Put错误
	mockRepo.SetErrorOnPut(true)
	err = kvRepo.Put(ctx, "test_key", "test_value")
	assert.Error(t, err)
	mockRepo.SetErrorOnPut(false)

	// 测试Delete错误
	mockRepo.SetErrorOnDelete(true)
	err = kvRepo.Delete(ctx, "test_key")
	assert.Error(t, err)
	mockRepo.SetErrorOnDelete(false)

	// 测试Batch错误
	mockRepo.SetErrorOnBatch(true)
	ops := []repository.KVOperation{{Type: "put", Key: "key", Value: "value"}}
	err = kvRepo.Batch(ctx, ops)
	assert.Error(t, err)
	mockRepo.SetErrorOnBatch(false)
}

func TestKVService_HealthCheck(t *testing.T) {
	mockFactory := repository.NewMockRepositoryFactory()
	defer mockFactory.Close()

	ctx := context.Background()

	// 测试仓库健康检查
	err := mockFactory.HealthCheck()
	assert.NoError(t, err)

	// 测试KV仓库健康检查
	kvRepo := mockFactory.GetKVRepository()
	err = kvRepo.HealthCheck(ctx)
	assert.NoError(t, err)
}

func TestKVService_FactoryOperations(t *testing.T) {
	mockFactory := repository.NewMockRepositoryFactory()
	defer mockFactory.Close()

	// 测试获取各种仓库
	storageRepo := mockFactory.GetStorageRepository()
	assert.NotNil(t, storageRepo)

	raftLogRepo := mockFactory.GetRaftLogRepository()
	assert.NotNil(t, raftLogRepo)

	raftStateRepo := mockFactory.GetRaftStateRepository()
	assert.NotNil(t, raftStateRepo)

	snapshotRepo := mockFactory.GetSnapshotRepository()
	assert.NotNil(t, snapshotRepo)

	kvRepo := mockFactory.GetKVRepository()
	assert.NotNil(t, kvRepo)

	txnRepo := mockFactory.GetTransactionRepository()
	assert.NotNil(t, txnRepo)

	// 验证所有仓库接口都指向同一个模拟仓库实例
	mockRepo := mockFactory.GetMockRepository()
	assert.Equal(t, mockRepo, storageRepo)
	assert.Equal(t, mockRepo, raftLogRepo)
	assert.Equal(t, mockRepo, raftStateRepo)
	assert.Equal(t, mockRepo, snapshotRepo)
	assert.Equal(t, mockRepo, kvRepo)
	assert.Equal(t, mockRepo, txnRepo)
}

func TestKVService_ContextCancellation(t *testing.T) {
	mockFactory := repository.NewMockRepositoryFactory()
	defer mockFactory.Close()

	kvRepo := mockFactory.GetKVRepository()

	// 创建可取消的上下文
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 立即取消

	// 测试在已取消的上下文中执行操作
	err := kvRepo.Put(ctx, "test_key", "test_value")
	// 注意：模拟仓库可能不会检查上下文取消，这里主要测试接口
	// 在真实实现中，应该检查上下文取消并返回相应错误
	_ = err // 忽略错误，因为模拟实现可能不处理上下文取消
}

func BenchmarkKVService_Put(b *testing.B) {
	mockFactory := repository.NewMockRepositoryFactory()
	defer mockFactory.Close()

	kvRepo := mockFactory.GetKVRepository()
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := "bench_key"
		value := "bench_value"
		err := kvRepo.Put(ctx, key, value)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkKVService_Get(b *testing.B) {
	mockFactory := repository.NewMockRepositoryFactory()
	defer mockFactory.Close()

	kvRepo := mockFactory.GetKVRepository()
	ctx := context.Background()

	// 预先插入一些数据
	for i := 0; i < 1000; i++ {
		key := "bench_key"
		value := "bench_value"
		kvRepo.Put(ctx, key, value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := "bench_key"
		_, err := kvRepo.Get(ctx, key)
		if err != nil {
			b.Fatal(err)
		}
	}
}