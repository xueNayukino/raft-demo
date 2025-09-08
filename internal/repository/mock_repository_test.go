package repository

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func TestMockRepository_KVOperations(t *testing.T) {
	mockRepo := NewMockRepository()
	defer mockRepo.Close()

	ctx := context.Background()

	// 测试Put和Get
	err := mockRepo.Put(ctx, "test_key", "test_value")
	assert.NoError(t, err)

	value, err := mockRepo.Get(ctx, "test_key")
	assert.NoError(t, err)
	assert.Equal(t, "test_value", value)

	// 测试不存在的键
	_, err = mockRepo.Get(ctx, "non_existent")
	assert.Error(t, err)

	// 测试Delete
	err = mockRepo.Delete(ctx, "test_key")
	assert.NoError(t, err)

	_, err = mockRepo.Get(ctx, "test_key")
	assert.Error(t, err)
}

func TestMockRepository_BatchOperations(t *testing.T) {
	mockRepo := NewMockRepository()
	defer mockRepo.Close()

	ctx := context.Background()

	// 测试批量操作
	ops := []KVOperation{
		{Type: "put", Key: "key1", Value: "value1"},
		{Type: "put", Key: "key2", Value: "value2"},
		{Type: "delete", Key: "key1"},
	}

	err := mockRepo.Batch(ctx, ops)
	assert.NoError(t, err)

	// 验证结果
	_, err = mockRepo.Get(ctx, "key1")
	assert.Error(t, err) // key1应该被删除

	value, err := mockRepo.Get(ctx, "key2")
	assert.NoError(t, err)
	assert.Equal(t, "value2", value)
}

func TestMockRepository_GetAll(t *testing.T) {
	mockRepo := NewMockRepository()
	defer mockRepo.Close()

	ctx := context.Background()

	// 插入测试数据
	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}

	for k, v := range testData {
		err := mockRepo.Put(ctx, k, v)
		assert.NoError(t, err)
	}

	// 测试GetAll
	allData, err := mockRepo.GetAll(ctx)
	assert.NoError(t, err)
	assert.Equal(t, testData, allData)
}

func TestMockRepository_RaftLogOperations(t *testing.T) {
	mockRepo := NewMockRepository()
	defer mockRepo.Close()

	// 测试初始状态
	firstIndex, err := mockRepo.FirstIndex()
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), firstIndex)

	lastIndex, err := mockRepo.LastIndex()
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), lastIndex)

	// 测试存储日志条目
	entries := []raftpb.Entry{
		{Index: 1, Term: 1, Data: []byte("entry1")},
		{Index: 2, Term: 1, Data: []byte("entry2")},
		{Index: 3, Term: 2, Data: []byte("entry3")},
	}

	err = mockRepo.StoreEntries(entries)
	assert.NoError(t, err)

	// 验证LastIndex更新
	lastIndex, err = mockRepo.LastIndex()
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), lastIndex)

	// 测试获取单个条目
	entry, err := mockRepo.GetEntry(2)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), entry.Index)
	assert.Equal(t, uint64(1), entry.Term)
	assert.Equal(t, []byte("entry2"), entry.Data)

	// 测试获取条目范围
	retrievedEntries, err := mockRepo.GetEntries(1, 3, 0)
	assert.NoError(t, err)
	assert.Len(t, retrievedEntries, 2)
	assert.Equal(t, uint64(1), retrievedEntries[0].Index)
	assert.Equal(t, uint64(2), retrievedEntries[1].Index)

	// 测试压缩
	err = mockRepo.Compact(2)
	assert.NoError(t, err)

	firstIndex, err = mockRepo.FirstIndex()
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), firstIndex)

	// 压缩后的条目应该不可访问
	_, err = mockRepo.GetEntry(1)
	assert.Error(t, err)
}

func TestMockRepository_RaftStateOperations(t *testing.T) {
	mockRepo := NewMockRepository()
	defer mockRepo.Close()

	// 测试硬状态
	hardState := raftpb.HardState{
		Term:   5,
		Vote:   2,
		Commit: 10,
	}

	err := mockRepo.StoreHardState(hardState)
	assert.NoError(t, err)

	retrievedHardState, err := mockRepo.GetHardState()
	assert.NoError(t, err)
	assert.Equal(t, hardState, retrievedHardState)

	// 测试配置状态
	confState := raftpb.ConfState{
		Voters:    []uint64{1, 2, 3},
		Learners:  []uint64{4},
		VotersOutgoing: []uint64{},
	}

	err = mockRepo.StoreConfState(confState)
	assert.NoError(t, err)

	retrievedConfState, err := mockRepo.GetConfState()
	assert.NoError(t, err)
	assert.Equal(t, confState, retrievedConfState)
}

func TestMockRepository_SnapshotOperations(t *testing.T) {
	mockRepo := NewMockRepository()
	defer mockRepo.Close()

	ctx := context.Background()

	// 测试创建快照
	snapshot, err := mockRepo.CreateSnapshot(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, snapshot.Data)

	// 测试存储快照
	testSnapshot := raftpb.Snapshot{
		Data: []byte("test snapshot data"),
		Metadata: raftpb.SnapshotMetadata{
			Index: 100,
			Term:  5,
		},
	}

	err = mockRepo.StoreSnapshot(testSnapshot)
	assert.NoError(t, err)

	// 测试获取快照
	retrievedSnapshot, err := mockRepo.GetSnapshot()
	assert.NoError(t, err)
	assert.Equal(t, testSnapshot.Data, retrievedSnapshot.Data)
	assert.Equal(t, testSnapshot.Metadata.Index, retrievedSnapshot.Metadata.Index)
	assert.Equal(t, testSnapshot.Metadata.Term, retrievedSnapshot.Metadata.Term)

	// 测试应用快照
	err = mockRepo.ApplySnapshot(testSnapshot)
	assert.NoError(t, err)
}

func TestMockRepository_TransactionOperations(t *testing.T) {
	mockRepo := NewMockRepository()
	defer mockRepo.Close()

	ctx := context.Background()
	txnID := uint64(123)

	// 测试开始事务
	err := mockRepo.BeginTransaction(ctx, txnID)
	assert.NoError(t, err)

	// 测试获取事务状态
	state, err := mockRepo.GetTransactionState(ctx, txnID)
	assert.NoError(t, err)
	assert.Equal(t, "prepare", state)

	// 测试添加事务操作
	op := KVOperation{Type: "put", Key: "txn_key", Value: "txn_value"}
	err = mockRepo.AddTransactionOperation(ctx, txnID, op)
	assert.NoError(t, err)

	// 测试提交事务
	err = mockRepo.CommitTransaction(ctx, txnID)
	assert.NoError(t, err)

	state, err = mockRepo.GetTransactionState(ctx, txnID)
	assert.NoError(t, err)
	assert.Equal(t, "commit", state)

	// 测试回滚事务
	txnID2 := uint64(456)
	err = mockRepo.BeginTransaction(ctx, txnID2)
	assert.NoError(t, err)

	err = mockRepo.RollbackTransaction(ctx, txnID2)
	assert.NoError(t, err)

	_, err = mockRepo.GetTransactionState(ctx, txnID2)
	assert.Error(t, err) // 事务应该被删除
}

func TestMockRepository_ErrorSimulation(t *testing.T) {
	mockRepo := NewMockRepository()
	defer mockRepo.Close()

	ctx := context.Background()

	// 测试Get错误
	mockRepo.SetErrorOnGet(true)
	_, err := mockRepo.Get(ctx, "test_key")
	assert.Error(t, err)
	mockRepo.SetErrorOnGet(false)

	// 测试Put错误
	mockRepo.SetErrorOnPut(true)
	err = mockRepo.Put(ctx, "test_key", "test_value")
	assert.Error(t, err)
	mockRepo.SetErrorOnPut(false)

	// 测试Delete错误
	mockRepo.SetErrorOnDelete(true)
	err = mockRepo.Delete(ctx, "test_key")
	assert.Error(t, err)
	mockRepo.SetErrorOnDelete(false)

	// 测试Batch错误
	mockRepo.SetErrorOnBatch(true)
	ops := []KVOperation{{Type: "put", Key: "key", Value: "value"}}
	err = mockRepo.Batch(ctx, ops)
	assert.Error(t, err)
	mockRepo.SetErrorOnBatch(false)
}

func TestMockRepository_HealthCheck(t *testing.T) {
	mockRepo := NewMockRepository()
	defer mockRepo.Close()

	ctx := context.Background()

	// 测试健康检查
	err := mockRepo.HealthCheck(ctx)
	assert.NoError(t, err)
}

func TestMockRepository_Clear(t *testing.T) {
	mockRepo := NewMockRepository()
	defer mockRepo.Close()

	ctx := context.Background()

	// 添加一些数据
	mockRepo.Put(ctx, "key1", "value1")
	mockRepo.Put(ctx, "key2", "value2")

	// 添加日志条目
	entries := []raftpb.Entry{
		{Index: 1, Term: 1, Data: []byte("entry1")},
	}
	mockRepo.StoreEntries(entries)

	// 验证数据存在
	assert.Equal(t, 2, mockRepo.GetKVDataSize())
	assert.Equal(t, 1, mockRepo.GetLogEntriesSize())

	// 清空数据
	mockRepo.Clear()

	// 验证数据被清空
	assert.Equal(t, 0, mockRepo.GetKVDataSize())
	assert.Equal(t, 0, mockRepo.GetLogEntriesSize())

	// 验证索引重置
	firstIndex, _ := mockRepo.FirstIndex()
	lastIndex, _ := mockRepo.LastIndex()
	assert.Equal(t, uint64(1), firstIndex)
	assert.Equal(t, uint64(0), lastIndex)
}

func TestMockRepositoryFactory(t *testing.T) {
	factory := NewMockRepositoryFactory()
	defer factory.Close()

	// 测试获取各种仓库
	storageRepo := factory.GetStorageRepository()
	assert.NotNil(t, storageRepo)

	raftLogRepo := factory.GetRaftLogRepository()
	assert.NotNil(t, raftLogRepo)

	raftStateRepo := factory.GetRaftStateRepository()
	assert.NotNil(t, raftStateRepo)

	snapshotRepo := factory.GetSnapshotRepository()
	assert.NotNil(t, snapshotRepo)

	kvRepo := factory.GetKVRepository()
	assert.NotNil(t, kvRepo)

	txnRepo := factory.GetTransactionRepository()
	assert.NotNil(t, txnRepo)

	// 测试健康检查
	err := factory.HealthCheck()
	assert.NoError(t, err)

	// 测试获取底层模拟仓库
	mockRepo := factory.GetMockRepository()
	assert.NotNil(t, mockRepo)

	// 验证所有仓库接口都指向同一个模拟仓库实例
	assert.Equal(t, mockRepo, storageRepo)
	assert.Equal(t, mockRepo, raftLogRepo)
	assert.Equal(t, mockRepo, raftStateRepo)
	assert.Equal(t, mockRepo, snapshotRepo)
	assert.Equal(t, mockRepo, kvRepo)
	assert.Equal(t, mockRepo, txnRepo)
}