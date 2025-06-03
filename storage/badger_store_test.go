package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"qklzl/fsm"

	"github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// 创建临时测试目录
func createTempDir(t *testing.T) string {
	dir, err := os.MkdirTemp("", "badger-test-*")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(dir)
	})
	return dir
}

// 创建BadgerDB内存实例
func createBadgerDB(t *testing.T) *badger.DB {
	opts := badger.DefaultOptions("").WithInMemory(true)
	opts.Logger = nil // 禁用日志
	db, err := badger.Open(opts)
	require.NoError(t, err)
	t.Cleanup(func() {
		db.Close()
	})
	return db
}

// 测试BadgerDB基本读写功能
func TestBadgerDBBasicOperations(t *testing.T) {
	// 创建BadgerDB内存实例
	db := createBadgerDB(t)

	// 创建状态机
	stateMachine := fsm.NewKVStateMachine(db)

	// 创建存储配置
	config := &BadgerConfig{
		GCInterval:       time.Second * 5,
		GCDiscardRatio:   0.5,
		SnapshotInterval: time.Second * 5,
		SnapshotCount:    100,
	}

	// 创建BadgerStorage，使用临时目录作为路径
	dir := createTempDir(t)
	store, err := NewBadgerStorage(dir, config, stateMachine)
	require.NoError(t, err)
	defer store.Stop()

	t.Run("基本读写测试", func(t *testing.T) {
		// 测试写入和读取日志条目 - 使用符合状态机要求的JSON格式
		entries := []raftpb.Entry{
			{Term: 1, Index: 1, Data: []byte(`{"op":"put","key":"key1","value":"value1"}`)},
			{Term: 1, Index: 2, Data: []byte(`{"op":"put","key":"key2","value":"value2"}`)},
			{Term: 1, Index: 3, Data: []byte(`{"op":"put","key":"key3","value":"value3"}`)},
		}

		// 保存日志条目
		err := store.SaveEntries(entries)
		require.NoError(t, err)

		// 读取日志条目
		readEntries, err := store.Entries(1, 4, 0)
		require.NoError(t, err)
		assert.Equal(t, len(entries), len(readEntries))

		for i, entry := range readEntries {
			assert.Equal(t, entries[i].Term, entry.Term)
			assert.Equal(t, entries[i].Index, entry.Index)
			assert.Equal(t, entries[i].Data, entry.Data)
		}
	})

	t.Run("FirstIndex和LastIndex测试", func(t *testing.T) {
		firstIndex, err := store.FirstIndex()
		require.NoError(t, err)
		assert.Equal(t, uint64(1), firstIndex)

		lastIndex, err := store.LastIndex()
		require.NoError(t, err)
		assert.Equal(t, uint64(3), lastIndex)
	})
}

// 测试日志持久化和恢复
func TestLogPersistence(t *testing.T) {
	// 创建临时目录
	dir := createTempDir(t)

	// 创建存储配置
	config := &BadgerConfig{
		GCInterval:       time.Second * 5,
		GCDiscardRatio:   0.5,
		SnapshotInterval: time.Second * 5,
		SnapshotCount:    100,
	}

	// 第一步：创建存储实例并写入数据
	{
		// 创建BadgerDB内存实例
		db := createBadgerDB(t)

		// 创建状态机
		stateMachine := fsm.NewKVStateMachine(db)

		store, err := NewBadgerStorage(dir, config, stateMachine)
		require.NoError(t, err)

		// 写入日志条目 - 使用符合状态机要求的JSON格式
		entries := []raftpb.Entry{
			{Term: 1, Index: 1, Data: []byte(`{"op":"put","key":"key1","value":"value1"}`)},
			{Term: 1, Index: 2, Data: []byte(`{"op":"put","key":"key2","value":"value2"}`)},
			{Term: 2, Index: 3, Data: []byte(`{"op":"put","key":"key3","value":"value3"}`)},
			{Term: 2, Index: 4, Data: []byte(`{"op":"put","key":"key4","value":"value4"}`)},
			{Term: 3, Index: 5, Data: []byte(`{"op":"put","key":"key5","value":"value5"}`)},
		}
		err = store.SaveEntries(entries)
		require.NoError(t, err)

		// 获取Term
		term, err := store.Term(3)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), term)

		// 关闭存储
		store.Stop()
		time.Sleep(100 * time.Millisecond) // 确保完全关闭
	}

	// 第二步：重新打开存储并验证数据
	{
		// 重新创建BadgerDB内存实例
		db := createBadgerDB(t)

		// 重新创建状态机
		stateMachine := fsm.NewKVStateMachine(db)

		store, err := NewBadgerStorage(dir, config, stateMachine)
		require.NoError(t, err)
		defer store.Stop()

		// 验证FirstIndex和LastIndex
		firstIndex, err := store.FirstIndex()
		require.NoError(t, err)
		assert.Equal(t, uint64(1), firstIndex)

		lastIndex, err := store.LastIndex()
		require.NoError(t, err)
		assert.Equal(t, uint64(5), lastIndex)

		// 读取日志条目并验证
		entries, err := store.Entries(1, 6, 0)
		require.NoError(t, err)
		assert.Equal(t, 5, len(entries))

		// 验证每个条目的索引和任期
		expectedTerms := []uint64{1, 1, 2, 2, 3}
		for i := uint64(1); i <= 5; i++ {
			entry := entries[i-1]
			assert.Equal(t, i, entry.Index)
			assert.Equal(t, expectedTerms[i-1], entry.Term)

			// 验证数据格式符合JSON要求
			expectedData := fmt.Sprintf(`{"op":"put","key":"key%d","value":"value%d"}`, i, i)
			assert.JSONEq(t, expectedData, string(entry.Data))
		}

		// 测试Term函数
		for i := uint64(1); i <= 5; i++ {
			term, err := store.Term(i)
			require.NoError(t, err)
			assert.Equal(t, expectedTerms[i-1], term)
		}
	}
}

// 测试状态持久化和恢复
func TestStatePersistence(t *testing.T) {
	// 创建临时目录
	dir := createTempDir(t)

	// 创建存储配置
	config := &BadgerConfig{
		GCInterval:       time.Second * 5,
		GCDiscardRatio:   0.5,
		SnapshotInterval: time.Second * 5,
		SnapshotCount:    100,
	}

	// 第一步：创建存储实例并保存状态
	{
		// 创建BadgerDB内存实例
		db := createBadgerDB(t)

		// 创建状态机
		stateMachine := fsm.NewKVStateMachine(db)

		store, err := NewBadgerStorage(dir, config, stateMachine)
		require.NoError(t, err)

		// 创建硬状态
		hardState := raftpb.HardState{
			Term:   5,
			Vote:   2,
			Commit: 10,
		}

		// 创建配置状态
		confState := raftpb.ConfState{
			Voters: []uint64{1, 2, 3},
		}

		// 保存状态
		err = store.SaveState(hardState, confState)
		require.NoError(t, err)

		// 关闭存储
		store.Stop()
		time.Sleep(100 * time.Millisecond) // 确保完全关闭
	}

	// 第二步：重新打开存储并验证状态
	{
		// 重新创建BadgerDB内存实例
		db := createBadgerDB(t)

		// 重新创建状态机
		stateMachine := fsm.NewKVStateMachine(db)

		store, err := NewBadgerStorage(dir, config, stateMachine)
		require.NoError(t, err)
		defer store.Stop()

		// 获取初始状态
		hs, cs, err := store.InitialState()
		require.NoError(t, err)

		// 验证硬状态
		assert.Equal(t, uint64(5), hs.Term)
		assert.Equal(t, uint64(2), hs.Vote)
		assert.Equal(t, uint64(10), hs.Commit)

		// 验证配置状态
		assert.Equal(t, []uint64{1, 2, 3}, cs.Voters)
	}
}

// 测试日志压缩
func TestLogCompaction(t *testing.T) {
	// 创建临时目录
	dir := createTempDir(t)

	// 创建BadgerDB内存实例
	db := createBadgerDB(t)

	// 创建状态机
	stateMachine := fsm.NewKVStateMachine(db)

	// 创建存储配置
	config := &BadgerConfig{
		GCInterval:       time.Second * 5,
		GCDiscardRatio:   0.5,
		SnapshotInterval: time.Second * 5,
		SnapshotCount:    100,
	}

	// 创建BadgerStorage
	store, err := NewBadgerStorage(dir, config, stateMachine)
	require.NoError(t, err)
	defer store.Stop()

	// 写入日志条目 - 使用符合状态机要求的JSON格式
	entries := []raftpb.Entry{
		{Term: 1, Index: 1, Data: []byte(`{"op":"put","key":"key1","value":"value1"}`)},
		{Term: 1, Index: 2, Data: []byte(`{"op":"put","key":"key2","value":"value2"}`)},
		{Term: 2, Index: 3, Data: []byte(`{"op":"put","key":"key3","value":"value3"}`)},
		{Term: 2, Index: 4, Data: []byte(`{"op":"put","key":"key4","value":"value4"}`)},
		{Term: 3, Index: 5, Data: []byte(`{"op":"put","key":"key5","value":"value5"}`)},
	}
	err = store.SaveEntries(entries)
	require.NoError(t, err)

	// 压缩日志
	err = store.CompactLog(3)
	require.NoError(t, err)

	// 验证FirstIndex已更新
	firstIndex, err := store.FirstIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(3), firstIndex)

	// 验证无法获取已压缩的日志
	_, err = store.Entries(1, 3, 0)
	assert.Equal(t, ErrLogCompacted, err)

	// 验证可以获取未压缩的日志
	entries, err = store.Entries(3, 6, 0)
	require.NoError(t, err)
	assert.Equal(t, 3, len(entries))
	assert.Equal(t, uint64(3), entries[0].Index)
	assert.Equal(t, uint64(4), entries[1].Index)
	assert.Equal(t, uint64(5), entries[2].Index)

	// 验证数据格式
	for i := 0; i < 3; i++ {
		index := i + 3
		expectedData := fmt.Sprintf(`{"op":"put","key":"key%d","value":"value%d"}`, index, index)
		assert.JSONEq(t, expectedData, string(entries[i].Data))
	}
}

// 测试快照功能
func TestSnapshot(t *testing.T) {
	// 创建临时目录
	dir := createTempDir(t)
	snapshotDir := filepath.Join(dir, "snapshots")
	err := os.MkdirAll(snapshotDir, 0755)
	require.NoError(t, err)

	// 创建BadgerDB内存实例
	db := createBadgerDB(t)

	// 创建状态机
	stateMachine := fsm.NewKVStateMachine(db)

	// 创建存储配置
	config := &BadgerConfig{
		GCInterval:       time.Second * 5,
		GCDiscardRatio:   0.5,
		SnapshotInterval: time.Second * 5,
		SnapshotCount:    100,
	}

	// 创建BadgerStorage
	store, err := NewBadgerStorage(dir, config, stateMachine)
	require.NoError(t, err)
	defer store.Stop()

	// 写入一些数据到状态机 - 使用符合状态机要求的JSON格式
	stateMachine.Apply(raftpb.Entry{Term: 1, Index: 1, Data: []byte(`{"op":"put","key":"key1","value":"value1"}`)})
	stateMachine.Apply(raftpb.Entry{Term: 1, Index: 2, Data: []byte(`{"op":"put","key":"key2","value":"value2"}`)})
	stateMachine.Apply(raftpb.Entry{Term: 1, Index: 3, Data: []byte(`{"op":"put","key":"key3","value":"value3"}`)})

	// 创建快照
	snapshotData, err := stateMachine.SaveSnapshot()
	require.NoError(t, err)

	snapshot := raftpb.Snapshot{
		Data: snapshotData,
		Metadata: raftpb.SnapshotMetadata{
			Index:     3,
			Term:      2,
			ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}},
		},
	}

	// 保存快照
	err = store.SaveSnapshot(snapshot)
	require.NoError(t, err)

	// 获取快照
	loadedSnapshot, err := store.Snapshot()
	require.NoError(t, err)
	assert.Equal(t, snapshot.Metadata.Index, loadedSnapshot.Metadata.Index)
	assert.Equal(t, snapshot.Metadata.Term, loadedSnapshot.Metadata.Term)
	assert.Equal(t, snapshot.Data, loadedSnapshot.Data)
	assert.Equal(t, snapshot.Metadata.ConfState.Voters, loadedSnapshot.Metadata.ConfState.Voters)
}

// 测试Append方法
func TestAppend(t *testing.T) {
	// 创建临时目录
	dir := createTempDir(t)

	// 创建BadgerDB内存实例
	db := createBadgerDB(t)

	// 创建状态机
	stateMachine := fsm.NewKVStateMachine(db)

	// 创建存储配置
	config := &BadgerConfig{
		GCInterval:       time.Second * 5,
		GCDiscardRatio:   0.5,
		SnapshotInterval: time.Second * 5,
		SnapshotCount:    100,
	}

	// 创建BadgerStorage
	store, err := NewBadgerStorage(dir, config, stateMachine)
	require.NoError(t, err)
	defer store.Stop()

	// 创建硬状态
	hardState := raftpb.HardState{
		Term:   5,
		Vote:   2,
		Commit: 10,
	}

	// 创建配置状态
	confState := raftpb.ConfState{
		Voters: []uint64{1, 2, 3},
	}

	// 创建日志条目 - 使用符合状态机要求的JSON格式
	entries := []raftpb.Entry{
		{Term: 5, Index: 11, Data: []byte(`{"op":"put","key":"key11","value":"value11"}`)},
		{Term: 5, Index: 12, Data: []byte(`{"op":"put","key":"key12","value":"value12"}`)},
	}

	// 使用Append方法一次性保存所有状态
	err = store.Append(&hardState, &confState, entries)
	require.NoError(t, err)

	// 验证硬状态
	hs, cs, err := store.InitialState()
	require.NoError(t, err)
	assert.Equal(t, hardState.Term, hs.Term)
	assert.Equal(t, hardState.Vote, hs.Vote)
	assert.Equal(t, hardState.Commit, hs.Commit)

	// 验证配置状态
	assert.Equal(t, confState.Voters, cs.Voters)

	// 验证日志条目
	readEntries, err := store.Entries(11, 13, 0)
	require.NoError(t, err)
	assert.Equal(t, 2, len(readEntries))

	// 验证索引和任期
	assert.Equal(t, entries[0].Term, readEntries[0].Term)
	assert.Equal(t, entries[0].Index, readEntries[0].Index)
	assert.Equal(t, entries[1].Term, readEntries[1].Term)
	assert.Equal(t, entries[1].Index, readEntries[1].Index)

	// 验证数据格式
	assert.JSONEq(t, `{"op":"put","key":"key11","value":"value11"}`, string(readEntries[0].Data))
	assert.JSONEq(t, `{"op":"put","key":"key12","value":"value12"}`, string(readEntries[1].Data))
}

// 测试并发操作
func TestConcurrentOperations(t *testing.T) {
	// 创建临时目录
	dir := createTempDir(t)

	// 创建BadgerDB内存实例
	db := createBadgerDB(t)

	// 创建状态机
	stateMachine := fsm.NewKVStateMachine(db)

	// 创建存储配置
	config := &BadgerConfig{
		GCInterval:       time.Second * 5,
		GCDiscardRatio:   0.5,
		SnapshotInterval: time.Second * 5,
		SnapshotCount:    100,
	}

	// 创建BadgerStorage
	store, err := NewBadgerStorage(dir, config, stateMachine)
	require.NoError(t, err)
	defer store.Stop()

	// 并发写入和读取测试
	const (
		goroutines        = 5
		entriesPerRoutine = 20
	)

	var wg sync.WaitGroup
	wg.Add(goroutines)

	// 用于检测错误的通道
	errCh := make(chan error, goroutines)

	// 启动多个goroutine并发写入
	for g := 0; g < goroutines; g++ {
		go func(routineID int) {
			defer wg.Done()

			baseIndex := uint64(routineID*entriesPerRoutine + 1)
			entries := make([]raftpb.Entry, entriesPerRoutine)

			// 准备日志条目
			for i := 0; i < entriesPerRoutine; i++ {
				index := baseIndex + uint64(i)
				data := fmt.Sprintf(`{"op":"put","key":"key%d_%d","value":"value%d_%d"}`, routineID, i, routineID, i)
				entries[i] = raftpb.Entry{
					Term:  uint64(routineID + 1),
					Index: index,
					Data:  []byte(data),
				}
			}

			// 写入日志条目
			if err := store.SaveEntries(entries); err != nil {
				errCh <- fmt.Errorf("goroutine %d 写入失败: %v", routineID, err)
				return
			}

			// 读取并验证日志条目
			for i := 0; i < entriesPerRoutine; i++ {
				index := baseIndex + uint64(i)
				term, err := store.Term(index)
				if err != nil {
					errCh <- fmt.Errorf("goroutine %d 读取Term失败, 索引 %d: %v", routineID, index, err)
					return
				}

				if term != uint64(routineID+1) {
					errCh <- fmt.Errorf("goroutine %d Term不匹配, 索引 %d: 期望 %d, 实际 %d",
						routineID, index, routineID+1, term)
					return
				}
			}
		}(g)
	}

	// 等待所有goroutine完成
	wg.Wait()
	close(errCh)

	// 检查是否有错误
	for err := range errCh {
		t.Error(err)
	}

	// 验证FirstIndex和LastIndex
	firstIndex, err := store.FirstIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), firstIndex)

	lastIndex, err := store.LastIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(goroutines*entriesPerRoutine), lastIndex)

	// 测试压缩日志
	compactIndex := uint64(goroutines * entriesPerRoutine / 2)
	err = store.CompactLog(compactIndex)
	require.NoError(t, err)

	// 验证FirstIndex已更新
	firstIndex, err = store.FirstIndex()
	require.NoError(t, err)
	assert.Equal(t, compactIndex, firstIndex)
}

// 测试高负载
func TestHighLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过高负载测试")
	}

	// 创建临时目录
	dir := createTempDir(t)

	// 创建BadgerDB内存实例
	db := createBadgerDB(t)

	// 创建状态机
	stateMachine := fsm.NewKVStateMachine(db)

	// 创建存储配置 - 设置更低的快照阈值以触发快照创建
	config := &BadgerConfig{
		GCInterval:       time.Second * 1,
		GCDiscardRatio:   0.5,
		SnapshotInterval: time.Second * 1,
		SnapshotCount:    50, // 较低的阈值，确保测试中会触发快照
	}

	// 创建BadgerStorage
	store, err := NewBadgerStorage(dir, config, stateMachine)
	require.NoError(t, err)
	defer store.Stop()

	// 写入大量日志条目
	const entriesCount = 200

	// 分批写入以模拟真实场景
	batchSize := 20
	for batch := 0; batch < entriesCount/batchSize; batch++ {
		entries := make([]raftpb.Entry, batchSize)
		for i := 0; i < batchSize; i++ {
			index := uint64(batch*batchSize + i + 1)
			data := fmt.Sprintf(`{"op":"put","key":"highload_key%d","value":"highload_value%d"}`, index, index)
			entries[i] = raftpb.Entry{
				Term:  uint64(batch + 1),
				Index: index,
				Data:  []byte(data),
			}
		}

		err := store.SaveEntries(entries)
		require.NoError(t, err)

		// 应用日志到状态机以触发快照
		for i := 0; i < batchSize; i++ {
			err := stateMachine.Apply(entries[i])
			require.NoError(t, err)
		}

		// 短暂暂停，让异步操作有机会执行
		time.Sleep(10 * time.Millisecond)
	}

	// 手动创建快照
	snapshotData, err := stateMachine.SaveSnapshot()
	require.NoError(t, err)
	require.NotEmpty(t, snapshotData, "快照数据不应为空")

	snapshot := raftpb.Snapshot{
		Data: snapshotData,
		Metadata: raftpb.SnapshotMetadata{
			Index:     entriesCount, // 使用最后一个索引
			Term:      10,           // 使用一个有效的任期
			ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}},
		},
	}

	// 保存快照
	err = store.SaveSnapshot(snapshot)
	require.NoError(t, err)

	// 等待可能的异步操作完成
	time.Sleep(2 * time.Second)

	// 验证数据完整性
	lastIndex, err := store.LastIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(entriesCount), lastIndex)

	// 获取快照
	loadedSnapshot, err := store.Snapshot()
	require.NoError(t, err)
	assert.NotNil(t, loadedSnapshot.Data, "加载的快照数据不应为空")
	assert.Greater(t, len(loadedSnapshot.Data), 0, "快照数据长度应大于0")

	// 验证快照元数据
	assert.Equal(t, uint64(entriesCount), loadedSnapshot.Metadata.Index, "快照索引应等于条目数量")

	// 压缩日志到快照索引
	err = store.CompactLog(loadedSnapshot.Metadata.Index)
	require.NoError(t, err)

	// 验证FirstIndex已更新到快照索引
	firstIndex, err := store.FirstIndex()
	require.NoError(t, err)
	assert.True(t, firstIndex == loadedSnapshot.Metadata.Index ||
		firstIndex == loadedSnapshot.Metadata.Index+1,
		"FirstIndex应等于快照索引或快照索引+1, 实际: %d, 期望: %d",
		firstIndex, loadedSnapshot.Metadata.Index)
}
