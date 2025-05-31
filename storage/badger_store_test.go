package storage

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"

	"qklzl/fsm"

	"github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// 创建临时测试存储
func createTestBadgerStorage(t *testing.T) (*BadgerStorage, string, func()) {
	// 创建临时目录，使用测试名称作为前缀以确保唯一性
	dir, err := os.MkdirTemp("", fmt.Sprintf("badger-test-%s-*", t.Name()))
	require.NoError(t, err)

	// 确保目录存在且为空
	err = os.RemoveAll(dir)
	require.NoError(t, err)
	err = os.MkdirAll(dir, 0755)
	require.NoError(t, err)

	// 设置BadgerDB选项
	opts := badger.DefaultOptions("").WithInMemory(true) // 使用内存模式，不设置目录
	opts.Logger = nil                                    // 禁用Badger的日志输出
	opts.SyncWrites = false                              // 禁用同步写入以提高测试速度
	opts.NumVersionsToKeep = 1                           // 只保留一个版本
	opts.DetectConflicts = false                         // 禁用冲突检测
	opts.NumGoroutines = 1                               // 限制goroutine数量

	// 创建BadgerDB实例
	db, err := badger.Open(opts)
	require.NoError(t, err)

	// 创建状态机
	stateMachine := fsm.NewKVStateMachine(db)

	// 创建存储
	store, err := NewBadgerStorage(dir, nil, stateMachine)
	require.NoError(t, err)

	// 清理函数
	cleanup := func() {
		// 先停止存储服务
		if store != nil {
			store.Stop()
		}

		// 确保数据库正确关闭
		if db != nil {
			err := db.Close()
			if err != nil {
				t.Logf("关闭数据库时出错: %v", err)
			}
		}

		// 等待一小段时间确保资源释放
		time.Sleep(100 * time.Millisecond)

		// 删除目录及其内容
		if err := os.RemoveAll(dir); err != nil {
			t.Logf("删除测试目录时出错: %v", err)
		}
	}

	// 注册清理函数，确保即使测试panic也能清理
	t.Cleanup(cleanup)

	return store, dir, cleanup
}

// 测试初始状态
func TestInitialState(t *testing.T) {
	store, _, cleanup := createTestBadgerStorage(t)
	defer cleanup()

	// 测试空存储的初始状态
	hardState, confState, err := store.InitialState()
	assert.NoError(t, err)
	assert.True(t, IsEmptyHardState(hardState))
	assert.True(t, IsEmptyConfState(confState))

	// 保存硬状态和配置状态
	testHardState := raftpb.HardState{
		Term:   1,
		Vote:   2,
		Commit: 3,
	}
	testConfState := raftpb.ConfState{
		Voters: []uint64{1, 2, 3},
	}

	err = store.SaveState(testHardState, testConfState)
	assert.NoError(t, err)

	// 重新加载状态
	hardState, confState, err = store.InitialState()
	assert.NoError(t, err)
	assert.Equal(t, testHardState, hardState)
	assert.Equal(t, testConfState, confState)
}

// 测试日志条目
func TestEntries(t *testing.T) {
	store, _, cleanup := createTestBadgerStorage(t)
	defer cleanup()

	// 等待存储初始化完成
	time.Sleep(100 * time.Millisecond)

	// 准备测试日志条目
	entries := []raftpb.Entry{
		{Index: 1, Term: 1, Data: []byte("entry1")},
		{Index: 2, Term: 1, Data: []byte("entry2")},
		{Index: 3, Term: 2, Data: []byte("entry3")},
		{Index: 4, Term: 2, Data: []byte(bytes.Repeat([]byte("large"), 1000))}, // 添加一个大的日志条目
	}

	// 保存日志条目并等待完成
	err := store.SaveEntries(entries)
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	// 测试1：获取全部日志条目
	retrievedEntries, err := store.Entries(1, 5, 0)
	require.NoError(t, err)
	require.Equal(t, len(entries), len(retrievedEntries))
	for i, entry := range entries {
		assert.Equal(t, entry, retrievedEntries[i])
	}

	// 测试2：测试大小限制 - 应该至少返回一个条目
	maxSize := uint64(entries[0].Size() / 2) // 设置一个小于第一个条目大小的限制
	retrievedEntries, err = store.Entries(1, 5, maxSize)
	require.NoError(t, err)
	require.Equal(t, 1, len(retrievedEntries))
	assert.Equal(t, entries[0], retrievedEntries[0])

	// 测试3：测试大小限制 - 返回多个条目但不超过限制
	maxSize = uint64(entries[0].Size() + entries[1].Size() + entries[2].Size()/2)
	retrievedEntries, err = store.Entries(1, 5, maxSize)
	require.NoError(t, err)
	assert.True(t, len(retrievedEntries) >= 2, "应该至少返回2个条目")
	assert.Equal(t, entries[0], retrievedEntries[0])
	assert.Equal(t, entries[1], retrievedEntries[1])

	// 测试4：测试大条目的处理
	maxSize = uint64(entries[3].Size() / 2)
	retrievedEntries, err = store.Entries(4, 5, maxSize)
	require.NoError(t, err)
	require.Equal(t, 1, len(retrievedEntries))
	assert.Equal(t, entries[3], retrievedEntries[0])

	// 测试5：索引越界
	_, err = store.Entries(5, 6, 0)
	require.Error(t, err)

	// 测试6：压缩后的日志访问
	// 先创建快照以确保不会删除未持久化的数据
	testSnapshot := raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index: 2,
			Term:  1,
			ConfState: raftpb.ConfState{
				Voters: []uint64{1, 2, 3},
			},
		},
		Data: []byte("snapshot data"),
	}
	err = store.SaveSnapshot(testSnapshot)
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	// 压缩日志
	err = store.CompactLog(3)
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	// 验证压缩后的状态
	firstIndex, err := store.FirstIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(3), firstIndex)

	// 验证可以访问未压缩的日志
	retrievedEntries, err = store.Entries(3, 5, 0)
	require.NoError(t, err)
	assert.Equal(t, 2, len(retrievedEntries))
	assert.Equal(t, entries[2:], retrievedEntries)
}

// 测试获取任期
func TestTerm(t *testing.T) {
	store, _, cleanup := createTestBadgerStorage(t)
	defer cleanup()

	// 准备测试日志条目
	entries := []raftpb.Entry{
		{Index: 1, Term: 1, Data: []byte("entry1")},
		{Index: 2, Term: 1, Data: []byte("entry2")},
		{Index: 3, Term: 2, Data: []byte("entry3")},
	}

	err := store.SaveEntries(entries)
	assert.NoError(t, err)

	// 测试获取任期
	term, err := store.Term(1)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), term)

	term, err = store.Term(3)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), term)

	// 测试索引越界
	_, err = store.Term(4)
	assert.Error(t, err)
}

// 测试最后和第一个索引
func TestIndexes(t *testing.T) {
	store, _, cleanup := createTestBadgerStorage(t)
	defer cleanup()

	// 准备测试日志条目
	entries := []raftpb.Entry{
		{Index: 1, Term: 1, Data: []byte("entry1")},
		{Index: 2, Term: 1, Data: []byte("entry2")},
		{Index: 3, Term: 2, Data: []byte("entry3")},
	}

	err := store.SaveEntries(entries)
	assert.NoError(t, err)

	// 测试最后索引
	lastIndex, err := store.LastIndex()
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), lastIndex)

	// 测试第一个索引
	firstIndex, err := store.FirstIndex()
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), firstIndex)
}

// 测试快照
func TestSnapshot(t *testing.T) {
	store, _, cleanup := createTestBadgerStorage(t)
	defer cleanup()

	// 测试空快照
	snapshot, err := store.Snapshot()
	assert.NoError(t, err)
	assert.True(t, IsEmptySnap(snapshot))

	// 创建测试快照
	testSnapshot := raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index: 3,
			Term:  2,
			ConfState: raftpb.ConfState{
				Voters: []uint64{1, 2, 3},
			},
		},
		Data: []byte("snapshot data"),
	}

	err = store.SaveSnapshot(testSnapshot)
	assert.NoError(t, err)

	// 加载快照
	loadedSnapshot, err := store.Snapshot()
	assert.NoError(t, err)
	assert.Equal(t, testSnapshot, loadedSnapshot)
}

// 测试日志压缩
func TestCompactLog(t *testing.T) {
	store, _, cleanup := createTestBadgerStorage(t)
	defer cleanup()

	// 等待存储初始化完成
	time.Sleep(100 * time.Millisecond)

	// 准备测试日志条目
	entries := []raftpb.Entry{
		{Index: 1, Term: 1, Data: []byte("entry1")},
		{Index: 2, Term: 1, Data: []byte("entry2")},
		{Index: 3, Term: 2, Data: []byte("entry3")},
		{Index: 4, Term: 2, Data: []byte("entry4")},
		{Index: 5, Term: 3, Data: []byte("entry5")},
	}

	// 保存日志条目并等待完成
	err := store.SaveEntries(entries)
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	// 创建快照以确保不会删除未持久化的数据
	testSnapshot := raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index: 3,
			Term:  2,
			ConfState: raftpb.ConfState{
				Voters: []uint64{1, 2, 3},
			},
		},
		Data: []byte("snapshot data"),
	}
	err = store.SaveSnapshot(testSnapshot)
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	// 压缩日志
	err = store.CompactLog(4)
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	// 验证压缩结果
	// 1. 检查第一个可用的索引
	firstIndex, err := store.FirstIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(4), firstIndex)

	// 2. 尝试访问已压缩的日志
	_, err = store.Entries(3, 4, 0)
	assert.Equal(t, ErrLogCompacted, err)

	// 3. 检查未压缩的日志是否可用
	remainingEntries, err := store.Entries(4, 6, 0)
	require.NoError(t, err)
	assert.Equal(t, 2, len(remainingEntries))
	assert.Equal(t, entries[3:], remainingEntries)

	// 4. 验证快照是否正确保存
	snapshot, err := store.Snapshot()
	require.NoError(t, err)
	assert.Equal(t, testSnapshot.Metadata.Index, snapshot.Metadata.Index)
	assert.Equal(t, testSnapshot.Metadata.Term, snapshot.Metadata.Term)

	// 5. 测试压缩点等于第一个索引
	err = store.CompactLog(firstIndex)
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	// 验证压缩后的状态
	newFirstIndex, err := store.FirstIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(4), newFirstIndex)

	// 6. 测试压缩点小于第一个索引
	err = store.CompactLog(newFirstIndex - 1)
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	// 验证最终状态
	finalFirstIndex, err := store.FirstIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(4), finalFirstIndex)

	finalLastIndex, err := store.LastIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(5), finalLastIndex)

	// 验证最后一个条目
	lastEntry, err := store.Entries(finalFirstIndex, finalLastIndex+1, 0)
	require.NoError(t, err)
	assert.Equal(t, 2, len(lastEntry))
	assert.Equal(t, entries[3:], lastEntry)
}

// 测试保存状态
func TestSaveState(t *testing.T) {
	store, _, cleanup := createTestBadgerStorage(t)
	defer cleanup()

	// 创建测试状态
	hardState := raftpb.HardState{
		Term:   2,
		Vote:   3,
		Commit: 4,
	}
	confState := raftpb.ConfState{
		Voters:   []uint64{1, 2, 3},
		Learners: []uint64{4, 5},
	}

	// 保存状态
	err := store.SaveState(hardState, confState)
	assert.NoError(t, err)

	// 验证保存的状态
	loadedHardState, loadedConfState, err := store.InitialState()
	assert.NoError(t, err)
	assert.Equal(t, hardState, loadedHardState)
	assert.Equal(t, confState, loadedConfState)
}

// 测试保存日志条目
func TestSaveEntries(t *testing.T) {
	store, _, cleanup := createTestBadgerStorage(t)
	defer cleanup()

	// 创建测试日志条目
	entries := []raftpb.Entry{
		{Index: 1, Term: 1, Data: []byte("test1")},
		{Index: 2, Term: 1, Data: []byte("test2")},
		{Index: 3, Term: 2, Data: []byte("test3")},
	}

	// 保存日志条目
	err := store.SaveEntries(entries)
	assert.NoError(t, err)

	// 验证保存的日志条目
	loadedEntries, err := store.Entries(1, 4, 0)
	assert.NoError(t, err)
	assert.Equal(t, entries, loadedEntries)
}

// 测试保存快照
func TestSaveSnapshot(t *testing.T) {
	store, _, cleanup := createTestBadgerStorage(t)
	defer cleanup()

	// 创建测试快照
	snapshot := raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index:     100,
			Term:      2,
			ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}},
		},
		Data: []byte("snapshot data"),
	}

	// 保存快照
	err := store.SaveSnapshot(snapshot)
	assert.NoError(t, err)

	// 验证保存的快照
	loadedSnapshot, err := store.Snapshot()
	assert.NoError(t, err)
	assert.Equal(t, snapshot, loadedSnapshot)

	// 测试保存空快照
	err = store.SaveSnapshot(raftpb.Snapshot{})
	assert.NoError(t, err)
}

// 测试原子性保存
func TestAppend(t *testing.T) {
	store, _, cleanup := createTestBadgerStorage(t)
	defer cleanup()

	// 等待存储初始化完成
	time.Sleep(100 * time.Millisecond)

	// 准备测试数据
	hardState := &raftpb.HardState{
		Term:   1,
		Vote:   2,
		Commit: 3,
	}
	confState := &raftpb.ConfState{
		Voters: []uint64{1, 2, 3},
	}
	entries := []raftpb.Entry{
		{Term: 1, Index: 1, Data: []byte("test1")},
		{Term: 2, Index: 2, Data: []byte("test2")},
	}

	// 原子性保存
	err := store.Append(hardState, confState, entries)
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	// 验证硬状态
	savedHardState, err := store.GetHardState()
	require.NoError(t, err)
	assert.Equal(t, hardState, savedHardState)

	// 验证配置状态
	savedConfState, err := store.GetConfState()
	require.NoError(t, err)
	assert.Equal(t, confState, savedConfState)

	// 验证日志条目
	savedEntries, err := store.Entries(1, 3, 0)
	require.NoError(t, err)
	assert.Equal(t, entries, savedEntries)

	// 测试空状态
	err = store.Append(nil, nil, nil)
	require.NoError(t, err)

	// 测试部分状态
	err = store.Append(hardState, nil, nil)
	require.NoError(t, err)
	err = store.Append(nil, confState, nil)
	require.NoError(t, err)
	err = store.Append(nil, nil, entries)
	require.NoError(t, err)
}

// 测试保存 Raft 硬状态
func TestSaveRaftState(t *testing.T) {
	store, _, cleanup := createTestBadgerStorage(t)
	defer cleanup()

	// 创建测试硬状态
	hardState := raftpb.HardState{
		Term:   5,
		Vote:   6,
		Commit: 7,
	}

	// 保存硬状态
	err := store.SaveRaftState(hardState)
	assert.NoError(t, err)

	// 验证保存的硬状态
	loadedHardState, _, err := store.InitialState()
	assert.NoError(t, err)
	assert.Equal(t, hardState, loadedHardState)
}

// 测试批量写入和资源泄漏
func TestBatchWriteAndResourceLeak(t *testing.T) {
	store, _, cleanup := createTestBadgerStorage(t)
	defer cleanup()

	// 等待存储初始化完成
	time.Sleep(100 * time.Millisecond)

	// 准备大量测试数据
	var entries []raftpb.Entry
	for i := uint64(1); i <= 1000; i++ {
		entries = append(entries, raftpb.Entry{
			Term:  1,
			Index: i,
			Data:  []byte(fmt.Sprintf("entry-%d", i)),
		})
	}

	// 批量保存日志条目
	err := store.SaveEntries(entries)
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	// 验证保存结果
	firstIndex, err := store.FirstIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), firstIndex)

	lastIndex, err := store.LastIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(1000), lastIndex)

	// 验证日志条目内容
	retrievedEntries, err := store.Entries(1, 101, 0) // 获取前100个条目
	require.NoError(t, err)
	assert.Equal(t, 100, len(retrievedEntries))
	for i, entry := range retrievedEntries {
		assert.Equal(t, entries[i], entry)
	}

	// 测试压缩
	err = store.CompactLog(500)
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	// 验证压缩后的状态
	firstIndex, err = store.FirstIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(500), firstIndex)

	// 验证压缩后的日志条目
	retrievedEntries, err = store.Entries(500, 600, 0)
	require.NoError(t, err)
	assert.Equal(t, 100, len(retrievedEntries))
	for i, entry := range retrievedEntries {
		expectedIndex := uint64(500 + i)
		assert.Equal(t, expectedIndex, entry.Index)
		assert.Equal(t, entries[expectedIndex-1], entry)
	}

	// 测试资源释放
	store.Stop()
	time.Sleep(100 * time.Millisecond)

	// 验证停止后的操作会返回错误
	_, err = store.FirstIndex()
	assert.Error(t, err)
}

// 测试大量日志条目压缩
func TestCompactLargeLog(t *testing.T) {
	store, _, cleanup := createTestBadgerStorage(t)
	defer cleanup()

	// 等待存储初始化完成
	time.Sleep(100 * time.Millisecond)

	// 准备大量测试日志条目
	var entries []raftpb.Entry
	for i := uint64(1); i <= 1000; i++ {
		entries = append(entries, raftpb.Entry{
			Index: i,
			Term:  i/100 + 1, // 每100个条目一个任期
			Data:  []byte(fmt.Sprintf("entry-%d", i)),
		})
	}

	// 分批保存日志条目
	batchSize := 100
	for i := 0; i < len(entries); i += batchSize {
		end := i + batchSize
		if end > len(entries) {
			end = len(entries)
		}
		err := store.SaveEntries(entries[i:end])
		require.NoError(t, err)
		time.Sleep(10 * time.Millisecond)
	}

	// 创建快照
	testSnapshot := raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index: 500,
			Term:  6,
			ConfState: raftpb.ConfState{
				Voters: []uint64{1, 2, 3},
			},
		},
		Data: []byte("snapshot data"),
	}
	err := store.SaveSnapshot(testSnapshot)
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	// 压缩前半部分日志
	err = store.CompactLog(501)
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	// 验证压缩结果
	firstIndex, err := store.FirstIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(501), firstIndex)

	// 验证剩余日志
	remainingEntries, err := store.Entries(501, 601, 0)
	require.NoError(t, err)
	assert.Equal(t, 100, len(remainingEntries))
	for i, entry := range remainingEntries {
		expectedIndex := uint64(501 + i)
		assert.Equal(t, expectedIndex, entry.Index)
		assert.Equal(t, entries[expectedIndex-1], entry)
	}

	// 验证快照
	snapshot, err := store.Snapshot()
	require.NoError(t, err)
	assert.Equal(t, testSnapshot.Metadata.Index, snapshot.Metadata.Index)
	assert.Equal(t, testSnapshot.Metadata.Term, snapshot.Metadata.Term)

	// 再次压缩一部分日志
	err = store.CompactLog(701)
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	// 验证最终状态
	finalFirstIndex, err := store.FirstIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(701), finalFirstIndex)

	finalLastIndex, err := store.LastIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(1000), finalLastIndex)

	// 验证最后的日志条目
	lastEntries, err := store.Entries(701, 801, 0)
	require.NoError(t, err)
	assert.Equal(t, 100, len(lastEntries))
	for i, entry := range lastEntries {
		expectedIndex := uint64(701 + i)
		assert.Equal(t, expectedIndex, entry.Index)
		assert.Equal(t, entries[expectedIndex-1], entry)
	}
}
