package storage

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// 创建临时测试存储
func createTestBadgerStorage(t *testing.T) (*BadgerStorage, string, func()) {
	// 创建临时目录
	dir, err := os.MkdirTemp("", "badger-storage-test-*")
	require.NoError(t, err)

	// 创建存储
	store, err := NewBadgerStorage(dir, nil)
	require.NoError(t, err)

	// 清理函数
	cleanup := func() {
		store.Stop()
		os.RemoveAll(dir)
	}

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

	// 准备测试日志条目
	entries := []raftpb.Entry{
		{Index: 1, Term: 1, Data: []byte("entry1")},
		{Index: 2, Term: 1, Data: []byte("entry2")},
		{Index: 3, Term: 2, Data: []byte("entry3")},
	}

	err := store.SaveEntries(entries)
	assert.NoError(t, err)

	// 获取全部日志条目
	retrievedEntries, err := store.Entries(1, 4, 0)
	assert.NoError(t, err)
	assert.Equal(t, len(entries), len(retrievedEntries))
	for i, entry := range entries {
		assert.Equal(t, entry, retrievedEntries[i])
	}

	// 测试大小限制
	retrievedEntries, err = store.Entries(1, 4, uint64(entries[0].Size()))
	assert.NoError(t, err)
	assert.Equal(t, 1, len(retrievedEntries))

	// 测试索引越界
	_, err = store.Entries(4, 5, 0)
	assert.Error(t, err, "Expected an error for out-of-range entries")
	assert.Contains(t, err.Error(), "entries index out of range")
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

	// 准备测试日志条目
	entries := []raftpb.Entry{
		{Index: 1, Term: 1, Data: []byte("entry1")},
		{Index: 2, Term: 1, Data: []byte("entry2")},
		{Index: 3, Term: 2, Data: []byte("entry3")},
	}

	err := store.SaveEntries(entries)
	assert.NoError(t, err)

	// 压缩日志
	err = store.CompactLog(2)
	assert.NoError(t, err)

	// 检查压缩后的状态
	firstIndex, err := store.FirstIndex()
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), firstIndex)

	// 尝试获取被压缩的日志条目
	_, err = store.Entries(1, 2, 0)
	assert.Error(t, err)
	assert.Equal(t, ErrLogCompacted, err)

	// 检查快照
	snapshot, err := store.Snapshot()
	assert.NoError(t, err)
	assert.False(t, IsEmptySnap(snapshot))
	assert.Equal(t, uint64(2), snapshot.Metadata.Index)
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

	// 创建测试数据
	hardState := &raftpb.HardState{
		Term:   2,
		Vote:   3,
		Commit: 4,
	}
	confState := &raftpb.ConfState{
		Voters: []uint64{1, 2, 3},
	}
	entries := []raftpb.Entry{
		{Index: 1, Term: 1, Data: []byte("test1")},
		{Index: 2, Term: 2, Data: []byte("test2")},
	}

	// 原子性保存
	err := store.Append(hardState, confState, entries)
	assert.NoError(t, err)

	// 验证硬状态
	loadedHardState, loadedConfState, err := store.InitialState()
	assert.NoError(t, err)
	assert.Equal(t, *hardState, loadedHardState)
	assert.Equal(t, *confState, loadedConfState)

	// 验证日志条目
	loadedEntries, err := store.Entries(1, 3, 0)
	assert.NoError(t, err)
	assert.Equal(t, entries, loadedEntries)

	// 测试空状态保存
	err = store.Append(nil, nil, nil)
	assert.NoError(t, err)
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
