package storage

import (
	"bytes"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"qklzl/fsm"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 创建测试快照管理器
func createTestSnapshotter(t *testing.T, config *SnapshotConfig) (*Snapshotter, *BadgerStorage, *fsm.KVStateMachine, string, func()) {
	// 创建临时目录
	dir, err := os.MkdirTemp("", "snapshotter-test-*")
	require.NoError(t, err)

	// 创建存储
	store, err := NewBadgerStorage(dir, nil)
	require.NoError(t, err)

	// 创建状态机
	stateMachine := fsm.NewKVStateMachine(store.db)

	// 如果没有提供配置，使用默认配置
	if config == nil {
		config = &SnapshotConfig{
			CheckInterval: 1 * time.Second,
			SnapshotCount: 100,
		}
	}

	// 创建快照管理器
	snapshotter := NewSnapshotter(dir, store.db, config, stateMachine)

	// 清理函数
	cleanup := func() {
		snapshotter.Stop()
		store.Stop()
		os.RemoveAll(dir)
	}

	return snapshotter, store, stateMachine, dir, cleanup
}

// 测试快照创建和加载
//func TestSnapshotCreateAndLoad(t *testing.T) {
//	snapshotter, store, _, _, cleanup := createTestSnapshotter(t, nil)
//	defer cleanup()
//
//	// 准备测试数据：保存一些日志条目和硬状态
//	entries := []raftpb.Entry{
//		{Index: 1, Term: 1, Data: []byte("entry1")},
//		{Index: 2, Term: 1, Data: []byte("entry2")},
//		{Index: 3, Term: 2, Data: []byte("entry3")},
//	}
//	err := store.SaveEntries(entries)
//	require.NoError(t, err)
//
//	// 保存硬状态，设置已提交的索引
//	hardState := raftpb.HardState{
//		Term:   2,
//		Vote:   1,
//		Commit: 3,
//	}
//	err = store.SaveRaftState(hardState)
//	require.NoError(t, err)
//
//	// 模拟日志条目累积
//	for i := 0; i < 150; i++ {
//		snapshotter.IncrementLogEntryCount()
//	}
//
//	// 等待异步快照创建
//	time.Sleep(2 * time.Second)
//
//	// 加载快照
//	snapshot, err := snapshotter.LoadNewestSnapshot()
//	assert.NoError(t, err)
//	assert.NotNil(t, snapshot)
//	if snapshot != nil {
//		assert.Equal(t, uint64(3), snapshot.Metadata.Index)
//		assert.Equal(t, uint64(2), snapshot.Metadata.Term)
//	}
//}

// 测试快照应用
//func TestSnapshotApply(t *testing.T) {
//	snapshotter, _, _, _, cleanup := createTestSnapshotter(t, nil)
//	defer cleanup()
//
//	// 创建测试快照
//	testSnapshot := raftpb.Snapshot{
//		Metadata: raftpb.SnapshotMetadata{
//			Index: 100,
//			Term:  2,
//			ConfState: raftpb.ConfState{
//				Voters: []uint64{1, 2, 3},
//			},
//		},
//		Data: []byte("test snapshot data"),
//	}
//
//	// 应用快照
//	err := snapshotter.ApplySnapshot(testSnapshot)
//	assert.NoError(t, err)
//
//	// 加载并验证快照
//	loadedSnapshot, err := snapshotter.LoadNewestSnapshot()
//	assert.NoError(t, err)
//	assert.NotNil(t, loadedSnapshot)
//	assert.Equal(t, testSnapshot.Metadata.Index, loadedSnapshot.Metadata.Index)
//	assert.Equal(t, testSnapshot.Metadata.Term, loadedSnapshot.Metadata.Term)
//	assert.Equal(t, testSnapshot.Data, loadedSnapshot.Data)
//}

// 测试快照计数器
func TestSnapshotCounter(t *testing.T) {
	snapshotter, _, _, _, cleanup := createTestSnapshotter(t, nil)
	defer cleanup()

	// 测试计数器增加
	for i := 0; i < 50; i++ {
		snapshotter.IncrementLogEntryCount()
	}
	count := atomic.LoadUint64(&snapshotter.logEntryCount)
	assert.Equal(t, uint64(50), count)

	// 测试计数器减少
	for i := 0; i < 20; i++ {
		snapshotter.DecrementLogEntryCount()
	}
	count = atomic.LoadUint64(&snapshotter.logEntryCount)
	assert.Equal(t, uint64(30), count)
}

// 测试自动快照触发
//func TestAutoSnapshot(t *testing.T) {
//	config := &SnapshotConfig{
//		CheckInterval: 500 * time.Millisecond,
//		SnapshotCount: 10,
//	}
//
//	snapshotter, store, _, _, cleanup := createTestSnapshotter(t, config)
//	defer cleanup()
//
//	// 准备测试数据
//	entries := []raftpb.Entry{
//		{Index: 1, Term: 1, Data: []byte("entry1")},
//		{Index: 2, Term: 1, Data: []byte("entry2")},
//	}
//	err := store.SaveEntries(entries)
//	require.NoError(t, err)
//
//	hardState := raftpb.HardState{
//		Term:   1,
//		Vote:   1,
//		Commit: 2,
//	}
//	err = store.SaveRaftState(hardState)
//	require.NoError(t, err)
//
//	// 快速增加日志条目
//	for i := 0; i < 20; i++ {
//		snapshotter.IncrementLogEntryCount()
//	}
//
//	// 等待自动快照触发
//	time.Sleep(1 * time.Second)
//
//	// 验证快照是否已创建
//	snapshot, err := snapshotter.LoadNewestSnapshot()
//	assert.NoError(t, err)
//	if snapshot != nil {
//		assert.Equal(t, uint64(2), snapshot.Metadata.Index)
//		assert.Equal(t, uint64(1), snapshot.Metadata.Term)
//	}
//}

// 测试日志键解析
func TestLogKeyParsing(t *testing.T) {
	// 测试数据
	testIndex := uint64(12345)

	// 生成日志键
	key := logKey(testIndex)

	// 验证键前缀
	assert.True(t, bytes.HasPrefix(key, prefixLogEntry))

	// 解析键
	parsedIndex := parseLogKey(key)
	assert.Equal(t, testIndex, parsedIndex)
}

// 测试快照键处理
func TestSnapshotKeyHandling(t *testing.T) {
	// 测试数据
	testIndex := uint64(67890)

	// 生成快照键
	key := snapshotKey(testIndex)

	// 验证键前缀
	assert.True(t, bytes.HasPrefix(key, keySnapshot))

	// 解析键
	parsedIndex := parseSnapshotKey(key)
	assert.Equal(t, testIndex, parsedIndex)

	// 测试不同索引值
	testCases := []uint64{0, 1, 100, 65535, 4294967295}
	for _, idx := range testCases {
		key := snapshotKey(idx)
		parsed := parseSnapshotKey(key)
		assert.Equal(t, idx, parsed)
	}
}

// 测试键生成的一致性
func TestKeyConsistency(t *testing.T) {
	// 测试数据
	testIndex := uint64(123)

	// 生成键
	logKey1 := logKey(testIndex)
	logKey2 := logKey(testIndex)
	snapKey1 := snapshotKey(testIndex)
	snapKey2 := snapshotKey(testIndex)

	// 验证相同索引生成的键相同
	assert.Equal(t, logKey1, logKey2)
	assert.Equal(t, snapKey1, snapKey2)

	// 验证不同类型的键不同
	assert.NotEqual(t, logKey1, snapKey1)

	// 验证不同索引生成的键不同
	assert.NotEqual(t, logKey(testIndex), logKey(testIndex+1))
	assert.NotEqual(t, snapshotKey(testIndex), snapshotKey(testIndex+1))
}
