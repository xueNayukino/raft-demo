package storage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"qklzl/fsm"

	"github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// 创建测试快照管理器
func createTestSnapshotter(t *testing.T, config *SnapshotConfig) (*Snapshotter, *BadgerStorage, *fsm.KVStateMachine, string, func()) {
	t.Logf("开始创建测试快照管理器...")

	dir, err := os.MkdirTemp("", fmt.Sprintf("snapshotter-test-%s-*", t.Name()))
	require.NoError(t, err)
	t.Logf("创建临时目录: %s", dir)

	err = os.RemoveAll(dir)
	require.NoError(t, err)
	err = os.MkdirAll(dir, 0755)
	require.NoError(t, err)
	t.Logf("重新创建目录结构")

	opts := badger.DefaultOptions("").WithInMemory(true)
	opts.Logger = nil
	opts.SyncWrites = false
	opts.NumVersionsToKeep = 1
	opts.DetectConflicts = false
	opts.NumGoroutines = 1
	t.Logf("配置 BadgerDB 选项")

	db, err := badger.Open(opts)
	require.NoError(t, err)
	t.Logf("成功打开 BadgerDB")

	stateMachine := fsm.NewKVStateMachine(db)
	t.Logf("创建状态机")

	store, err := NewBadgerStorage(dir, nil, stateMachine)
	require.NoError(t, err)
	t.Logf("创建存储实例")

	if config == nil {
		config = &SnapshotConfig{
			CheckInterval: 1 * time.Second,
			SnapshotCount: 100,
		}
		t.Logf("使用默认快照配置: 检查间隔=%v, 快照计数=%d", config.CheckInterval, config.SnapshotCount)
	} else {
		t.Logf("使用自定义快照配置: 检查间隔=%v, 快照计数=%d", config.CheckInterval, config.SnapshotCount)
	}

	snapshotter, err := NewSnapshotter(dir, store.db, config, stateMachine)
	require.NoError(t, err)
	t.Logf("成功创建快照管理器")

	cleanup := func() {
		t.Log("开始清理资源...")
		if snapshotter != nil {
			snapshotter.Stop()
			t.Log("停止快照管理器")
		}
		if store != nil {
			store.Stop()
			t.Log("停止存储服务")
		}
		if db != nil {
			err := db.Close()
			if err != nil {
				t.Logf("关闭数据库时出错: %v", err)
			} else {
				t.Log("成功关闭数据库")
			}
		}
		time.Sleep(100 * time.Millisecond)
		if err := os.RemoveAll(dir); err != nil {
			t.Logf("删除测试目录时出错: %v", err)
		} else {
			t.Log("成功删除测试目录")
		}
		t.Log("资源清理完成")
	}

	t.Cleanup(cleanup)
	return snapshotter, store, stateMachine, dir, cleanup
}

// 测试快照创建和加载
func TestSnapshotCreateAndLoad(t *testing.T) {
	t.Log("[Test] 开始测试快照创建和加载...")
	snapshotter, store, stateMachine, _, cleanup := createTestSnapshotter(t, nil)
	defer cleanup()

	lastIndex, _ := store.LastIndex()
	t.Logf("[Test] 保存快照前，lastIndex=%d", lastIndex)
	// 准备测试数据
	t.Log("准备测试日志条目...")
	entries := []raftpb.Entry{
		{Index: 1, Term: 1, Type: raftpb.EntryNormal, Data: []byte("entry1")},
		{Index: 2, Term: 1, Type: raftpb.EntryNormal, Data: []byte("entry2")},
		{Index: 3, Term: 2, Type: raftpb.EntryNormal, Data: []byte("entry3")},
	}
	err := store.SaveEntries(entries)
	require.NoError(t, err)
	t.Log("成功保存日志条目")

	// 保存硬状态和配置状态
	t.Log("保存硬状态和配置状态...")
	hardState := raftpb.HardState{
		Term:   2,
		Vote:   1,
		Commit: 3,
	}
	confState := raftpb.ConfState{
		Voters: []uint64{1, 2, 3},
	}
	err = store.SaveState(hardState, confState)
	require.NoError(t, err)
	t.Log("成功保存状态")

	// 准备状态机数据
	t.Log("准备状态机数据...")
	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
	}
	for k, v := range testData {
		cmd := fsm.Command{
			Op:    fsm.OpPut,
			Key:   k,
			Value: v,
		}
		data, err := json.Marshal(cmd)
		require.NoError(t, err)
		err = stateMachine.Apply(raftpb.Entry{Data: data})
		require.NoError(t, err)
	}
	t.Log("成功应用状态机数据")

	// 触发快照创建
	t.Log("触发快照创建...")
	for i := 0; i < 150; i++ {
		snapshotter.IncrementLogEntryCount()
	}
	t.Logf("增加日志条目计数到 150")

	// 等待异步快照创建
	t.Log("等待异步快照创建...")
	time.Sleep(2 * time.Second)

	// 验证快照
	t.Log("开始验证快照...（准备调用 LoadNewestSnapshot）")
	snapshot, err := snapshotter.LoadNewestSnapshot()
	t.Log("[Test] LoadNewestSnapshot 返回")
	require.NoError(t, err)
	require.NotNil(t, snapshot)
	t.Logf("[Test] 加载快照后，Index=%d, Term=%d", snapshot.Metadata.Index, snapshot.Metadata.Term)
	firstIndex, _ := store.FirstIndex()
	lastIndex, _ = store.LastIndex()
	t.Logf("[Test] 恢复快照后，FirstIndex=%d, LastIndex=%d", firstIndex, lastIndex)

	// 验证快照元数据
	t.Log("验证快照元数据...")
	assert.Equal(t, uint64(3), snapshot.Metadata.Index)
	assert.Equal(t, uint64(2), snapshot.Metadata.Term)
	assert.Equal(t, confState, snapshot.Metadata.ConfState)
	t.Log("快照元数据验证通过")

	// 验证快照数据
	t.Log("验证快照数据...")
	var snapshotData fsm.SnapshotData
	err = json.Unmarshal(snapshot.Data, &snapshotData)
	require.NoError(t, err)
	assert.Equal(t, testData, snapshotData.Data)
	t.Log("快照数据验证通过")

	// 验证日志压缩
	t.Log("验证日志压缩...")
	compactedEntries, err := store.Entries(1, 3, 0)
	assert.Equal(t, ErrLogCompacted, err)
	assert.Nil(t, compactedEntries)
	t.Log("日志压缩验证通过")

	// 验证新日志条目可以正常追加
	t.Log("测试新日志条目追加...")
	newEntries := []raftpb.Entry{
		{Index: 4, Term: 2, Type: raftpb.EntryNormal, Data: []byte("entry4")},
		{Index: 5, Term: 2, Type: raftpb.EntryNormal, Data: []byte("entry5")},
	}
	err = store.SaveEntries(newEntries)
	require.NoError(t, err)
	t.Log("成功追加新日志条目")

	// 验证可以读取新日志条目
	t.Log("验证新日志条目...")
	retrievedEntries, err := store.Entries(4, 6, 0)
	assert.NoError(t, err)
	assert.Equal(t, newEntries, retrievedEntries)
}

// 测试快照应用
func TestSnapshotApply(t *testing.T) {
	t.Log("[Test] 开始测试快照应用...")
	snapshotter, store, stateMachine, _, cleanup := createTestSnapshotter(t, nil)
	defer cleanup()
	firstIndex, _ := store.FirstIndex()
	lastIndex, _ := store.LastIndex()
	t.Logf("[Test] 应用快照前，FirstIndex=%d, LastIndex=%d", firstIndex, lastIndex)
	// 准备初始状态
	t.Log("准备初始日志条目...")
	initialEntries := []raftpb.Entry{
		{Index: 1, Term: 1, Type: raftpb.EntryNormal, Data: []byte("old1")},
		{Index: 2, Term: 1, Type: raftpb.EntryNormal, Data: []byte("old2")},
		{Index: 3, Term: 1, Type: raftpb.EntryNormal, Data: []byte("old3")},
	}
	err := store.SaveEntries(initialEntries)
	require.NoError(t, err)
	t.Log("成功保存初始日志条目")

	t.Log("保存初始状态...")
	initialState := raftpb.HardState{Term: 1, Vote: 1, Commit: 3}
	initialConf := raftpb.ConfState{Voters: []uint64{1, 2, 3}}
	err = store.SaveState(initialState, initialConf)
	require.NoError(t, err)
	t.Log("成功保存初始状态")

	// 准备状态机数据
	t.Log("准备初始状态机数据...")
	oldData := map[string]string{
		"old_key1": "old_value1",
		"old_key2": "old_value2",
	}
	for k, v := range oldData {
		cmd := fsm.Command{
			Op:    fsm.OpPut,
			Key:   k,
			Value: v,
		}
		data, err := json.Marshal(cmd)
		require.NoError(t, err)
		err = stateMachine.Apply(raftpb.Entry{Data: data})
		require.NoError(t, err)
	}
	t.Log("成功应用初始状态机数据")

	// 准备新的快照数据
	t.Log("准备新的快照数据...")
	newData := map[string]string{
		"key1": "value1",
		"key2": "value2",
	}
	snapshotData := fsm.SnapshotData{
		Data:         newData,
		AppliedIndex: 3,
	}
	snapshotBytes, err := json.Marshal(snapshotData)
	require.NoError(t, err)
	t.Log("成功序列化快照数据")

	// 测试场景1：应用有效的快照
	t.Log("测试场景1：应用有效的快照...")
	snapshot1 := raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index:     3,
			Term:      1,
			ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}},
		},
		Data: snapshotBytes,
	}
	err = snapshotter.ApplySnapshot(snapshot1)
	assert.NoError(t, err)
	t.Log("成功应用第一个快照")

	// 验证状态机数据
	t.Log("验证状态机数据...")
	for k, v := range newData {
		value, err := stateMachine.Get(k)
		assert.NoError(t, err)
		assert.Equal(t, v, value)
		t.Logf("验证键值对: %s = %s", k, v)
	}
	t.Log("状态机数据验证通过")

	// 测试场景2：应用更新的快照
	t.Log("测试场景2：应用更新的快照...")
	snapshotData.AppliedIndex = 6
	snapshotBytes, err = json.Marshal(snapshotData)
	require.NoError(t, err)

	snapshot2 := raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index:     6,
			Term:      2,
			ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3, 4}},
		},
		Data: snapshotBytes,
	}
	err = snapshotter.ApplySnapshot(snapshot2)
	assert.NoError(t, err)
	t.Log("成功应用第二个快照")

	// 验证元数据更新
	t.Log("验证快照元数据...")
	latestSnapshot, err := snapshotter.LoadNewestSnapshot()
	assert.NoError(t, err)
	require.NotNil(t, latestSnapshot, "快照不应该为空")
	assert.Equal(t, snapshot2.Metadata.Index, latestSnapshot.Metadata.Index)
	assert.Equal(t, snapshot2.Metadata.Term, latestSnapshot.Metadata.Term)
	assert.Equal(t, snapshot2.Metadata.ConfState.Voters, latestSnapshot.Metadata.ConfState.Voters)
	t.Log("快照元数据验证通过")

	// 验证状态机数据
	t.Log("再次验证状态机数据...")
	for k, v := range newData {
		value, err := stateMachine.Get(k)
		assert.NoError(t, err)
		assert.Equal(t, v, value)
		t.Logf("验证键值对: %s = %s", k, v)
	}
	t.Log("状态机数据再次验证通过")

	// 测试场景3：应用无效快照
	t.Log("测试场景3：应用无效快照...")
	invalidSnapshots := []struct {
		name     string
		snapshot raftpb.Snapshot
		errMsg   string
	}{
		{
			name: "空数据",
			snapshot: raftpb.Snapshot{
				Metadata: raftpb.SnapshotMetadata{Index: 10, Term: 3},
				Data:     nil,
			},
			errMsg: "快照数据不能为空",
		},
		{
			name: "无效索引",
			snapshot: raftpb.Snapshot{
				Metadata: raftpb.SnapshotMetadata{Index: 0, Term: 3},
				Data:     snapshotBytes,
			},
			errMsg: "无效的快照索引",
		},
		{
			name: "过期快照",
			snapshot: raftpb.Snapshot{
				Metadata: raftpb.SnapshotMetadata{Index: 4, Term: 2},
				Data:     snapshotBytes,
			},
			errMsg: "早于当前快照",
		},
	}

	for _, tc := range invalidSnapshots {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("测试无效快照场景: %s", tc.name)
			err := snapshotter.ApplySnapshot(tc.snapshot)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tc.errMsg)
			t.Logf("成功捕获预期错误: %v", err)
		})
	}

	// 验证日志压缩
	t.Log("验证日志压缩...")
	entries, err := store.Entries(1, 4, 0)
	assert.Equal(t, ErrLogCompacted, err)
	assert.Nil(t, entries)
	t.Log("日志压缩验证通过")

	t.Log("测试完成")
	firstIndex, _ = store.FirstIndex()
	lastIndex, _ = store.LastIndex()
	t.Logf("[Test] 应用快照后，FirstIndex=%d, LastIndex=%d", firstIndex, lastIndex)
}

// 测试快照计数器
func TestSnapshotCounter(t *testing.T) {
	t.Log("开始测试快照计数器...")
	snapshotter, _, _, _, cleanup := createTestSnapshotter(t, nil)
	defer cleanup()

	// 测试计数器增加
	t.Log("测试计数器增加...")
	for i := 0; i < 50; i++ {
		snapshotter.IncrementLogEntryCount()
	}
	count := atomic.LoadUint64(&snapshotter.logEntryCount)
	assert.Equal(t, uint64(50), count)
	t.Logf("计数器成功增加到 %d", count)

	// 测试计数器减少
	t.Log("测试计数器减少...")
	for i := 0; i < 20; i++ {
		snapshotter.DecrementLogEntryCount()
	}
	count = atomic.LoadUint64(&snapshotter.logEntryCount)
	assert.Equal(t, uint64(30), count)
	t.Logf("计数器成功减少到 %d", count)

	// 测试并发操作
	t.Log("测试并发操作...")
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				snapshotter.IncrementLogEntryCount()
				time.Sleep(time.Millisecond)
				snapshotter.DecrementLogEntryCount()
			}
		}()
	}
	wg.Wait()
	t.Log("并发操作完成")

	// 验证最终计数
	finalCount := atomic.LoadUint64(&snapshotter.logEntryCount)
	assert.Equal(t, uint64(30), finalCount)
	t.Logf("最终计数验证通过: %d", finalCount)

	t.Log("测试完成")
}

// 测试自动快照触发
func TestAutoSnapshot(t *testing.T) {
	t.Log("开始测试自动快照触发...")
	config := &SnapshotConfig{
		CheckInterval: 1 * time.Second,
		SnapshotCount: 10,
	}
	t.Logf("使用配置: 检查间隔=%v, 快照计数=%d", config.CheckInterval, config.SnapshotCount)

	snapshotter, store, stateMachine, _, cleanup := createTestSnapshotter(t, config)
	defer cleanup()

	// 准备初始状态
	t.Log("准备初始日志条目...")
	entries := []raftpb.Entry{
		{Index: 1, Term: 1, Type: raftpb.EntryNormal, Data: []byte("entry1")},
		{Index: 2, Term: 1, Type: raftpb.EntryNormal, Data: []byte("entry2")},
	}
	err := store.SaveEntries(entries)
	require.NoError(t, err)
	t.Log("成功保存初始日志条目")

	t.Log("保存初始状态...")
	hardState := raftpb.HardState{Term: 1, Vote: 1, Commit: 2}
	confState := raftpb.ConfState{Voters: []uint64{1, 2, 3}}
	err = store.SaveState(hardState, confState)
	require.NoError(t, err)

	// 准备状态机数据
	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
	}
	for k, v := range testData {
		cmd := fsm.Command{
			Op:    fsm.OpPut,
			Key:   k,
			Value: v,
		}
		data, err := json.Marshal(cmd)
		require.NoError(t, err)
		err = stateMachine.Apply(raftpb.Entry{Data: data})
		require.NoError(t, err)
	}

	// 触发自动快照
	for i := 0; i < 20; i++ {
		snapshotter.IncrementLogEntryCount()
	}

	// 等待快照创建
	time.Sleep(2 * time.Second)

	// 验证快照
	snapshot, err := snapshotter.LoadNewestSnapshot()
	assert.NoError(t, err)
	require.NotNil(t, snapshot)

	// 验证快照元数据
	assert.Equal(t, uint64(2), snapshot.Metadata.Index)
	assert.Equal(t, uint64(1), snapshot.Metadata.Term)
	assert.Equal(t, confState, snapshot.Metadata.ConfState)

	// 验证快照数据
	var snapshotData fsm.SnapshotData
	err = json.Unmarshal(snapshot.Data, &snapshotData)
	require.NoError(t, err)
	assert.Equal(t, testData, snapshotData.Data)

	// 验证日志压缩
	_, err = store.Entries(1, 2, 0)
	assert.Equal(t, ErrLogCompacted, err)
}

// 测试键处理
func TestKeyHandling(t *testing.T) {
	// 测试日志键
	testIndex := uint64(12345)
	key := logKey(testIndex)
	assert.True(t, bytes.HasPrefix(key, prefixLogEntry))
	parsedIndex := parseLogKey(key)
	assert.Equal(t, testIndex, parsedIndex)

	// 测试快照键
	key = snapshotKey(testIndex)
	assert.True(t, bytes.HasPrefix(key, keySnapshot))
	parsedIndex = parseSnapshotKey(key)
	assert.Equal(t, testIndex, parsedIndex)

	// 测试边界值
	testCases := []uint64{0, 1, 100, 65535, 4294967295}
	for _, idx := range testCases {
		t.Run(fmt.Sprintf("Index_%d", idx), func(t *testing.T) {
			// 测试日志键
			logK := logKey(idx)
			parsed := parseLogKey(logK)
			assert.Equal(t, idx, parsed)

			// 测试快照键
			snapK := snapshotKey(idx)
			parsed = parseSnapshotKey(snapK)
			assert.Equal(t, idx, parsed)
		})
	}
}
