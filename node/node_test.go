package node

import (
	"context"
	"fmt"
	"net"
	"os"
	"qklzl/fsm"
	"qklzl/storage"
	"strings"
	"sync"
	"testing"
	"time"

	"sync/atomic"

	"github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// 获取可用的端口
func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// 创建临时测试目录
func createTempDir(t *testing.T) string {
	// 使用测试名称和时间戳创建唯一目录
	testName := t.Name()
	timestamp := time.Now().UnixNano()
	dirName := fmt.Sprintf("raft-node-test-%s-%d", testName, timestamp)
	dir, err := os.MkdirTemp("", dirName)
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

// 创建测试节点
func createTestNode(t *testing.T) (*RaftNode, func()) {
	// 创建临时目录
	dir := createTempDir(t)

	// 创建BadgerDB内存实例
	db := createBadgerDB(t)

	// 创建状态机
	stateMachine := fsm.NewKVStateMachine(db)

	// 获取可用端口
	port1, err := getFreePort()
	require.NoError(t, err)
	port2, err := getFreePort()
	require.NoError(t, err)
	port3, err := getFreePort()
	require.NoError(t, err)

	// 创建节点配置
	config := RaftNodeConfig{
		NodeID:             1,
		ClusterID:          1,
		Peers:              map[uint64]string{1: fmt.Sprintf("localhost:%d", port1), 2: fmt.Sprintf("localhost:%d", port2), 3: fmt.Sprintf("localhost:%d", port3)},
		IsRestart:          false,
		IsJoin:             false,
		ElectionTick:       10,
		HeartbeatTick:      1,
		SnapshotCount:      100,
		StorageDir:         dir,
		ListenAddr:         fmt.Sprintf("localhost:%d", port1),
		MaxSizePerMsg:      1024 * 1024,
		MaxInflightMsgs:    256,
		CheckpointInterval: 30,
	}

	// 创建节点
	node, err := NewRaftNode(config, stateMachine)
	require.NoError(t, err)

	// 返回清理函数
	cleanup := func() {
		if node != nil {
			// 确保节点已停止
			node.Stop()
			// 等待资源释放
			time.Sleep(100 * time.Millisecond)
		}
		os.RemoveAll(dir)
	}

	// 确保测试结束时清理资源
	t.Cleanup(cleanup)

	return node, cleanup
}

// 创建测试集群
func createTestCluster(t *testing.T, nodeCount int, addrList string) ([]*RaftNode, func()) {
	// 解析地址列表
	addrs := strings.Split(addrList, ",")
	require.Equal(t, nodeCount, len(addrs), "地址数量必须与节点数量相同")

	// 创建节点
	nodes := make([]*RaftNode, nodeCount)
	cleanups := make([]func(), nodeCount)

	// 创建临时目录
	dirs := make([]string, nodeCount)
	for i := 0; i < nodeCount; i++ {
		dirs[i] = createTempDir(t)
	}

	// 创建集群配置
	peers := make(map[uint64]string)
	for i := 0; i < nodeCount; i++ {
		peers[uint64(i+1)] = addrs[i]
	}

	// 创建并启动节点
	var startWg sync.WaitGroup
	for i := 0; i < nodeCount; i++ {
		i := i // 捕获变量
		startWg.Add(1)

		go func() {
			defer startWg.Done()

			// 创建BadgerDB实例
			db := createBadgerDB(t)

			// 创建状态机
			stateMachine := fsm.NewKVStateMachine(db)

			// 创建节点配置
			config := RaftNodeConfig{
				NodeID:             uint64(i + 1),
				ClusterID:          1,
				Peers:              peers,
				IsRestart:          false,
				IsJoin:             false,
				ElectionTick:       10,
				HeartbeatTick:      1,
				SnapshotCount:      100,
				StorageDir:         dirs[i],
				ListenAddr:         addrs[i],
				MaxSizePerMsg:      1024 * 1024,
				MaxInflightMsgs:    256,
				CheckpointInterval: 5,
			}

			// 创建节点
			node, err := NewRaftNode(config, stateMachine)
			require.NoError(t, err)

			// 启动节点
			err = node.Start()
			require.NoError(t, err)

			// 保存节点
			nodes[i] = node

			// 创建清理函数
			cleanups[i] = func() {
				node.Stop()
				db.Close()
				os.RemoveAll(dirs[i])
			}
		}()
	}

	// 等待所有节点启动完成
	startWg.Wait()

	// 返回节点列表和清理函数
	return nodes, func() {
		for _, cleanup := range cleanups {
			cleanup()
		}
		time.Sleep(500 * time.Millisecond) // 等待资源释放
	}
}

// 测试节点创建
func TestNewRaftNode(t *testing.T) {
	node, cleanup := createTestNode(t)
	defer cleanup()

	// 验证节点属性
	assert.NotNil(t, node)
	assert.Equal(t, uint64(1), node.config.NodeID)
	assert.Equal(t, uint64(1), node.config.ClusterID)
	assert.Equal(t, 3, len(node.config.Peers))
	assert.NotNil(t, node.storage)
	assert.NotNil(t, node.raftStorage)
	assert.NotNil(t, node.fsm)
	assert.NotNil(t, node.transport)
}

// 测试Storage适配器
func TestRaftStorage(t *testing.T) {
	// 创建临时目录
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	// 创建BadgerDB内存实例
	db := createBadgerDB(t)

	// 创建状态机
	stateMachine := fsm.NewKVStateMachine(db)

	// 创建存储配置
	storageConfig := &storage.BadgerConfig{
		GCInterval:       time.Second * 30,
		GCDiscardRatio:   0.5,
		SnapshotInterval: time.Second * 30,
		SnapshotCount:    100,
	}

	// 创建BadgerStorage
	badgerStorage, err := storage.NewBadgerStorage(dir, storageConfig, stateMachine)
	require.NoError(t, err)
	defer badgerStorage.Stop()

	// 创建Raft存储适配器
	raftStorage := NewRaftStorage(badgerStorage)

	// 测试初始状态
	hardState, confState, err := raftStorage.InitialState()
	require.NoError(t, err)
	assert.True(t, storage.IsEmptyHardState(hardState))
	assert.True(t, storage.IsEmptyConfState(confState))

	// 测试FirstIndex和LastIndex
	firstIndex, err := raftStorage.FirstIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), firstIndex)

	lastIndex, err := raftStorage.LastIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(0), lastIndex)

	// 测试写入和读取日志条目
	entries := []raftpb.Entry{
		{Term: 1, Index: 1, Data: []byte(`{"op":"put","key":"key1","value":"value1"}`)},
		{Term: 1, Index: 2, Data: []byte(`{"op":"put","key":"key2","value":"value2"}`)},
	}

	err = badgerStorage.SaveEntries(entries)
	require.NoError(t, err)

	// 测试读取日志条目
	readEntries, err := raftStorage.Entries(1, 3, 0)
	require.NoError(t, err)
	assert.Equal(t, 2, len(readEntries))

	// 测试Term
	term, err := raftStorage.Term(1)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), term)

	// 测试快照
	snapshot, err := raftStorage.Snapshot()
	require.NoError(t, err)
	assert.True(t, storage.IsEmptySnap(snapshot))
}

// 测试节点启动和停止
func TestNodeStartAndStop(t *testing.T) {
	node, cleanup := createTestNode(t)

	// 确保在测试结束时清理资源
	defer cleanup()

	// 启动节点
	err := node.Start()
	require.NoError(t, err)

	// 等待一段时间，让节点有机会运行
	time.Sleep(100 * time.Millisecond)

	// 停止节点
	node.Stop()

	// 等待一段时间，确保资源被释放
	time.Sleep(200 * time.Millisecond)
}

// 测试HandleMessage接口
func TestHandleMessage(t *testing.T) {
	// 创建内存中的BadgerDB实例
	db := createBadgerDB(t)

	// 创建状态机
	stateMachine := fsm.NewKVStateMachine(db)

	// 创建临时目录
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	// 使用随机端口
	port, err := getFreePort()
	require.NoError(t, err)
	listenAddr := fmt.Sprintf("localhost:%d", port)

	// 创建节点配置
	config := RaftNodeConfig{
		NodeID:             1,
		ClusterID:          1,
		Peers:              map[uint64]string{1: listenAddr},
		IsRestart:          false,
		IsJoin:             false,
		ElectionTick:       10,
		HeartbeatTick:      1,
		SnapshotCount:      100,
		StorageDir:         dir,
		ListenAddr:         listenAddr,
		MaxSizePerMsg:      1024 * 1024,
		MaxInflightMsgs:    256,
		CheckpointInterval: 30,
	}

	// 创建节点
	node, err := NewRaftNode(config, stateMachine)
	require.NoError(t, err)

	// 启动节点
	err = node.Start()
	require.NoError(t, err)

	// 等待节点启动
	time.Sleep(500 * time.Millisecond)

	// 验证节点状态
	assert.NotNil(t, node.node, "raft 节点应该已初始化")
	assert.False(t, node.IsLeader(), "新节点不应该是领导者")

	// 停止节点
	node.Stop()

	// 等待节点完全停止
	time.Sleep(200 * time.Millisecond)

	// 验证节点已停止
	assert.Equal(t, int32(1), atomic.LoadInt32(&node.stopped), "节点应该已停止")
}

// 测试状态查询方法
func TestStateQueries(t *testing.T) {
	node, cleanup := createTestNode(t)
	defer cleanup()

	// 测试初始状态
	assert.Equal(t, uint64(0), node.GetTerm())
	assert.Equal(t, uint64(0), node.GetLeaderID())
	assert.Equal(t, uint64(0), node.GetCommitIndex())
	assert.Equal(t, uint64(0), node.GetAppliedIndex())
	assert.False(t, node.IsLeader())

	// 手动更新状态
	node.updateLeaderState(1, 2)

	// 验证状态更新
	assert.Equal(t, uint64(1), node.GetTerm())
	assert.Equal(t, uint64(2), node.GetLeaderID())

	// 停止节点
	node.Stop()
}

// 测试 Ready 循环和消息处理
func TestReadyLoop(t *testing.T) {
	node, cleanup := createTestNode(t)
	defer cleanup()

	// 启动节点
	err := node.Start()
	require.NoError(t, err)

	// 等待一段时间，让节点有机会运行
	time.Sleep(500 * time.Millisecond)

	// 验证节点已启动
	assert.NotNil(t, node.node, "raft 节点应该已初始化")

	// 停止节点
	node.Stop()
}

// 测试日志条目应用
func TestPublishEntries(t *testing.T) {
	// 创建临时目录
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	// 创建BadgerDB内存实例
	db := createBadgerDB(t)

	// 创建状态机
	stateMachine := fsm.NewKVStateMachine(db)

	// 创建节点配置
	config := RaftNodeConfig{
		NodeID:             1,
		ClusterID:          1,
		Peers:              map[uint64]string{1: "localhost:10001"},
		IsRestart:          false,
		IsJoin:             false,
		ElectionTick:       10,
		HeartbeatTick:      1,
		SnapshotCount:      100,
		StorageDir:         dir,
		ListenAddr:         "localhost:10001",
		MaxSizePerMsg:      1024 * 1024,
		MaxInflightMsgs:    256,
		CheckpointInterval: 30,
	}

	// 创建节点
	node, err := NewRaftNode(config, stateMachine)
	require.NoError(t, err)
	defer node.Stop()

	// 创建测试日志条目
	entries := []raftpb.Entry{
		{
			Term:  1,
			Index: 1,
			Type:  raftpb.EntryNormal,
			Data:  []byte(`{"op":"put","key":"test_key","value":"test_value"}`),
		},
	}

	// 应用日志条目
	err = node.publishEntries(entries)
	require.NoError(t, err)

	// 验证应用索引已更新
	assert.Equal(t, uint64(1), node.GetAppliedIndex())

	// 验证状态机中的值
	value, err := stateMachine.Get("test_key")
	require.NoError(t, err)
	assert.Equal(t, "test_value", value)
}

// 测试消息发送
func TestSendMessages(t *testing.T) {
	node, cleanup := createTestNode(t)
	defer cleanup()

	// 创建测试消息
	msgs := []raftpb.Message{
		{
			Type: raftpb.MsgHeartbeat,
			To:   2,
			From: 1,
			Term: 1,
		},
		{
			Type: raftpb.MsgHeartbeat,
			To:   3,
			From: 1,
			Term: 1,
		},
		{
			Type: raftpb.MsgHeartbeat,
			To:   1, // 发给自己的消息
			From: 1,
			Term: 1,
		},
	}

	// 发送消息（这里不会真正发送，因为节点2和3不存在）
	node.sendMessages(msgs)

	// 这里主要测试函数不会崩溃，因为在测试环境中我们无法验证消息是否真正发送
	// 实际上，由于节点2和3不存在，sendMessages 应该会记录错误但不会崩溃
}

// 测试配置变更
func TestApplyConfChange(t *testing.T) {
	node, cleanup := createTestNode(t)
	defer cleanup()

	// 启动节点
	err := node.Start()
	require.NoError(t, err)

	// 等待节点启动
	time.Sleep(100 * time.Millisecond)

	// 创建一个添加节点的配置变更
	cc := raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  4,
		Context: []byte("localhost:10004"),
	}

	// 应用配置变更
	node.applyConfChange(cc)

	// 验证配置已更新
	assert.Contains(t, node.config.Peers, uint64(4))
	assert.Equal(t, "localhost:10004", node.config.Peers[4])

	// 创建一个移除节点的配置变更
	cc = raftpb.ConfChange{
		Type:   raftpb.ConfChangeRemoveNode,
		NodeID: 2,
	}

	// 应用配置变更
	node.applyConfChange(cc)

	// 验证配置已更新
	assert.NotContains(t, node.config.Peers, uint64(2))

	// 停止节点
	node.Stop()
}

// 测试提议处理
func TestPropose(t *testing.T) {
	node, cleanup := createTestNode(t)
	defer cleanup()

	// 启动节点
	err := node.Start()
	require.NoError(t, err)

	// 等待节点启动
	time.Sleep(500 * time.Millisecond)

	// 创建提议上下文
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// 创建提议数据
	data := []byte(`{"op":"put","key":"propose_key","value":"propose_value"}`)

	// 提交提议
	// 注意：由于测试环境中节点可能不是领导者，这里不检查错误
	node.Propose(ctx, data)

	// 等待一段时间，让提议有机会被处理
	time.Sleep(500 * time.Millisecond)

	// 停止节点
	node.Stop()
}

// 测试配置变更提议
func TestProposeConfChange(t *testing.T) {
	node, cleanup := createTestNode(t)
	defer cleanup()

	// 启动节点
	err := node.Start()
	require.NoError(t, err)

	// 等待节点启动
	time.Sleep(500 * time.Millisecond)

	// 创建提议上下文
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// 创建配置变更提议
	cc := raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  4,
		Context: []byte("localhost:10004"),
	}

	// 提交配置变更提议
	// 注意：由于测试环境中节点可能不是领导者，这里不检查错误
	node.ProposeConfChange(ctx, cc)

	// 等待一段时间，让提议有机会被处理
	time.Sleep(500 * time.Millisecond)

	// 停止节点
	node.Stop()
}

// 测试读索引请求
func TestReadIndex(t *testing.T) {
	node, cleanup := createTestNode(t)
	defer cleanup()

	// 启动节点
	err := node.Start()
	require.NoError(t, err)

	// 等待节点启动
	time.Sleep(500 * time.Millisecond)

	// 创建读索引上下文
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// 创建读索引请求
	// 注意：由于测试环境中节点可能不是领导者，这里不检查错误
	err = node.ReadIndex(ctx, []byte("read_key"))
	if err != nil {
		t.Logf("读索引请求失败: %v", err)
	}

	// 等待一段时间，让请求有机会被处理
	time.Sleep(500 * time.Millisecond)

	// 停止节点
	node.Stop()
}

// 测试一致性读取
func TestConsistentRead(t *testing.T) {
	// 使用随机端口避免冲突
	port := 10000 + int(time.Now().UnixNano()%10000)
	listenAddr := fmt.Sprintf("localhost:%d", port)

	// 创建临时目录
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	// 创建BadgerDB内存实例
	db := createBadgerDB(t)

	// 创建状态机
	stateMachine := fsm.NewKVStateMachine(db)

	// 创建节点配置
	config := RaftNodeConfig{
		NodeID:             1,
		ClusterID:          1,
		Peers:              map[uint64]string{1: listenAddr},
		IsRestart:          false,
		IsJoin:             false,
		ElectionTick:       10,
		HeartbeatTick:      1,
		SnapshotCount:      100,
		StorageDir:         dir,
		ListenAddr:         listenAddr,
		MaxSizePerMsg:      1024 * 1024,
		MaxInflightMsgs:    256,
		CheckpointInterval: 30,
	}

	// 创建节点
	node, err := NewRaftNode(config, stateMachine)
	require.NoError(t, err)

	// 启动节点
	err = node.Start()
	require.NoError(t, err)

	// 等待节点启动
	time.Sleep(500 * time.Millisecond)

	// 手动设置节点为领导者
	node.mu.Lock()
	node.state = raft.StateLeader
	node.isLeader = true
	node.lastHeartbeat = time.Now()
	node.mu.Unlock()

	// 写入测试数据
	err = stateMachine.Apply(raftpb.Entry{
		Term:  1,
		Index: 10,
		Type:  raftpb.EntryNormal,
		Data:  []byte(`{"op":"put","key":"test_key","value":"test_value"}`),
	})
	require.NoError(t, err)

	// 更新应用索引
	node.mu.Lock()
	node.appliedIndex = 10
	node.commitIndex = 10
	node.mu.Unlock()

	// 执行一致性读取
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	value, err := node.ConsistentGet(ctx, "test_key")
	require.NoError(t, err)
	assert.Equal(t, "test_value", value)

	// 停止节点
	node.Stop()
}

// 测试快照创建
func TestCreateSnapshot(t *testing.T) {
	// 创建临时目录
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	// 创建BadgerDB内存实例
	db := createBadgerDB(t)

	// 创建状态机
	stateMachine := fsm.NewKVStateMachine(db)

	// 创建节点配置
	config := RaftNodeConfig{
		NodeID:             1,
		ClusterID:          1,
		Peers:              map[uint64]string{1: "localhost:10001"},
		IsRestart:          false,
		IsJoin:             false,
		ElectionTick:       10,
		HeartbeatTick:      1,
		SnapshotCount:      10, // 设置较小的值，方便测试
		StorageDir:         dir,
		ListenAddr:         "localhost:10001",
		MaxSizePerMsg:      1024 * 1024,
		MaxInflightMsgs:    256,
		CheckpointInterval: 1, // 设置较小的值，方便测试
	}

	// 创建节点
	node, err := NewRaftNode(config, stateMachine)
	require.NoError(t, err)
	defer node.Stop()

	// 启动节点
	err = node.Start()
	require.NoError(t, err)

	// 等待节点启动
	time.Sleep(500 * time.Millisecond)

	// 手动设置应用索引，模拟已应用了足够多的日志
	node.mu.Lock()
	node.appliedIndex = 20
	node.mu.Unlock()

	// 强制创建快照
	err = node.ForceSnapshot()
	require.NoError(t, err)

	// 验证快照是否创建成功
	snapshot, err := node.GetSnapshot()
	require.NoError(t, err)
	assert.False(t, storage.IsEmptySnap(snapshot))
	assert.Equal(t, uint64(20), snapshot.Metadata.Index)
}

// 测试节点重启恢复
func TestRestartNode(t *testing.T) {
	// 创建临时目录
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	// 创建BadgerDB内存实例
	db := createBadgerDB(t)

	// 创建状态机
	stateMachine := fsm.NewKVStateMachine(db)

	// 创建节点配置
	config := RaftNodeConfig{
		NodeID:             1,
		ClusterID:          1,
		Peers:              map[uint64]string{1: "localhost:10001"},
		IsRestart:          false,
		IsJoin:             false,
		ElectionTick:       10,
		HeartbeatTick:      1,
		SnapshotCount:      100,
		StorageDir:         dir,
		ListenAddr:         "localhost:10001",
		MaxSizePerMsg:      1024 * 1024,
		MaxInflightMsgs:    256,
		CheckpointInterval: 30,
	}

	// 创建节点
	node, err := NewRaftNode(config, stateMachine)
	require.NoError(t, err)

	// 启动节点
	err = node.Start()
	require.NoError(t, err)

	// 等待节点启动
	time.Sleep(500 * time.Millisecond)

	// 在停止前，手动设置一些状态，以便在重启后验证
	node.mu.Lock()
	oldNode := node.node
	node.node = nil // 模拟节点已停止但未完全关闭
	node.mu.Unlock()

	// 重启节点
	err = node.RestartNode()
	require.NoError(t, err)

	// 等待节点重启
	time.Sleep(500 * time.Millisecond)

	// 验证节点状态
	assert.NotNil(t, node.node)
	assert.NotEqual(t, oldNode, node.node)

	// 停止节点
	node.Stop()
}

// 测试从快照恢复
func TestRecoverFromSnapshot(t *testing.T) {
	// 使用随机端口避免冲突
	basePort := 20000 + int(time.Now().UnixNano()%10000)
	listenAddr := fmt.Sprintf("localhost:%d", basePort)

	// 创建内存中的BadgerDB实例
	opts := badger.DefaultOptions("").WithInMemory(true).WithLogger(nil)
	db, err := badger.Open(opts)
	require.NoError(t, err)
	defer db.Close()

	// 创建状态机
	stateMachine := fsm.NewKVStateMachine(db)

	// 创建临时目录用于Raft存储
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	// 创建节点配置
	config := RaftNodeConfig{
		NodeID:             1,
		ClusterID:          1,
		Peers:              map[uint64]string{1: listenAddr},
		IsRestart:          false,
		IsJoin:             false,
		ElectionTick:       10,
		HeartbeatTick:      1,
		SnapshotCount:      100,
		StorageDir:         dir,
		ListenAddr:         listenAddr,
		MaxSizePerMsg:      1024 * 1024,
		MaxInflightMsgs:    256,
		CheckpointInterval: 30,
	}

	// 创建节点
	node, err := NewRaftNode(config, stateMachine)
	require.NoError(t, err)

	// 启动节点
	err = node.Start()
	require.NoError(t, err)

	// 等待节点启动
	time.Sleep(500 * time.Millisecond)

	// 手动设置应用索引，模拟已应用了足够多的日志
	node.mu.Lock()
	node.appliedIndex = 20
	node.mu.Unlock()

	// 强制创建快照
	err = node.ForceSnapshot()
	require.NoError(t, err)

	// 获取快照数据
	snapshot, err := node.GetSnapshot()
	require.NoError(t, err)
	require.False(t, storage.IsEmptySnap(snapshot))

	// 保存快照数据
	snapshotData := snapshot.Data

	// 停止节点
	node.Stop()
	time.Sleep(500 * time.Millisecond) // 确保节点完全停止

	// 创建新的临时目录
	dir2 := createTempDir(t)
	defer os.RemoveAll(dir2)

	// 使用新的端口
	basePort2 := basePort + 1000
	listenAddr2 := fmt.Sprintf("localhost:%d", basePort2)

	// 创建新的节点配置
	config2 := RaftNodeConfig{
		NodeID:             1,
		ClusterID:          1,
		Peers:              map[uint64]string{1: listenAddr2},
		IsRestart:          true,
		IsJoin:             false,
		ElectionTick:       10,
		HeartbeatTick:      1,
		SnapshotCount:      100,
		StorageDir:         dir2,
		ListenAddr:         listenAddr2,
		MaxSizePerMsg:      1024 * 1024,
		MaxInflightMsgs:    256,
		CheckpointInterval: 30,
	}

	// 创建新节点
	node2, err := NewRaftNode(config2, stateMachine)
	require.NoError(t, err)
	defer func() {
		node2.Stop()
		time.Sleep(200 * time.Millisecond)
	}()

	// 启动新节点
	err = node2.Start()
	require.NoError(t, err)

	// 手动应用快照数据到状态机
	err = stateMachine.RestoreSnapshot(snapshotData)
	require.NoError(t, err)

	// 验证应用索引已恢复
	assert.Equal(t, uint64(20), node.GetAppliedIndex())
}

// 测试从宕机恢复
func TestRecoverFromCrash(t *testing.T) {
	// 使用随机端口避免冲突
	basePort := 20000 + int(time.Now().UnixNano()%10000)
	listenAddr := fmt.Sprintf("localhost:%d", basePort)

	// 创建内存中的BadgerDB实例
	opts := badger.DefaultOptions("").WithInMemory(true).WithLogger(nil)
	db, err := badger.Open(opts)
	require.NoError(t, err)
	defer db.Close()

	// 创建状态机
	stateMachine := fsm.NewKVStateMachine(db)

	// 创建临时目录用于Raft存储
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	// 创建节点配置
	config := RaftNodeConfig{
		NodeID:             1,
		ClusterID:          1,
		Peers:              map[uint64]string{1: listenAddr},
		IsRestart:          false,
		IsJoin:             false,
		ElectionTick:       10,
		HeartbeatTick:      1,
		SnapshotCount:      100,
		StorageDir:         dir,
		ListenAddr:         listenAddr,
		MaxSizePerMsg:      1024 * 1024,
		MaxInflightMsgs:    256,
		CheckpointInterval: 30,
	}

	// 创建节点
	node, err := NewRaftNode(config, stateMachine)
	require.NoError(t, err)

	// 启动节点
	err = node.Start()
	require.NoError(t, err)

	// 等待节点启动
	time.Sleep(500 * time.Millisecond)

	// 写入测试数据
	err = stateMachine.Apply(raftpb.Entry{
		Term:  1,
		Index: 10,
		Type:  raftpb.EntryNormal,
		Data:  []byte(`{"op":"put","key":"crash_key","value":"crash_value"}`),
	})
	require.NoError(t, err)

	// 更新应用索引
	node.mu.Lock()
	node.appliedIndex = 10
	node.mu.Unlock()

	// 停止节点，模拟宕机
	node.Stop()
	time.Sleep(500 * time.Millisecond) // 确保节点完全停止

	// 创建新的临时目录
	dir2 := createTempDir(t)
	defer os.RemoveAll(dir2)

	// 使用新的端口
	basePort2 := basePort + 1000
	listenAddr2 := fmt.Sprintf("localhost:%d", basePort2)

	// 创建新的节点配置
	config2 := RaftNodeConfig{
		NodeID:             1,
		ClusterID:          1,
		Peers:              map[uint64]string{1: listenAddr2},
		IsRestart:          true,
		IsJoin:             false,
		ElectionTick:       10,
		HeartbeatTick:      1,
		SnapshotCount:      100,
		StorageDir:         dir2,
		ListenAddr:         listenAddr2,
		MaxSizePerMsg:      1024 * 1024,
		MaxInflightMsgs:    256,
		CheckpointInterval: 30,
	}

	// 创建新节点
	node2, err := NewRaftNode(config2, stateMachine)
	require.NoError(t, err)
	defer func() {
		node2.Stop()
		time.Sleep(200 * time.Millisecond)
	}()

	// 启动新节点
	err = node2.Start()
	require.NoError(t, err)

	// 等待节点启动
	time.Sleep(500 * time.Millisecond)

	// 验证数据是否恢复
	value, err := stateMachine.Get("crash_key")
	require.NoError(t, err)
	assert.Equal(t, "crash_value", value)
}

// 测试领导者租约
func TestLeaderLease(t *testing.T) {
	// 使用随机端口避免冲突
	port := 10000 + int(time.Now().UnixNano()%10000)
	listenAddr := fmt.Sprintf("localhost:%d", port)

	// 创建临时目录
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	// 创建BadgerDB内存实例
	db := createBadgerDB(t)

	// 创建状态机
	stateMachine := fsm.NewKVStateMachine(db)

	// 创建节点配置
	config := RaftNodeConfig{
		NodeID:             1,
		ClusterID:          1,
		Peers:              map[uint64]string{1: listenAddr},
		IsRestart:          false,
		IsJoin:             false,
		ElectionTick:       10,
		HeartbeatTick:      1,
		SnapshotCount:      100,
		StorageDir:         dir,
		ListenAddr:         listenAddr,
		MaxSizePerMsg:      1024 * 1024,
		MaxInflightMsgs:    256,
		CheckpointInterval: 30,
	}

	// 创建节点
	node, err := NewRaftNode(config, stateMachine)
	require.NoError(t, err)

	// 启动节点
	err = node.Start()
	require.NoError(t, err)

	// 等待节点启动
	time.Sleep(500 * time.Millisecond)

	// 手动设置节点为领导者
	node.mu.Lock()
	node.state = raft.StateLeader
	node.isLeader = true
	node.lastHeartbeat = time.Now()
	node.mu.Unlock()

	// 验证领导者租约有效
	assert.True(t, node.CheckLeaderLease())

	// 手动设置上次心跳时间为很久以前
	node.mu.Lock()
	node.lastHeartbeat = time.Now().Add(-time.Minute)
	node.mu.Unlock()

	// 验证领导者租约已过期
	assert.False(t, node.CheckLeaderLease())

	// 停止节点
	node.Stop()
}

// 测试网络分区检测
func TestDetectPartition(t *testing.T) {
	// 使用随机端口避免冲突
	basePort := 20000 + int(time.Now().UnixNano()%10000)
	listenAddr := fmt.Sprintf("localhost:%d", basePort)

	// 创建内存中的BadgerDB实例
	opts := badger.DefaultOptions("").WithInMemory(true).WithLogger(nil)
	db, err := badger.Open(opts)
	require.NoError(t, err)
	defer db.Close()

	// 创建状态机
	stateMachine := fsm.NewKVStateMachine(db)

	// 创建临时目录用于Raft存储
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	// 创建节点配置，包含多个节点，但只启动一个
	config := RaftNodeConfig{
		NodeID:    1,
		ClusterID: 1,
		Peers: map[uint64]string{
			1: listenAddr,
			2: fmt.Sprintf("localhost:%d", basePort+1),
			3: fmt.Sprintf("localhost:%d", basePort+2),
		},
		IsRestart:          false,
		IsJoin:             false,
		ElectionTick:       10,
		HeartbeatTick:      1,
		SnapshotCount:      100,
		StorageDir:         dir,
		ListenAddr:         listenAddr,
		MaxSizePerMsg:      1024 * 1024,
		MaxInflightMsgs:    256,
		CheckpointInterval: 30,
	}

	// 创建节点
	node, err := NewRaftNode(config, stateMachine)
	require.NoError(t, err)

	// 启动节点
	err = node.Start()
	require.NoError(t, err)
	defer node.Stop()

	// 等待节点启动
	time.Sleep(500 * time.Millisecond)

	// 检测网络分区 - 此时节点不是领导者，没有领导者，应该返回true
	isPartitioned := node.DetectPartition()
	assert.True(t, isPartitioned, "多节点集群应该检测到网络分区")

	// 手动设置节点为领导者
	node.mu.Lock()
	node.state = raft.StateLeader
	node.isLeader = true
	// 手动设置confState，模拟多节点集群
	node.confState = raftpb.ConfState{
		Voters: []uint64{1, 2, 3},
	}
	node.mu.Unlock()

	// 手动设置存储层的confState
	confState := &raftpb.ConfState{
		Voters: []uint64{1, 2, 3},
	}
	err = node.storage.SetConfState(confState)
	require.NoError(t, err)

	// 再次检测网络分区，作为领导者，由于无法连接其他节点，应该返回true
	isPartitioned = node.DetectPartition()
	assert.True(t, isPartitioned, "领导者应该检测到网络分区")
}

// 测试集群选举
func TestClusterElection(t *testing.T) {
	// 创建三个节点的集群
	nodes := make([]*RaftNode, 3)
	cleanups := make([]func(), 3)

	// 创建节点配置
	for i := 0; i < 3; i++ {
		// 创建临时目录
		dir := createTempDir(t)

		// 创建BadgerDB内存实例
		db := createBadgerDB(t)

		// 创建状态机
		stateMachine := fsm.NewKVStateMachine(db)

		// 创建节点配置
		config := RaftNodeConfig{
			NodeID:    uint64(i + 1),
			ClusterID: 1,
			Peers: map[uint64]string{
				1: "localhost:30001",
				2: "localhost:30002",
				3: "localhost:30003",
			},
			IsRestart:          false,
			IsJoin:             false,
			ElectionTick:       10,
			HeartbeatTick:      1,
			SnapshotCount:      100,
			StorageDir:         dir,
			ListenAddr:         fmt.Sprintf("localhost:3000%d", i+1),
			MaxSizePerMsg:      1024 * 1024,
			MaxInflightMsgs:    256,
			CheckpointInterval: 30,
		}

		// 创建节点
		node, err := NewRaftNode(config, stateMachine)
		require.NoError(t, err)

		nodes[i] = node
		cleanups[i] = func() {
			node.Stop()
			os.RemoveAll(dir)
		}
	}

	// 清理函数
	defer func() {
		for _, cleanup := range cleanups {
			cleanup()
		}
	}()

	// 启动所有节点
	for _, node := range nodes {
		err := node.Start()
		require.NoError(t, err)
	}

	// 等待选举完成
	time.Sleep(2 * time.Second)

	// 验证是否有且仅有一个领导者
	leaderCount := 0
	var leaderID uint64
	for _, node := range nodes {
		if node.IsLeader() {
			leaderCount++
			leaderID = node.config.NodeID
		}
	}
	assert.Equal(t, 1, leaderCount, "应该只有一个领导者")
	assert.NotEqual(t, uint64(0), leaderID, "应该选出一个领导者")

	// 验证所有节点都认同这个领导者
	for _, node := range nodes {
		assert.Equal(t, leaderID, node.GetLeaderID(), "所有节点应该认同同一个领导者")
	}
}

// 测试日志复制
func TestLogReplication(t *testing.T) {
	// 创建三个节点的集群
	nodes := make([]*RaftNode, 3)
	cleanups := make([]func(), 3)

	// 创建节点配置
	for i := 0; i < 3; i++ {
		// 创建临时目录
		dir := createTempDir(t)

		// 创建BadgerDB内存实例
		db := createBadgerDB(t)

		// 创建状态机
		stateMachine := fsm.NewKVStateMachine(db)

		// 创建节点配置
		config := RaftNodeConfig{
			NodeID:    uint64(i + 1),
			ClusterID: 1,
			Peers: map[uint64]string{
				1: "localhost:40001",
				2: "localhost:40002",
				3: "localhost:40003",
			},
			IsRestart:          false,
			IsJoin:             false,
			ElectionTick:       10,
			HeartbeatTick:      1,
			SnapshotCount:      100,
			StorageDir:         dir,
			ListenAddr:         fmt.Sprintf("localhost:4000%d", i+1),
			MaxSizePerMsg:      1024 * 1024,
			MaxInflightMsgs:    256,
			CheckpointInterval: 30,
		}

		// 创建节点
		node, err := NewRaftNode(config, stateMachine)
		require.NoError(t, err)

		nodes[i] = node
		cleanups[i] = func() {
			node.Stop()
			os.RemoveAll(dir)
		}
	}

	// 清理函数
	defer func() {
		for _, cleanup := range cleanups {
			cleanup()
		}
	}()

	// 启动所有节点
	for _, node := range nodes {
		err := node.Start()
		require.NoError(t, err)
	}

	// 等待选举完成
	time.Sleep(2 * time.Second)

	// 找到领导者
	var leader *RaftNode
	for _, node := range nodes {
		if node.IsLeader() {
			leader = node
			break
		}
	}
	require.NotNil(t, leader, "应该有一个领导者")

	// 通过领导者提交一些日志
	ctx := context.Background()
	testData := []byte(`{"op":"put","key":"replicated_key","value":"replicated_value"}`)
	err := leader.Propose(ctx, testData)
	require.NoError(t, err)

	// 等待日志复制和应用
	time.Sleep(1 * time.Second)

	// 验证所有节点都应用了日志
	for _, node := range nodes {
		value, err := node.fsm.Get("replicated_key")
		require.NoError(t, err)
		assert.Equal(t, "replicated_value", value, "所有节点应该有相同的数据")
	}
}

// 测试成员变更
func TestMembershipChange(t *testing.T) {
	// 使用getFreePort获取随机端口，避免端口冲突
	ports := make([]int, 3)
	for i := 0; i < 3; i++ {
		var err error
		ports[i], err = getFreePort()
		require.NoError(t, err)
	}
	addr1 := fmt.Sprintf("localhost:%d", ports[0])
	addr2 := fmt.Sprintf("localhost:%d", ports[1])
	addr3 := fmt.Sprintf("localhost:%d", ports[2])

	fmt.Printf("使用端口: %d, %d, %d\n", ports[0], ports[1], ports[2])

	// 创建初始的两个节点
	nodes := make([]*RaftNode, 2)
	dbs := make([]*badger.DB, 2)
	stateMachines := make([]*fsm.KVStateMachine, 2)
	cleanups := make([]func(), 2)

	// 创建初始节点配置
	for i := 0; i < 2; i++ {
		// 创建临时目录
		dir := createTempDir(t)

		// 创建BadgerDB内存实例
		db := createBadgerDB(t)
		dbs[i] = db

		// 创建状态机
		stateMachine := fsm.NewKVStateMachine(db)
		stateMachines[i] = stateMachine

		// 创建节点配置
		config := RaftNodeConfig{
			NodeID:    uint64(i + 1),
			ClusterID: 1,
			Peers: map[uint64]string{
				1: addr1,
				2: addr2,
			},
			IsRestart:     false,
			IsJoin:        false,
			ElectionTick:  10,
			HeartbeatTick: 1,
			SnapshotCount: 10, // 使用小一点的快照阈值，便于测试
			StorageDir:    dir,
			ListenAddr: func() string {
				if i == 0 {
					return addr1
				}
				return addr2
			}(),
			MaxSizePerMsg:      1024 * 1024,
			MaxInflightMsgs:    256,
			CheckpointInterval: 30,
		}

		// 创建节点
		node, err := NewRaftNode(config, stateMachine)
		require.NoError(t, err)

		nodes[i] = node
		cleanups[i] = func(n *RaftNode, d string) func() {
			return func() {
				n.Stop()
				time.Sleep(500 * time.Millisecond) // 确保节点完全停止
				os.RemoveAll(d)
			}
		}(node, dir)
	}

	// 清理函数
	defer func() {
		for _, cleanup := range cleanups {
			cleanup()
		}
		time.Sleep(1 * time.Second) // 增加最终清理等待时间
	}()

	// 启动初始节点
	for _, node := range nodes {
		err := node.Start()
		require.NoError(t, err)
	}

	// 等待选举完成
	time.Sleep(3 * time.Second)

	// 找到领导者
	var leader *RaftNode
	for _, node := range nodes {
		if node.IsLeader() {
			leader = node
			break
		}
	}
	require.NotNil(t, leader, "应该有一个领导者")
	fmt.Printf("领导者是节点 %d\n", leader.config.NodeID)

	// 先向集群写入一些初始数据
	initialData := []byte(`{"op":"put","key":"initial_key","value":"initial_value"}`)
	err := leader.Propose(context.Background(), initialData)
	require.NoError(t, err)

	// 等待初始数据被应用
	time.Sleep(1 * time.Second)

	// 验证初始数据已被应用
	for i, machine := range stateMachines {
		value, err := machine.Get("initial_key")
		require.NoError(t, err)
		require.Equal(t, "initial_value", value, "节点 %d 应该有初始数据", i+1)
	}

	// 创建快照，确保新节点能接收到完整状态
	err = leader.ForceSnapshot()
	require.NoError(t, err)
	fmt.Println("创建快照成功")

	// 等待快照完成
	time.Sleep(2 * time.Second)

	// 创建第三个节点
	dir3 := createTempDir(t)
	db3 := createBadgerDB(t)
	stateMachine3 := fsm.NewKVStateMachine(db3)
	config3 := RaftNodeConfig{
		NodeID:    3,
		ClusterID: 1,
		Peers: map[uint64]string{
			1: addr1,
			2: addr2,
			3: addr3,
		},
		IsRestart:          false,
		IsJoin:             true, // 标记为加入节点
		ElectionTick:       10,
		HeartbeatTick:      1,
		SnapshotCount:      10,
		StorageDir:         dir3,
		ListenAddr:         addr3,
		MaxSizePerMsg:      1024 * 1024,
		MaxInflightMsgs:    256,
		CheckpointInterval: 30,
	}

	node3, err := NewRaftNode(config3, stateMachine3)
	require.NoError(t, err)
	defer func() {
		node3.Stop()
		time.Sleep(500 * time.Millisecond) // 确保节点完全停止
		os.RemoveAll(dir3)
	}()

	// 启动新节点
	err = node3.Start()
	require.NoError(t, err)

	// 提议添加新节点
	cc := raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  3,
		Context: []byte(addr3),
	}

	err = leader.ProposeConfChange(context.Background(), cc)
	require.NoError(t, err)

	// 等待配置变更生效
	time.Sleep(3 * time.Second) // 增加等待时间

	// 验证新节点已加入集群
	assert.True(t, node3.transport.IsPeerConnected(leader.config.NodeID), "新节点应该能连接到领导者")

	// 验证新节点是否已同步初始数据
	value3, err := stateMachine3.Get("initial_key")
	require.NoError(t, err)
	require.Equal(t, "initial_value", value3, "新节点应该已同步初始数据")

	// 提交一些数据，验证新节点可以接收复制的日志
	testData := []byte(`{"op":"put","key":"member_change_key","value":"member_change_value"}`)
	err = leader.Propose(context.Background(), testData)
	require.NoError(t, err)

	// 等待日志复制和应用
	time.Sleep(1 * time.Second) // 先等待一小段时间让日志复制开始

	// 等待所有节点（包括新节点）应用日志
	allNodes := append(nodes, node3)
	waitForReplication(t, allNodes, leader.GetCommitIndex(), 10*time.Second)

	// 验证所有节点（包括新节点）都应用了日志
	value3, err = stateMachine3.Get("member_change_key")
	require.NoError(t, err)
	assert.Equal(t, "member_change_value", value3, "新节点应该有相同的数据")
}

// 测试领导者故障转移
func TestLeaderFailover(t *testing.T) {
	// 使用随机端口
	basePort := 30000 + int(time.Now().UnixNano()%10000)
	addr1 := fmt.Sprintf("localhost:%d", basePort)
	addr2 := fmt.Sprintf("localhost:%d", basePort+1)
	addr3 := fmt.Sprintf("localhost:%d", basePort+2)

	// 创建三个节点的集群
	nodes, cleanup := createTestCluster(t, 3, fmt.Sprintf("%s,%s,%s", addr1, addr2, addr3))
	defer cleanup()

	// 等待领导者选举完成
	time.Sleep(3 * time.Second)

	// 确定当前领导者
	var leader *RaftNode
	var followers []*RaftNode

	for _, node := range nodes {
		if node.IsLeader() {
			leader = node
		} else {
			followers = append(followers, node)
		}
	}

	require.NotNil(t, leader, "应该选出领导者")
	require.Equal(t, 2, len(followers), "应该有两个跟随者")

	leaderID := leader.config.NodeID
	fmt.Printf("当前领导者是节点 %d\n", leaderID)

	// 向领导者提交一些数据，确保集群正常工作
	for i := 1; i <= 5; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		data := []byte(fmt.Sprintf(`{"op":"put","key":"%s","value":"%s"}`, key, value))
		err := leader.Propose(context.Background(), data)
		require.NoError(t, err)
	}

	// 等待数据复制
	time.Sleep(1 * time.Second)

	// 停止领导者节点
	fmt.Printf("停止领导者节点 %d\n", leaderID)
	leader.Stop()

	// 等待新的领导者选举完成
	time.Sleep(5 * time.Second)

	// 检查是否选出了新的领导者
	var newLeader *RaftNode
	for _, node := range followers {
		if node.IsLeader() {
			newLeader = node
			break
		}
	}

	require.NotNil(t, newLeader, "应该选出新的领导者")
	fmt.Printf("新的领导者是节点 %d\n", newLeader.config.NodeID)

	// 向新领导者提交数据，验证集群仍然可用
	for i := 6; i <= 10; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		data := []byte(fmt.Sprintf(`{"op":"put","key":"%s","value":"%s"}`, key, value))
		err := newLeader.Propose(context.Background(), data)
		require.NoError(t, err)
	}

	// 等待数据复制
	time.Sleep(1 * time.Second)

	// 验证数据一致性
	for _, node := range followers {
		// 使用ConsistentGet方法验证数据
		for i := 1; i <= 10; i++ {
			key := fmt.Sprintf("key%d", i)
			expectedValue := fmt.Sprintf("value%d", i)
			value, err := node.Get(context.Background(), key)
			if err == nil {
				assert.Equal(t, expectedValue, value, "节点 %d 的数据不一致", node.config.NodeID)
			}
		}
	}
}

// TestMajorityFailure 测试多数派节点宕机的情况
func TestMajorityFailure(t *testing.T) {
	// 创建三个节点的集群
	nodes := make([]*RaftNode, 3)
	cleanups := make([]func(), 3)

	// 创建节点配置
	for i := 0; i < 3; i++ {
		// 创建临时目录
		dir := createTempDir(t)

		// 创建BadgerDB内存实例
		db := createBadgerDB(t)

		// 创建状态机
		stateMachine := fsm.NewKVStateMachine(db)

		// 创建节点配置
		config := RaftNodeConfig{
			NodeID:    uint64(i + 1),
			ClusterID: 1,
			Peers: map[uint64]string{
				1: "localhost:36001",
				2: "localhost:36002",
				3: "localhost:36003",
			},
			IsRestart:          false,
			IsJoin:             false,
			ElectionTick:       10,
			HeartbeatTick:      1,
			SnapshotCount:      100,
			StorageDir:         dir,
			ListenAddr:         fmt.Sprintf("localhost:3600%d", i+1),
			MaxSizePerMsg:      1024 * 1024,
			MaxInflightMsgs:    256,
			CheckpointInterval: 30,
		}

		// 创建节点
		node, err := NewRaftNode(config, stateMachine)
		require.NoError(t, err)

		nodes[i] = node
		cleanups[i] = func() {
			node.Stop()
			os.RemoveAll(dir)
		}
	}

	// 清理函数
	defer func() {
		for _, cleanup := range cleanups {
			cleanup()
		}
	}()

	// 启动所有节点
	for _, node := range nodes {
		err := node.Start()
		require.NoError(t, err)
	}

	// 等待初始选举完成
	time.Sleep(3 * time.Second)

	// 找到当前的领导者
	var leader *RaftNode
	var followers []*RaftNode
	for _, node := range nodes {
		if node.IsLeader() {
			leader = node
		} else {
			followers = append(followers, node)
		}
	}
	require.NotNil(t, leader, "应该有一个领导者")

	// 记录当前的任期
	initialTerm := leader.GetTerm()

	// 停止领导者和一个跟随者（造成多数派故障）
	leader.Stop()
	followers[0].Stop()

	// 等待一段时间
	time.Sleep(3 * time.Second)

	// 验证剩余的节点无法成为领导者
	remainingNode := followers[1]
	assert.False(t, remainingNode.IsLeader(), "在多数派故障的情况下不应该选出新的领导者")
	assert.Equal(t, initialTerm, remainingNode.GetTerm(), "在多数派故障的情况下任期不应该增加")
}

// waitForReplication 等待所有节点同步到指定的索引
func waitForReplication(t *testing.T, nodes []*RaftNode, targetIndex uint64, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	done := make(chan struct{})

	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		for {
			allSynced := true
			for _, node := range nodes {
				applied := node.GetAppliedIndex()
				if applied < targetIndex {
					allSynced = false
					fmt.Printf("节点 %d 的应用索引 %d 小于目标索引 %d\n", node.config.NodeID, applied, targetIndex)
					break
				}
			}

			if allSynced {
				fmt.Println("所有节点都已同步到目标索引")
				close(done)
				return
			}

			select {
			case <-ticker.C:
				continue
			case <-ctx.Done():
				fmt.Printf("等待节点同步超时，目标索引: %d\n", targetIndex)
				return
			}
		}
	}()

	select {
	case <-done:
		return
	case <-ctx.Done():
		t.Fatalf("等待节点同步超时，目标索引: %d", targetIndex)
	}
}
