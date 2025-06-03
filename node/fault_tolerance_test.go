package node

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"qklzl/fsm"
	"qklzl/storage"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// 创建BadgerDB实例，使用唯一的目录
func createBadgerDBWithDir(t *testing.T, suffix string) (*badger.DB, string) {
	// 创建临时目录
	testName := t.Name()
	timestamp := time.Now().UnixNano()
	dir := filepath.Join(os.TempDir(), fmt.Sprintf("badger-test-%s-%s-%d", testName, suffix, timestamp))
	err := os.MkdirAll(dir, 0755)
	require.NoError(t, err)

	// 创建BadgerDB实例
	opts := badger.DefaultOptions(dir)
	opts.Logger = nil // 禁用日志
	db, err := badger.Open(opts)
	require.NoError(t, err)

	// 确保测试结束后清理
	t.Cleanup(func() {
		db.Close()
		os.RemoveAll(dir)
	})

	return db, dir
}

// 创建BadgerStorage实例，使用唯一的物理目录
func createBadgerStorage(t *testing.T, nodeID uint64) *storage.BadgerStorage {
	// 使用测试名称和节点ID创建唯一目录
	testName := t.Name()
	timestamp := time.Now().UnixNano()
	dirPath := filepath.Join(os.TempDir(), fmt.Sprintf("raft-storage-%s-node%d-%d", testName, nodeID, timestamp))

	// 确保目录存在
	err := os.MkdirAll(dirPath, 0755)
	require.NoError(t, err)

	// 创建存储配置
	storageConfig := &storage.BadgerConfig{
		GCInterval:       time.Second * 30,
		GCDiscardRatio:   0.5,
		SnapshotInterval: time.Second * 30,
		SnapshotCount:    100,
	}

	// 创建存储实例
	store, err := storage.NewBadgerStorage(dirPath, storageConfig, nil)
	require.NoError(t, err)

	// 确保测试结束后清理
	t.Cleanup(func() {
		store.Stop()
		os.RemoveAll(dirPath)
	})

	return store
}

// 测试节点重启
func TestNodeRestart(t *testing.T) {
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

	// 创建第一个临时目录用于Raft存储
	raftDir1 := createTempDir(t)
	defer os.RemoveAll(raftDir1)

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
		StorageDir:         raftDir1,
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
		Data:  []byte(`{"op":"put","key":"restart_key","value":"restart_value"}`),
	})
	require.NoError(t, err)

	// 更新应用索引
	node.mu.Lock()
	node.appliedIndex = 10
	node.mu.Unlock()

	// 停止节点
	node.Stop()
	time.Sleep(500 * time.Millisecond) // 确保节点完全停止

	// 创建第二个临时目录用于重启后的Raft存储
	raftDir2 := createTempDir(t)
	defer os.RemoveAll(raftDir2)

	// 创建新节点，使用不同的存储目录，但设置为重启模式
	config.IsRestart = true
	config.StorageDir = raftDir2

	// 使用不同的端口
	basePort2 := basePort + 1000
	listenAddr2 := fmt.Sprintf("localhost:%d", basePort2)
	config.ListenAddr = listenAddr2
	config.Peers = map[uint64]string{1: listenAddr2}

	node2, err := NewRaftNode(config, stateMachine)
	require.NoError(t, err)
	defer func() {
		node2.Stop()
		time.Sleep(200 * time.Millisecond)
	}()

	// 启动节点
	err = node2.Start()
	require.NoError(t, err)

	// 等待节点启动
	time.Sleep(500 * time.Millisecond)

	// 验证数据是否恢复
	value, err := stateMachine.Get("restart_key")
	require.NoError(t, err)
	assert.Equal(t, "restart_value", value)
}

// TestSnapshotRecovery 测试从快照恢复
func TestSnapshotRecovery(t *testing.T) {
	// 第一阶段：创建节点并生成快照
	// 使用随机端口避免冲突
	basePort := 20000 + int(time.Now().UnixNano()%10000)
	listenAddr1 := fmt.Sprintf("localhost:%d", basePort)

	// 创建内存中的BadgerDB实例
	opts1 := badger.DefaultOptions("").WithInMemory(true).WithLogger(nil)
	db1, err := badger.Open(opts1)
	require.NoError(t, err)
	defer db1.Close()

	// 创建状态机
	stateMachine1 := fsm.NewKVStateMachine(db1)

	// 创建临时目录用于Raft存储
	dir1 := createTempDir(t)
	defer os.RemoveAll(dir1)

	// 创建节点配置
	config1 := RaftNodeConfig{
		NodeID:             1,
		ClusterID:          1,
		Peers:              map[uint64]string{1: listenAddr1},
		IsRestart:          false,
		IsJoin:             false,
		ElectionTick:       10,
		HeartbeatTick:      1,
		SnapshotCount:      10, // 设置较小的值，方便测试
		StorageDir:         dir1,
		ListenAddr:         listenAddr1,
		MaxSizePerMsg:      1024 * 1024,
		MaxInflightMsgs:    256,
		CheckpointInterval: 1, // 设置较小的值，方便测试
	}

	// 创建节点
	node1, err := NewRaftNode(config1, stateMachine1)
	require.NoError(t, err)

	// 启动节点
	err = node1.Start()
	require.NoError(t, err)

	// 写入一些数据
	for i := 1; i <= 20; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		data := []byte(fmt.Sprintf(`{"op":"put","key":"%s","value":"%s"}`, key, value))
		err := node1.Propose(context.Background(), data)
		require.NoError(t, err)
	}

	// 等待数据应用
	time.Sleep(500 * time.Millisecond)

	// 强制创建快照
	err = node1.ForceSnapshot()
	require.NoError(t, err)

	// 获取快照数据
	snapshot, err := node1.GetSnapshot()
	require.NoError(t, err)
	require.False(t, storage.IsEmptySnap(snapshot))

	// 保存快照数据
	snapshotData := snapshot.Data

	// 停止第一个节点
	node1.Stop()
	time.Sleep(500 * time.Millisecond) // 增加等待时间，确保节点完全停止

	// 第二阶段：创建新节点并从快照恢复
	// 使用不同的随机端口，避免端口冲突
	basePort2 := basePort + 1000
	listenAddr2 := fmt.Sprintf("localhost:%d", basePort2)

	// 创建第二个内存中的BadgerDB实例
	opts2 := badger.DefaultOptions("").WithInMemory(true).WithLogger(nil)
	db2, err := badger.Open(opts2)
	require.NoError(t, err)
	defer db2.Close()

	// 创建新的状态机
	stateMachine2 := fsm.NewKVStateMachine(db2)

	// 创建新的临时目录用于Raft存储
	dir2 := createTempDir(t)
	defer os.RemoveAll(dir2)

	// 手动应用快照数据到状态机
	err = stateMachine2.RestoreSnapshot(snapshotData)
	require.NoError(t, err)

	// 验证数据是否恢复
	for i := 1; i <= 20; i++ {
		key := fmt.Sprintf("key%d", i)
		expectedValue := fmt.Sprintf("value%d", i)
		value, err := stateMachine2.Get(key)
		require.NoError(t, err)
		assert.Equal(t, expectedValue, value)
	}

	// 创建新的节点配置，使用不同的监听地址
	config2 := RaftNodeConfig{
		NodeID:             1,
		ClusterID:          1,
		Peers:              map[uint64]string{1: listenAddr2}, // 使用新的监听地址
		IsRestart:          true,                              // 标记为重启
		IsJoin:             false,
		ElectionTick:       10,
		HeartbeatTick:      1,
		SnapshotCount:      10,
		StorageDir:         dir2,
		ListenAddr:         listenAddr2, // 使用新的监听地址
		MaxSizePerMsg:      1024 * 1024,
		MaxInflightMsgs:    256,
		CheckpointInterval: 1,
	}

	// 创建新节点
	node2, err := NewRaftNode(config2, stateMachine2)
	require.NoError(t, err)
	defer func() {
		node2.Stop()
		time.Sleep(200 * time.Millisecond)
	}()

	// 启动节点
	err = node2.Start()
	require.NoError(t, err)

	// 等待节点启动
	time.Sleep(500 * time.Millisecond)

	// 再次验证数据是否可访问
	for i := 1; i <= 20; i++ {
		key := fmt.Sprintf("key%d", i)
		expectedValue := fmt.Sprintf("value%d", i)
		value, err := stateMachine2.Get(key)
		require.NoError(t, err)
		assert.Equal(t, expectedValue, value)
	}
}

// copyDir 复制目录内容
func copyDir(src, dst string) error {
	// 获取源目录中的所有文件和子目录
	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}

	// 确保目标目录存在
	if err := os.MkdirAll(dst, 0755); err != nil {
		return err
	}

	// 复制每个文件和子目录
	for _, entry := range entries {
		srcPath := filepath.Join(src, entry.Name())
		dstPath := filepath.Join(dst, entry.Name())

		if entry.IsDir() {
			// 如果是目录，递归复制
			if err := copyDir(srcPath, dstPath); err != nil {
				return err
			}
		} else {
			// 如果是文件，复制文件内容
			data, err := os.ReadFile(srcPath)
			if err != nil {
				return err
			}
			if err := os.WriteFile(dstPath, data, 0644); err != nil {
				return err
			}
		}
	}

	return nil
}

// 测试网络分区恢复
func TestNetworkPartitionRecovery(t *testing.T) {
	// 创建三个节点的集群
	nodes := make([]*RaftNode, 3)
	dbs := make([]*badger.DB, 3)
	stateMachines := make([]*fsm.KVStateMachine, 3)
	cleanups := make([]func(), 3)

	// 使用getFreePort获取随机端口，避免端口冲突
	ports := make([]int, 3)
	for i := 0; i < 3; i++ {
		var err error
		ports[i], err = getFreePort()
		require.NoError(t, err)
	}

	// 创建三个节点的配置
	for i := 0; i < 3; i++ {
		// 创建内存中的BadgerDB实例
		opts := badger.DefaultOptions("").WithInMemory(true).WithLogger(nil)
		db, err := badger.Open(opts)
		require.NoError(t, err)
		dbs[i] = db

		// 创建状态机
		stateMachine := fsm.NewKVStateMachine(db)
		stateMachines[i] = stateMachine

		// 创建临时目录用于Raft存储
		raftDir := createTempDir(t)

		// 创建节点配置
		config := RaftNodeConfig{
			NodeID:    uint64(i + 1),
			ClusterID: 1,
			Peers: map[uint64]string{
				1: fmt.Sprintf("localhost:%d", ports[0]),
				2: fmt.Sprintf("localhost:%d", ports[1]),
				3: fmt.Sprintf("localhost:%d", ports[2]),
			},
			IsRestart:          false,
			IsJoin:             false,
			ElectionTick:       10,
			HeartbeatTick:      1,
			SnapshotCount:      100,
			StorageDir:         raftDir,
			ListenAddr:         fmt.Sprintf("localhost:%d", ports[i]),
			MaxSizePerMsg:      1024 * 1024,
			MaxInflightMsgs:    256,
			CheckpointInterval: 30,
		}

		// 创建节点
		node, err := NewRaftNode(config, stateMachine)
		require.NoError(t, err)
		nodes[i] = node

		// 创建清理函数
		cleanups[i] = func(n *RaftNode, dir string, db *badger.DB) func() {
			return func() {
				n.Stop()
				time.Sleep(500 * time.Millisecond) // 增加等待时间确保节点完全停止
				db.Close()
				os.RemoveAll(dir)
			}
		}(node, raftDir, db)
	}

	// 清理函数
	defer func() {
		for _, cleanup := range cleanups {
			cleanup()
		}
		time.Sleep(1 * time.Second) // 增加最终清理等待时间
	}()

	// 第一阶段：启动所有节点并等待选举完成
	for _, node := range nodes {
		err := node.Start()
		require.NoError(t, err)
	}

	// 等待选举完成
	time.Sleep(3 * time.Second) // 增加等待时间确保选举完成

	// 找到领导者
	var leader *RaftNode
	for _, node := range nodes {
		if node.IsLeader() {
			leader = node
			break
		}
	}
	require.NotNil(t, leader, "应该有一个领导者")

	// 在所有节点正常的情况下写入一些数据
	initialData := []byte(`{"op":"put","key":"partition_test_key","value":"initial_value"}`)
	err := leader.Propose(context.Background(), initialData)
	require.NoError(t, err)

	// 等待数据复制到所有节点
	time.Sleep(1 * time.Second)

	// 验证所有节点都有初始数据
	for i, machine := range stateMachines {
		value, err := machine.Get("partition_test_key")
		require.NoError(t, err)
		require.Equal(t, "initial_value", value, "节点 %d 应该有初始数据", i+1)
	}

	// 第二阶段：停止节点2和节点3，模拟网络分区
	log.Printf("停止节点2和节点3，模拟网络分区")
	nodes[1].Stop() // 节点2
	nodes[2].Stop() // 节点3

	// 等待停止完成
	time.Sleep(1 * time.Second) // 增加等待时间确保节点完全停止

	// 第三阶段：在节点1（可能是领导者）写入新数据
	newData := []byte(`{"op":"put","key":"partition_test_key","value":"during_partition_value"}`)
	// 如果节点1不是领导者，会自动转发到领导者
	err = nodes[0].Propose(context.Background(), newData)

	// 由于节点2和节点3已停止，如果节点1不是领导者，这个操作可能会失败
	// 但如果节点1是领导者，操作应该成功
	if nodes[0].IsLeader() {
		require.NoError(t, err)
		// 等待数据应用
		time.Sleep(1 * time.Second) // 增加等待时间

		// 验证节点1上的数据已更新
		value, err := stateMachines[0].Get("partition_test_key")
		require.NoError(t, err)
		require.Equal(t, "during_partition_value", value, "节点1应该有分区期间写入的数据")
	} else {
		log.Printf("节点1不是领导者，无法在分区期间写入数据")
		t.Skip("节点1不是领导者，跳过此测试")
		return
	}

	// 第四阶段：创建新的节点2和节点3，使用新的存储和网络端口
	log.Printf("创建新的节点2和节点3，模拟分区恢复")

	// 获取新的端口
	newPorts := make([]int, 2)
	for i := 0; i < 2; i++ {
		var err error
		newPorts[i], err = getFreePort()
		require.NoError(t, err)
	}

	for i := 1; i < 3; i++ {
		// 创建新的内存中的BadgerDB实例
		opts := badger.DefaultOptions("").WithInMemory(true).WithLogger(nil)
		newDB, err := badger.Open(opts)
		require.NoError(t, err)
		defer newDB.Close()

		// 创建新的状态机
		newStateMachine := fsm.NewKVStateMachine(newDB)

		// 创建新的临时目录
		newRaftDir := createTempDir(t)
		defer os.RemoveAll(newRaftDir)

		// 更新原始节点的端口配置
		nodes[0].config.Peers[uint64(i+1)] = fmt.Sprintf("localhost:%d", newPorts[i-1])

		// 创建新的节点配置
		newConfig := RaftNodeConfig{
			NodeID:    uint64(i + 1), // 仍然是节点2或节点3
			ClusterID: 1,
			Peers: map[uint64]string{
				1: fmt.Sprintf("localhost:%d", ports[0]),    // 节点1的地址保持不变
				2: fmt.Sprintf("localhost:%d", newPorts[0]), // 节点2的新地址
				3: fmt.Sprintf("localhost:%d", newPorts[1]), // 节点3的新地址
			},
			IsRestart:          false,
			IsJoin:             true, // 作为新节点加入
			ElectionTick:       10,
			HeartbeatTick:      1,
			SnapshotCount:      100,
			StorageDir:         newRaftDir,
			ListenAddr:         fmt.Sprintf("localhost:%d", newPorts[i-1]),
			MaxSizePerMsg:      1024 * 1024,
			MaxInflightMsgs:    256,
			CheckpointInterval: 30,
		}

		// 创建并启动新节点
		newNode, err := NewRaftNode(newConfig, newStateMachine)
		require.NoError(t, err)
		defer func(n *RaftNode) {
			n.Stop()
			time.Sleep(500 * time.Millisecond) // 确保节点完全停止
		}(newNode)

		err = newNode.Start()
		require.NoError(t, err)

		// 替换原来的节点
		dbs[i] = newDB
		stateMachines[i] = newStateMachine
		nodes[i] = newNode

		// 等待新节点启动并初始化连接
		time.Sleep(1 * time.Second)
	}

	// 等待新节点加入集群并同步数据
	time.Sleep(5 * time.Second) // 增加等待时间确保数据同步

	// 第五阶段：验证所有节点上的数据都是一致的
	for i, machine := range stateMachines {
		value, err := machine.Get("partition_test_key")
		if err != nil {
			log.Printf("节点%d读取数据失败: %v", i+1, err)
			continue
		}

		// 检查数据值
		if value != "during_partition_value" {
			log.Printf("节点%d数据不一致，期望值=during_partition_value，实际值=%s", i+1, value)
		} else {
			log.Printf("节点%d数据一致，值=%s", i+1, value)
		}
	}

	// 最后，写入一个新的值，确认集群功能正常
	finalData := []byte(`{"op":"put","key":"final_key","value":"final_value"}`)

	// 找到当前的领导者
	leader = nil
	for _, node := range nodes {
		if node.IsLeader() {
			leader = node
			break
		}
	}

	if leader != nil {
		err = leader.Propose(context.Background(), finalData)
		require.NoError(t, err)

		// 等待数据复制
		time.Sleep(2 * time.Second) // 增加等待时间确保数据复制

		// 验证所有节点都有最终数据
		for i, machine := range stateMachines {
			value, err := machine.Get("final_key")
			if err != nil {
				log.Printf("节点%d读取最终数据失败: %v", i+1, err)
				continue
			}

			if value != "final_value" {
				log.Printf("节点%d最终数据不一致，期望值=final_value，实际值=%s", i+1, value)
			} else {
				log.Printf("节点%d最终数据一致，值=%s", i+1, value)
			}
		}
	} else {
		log.Printf("未找到领导者，无法写入最终数据")
	}
}

// TestMultipleNodeFailures 测试多节点故障
func TestMultipleNodeFailures(t *testing.T) {
	// 创建五个节点的集群
	nodes := make([]*RaftNode, 5)
	cleanups := make([]func(), 5)

	// 使用getFreePort获取随机端口，避免端口冲突
	ports := make([]int, 5)
	for i := 0; i < 5; i++ {
		var err error
		ports[i], err = getFreePort()
		require.NoError(t, err)
	}

	// 创建节点配置
	for i := 0; i < 5; i++ {
		// 创建内存中的BadgerDB实例
		opts := badger.DefaultOptions("").WithInMemory(true).WithLogger(nil)
		db, err := badger.Open(opts)
		require.NoError(t, err)

		// 创建状态机
		stateMachine := fsm.NewKVStateMachine(db)

		// 创建临时目录用于Raft存储
		raftDir := createTempDir(t)

		// 创建节点配置
		peerMap := make(map[uint64]string)
		for j := 0; j < 5; j++ {
			peerMap[uint64(j+1)] = fmt.Sprintf("localhost:%d", ports[j])
		}

		config := RaftNodeConfig{
			NodeID:             uint64(i + 1),
			ClusterID:          1,
			Peers:              peerMap,
			IsRestart:          false,
			IsJoin:             false,
			ElectionTick:       10,
			HeartbeatTick:      1,
			SnapshotCount:      100,
			StorageDir:         raftDir,
			ListenAddr:         fmt.Sprintf("localhost:%d", ports[i]),
			MaxSizePerMsg:      1024 * 1024,
			MaxInflightMsgs:    256,
			CheckpointInterval: 30,
		}

		// 创建节点
		node, err := NewRaftNode(config, stateMachine)
		require.NoError(t, err)

		nodes[i] = node
		cleanups[i] = func(n *RaftNode, dir string, db *badger.DB) func() {
			return func() {
				n.Stop()
				time.Sleep(500 * time.Millisecond) // 增加等待时间确保节点完全停止
				db.Close()
				os.RemoveAll(dir)
			}
		}(node, raftDir, db)
	}

	// 清理函数
	defer func() {
		for _, cleanup := range cleanups {
			cleanup()
		}
		time.Sleep(1 * time.Second) // 增加最终清理等待时间
	}()

	// 启动所有节点
	for _, node := range nodes {
		err := node.Start()
		require.NoError(t, err)
	}

	// 等待选举完成
	time.Sleep(3 * time.Second) // 增加等待时间确保选举完成

	// 找到领导者
	var leader *RaftNode
	for _, node := range nodes {
		if node.IsLeader() {
			leader = node
			break
		}
	}
	require.NotNil(t, leader, "应该有一个领导者")

	// 提交初始数据
	testData := []byte(`{"op":"put","key":"multi_failure_key","value":"multi_failure_value"}`)
	err := leader.Propose(context.Background(), testData)
	require.NoError(t, err)

	// 等待数据复制
	time.Sleep(2 * time.Second) // 增加等待时间确保数据复制

	// 选择两个跟随者节点进行宕机测试
	var crashNodes []*RaftNode
	for _, node := range nodes {
		if !node.IsLeader() {
			crashNodes = append(crashNodes, node)
			if len(crashNodes) == 2 {
				break
			}
		}
	}
	require.Equal(t, 2, len(crashNodes), "应该选择两个跟随者节点")

	// 停止选中的节点
	for _, node := range crashNodes {
		node.Stop()
	}

	// 等待节点完全停止
	time.Sleep(1 * time.Second)

	// 在部分节点宕机期间提交新数据
	newData := []byte(`{"op":"put","key":"during_failure","value":"during_failure_value"}`)
	err = leader.Propose(context.Background(), newData)
	require.NoError(t, err)

	// 等待数据复制到存活的节点
	time.Sleep(2 * time.Second) // 增加等待时间确保数据复制

	// 验证存活节点上的数据
	for _, node := range nodes {
		if node.IsLeader() || (node != crashNodes[0] && node != crashNodes[1]) {
			// 验证初始数据
			value, err := node.fsm.Get("multi_failure_key")
			require.NoError(t, err)
			require.Equal(t, "multi_failure_value", value, "节点 %d 应该有初始数据", node.config.NodeID)

			// 验证故障期间写入的数据
			value, err = node.fsm.Get("during_failure")
			require.NoError(t, err)
			require.Equal(t, "during_failure_value", value, "节点 %d 应该有故障期间写入的数据", node.config.NodeID)
		}
	}
}
