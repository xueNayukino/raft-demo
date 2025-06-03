package transport

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// MockMessageHandler 模拟消息处理器
type MockMessageHandler struct {
	mu             sync.Mutex
	receivedMsgs   []raftpb.Message
	currentTerm    uint64
	shouldFail     bool
	failureMessage string
}

func NewMockMessageHandler(term uint64) *MockMessageHandler {
	return &MockMessageHandler{
		receivedMsgs: make([]raftpb.Message, 0),
		currentTerm:  term,
	}
}

func (m *MockMessageHandler) HandleMessage(msg raftpb.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.shouldFail {
		return fmt.Errorf("%s", m.failureMessage)
	}

	m.receivedMsgs = append(m.receivedMsgs, msg)
	// 如果收到的消息任期更高，更新当前任期
	if msg.Term > m.currentTerm {
		m.currentTerm = msg.Term
	}
	return nil
}

func (m *MockMessageHandler) GetTerm() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.currentTerm
}

func (m *MockMessageHandler) GetReceivedMessages() []raftpb.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]raftpb.Message, len(m.receivedMsgs))
	copy(result, m.receivedMsgs)
	return result
}

func (m *MockMessageHandler) SetShouldFail(fail bool, message string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.shouldFail = fail
	m.failureMessage = message
}

func (m *MockMessageHandler) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.receivedMsgs = make([]raftpb.Message, 0)
}

// 获取可用的本地端口
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

// 测试基本消息传输功能
func TestBasicMessageTransport(t *testing.T) {
	// 创建两个节点
	port1, err := getFreePort()
	require.NoError(t, err)
	port2, err := getFreePort()
	require.NoError(t, err)

	addr1 := fmt.Sprintf("localhost:%d", port1)
	addr2 := fmt.Sprintf("localhost:%d", port2)

	handler1 := NewMockMessageHandler(1)
	handler2 := NewMockMessageHandler(1)

	// 创建两个传输实例
	transport1, err := NewTransport(1, addr1, handler1)
	require.NoError(t, err)
	defer transport1.Stop()

	transport2, err := NewTransport(2, addr2, handler2)
	require.NoError(t, err)
	defer transport2.Stop()

	// 添加对等节点
	err = transport1.AddPeer(2, addr2)
	require.NoError(t, err)
	err = transport2.AddPeer(1, addr1)
	require.NoError(t, err)

	// 等待连接建立
	time.Sleep(500 * time.Millisecond)

	// 测试各种Raft消息类型
	t.Run("AppendEntries消息", func(t *testing.T) {
		handler2.Reset()

		// 创建AppendEntries消息
		entries := []raftpb.Entry{
			{Term: 1, Index: 1, Type: raftpb.EntryNormal, Data: []byte("test data")},
		}
		msg := raftpb.Message{
			Type:    raftpb.MsgApp,
			To:      2,
			From:    1,
			Term:    1,
			LogTerm: 1,
			Index:   0,
			Entries: entries,
			Commit:  0,
		}

		// 发送消息
		err := transport1.Send(msg)
		assert.NoError(t, err)

		// 等待消息处理
		time.Sleep(100 * time.Millisecond)

		// 验证消息接收
		receivedMsgs := handler2.GetReceivedMessages()
		assert.Equal(t, 1, len(receivedMsgs))
		assert.Equal(t, raftpb.MsgApp, receivedMsgs[0].Type)
		assert.Equal(t, uint64(1), receivedMsgs[0].From)
		assert.Equal(t, uint64(2), receivedMsgs[0].To)
		assert.Equal(t, 1, len(receivedMsgs[0].Entries))
		assert.Equal(t, []byte("test data"), receivedMsgs[0].Entries[0].Data)
	})

	t.Run("投票请求消息", func(t *testing.T) {
		handler2.Reset()

		// 创建投票请求消息
		msg := raftpb.Message{
			Type:    raftpb.MsgVote,
			To:      2,
			From:    1,
			Term:    2, // 更高的任期
			LogTerm: 1,
			Index:   1,
		}

		// 发送消息
		err := transport1.Send(msg)
		assert.NoError(t, err)

		// 等待消息处理
		time.Sleep(100 * time.Millisecond)

		// 验证消息接收
		receivedMsgs := handler2.GetReceivedMessages()
		assert.Equal(t, 1, len(receivedMsgs))
		assert.Equal(t, raftpb.MsgVote, receivedMsgs[0].Type)
		assert.Equal(t, uint64(1), receivedMsgs[0].From)
		assert.Equal(t, uint64(2), receivedMsgs[0].To)
		assert.Equal(t, uint64(2), receivedMsgs[0].Term)

		// 验证任期更新
		assert.Equal(t, uint64(2), handler2.GetTerm())
	})

	t.Run("心跳消息", func(t *testing.T) {
		handler2.Reset()

		// 创建心跳消息
		msg := raftpb.Message{
			Type:   raftpb.MsgHeartbeat,
			To:     2,
			From:   1,
			Term:   2,
			Commit: 1,
		}

		// 发送消息
		err := transport1.Send(msg)
		assert.NoError(t, err)

		// 等待消息处理
		time.Sleep(100 * time.Millisecond)

		// 验证消息接收
		receivedMsgs := handler2.GetReceivedMessages()
		assert.Equal(t, 1, len(receivedMsgs))
		assert.Equal(t, raftpb.MsgHeartbeat, receivedMsgs[0].Type)
		assert.Equal(t, uint64(1), receivedMsgs[0].From)
		assert.Equal(t, uint64(2), receivedMsgs[0].To)
		assert.Equal(t, uint64(2), receivedMsgs[0].Term)
	})
}

// 测试快照传输功能
func TestSnapshotTransport(t *testing.T) {
	// 创建两个节点
	port1, err := getFreePort()
	require.NoError(t, err)
	port2, err := getFreePort()
	require.NoError(t, err)

	addr1 := fmt.Sprintf("localhost:%d", port1)
	addr2 := fmt.Sprintf("localhost:%d", port2)

	handler1 := NewMockMessageHandler(1)
	handler2 := NewMockMessageHandler(1)

	// 创建两个传输实例
	transport1, err := NewTransport(1, addr1, handler1)
	require.NoError(t, err)
	defer transport1.Stop()

	transport2, err := NewTransport(2, addr2, handler2)
	require.NoError(t, err)
	defer transport2.Stop()

	// 添加对等节点
	err = transport1.AddPeer(2, addr2)
	require.NoError(t, err)
	err = transport2.AddPeer(1, addr1)
	require.NoError(t, err)

	// 等待连接建立
	time.Sleep(500 * time.Millisecond)

	// 创建快照消息
	snapshotData := make([]byte, 1024*1024) // 1MB 的快照数据
	for i := range snapshotData {
		snapshotData[i] = byte(i % 256)
	}

	confState := raftpb.ConfState{
		Voters: []uint64{1, 2, 3},
	}

	snapshot := raftpb.Snapshot{
		Data: snapshotData,
		Metadata: raftpb.SnapshotMetadata{
			Index:     100,
			Term:      1,
			ConfState: confState,
		},
	}

	msg := raftpb.Message{
		Type:     raftpb.MsgSnap,
		To:       2,
		From:     1,
		Term:     1,
		Snapshot: snapshot,
	}

	// 发送快照消息
	err = transport1.Send(msg)
	assert.NoError(t, err)

	// 等待快照处理完成
	time.Sleep(2 * time.Second)

	// 验证快照接收
	receivedMsgs := handler2.GetReceivedMessages()
	assert.GreaterOrEqual(t, len(receivedMsgs), 1)

	// 查找快照消息
	var receivedSnap *raftpb.Message
	for _, m := range receivedMsgs {
		if m.Type == raftpb.MsgSnap {
			receivedSnap = &m
			break
		}
	}

	assert.NotNil(t, receivedSnap, "应该收到快照消息")
	if receivedSnap != nil {
		assert.Equal(t, uint64(1), receivedSnap.From)
		assert.Equal(t, uint64(2), receivedSnap.To)
		assert.Equal(t, uint64(100), receivedSnap.Snapshot.Metadata.Index)
		assert.Equal(t, uint64(1), receivedSnap.Snapshot.Metadata.Term)
		assert.Equal(t, confState.Voters, receivedSnap.Snapshot.Metadata.ConfState.Voters)
	}
}

// 测试节点故障和恢复
func TestNodeFailureAndRecovery(t *testing.T) {
	// 创建两个节点
	port1, err := getFreePort()
	require.NoError(t, err)
	port2, err := getFreePort()
	require.NoError(t, err)

	addr1 := fmt.Sprintf("localhost:%d", port1)
	addr2 := fmt.Sprintf("localhost:%d", port2)

	handler1 := NewMockMessageHandler(1)
	handler2 := NewMockMessageHandler(1)

	// 创建两个传输实例
	transport1, err := NewTransport(1, addr1, handler1)
	require.NoError(t, err)
	defer transport1.Stop()

	transport2, err := NewTransport(2, addr2, handler2)
	require.NoError(t, err)

	// 添加对等节点
	err = transport1.AddPeer(2, addr2)
	require.NoError(t, err)
	err = transport2.AddPeer(1, addr1)
	require.NoError(t, err)

	// 等待连接建立
	time.Sleep(500 * time.Millisecond)

	// 先发送一条消息确认连接正常
	msg := raftpb.Message{
		Type: raftpb.MsgHeartbeat,
		To:   2,
		From: 1,
		Term: 1,
	}

	err = transport1.Send(msg)
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 1, len(handler2.GetReceivedMessages()))

	// 停止节点2
	transport2.Stop()

	// 清理节点2的消息
	handler2.Reset()

	// 发送消息到已停止的节点
	err = transport1.Send(msg)
	assert.Error(t, err, "发送到停止的节点应该失败")

	// 检查连接状态
	assert.False(t, transport1.IsPeerConnected(2), "节点2应该被标记为断开连接")

	// 重启节点2
	transport2, err = NewTransport(2, addr2, handler2)
	require.NoError(t, err)
	defer transport2.Stop()

	// 添加对等节点
	err = transport2.AddPeer(1, addr1)
	require.NoError(t, err)

	// 等待重连
	time.Sleep(6 * time.Second)

	// 再次发送消息
	err = transport1.Send(msg)
	assert.NoError(t, err, "重连后发送应该成功")

	time.Sleep(100 * time.Millisecond)
	assert.GreaterOrEqual(t, len(handler2.GetReceivedMessages()), 1, "应该收到重连后的消息")
}

// 测试任期更新
func TestTermUpdate(t *testing.T) {
	// 创建两个节点
	port1, err := getFreePort()
	require.NoError(t, err)
	port2, err := getFreePort()
	require.NoError(t, err)

	addr1 := fmt.Sprintf("localhost:%d", port1)
	addr2 := fmt.Sprintf("localhost:%d", port2)

	handler1 := NewMockMessageHandler(1)
	handler2 := NewMockMessageHandler(1)

	// 创建两个传输实例
	transport1, err := NewTransport(1, addr1, handler1)
	require.NoError(t, err)
	defer transport1.Stop()

	transport2, err := NewTransport(2, addr2, handler2)
	require.NoError(t, err)
	defer transport2.Stop()

	// 添加对等节点
	err = transport1.AddPeer(2, addr2)
	require.NoError(t, err)
	err = transport2.AddPeer(1, addr1)
	require.NoError(t, err)

	// 等待连接建立
	time.Sleep(500 * time.Millisecond)

	// 发送更高任期的消息
	msg := raftpb.Message{
		Type: raftpb.MsgVote,
		To:   2,
		From: 1,
		Term: 5, // 更高的任期
	}

	err = transport1.Send(msg)
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	// 验证任期已更新
	assert.Equal(t, uint64(5), handler2.GetTerm())

	// 发送心跳检查任期传播
	time.Sleep(4 * time.Second) // 等待健康检查循环

	// 验证节点1也应该接收到更高的任期
	assert.Equal(t, uint64(5), handler1.GetTerm())
}

// 测试网络分区恢复
func TestNetworkPartitionRecovery(t *testing.T) {
	// 模拟网络分区需要更长的测试时间
	if testing.Short() {
		t.Skip("跳过网络分区恢复测试")
	}

	// 创建两个节点
	port1, err := getFreePort()
	require.NoError(t, err)
	port2, err := getFreePort()
	require.NoError(t, err)

	addr1 := fmt.Sprintf("localhost:%d", port1)
	addr2 := fmt.Sprintf("localhost:%d", port2)

	handler1 := NewMockMessageHandler(1)
	handler2 := NewMockMessageHandler(1)

	// 创建两个传输实例
	transport1, err := NewTransport(1, addr1, handler1)
	require.NoError(t, err)
	defer transport1.Stop()

	transport2, err := NewTransport(2, addr2, handler2)
	require.NoError(t, err)
	defer transport2.Stop()

	// 添加对等节点
	err = transport1.AddPeer(2, addr2)
	require.NoError(t, err)
	err = transport2.AddPeer(1, addr1)
	require.NoError(t, err)

	// 等待连接建立
	time.Sleep(500 * time.Millisecond)

	// 确认连接正常
	msg := raftpb.Message{
		Type: raftpb.MsgHeartbeat,
		To:   2,
		From: 1,
		Term: 1,
	}

	err = transport1.Send(msg)
	assert.NoError(t, err)

	// 模拟网络分区
	transport1.offlineNodesMu.Lock()
	transport1.offlineNodes[2] = time.Now()
	transport1.offlineNodesMu.Unlock()

	transport2.offlineNodesMu.Lock()
	transport2.offlineNodes[1] = time.Now()
	transport2.offlineNodesMu.Unlock()

	// 尝试发送消息
	err = transport1.Send(msg)
	assert.Error(t, err, "网络分区时发送应该失败")

	// 模拟网络恢复
	transport1.offlineNodesMu.Lock()
	delete(transport1.offlineNodes, 2)
	transport1.offlineNodesMu.Unlock()

	transport2.offlineNodesMu.Lock()
	delete(transport2.offlineNodes, 1)
	transport2.offlineNodesMu.Unlock()

	// 重置失败计数
	transport1.resetFailureCount(2)
	transport2.resetFailureCount(1)

	// 等待重连
	time.Sleep(6 * time.Second)

	// 清理接收的消息
	handler2.Reset()

	// 再次发送消息
	err = transport1.Send(msg)
	assert.NoError(t, err, "网络恢复后发送应该成功")

	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 1, len(handler2.GetReceivedMessages()), "应该收到网络恢复后的消息")
}

// 测试心跳机制
func TestHeartbeatMechanism(t *testing.T) {
	// 创建两个节点
	port1, err := getFreePort()
	require.NoError(t, err)
	port2, err := getFreePort()
	require.NoError(t, err)

	addr1 := fmt.Sprintf("localhost:%d", port1)
	addr2 := fmt.Sprintf("localhost:%d", port2)

	handler1 := NewMockMessageHandler(1)
	handler2 := NewMockMessageHandler(1)

	// 创建两个传输实例
	transport1, err := NewTransport(1, addr1, handler1)
	require.NoError(t, err)
	defer transport1.Stop()

	transport2, err := NewTransport(2, addr2, handler2)
	require.NoError(t, err)
	defer transport2.Stop()

	// 添加对等节点
	err = transport1.AddPeer(2, addr2)
	require.NoError(t, err)
	err = transport2.AddPeer(1, addr1)
	require.NoError(t, err)

	// 等待连接建立
	time.Sleep(500 * time.Millisecond)

	// 等待健康检查循环发送心跳
	time.Sleep(4 * time.Second)

	// 验证是否收到心跳消息
	receivedMsgs := handler2.GetReceivedMessages()
	heartbeatReceived := false
	for _, msg := range receivedMsgs {
		if msg.Type == raftpb.MsgHeartbeat {
			heartbeatReceived = true
			break
		}
	}

	assert.True(t, heartbeatReceived, "应该收到心跳消息")

	// 验证连接状态
	assert.True(t, transport1.IsPeerConnected(2), "节点2应该处于连接状态")
	assert.True(t, transport2.IsPeerConnected(1), "节点1应该处于连接状态")
}

// 测试错误处理
func TestErrorHandling(t *testing.T) {
	// 创建两个节点
	port1, err := getFreePort()
	require.NoError(t, err)
	port2, err := getFreePort()
	require.NoError(t, err)

	addr1 := fmt.Sprintf("localhost:%d", port1)
	addr2 := fmt.Sprintf("localhost:%d", port2)

	handler1 := NewMockMessageHandler(1)
	handler2 := NewMockMessageHandler(1)

	// 创建两个传输实例
	transport1, err := NewTransport(1, addr1, handler1)
	require.NoError(t, err)
	defer transport1.Stop()

	transport2, err := NewTransport(2, addr2, handler2)
	require.NoError(t, err)
	defer transport2.Stop()

	// 添加对等节点
	err = transport1.AddPeer(2, addr2)
	require.NoError(t, err)
	err = transport2.AddPeer(1, addr1)
	require.NoError(t, err)

	// 等待连接建立
	time.Sleep(500 * time.Millisecond)

	// 设置handler2返回错误
	handler2.SetShouldFail(true, "模拟处理错误")

	// 发送消息
	msg := raftpb.Message{
		Type: raftpb.MsgHeartbeat,
		To:   2,
		From: 1,
		Term: 1,
	}

	// 发送应该成功，但处理应该失败
	err = transport1.Send(msg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "模拟处理错误")

	// 恢复正常处理
	handler2.SetShouldFail(false, "")

	// 再次发送
	err = transport1.Send(msg)
	assert.NoError(t, err)
}

// 测试节点移除
func TestPeerRemoval(t *testing.T) {
	// 创建两个节点
	port1, err := getFreePort()
	require.NoError(t, err)
	port2, err := getFreePort()
	require.NoError(t, err)

	addr1 := fmt.Sprintf("localhost:%d", port1)
	addr2 := fmt.Sprintf("localhost:%d", port2)

	handler1 := NewMockMessageHandler(1)
	handler2 := NewMockMessageHandler(1)

	// 创建两个传输实例
	transport1, err := NewTransport(1, addr1, handler1)
	require.NoError(t, err)
	defer transport1.Stop()

	transport2, err := NewTransport(2, addr2, handler2)
	require.NoError(t, err)
	defer transport2.Stop()

	// 添加对等节点
	err = transport1.AddPeer(2, addr2)
	require.NoError(t, err)

	// 等待连接建立
	time.Sleep(500 * time.Millisecond)

	// 发送消息确认连接正常
	msg := raftpb.Message{
		Type: raftpb.MsgHeartbeat,
		To:   2,
		From: 1,
		Term: 1,
	}

	err = transport1.Send(msg)
	assert.NoError(t, err)

	// 移除节点
	transport1.RemovePeer(2)

	// 尝试发送消息到已移除的节点
	err = transport1.Send(msg)
	assert.Error(t, err, "发送到已移除的节点应该失败")

	// 验证节点已被移除
	t.Run("验证GetPeerAddrs", func(t *testing.T) {
		addrs := transport1.GetPeerAddrs()
		_, exists := addrs[2]
		assert.False(t, exists, "移除的节点不应该在地址映射中")
	})
}
