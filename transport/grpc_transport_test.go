package transport

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

// 测试创建传输实例
func TestNewTransport(t *testing.T) {
	port, err := getFreePort()
	if err != nil {
		t.Fatalf("获取端口失败: %v", err)
	}

	handler := &testMessageHandler{}
	transport, err := NewTransport(1, fmt.Sprintf("localhost:%d", port), handler)
	if err != nil {
		t.Fatalf("创建传输实例失败: %v", err)
	}
	defer transport.Stop()

	// 验证基本属性
	if transport.nodeID != 1 {
		t.Errorf("节点ID不匹配，期望1，实际%d", transport.nodeID)
	}
}

// 测试消息处理器
type testMessageHandler struct {
	messages []raftpb.Message
	mu       sync.Mutex
}

func (h *testMessageHandler) HandleMessage(msg raftpb.Message) error {
	h.mu.Lock()
	h.messages = append(h.messages, msg)
	h.mu.Unlock()
	return nil
}

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

// 测试基本的消息发送和接收
func TestBasicSendReceive(t *testing.T) {
	// 创建两个节点
	port1, err := getFreePort()
	if err != nil {
		t.Fatalf("获取端口失败: %v", err)
	}
	port2, err := getFreePort()
	if err != nil {
		t.Fatalf("获取端口失败: %v", err)
	}

	handler1 := &testMessageHandler{}
	handler2 := &testMessageHandler{}

	// 创建两个传输实例
	transport1, err := NewTransport(1, fmt.Sprintf("localhost:%d", port1), handler1)
	if err != nil {
		t.Fatalf("创建传输实例1失败: %v", err)
	}
	defer transport1.Stop()

	transport2, err := NewTransport(2, fmt.Sprintf("localhost:%d", port2), handler2)
	if err != nil {
		t.Fatalf("创建传输实例2失败: %v", err)
	}
	defer transport2.Stop()

	// 添加对等节点
	err = transport1.AddPeer(2, fmt.Sprintf("localhost:%d", port2))
	if err != nil {
		t.Fatalf("添加对等节点失败: %v", err)
	}

	// 发送测试消息
	testMsg := raftpb.Message{
		Type: raftpb.MsgHeartbeat,
		To:   2,
		From: 1,
		Term: 1,
	}

	// 等待连接建立
	time.Sleep(100 * time.Millisecond)

	err = transport1.Send(testMsg)
	if err != nil {
		t.Fatalf("发送消息失败: %v", err)
	}

	// 等待消息处理
	time.Sleep(50 * time.Millisecond)

	// 验证消息接收
	if len(handler2.messages) != 1 {
		t.Fatalf("期望收到1条消息，实际收到%d条", len(handler2.messages))
	}

	receivedMsg := handler2.messages[0]
	if receivedMsg.Type != testMsg.Type ||
		receivedMsg.To != testMsg.To ||
		receivedMsg.From != testMsg.From ||
		receivedMsg.Term != testMsg.Term {
		t.Errorf("收到的消息与发送的消息不匹配")
	}
}

// 测试流式通信
func TestStreamCommunication(t *testing.T) {
	port1, err := getFreePort()
	if err != nil {
		t.Fatalf("获取端口失败: %v", err)
	}
	port2, err := getFreePort()
	if err != nil {
		t.Fatalf("获取端口失败: %v", err)
	}

	handler1 := &testMessageHandler{}
	handler2 := &testMessageHandler{}

	transport1, err := NewTransport(1, fmt.Sprintf("localhost:%d", port1), handler1)
	if err != nil {
		t.Fatalf("创建传输实例1失败: %v", err)
	}
	defer transport1.Stop()

	transport2, err := NewTransport(2, fmt.Sprintf("localhost:%d", port2), handler2)
	if err != nil {
		t.Fatalf("创建传输实例2失败: %v", err)
	}
	defer transport2.Stop()

	// 添加对等节点
	err = transport1.AddPeer(2, fmt.Sprintf("localhost:%d", port2))
	if err != nil {
		t.Fatalf("添加对等节点失败: %v", err)
	}

	// 等待连接建立
	time.Sleep(time.Second)

	// 发送多条消息
	for i := 0; i < 5; i++ {
		msg := raftpb.Message{
			Type: raftpb.MsgHeartbeat,
			To:   2,
			From: 1,
			Term: uint64(i + 1),
		}
		err = transport1.Send(msg)
		if err != nil {
			t.Fatalf("发送消息失败: %v", err)
		}
	}

	// 等待消息处理
	time.Sleep(100 * time.Millisecond)

	// 验证所有消息都被接收
	if len(handler2.messages) != 5 {
		t.Fatalf("期望收到5条消息，实际收到%d条", len(handler2.messages))
	}

	// 验证消息顺序
	for i, msg := range handler2.messages {
		if msg.Term != uint64(i+1) {
			t.Errorf("消息顺序错误: 期望term=%d, 实际term=%d", i+1, msg.Term)
		}
	}
}

// 测试节点管理
func TestPeerManagement(t *testing.T) {
	t.Log("开始测试")
	port1, err := getFreePort()
	if err != nil {
		t.Fatalf("获取端口失败: %v", err)
	}
	t.Logf("获取到端口: port1=%d", port1)

	handler := &testMessageHandler{}
	t.Log("创建传输实例1")
	transport, err := NewTransport(1, fmt.Sprintf("localhost:%d", port1), handler)
	if err != nil {
		t.Fatalf("创建传输实例1失败: %v", err)
	}
	defer transport.Stop()

	// 测试添加节点
	port2, err := getFreePort()
	if err != nil {
		t.Fatalf("获取端口失败: %v", err)
	}
	t.Logf("获取到端口: port2=%d", port2)

	t.Log("创建传输实例2")
	transport2, err := NewTransport(2, fmt.Sprintf("localhost:%d", port2), handler)
	if err != nil {
		t.Fatalf("创建传输实例2失败: %v", err)
	}
	defer transport2.Stop()

	t.Log("添加对等节点")
	err = transport.AddPeer(2, fmt.Sprintf("localhost:%d", port2))
	if err != nil {
		t.Fatalf("添加节点失败: %v", err)
	}

	// 验证节点是否添加成功
	t.Log("验证节点添加")
	transport.peersMu.RLock()
	addr, ok := transport.peers[2]
	transport.peersMu.RUnlock()
	if !ok {
		t.Error("节点未成功添加")
	}
	if addr != fmt.Sprintf("localhost:%d", port2) {
		t.Errorf("节点地址不匹配，期望 localhost:%d，实际 %s", port2, addr)
	}

	// 测试重复添加节点
	t.Log("测试重复添加节点")
	err = transport.AddPeer(2, fmt.Sprintf("localhost:%d", port2))
	if err == nil {
		t.Error("重复添加节点应该失败")
	}

	// 测试移除节点
	t.Log("测试移除节点")
	transport.RemovePeer(2)

	// 验证节点是否移除成功
	transport.peersMu.RLock()
	_, ok = transport.peers[2]
	transport.peersMu.RUnlock()
	if ok {
		t.Error("节点未成功移除")
	}

	// 验证连接是否关闭
	transport.connsMu.RLock()
	_, ok = transport.conns[2]
	transport.connsMu.RUnlock()
	if ok {
		t.Error("连接未成功关闭")
	}

	// 验证流是否关闭
	transport.streamMu.RLock()
	_, ok = transport.streams[2]
	transport.streamMu.RUnlock()
	if ok {
		t.Error("流未成功关闭")
	}

	// 测试向已移除的节点发送消息
	t.Log("测试向已移除节点发送消息")
	err = transport.Send(raftpb.Message{To: 2})
	if err == nil {
		t.Error("向已移除的节点发送消息应该失败")
	}

	t.Log("测试完成")
}

// 测试错误处理
func TestErrorHandling(t *testing.T) {
	port1, err := getFreePort()
	if err != nil {
		t.Fatalf("获取端口失败: %v", err)
	}

	handler := &testMessageHandler{}
	transport, err := NewTransport(1, fmt.Sprintf("localhost:%d", port1), handler)
	if err != nil {
		t.Fatalf("创建传输实例失败: %v", err)
	}
	defer transport.Stop()

	// 测试发送到不存在的节点
	err = transport.Send(raftpb.Message{To: 999})
	if err == nil {
		t.Error("向不存在的节点发送消息应该失败")
	}

	// 测试无效地址
	err = transport.AddPeer(2, "invalid:address")
	if err == nil {
		t.Error("添加无效地址应该失败")
	}

	// 测试连接超时
	port2, _ := getFreePort()
	transport2, err := NewTransport(2, fmt.Sprintf("localhost:%d", port2), handler)
	if err != nil {
		t.Fatalf("创建传输实例2失败: %v", err)
	}
	defer transport2.Stop()

	// 使用较短的超时时间
	transport2.Stop() // 先停止服务
	err = transport.AddPeer(2, fmt.Sprintf("localhost:%d", port2))
	if err == nil {
		t.Error("连接到已停止的节点应该失败")
	}
}

// 测试快照传输
func TestSnapshotTransfer(t *testing.T) {
	t.Log("开始测试")
	port1, err := getFreePort()
	if err != nil {
		t.Fatalf("获取端口失败: %v", err)
	}
	port2, err := getFreePort()
	if err != nil {
		t.Fatalf("获取端口失败: %v", err)
	}
	t.Logf("获取到端口: port1=%d, port2=%d", port1, port2)

	handler1 := &testMessageHandler{}
	handler2 := &testMessageHandler{}

	t.Log("创建传输实例1")
	transport1, err := NewTransport(1, fmt.Sprintf("localhost:%d", port1), handler1)
	if err != nil {
		t.Fatalf("创建传输实例1失败: %v", err)
	}
	defer transport1.Stop()

	t.Log("创建传输实例2")
	transport2, err := NewTransport(2, fmt.Sprintf("localhost:%d", port2), handler2)
	if err != nil {
		t.Fatalf("创建传输实例2失败: %v", err)
	}
	defer transport2.Stop()

	t.Log("添加对等节点")
	err = transport1.AddPeer(2, fmt.Sprintf("localhost:%d", port2))
	if err != nil {
		t.Fatalf("添加对等节点失败: %v", err)
	}

	t.Log("等待连接建立")
	time.Sleep(100 * time.Millisecond)

	// 创建大型快照数据
	t.Log("创建快照数据")
	snapshotSize := 5 * 1024 * 1024 // 5MB
	snapshotData := make([]byte, snapshotSize)
	for i := range snapshotData {
		snapshotData[i] = byte(i % 256)
	}

	t.Log("创建快照消息")
	snapMsg := raftpb.Message{
		Type: raftpb.MsgSnap,
		To:   2,
		From: 1,
		Term: 1,
		Snapshot: raftpb.Snapshot{
			Data: snapshotData,
			Metadata: raftpb.SnapshotMetadata{
				Index: 1000,
				Term:  1,
				ConfState: raftpb.ConfState{
					Voters: []uint64{1, 2, 3},
				},
			},
		},
	}

	t.Log("发送快照")
	err = transport1.Send(snapMsg)
	if err != nil {
		t.Fatalf("发送快照失败: %v", err)
	}

	t.Log("等待快照传输完成")
	time.Sleep(500 * time.Millisecond)

	t.Log("验证快照接收")
	if len(handler2.messages) != 1 {
		t.Fatalf("期望收到1条快照消息，实际收到%d条", len(handler2.messages))
	}

	receivedMsg := handler2.messages[0]
	if receivedMsg.Type != raftpb.MsgSnap {
		t.Error("接收到的消息类型不是快照")
	}

	if !bytes.Equal(receivedMsg.Snapshot.Data, snapshotData) {
		t.Error("快照数据不匹配")
	}

	if receivedMsg.Snapshot.Metadata.Index != 1000 {
		t.Errorf("快照索引不匹配，期望1000，实际%d", receivedMsg.Snapshot.Metadata.Index)
	}

	if receivedMsg.Snapshot.Metadata.Term != 1 {
		t.Errorf("快照任期不匹配，期望1，实际%d", receivedMsg.Snapshot.Metadata.Term)
	}

	if len(receivedMsg.Snapshot.Metadata.ConfState.Voters) != 3 {
		t.Errorf("快照配置状态不匹配，期望3个投票者，实际%d个",
			len(receivedMsg.Snapshot.Metadata.ConfState.Voters))
	}

	t.Log("测试完成")
}

// 测试流式连接重连
func TestStreamReconnection(t *testing.T) {
	port1, err := getFreePort()
	if err != nil {
		t.Fatalf("获取端口失败: %v", err)
	}
	port2, err := getFreePort()
	if err != nil {
		t.Fatalf("获取端口失败: %v", err)
	}

	handler1 := &testMessageHandler{}
	handler2 := &testMessageHandler{}

	transport1, err := NewTransport(1, fmt.Sprintf("localhost:%d", port1), handler1)
	if err != nil {
		t.Fatalf("创建传输实例1失败: %v", err)
	}
	defer transport1.Stop()

	transport2, err := NewTransport(2, fmt.Sprintf("localhost:%d", port2), handler2)
	if err != nil {
		t.Fatalf("创建传输实例2失败: %v", err)
	}

	// 添加对等节点
	err = transport1.AddPeer(2, fmt.Sprintf("localhost:%d", port2))
	if err != nil {
		t.Fatalf("添加对等节点失败: %v", err)
	}

	// 等待连接建立
	time.Sleep(100 * time.Millisecond)

	// 发送测试消息
	msg := raftpb.Message{
		Type: raftpb.MsgHeartbeat,
		To:   2,
		From: 1,
		Term: 1,
	}

	// 第一次发送应该成功
	err = transport1.Send(msg)
	if err != nil {
		t.Fatalf("发送消息失败: %v", err)
	}

	// 停止节点2
	transport2.Stop()

	// 发送消息应该失败
	err = transport1.Send(msg)
	if err == nil {
		t.Error("向已停止的节点发送消息应该失败")
	}

	// 重启节点2
	transport2, err = NewTransport(2, fmt.Sprintf("localhost:%d", port2), handler2)
	if err != nil {
		t.Fatalf("重启节点2失败: %v", err)
	}
	defer transport2.Stop()

	// 等待重连
	time.Sleep(200 * time.Millisecond)

	// 再次发送消息应该成功
	err = transport1.Send(msg)
	if err != nil {
		t.Fatalf("重连后发送消息失败: %v", err)
	}
}

// 测试快照分块传输
func TestSnapshotChunkTransfer(t *testing.T) {
	port1, err := getFreePort()
	if err != nil {
		t.Fatalf("获取端口失败: %v", err)
	}
	port2, err := getFreePort()
	if err != nil {
		t.Fatalf("获取端口失败: %v", err)
	}

	handler1 := &testMessageHandler{}
	handler2 := &testMessageHandler{}

	transport1, err := NewTransport(1, fmt.Sprintf("localhost:%d", port1), handler1)
	if err != nil {
		t.Fatalf("创建传输实例1失败: %v", err)
	}
	defer transport1.Stop()

	transport2, err := NewTransport(2, fmt.Sprintf("localhost:%d", port2), handler2)
	if err != nil {
		t.Fatalf("创建传输实例2失败: %v", err)
	}
	defer transport2.Stop()

	// 添加对等节点
	err = transport1.AddPeer(2, fmt.Sprintf("localhost:%d", port2))
	if err != nil {
		t.Fatalf("添加对等节点失败: %v", err)
	}

	// 等待连接建立
	time.Sleep(100 * time.Millisecond)

	// 创建测试快照数据
	snapshotData := make([]byte, 3*1024*1024) // 3MB 数据
	for i := range snapshotData {
		snapshotData[i] = byte(i % 256)
	}

	// 创建快照消息
	snapMsg := raftpb.Message{
		Type: raftpb.MsgSnap,
		To:   2,
		From: 1,
		Term: 1,
		Snapshot: raftpb.Snapshot{
			Data: snapshotData,
			Metadata: raftpb.SnapshotMetadata{
				Index: 100,
				Term:  1,
				ConfState: raftpb.ConfState{
					Voters: []uint64{1, 2, 3},
				},
			},
		},
	}

	// 发送快照
	err = transport1.Send(snapMsg)
	if err != nil {
		t.Fatalf("发送快照失败: %v", err)
	}

	// 等待快照传输完成
	time.Sleep(200 * time.Millisecond)

	// 验证快照接收
	handler2.mu.Lock()
	receivedMsgs := handler2.messages
	handler2.mu.Unlock()

	var lastSnapshotMsg *raftpb.Message
	for i := len(receivedMsgs) - 1; i >= 0; i-- {
		if receivedMsgs[i].Type == raftpb.MsgSnap {
			msg := receivedMsgs[i]
			lastSnapshotMsg = &msg
			break
		}
	}

	if lastSnapshotMsg == nil {
		t.Fatal("节点2未收到快照消息")
	}

	// 验证快照消息的属性
	if lastSnapshotMsg.From != 1 {
		t.Errorf("快照消息的源节点错误，期望 1，实际 %d", lastSnapshotMsg.From)
	}

	if lastSnapshotMsg.To != 2 {
		t.Errorf("快照消息的目标节点错误，期望 2，实际 %d", lastSnapshotMsg.To)
	}

	// 测试节点移除时的快照清理
	transport1.RemovePeer(2)
	transport1.snapshotMu.RLock()
	snapshotCount := len(transport1.snapshots)
	transport1.snapshotMu.RUnlock()
	if snapshotCount != 0 {
		t.Errorf("移除节点后快照状态未清理，剩余 %d 个快照", snapshotCount)
	}
}

// 测试心跳机制
func TestHeartbeat(t *testing.T) {
	t.Log("开始测试")
	port1, err := getFreePort()
	if err != nil {
		t.Fatalf("获取端口失败: %v", err)
	}
	port2, err := getFreePort()
	if err != nil {
		t.Fatalf("获取端口失败: %v", err)
	}
	t.Logf("获取到端口: port1=%d, port2=%d", port1, port2)

	handler1 := &testMessageHandler{}
	handler2 := &testMessageHandler{}

	t.Log("创建传输实例1")
	transport1, err := NewTransport(1, fmt.Sprintf("localhost:%d", port1), handler1)
	if err != nil {
		t.Fatalf("创建传输实例1失败: %v", err)
	}
	defer transport1.Stop()

	t.Log("创建传输实例2")
	transport2, err := NewTransport(2, fmt.Sprintf("localhost:%d", port2), handler2)
	if err != nil {
		t.Fatalf("创建传输实例2失败: %v", err)
	}
	defer transport2.Stop()

	t.Log("添加对等节点")
	err = transport1.AddPeer(2, fmt.Sprintf("localhost:%d", port2))
	if err != nil {
		t.Fatalf("添加对等节点失败: %v", err)
	}

	t.Log("等待连接建立")
	time.Sleep(100 * time.Millisecond)

	t.Log("创建心跳请求")
	req := &HeartbeatRequest{
		From: 1,
		To:   2,
		Term: 1,
	}

	t.Log("发送心跳")
	client := NewRaftServiceClient(transport1.conns[2])
	resp, err := client.Heartbeat(context.Background(), req)
	if err != nil {
		t.Fatalf("发送心跳失败: %v", err)
	}

	t.Log("验证心跳响应")
	if !resp.Success {
		t.Error("心跳响应应该成功")
	}
	if resp.ConnState != ConnectionState_Connected {
		t.Error("连接状态应该是 Connected")
	}
	if resp.Term != req.Term {
		t.Errorf("任期不匹配，期望 %d，实际 %d", req.Term, resp.Term)
	}

	// 验证心跳消息是否被正确处理
	if len(handler2.messages) != 1 {
		t.Fatalf("期望收到1条心跳消息，实际收到%d条", len(handler2.messages))
	}

	msg := handler2.messages[0]
	if msg.Type != raftpb.MsgHeartbeat {
		t.Error("消息类型应该是心跳")
	}
	if msg.From != req.From {
		t.Errorf("消息来源不匹配，期望 %d，实际 %d", req.From, msg.From)
	}
	if msg.To != req.To {
		t.Errorf("消息目标不匹配，期望 %d，实际 %d", req.To, msg.To)
	}
	if msg.Term != req.Term {
		t.Errorf("消息任期不匹配，期望 %d，实际 %d", req.Term, msg.Term)
	}

	t.Log("测试完成")
}

// 测试添加对等节点
func TestAddPeer(t *testing.T) {
	port1, err := getFreePort()
	if err != nil {
		t.Fatalf("获取端口失败: %v", err)
	}
	port2, err := getFreePort()
	if err != nil {
		t.Fatalf("获取端口失败: %v", err)
	}

	handler1 := &testMessageHandler{}
	handler2 := &testMessageHandler{}

	transport1, err := NewTransport(1, fmt.Sprintf("localhost:%d", port1), handler1)
	if err != nil {
		t.Fatalf("创建传输实例1失败: %v", err)
	}
	defer transport1.Stop()

	transport2, err := NewTransport(2, fmt.Sprintf("localhost:%d", port2), handler2)
	if err != nil {
		t.Fatalf("创建传输实例2失败: %v", err)
	}
	defer transport2.Stop()

	// 添加对等节点
	err = transport1.AddPeer(2, fmt.Sprintf("localhost:%d", port2))
	if err != nil {
		t.Fatalf("添加对等节点失败: %v", err)
	}

	// 验证对等节点是否添加成功
	transport1.peersMu.RLock()
	addr, ok := transport1.peers[2]
	transport1.peersMu.RUnlock()

	if !ok {
		t.Error("对等节点未添加")
	}
	if addr != fmt.Sprintf("localhost:%d", port2) {
		t.Errorf("对等节点地址不匹配，期望 localhost:%d，实际 %s", port2, addr)
	}

	// 验证连接是否建立
	transport1.connsMu.RLock()
	_, ok = transport1.conns[2]
	transport1.connsMu.RUnlock()

	if !ok {
		t.Error("连接未建立")
	}
}

// 测试基本消息发送
func TestBasicSend(t *testing.T) {
	t.Log("开始测试")
	port1, err := getFreePort()
	if err != nil {
		t.Fatalf("获取端口失败: %v", err)
	}
	port2, err := getFreePort()
	if err != nil {
		t.Fatalf("获取端口失败: %v", err)
	}
	t.Logf("获取到端口: port1=%d, port2=%d", port1, port2)

	handler1 := &testMessageHandler{}
	handler2 := &testMessageHandler{}

	t.Log("创建传输实例1")
	transport1, err := NewTransport(1, fmt.Sprintf("localhost:%d", port1), handler1)
	if err != nil {
		t.Fatalf("创建传输实例1失败: %v", err)
	}
	defer transport1.Stop()

	t.Log("创建传输实例2")
	transport2, err := NewTransport(2, fmt.Sprintf("localhost:%d", port2), handler2)
	if err != nil {
		t.Fatalf("创建传输实例2失败: %v", err)
	}
	defer transport2.Stop()

	t.Log("添加对等节点")
	err = transport1.AddPeer(2, fmt.Sprintf("localhost:%d", port2))
	if err != nil {
		t.Fatalf("添加对等节点失败: %v", err)
	}

	t.Log("等待连接建立")
	time.Sleep(100 * time.Millisecond)

	t.Log("准备发送测试消息")
	testMsg := raftpb.Message{
		Type: raftpb.MsgHeartbeat,
		To:   2,
		From: 1,
		Term: 1,
	}

	t.Log("发送消息")
	err = transport1.Send(testMsg)
	if err != nil {
		t.Fatalf("发送消息失败: %v", err)
	}

	t.Log("等待消息处理")
	time.Sleep(100 * time.Millisecond)

	t.Log("验证消息接收")
	if len(handler2.messages) != 1 {
		t.Fatalf("期望收到1条消息，实际收到%d条", len(handler2.messages))
	}

	receivedMsg := handler2.messages[0]
	if receivedMsg.Type != testMsg.Type ||
		receivedMsg.To != testMsg.To ||
		receivedMsg.From != testMsg.From ||
		receivedMsg.Term != testMsg.Term {
		t.Error("收到的消息与发送的消息不匹配")
	}
	t.Log("测试完成")
}

// 测试重连机制
func TestReconnection(t *testing.T) {
	t.Log("开始测试")
	port1, err := getFreePort()
	if err != nil {
		t.Fatalf("获取端口失败: %v", err)
	}
	port2, err := getFreePort()
	if err != nil {
		t.Fatalf("获取端口失败: %v", err)
	}
	t.Logf("获取到端口: port1=%d, port2=%d", port1, port2)

	handler1 := &testMessageHandler{}
	handler2 := &testMessageHandler{}

	t.Log("创建传输实例1")
	transport1, err := NewTransport(1, fmt.Sprintf("localhost:%d", port1), handler1)
	if err != nil {
		t.Fatalf("创建传输实例1失败: %v", err)
	}
	defer transport1.Stop()

	t.Log("创建传输实例2")
	transport2, err := NewTransport(2, fmt.Sprintf("localhost:%d", port2), handler2)
	if err != nil {
		t.Fatalf("创建传输实例2失败: %v", err)
	}

	t.Log("添加对等节点")
	err = transport1.AddPeer(2, fmt.Sprintf("localhost:%d", port2))
	if err != nil {
		t.Fatalf("添加对等节点失败: %v", err)
	}

	t.Log("等待连接建立")
	time.Sleep(100 * time.Millisecond)

	t.Log("发送第一条消息")
	msg1 := raftpb.Message{
		Type: raftpb.MsgHeartbeat,
		To:   2,
		From: 1,
		Term: 1,
	}
	err = transport1.Send(msg1)
	if err != nil {
		t.Fatalf("发送第一条消息失败: %v", err)
	}

	t.Log("等待消息处理")
	time.Sleep(100 * time.Millisecond)

	t.Log("验证第一条消息")
	if len(handler2.messages) != 1 {
		t.Fatalf("期望收到1条消息，实际收到%d条", len(handler2.messages))
	}

	t.Log("停止节点2")
	transport2.Stop()

	t.Log("尝试发送消息到已停止的节点")
	msg2 := raftpb.Message{
		Type: raftpb.MsgHeartbeat,
		To:   2,
		From: 1,
		Term: 2,
	}
	err = transport1.Send(msg2)
	if err == nil {
		t.Error("向已停止的节点发送消息应该失败")
	}

	t.Log("重启节点2")
	transport2, err = NewTransport(2, fmt.Sprintf("localhost:%d", port2), handler2)
	if err != nil {
		t.Fatalf("重启节点2失败: %v", err)
	}
	defer transport2.Stop()

	t.Log("等待重连")
	time.Sleep(200 * time.Millisecond)

	t.Log("发送第三条消息")
	msg3 := raftpb.Message{
		Type: raftpb.MsgHeartbeat,
		To:   2,
		From: 1,
		Term: 3,
	}
	err = transport1.Send(msg3)
	if err != nil {
		t.Fatalf("重连后发送消息失败: %v", err)
	}

	t.Log("等待消息处理")
	time.Sleep(100 * time.Millisecond)

	t.Log("验证重连后的消息")
	if len(handler2.messages) != 2 {
		t.Fatalf("期望总共收到2条消息，实际收到%d条", len(handler2.messages))
	}

	lastMsg := handler2.messages[len(handler2.messages)-1]
	if lastMsg.Term != 3 {
		t.Errorf("最后一条消息的任期不匹配，期望3，实际%d", lastMsg.Term)
	}

	t.Log("测试完成")
}
