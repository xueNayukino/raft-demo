package transport

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	defaultDialTimeout = 5 * time.Second
	defaultSendTimeout = 3 * time.Second
)

// Transport 实现了基于 gRPC 的节点间通信
type Transport struct {
	// 节点标识
	nodeID  uint64
	peers   map[uint64]string // nodeID -> address
	peersMu sync.RWMutex

	// gRPC 服务器
	server   *grpc.Server
	listener net.Listener

	// gRPC 客户端连接池
	conns   map[uint64]*grpc.ClientConn
	connsMu sync.RWMutex

	// 消息处理器
	handler MessageHandler

	// 上下文控制
	ctx    context.Context
	cancel context.CancelFunc

	// 流式消息管理
	streams   map[uint64]RaftService_StreamMessageClient
	streamMu  sync.RWMutex
	reconnect chan uint64 // 重连通道
	sequence  uint64      // 消息序列号

	// 快照传输管理
	snapshots   map[uint64]*snapshotState
	snapshotMu  sync.RWMutex
	chunkSize   int
	maxChunkNum uint64

	// 离线节点管理
	offlineNodes      map[uint64]time.Time // 记录节点离线时间
	offlineNodesMu    sync.RWMutex
	failureCounter    map[uint64]int // 记录连续失败次数
	failureCounterMu  sync.RWMutex
	offlineTimeout    time.Duration // 节点被标记为离线的超时时间
	reconnectInterval time.Duration // 重连间隔

	// 必须嵌入未实现的服务器
	UnimplementedRaftServiceServer
}

// snapshotState 管理快照传输状态
type snapshotState struct {
	snapshot    *Snapshot
	chunkSize   int
	currentSize int
	chunks      map[uint64][]byte
	metadata    *SnapshotMetadata
	done        chan struct{}
	fromNode    uint64 // 添加源节点ID字段
}

// MessageHandler 定义了消息处理接口
type MessageHandler interface {
	HandleMessage(msg raftpb.Message) error
	GetTerm() uint64
	GetLeaderID() uint64
	ProposeConfChange(ctx context.Context, cc raftpb.ConfChange) error
}

// NewTransport 创建新的传输实例
func NewTransport(nodeID uint64, addr string, handler MessageHandler) (*Transport, error) {
	ctx, cancel := context.WithCancel(context.Background())

	fmt.Printf("[Transport] 节点 %d 启动传输层，监听地址: %s\n", nodeID, addr)

	t := &Transport{
		nodeID:            nodeID,
		peers:             make(map[uint64]string),
		conns:             make(map[uint64]*grpc.ClientConn),
		streams:           make(map[uint64]RaftService_StreamMessageClient),
		handler:           handler,
		ctx:               ctx,
		cancel:            cancel,
		reconnect:         make(chan uint64, 100),
		snapshots:         make(map[uint64]*snapshotState),
		chunkSize:         1024 * 1024, // 1MB
		maxChunkNum:       1000,        // 最大1000个块
		offlineNodes:      make(map[uint64]time.Time),
		failureCounter:    make(map[uint64]int),
		offlineTimeout:    30 * time.Second, // 默认30秒离线超时
		reconnectInterval: 5 * time.Second,  // 默认5秒重连间隔
	}

	// 启动 gRPC 服务器
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("监听地址失败: %v", err)
	}
	// 移除调试输出
	// fmt.Printf("[Transport] 节点 %d 成功监听地址 %s\n", nodeID, addr)

	// 设置消息大小限制
	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(16 * 1024 * 1024), // 16MB
		grpc.MaxSendMsgSize(16 * 1024 * 1024), // 16MB
	}

	server := grpc.NewServer(opts...)
	RegisterRaftServiceServer(server, t)
	t.server = server
	t.listener = lis

	// 启动 gRPC 服务器和重连循环
	go t.serve()
	go t.reconnectLoop()   // 启动重连循环
	go t.healthCheckLoop() // 启动健康检查循环

	return t, nil
}

// AddPeer 添加对等节点
func (t *Transport) AddPeer(id uint64, addr string) error {
	t.peersMu.Lock()
	defer t.peersMu.Unlock()

	// 移除调试输出
	// fmt.Printf("[Transport] 节点 %d 尝试连接到对等节点 %d，地址: %s\n", t.nodeID, id, addr)

	if _, exists := t.peers[id]; exists {
		return fmt.Errorf("节点 %d 已存在", id)
	}

	t.peers[id] = addr
	err := t.dialPeer(id, addr)
	if err != nil {
		// 移除调试输出
		// fmt.Printf("[Transport] 节点 %d 连接对等节点 %d 失败: %v\n", t.nodeID, id, err)
		return err
	}

	// 移除调试输出
	// fmt.Printf("[Transport] 节点 %d 成功连接到对等节点 %d\n", t.nodeID, id)
	return nil
}

// RemovePeer 移除对等节点
func (t *Transport) RemovePeer(id uint64) {
	t.peersMu.Lock()
	defer t.peersMu.Unlock()

	delete(t.peers, id)

	t.connsMu.Lock()
	if conn, ok := t.conns[id]; ok {
		conn.Close()
		delete(t.conns, id)
	}
	t.connsMu.Unlock()

	t.streamMu.Lock()
	delete(t.streams, id)
	t.streamMu.Unlock()

	// 清理该节点的快照状态
	t.snapshotMu.Lock()
	for sid, snap := range t.snapshots {
		if snap.fromNode == id {
			delete(t.snapshots, sid)
		}
	}
	t.snapshotMu.Unlock()
}

// Send 发送消息到目标节点
func (t *Transport) Send(msg raftpb.Message) error {
	// 检查消息有效性
	if msg.To == 0 {
		return fmt.Errorf("无效的目标节点ID: %d", msg.To)
	}

	// 记录关键消息类型
	// 移除调试输出
	// if msg.Type == raftpb.MsgHeartbeat || msg.Type == raftpb.MsgVote || msg.Type == raftpb.MsgPreVote {
	//     fmt.Printf("[Transport] 节点 %d 发送 %v 消息到节点 %d (Term=%d)\n",
	//         t.nodeID, msg.Type, msg.To, msg.Term)
	// }

	// 转换为内部消息格式
	req := &RaftMessageRequest{
		Message:  t.convertToTransportMessage(msg),
		Sequence: atomic.AddUint64(&t.sequence, 1),
	}

	// 发送消息
	return t.sendRPC(msg.To, req)
}

// Stop 停止传输服务
func (t *Transport) Stop() {
	t.cancel()

	// 关闭所有连接
	t.connsMu.Lock()
	for _, conn := range t.conns {
		conn.Close()
	}
	t.connsMu.Unlock()

	// 停止服务器
	if t.server != nil {
		t.server.GracefulStop()
	}
	if t.listener != nil {
		t.listener.Close()
	}
}

// GetPeerAddrs 返回所有节点ID到地址的映射
func (t *Transport) GetPeerAddrs() map[uint64]string {
	t.peersMu.RLock()
	defer t.peersMu.RUnlock()

	// 创建一个副本以避免并发修改
	result := make(map[uint64]string)

	// 复制所有peer地址
	for id, addr := range t.peers {
		result[id] = addr
	}

	// 添加自己的地址（如果不存在）
	if _, exists := result[t.nodeID]; !exists && t.listener != nil {
		// 使用监听地址作为自己的地址
		result[t.nodeID] = t.listener.Addr().String()
	}

	return result
}

// 实现 RaftServiceServer 接口
func (t *Transport) SendMessage(ctx context.Context, req *RaftMessageRequest) (*RaftMessageResponse, error) {
	msg := t.convertFromTransportMessage(req.Message)
	err := t.handler.HandleMessage(msg)
	if err != nil {
		return &RaftMessageResponse{Success: false, Error: err.Error()}, nil
	}
	return &RaftMessageResponse{Success: true}, nil
}

func (t *Transport) StreamMessage(stream RaftService_StreamMessageServer) error {
	var nodeID uint64
	firstMsg := true
	recvChan := make(chan *RaftMessageRequest)
	errChan := make(chan error)

	// 启动接收消息的 goroutine
	go func() {
		for {
			req, err := stream.Recv()
			if err != nil {
				errChan <- err
				return
			}
			recvChan <- req
		}
	}()

	for {
		select {
		case <-t.ctx.Done():
			return nil
		case err := <-errChan:
			if err == io.EOF {
				return nil
			}
			if nodeID != 0 {
				select {
				case t.reconnect <- nodeID:
				default:
				}
			}
			return err
		case req := <-recvChan:
			if firstMsg {
				nodeID = req.Message.From
				firstMsg = false
			}

			msg := t.convertFromTransportMessage(req.Message)
			err := t.handler.HandleMessage(msg)

			resp := &RaftMessageResponse{
				Success:  err == nil,
				Sequence: req.Sequence, // 包含原始序列号
			}
			if err != nil {
				resp.Error = err.Error()
				resp.ConnState = ConnectionState_Failed
				resp.StreamState = StreamState_StreamError
			} else {
				resp.ConnState = ConnectionState_Connected
				resp.StreamState = StreamState_StreamActive
			}

			// 异步发送响应
			go func(r *RaftMessageResponse) {
				if err := stream.Send(r); err != nil {
					log.Printf("发送响应失败: %v", err)
				}
			}(resp)
		}
	}
}

// SendSnapshot 实现 RaftServiceServer 接口的快照发送方法
func (t *Transport) SendSnapshot(stream RaftService_SendSnapshotServer) error {
	var (
		currentSnapshot *snapshotState
		snapshotID      uint64
	)

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			if currentSnapshot != nil {
				close(currentSnapshot.done)
			}
			return nil
		}
		if err != nil {
			return err
		}

		if currentSnapshot == nil {
			// 新的快照传输
			snapshotID = chunk.SnapshotId
			currentSnapshot = &snapshotState{
				snapshot: &Snapshot{
					Data:       make([]byte, 0), // 不预分配内存
					Metadata:   chunk.Metadata,
					SnapshotId: snapshotID,
				},
				chunks:   make(map[uint64][]byte),
				done:     make(chan struct{}),
				metadata: chunk.Metadata,
				fromNode: chunk.Metadata.ConfState.Voters[0], // 使用第一个voter作为源节点ID
			}
			t.snapshotMu.Lock()
			t.snapshots[snapshotID] = currentSnapshot
			t.snapshotMu.Unlock()
		}

		// 保存块数据
		currentSnapshot.chunks[chunk.ChunkId] = chunk.Data
		currentSnapshot.currentSize += len(chunk.Data)

		// 发送响应
		resp := &SnapshotResponse{
			Success:    true,
			ChunkId:    chunk.ChunkId,
			SnapshotId: snapshotID,
		}

		// 异步发送响应
		go func(r *SnapshotResponse) {
			if err := stream.Send(r); err != nil {
				log.Printf("发送快照响应失败: %v", err)
			}
		}(resp)

		// 检查是否接收完成
		if uint64(len(currentSnapshot.chunks)) == chunk.TotalChunks {
			// 流式处理块数据
			for i := uint64(0); i < chunk.TotalChunks; i++ {
				if err := t.handler.HandleMessage(raftpb.Message{
					Type: raftpb.MsgSnap,
					From: currentSnapshot.fromNode,
					To:   t.nodeID,
					Snapshot: raftpb.Snapshot{
						Data: currentSnapshot.chunks[i],
						Metadata: raftpb.SnapshotMetadata{
							Index: currentSnapshot.metadata.Index,
							Term:  currentSnapshot.metadata.Term,
							ConfState: raftpb.ConfState{
								Voters:   currentSnapshot.metadata.ConfState.Voters,
								Learners: currentSnapshot.metadata.ConfState.Learners,
							},
						},
					},
				}); err != nil {
					return fmt.Errorf("应用快照块失败: %v", err)
				}
				// 处理完后立即释放内存
				delete(currentSnapshot.chunks, i)
			}

			// 清理快照状态
			t.snapshotMu.Lock()
			delete(t.snapshots, snapshotID)
			t.snapshotMu.Unlock()

			return nil
		}
	}
}

// Heartbeat 实现 RaftServiceServer 接口的心跳方法
func (t *Transport) Heartbeat(ctx context.Context, req *HeartbeatRequest) (*HeartbeatResponse, error) {
	// 获取当前节点的Term
	var currentTerm uint64 = 0
	if t.handler != nil {
		// 尝试通过handler获取当前Term
		if termGetter, ok := t.handler.(interface{ GetTerm() uint64 }); ok {
			currentTerm = termGetter.GetTerm()
		}
	}

	// 如果收到的Term比自己的小，拒绝心跳
	if req.Term > 0 && currentTerm > req.Term {
		return &HeartbeatResponse{
			Success:   false,
			Error:     "Term过期",
			Term:      currentTerm,
			ConnState: ConnectionState_Connected,
		}, nil
	}

	// 如果收到的Term比自己的大，接受并更新自己的Term
	if req.Term > 0 && req.Term > currentTerm {
		// 创建MsgHeartbeat消息，包含较高的Term
		msg := raftpb.Message{
			Type: raftpb.MsgHeartbeat,
			To:   t.nodeID,
			From: req.From,
			Term: req.Term,
		}

		err := t.handler.HandleMessage(msg)
		if err != nil {
			return &HeartbeatResponse{
				Success:   false,
				Error:     err.Error(),
				Term:      currentTerm,
				ConnState: ConnectionState_Failed,
			}, nil
		}

		return &HeartbeatResponse{
			Success:   true,
			Term:      req.Term,
			ConnState: ConnectionState_Connected,
		}, nil
	}

	// 处理普通心跳
	msg := raftpb.Message{
		Type: raftpb.MsgHeartbeat,
		To:   t.nodeID,
		From: req.From,
		Term: req.Term,
	}

	err := t.handler.HandleMessage(msg)
	if err != nil {
		return &HeartbeatResponse{
			Success:   false,
			Error:     err.Error(),
			Term:      currentTerm,
			ConnState: ConnectionState_Failed,
		}, nil
	}

	return &HeartbeatResponse{
		Success:   true,
		Term:      currentTerm,
		ConnState: ConnectionState_Connected,
	}, nil
}

// IsEmptySnapshot 检查快照是否为空
func IsEmptySnapshot(snap raftpb.Snapshot) bool {
	return snap.Metadata.Index == 0
}

// IsPeerConnected 检查指定节点的连接状态
func (t *Transport) IsPeerConnected(nodeID uint64) bool {
	// 检查节点是否被标记为离线
	t.offlineNodesMu.RLock()
	offlineTime, isOffline := t.offlineNodes[nodeID]
	t.offlineNodesMu.RUnlock()

	// 如果节点已标记为离线且离线时间不超过超时时间，直接返回false
	if isOffline {
		// 检查离线时间是否超过了重连时间
		if time.Since(offlineTime) < t.offlineTimeout {
			return false
		}
		// 超过离线时间，尝试重连
		t.offlineNodesMu.Lock()
		delete(t.offlineNodes, nodeID)
		t.offlineNodesMu.Unlock()

		t.failureCounterMu.Lock()
		delete(t.failureCounter, nodeID)
		t.failureCounterMu.Unlock()

		// 触发重连
		select {
		case t.reconnect <- nodeID:
		default:
		}
	}

	// 先检查对等节点是否在配置中
	t.peersMu.RLock()
	_, peerExists := t.peers[nodeID]
	t.peersMu.RUnlock()
	if !peerExists {
		return false
	}

	// 检查gRPC连接是否建立
	t.connsMu.RLock()
	conn, connExists := t.conns[nodeID]
	t.connsMu.RUnlock()
	if !connExists || conn == nil {
		return false
	}

	// 检查连接状态
	state := conn.GetState()
	if state == connectivity.Shutdown || state == connectivity.TransientFailure {
		return false
	}

	// 检查心跳状态
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	client := NewRaftServiceClient(conn)
	_, err := client.Heartbeat(ctx, &HeartbeatRequest{
		From: t.nodeID,
		To:   nodeID,
	})

	if err != nil {
		// 连接测试失败，增加失败计数
		t.incrementFailureCount(nodeID)
		return false
	}

	// 重置失败计数
	t.resetFailureCount(nodeID)
	return true
}

// incrementFailureCount 增加节点的失败计数，达到阈值时标记为离线
func (t *Transport) incrementFailureCount(nodeID uint64) {
	t.failureCounterMu.Lock()
	defer t.failureCounterMu.Unlock()

	t.failureCounter[nodeID]++

	// 达到阈值（3次连续失败）时标记为离线
	if t.failureCounter[nodeID] >= 3 {
		t.offlineNodesMu.Lock()
		t.offlineNodes[nodeID] = time.Now()
		t.offlineNodesMu.Unlock()

		log.Printf("节点 %d 被标记为离线，连续失败 %d 次", nodeID, t.failureCounter[nodeID])

		// 关闭连接
		t.connsMu.Lock()
		if conn, exists := t.conns[nodeID]; exists && conn != nil {
			conn.Close()
			delete(t.conns, nodeID)
		}
		t.connsMu.Unlock()

		// 关闭流
		t.streamMu.Lock()
		if stream, exists := t.streams[nodeID]; exists && stream != nil {
			stream.CloseSend()
			delete(t.streams, nodeID)
		}
		t.streamMu.Unlock()
	}
}

// resetFailureCount 重置节点的失败计数
func (t *Transport) resetFailureCount(nodeID uint64) {
	t.failureCounterMu.Lock()
	defer t.failureCounterMu.Unlock()

	delete(t.failureCounter, nodeID)

	// 如果节点之前被标记为离线，现在移除标记
	t.offlineNodesMu.Lock()
	if _, exists := t.offlineNodes[nodeID]; exists {
		delete(t.offlineNodes, nodeID)
		log.Printf("节点 %d 重新上线", nodeID)
	}
	t.offlineNodesMu.Unlock()
}

// 内部辅助方法

func (t *Transport) serve() {
	if err := t.server.Serve(t.listener); err != nil {
		log.Printf("gRPC 服务器错误: %v", err)
	}
}

func (t *Transport) dialPeer(id uint64, addr string) error {
	ctx, cancel := context.WithTimeout(t.ctx, defaultDialTimeout)
	defer cancel()

	// 设置客户端消息大小限制
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(16*1024*1024), // 16MB
			grpc.MaxCallSendMsgSize(16*1024*1024), // 16MB
		),
	}

	conn, err := grpc.DialContext(ctx, addr, opts...)
	if err != nil {
		return fmt.Errorf("连接节点 %d 失败: %v", id, err)
	}

	t.connsMu.Lock()
	if oldConn, ok := t.conns[id]; ok {
		oldConn.Close()
	}
	t.conns[id] = conn
	t.connsMu.Unlock()

	// 建立流式连接
	client := NewRaftServiceClient(conn)
	stream, err := client.StreamMessage(t.ctx)
	if err != nil {
		return fmt.Errorf("建立流式连接失败: %v", err)
	}

	t.streamMu.Lock()
	if oldStream, ok := t.streams[id]; ok {
		if oldStream != nil {
			oldStream.CloseSend()
		}
	}
	t.streams[id] = stream
	t.streamMu.Unlock()

	return nil
}

func (t *Transport) getStream(nodeID uint64) (RaftService_StreamMessageClient, bool) {
	t.streamMu.RLock()
	defer t.streamMu.RUnlock()
	stream, ok := t.streams[nodeID]
	return stream, ok
}

func (t *Transport) sendStream(stream RaftService_StreamMessageClient, req *RaftMessageRequest) error {
	if err := stream.Send(req); err != nil {
		return fmt.Errorf("发送流式消息失败: %v", err)
	}

	resp, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("接收流式响应失败: %v", err)
	}

	if !resp.Success {
		return fmt.Errorf("消息处理失败: %s", resp.Error)
	}

	return nil
}

func (t *Transport) sendRPC(nodeID uint64, req *RaftMessageRequest) error {
	// 检查节点是否被标记为离线
	t.offlineNodesMu.RLock()
	offlineTime, isOffline := t.offlineNodes[nodeID]
	t.offlineNodesMu.RUnlock()

	if isOffline {
		// 检查是否超过离线时间，尝试重连
		if time.Since(offlineTime) > t.reconnectInterval {
			// 触发重连
			select {
			case t.reconnect <- nodeID:
				// 已触发重连
			default:
				// 重连通道已满
			}
		}

		// 对选举相关消息，返回特殊错误减少日志输出
		if MessageType(req.Message.Type) == MessageType_MsgPreVote ||
			MessageType(req.Message.Type) == MessageType_MsgVote {
			return fmt.Errorf("节点 %d 当前离线", nodeID)
		}
		return fmt.Errorf("节点 %d 当前离线，已加入重连队列", nodeID)
	}

	t.connsMu.RLock()
	conn, ok := t.conns[nodeID]
	t.connsMu.RUnlock()

	if !ok {
		return fmt.Errorf("节点 %d 未连接", nodeID)
	}

	// 检查连接状态
	state := conn.GetState()
	if state == connectivity.Shutdown || state == connectivity.TransientFailure {
		// 尝试重新连接
		t.peersMu.RLock()
		_, exists := t.peers[nodeID]
		t.peersMu.RUnlock()

		if !exists {
			return fmt.Errorf("节点 %d 不在集群中", nodeID)
		}

		// 使用非阻塞方式触发重连
		select {
		case t.reconnect <- nodeID:
			// 成功触发重连
		default:
			// 重连通道已满，忽略
		}

		// 增加失败计数
		t.incrementFailureCount(nodeID)

		// 对于投票请求消息，返回特殊错误以减少日志输出
		if MessageType(req.Message.Type) == MessageType_MsgPreVote ||
			MessageType(req.Message.Type) == MessageType_MsgVote {
			return fmt.Errorf("节点 %d 当前不可用", nodeID)
		}

		return fmt.Errorf("节点 %d 连接失败，已触发重连", nodeID)
	}

	ctx, cancel := context.WithTimeout(t.ctx, defaultSendTimeout)
	defer cancel()

	client := NewRaftServiceClient(conn)
	resp, err := client.SendMessage(ctx, req)

	// 处理连接错误
	if err != nil {
		// 增加失败计数
		t.incrementFailureCount(nodeID)

		// 检查是否是连接被拒绝的错误
		if grpcErr, ok := err.(interface{ GRPCStatus() *status.Status }); ok {
			errStatus := grpcErr.GRPCStatus()
			if errStatus.Code() == codes.Unavailable {
				// 连接不可用，标记为不可用状态并触发重连
				t.connsMu.Lock()
				if oldConn, exists := t.conns[nodeID]; exists && oldConn == conn {
					// 关闭旧连接
					conn.Close()
					delete(t.conns, nodeID)
				}
				t.connsMu.Unlock()

				// 异步触发重连
				select {
				case t.reconnect <- nodeID:
					// 成功触发重连
				default:
					// 重连通道已满，忽略
				}

				// 对于选举相关消息，减少错误日志输出
				if MessageType(req.Message.Type) == MessageType_MsgPreVote ||
					MessageType(req.Message.Type) == MessageType_MsgVote {
					return fmt.Errorf("节点 %d 连接失败", nodeID)
				}
			}
		}
		return fmt.Errorf("发送消息失败: %v", err)
	}

	// 成功发送，重置失败计数
	t.resetFailureCount(nodeID)

	if !resp.Success {
		return fmt.Errorf("消息处理失败: %s", resp.Error)
	}

	return nil
}

// 消息转换方法

func (t *Transport) convertToTransportMessage(msg raftpb.Message) *Message {
	entries := make([]*Entry, len(msg.Entries))
	for i, e := range msg.Entries {
		entries[i] = &Entry{
			Term:  e.Term,
			Index: e.Index,
			Type:  EntryType(e.Type),
			Data:  e.Data,
		}
	}

	var snapshot *Snapshot
	if !IsEmptySnapshot(msg.Snapshot) {
		snapshot = &Snapshot{
			Data: msg.Snapshot.Data,
			Metadata: &SnapshotMetadata{
				ConfState: &ConfState{
					Voters:   msg.Snapshot.Metadata.ConfState.Voters,
					Learners: msg.Snapshot.Metadata.ConfState.Learners,
				},
				Index: msg.Snapshot.Metadata.Index,
				Term:  msg.Snapshot.Metadata.Term,
			},
		}
	}

	return &Message{
		Type:       MessageType(msg.Type),
		To:         msg.To,
		From:       msg.From,
		Term:       msg.Term,
		LogTerm:    msg.LogTerm,
		Index:      msg.Index,
		Entries:    entries,
		Commit:     msg.Commit,
		Snapshot:   snapshot,
		Reject:     msg.Reject,
		RejectHint: msg.RejectHint,
		Context:    msg.Context,
	}
}

func (t *Transport) convertFromTransportMessage(msg *Message) raftpb.Message {
	entries := make([]raftpb.Entry, len(msg.Entries))
	for i, e := range msg.Entries {
		entries[i] = raftpb.Entry{
			Term:  e.Term,
			Index: e.Index,
			Type:  raftpb.EntryType(e.Type),
			Data:  e.Data,
		}
	}

	var snapshot raftpb.Snapshot
	if msg.Snapshot != nil {
		snapshot = raftpb.Snapshot{
			Data: msg.Snapshot.Data,
			Metadata: raftpb.SnapshotMetadata{
				ConfState: raftpb.ConfState{
					Voters:   msg.Snapshot.Metadata.ConfState.Voters,
					Learners: msg.Snapshot.Metadata.ConfState.Learners,
				},
				Index: msg.Snapshot.Metadata.Index,
				Term:  msg.Snapshot.Metadata.Term,
			},
		}
	}

	return raftpb.Message{
		Type:       raftpb.MessageType(msg.Type),
		To:         msg.To,
		From:       msg.From,
		Term:       msg.Term,
		LogTerm:    msg.LogTerm,
		Index:      msg.Index,
		Entries:    entries,
		Commit:     msg.Commit,
		Snapshot:   snapshot,
		Reject:     msg.Reject,
		RejectHint: msg.RejectHint,
		Context:    msg.Context,
	}
}

// reconnectLoop 处理流式连接的重连
func (t *Transport) reconnectLoop() {
	// 定期检查离线节点
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-t.ctx.Done():
			return

		case <-ticker.C:
			// 检查离线节点，尝试重连
			t.offlineNodesMu.Lock()
			now := time.Now()
			for nodeID, offlineTime := range t.offlineNodes {
				// 超过离线超时时间的节点尝试重连
				if now.Sub(offlineTime) > t.reconnectInterval {
					// 使用非阻塞方式触发重连
					select {
					case t.reconnect <- nodeID:
						// 更新重连时间
						t.offlineNodes[nodeID] = now
					default:
						// 重连通道已满，下次再试
					}
				}
			}
			t.offlineNodesMu.Unlock()

		case nodeID := <-t.reconnect:
			t.peersMu.RLock()
			addr, ok := t.peers[nodeID]
			t.peersMu.RUnlock()
			if !ok {
				continue
			}

			// 重新建立连接
			if err := t.dialPeer(nodeID, addr); err != nil {
				log.Printf("重连节点 %d 失败: %v", nodeID, err)

				// 更新失败计数和离线状态
				t.incrementFailureCount(nodeID)

				// 等待一段时间后再重试
				go func(id uint64) {
					time.Sleep(t.reconnectInterval)
					select {
					case <-t.ctx.Done():
						return
					case t.reconnect <- id:
						// 已触发重连
					default:
						// 重连通道已满，忽略
					}
				}(nodeID)
			} else {
				// 连接成功，重置失败计数和离线状态
				t.resetFailureCount(nodeID)
				log.Printf("成功重连节点 %d", nodeID)
			}
		}
	}
}

// convertToRaftSnapshot 将内部快照转换为 raft 快照
func (s *Snapshot) convertToRaftSnapshot() *raftpb.Snapshot {
	return &raftpb.Snapshot{
		Data: s.Data,
		Metadata: raftpb.SnapshotMetadata{
			Index: s.Metadata.Index,
			Term:  s.Metadata.Term,
			ConfState: raftpb.ConfState{
				Voters:   s.Metadata.ConfState.Voters,
				Learners: s.Metadata.ConfState.Learners,
			},
		},
	}
}

// healthCheckLoop 定期检查所有节点的健康状态
func (t *Transport) healthCheckLoop() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-t.ctx.Done():
			return
		case <-ticker.C:
			// 获取当前Term
			var currentTerm uint64 = 0
			if t.handler != nil {
				currentTerm = t.handler.GetTerm()
			}

			t.peersMu.RLock()
			peers := make([]uint64, 0, len(t.peers))
			for id := range t.peers {
				if id != t.nodeID { // 不检查自己
					peers = append(peers, id)
				}
			}
			t.peersMu.RUnlock()

			// 并行检查所有节点健康状态
			for _, id := range peers {
				go func(nodeID uint64, term uint64) {
					t.connsMu.RLock()
					conn, exists := t.conns[nodeID]
					t.connsMu.RUnlock()

					// 如果没有连接，尝试重连
					if !exists || conn == nil {
						select {
						case t.reconnect <- nodeID:
						default:
						}
						return
					}

					// 检查连接状态
					if conn.GetState() != connectivity.Ready {
						select {
						case t.reconnect <- nodeID:
						default:
						}
						return
					}

					// 发送心跳检查连接，包含当前Term
					ctx, cancel := context.WithTimeout(t.ctx, 500*time.Millisecond)
					defer cancel()

					client := NewRaftServiceClient(conn)
					resp, err := client.Heartbeat(ctx, &HeartbeatRequest{
						From: t.nodeID,
						To:   nodeID,
						Term: term, // 使用当前Term
					})

					if err != nil || (resp != nil && !resp.Success) {
						// 连接异常，增加失败计数
						t.incrementFailureCount(nodeID)
					} else {
						// 连接正常，重置失败计数
						t.resetFailureCount(nodeID)

						// 检查Term，如果收到的Term更高，通知处理程序
						if resp != nil && resp.Term > term {
							// 创建一个特殊的心跳响应消息
							msg := raftpb.Message{
								Type: raftpb.MsgHeartbeatResp,
								To:   t.nodeID,
								From: nodeID,
								Term: resp.Term,
							}

							// 异步处理，避免阻塞
							go func() {
								if err := t.handler.HandleMessage(msg); err != nil {
									log.Printf("处理更高Term的心跳响应失败: %v", err)
								}
							}()
						}
					}
				}(id, currentTerm)
			}
		}
	}
}

// GetConnection 获取到指定节点的gRPC连接
func (t *Transport) GetConnection(nodeID uint64) (*grpc.ClientConn, bool) {
	t.connsMu.RLock()
	defer t.connsMu.RUnlock()

	conn, ok := t.conns[nodeID]
	return conn, ok
}

// IsStarted 检查传输层是否已启动
func (t *Transport) IsStarted() bool {
	t.connsMu.RLock()
	defer t.connsMu.RUnlock()

	// 检查服务器和监听器是否已初始化
	if t.server == nil || t.listener == nil {
		return false
	}

	// 检查上下文是否已取消
	select {
	case <-t.ctx.Done():
		return false
	default:
		return true
	}
}

// LeaderInfo 包含领导者信息
type LeaderInfo struct {
	LeaderID uint64
	Term     uint64
}

// GetLeaderInfo 获取节点的领导者信息
func (t *Transport) GetLeaderInfo(ctx context.Context, req *GetLeaderInfoRequest) (*GetLeaderInfoResponse, error) {
	if t.handler == nil {
		return &GetLeaderInfoResponse{
			Success: false,
			Error:   "消息处理器未初始化",
		}, nil
	}

	leaderID := t.handler.GetLeaderID()
	term := t.handler.GetTerm()

	return &GetLeaderInfoResponse{
		LeaderId: leaderID,
		Term:     term,
		Success:  true,
	}, nil
}

// SendConfChange 发送配置变更请求
func (t *Transport) SendConfChange(ctx context.Context, req *ConfChangeRequest) (*ConfChangeResponse, error) {
	if t.handler == nil {
		return &ConfChangeResponse{
			Success: false,
			Error:   "消息处理器未初始化",
		}, nil
	}

	var cc raftpb.ConfChange
	if err := cc.Unmarshal(req.ConfChange); err != nil {
		return &ConfChangeResponse{
			Success: false,
			Error:   fmt.Sprintf("解析配置变更失败: %v", err),
		}, nil
	}

	if err := t.handler.ProposeConfChange(ctx, cc); err != nil {
		return &ConfChangeResponse{
			Success: false,
			Error:   fmt.Sprintf("提议配置变更失败: %v", err),
		}, nil
	}

	return &ConfChangeResponse{
		Success: true,
	}, nil
}

// GetLeaderInfoFromPeer 从对等节点获取领导者信息
func (t *Transport) GetLeaderInfoFromPeer(nodeID uint64) (*LeaderInfo, error) {
	t.connsMu.RLock()
	conn, ok := t.conns[nodeID]
	t.connsMu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("节点 %d 未连接", nodeID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultSendTimeout)
	defer cancel()

	client := NewRaftServiceClient(conn)
	resp, err := client.GetLeaderInfo(ctx, &GetLeaderInfoRequest{
		From: t.nodeID,
		To:   nodeID,
	})

	if err != nil {
		return nil, fmt.Errorf("获取领导者信息失败: %v", err)
	}

	if !resp.Success {
		return nil, fmt.Errorf("获取领导者信息失败: %s", resp.Error)
	}

	return &LeaderInfo{
		LeaderID: resp.LeaderId,
		Term:     resp.Term,
	}, nil
}

// SendConfChangeToPeer 向对等节点发送配置变更请求
func (t *Transport) SendConfChangeToPeer(nodeID uint64, cc raftpb.ConfChange) error {
	t.connsMu.RLock()
	conn, ok := t.conns[nodeID]
	t.connsMu.RUnlock()

	if !ok {
		return fmt.Errorf("节点 %d 未连接", nodeID)
	}

	// 序列化配置变更
	ccData, err := cc.Marshal()
	if err != nil {
		return fmt.Errorf("序列化配置变更失败: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultSendTimeout)
	defer cancel()

	client := NewRaftServiceClient(conn)
	resp, err := client.SendConfChange(ctx, &ConfChangeRequest{
		From:       t.nodeID,
		To:         nodeID,
		ConfChange: ccData,
	})

	if err != nil {
		return fmt.Errorf("发送配置变更请求失败: %v", err)
	}

	if !resp.Success {
		return fmt.Errorf("配置变更请求被拒绝: %s", resp.Error)
	}

	return nil
}

// raftServer 实现 RaftServiceServer 接口
type raftServer struct {
	UnimplementedRaftServiceServer
	node MessageHandler
}

// GetLeaderInfo 处理获取领导者信息的请求
func (s *raftServer) GetLeaderInfo(ctx context.Context, req *GetLeaderInfoRequest) (*GetLeaderInfoResponse, error) {
	// 获取当前领导者信息
	leaderID := s.node.GetLeaderID()
	term := s.node.GetTerm()

	return &GetLeaderInfoResponse{
		LeaderId: leaderID,
		Term:     term,
		Success:  true,
	}, nil
}

// SendConfChange 处理配置变更请求
func (s *raftServer) SendConfChange(ctx context.Context, req *ConfChangeRequest) (*ConfChangeResponse, error) {
	// 将字节转换为 ConfChange
	var cc raftpb.ConfChange
	if err := cc.Unmarshal(req.ConfChange); err != nil {
		return &ConfChangeResponse{
			Success: false,
			Error:   fmt.Sprintf("解析配置变更失败: %v", err),
		}, nil
	}

	// 提议配置变更
	if err := s.node.ProposeConfChange(ctx, cc); err != nil {
		return &ConfChangeResponse{
			Success: false,
			Error:   fmt.Sprintf("提议配置变更失败: %v", err),
		}, nil
	}

	return &ConfChangeResponse{
		Success: true,
	}, nil
}

// IsPeerOffline 检查节点是否被标记为离线
func (t *Transport) IsPeerOffline(nodeID uint64) bool {
	t.offlineNodesMu.RLock()
	_, isOffline := t.offlineNodes[nodeID]
	t.offlineNodesMu.RUnlock()
	return isOffline
}
