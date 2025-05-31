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
	"google.golang.org/grpc/credentials/insecure"
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
}

// NewTransport 创建新的传输实例
func NewTransport(nodeID uint64, addr string, handler MessageHandler) (*Transport, error) {
	ctx, cancel := context.WithCancel(context.Background())

	t := &Transport{
		nodeID:      nodeID,
		peers:       make(map[uint64]string),
		conns:       make(map[uint64]*grpc.ClientConn),
		streams:     make(map[uint64]RaftService_StreamMessageClient),
		handler:     handler,
		ctx:         ctx,
		cancel:      cancel,
		reconnect:   make(chan uint64, 100),
		snapshots:   make(map[uint64]*snapshotState),
		chunkSize:   1024 * 1024, // 1MB
		maxChunkNum: 1000,        // 最大1000个块
	}

	// 启动 gRPC 服务器
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("监听地址失败: %v", err)
	}

	// 设置消息大小限制
	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(16 * 1024 * 1024), // 16MB
		grpc.MaxSendMsgSize(16 * 1024 * 1024), // 16MB
	}

	server := grpc.NewServer(opts...)
	RegisterRaftServiceServer(server, t)
	t.server = server
	t.listener = lis

	go t.serve()
	go t.reconnectLoop() // 启动重连循环

	return t, nil
}

// AddPeer 添加对等节点
func (t *Transport) AddPeer(id uint64, addr string) error {
	t.peersMu.Lock()
	defer t.peersMu.Unlock()

	if _, exists := t.peers[id]; exists {
		return fmt.Errorf("节点 %d 已存在", id)
	}

	t.peers[id] = addr
	return t.dialPeer(id, addr)
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
	if msg.To == t.nodeID {
		return t.handler.HandleMessage(msg)
	}

	// 转换为内部消息格式
	req := &RaftMessageRequest{
		Message:  t.convertToTransportMessage(msg),
		Sequence: atomic.AddUint64(&t.sequence, 1),
	}

	// 尝试使用流式传输
	if stream, ok := t.getStream(msg.To); ok {
		err := t.sendStream(stream, req)
		if err != nil {
			// 如果发送失败，尝试重连
			t.peersMu.RLock()
			addr, ok := t.peers[msg.To]
			t.peersMu.RUnlock()
			if ok {
				if dialErr := t.dialPeer(msg.To, addr); dialErr == nil {
					// 重连成功，重试发送
					if newStream, ok := t.getStream(msg.To); ok {
						return t.sendStream(newStream, req)
					}
				}
			}
			return fmt.Errorf("发送消息失败: %v", err)
		}
		return nil
	}

	// 回退到普通 RPC
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
			Term:      req.Term,
			ConnState: ConnectionState_Failed,
		}, nil
	}

	return &HeartbeatResponse{
		Success:   true,
		Term:      req.Term,
		ConnState: ConnectionState_Connected,
	}, nil
}

// IsEmptySnapshot 检查快照是否为空
func IsEmptySnapshot(snap raftpb.Snapshot) bool {
	return snap.Metadata.Index == 0
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
	t.connsMu.RLock()
	conn, ok := t.conns[nodeID]
	t.connsMu.RUnlock()

	if !ok {
		return fmt.Errorf("节点 %d 未连接", nodeID)
	}

	ctx, cancel := context.WithTimeout(t.ctx, defaultSendTimeout)
	defer cancel()

	client := NewRaftServiceClient(conn)
	resp, err := client.SendMessage(ctx, req)
	if err != nil {
		return fmt.Errorf("发送消息失败: %v", err)
	}

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
	for {
		select {
		case <-t.ctx.Done():
			return
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
				// 延迟后重试
				time.Sleep(time.Second)
				select {
				case t.reconnect <- nodeID:
				default:
				}
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
