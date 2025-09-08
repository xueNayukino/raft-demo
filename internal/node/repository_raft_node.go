package node

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"

	"qklzl/internal/fsm"
	"qklzl/internal/repository"
)

// RepositoryRaftNode 基于仓库模式的Raft节点
type RepositoryRaftNode struct {
	config       RaftNodeConfig
	fsm          fsm.StateMachine
	repoFactory  repository.RepositoryFactory
	raftStorage  *RepositoryRaftStorage
	node         raft.Node
	ctx          context.Context
	cancel       context.CancelFunc
	stopc        chan struct{}
	proposeCh    chan []byte
	confChangeCh chan raftpb.ConfChange
	readIndexCh  chan *readIndexRequest
	mu           sync.RWMutex
	started      int32
}

// RepositoryRaftStorage 基于仓库的Raft存储适配器
type RepositoryRaftStorage struct {
	logRepo   repository.RaftLogRepository
	stateRepo repository.RaftStateRepository
	snapRepo  repository.SnapshotRepository
}

// NewRepositoryRaftStorage 创建基于仓库的Raft存储适配器
func NewRepositoryRaftStorage(factory repository.RepositoryFactory) *RepositoryRaftStorage {
	return &RepositoryRaftStorage{
		logRepo:   factory.GetRaftLogRepository(),
		stateRepo: factory.GetRaftStateRepository(),
		snapRepo:  factory.GetSnapshotRepository(),
	}
}

// InitialState 实现 raft.Storage 接口
func (rs *RepositoryRaftStorage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	hs, err := rs.stateRepo.GetHardState()
	if err != nil {
		return raftpb.HardState{}, raftpb.ConfState{}, err
	}

	cs, err := rs.stateRepo.GetConfState()
	if err != nil {
		return raftpb.HardState{}, raftpb.ConfState{}, err
	}

	return hs, cs, nil
}

// Entries 实现 raft.Storage 接口
func (rs *RepositoryRaftStorage) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	return rs.logRepo.GetEntries(lo, hi, maxSize)
}

// Term 实现 raft.Storage 接口
func (rs *RepositoryRaftStorage) Term(i uint64) (uint64, error) {
	return rs.logRepo.Term(i)
}

// LastIndex 实现 raft.Storage 接口
func (rs *RepositoryRaftStorage) LastIndex() (uint64, error) {
	return rs.logRepo.LastIndex()
}

// FirstIndex 实现 raft.Storage 接口
func (rs *RepositoryRaftStorage) FirstIndex() (uint64, error) {
	return rs.logRepo.FirstIndex()
}

// Snapshot 实现 raft.Storage 接口
func (rs *RepositoryRaftStorage) Snapshot() (raftpb.Snapshot, error) {
	return rs.snapRepo.GetSnapshot()
}

// NewRepositoryRaftNode 创建基于仓库的Raft节点
func NewRepositoryRaftNode(config RaftNodeConfig, fsm fsm.StateMachine, factory repository.RepositoryFactory) (*RepositoryRaftNode, error) {
	// 参数验证
	if config.NodeID == 0 {
		return nil, fmt.Errorf("节点ID不能为0")
	}
	if config.ClusterID == 0 {
		return nil, fmt.Errorf("集群ID不能为0")
	}
	if config.ElectionTick <= 0 {
		config.ElectionTick = 10
	}
	if config.HeartbeatTick <= 0 {
		config.HeartbeatTick = 1
	}
	if config.SnapshotCount == 0 {
		config.SnapshotCount = 10000
	}
	if config.MaxSizePerMsg == 0 {
		config.MaxSizePerMsg = 1024 * 1024
	}
	if config.MaxInflightMsgs == 0 {
		config.MaxInflightMsgs = 256
	}
	if config.CheckpointInterval <= 0 {
		config.CheckpointInterval = 30
	}

	// 创建上下文
	ctx, cancel := context.WithCancel(context.Background())

	// 创建仓库存储适配器
	raftStorage := NewRepositoryRaftStorage(factory)

	// 创建节点实例
	rn := &RepositoryRaftNode{
		config:       config,
		fsm:          fsm,
		repoFactory:  factory,
		raftStorage:  raftStorage,
		ctx:          ctx,
		cancel:       cancel,
		stopc:        make(chan struct{}),
		proposeCh:    make(chan []byte, 256),
		confChangeCh: make(chan raftpb.ConfChange),
		readIndexCh:  make(chan *readIndexRequest, 64),
	}

	return rn, nil
}

// Start 启动Raft节点
func (rn *RepositoryRaftNode) Start() error {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.started != 0 {
		return fmt.Errorf("节点已经启动")
	}

	// 获取初始状态
	_, cs, err := rn.raftStorage.InitialState()
	if err != nil {
		return fmt.Errorf("获取初始状态失败: %v", err)
	}

	// 创建Raft配置
	c := &raft.Config{
		ID:                        rn.config.NodeID,
		ElectionTick:              rn.config.ElectionTick,
		HeartbeatTick:             rn.config.HeartbeatTick,
		Storage:                   rn.raftStorage,
		Applied:                   0,
		MaxSizePerMsg:             rn.config.MaxSizePerMsg,
		MaxInflightMsgs:           rn.config.MaxInflightMsgs,
		CheckQuorum:               true,
		PreVote:                   true,
		DisableProposalForwarding: false,
	}

	// 创建或重启Raft节点
	if len(cs.Voters) == 0 {
		// 新集群
		peers := []raft.Peer{{ID: rn.config.NodeID}}
		rn.node = raft.StartNode(c, peers)
	} else {
		// 重启节点
		rn.node = raft.RestartNode(c)
	}

	// 启动主循环
	go rn.run()

	rn.started = 1
	fmt.Printf("Raft节点 %d 启动成功\n", rn.config.NodeID)
	return nil
}

// Stop 停止Raft节点
func (rn *RepositoryRaftNode) Stop() {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.started == 0 {
		return
	}

	close(rn.stopc)
	rn.cancel()

	if rn.node != nil {
		rn.node.Stop()
	}

	rn.started = 0
	fmt.Printf("Raft节点 %d 已停止\n", rn.config.NodeID)
}

// Propose 提议一个新的操作
func (rn *RepositoryRaftNode) Propose(data []byte) error {
	select {
	case rn.proposeCh <- data:
		return nil
	case <-rn.ctx.Done():
		return fmt.Errorf("节点已停止")
	case <-time.After(5 * time.Second):
		return fmt.Errorf("提议超时")
	}
}

// ProposeConfChange 提议配置变更
func (rn *RepositoryRaftNode) ProposeConfChange(cc raftpb.ConfChange) error {
	select {
	case rn.confChangeCh <- cc:
		return nil
	case <-rn.ctx.Done():
		return fmt.Errorf("节点已停止")
	case <-time.After(5 * time.Second):
		return fmt.Errorf("配置变更提议超时")
	}
}

// ReadIndex 执行一致性读
func (rn *RepositoryRaftNode) ReadIndex(ctx context.Context, rctx []byte) error {
	req := &readIndexRequest{
		ctx:      ctx,
		id:       uint64(time.Now().UnixNano()),
		proposal: rctx,
		result:   make(chan error, 1),
	}

	select {
	case rn.readIndexCh <- req:
		return <-req.result
	case <-ctx.Done():
		return ctx.Err()
	case <-rn.ctx.Done():
		return fmt.Errorf("节点已停止")
	}
}

// run 主循环
func (rn *RepositoryRaftNode) run() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rn.node.Tick()

		case prop := <-rn.proposeCh:
			if err := rn.node.Propose(rn.ctx, prop); err != nil {
				fmt.Printf("提议失败: %v\n", err)
			}

		case cc := <-rn.confChangeCh:
			if err := rn.node.ProposeConfChange(rn.ctx, cc); err != nil {
				fmt.Printf("配置变更提议失败: %v\n", err)
			}

		case req := <-rn.readIndexCh:
			if err := rn.node.ReadIndex(req.ctx, req.proposal); err != nil {
				req.result <- err
			} else {
				req.result <- nil
			}

		case rd := <-rn.node.Ready():
			if err := rn.processReady(rd); err != nil {
				fmt.Printf("处理Ready失败: %v\n", err)
			}
			rn.node.Advance()

		case <-rn.stopc:
			return
		case <-rn.ctx.Done():
			return
		}
	}
}

// processReady 处理Ready状态
func (rn *RepositoryRaftNode) processReady(rd raft.Ready) error {
	// 保存硬状态
	if !raft.IsEmptyHardState(rd.HardState) {
		if err := rn.raftStorage.stateRepo.StoreHardState(rd.HardState); err != nil {
			return fmt.Errorf("保存硬状态失败: %v", err)
		}
	}

	// 保存日志条目
	if len(rd.Entries) > 0 {
		if err := rn.raftStorage.logRepo.StoreEntries(rd.Entries); err != nil {
			return fmt.Errorf("保存日志条目失败: %v", err)
		}
	}

	// 保存快照
	if !raft.IsEmptySnap(rd.Snapshot) {
		if err := rn.raftStorage.snapRepo.StoreSnapshot(rd.Snapshot); err != nil {
			return fmt.Errorf("保存快照失败: %v", err)
		}
		if err := rn.fsm.ApplySnapshot(rd.Snapshot); err != nil {
			return fmt.Errorf("应用快照失败: %v", err)
		}
	}

	// 应用已提交的条目
	for _, entry := range rd.CommittedEntries {
		if entry.Type == raftpb.EntryConfChange {
			var cc raftpb.ConfChange
			if err := cc.Unmarshal(entry.Data); err != nil {
				return fmt.Errorf("解析配置变更失败: %v", err)
			}
			rn.node.ApplyConfChange(cc)
			if err := rn.raftStorage.stateRepo.StoreConfState(*rn.node.ApplyConfChange(cc)); err != nil {
				return fmt.Errorf("保存配置状态失败: %v", err)
			}
		} else if len(entry.Data) > 0 {
			if err := rn.fsm.Apply(entry); err != nil {
				return fmt.Errorf("应用日志条目失败: %v", err)
			}
		}
	}

	return nil
}

// GetNodeID 获取节点ID
func (rn *RepositoryRaftNode) GetNodeID() uint64 {
	return rn.config.NodeID
}

// GetClusterID 获取集群ID
func (rn *RepositoryRaftNode) GetClusterID() uint64 {
	return rn.config.ClusterID
}

// IsLeader 检查是否为领导者
func (rn *RepositoryRaftNode) IsLeader() bool {
	if rn.node == nil {
		return false
	}
	return rn.node.Status().Lead == rn.config.NodeID
}

// GetStatus 获取节点状态
func (rn *RepositoryRaftNode) GetStatus() raft.Status {
	if rn.node == nil {
		return raft.Status{}
	}
	return rn.node.Status()
}

// Close 关闭节点
func (rn *RepositoryRaftNode) Close() error {
	rn.Stop()
	return nil
}