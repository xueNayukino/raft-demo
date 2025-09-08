package node

import (
	"context"
	"fmt"
	"log"
	"qklzl/internal/fsm"
	"qklzl/internal/storage"
	"qklzl/internal/transport"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

const (
	defaultSendTimeout = 3 * time.Second
)

// RaftNodeConfig 是 RaftNode 的配置
type RaftNodeConfig struct {
	// 节点标识
	NodeID uint64
	// 集群ID
	ClusterID uint64
	// 集群成员
	Peers map[uint64]string // nodeID -> address

	// 是否是重启
	IsRestart bool
	// 是否是新加入的节点
	IsJoin bool

	// 选举超时（毫秒）
	ElectionTick int
	// 心跳超时（毫秒）
	HeartbeatTick int

	// 快照阈值（应用的日志条目数）
	SnapshotCount uint64
	// 存储目录
	StorageDir string

	// 监听地址
	ListenAddr string

	// 最大日志大小（字节）
	MaxSizePerMsg uint64
	// 最大未提交日志数
	MaxInflightMsgs int

	// 检查点间隔（秒）
	CheckpointInterval int
}

// RaftNode 表示一个 Raft 节点
type RaftNode struct {
	// 配置
	config RaftNodeConfig

	// Raft 相关
	node        raft.Node              //etcd/raft库提供的核心Raft实现
	raftStorage *RaftStorage           //Raft存储适配器，连接etcd/raft和底层存储
	storage     *storage.BadgerStorage //底层存储
	transport   *transport.Transport   //网络层
	raftConfig  *raft.Config           //Raft配置

	// 状态机
	fsm *fsm.KVStateMachine

	// 状态
	appliedIndex  uint64
	commitIndex   uint64
	term          uint64
	isLeader      bool
	confState     raftpb.ConfState
	currentLeader uint64 //当前领导者ID
	lastHeartbeat time.Time
	stopped       int32          //节点停止状态
	state         raft.StateType //节点当前状态

	// 上下文控制
	ctx    context.Context
	cancel context.CancelFunc

	// 通道
	stopc               chan struct{}          //停止信号
	confChangeCompleted chan struct{}          //配置变更完成信号
	proposeCh           chan []byte            //提议通道
	confChangeCh        chan raftpb.ConfChange //配置变更通道
	readIndexCh         chan *readIndexRequest //读索引请求通道

	// 读索引映射
	readIndexes map[string]*readIndexContext //// 用于一致性读的上下文

	// 互斥锁
	mu sync.RWMutex

	// 并发控制
	started int32 //启动标志
}

// RaftStorage 是 etcd/raft Storage 接口的适配器
type RaftStorage struct {
	storage *storage.BadgerStorage
}

// NewRaftStorage 创建新的 Raft 存储适配器
func NewRaftStorage(s *storage.BadgerStorage) *RaftStorage {
	return &RaftStorage{
		storage: s,
	}
}

// InitialState 实现 raft.Storage 接口，返回初始状态
func (rs *RaftStorage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	return rs.storage.InitialState()
}

// Entries 实现 raft.Storage 接口，返回指定范围的日志条目
func (rs *RaftStorage) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	return rs.storage.Entries(lo, hi, maxSize)
}

// Term 实现 raft.Storage 接口，返回指定索引的任期
func (rs *RaftStorage) Term(i uint64) (uint64, error) {
	return rs.storage.Term(i)
}

// LastIndex 实现 raft.Storage 接口，返回最后一个日志索引
func (rs *RaftStorage) LastIndex() (uint64, error) {
	return rs.storage.LastIndex()
}

// FirstIndex 实现 raft.Storage 接口，返回第一个日志索引
func (rs *RaftStorage) FirstIndex() (uint64, error) {
	return rs.storage.FirstIndex()
}

// Snapshot 实现 raft.Storage 接口，返回当前快照
func (rs *RaftStorage) Snapshot() (raftpb.Snapshot, error) {
	return rs.storage.Snapshot()
}

// readIndexRequest 表示一个读索引请求
type readIndexRequest struct {
	ctx      context.Context //请求上下文
	id       uint64          //请求ID
	proposal []byte          //提议数据
	result   chan error      //结果通道
}

// readIndexContext 表示一致性读的上下文
type readIndexContext struct {
	RequiredIndex uint64        // 要求的最小Raft日志索引
	ReadResult    chan struct{} // 读取完成信号
}

// NewRaftNode 创建一个新的 Raft 节点
func NewRaftNode(config RaftNodeConfig, fsm *fsm.KVStateMachine) (*RaftNode, error) {
	// 参数验证
	if config.NodeID == 0 {
		return nil, fmt.Errorf("节点ID不能为0")
	}
	if config.ClusterID == 0 {
		return nil, fmt.Errorf("集群ID不能为0")
	}
	if config.ElectionTick <= 0 {
		config.ElectionTick = 10 // 选举超时，默认值
	}
	if config.HeartbeatTick <= 0 {
		config.HeartbeatTick = 1 // 心跳间隔，默认值
	}
	if config.SnapshotCount == 0 {
		config.SnapshotCount = 10000 //快照阈值， 默认值
	}
	if config.MaxSizePerMsg == 0 {
		config.MaxSizePerMsg = 1024 * 1024 // 最大消息大小默认值 1MB
	}
	if config.MaxInflightMsgs == 0 {
		config.MaxInflightMsgs = 256 // 最大未确定消息数默认值
	}
	if config.CheckpointInterval <= 0 {
		config.CheckpointInterval = 30 // 检查间隔，默认值 30秒
	}

	// 创建上下文
	ctx, cancel := context.WithCancel(context.Background())

	// 创建节点实例
	rn := &RaftNode{
		config:              config,
		fsm:                 fsm,
		ctx:                 ctx,
		cancel:              cancel,
		stopc:               make(chan struct{}),
		proposeCh:           make(chan []byte, 256),
		confChangeCh:        make(chan raftpb.ConfChange),
		readIndexCh:         make(chan *readIndexRequest),
		readIndexes:         make(map[string]*readIndexContext),
		confChangeCompleted: make(chan struct{}),
		started:             0,
		stopped:             1, // 初始化为已停止状态
	}

	// 创建存储层
	storageConfig := &storage.BadgerConfig{
		GCInterval:       time.Duration(config.CheckpointInterval) * time.Second,
		GCDiscardRatio:   0.5,
		SnapshotInterval: time.Duration(config.CheckpointInterval) * time.Second,
		SnapshotCount:    config.SnapshotCount,
	}

	var err error
	rn.storage, err = storage.NewBadgerStorage(config.StorageDir, storageConfig, fsm)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("创建存储层失败: %v", err)
	}

	// 创建 Raft 存储适配器，连接etcd/raft和底层存储
	rn.raftStorage = NewRaftStorage(rn.storage)

	// 创建网络层
	rn.transport, err = transport.NewTransport(config.NodeID, config.ListenAddr, rn) //实现了 MessageHandler 接口
	if err != nil {
		cancel()
		rn.storage.Stop()
		return nil, fmt.Errorf("创建网络层失败: %v", err)
	}

	// 添加集群成员
	for id, addr := range config.Peers {
		if id != config.NodeID {
			if err := rn.transport.AddPeer(id, addr); err != nil {
				fmt.Printf("添加节点 %d 失败: %v\n", id, err)
			}
		}
	}

	// 创建 raft 配置
	c := &raft.Config{
		ID:              config.NodeID,
		ElectionTick:    config.ElectionTick,
		HeartbeatTick:   config.HeartbeatTick,
		Storage:         rn.raftStorage,
		MaxSizePerMsg:   config.MaxSizePerMsg,
		MaxInflightMsgs: config.MaxInflightMsgs,
		PreVote:         true, // 启用 PreVote 以防止网络分区导致的不必要选举,预投票阶段避免无效选举
	}

	// 如果是单节点集群，直接成为领导者
	if len(config.Peers) == 1 && !config.IsJoin {
		c.Applied = 0
		c.DisableProposalForwarding = true // 禁用提议转发,无其他节点可转发
		// 确保这个节点能立即成为领导者
		c.ElectionTick = 2  // 选举tick必须大于心跳tick
		c.HeartbeatTick = 1 // 快速心跳
	}

	// 保存 raft 配置
	rn.raftConfig = c

	return rn, nil
}

// Start 启动 Raft 节点
func (rn *RaftNode) Start() error {
	// 检查节点是否已经启动
	if !atomic.CompareAndSwapInt32(&rn.stopped, 1, 0) {
		return fmt.Errorf("节点已经启动")
	}

	// 如果传输层已存在，先停止它，这是对重启节点的，端口冲突和连接泄漏
	if rn.transport != nil {
		rn.transport.Stop()
	}

	// 创建传输层，将自身作为消息处理器传入
	transport, err := transport.NewTransport(rn.config.NodeID, rn.config.ListenAddr, rn)
	if err != nil {
		// 如果创建传输层失败，将节点标记为已停止
		atomic.StoreInt32(&rn.stopped, 1)
		return fmt.Errorf("创建传输层失败: %v", err)
	}
	rn.transport = transport

	// 连接所有对等节点，此时仅是物理连接，尚未加入 Raft 集群
	for id, addr := range rn.config.Peers {
		if id != rn.config.NodeID {
			if err := transport.AddPeer(id, addr); err != nil {
				fmt.Printf("添加节点 %d 失败: %v\n", id, err)
			} else {
				fmt.Printf("添加节点 %d 成功\n", id)
			}
		}
	}

	// 如果是重启，从存储中恢复状态
	if rn.config.IsRestart {
		fmt.Printf("节点 %d 重启，从存储中恢复状态\n", rn.config.NodeID)
		// 获取最新的快照
		snapshot, err := rn.storage.Snapshot()
		if err != nil && err.Error() != "没有可用的快照" {
			atomic.StoreInt32(&rn.stopped, 1)
			return fmt.Errorf("获取快照失败: %v", err)
		}

		// 如果有快照，设置已应用的索引
		if snapshot.Metadata.Index > 0 {
			rn.raftConfig.Applied = snapshot.Metadata.Index
		}

		// 获取硬状态
		hardState, err := rn.storage.GetHardState()
		if err != nil {
			atomic.StoreInt32(&rn.stopped, 1)
			return fmt.Errorf("获取硬状态失败: %v", err)
		}

		// 如果有硬状态，设置任期和提交索引
		if hardState.Commit > 0 {
			rn.raftConfig.Applied = hardState.Commit
		}

		rn.node = raft.RestartNode(rn.raftConfig) //使用 etcd/raft 的 RestartNode API
		fmt.Printf("节点 %d 重启成功\n", rn.config.NodeID)
	} else if rn.config.IsJoin {
		fmt.Printf("节点 %d 作为新节点加入集群\n", rn.config.NodeID)
		// 当节点以加入模式启动时，应使用RestartNode而不是传入空peers的StartNode
		rn.node = raft.RestartNode(rn.raftConfig)

		// 确保节点可以收到其他节点的消息
		peers := make([]raft.Peer, 0, len(rn.config.Peers))
		for id := range rn.config.Peers {
			if id != rn.config.NodeID {
				peers = append(peers, raft.Peer{ID: id}) //节点初始为跟随者状态，不会触发选举
			}
		}
		fmt.Printf("节点 %d 配置的对等节点: %v\n", rn.config.NodeID, peers)
	} else {
		// 初始化集群成员
		peers := make([]raft.Peer, 0, len(rn.config.Peers))
		for id := range rn.config.Peers {
			peers = append(peers, raft.Peer{ID: id})
		}

		// 确保peers列表不为空
		if len(peers) == 0 {
			atomic.StoreInt32(&rn.stopped, 1)
			return fmt.Errorf("peers列表为空，无法启动节点")
		}

		fmt.Printf("节点 %d 首次启动，初始化集群成员: %v\n", rn.config.NodeID, peers)
		rn.node = raft.StartNode(rn.raftConfig, peers)
		fmt.Printf("节点 %d 的 raft.StartNode 调用成功\n", rn.config.NodeID)
	}

	// 启动 Raft 主循环
	go rn.serveRaft()

	// 启动提议处理
	go rn.serveProposals()

	// 启动定时任务
	go rn.serveScheduledTasks()

	log.Printf("节点 %d 启动成功，监听地址: %s", rn.config.NodeID, rn.config.ListenAddr)
	return nil
}

// Stop 停止 Raft 节点
func (rn *RaftNode) Stop() {
	// 检查节点是否已经停止
	if !atomic.CompareAndSwapInt32(&rn.stopped, 0, 1) {
		return
	}

	fmt.Printf("节点 %d 正在停止...\n", rn.config.NodeID)

	// 取消上下文
	if rn.cancel != nil {
		rn.cancel()
	}

	// 停止传输层
	if rn.transport != nil {
		rn.transport.Stop()
	}

	// 停止 Raft 节点
	rn.mu.Lock()
	if rn.node != nil {
		rn.node.Stop()
		rn.node = nil
	}
	rn.mu.Unlock()

	// 关闭所有通道
	close(rn.stopc)

	// 等待所有 goroutine 退出
	time.Sleep(200 * time.Millisecond)

	fmt.Printf("节点 %d 已停止\n", rn.config.NodeID)
}

// HandleMessage 实现 MessageHandler 接口，处理网络层收到的消息
func (rn *RaftNode) HandleMessage(msg raftpb.Message) error {
	// 检查节点是否已关闭
	rn.mu.RLock()
	if rn.node == nil {
		rn.mu.RUnlock()
		return fmt.Errorf("节点已关闭")
	}
	node := rn.node
	rn.mu.RUnlock()

	// 将消息转发给 raft 节点
	return node.Step(rn.ctx, msg) //etcd Raft 库方法，Raft 协议的状态机入口，将消息输入 Raft 状态机。根据节点角色（Leader、Follower、Candidate）和消息类型（如 MsgVote、MsgApp）更新状态领导者处理投票请求，跟随者处理日志追加消息
}

// GetTerm 获取当前任期，实现 MessageHandler 接口
func (rn *RaftNode) GetTerm() uint64 {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.term
}

// IsLeader 检查当前节点是否是领导者
func (rn *RaftNode) IsLeader() bool {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.isLeader
}

// GetLeaderID 获取当前领导者ID
func (rn *RaftNode) GetLeaderID() uint64 {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.currentLeader
}

// GetCommitIndex 返回当前的提交索引
func (rn *RaftNode) GetCommitIndex() uint64 {
	rn.mu.RLock()
	defer rn.mu.RUnlock()

	if rn.node == nil {
		return 0
	}

	status := rn.node.Status() //raft.Status 结构体包含：Commit：已提交的日志索引。其他字段如 Term（任期）、Lead（领导者 ID）、RaftState（节点角色）
	return status.Commit
}

// GetAppliedIndex 获取已应用的日志索引
func (rn *RaftNode) GetAppliedIndex() uint64 {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.appliedIndex
}

// publishEntries 将已提交的日志条目应用到状态机
func (rn *RaftNode) publishEntries(ents []raftpb.Entry) error {
	if len(ents) == 0 {
		return nil
	}

	for i := range ents {
		switch ents[i].Type {
		case raftpb.EntryNormal:
			if len(ents[i].Data) == 0 {
				// 忽略空条目
				continue
			}
			// 应用到状态机
			if err := rn.fsm.Apply(ents[i]); err != nil {
				return fmt.Errorf("应用日志条目失败: %v", err)
			}
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange                           ////raftpb.ConfChange：etcd Raft 库结构体，包含变更类型（Type）、节点 ID（NodeID）、上下文（Context）等
			if err := cc.Unmarshal(ents[i].Data); err != nil { //条目数据（ents[i].Data）反序列化为 raftpb.ConfChange
				return fmt.Errorf("解析配置变更失败: %v", err)
			}
			confState := rn.node.ApplyConfChange(cc) //raft.Node.ApplyConfChange(cc raftpb.ConfChange) *raftpb.ConfState：应用配置变更，返回新的配置状态
			rn.mu.Lock()
			rn.confState = *confState
			rn.mu.Unlock()

			// 处理配置变更
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				if cc.NodeID != rn.config.NodeID {
					if len(cc.Context) > 0 {
						// 添加节点到传输层
						rn.transport.AddPeer(cc.NodeID, string(cc.Context)) //cc.Context：字节切片，通常包含新节点的地址（如 "127.0.0.1:7002"）
						// 更新配置中的对等节点映射
						rn.mu.Lock()
						rn.config.Peers[cc.NodeID] = string(cc.Context)
						rn.mu.Unlock()
						log.Printf("节点 %d 添加了对等节点 %d -> %s", rn.config.NodeID, cc.NodeID, string(cc.Context))
					}
				}
			case raftpb.ConfChangeRemoveNode: //处理移除节点变更
				if cc.NodeID != rn.config.NodeID {
					rn.transport.RemovePeer(cc.NodeID) //网络层
					// 从配置中删除对等节点
					rn.mu.Lock()
					delete(rn.config.Peers, cc.NodeID) //节点层li q
					rn.mu.Unlock()
					log.Printf("节点 %d 移除了对等节点 %d", rn.config.NodeID, cc.NodeID)
				}
			}

			// 通知配置变更完成
			select {
			case rn.confChangeCompleted <- struct{}{}:
			default:
			}
		}

		// 更新应用索引
		rn.mu.Lock()
		rn.appliedIndex = ents[i].Index //更新已应用索引为当前条目索引
		rn.mu.Unlock()
	}

	return nil
}

// publishSnapshot 将快照应用到状态机
func (rn *RaftNode) publishSnapshot(snap raftpb.Snapshot) error {
	if raft.IsEmptySnap(snap) {
		return nil
	}

	fmt.Printf("节点 %d 应用快照，索引: %d, 任期: %d\n",
		rn.config.NodeID, snap.Metadata.Index, snap.Metadata.Term)

	// 应用快照到状态机
	if err := rn.fsm.RestoreSnapshot(snap.Data); err != nil {
		return fmt.Errorf("恢复快照失败: %v", err)
	}

	// 更新应用索引和配置状态
	rn.mu.Lock()
	prevIndex := rn.appliedIndex //保存当前应用索引，用于日志记录
	rn.appliedIndex = snap.Metadata.Index
	rn.confState = snap.Metadata.ConfState
	rn.mu.Unlock()

	fmt.Printf("节点 %d 成功应用快照，应用索引从 %d 更新到 %d\n",
		rn.config.NodeID, prevIndex, snap.Metadata.Index)

	return nil
}

// processSnapshot 处理快照，几乎相同，逻辑一致，可能是代码重复或用于不同上下文。以下逐行解释，突出与 publishSnapshot 的细微差异
func (rn *RaftNode) processSnapshot(snap raftpb.Snapshot) error {
	if raft.IsEmptySnap(snap) {
		return nil
	}

	fmt.Printf("节点 %d 处理快照，索引: %d, 任期: %d\n",
		rn.config.NodeID, snap.Metadata.Index, snap.Metadata.Term)

	// 应用快照到状态机
	if err := rn.fsm.RestoreSnapshot(snap.Data); err != nil {
		return fmt.Errorf("恢复快照失败: %v", err)
	}

	// 更新应用索引和配置状态
	rn.mu.Lock()
	prevIndex := rn.appliedIndex
	rn.appliedIndex = snap.Metadata.Index
	rn.confState = snap.Metadata.ConfState
	rn.mu.Unlock()

	fmt.Printf("节点 %d 成功处理快照，应用索引从 %d 更新到 %d\n",
		rn.config.NodeID, prevIndex, snap.Metadata.Index)

	return nil
}

// createSnapshot 创建快照
func (rn *RaftNode) createSnapshot() error {
	// 获取当前应用的索引
	appliedIndex := rn.GetAppliedIndex()

	// 获取状态机快照
	data, err := rn.fsm.SaveSnapshot()
	if err != nil {
		return fmt.Errorf("获取状态机快照失败: %v", err)
	}

	// 创建 Raft 快照
	snap := raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index:     appliedIndex,
			Term:      rn.GetTerm(),
			ConfState: rn.confState,
		},
		Data: data,
	}

	// 保存快照
	if err := rn.storage.SaveSnapshot(snap); err != nil {
		return fmt.Errorf("保存快照失败: %v", err)
	}

	// 压缩日志
	if err := rn.storage.CompactLog(appliedIndex); err != nil {
		return fmt.Errorf("压缩日志失败: %v", err)
	}

	return nil
}

// maybeTakeSnapshot 根据条件创建快照
func (rn *RaftNode) maybeTakeSnapshot() error {
	// 获取当前应用的索引
	appliedIndex := rn.GetAppliedIndex()

	// 获取最后一次快照的索引
	snap, err := rn.storage.Snapshot()
	if err != nil {
		return fmt.Errorf("获取最后快照失败: %v", err)
	}

	// 如果应用的索引超过最后快照索引指定数量，则创建新快照，计算当前索引与最后快照索引的差距是否大于定义的值
	if appliedIndex-snap.Metadata.Index >= rn.config.SnapshotCount {
		log.Printf("创建快照 at index %d", appliedIndex)
		return rn.createSnapshot()
	}

	return nil
}

// serveProposals 处理提议和读索引请求
func (rn *RaftNode) serveProposals() {
	for {
		select {
		case <-rn.ctx.Done():
			return
		case prop := <-rn.proposeCh: //提议的字节切片，包含客户端写入的数据
			// 处理普通提议
			if err := rn.node.Propose(rn.ctx, prop); err != nil {
				fmt.Printf("提议失败: %v\n", err)
			}
		case cc := <-rn.confChangeCh:
			// 处理配置变更提议
			if err := rn.node.ProposeConfChange(rn.ctx, cc); err != nil {
				fmt.Printf("配置变更提议失败: %v\n", err)
			}
		case req := <-rn.readIndexCh:
			// 处理读索引请求，客户查询包裹状态（读请求），总部确认所有分部已同步最新数据后返回结果
			// 提交读索引请求
			if err := rn.node.ReadIndex(req.ctx, req.proposal); err != nil { //领导者确认其地位，返回提交索引，确保读操作看到最新状态
				req.result <- err
			} else {
				// 成功提交，结果将在 Ready 中返回
				req.result <- nil
			}
		}
	}
}

// serveScheduledTasks 处理定时任务，如快照和检查点
func (rn *RaftNode) serveScheduledTasks() {
	// 创建定时器
	snapshotTicker := time.NewTicker(time.Duration(rn.config.CheckpointInterval) * time.Second)
	defer snapshotTicker.Stop()

	// 创建网络分区检测定时器
	partitionTicker := time.NewTicker(5 * time.Second)
	defer partitionTicker.Stop()

	for {
		select {
		case <-rn.ctx.Done():
			return
		case <-snapshotTicker.C:
			// 检查是否需要创建快照
			rn.maybeTakeSnapshot()
		case <-partitionTicker.C:
			// 检测和处理网络分区
			rn.HandlePartition()
		}
	}
}

// Propose 提交一个提议
func (rn *RaftNode) Propose(ctx context.Context, data []byte) error {
	return rn.node.Propose(ctx, data) //如果当前节点是领导者，提议会被封装为日志条目，追加到日志并广播给其他节点。不是就会报错
}

// ProposeConfChange 提交一个配置变更提议
func (rn *RaftNode) ProposeConfChange(ctx context.Context, cc raftpb.ConfChange) error {
	//raftpb.ConfChange 结构体，含字段如 Type（变更类型，如 ConfChangeAddNode）、NodeID（目标节点 ID）、Context（附加数据）
	return rn.node.ProposeConfChange(ctx, cc) //提议集群配置变更，仅领导者可提交配置变更，否则返回错误
}

// ReadIndex 执行一致性读
func (rn *RaftNode) ReadIndex(ctx context.Context, proposal []byte) error {
	// 创建读索引请求
	req := &readIndexRequest{
		ctx:      ctx,
		proposal: proposal, //标识或携带上下文信息
		result:   make(chan error, 1),
	}

	// 发送读索引请求
	select {
	case rn.readIndexCh <- req:
	case <-ctx.Done():
		return ctx.Err()
	case <-rn.stopc:
		return fmt.Errorf("节点已停止")
	}

	// 等待结果
	select {
	case err := <-req.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-rn.stopc:
		return fmt.Errorf("节点已停止")
	}
}

// GetSnapshot 获取当前快照
func (rn *RaftNode) GetSnapshot() (raftpb.Snapshot, error) {
	return rn.storage.Snapshot()
}

// ForceSnapshot 强制创建快照
func (rn *RaftNode) ForceSnapshot() error {
	return rn.createSnapshot()
}

// updateLeaderState 更新领导者状态
func (rn *RaftNode) updateLeaderState(term uint64, lead uint64) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.term = term
	rn.currentLeader = lead
}

// ConsistentGet 执行一致性读
func (rn *RaftNode) ConsistentGet(ctx context.Context, key string) (string, error) {
	// 如果是领导者且租约有效，可以直接读取
	if rn.IsLeader() && rn.CheckLeaderLease() {
		// 领导者租约有效，直接读取
		return rn.fsm.Get(key)
	}

	// 否则，执行读索引
	if err := rn.ReadIndex(ctx, []byte(key)); err != nil {
		// 如果读索引失败，尝试转发到领导者
		if rn.currentLeader != 0 && rn.currentLeader != rn.config.NodeID {
			// 获取领导者地址
			leaderAddr, ok := rn.config.Peers[rn.currentLeader]
			if ok {
				fmt.Printf("转发读请求到领导者 %d (%s)\n", rn.currentLeader, leaderAddr)
				// 在实际实现中，这里应该通过 RPC 调用领导者的 ConsistentGet 方法
				// 由于这是示例代码，我们只记录日志
			}
		}
		return "", fmt.Errorf("读索引失败: %v", err)
	}

	// 等待状态机应用到最新的索引
	if err := rn.fsm.WaitForIndex(rn.GetCommitIndex()); err != nil {
		return "", fmt.Errorf("等待状态机应用索引失败: %v", err)
	}

	// 直接调用 Get 方法
	value, err := rn.fsm.Get(key)
	if err != nil {
		return "", fmt.Errorf("读取键值失败: %v", err)
	}
	return value, nil
}

// RestartNode 重启节点
func (rn *RaftNode) RestartNode() error {
	// 检查节点是否已停止
	if atomic.LoadInt32(&rn.stopped) == 1 {
		return fmt.Errorf("节点已停止，无法重启")
	}

	// 如果节点已经启动，返回错误
	rn.mu.Lock()
	if rn.node != nil {
		rn.mu.Unlock()
		return fmt.Errorf("节点已启动，无需重启")
	}
	rn.mu.Unlock()

	// 获取最新的快照
	snapshot, err := rn.storage.Snapshot()
	if err != nil && err.Error() != "没有可用的快照" {
		return fmt.Errorf("获取快照失败: %v", err)
	}

	// 如果有快照，应用到状态机
	if !raft.IsEmptySnap(snapshot) {
		if err := rn.processSnapshot(snapshot); err != nil {
			return fmt.Errorf("应用快照失败: %v", err)
		}
		fmt.Printf("节点 %d 从快照恢复成功，快照索引: %d\n", rn.config.NodeID, snapshot.Metadata.Index)
	}

	// 创建 raft 配置
	raftConfig := &raft.Config{
		ID:                        rn.config.NodeID,
		ElectionTick:              rn.config.ElectionTick,
		HeartbeatTick:             rn.config.HeartbeatTick,
		Storage:                   rn.raftStorage,
		MaxSizePerMsg:             rn.config.MaxSizePerMsg,
		MaxInflightMsgs:           rn.config.MaxInflightMsgs,
		CheckQuorum:               true, // 启用法定人数检查
		PreVote:                   true, // 启用预投票
		DisableProposalForwarding: false,
	}

	// 获取存储的硬状态
	hardState, err := rn.storage.GetHardState()
	if err != nil {
		return fmt.Errorf("获取硬状态失败: %v", err)
	}

	// 设置已应用的索引
	if !raft.IsEmptySnap(snapshot) {
		raftConfig.Applied = snapshot.Metadata.Index
	} else if hardState.Commit > 0 {
		raftConfig.Applied = hardState.Commit
	}

	// 重启节点
	rn.mu.Lock()
	rn.node = raft.RestartNode(raftConfig)
	rn.mu.Unlock()

	// 启动主循环
	go rn.serveRaft()
	// 启动提议处理
	go rn.serveProposals()
	// 启动定时任务
	go rn.serveScheduledTasks()

	fmt.Printf("节点 %d 重启成功，监听地址: %s\n", rn.config.NodeID, rn.config.ListenAddr)
	return nil
}

// RecoverFromSnapshot 从快照恢复
func (rn *RaftNode) RecoverFromSnapshot() error {
	// 获取最新的快照
	snapshot, err := rn.storage.Snapshot()
	if err != nil {
		return fmt.Errorf("获取快照失败: %v", err)
	}

	// 检查快照是否为空
	if raft.IsEmptySnap(snapshot) {
		return fmt.Errorf("没有可用的快照")
	}

	// 应用快照到状态机
	if err := rn.processSnapshot(snapshot); err != nil {
		return fmt.Errorf("应用快照失败: %v", err)
	}

	fmt.Printf("节点 %d 从快照恢复成功，快照索引: %d\n", rn.config.NodeID, snapshot.Metadata.Index)
	return nil
}

// CheckLeaderLease 检查领导者租约是否有效
func (rn *RaftNode) CheckLeaderLease() bool {
	// 如果不是领导者，直接返回 false
	if !rn.IsLeader() {
		return false
	}

	// 获取当前时间
	now := time.Now()

	// 获取上次心跳时间
	rn.mu.RLock()
	defer rn.mu.RUnlock()

	// 如果没有设置上次心跳时间，返回 false
	if rn.lastHeartbeat.IsZero() { //time.Time.IsZero() bool：time 包方法，检查时间是否为零值（未初始化，如 0001-01-01 00:00:00 UTC）
		return false
	}

	// 检查是否超过租约时间
	// 使用更合理的租约超时计算：心跳间隔的 5 倍。time.Duration(...)：将整数转换为 time.Duration 类型（纳秒单位）
	leaseTimeout := time.Duration(rn.config.HeartbeatTick*5) * 100 * time.Millisecond
	return now.Sub(rn.lastHeartbeat) < leaseTimeout //time.Time.Sub(t time.Time) time.Duration：time 包方法，计算两个时间点之间的差值（纳秒单位）
}

// updateLeaderHeartbeat 更新领导者心跳时间
func (rn *RaftNode) updateLeaderHeartbeat() {
	if rn.IsLeader() {
		rn.mu.Lock()
		rn.lastHeartbeat = time.Now()
		rn.mu.Unlock()
	}
}

// RejoinCluster 重新加入集群
func (rn *RaftNode) RejoinCluster() error {
	// 检查节点是否已停止
	if atomic.LoadInt32(&rn.stopped) == 1 {
		return fmt.Errorf("节点已停止，无法重新加入集群")
	}

	// 获取当前集群成员
	confState, err := rn.storage.GetConfState()
	if err != nil {
		return fmt.Errorf("获取集群配置失败: %v", err)
	}

	// 检查是否仍然是集群成员
	isMember := false
	for _, id := range confState.Voters {
		if id == rn.config.NodeID {
			isMember = true
			break
		}
	}

	// 如果不是集群成员，通过配置变更重新加入
	if !isMember {
		// 重新连接所有节点
		for id, addr := range rn.config.Peers {
			if id != rn.config.NodeID {
				if err := rn.transport.AddPeer(id, addr); err != nil {
					fmt.Printf("重新连接节点 %d 失败: %v\n", id, err)
				} else {
					fmt.Printf("重新连接节点 %d 成功\n", id)
				}
			}
		}

		// 等待连接建立
		time.Sleep(1 * time.Second)

		// 找到当前的领导者
		var leaderID uint64
		for id := range rn.config.Peers {
			if id != rn.config.NodeID && rn.transport.IsPeerConnected(id) {
				// 尝试获取领导者信息
				if resp, err := rn.transport.GetLeaderInfoFromPeer(id); err == nil {
					leaderID = resp.LeaderID
					break
				}
			}
		}

		if leaderID == 0 {
			return fmt.Errorf("无法找到集群领导者")
		}

		// 向领导者发送加入请求
		cc := raftpb.ConfChange{
			Type:    raftpb.ConfChangeAddNode,
			NodeID:  rn.config.NodeID,
			Context: []byte(rn.config.ListenAddr),
		}

		// 发送配置变更请求
		if err := rn.transport.SendConfChangeToPeer(leaderID, cc); err != nil {
			return fmt.Errorf("发送配置变更请求失败: %v", err)
		}

		// 等待配置变更完成
		select {
		case <-rn.confChangeCompleted:
			fmt.Printf("节点 %d 成功重新加入集群\n", rn.config.NodeID)
		case <-time.After(5 * time.Second):
			return fmt.Errorf("等待配置变更超时")
		}

		// 等待数据同步
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// 使用 ReadIndex 确保数据同步
		if err := rn.ReadIndex(ctx, []byte("rejoin")); err != nil {
			fmt.Printf("等待数据同步失败: %v\n", err)
			// 尝试从快照恢复
			if err := rn.RecoverFromSnapshot(); err != nil {
				fmt.Printf("从快照恢复失败: %v\n", err)
			}
		}

		// 等待状态机应用到最新的索引
		if err := rn.fsm.WaitForIndex(rn.GetCommitIndex()); err != nil {
			fmt.Printf("等待状态机应用索引失败: %v\n", err)
		}
	}

	// 如果节点已经停止，重新启动
	if rn.node == nil {
		// 获取最新的快照
		snapshot, err := rn.storage.Snapshot()
		if err != nil && err.Error() != "没有可用的快照" {
			return fmt.Errorf("获取快照失败: %v", err)
		}

		// 获取硬状态
		hardState, err := rn.storage.GetHardState()
		if err != nil {
			return fmt.Errorf("获取硬状态失败: %v", err)
		}

		// 创建新的 raft 配置
		c := &raft.Config{
			ID:                        rn.config.NodeID,
			ElectionTick:              rn.config.ElectionTick,
			HeartbeatTick:             rn.config.HeartbeatTick,
			Storage:                   rn.raftStorage,
			MaxSizePerMsg:             rn.config.MaxSizePerMsg,
			MaxInflightMsgs:           rn.config.MaxInflightMsgs,
			PreVote:                   true,  // 启用 PreVote 防止不必要的选举
			CheckQuorum:               true,  // 启用法定人数检查
			DisableProposalForwarding: false, // 允许转发提议
		}

		// 设置已应用的索引
		if !raft.IsEmptySnap(snapshot) {
			c.Applied = snapshot.Metadata.Index
		} else if hardState.Commit > 0 {
			c.Applied = hardState.Commit
		}

		// 重启节点
		rn.mu.Lock()
		rn.node = raft.RestartNode(c)
		rn.mu.Unlock()

		// 启动主循环
		go rn.serveRaft()
		go rn.serveProposals()
		go rn.serveScheduledTasks()

		// 等待节点追赶上集群
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// 使用 ReadIndex 确保节点已经同步
		if err := rn.ReadIndex(ctx, []byte("rejoin")); err != nil {
			fmt.Printf("等待节点同步失败: %v\n", err)
			// 尝试从快照恢复
			if err := rn.RecoverFromSnapshot(); err != nil {
				fmt.Printf("从快照恢复失败: %v\n", err)
			}
		}

		// 等待状态机应用到最新的索引
		if err := rn.fsm.WaitForIndex(rn.GetCommitIndex()); err != nil {
			fmt.Printf("等待状态机应用索引失败: %v\n", err)
		}
	}

	fmt.Printf("节点 %d 重新加入集群成功\n", rn.config.NodeID)
	return nil
}

// IsClusterHealthy 检查集群健康状态
func (rn *RaftNode) IsClusterHealthy() bool {
	// 如果不是领导者，无法判断集群健康状态
	if !rn.IsLeader() {
		return false
	}

	// 获取当前集群成员
	confState, err := rn.storage.GetConfState()
	if err != nil {
		fmt.Printf("获取集群配置失败: %v\n", err)
		return false
	}

	// 检查是否有足够的成员
	if len(confState.Voters) < 2 {
		// 单节点集群，认为是健康的
		return true
	}

	// 检查连接状态
	connectedPeers := 0
	for _, id := range confState.Voters {
		if id != rn.config.NodeID {
			if rn.transport.IsPeerConnected(id) {
				connectedPeers++
			}
		}
	}

	// 如果连接的节点数量超过半数，认为集群是健康的
	return connectedPeers >= len(confState.Voters)/2
}

// DetectPartition 检测网络分区
func (rn *RaftNode) DetectPartition() bool {
	// 检查节点是否已经停止
	if atomic.LoadInt32(&rn.stopped) == 1 {
		return false
	}

	// 如果节点已初始化且集群中有多个节点
	rn.mu.RLock()
	defer rn.mu.RUnlock()

	// 如果节点正在启动阶段，不进行分区检测
	if len(rn.config.Peers) <= 1 {
		return false
	}

	// 如果是领导者，总是认为集群正常
	if rn.isLeader {
		return false
	}

	// 如果不是领导者，但已经有领导者，则认为集群正常
	if rn.currentLeader != 0 {
		return false
	}

	// 如果没有领导者，且已经有配置的对等节点，可能存在网络分区
	return len(rn.config.Peers) > 1
}

// HandlePartition 处理网络分区
func (rn *RaftNode) HandlePartition() {
	// 检测是否存在网络分区
	if !rn.DetectPartition() {
		return
	}

	// 避免频繁输出日志
	rn.mu.RLock()
	lastDetection := rn.lastHeartbeat
	rn.mu.RUnlock()

	// 如果距离上次检测时间不足10秒，不重复检测
	if !lastDetection.IsZero() && time.Since(lastDetection) < 10*time.Second {
		return
	}

	fmt.Printf("节点 %d 检测到网络分区，尝试恢复\n", rn.config.NodeID)

	// 更新检测时间
	rn.mu.Lock()
	rn.lastHeartbeat = time.Now()
	rn.mu.Unlock()

	// 尝试重新连接所有节点
	for id, addr := range rn.config.Peers {
		if id != rn.config.NodeID {
			if err := rn.transport.AddPeer(id, addr); err != nil {
				fmt.Printf("重新连接节点 %d 失败: %v\n", id, err)
			} else {
				fmt.Printf("重新连接节点 %d 成功\n", id)
			}
		}
	}
}

// RecoverFromCrash 从节点宕机中恢复
func (rn *RaftNode) RecoverFromCrash(ctx context.Context) error {
	// 检查节点是否已停止
	if atomic.LoadInt32(&rn.stopped) == 0 {
		return fmt.Errorf("节点未停止，无需恢复")
	}

	// 重置停止标志
	atomic.StoreInt32(&rn.stopped, 0)

	// 创建新的上下文
	rn.ctx, rn.cancel = context.WithCancel(context.Background())

	// 重新创建通道
	rn.stopc = make(chan struct{})
	rn.proposeCh = make(chan []byte)
	rn.confChangeCh = make(chan raftpb.ConfChange)
	rn.readIndexCh = make(chan *readIndexRequest)
	rn.readIndexes = make(map[string]*readIndexContext)

	// 重新启动存储层
	storageConfig := &storage.BadgerConfig{
		GCInterval:       time.Duration(rn.config.CheckpointInterval) * time.Second,
		GCDiscardRatio:   0.5,
		SnapshotInterval: time.Duration(rn.config.CheckpointInterval) * time.Second,
		SnapshotCount:    rn.config.SnapshotCount,
	}

	var err error
	rn.storage, err = storage.NewBadgerStorage(rn.config.StorageDir, storageConfig, rn.fsm)
	if err != nil {
		return fmt.Errorf("重新创建存储层失败: %v", err)
	}

	// 重新创建 Raft 存储适配器
	rn.raftStorage = NewRaftStorage(rn.storage)

	// 重新创建网络层
	rn.transport, err = transport.NewTransport(rn.config.NodeID, rn.config.ListenAddr, rn)
	if err != nil {
		rn.storage.Stop()
		return fmt.Errorf("重新创建网络层失败: %v", err)
	}

	// 添加集群成员
	for id, addr := range rn.config.Peers {
		if id != rn.config.NodeID {
			if err := rn.transport.AddPeer(id, addr); err != nil {
				fmt.Printf("添加节点 %d 失败: %v\n", id, err)
			}
		}
	}

	// 从快照恢复状态机
	if err := rn.RecoverFromSnapshot(); err != nil {
		fmt.Printf("从快照恢复状态机失败: %v\n", err)
		// 继续执行，因为可能没有快照
	}

	// 设置节点为 nil，以便 RestartNode 可以重新创建
	rn.mu.Lock()
	rn.node = nil
	rn.mu.Unlock()

	// 创建 raft 配置
	raftConfig := &raft.Config{
		ID:                        rn.config.NodeID,
		ElectionTick:              rn.config.ElectionTick,
		HeartbeatTick:             rn.config.HeartbeatTick,
		Storage:                   rn.raftStorage,
		MaxSizePerMsg:             rn.config.MaxSizePerMsg,
		MaxInflightMsgs:           rn.config.MaxInflightMsgs,
		CheckQuorum:               true, // 启用法定人数检查
		PreVote:                   true, // 启用预投票
		DisableProposalForwarding: false,
	}

	// 重启节点
	rn.mu.Lock()
	rn.node = raft.RestartNode(raftConfig)
	rn.mu.Unlock()

	// 启动主循环
	go rn.serveRaft()
	// 启动提议处理
	go rn.serveProposals()
	// 启动定时任务
	go rn.serveScheduledTasks()

	fmt.Printf("节点 %d 从宕机中恢复成功\n", rn.config.NodeID)
	return nil
}

// Restart 重启节点
func (rn *RaftNode) Restart() error {
	// 重置停止标志
	atomic.StoreInt32(&rn.stopped, 0)

	// 重新创建通道
	rn.stopc = make(chan struct{})
	rn.proposeCh = make(chan []byte)
	rn.confChangeCh = make(chan raftpb.ConfChange)
	rn.readIndexCh = make(chan *readIndexRequest)
	rn.confChangeCompleted = make(chan struct{})

	// 重新创建上下文
	rn.ctx, rn.cancel = context.WithCancel(context.Background())

	// 启动节点
	return rn.Start()
}

// GetState 获取节点状态
func (rn *RaftNode) GetState() raft.StateType {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.state
}

// Get 获取键值对（使用 ReadIndex 确保一致性读）
func (rn *RaftNode) Get(ctx context.Context, key string) (string, error) {
	// 获取当前的提交索引
	commitIndex := rn.GetCommitIndex()
	if commitIndex == 0 {
		value, err := rn.fsm.Get(key)
		if err != nil {
			return "", fmt.Errorf("从状态机读取失败: %v", err)
		}
		fmt.Printf("节点 %d 刚启动，直接读取键 %s = %s\n", rn.config.NodeID, key, value)
		return value, nil
	}

	appliedIndex := rn.GetAppliedIndex()
	//
	if commitIndex > 0 && (commitIndex-appliedIndex <= 5 || appliedIndex == 0) {
		value, err := rn.fsm.Get(key)
		if err != nil {
			return "", fmt.Errorf("从状态机读取失败: %v", err)
		}
		if value != "" {
			return value, nil
		}
	}
	//

	// 使用 ReadIndex 确保一致性读
	err := rn.ReadIndex(ctx, []byte(key))
	if err != nil {
		fmt.Printf("节点 %d 读索引失败: %v，尝试使用当前提交索引\n", rn.config.NodeID, err)
		// 如果ReadIndex失败，我们直接使用当前已知的提交索引
	}

	// 等待状态机应用到最新的索引，设置更长的超时时间
	if err := rn.fsm.WaitForIndex(commitIndex); err != nil {
		fmt.Printf("节点 %d 等待索引 %d 失败: %v，尝试直接读取\n",
			rn.config.NodeID, commitIndex, err)
		// 尝试直接读取，但提示这可能不是最新的数据
		value, readErr := rn.fsm.Get(key)
		if readErr != nil {
			return "", fmt.Errorf("读取失败: %v (等待索引错误: %v)", readErr, err)
		}
		return value, nil
	}

	// 从状态机读取
	value, err := rn.fsm.Get(key)
	if err != nil {
		return "", fmt.Errorf("从状态机读取失败: %v", err)
	}

	fmt.Printf("节点 %d 成功读取键 %s = %s，索引 %d\n",
		rn.config.NodeID, key, value, commitIndex)
	return value, nil
}

// sendMessages 发送消息到其他节点
func (rn *RaftNode) sendMessages(msgs []raftpb.Message) {
	// 检查transport是否为nil
	if rn.transport == nil {
		//第一条消息不是选举相关消息（MsgVote 或 MsgPreVote与投票）
		if len(msgs) > 0 && msgs[0].Type != raftpb.MsgVote && msgs[0].Type != raftpb.MsgPreVote {
			// 只对非选举消息记录警告，减少日志量
			log.Printf("警告: transport为nil，无法发送消息")
		}
		return
	}

	for _, msg := range msgs {
		// 跳过发给自己的消息
		if msg.To == rn.config.NodeID {
			continue
		}

		// 检查目标节点是否在集群中
		rn.mu.RLock()
		_, exists := rn.config.Peers[msg.To]
		rn.mu.RUnlock()

		if !exists {
			// 目标节点不在集群中，跳过发送
			continue
		}

		// 对于非关键消息，如果节点离线，则减少重试次数
		if rn.transport.IsPeerOffline(msg.To) &&
			(msg.Type == raftpb.MsgVote || msg.Type == raftpb.MsgPreVote ||
				msg.Type == raftpb.MsgHeartbeat || msg.Type == raftpb.MsgHeartbeatResp) {
			// 对于心跳和选举消息，如果节点已知离线，则跳过发送
			continue
		}

		// 发送消息，忽略错误（transport会处理重试和错误日志）
		rn.transport.Send(msg)
	}
}

// applyConfChange 应用配置变更
func (rn *RaftNode) applyConfChange(cc raftpb.ConfChange) {
	// 应用配置变更
	confState := rn.node.ApplyConfChange(cc)
	rn.mu.Lock()
	rn.confState = *confState
	rn.mu.Unlock()

	// 处理配置变更
	switch cc.Type {
	case raftpb.ConfChangeAddNode:
		if cc.NodeID != rn.config.NodeID {
			if len(cc.Context) > 0 {
				rn.transport.AddPeer(cc.NodeID, string(cc.Context))
				rn.mu.Lock()
				rn.config.Peers[cc.NodeID] = string(cc.Context)
				rn.mu.Unlock()
				log.Printf("节点 %d 添加了对等节点 %d -> %s", rn.config.NodeID, cc.NodeID, string(cc.Context))
			}
		}
	case raftpb.ConfChangeRemoveNode:
		if cc.NodeID != rn.config.NodeID {
			rn.transport.RemovePeer(cc.NodeID)
			// 从配置中删除对等节点
			rn.mu.Lock()
			delete(rn.config.Peers, cc.NodeID)
			rn.mu.Unlock()
			log.Printf("节点 %d 移除了对等节点 %d", rn.config.NodeID, cc.NodeID)
		}
	}

	// 通知配置变更完成
	select {
	case rn.confChangeCompleted <- struct{}{}:
	default:
	}
}

// serveRaft 处理 Raft 节点的主循环
func (rn *RaftNode) serveRaft() {
	// 添加panic恢复
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("节点 %d 的serveRaft方法出现panic: %v\n", rn.config.NodeID, r)
		}
	}()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-rn.stopc:
			return
		case <-ticker.C:
			rn.mu.RLock()
			if rn.node == nil {
				rn.mu.RUnlock()
				return
			}
			rn.node.Tick() //tcd Raft 库的方法，模拟时间流逝，触发心跳或选举超时。每次调用相当于增加一个时间刻度（tick），用于 Raft 的定时机制，Tick 是 Raft 协议的核心驱动，控制节点状态（如从 Follower 到 Candidate）
			rn.mu.RUnlock()
		case rd, ok := <-rn.node.Ready(): // raft.Node.Ready() chan raft.Ready：返回一个通道，包含 Raft 节点的最新状态（如日志、消息、快照，已经提交的日志条目，赢状态和软状态（如领导者状态））
			if !ok {
				return //如果 Ready 通道已关闭（ok == false），退出方法，表示 Raft 节点停止
			}

			// 先检查所有关键组件，是否为空
			rn.mu.RLock()
			nodeNil := rn.node == nil
			transportNil := rn.transport == nil
			storageNil := rn.storage == nil
			fsmNil := rn.fsm == nil
			rn.mu.RUnlock()

			if nodeNil {
				log.Printf("警告: node为nil，无法处理Ready状态")
				continue
			}

			if storageNil {
				log.Printf("警告: storage为nil，无法处理Ready状态")
				continue
			}

			// 保存状态
			if err := rn.storage.SaveEntries(rd.Entries); err != nil { //保存未提交的日志条目（rd.Entries）
				log.Printf("保存日志条目失败: %v", err)
				continue
			}

			// 保存硬状态
			if !raft.IsEmptyHardState(rd.HardState) {
				if err := rn.storage.SaveRaftState(rd.HardState); err != nil {
					log.Printf("保存硬状态失败: %v", err)
					continue
				}

				// 更新提交索引
				if rd.HardState.Commit > rn.commitIndex {
					rn.mu.Lock()
					rn.commitIndex = rd.HardState.Commit
					rn.mu.Unlock()
				}

				// 更新任期
				if rd.HardState.Term > rn.term {
					rn.mu.Lock()
					rn.term = rd.HardState.Term
					rn.mu.Unlock()
				}
			}

			// 处理快照
			if !raft.IsEmptySnap(rd.Snapshot) {
				if err := rn.storage.SaveSnapshot(rd.Snapshot); err != nil {
					log.Printf("保存快照失败: %v", err)
					continue
				}

				if fsmNil {
					log.Printf("警告: fsm为nil，无法应用快照")
					continue
				}

				if err := rn.publishSnapshot(rd.Snapshot); err != nil {
					log.Printf("应用快照失败: %v", err)
					continue
				}
			}

			// 发送消息
			if len(rd.Messages) > 0 {
				if transportNil {
					log.Printf("警告: transport为nil，无法发送消息")
				} else {
					rn.sendMessages(rd.Messages)
				}
			}

			// 应用已提交的条目
			if len(rd.CommittedEntries) > 0 {
				if fsmNil {
					log.Printf("警告: fsm为nil，无法应用提交的条目")
					continue
				}

				if err := rn.publishEntries(rd.CommittedEntries); err != nil {
					log.Printf("应用日志条目失败: %v", err)
					continue
				}
			}

			// 更新状态
			if rd.SoftState != nil {
				rn.mu.Lock()
				prevState := rn.state //保存当前状态和领导者 ID，用于后续比较
				prevLeader := rn.currentLeader

				rn.state = rd.SoftState.RaftState
				rn.isLeader = rd.SoftState.RaftState == raft.StateLeader

				// 更新领导者ID
				if rd.SoftState.Lead != rn.currentLeader {
					rn.currentLeader = rd.SoftState.Lead
					log.Printf("节点 %d 更新领导者为 %d", rn.config.NodeID, rd.SoftState.Lead)
				}

				// 如果是领导者，更新心跳时间
				if rn.isLeader {
					rn.lastHeartbeat = time.Now()

					// 如果状态从跟随者变为领导者，记录日志
					if prevState != raft.StateLeader {
						log.Printf("节点 %d 成为领导者，任期: %d", rn.config.NodeID, rn.term)
					}
				} else if prevState == raft.StateLeader && rn.state != raft.StateLeader {
					// 如果从领导者变为非领导者，记录日志
					log.Printf("节点 %d 不再是领导者，新领导者: %d", rn.config.NodeID, rd.SoftState.Lead)
				}

				// 如果领导者发生变化，记录日志，之前的领导者现在不是了
				if prevLeader != rd.SoftState.Lead && rd.SoftState.Lead != 0 {
					log.Printf("节点 %d 检测到领导者变更: %d -> %d", rn.config.NodeID, prevLeader, rd.SoftState.Lead)
				}

				rn.mu.Unlock()
			}

			// 通知处理完成
			if !nodeNil {
				rn.node.Advance() //raft.Node.Advance()：通知 Raft 节点已处理完当前 Ready 数据，可以准备下一轮状态更新。确保状态机向前
			}
		}
	}
}

// GetLeaderAddr 获取当前领导者的地址
func (rn *RaftNode) GetLeaderAddr() string {
	rn.mu.RLock()
	defer rn.mu.RUnlock()

	// 如果当前节点是领导者，返回自己的地址
	if rn.isLeader {
		return rn.config.ListenAddr
	}

	// 如果知道领导者ID，从peers列表中查找对应地址
	if rn.currentLeader != 0 {
		if addr, ok := rn.config.Peers[rn.currentLeader]; ok {
			return addr
		}
	}

	return ""
}

// GetClusterMembers 获取当前集群成员
func (rn *RaftNode) GetClusterMembers() map[uint64]string {
	rn.mu.RLock()
	defer rn.mu.RUnlock()

	// 创建一个副本，避免并发修改问题
	members := make(map[uint64]string)
	for id, addr := range rn.config.Peers {
		members[id] = addr
	}

	return members
}

// AddPeer 添加对等节点
func (rn *RaftNode) AddPeer(id uint64, addr string) error {
	// 如果节点ID相同，忽略
	if id == rn.config.NodeID {
		return nil
	}

	// 更新配置
	rn.mu.Lock()
	rn.config.Peers[id] = addr
	rn.mu.Unlock()

	// 添加传输层连接
	if rn.transport != nil {
		return rn.transport.AddPeer(id, addr)
	}

	return nil
}
