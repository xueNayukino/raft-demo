package node

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"qklzl/fsm"
	"qklzl/storage"
	"qklzl/transport"

	"github.com/dgraph-io/badger/v3"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

const (
	// 心跳间隔
	defaultTickInterval = 100 * time.Millisecond
	// 选举超时范围
	minElectionTimeout = 5000
	maxElectionTimeout = 10000
)

// 进度跟踪结构体
type Progress struct {
	Match uint64 // 已复制的最大日志索引
	Next  uint64 // 下一个要发送的日志索引
}

// RaftNode 表示一个 Raft 节点
type RaftNode struct {
	// 节点配置
	id           uint64
	peers        []uint64
	storageDir   string
	storage      *storage.BadgerStorage
	stateMachine *fsm.KVStateMachine
	transport    *transport.Transport
	rawNode      *raft.RawNode
	ctx          context.Context
	cancel       context.CancelFunc
	proposeC     chan []byte
	confChangeC  chan raftpb.ConfChange
	errorC       chan error
	mu           sync.RWMutex
	db           *badger.DB

	// 进度跟踪
	progress map[uint64]Progress

	// 配置
	config *raft.Config
}

// NewRaftNode 创建新的 Raft 节点
func NewRaftNode(
	id uint64,
	peers []uint64,
	dataDir string,
	addr string,
	peerAddrs map[uint64]string,
) (*RaftNode, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// 创建 BadgerDB
	opts := badger.DefaultOptions(dataDir).WithInMemory(false)
	db, err := badger.Open(opts)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("打开 BadgerDB 失败: %v", err)
	}
	// 初始化状态机
	stateMachine := fsm.NewKVStateMachine(db)
	// 初始化存储
	badgerStore, err := storage.NewBadgerStorage(dataDir, nil, stateMachine)
	if err != nil {
		db.Close()
		cancel()
		return nil, fmt.Errorf("创建存储失败: %v", err)
	}

	// 检查是否有已存在的状态
	hardState, confState, err := badgerStore.InitialState()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("获取初始状态失败: %v", err)
	}

	// 如果是新节点，初始化状态
	if raft.IsEmptyHardState(hardState) {
		// 初始化配置状态
		confState = raftpb.ConfState{
			Voters: peers,
		}

		// 保存配置状态
		if err := badgerStore.SetConfState(&confState); err != nil {
			cancel()
			return nil, fmt.Errorf("设置配置状态失败: %v", err)
		}

		// 创建初始日志条目
		entries := []raftpb.Entry{{
			Term:  1,
			Index: 1,
			Type:  raftpb.EntryNormal,
		}}

		// 保存初始日志条目
		if err := badgerStore.SaveEntries(entries); err != nil {
			cancel()
			return nil, fmt.Errorf("保存初始日志条目失败: %v", err)
		}

		// 设置初始硬状态
		hardState = raftpb.HardState{
			Term:   1,
			Commit: 1,
		}

		// 保存初始硬状态
		if err := badgerStore.SaveState(hardState, confState); err != nil {
			cancel()
			return nil, fmt.Errorf("保存初始状态失败: %v", err)
		}
	}

	// Raft 配置
	config := &raft.Config{
		ID:              id,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         badgerStore,
		MaxSizePerMsg:   1024 * 1024, // 1MB
		MaxInflightMsgs: 256,
		Applied:         hardState.Commit, // 使用硬状态中的提交索引
		PreVote:         true,             // 支持预投票
	}

	// 创建 RawNode
	rawNode, err := raft.NewRawNode(config)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("创建 RawNode 失败: %v", err)
	}

	node := &RaftNode{
		id:           id,
		peers:        peers,
		storageDir:   dataDir,
		storage:      badgerStore,
		stateMachine: stateMachine,
		ctx:          ctx,
		cancel:       cancel,
		proposeC:     make(chan []byte, 1024),
		confChangeC:  make(chan raftpb.ConfChange, 16),
		errorC:       make(chan error, 256),
		rawNode:      rawNode,
		db:           db,
	}

	// 初始化进度跟踪
	lastIndex := hardState.Commit
	for _, peer := range peers {
		node.progress[peer] = Progress{
			Match: lastIndex,
			Next:  lastIndex + 1,
		}
	}

	// 初始化网络层
	trans, err := transport.NewTransport(id, addr, node)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("创建 Transport 失败: %v", err)
	}
	for pid, paddr := range peerAddrs {
		if pid != id {
			trans.AddPeer(pid, paddr)
		}
	}
	node.transport = trans

	go node.run()
	return node, nil
}

// 随机生成选举超时时间
func randomElectionTimeout() int {
	return minElectionTimeout +
		int(time.Now().UnixNano()%int64(maxElectionTimeout-minElectionTimeout))
}

// Start 启动 Raft 节点
func (rn *RaftNode) Start() {
	go rn.run()
}

// Stop 停止 Raft 节点
func (rn *RaftNode) Stop() {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	select {
	case <-rn.ctx.Done():
		// 已经停止
		return
	default:
		// 关闭上下文
		rn.cancel()
		// 关闭存储
		if rn.storage != nil {
			rn.storage.Stop()
		}
		// 清理资源
		close(rn.proposeC)
		close(rn.confChangeC)
		close(rn.errorC)
		// 关闭底层 BadgerDB
		if rn.db != nil {
			rn.db.Close()
			// 添加短暂延迟，确保文件锁被完全释放
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// Propose 提交新的日志条目
func (rn *RaftNode) Propose(data []byte) error {
	select {
	case rn.proposeC <- data:
		return nil
	case <-rn.ctx.Done():
		return fmt.Errorf("节点已停止")
	}
}

// ProposeConfChange 提交配置变更
func (rn *RaftNode) ProposeConfChange(cc raftpb.ConfChange) error {
	select {
	case rn.confChangeC <- cc:
		return nil
	case <-rn.ctx.Done():
		return fmt.Errorf("节点已停止")
	}
}

// run 主循环处理 Raft 节点逻辑
func (rn *RaftNode) run() {
	ticker := time.NewTicker(defaultTickInterval)
	defer ticker.Stop()

	// 如果是单节点模式，立即启动选举
	if len(rn.peers) == 1 && rn.peers[0] == rn.id {
		rn.mu.Lock()
		if rn.rawNode != nil {
			rn.rawNode.Campaign()
		}
		rn.mu.Unlock()
	}

	for {
		select {
		case <-rn.ctx.Done():
			return

		case <-ticker.C:
			rn.mu.Lock()
			if rn.rawNode != nil {
				rn.rawNode.Tick()
			}
			rn.mu.Unlock()

			// 处理 Ready 状态
			rn.processReady()

		case prop := <-rn.proposeC:
			rn.mu.Lock()
			if rn.rawNode != nil && rn.rawNode.Status().RaftState == raft.StateLeader {
				if err := rn.rawNode.Propose(prop); err != nil {
					log.Printf("提交日志失败: %v", err)
				}
			} else {
				log.Printf("节点 %d 不是 leader，无法提交日志", rn.id)
			}
			rn.mu.Unlock()

			// 处理 Ready 状态
			rn.processReady()

		case cc := <-rn.confChangeC:
			rn.mu.Lock()
			if rn.rawNode != nil && rn.rawNode.Status().RaftState == raft.StateLeader {
				if err := rn.rawNode.ProposeConfChange(cc); err != nil {
					log.Printf("提议配置变更失败: %v", err)
				}
			} else {
				log.Printf("节点 %d 不是 leader，无法处理配置变更", rn.id)
			}
			rn.mu.Unlock()

			// 处理 Ready 状态
			rn.processReady()
		}
	}
}

// processReady 处理 Ready 状态
func (rn *RaftNode) processReady() {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if !rn.rawNode.HasReady() {
		return
	}

	// 获取 Ready
	rd := rn.rawNode.Ready()

	// 1. 持久化日志条目和状态
	if !raft.IsEmptyHardState(rd.HardState) {
		confState, err := rn.storage.GetConfState()
		if err != nil {
			log.Printf("获取配置状态失败: %v", err)
			return
		}
		if err := rn.storage.SaveState(rd.HardState, *confState); err != nil {
			log.Printf("节点 %d 持久化失败: %v", rn.id, err)
			return
		}
	}

	// 保存新的日志条目
	if len(rd.Entries) > 0 {
		if err := rn.storage.SaveEntries(rd.Entries); err != nil {
			log.Printf("节点 %d 保存日志条目失败: %v", rn.id, err)
			return
		}
	}

	// 2. 应用已提交的日志条目
	if len(rd.CommittedEntries) > 0 {
		if err := rn.applyCommittedEntries(rd.CommittedEntries); err != nil {
			log.Printf("节点 %d 应用日志失败: %v", rn.id, err)
			return
		}
	}

	// 3. 处理快照
	if !raft.IsEmptySnap(rd.Snapshot) {
		if err := rn.storage.SaveSnapshot(rd.Snapshot); err != nil {
			log.Printf("节点 %d 保存快照失败: %v", rn.id, err)
			return
		}
		if err := rn.stateMachine.RestoreSnapshot(rd.Snapshot.Data); err != nil {
			log.Printf("节点 %d 恢复快照到状态机失败: %v", rn.id, err)
			return
		}
	}

	// 4. 发送消息
	if len(rd.Messages) > 0 {
		if err := rn.sendMessages(rd.Messages); err != nil {
			log.Printf("节点 %d 发送消息失败: %v", rn.id, err)
			return
		}
	}

	// 5. 更新进度
	for _, msg := range rd.Messages {
		if msg.Type == raftpb.MsgAppResp {
			if msg.Reject {
				if p, ok := rn.progress[msg.From]; ok {
					p.Next = msg.RejectHint
					rn.progress[msg.From] = p
				}
			} else {
				if p, ok := rn.progress[msg.From]; ok {
					p.Match = msg.Index
					p.Next = p.Match + 1
					rn.progress[msg.From] = p
				}
			}
		}
	}

	// 6. 推进状态机
	rn.rawNode.Advance(rd)
}

// applyCommittedEntries 应用已提交的日志条目
func (rn *RaftNode) applyCommittedEntries(entries []raftpb.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	rn.mu.Lock()
	defer rn.mu.Unlock()

	for _, entry := range entries {
		switch entry.Type {
		case raftpb.EntryNormal:
			if len(entry.Data) == 0 {
				continue
			}
			if err := rn.stateMachine.Apply(entry); err != nil {
				return fmt.Errorf("应用日志到状态机失败: %v", err)
			}
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			if err := cc.Unmarshal(entry.Data); err != nil {
				return fmt.Errorf("解析配置变更失败: %v", err)
			}
			cs := rn.rawNode.ApplyConfChange(cc)
			confState := &raftpb.ConfState{
				Voters: cs.Voters,
			}
			if err := rn.storage.SetConfState(confState); err != nil {
				return fmt.Errorf("保存配置状态失败: %v", err)
			}
			// 根据配置变更类型更新节点信息
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				if _, exists := rn.progress[cc.NodeID]; !exists {
					rn.progress[cc.NodeID] = Progress{
						Match: 0,
						Next:  1,
					}
					// 检查是否已经存在该节点
					exists := false
					for _, id := range rn.peers {
						if id == cc.NodeID {
							exists = true
							break
						}
					}
					if !exists {
						rn.peers = append(rn.peers, cc.NodeID)
					}
				}
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == rn.id {
					log.Printf("节点被移除，准备关闭...")
					rn.Stop()
					return nil
				}
				delete(rn.progress, cc.NodeID)
				// 从 peers 列表中移除节点
				newPeers := make([]uint64, 0, len(rn.peers)-1)
				for _, id := range rn.peers {
					if id != cc.NodeID {
						newPeers = append(newPeers, id)
					}
				}
				rn.peers = newPeers
			}
		}
	}

	return nil
}

// sendMessages 发送消息到其他节点
func (rn *RaftNode) sendMessages(msgs []raftpb.Message) error {
	for _, msg := range msgs {
		if err := rn.transport.Send(msg); err != nil {
			return fmt.Errorf("发送消息失败 [type=%v]: %v", msg.Type, err)
		}
	}
	return nil
}

// sendHeartbeat 发送心跳消息
func (rn *RaftNode) sendHeartbeat(msg raftpb.Message) error {
	return rn.transport.Send(msg)
}

// sendAppendEntries 发送追加日志消息
func (rn *RaftNode) sendAppendEntries(msg raftpb.Message) error {
	return rn.transport.Send(msg)
}

// sendRequestVote 发送投票请求消息
func (rn *RaftNode) sendRequestVote(msg raftpb.Message) error {
	return rn.transport.Send(msg)
}

// Step 实现 transport.MessageHandler 接口
func (n *RaftNode) HandleMessage(msg raftpb.Message) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.rawNode.Step(msg)
}

// Tick 驱动 raft
func (n *RaftNode) tick() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.rawNode.Tick()
}

// TransferLeader 主动转移领导权
func (rn *RaftNode) TransferLeader(targetID uint64) error {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	if rn.rawNode == nil {
		return fmt.Errorf("RawNode 未初始化")
	}
	rn.rawNode.TransferLeader(targetID)
	return nil
}

// ReadIndex 发起只读请求，返回已提交索引
func (rn *RaftNode) ReadIndex(ctx []byte) (uint64, error) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	if rn.rawNode == nil {
		return 0, fmt.Errorf("RawNode 未初始化")
	}
	rn.rawNode.ReadIndex(ctx)
	// 简单实现：等待 Ready 中的 ReadState
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for i := 0; i < 30; i++ { // 最多等3秒
		select {
		case <-ticker.C:
			rd := rn.rawNode.Ready()
			if len(rd.ReadStates) > 0 {
				return rd.ReadStates[0].Index, nil
			}
			rn.rawNode.Advance(rd)
		}
	}
	return 0, fmt.Errorf("ReadIndex 超时")
}

// TriggerSnapshot 主动触发快照
func (rn *RaftNode) TriggerSnapshot() error {
	if rn.storage == nil {
		return fmt.Errorf("存储未初始化")
	}
	lastIndex, err := rn.storage.LastIndex()
	if err != nil {
		return err
	}
	snap, err := rn.storage.Snapshot()
	if err != nil {
		return err
	}
	if snap.Metadata.Index == lastIndex {
		return fmt.Errorf("无需快照，已是最新")
	}
	return rn.storage.SaveSnapshot(snap)
}

// Status 获取当前节点状态
func (rn *RaftNode) Status() raft.Status {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	if rn.rawNode == nil {
		return raft.Status{}
	}
	return rn.rawNode.Status()
}

// GetLeader 获取当前 leader
func (rn *RaftNode) GetLeader() uint64 {
	return rn.Status().Lead
}

// GetTerm 获取当前任期
func (rn *RaftNode) GetTerm() uint64 {
	return rn.Status().Term
}

// GetProgress 获取所有节点进度
func (rn *RaftNode) GetProgress() map[uint64]Progress {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	result := make(map[uint64]Progress)
	for k, v := range rn.progress {
		result[k] = v
	}
	return result
}
