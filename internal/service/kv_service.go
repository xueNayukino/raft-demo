package service

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"go.etcd.io/etcd/raft/v3/raftpb"

	"qklzl/internal/fsm"
	"qklzl/internal/node"
	"qklzl/internal/repository"
)

// KVService 键值服务，封装业务逻辑
type KVService struct {
	raftNode    *node.RepositoryRaftNode
	fsm         fsm.StateMachine
	repoFactory repository.RepositoryFactory
	kvRepo      repository.KVRepository
	mu          sync.RWMutex
	started     bool
}

// NewKVService 创建键值服务
func NewKVService(raftNode *node.RepositoryRaftNode, fsm fsm.StateMachine, factory repository.RepositoryFactory) *KVService {
	return &KVService{
		raftNode:    raftNode,
		fsm:         fsm,
		repoFactory: factory,
		kvRepo:      factory.GetKVRepository(),
	}
}

// Start 启动服务
func (s *KVService) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.started {
		return fmt.Errorf("服务已启动")
	}

	if err := s.raftNode.Start(); err != nil {
		return fmt.Errorf("启动Raft节点失败: %v", err)
	}

	s.started = true
	fmt.Println("KV服务启动成功")
	return nil
}

// Stop 停止服务
func (s *KVService) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started {
		return
	}

	s.raftNode.Stop()
	s.started = false
	fmt.Println("KV服务已停止")
}

// Put 设置键值对
func (s *KVService) Put(ctx context.Context, key, value string) error {
	if !s.started {
		return fmt.Errorf("服务未启动")
	}

	// 检查是否为领导者
	if !s.raftNode.IsLeader() {
		return fmt.Errorf("当前节点不是领导者")
	}

	// 创建命令
	cmd := fsm.Command{
		Op:    fsm.OpPut,
		Key:   key,
		Value: value,
	}

	// 序列化命令
	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("序列化命令失败: %v", err)
	}

	// 提议到Raft
	if err := s.raftNode.Propose(data); err != nil {
		return fmt.Errorf("提议失败: %v", err)
	}

	fmt.Printf("PUT操作提议成功: 键=%s, 值=%s\n", key, value)
	return nil
}

// Get 获取键值
func (s *KVService) Get(ctx context.Context, key string) (string, error) {
	if !s.started {
		return "", fmt.Errorf("服务未启动")
	}

	// 如果是领导者，执行一致性读
	if s.raftNode.IsLeader() {
		return s.consistentGet(ctx, key)
	}

	// 非领导者直接从本地状态机读取
	return s.fsm.Get(key)
}

// Delete 删除键
func (s *KVService) Delete(ctx context.Context, key string) error {
	if !s.started {
		return fmt.Errorf("服务未启动")
	}

	// 检查是否为领导者
	if !s.raftNode.IsLeader() {
		return fmt.Errorf("当前节点不是领导者")
	}

	// 创建命令
	cmd := fsm.Command{
		Op:  fsm.OpDelete,
		Key: key,
	}

	// 序列化命令
	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("序列化命令失败: %v", err)
	}

	// 提议到Raft
	if err := s.raftNode.Propose(data); err != nil {
		return fmt.Errorf("提议失败: %v", err)
	}

	fmt.Printf("DELETE操作提议成功: 键=%s\n", key)
	return nil
}

// BatchPut 批量设置键值对
func (s *KVService) BatchPut(ctx context.Context, kvs map[string]string) error {
	if !s.started {
		return fmt.Errorf("服务未启动")
	}

	// 检查是否为领导者
	if !s.raftNode.IsLeader() {
		return fmt.Errorf("当前节点不是领导者")
	}

	// 构建批量操作
	var ops []repository.KVOperation
	for key, value := range kvs {
		ops = append(ops, repository.KVOperation{
			Type:  "put",
			Key:   key,
			Value: value,
		})
	}

	// 执行批量操作
	if err := s.kvRepo.Batch(ctx, ops); err != nil {
		return fmt.Errorf("批量操作失败: %v", err)
	}

	fmt.Printf("批量PUT操作成功: %d个键值对\n", len(kvs))
	return nil
}

// GetAll 获取所有键值对
func (s *KVService) GetAll(ctx context.Context) (map[string]string, error) {
	if !s.started {
		return nil, fmt.Errorf("服务未启动")
	}

	return s.kvRepo.GetAll(ctx)
}

// consistentGet 执行一致性读
func (s *KVService) consistentGet(ctx context.Context, key string) (string, error) {
	// 执行ReadIndex
	rctx := []byte(fmt.Sprintf("read-%d", time.Now().UnixNano()))
	if err := s.raftNode.ReadIndex(ctx, rctx); err != nil {
		return "", fmt.Errorf("ReadIndex失败: %v", err)
	}

	// 从状态机读取
	return s.fsm.Get(key)
}

// AddNode 添加节点到集群
func (s *KVService) AddNode(ctx context.Context, nodeID uint64, address string) error {
	if !s.started {
		return fmt.Errorf("服务未启动")
	}

	// 检查是否为领导者
	if !s.raftNode.IsLeader() {
		return fmt.Errorf("当前节点不是领导者")
	}

	// 创建配置变更
	cc := raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  nodeID,
		Context: []byte(address),
	}

	// 提议配置变更
	if err := s.raftNode.ProposeConfChange(cc); err != nil {
		return fmt.Errorf("提议配置变更失败: %v", err)
	}

	fmt.Printf("添加节点提议成功: 节点ID=%d, 地址=%s\n", nodeID, address)
	return nil
}

// RemoveNode 从集群移除节点
func (s *KVService) RemoveNode(ctx context.Context, nodeID uint64) error {
	if !s.started {
		return fmt.Errorf("服务未启动")
	}

	// 检查是否为领导者
	if !s.raftNode.IsLeader() {
		return fmt.Errorf("当前节点不是领导者")
	}

	// 创建配置变更
	cc := raftpb.ConfChange{
		Type:   raftpb.ConfChangeRemoveNode,
		NodeID: nodeID,
	}

	// 提议配置变更
	if err := s.raftNode.ProposeConfChange(cc); err != nil {
		return fmt.Errorf("提议配置变更失败: %v", err)
	}

	fmt.Printf("移除节点提议成功: 节点ID=%d\n", nodeID)
	return nil
}

// GetClusterStatus 获取集群状态
func (s *KVService) GetClusterStatus() map[string]interface{} {
	status := s.raftNode.GetStatus()
	return map[string]interface{}{
		"node_id":     s.raftNode.GetNodeID(),
		"cluster_id":  s.raftNode.GetClusterID(),
		"is_leader":   s.raftNode.IsLeader(),
		"leader_id":   status.Lead,
		"term":        status.Term,
		"commit":      status.Commit,
		"applied":     status.Applied,
		"progress":    status.Progress,
		"started":     s.started,
	}
}

// CreateSnapshot 创建快照
func (s *KVService) CreateSnapshot(ctx context.Context) error {
	if !s.started {
		return fmt.Errorf("服务未启动")
	}

	// 检查是否为领导者
	if !s.raftNode.IsLeader() {
		return fmt.Errorf("当前节点不是领导者")
	}

	// 创建快照
	snapshot, err := s.fsm.CreateSnapshot()
	if err != nil {
		return fmt.Errorf("创建快照失败: %v", err)
	}

	// 存储快照
	snapRepo := s.repoFactory.GetSnapshotRepository()
	if err := snapRepo.StoreSnapshot(snapshot); err != nil {
		return fmt.Errorf("存储快照失败: %v", err)
	}

	fmt.Printf("快照创建成功: 索引=%d\n", snapshot.Metadata.Index)
	return nil
}

// HealthCheck 健康检查
func (s *KVService) HealthCheck(ctx context.Context) error {
	if !s.started {
		return fmt.Errorf("服务未启动")
	}

	// 检查仓库健康状态
	if err := s.repoFactory.HealthCheck(); err != nil {
		return fmt.Errorf("仓库健康检查失败: %v", err)
	}

	return nil
}

// Close 关闭服务
func (s *KVService) Close() error {
	s.Stop()
	return nil
}