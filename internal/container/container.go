package container

import (
	"fmt"
	"sync"

	"qklzl/internal/fsm"
	"qklzl/internal/node"
	"qklzl/internal/repository"
	"qklzl/internal/service"
)

// Container 依赖注入容器
type Container struct {
	repoFactory repository.RepositoryFactory
	fsm         fsm.StateMachine
	raftNode    *node.RepositoryRaftNode
	kvService   *service.KVService
	mu          sync.RWMutex
	closed      bool
}

// Config 容器配置
type Config struct {
	DataDir   string
	NodeID    uint64
	ClusterID uint64
	Address   string
}

// NewContainer 创建依赖注入容器
func NewContainer(config Config) (*Container, error) {
	if config.DataDir == "" {
		return nil, fmt.Errorf("数据目录不能为空")
	}
	if config.NodeID == 0 {
		return nil, fmt.Errorf("节点ID不能为0")
	}
	if config.ClusterID == 0 {
		return nil, fmt.Errorf("集群ID不能为0")
	}

	// 创建仓库工厂
	repoFactory, err := repository.NewRepositoryFactory(config.DataDir)
	if err != nil {
		return nil, fmt.Errorf("创建仓库工厂失败: %v", err)
	}

	// 创建状态机
	fsm := fsm.NewRepositoryStateMachine(repoFactory)

	// 创建Raft节点配置
	raftConfig := node.RaftNodeConfig{
		NodeID:              config.NodeID,
		ClusterID:           config.ClusterID,
		ElectionTick:        10,
		HeartbeatTick:       1,
		SnapshotCount:       10000,
		MaxSizePerMsg:       1024 * 1024,
		MaxInflightMsgs:     256,
		CheckpointInterval:  30,
	}

	// 创建Raft节点
	raftNode, err := node.NewRepositoryRaftNode(raftConfig, fsm, repoFactory)
	if err != nil {
		return nil, fmt.Errorf("创建Raft节点失败: %v", err)
	}

	// 创建KV服务
	kvService := service.NewKVService(raftNode, fsm, repoFactory)

	return &Container{
		repoFactory: repoFactory,
		fsm:         fsm,
		raftNode:    raftNode,
		kvService:   kvService,
	}, nil
}

// GetRepositoryFactory 获取仓库工厂
func (c *Container) GetRepositoryFactory() repository.RepositoryFactory {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.repoFactory
}

// GetStateMachine 获取状态机
func (c *Container) GetStateMachine() fsm.StateMachine {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.fsm
}

// GetRaftNode 获取Raft节点
func (c *Container) GetRaftNode() *node.RepositoryRaftNode {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.raftNode
}

// GetKVService 获取KV服务
func (c *Container) GetKVService() *service.KVService {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.kvService
}

// Start 启动容器中的所有服务
func (c *Container) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return fmt.Errorf("容器已关闭")
	}

	// 启动KV服务（会自动启动Raft节点）
	if err := c.kvService.Start(); err != nil {
		return fmt.Errorf("启动KV服务失败: %v", err)
	}

	fmt.Println("依赖注入容器启动成功")
	return nil
}

// Stop 停止容器中的所有服务
func (c *Container) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return
	}

	// 停止KV服务
	if c.kvService != nil {
		c.kvService.Stop()
	}

	// 关闭状态机
	if c.fsm != nil {
		if closer, ok := c.fsm.(interface{ Close() error }); ok {
			closer.Close()
		}
	}

	// 关闭仓库工厂
	if c.repoFactory != nil {
		c.repoFactory.Close()
	}

	c.closed = true
	fmt.Println("依赖注入容器已停止")
}

// HealthCheck 健康检查
func (c *Container) HealthCheck() error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return fmt.Errorf("容器已关闭")
	}

	// 检查仓库工厂
	if err := c.repoFactory.HealthCheck(); err != nil {
		return fmt.Errorf("仓库工厂健康检查失败: %v", err)
	}

	return nil
}

// IsClosed 检查容器是否已关闭
func (c *Container) IsClosed() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.closed
}

// GetConfig 获取容器配置信息
func (c *Container) GetConfig() map[string]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	config := make(map[string]interface{})
	if c.raftNode != nil {
		config["node_id"] = c.raftNode.GetNodeID()
		config["cluster_id"] = c.raftNode.GetClusterID()
	}
	config["closed"] = c.closed

	return config
}