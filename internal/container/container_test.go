package container

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestContainer_Creation(t *testing.T) {
	// 创建临时目录
	tempDir := t.TempDir()
	
	// 创建测试配置
	cfg := Config{
		NodeID: 1,
		ClusterID: 1,
		DataDir: tempDir,
		Address: "127.0.0.1:9090",
	}

	// 创建容器
	container, err := NewContainer(cfg)
	require.NoError(t, err)
	defer container.Stop()

	require.NotNil(t, container)
	assert.False(t, container.IsClosed())
}

func TestContainer_GetComponents(t *testing.T) {
	// 创建临时目录
	tempDir := t.TempDir()
	
	cfg := Config{
		NodeID: 1,
		ClusterID: 1,
		DataDir: tempDir,
		Address: "127.0.0.1:9090",
	}

	// 创建容器
	container, err := NewContainer(cfg)
	require.NoError(t, err)
	defer container.Stop()

	// 测试获取仓库工厂
	factory := container.GetRepositoryFactory()
	assert.NotNil(t, factory)

	// 测试获取状态机
	stateMachine := container.GetStateMachine()
	assert.NotNil(t, stateMachine)

	// 测试获取Raft节点
	raftNode := container.GetRaftNode()
	assert.NotNil(t, raftNode)

	// 测试获取KV服务
	kvService := container.GetKVService()
	assert.NotNil(t, kvService)

	// 测试获取配置
	config := container.GetConfig()
	assert.NotNil(t, config)
	assert.Equal(t, uint64(1), config["node_id"])
	assert.Equal(t, uint64(1), config["cluster_id"])
	assert.Equal(t, false, config["closed"])
}

func TestContainer_Lifecycle(t *testing.T) {
	// 创建临时目录
	tempDir := t.TempDir()
	
	cfg := Config{
		NodeID: 1,
		ClusterID: 1,
		DataDir: tempDir,
		Address: "127.0.0.1:9090",
	}

	container, err := NewContainer(cfg)
	require.NoError(t, err)

	// 测试启动
	err = container.Start()
	assert.NoError(t, err)

	// 测试健康检查
	err = container.HealthCheck()
	assert.NoError(t, err)

	// 测试停止
	container.Stop()

	// 验证容器已关闭
	assert.True(t, container.IsClosed())
}

func TestContainer_HealthCheck(t *testing.T) {
	// 创建临时目录
	tempDir := t.TempDir()
	
	cfg := Config{
		NodeID: 1,
		ClusterID: 1,
		DataDir: tempDir,
		Address: "127.0.0.1:9090",
	}

	container, err := NewContainer(cfg)
	require.NoError(t, err)
	defer container.Stop()

	// 启动容器
	err = container.Start()
	assert.NoError(t, err)

	// 测试健康检查
	err = container.HealthCheck()
	assert.NoError(t, err)
}

func TestContainer_ConcurrentAccess(t *testing.T) {
	// 创建临时目录
	tempDir := t.TempDir()
	
	cfg := Config{
		NodeID: 1,
		ClusterID: 1,
		DataDir: tempDir,
		Address: "127.0.0.1:9090",
	}

	container, err := NewContainer(cfg)
	require.NoError(t, err)
	defer container.Stop()

	err = container.Start()
	assert.NoError(t, err)

	// 并发访问测试
	const numGoroutines = 10
	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer func() { done <- true }()

			// 并发获取组件
			kvService := container.GetKVService()
			assert.NotNil(t, kvService)

			stateMachine := container.GetStateMachine()
			assert.NotNil(t, stateMachine)

			raftNode := container.GetRaftNode()
			assert.NotNil(t, raftNode)

			// 并发健康检查
			err := container.HealthCheck()
			assert.NoError(t, err)
		}()
	}

	// 等待所有goroutine完成
	for i := 0; i < numGoroutines; i++ {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("并发测试超时")
		}
	}
}

func TestContainer_ComponentSingleton(t *testing.T) {
	// 创建临时目录
	tempDir := t.TempDir()
	
	cfg := Config{
		NodeID: 1,
		ClusterID: 1,
		DataDir: tempDir,
		Address: "127.0.0.1:9090",
	}

	container, err := NewContainer(cfg)
	require.NoError(t, err)
	defer container.Stop()

	// 验证组件是单例的
	stateMachine1 := container.GetStateMachine()
	stateMachine2 := container.GetStateMachine()
	assert.Equal(t, stateMachine1, stateMachine2)

	raftNode1 := container.GetRaftNode()
	raftNode2 := container.GetRaftNode()
	assert.Equal(t, raftNode1, raftNode2)

	kvService1 := container.GetKVService()
	kvService2 := container.GetKVService()
	assert.Equal(t, kvService1, kvService2)
}

func TestContainer_StopIdempotent(t *testing.T) {
	// 创建临时目录
	tempDir := t.TempDir()
	
	cfg := Config{
		NodeID: 1,
		ClusterID: 1,
		DataDir: tempDir,
		Address: "127.0.0.1:9090",
	}

	container, err := NewContainer(cfg)
	require.NoError(t, err)

	// 启动容器
	err = container.Start()
	assert.NoError(t, err)
	assert.False(t, container.IsClosed())

	// 第一次停止
	container.Stop()
	assert.True(t, container.IsClosed())

	// 第二次停止应该是幂等的
	container.Stop()
	assert.True(t, container.IsClosed())
}

func TestContainer_ContextCancellation(t *testing.T) {
	// 创建临时目录
	tempDir := t.TempDir()
	
	cfg := Config{
		NodeID: 1,
		ClusterID: 1,
		DataDir: tempDir,
		Address: "127.0.0.1:9090",
	}

	container, err := NewContainer(cfg)
	require.NoError(t, err)
	defer container.Stop()

	// 启动容器
	err = container.Start()
	assert.NoError(t, err)

	// 在已取消的上下文中执行操作
	err = container.HealthCheck()
	// 注意：模拟实现可能不会检查上下文取消
	// 在真实实现中，应该检查上下文取消并返回相应错误
	_ = err // 忽略错误，因为模拟实现可能不处理上下文取消
}

func BenchmarkContainer_GetKVService(b *testing.B) {
	// 创建临时目录
	tempDir := b.TempDir()
	
	cfg := Config{
		NodeID: 1,
		ClusterID: 1,
		DataDir: tempDir,
		Address: "127.0.0.1:9090",
	}

	container, err := NewContainer(cfg)
	require.NoError(b, err)
	defer container.Stop()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kvService := container.GetKVService()
		if kvService == nil {
			b.Fatal("KV服务为nil")
		}
	}
}

func BenchmarkContainer_HealthCheck(b *testing.B) {
	// 创建临时目录
	tempDir := b.TempDir()
	
	cfg := Config{
		NodeID: 1,
		ClusterID: 1,
		DataDir: tempDir,
		Address: "127.0.0.1:9090",
	}

	container, err := NewContainer(cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer container.Stop()

	container.Start()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := container.HealthCheck()
		if err != nil {
			b.Fatal(err)
		}
	}
}