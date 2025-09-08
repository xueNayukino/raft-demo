package config

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
)

// Config 应用程序配置
type Config struct {
	// 节点配置
	NodeID    uint64
	RaftAddr  string
	HTTPAddr  string
	DataDir   string
	Bootstrap bool
	JoinAddr  string

	// 派生配置
	NodeDataDir string
	FSMDir      string
	StorageDir  string
}

// ParseFlags 解析命令行参数并返回配置
func ParseFlags() (*Config, error) {
	var (
		nodeID    = flag.Uint64("id", 1, "节点ID")
		raftAddr  = flag.String("raft-addr", "127.0.0.1:12000", "RAFT协议通信地址")
		httpAddr  = flag.String("http-addr", "127.0.0.1:8000", "HTTP服务地址")
		dataDir   = flag.String("data-dir", "data", "数据目录")
		bootstrap = flag.Bool("bootstrap", false, "是否创建新集群")
		joinAddr  = flag.String("join", "", "加入到集群的地址")
	)

	flag.Parse()

	// 创建配置实例
	config := &Config{
		NodeID:    *nodeID,
		RaftAddr:  *raftAddr,
		HTTPAddr:  *httpAddr,
		DataDir:   *dataDir,
		Bootstrap: *bootstrap,
		JoinAddr:  *joinAddr,
	}

	// 计算派生路径
	config.NodeDataDir = filepath.Join(*dataDir, fmt.Sprintf("node%d", *nodeID))
	config.FSMDir = filepath.Join(config.NodeDataDir, "fsm")
	config.StorageDir = filepath.Join(config.NodeDataDir, "storage")

	return config, nil
}

// CreateDirectories 创建必要的目录
func (c *Config) CreateDirectories() error {
	// 创建数据目录
	if err := os.MkdirAll(c.NodeDataDir, 0755); err != nil {
		return fmt.Errorf("无法创建数据目录: %v", err)
	}

	// 创建状态机目录
	if err := os.MkdirAll(c.FSMDir, 0755); err != nil {
		return fmt.Errorf("无法创建状态机目录: %v", err)
	}

	// 创建存储层目录
	if err := os.MkdirAll(c.StorageDir, 0755); err != nil {
		return fmt.Errorf("无法创建存储层目录: %v", err)
	}

	return nil
}

// PrintDirectories 打印目录信息
func (c *Config) PrintDirectories() {
	fmt.Printf("使用数据目录: %s\n", c.NodeDataDir)
	fmt.Printf("状态机目录: %s\n", c.FSMDir)
	fmt.Printf("存储层目录: %s\n", c.StorageDir)
}