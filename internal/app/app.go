package app

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"qklzl/internal/config"
	"qklzl/internal/fsm"
	"qklzl/internal/node"
	"qklzl/internal/server"

	"github.com/dgraph-io/badger/v3"
)

// App 应用程序主结构
type App struct {
	config       *config.Config
	db           *badger.DB
	stateMachine *fsm.KVStateMachine
	raftNode     *node.RaftNode
	httpServer   *server.Server
}

// New 创建新的应用程序实例
func New(cfg *config.Config) (*App, error) {
	return &App{
		config: cfg,
	}, nil
}

// Initialize 初始化应用程序组件
func (a *App) Initialize() error {
	// 创建目录
	if err := a.config.CreateDirectories(); err != nil {
		return err
	}

	a.config.PrintDirectories()

	// 创建数据库
	db, err := a.createBadgerDB()
	if err != nil {
		return fmt.Errorf("无法创建数据库: %v", err)
	}
	a.db = db
	fmt.Println("BadgerDB创建成功")

	// 创建状态机
	a.stateMachine = fsm.NewKVStateMachine(a.db)
	fmt.Println("状态机创建成功")

	// 创建节点配置
	nodeConfig := node.RaftNodeConfig{
		NodeID:             a.config.NodeID,
		ClusterID:          1,
		Peers:              make(map[uint64]string),
		IsRestart:          false,
		IsJoin:             !a.config.Bootstrap && a.config.JoinAddr != "",
		ElectionTick:       10,
		HeartbeatTick:      1,
		SnapshotCount:      1000,
		StorageDir:         a.config.StorageDir,
		ListenAddr:         a.config.RaftAddr,
		MaxSizePerMsg:      1024 * 1024,
		MaxInflightMsgs:    256,
		CheckpointInterval: 30,
	}

	// 如果是引导节点，将自己添加为唯一的对等节点
	if a.config.Bootstrap {
		nodeConfig.Peers[a.config.NodeID] = a.config.RaftAddr
		fmt.Printf("配置为引导节点，添加自身为对等节点: %d -> %s\n", a.config.NodeID, a.config.RaftAddr)
	}

	fmt.Println("开始创建RAFT节点...")

	// 创建Raft节点
	raftNode, err := node.NewRaftNode(nodeConfig, a.stateMachine)
	if err != nil {
		return fmt.Errorf("无法创建RAFT节点: %v\n详细错误: %+v", err, err)
	}
	a.raftNode = raftNode

	fmt.Println("RAFT节点创建成功")

	// 创建HTTP服务器
	a.httpServer = server.NewServer(a.raftNode)
	fmt.Println("HTTP服务器创建成功")

	return nil
}

// Start 启动应用程序
func (a *App) Start() error {
	// 启动Raft节点
	if err := a.raftNode.Start(); err != nil {
		return fmt.Errorf("启动RAFT节点失败: %v\n详细错误: %+v", err, err)
	}
	fmt.Println("RAFT节点启动成功")

	// 启动HTTP服务
	go func() {
		log.Printf("正在启动HTTP服务于 %s", a.config.HTTPAddr)
		if err := a.httpServer.Start(a.config.HTTPAddr); err != nil {
			log.Printf("HTTP服务错误: %v", err)
		}
	}()

	// 等待HTTP服务启动
	time.Sleep(1 * time.Second)
	log.Printf("HTTP服务启动过程完成，尝试访问 http://%s/status 检查状态", a.config.HTTPAddr)

	// 如果需要加入集群
	if err := a.joinClusterIfNeeded(); err != nil {
		log.Printf("加入集群失败: %v", err)
	}

	return nil
}

// Stop 停止应用程序
func (a *App) Stop() {
	log.Println("正在关闭服务...")

	// 停止Raft节点
	if a.raftNode != nil {
		a.raftNode.Stop()
	}

	// 关闭数据库
	if a.db != nil {
		a.db.Close()
	}

	log.Println("服务已关闭")
}

// Run 运行应用程序直到收到停止信号
func (a *App) Run() error {
	if err := a.Start(); err != nil {
		return err
	}

	// 监听系统信号，优雅关闭服务
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)

	fmt.Println("服务已启动，等待信号...")
	<-signalCh

	a.Stop()
	return nil
}

// createBadgerDB 创建BadgerDB实例
func (a *App) createBadgerDB() (*badger.DB, error) {
	fmt.Printf("创建BadgerDB，目录: %s\n", a.config.FSMDir)
	opts := badger.DefaultOptions(a.config.FSMDir).WithLogger(nil)
	return badger.Open(opts)
}

// joinClusterIfNeeded 如果需要则加入集群
func (a *App) joinClusterIfNeeded() error {
	if a.config.JoinAddr == "" || a.config.Bootstrap {
		return nil
	}

	log.Printf("尝试加入集群 %s...", a.config.JoinAddr)

	// 构造加入请求
	joinURL := fmt.Sprintf("http://%s/join", a.config.JoinAddr)
	joinData := map[string]interface{}{
		"id":   a.config.NodeID,
		"addr": a.config.RaftAddr,
	}

	jsonData, err := json.Marshal(joinData)
	if err != nil {
		return fmt.Errorf("序列化加入请求失败: %v", err)
	}

	// 设置重试次数
	maxRetries := 5
	retryInterval := 2 * time.Second

	// 发送加入请求并重试
	for i := 0; i < maxRetries; i++ {
		resp, err := http.Post(joinURL, "application/json", bytes.NewBuffer(jsonData))
		if err == nil {
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				// 解析响应体
				var joinResp struct {
					Success     bool `json:"success"`
					ClusterInfo struct {
						LeaderID   uint64            `json:"leader_id"`
						LeaderAddr string            `json:"leader_addr"`
						Term       uint64            `json:"term"`
						Members    map[uint64]string `json:"members"`
					} `json:"cluster_info"`
				}

				if err := json.NewDecoder(resp.Body).Decode(&joinResp); err != nil {
					log.Printf("解析加入响应失败: %v", err)
				} else if joinResp.Success {
					log.Printf("成功加入集群，当前领导者: %d, 任期: %d",
						joinResp.ClusterInfo.LeaderID, joinResp.ClusterInfo.Term)

					// 更新本地节点的集群信息
					if len(joinResp.ClusterInfo.Members) > 0 {
						for id, addr := range joinResp.ClusterInfo.Members {
							if id != a.config.NodeID {
								a.raftNode.AddPeer(id, addr)
							}
						}
					}
				}

				log.Printf("成功加入集群")
				return nil
			} else if resp.StatusCode == http.StatusTemporaryRedirect {
				body, _ := io.ReadAll(resp.Body)
				errMsg := string(body)
				log.Printf("重定向: %s", errMsg)

				if strings.Contains(errMsg, "not leader, current leader is") {
					// 这里可以解析出领导者ID并重新发送请求
					// 简化处理，直接等待重试
				}
			} else {
				log.Printf("警告: 服务器返回非200状态码: %d", resp.StatusCode)
			}
		} else {
			log.Printf("警告: 加入集群失败: %v", err)
		}

		// 如果不是最后一次重试，等待后重试
		if i < maxRetries-1 {
			log.Printf("将在 %v 后重试加入集群...", retryInterval)
			time.Sleep(retryInterval)
		}
	}

	return fmt.Errorf("加入集群失败，已重试 %d 次", maxRetries)
}