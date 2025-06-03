package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"qklzl/fsm"
	"qklzl/node"
	"qklzl/server"

	"github.com/dgraph-io/badger/v3"
)

func main() {
	// 定义命令行参数
	var (
		nodeID    = flag.Uint64("id", 1, "节点ID")
		raftAddr  = flag.String("raft-addr", "127.0.0.1:12000", "RAFT协议通信地址")
		httpAddr  = flag.String("http-addr", "127.0.0.1:8000", "HTTP服务地址")
		dataDir   = flag.String("data-dir", "data", "数据目录")
		bootstrap = flag.Bool("bootstrap", false, "是否创建新集群")
		joinAddr  = flag.String("join", "", "加入到集群的地址")
	)

	flag.Parse() // 解析命令行参数

	// 创建数据目录
	nodeDataDir := filepath.Join(*dataDir, fmt.Sprintf("node%d", *nodeID))
	if err := os.MkdirAll(nodeDataDir, 0755); err != nil {
		log.Fatalf("无法创建数据目录: %v", err)
	}

	// 创建状态机和存储层的独立目录
	fsmDir := filepath.Join(nodeDataDir, "fsm")
	storageDir := filepath.Join(nodeDataDir, "storage")

	if err := os.MkdirAll(fsmDir, 0755); err != nil {
		log.Fatalf("无法创建状态机目录: %v", err)
	}
	if err := os.MkdirAll(storageDir, 0755); err != nil {
		log.Fatalf("无法创建存储层目录: %v", err)
	}

	fmt.Printf("使用数据目录: %s\n", nodeDataDir)
	fmt.Printf("状态机目录: %s\n", fsmDir)
	fmt.Printf("存储层目录: %s\n", storageDir)

	// 创建状态机
	db, err := createBadgerDB(fsmDir)
	if err != nil {
		log.Fatalf("无法创建数据库: %v", err)
	}
	defer db.Close()

	fmt.Println("BadgerDB创建成功")

	stateMachine := fsm.NewKVStateMachine(db)
	fmt.Println("状态机创建成功")

	// 创建节点配置
	config := node.RaftNodeConfig{
		NodeID:             *nodeID,
		ClusterID:          1,
		Peers:              make(map[uint64]string),
		IsRestart:          false,
		IsJoin:             !*bootstrap && *joinAddr != "",
		ElectionTick:       10,
		HeartbeatTick:      1,
		SnapshotCount:      1000,
		StorageDir:         storageDir, // 使用独立的存储目录
		ListenAddr:         *raftAddr,
		MaxSizePerMsg:      1024 * 1024,
		MaxInflightMsgs:    256,
		CheckpointInterval: 30,
	}

	// 如果是引导节点，将自己添加为唯一的对等节点
	if *bootstrap {
		config.Peers[*nodeID] = *raftAddr
		fmt.Printf("配置为引导节点，添加自身为对等节点: %d -> %s\n", *nodeID, *raftAddr)
	}

	fmt.Println("开始创建RAFT节点...")

	// 创建并启动Raft节点
	raftNode, err := node.NewRaftNode(config, stateMachine)
	if err != nil {
		log.Fatalf("无法创建RAFT节点: %v\n详细错误: %+v", err, err)
	}

	fmt.Println("RAFT节点创建成功，开始启动...")

	if err := raftNode.Start(); err != nil {
		log.Fatalf("启动RAFT节点失败: %v\n详细错误: %+v", err, err)
	}

	fmt.Println("RAFT节点启动成功")

	// 创建HTTP服务器
	httpServer := server.NewServer(raftNode)
	fmt.Println("HTTP服务器创建成功")

	// 启动HTTP服务
	go func() {
		log.Printf("正在启动HTTP服务于 %s", *httpAddr)
		if err := httpServer.Start(*httpAddr); err != nil {
			log.Printf("HTTP服务错误: %v", err)
			// 不要使用Fatal，这会导致整个程序退出
			// 而是记录错误并允许主程序继续运行
		}
	}()

	// 等待HTTP服务启动
	time.Sleep(1 * time.Second)
	log.Printf("HTTP服务启动过程完成，尝试访问 http://%s/status 检查状态", *httpAddr)

	// 如果指定了join参数，则尝试加入集群
	if *joinAddr != "" && !*bootstrap {
		log.Printf("尝试加入集群 %s...", *joinAddr)

		// 构造加入请求
		joinURL := fmt.Sprintf("http://%s/join", *joinAddr)
		joinData := map[string]interface{}{
			"id":   *nodeID,
			"addr": *raftAddr,
		}

		jsonData, err := json.Marshal(joinData)
		if err != nil {
			log.Fatalf("序列化加入请求失败: %v", err)
		}

		// 设置重试次数
		maxRetries := 5
		retryInterval := 2 * time.Second

		// 发送加入请求并重试
		for i := 0; i < maxRetries; i++ {
			// 发送加入请求
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
							// 更新Peers列表
							for id, addr := range joinResp.ClusterInfo.Members {
								if id != *nodeID {
									raftNode.AddPeer(id, addr)
								}
							}
						}
					}

					log.Printf("成功加入集群")
					break
				} else if resp.StatusCode == http.StatusTemporaryRedirect {
					// 如果当前节点不是领导者，尝试从响应中获取领导者信息
					body, _ := io.ReadAll(resp.Body)
					errMsg := string(body)
					log.Printf("重定向: %s", errMsg)

					// 假设错误消息格式为 "not leader, current leader is X"
					if strings.Contains(errMsg, "not leader, current leader is") {
						// 这里可以解析出领导者ID并重新发送请求
						// 简化处理，直接等待重试
					}
				} else {
					log.Printf("警告: 服务器返回非200状态码: %d", resp.StatusCode)
				}
				resp.Body.Close()
			} else {
				log.Printf("警告: 加入集群失败: %v", err)
			}

			// 如果不是最后一次重试，等待后重试
			if i < maxRetries-1 {
				log.Printf("将在 %v 后重试加入集群...", retryInterval)
				time.Sleep(retryInterval)
			}
		}
	}

	// 监听系统信号，优雅关闭服务
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)

	fmt.Println("服务已启动，等待信号...")
	<-signalCh
	log.Println("正在关闭服务...")

	// 停止Raft节点
	raftNode.Stop()

	log.Println("服务已关闭")
}

// createBadgerDB 创建BadgerDB实例
func createBadgerDB(dir string) (*badger.DB, error) {
	fmt.Printf("创建BadgerDB，目录: %s\n", dir)
	opts := badger.DefaultOptions(dir).WithLogger(nil)
	return badger.Open(opts)
}
