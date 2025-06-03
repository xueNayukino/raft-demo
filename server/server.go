package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"qklzl/fsm"
	"qklzl/node"
	"time"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

// Server 将Raft节点包装为HTTP服务
type Server struct {
	// 包含一个raft.Node指针，用于实际处理存储操作
	node *node.RaftNode
}

// NewServer 创建新的HTTP服务器
func NewServer(node *node.RaftNode) *Server {
	return &Server{node: node}
}

// Start 启动HTTP服务
func (s *Server) Start(addr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/kv", s.handleKV)
	mux.HandleFunc("/join", s.handleJoin)
	mux.HandleFunc("/status", s.handleStatus)

	// 添加健康检查端点
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	fmt.Printf("HTTP服务器监听于 %s\n", addr)
	return server.ListenAndServe()
}

// handleKV 处理KV操作请求
func (s *Server) handleKV(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "key is required", http.StatusBadRequest)
			return
		}

		// 使用一致性读取获取值
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		val, err := s.node.Get(ctx, key)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to get key: %v", err), http.StatusInternalServerError)
			return
		}

		if val == "" {
			http.Error(w, "key not found", http.StatusNotFound)
			return
		}

		w.Write([]byte(val))
		return
	}

	if r.Method == http.MethodPost {
		// 检查是否是Leader节点
		if !s.node.IsLeader() {
			// 如果不是Leader，返回重定向到Leader的响应
			leaderID := s.node.GetLeaderID()
			leaderAddr := s.node.GetLeaderAddr()

			if leaderAddr == "" {
				// Leader地址未知，返回错误
				http.Error(w, fmt.Sprintf("非Leader节点不能执行写操作，当前Leader是节点%d，但Leader地址未知", leaderID), http.StatusServiceUnavailable)
				return
			}

			// 构建重定向URL
			redirectURL := fmt.Sprintf("http://%s/kv", leaderAddr)

			// 返回重定向响应
			w.Header().Set("Location", redirectURL)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTemporaryRedirect)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success":  false,
				"error":    "非Leader节点不能执行写操作",
				"leader":   leaderID,
				"redirect": redirectURL,
			})
			return
		}

		var cmd fsm.Command
		err := json.NewDecoder(r.Body).Decode(&cmd)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// 构建raft提案
		data, err := json.Marshal(cmd)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// 提交提案到Raft集群
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err = s.node.Propose(ctx, data)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// 返回成功响应
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
		})
		return
	}

	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
}

// ConfChangeAddNode 辅助函数
func ConfChangeAddNode(nodeID uint64, addr string) raftpb.ConfChange {
	return raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  nodeID,
		Context: []byte(addr),
	}
}

// handleJoin 处理节点加入请求
func (s *Server) handleJoin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		ID   uint64 `json:"id"`
		Addr string `json:"addr"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// 验证请求
	if req.ID == 0 || req.Addr == "" {
		http.Error(w, "invalid request: id and addr are required", http.StatusBadRequest)
		return
	}

	// 如果不是领导者，返回错误
	if !s.node.IsLeader() {
		leaderID := s.node.GetLeaderID()
		http.Error(w, fmt.Sprintf("not leader, current leader is %d", leaderID), http.StatusTemporaryRedirect)
		return
	}

	// 向集群添加新节点
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 创建配置变更请求
	cc := raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  req.ID,
		Context: []byte(req.Addr),
	}

	err := s.node.ProposeConfChange(ctx, cc)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to add node: %v", err), http.StatusInternalServerError)
		return
	}

	fmt.Printf("成功添加节点 %d 到集群\n", req.ID)

	// 返回成功响应，包含当前集群信息
	w.Header().Set("Content-Type", "application/json")
	resp := map[string]interface{}{
		"success": true,
		"cluster_info": map[string]interface{}{
			"leader_id":   s.node.GetLeaderID(),
			"leader_addr": s.node.GetLeaderAddr(),
			"term":        s.node.GetTerm(),
			"members":     s.node.GetClusterMembers(),
		},
	}
	json.NewEncoder(w).Encode(resp)
}

// handleStatus 处理状态查询请求
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 获取节点状态
	isLeader := s.node.IsLeader()
	leaderID := s.node.GetLeaderID()
	appliedIndex := s.node.GetAppliedIndex()
	commitIndex := s.node.GetCommitIndex()
	term := s.node.GetTerm()

	// 构建状态响应
	status := map[string]interface{}{
		"is_leader":     isLeader,
		"leader_id":     leaderID,
		"term":          term,
		"applied_index": appliedIndex,
		"commit_index":  commitIndex,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}
