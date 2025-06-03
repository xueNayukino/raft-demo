package fsm

import (
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// StateMachine 定义了状态机的接口
type StateMachine interface {
	// Apply 应用一个 Raft 日志条目到状态机
	Apply(entry raftpb.Entry) error

	// ApplySnapshot 应用快照到状态机
	ApplySnapshot(snapshot raftpb.Snapshot) error

	// CreateSnapshot 创建状态机的快照
	CreateSnapshot() (raftpb.Snapshot, error)

	// Get 获取键对应的值
	Get(key string) (string, error)

	// WaitForIndex 等待状态机应用到指定的索引
	WaitForIndex(index uint64) error
}
