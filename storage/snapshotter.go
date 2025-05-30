package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"qklzl/fsm"

	"github.com/dgraph-io/badger/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

const (
	// 默认快照检查间隔
	defaultSnapshotCheckInterval = 30 * time.Minute
	// 默认快照阈值（日志条目数）
	defaultSnapshotCount = 10000
	// 快照创建通道缓冲区大小
	snapshotChannelBuffer = 10
)

// SnapshotConfig 快照配置
type SnapshotConfig struct {
	// 检查间隔
	CheckInterval time.Duration
	// 触发快照的日志条目数
	SnapshotCount uint64
}

// Snapshotter 管理快照的创建和恢复
type Snapshotter struct {
	dir    string
	db     *badger.DB
	config *SnapshotConfig
	stopCh chan struct{}
	mu     sync.RWMutex

	// 日志条目计数器
	logEntryCount uint64
	fsm           *fsm.KVStateMachine

	// 异步快照创建
	snapshotCh chan struct{}
	// 快照创建状态
	isCreatingSnapshot int32
}

// 1. **`NewSnapshotter`**:
// - 创建 `Snapshotter` 实例时需要传入 `fsm.KVStateMachine`。

// 2. **`checkAndCreateSnapshot`**:
//    - 调用 `s.fsm.SaveSnapshot()` 来获取状态机的完整状态。

// 3. **`ApplySnapshot`**:
//    - 调用 `s.fsm.RestoreSnapshot(snapshot.Data)` 来恢复状态机的状态。

// 这些方法与 `fsm` 包的 `KVStateMachine` 交互，用于保存和恢复状态机的快照。
// NewSnapshotter 创建新的快照管理器
func NewSnapshotter(dir string, db *badger.DB, config *SnapshotConfig, fsm *fsm.KVStateMachine) *Snapshotter {
	if config == nil {
		config = &SnapshotConfig{
			CheckInterval: defaultSnapshotCheckInterval,
			SnapshotCount: defaultSnapshotCount,
		}
	}

	s := &Snapshotter{
		dir:           dir,
		db:            db,
		config:        config,
		stopCh:        make(chan struct{}),
		fsm:           fsm,
		snapshotCh:    make(chan struct{}, snapshotChannelBuffer),
		logEntryCount: 0,
	}

	// 初始化日志条目计数
	s.initLogEntryCount()

	// 启动自动快照检查
	go s.autoCheckSnapshot()
	// 启动异步快照处理
	go s.asyncSnapshotHandler()

	return s
}

// initLogEntryCount 初始化日志条目计数
func (s *Snapshotter) initLogEntryCount() {
	s.mu.RLock()
	defer s.mu.RUnlock()

	count, err := s.countLogEntries()
	if err == nil {
		atomic.StoreUint64(&s.logEntryCount, count)
	}
}

// countLogEntries 计算日志条目数量
func (s *Snapshotter) countLogEntries() (uint64, error) {
	var count uint64
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefixLogEntry
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefixLogEntry); it.Valid(); it.Next() {
			key := it.Item().Key()
			if !bytes.HasPrefix(key, prefixLogEntry) {
				break
			}
			count++
		}
		return nil
	})

	return count, err
}

// IncrementLogEntryCount 增加日志条目计数
func (s *Snapshotter) IncrementLogEntryCount() {
	newCount := atomic.AddUint64(&s.logEntryCount, 1)
	// 检查是否需要触发快照
	if newCount >= s.config.SnapshotCount {
		// 如果已经在创建快照，不重复触发
		if atomic.LoadInt32(&s.isCreatingSnapshot) == 1 {
			return
		}
		select {
		case s.snapshotCh <- struct{}{}:
			// 标记快照创建状态
			atomic.StoreInt32(&s.isCreatingSnapshot, 1)
		default:
			// 如果通道已满，说明已经有快照任务在进行中
		}
	}
}

// DecrementLogEntryCount 减少日志条目计数
func (s *Snapshotter) DecrementLogEntryCount() {
	// 使用原子操作减1，确保不会出现负数
	for {
		current := atomic.LoadUint64(&s.logEntryCount)
		if current == 0 {
			return
		}
		if atomic.CompareAndSwapUint64(&s.logEntryCount, current, current-1) {
			return
		}
	}
}

// asyncSnapshotHandler 异步处理快照创建
func (s *Snapshotter) asyncSnapshotHandler() {
	for {
		select {
		case <-s.snapshotCh:
			// 异步创建快照，不阻塞主线程
			if err := s.checkAndCreateSnapshot(); err != nil {
				fmt.Printf("Async snapshot creation failed: %v\n", err)
			}
		case <-s.stopCh:
			return
		}
	}
}

// Stop 停止快照管理器
func (s *Snapshotter) Stop() {
	close(s.stopCh)
}

// autoCheckSnapshot 定期检查是否需要创建快照
func (s *Snapshotter) autoCheckSnapshot() {
	ticker := time.NewTicker(s.config.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// 触发异步快照创建
			select {
			case s.snapshotCh <- struct{}{}:
			default:
				// 如果通道已满，忽略
			}
		case <-s.stopCh:
			return
		}
	}
}

// checkAndCreateSnapshot 检查是否需要创建快照
func (s *Snapshotter) checkAndCreateSnapshot() error {
	defer atomic.StoreInt32(&s.isCreatingSnapshot, 0)

	// 获取当前日志条目数
	currentEntryCount := atomic.LoadUint64(&s.logEntryCount)
	if currentEntryCount < s.config.SnapshotCount {
		return nil
	}

	// 使用互斥锁保护快照创建过程
	s.mu.Lock()
	defer s.mu.Unlock()

	// 再次检查计数，避免并发问题
	if atomic.LoadUint64(&s.logEntryCount) < s.config.SnapshotCount {
		return nil
	}

	var lastIndex, firstIndex, committedIndex uint64
	err := s.db.View(func(txn *badger.Txn) error {
		// 获取硬状态以获取已提交的索引
		item, err := txn.Get(keyHardState)
		if err != nil && err != badger.ErrKeyNotFound {
			return fmt.Errorf("failed to get hard state: %v", err)
		}
		if err == nil {
			var hs raftpb.HardState
			err = item.Value(func(val []byte) error {
				return hs.Unmarshal(val)
			})
			if err != nil {
				return fmt.Errorf("failed to unmarshal hard state: %v", err)
			}
			committedIndex = hs.Commit
		}

		// 获取日志范围
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefixLogEntry
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		first := true
		for it.Seek(prefixLogEntry); it.Valid(); it.Next() {
			key := it.Item().Key()
			if !bytes.HasPrefix(key, prefixLogEntry) {
				break
			}
			index := binary.BigEndian.Uint64(key[len(prefixLogEntry):])
			if first {
				firstIndex = index
				first = false
			}
			lastIndex = index
		}
		return nil
	})

	if err != nil {
		return err
	}

	// 验证快照点的有效性
	if committedIndex == 0 || committedIndex < firstIndex || committedIndex > lastIndex {
		return fmt.Errorf("invalid committed index %d (first: %d, last: %d)", committedIndex, firstIndex, lastIndex)
	}

	var snapshot raftpb.Snapshot
	err = s.db.View(func(txn *badger.Txn) error {
		// 获取快照点的日志条目
		item, err := txn.Get(logKey(committedIndex))
		if err != nil {
			return fmt.Errorf("failed to get snapshot entry: %v", err)
		}

		var entry raftpb.Entry
		err = item.Value(func(val []byte) error {
			return entry.Unmarshal(val)
		})
		if err != nil {
			return fmt.Errorf("failed to unmarshal snapshot entry: %v", err)
		}

		// 获取当前配置状态
		item, err = txn.Get(keyConfState)
		if err != nil && err != badger.ErrKeyNotFound {
			return fmt.Errorf("failed to get conf state: %v", err)
		}

		var confState raftpb.ConfState
		if err == nil {
			err = item.Value(func(val []byte) error {
				return confState.Unmarshal(val)
			})
			if err != nil {
				return fmt.Errorf("failed to unmarshal conf state: %v", err)
			}
		}

		// 获取状态机的完整状态
		stateData, err := s.fsm.SaveSnapshot()
		if err != nil {
			return fmt.Errorf("failed to save state machine snapshot: %v", err)
		}

		// 创建快照元数据
		snapshot.Metadata.Index = committedIndex
		snapshot.Metadata.Term = entry.Term
		snapshot.Metadata.ConfState = confState
		snapshot.Data = stateData

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to create snapshot: %v", err)
	}

	// 使用批量写入保存快照
	wb := s.db.NewWriteBatch()
	defer wb.Cancel()

	// 序列化快照
	data, err := snapshot.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal snapshot: %v", err)
	}

	// 删除旧的快照并写入新的快照
	if err := wb.Delete(keySnapshot); err != nil {
		return fmt.Errorf("failed to delete old snapshot: %v", err)
	}
	if err := wb.Set(keySnapshot, data); err != nil {
		return fmt.Errorf("failed to save new snapshot: %v", err)
	}

	// 提交批量写入
	if err := wb.Flush(); err != nil {
		return fmt.Errorf("failed to flush snapshot batch: %v", err)
	}

	// 重置日志条目计数
	atomic.StoreUint64(&s.logEntryCount, 0)

	return nil
}

// ApplySnapshot 应用快照
func (s *Snapshotter) ApplySnapshot(snapshot raftpb.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 验证快照的有效性
	if snapshot.Metadata.Index == 0 {
		return fmt.Errorf("invalid snapshot index")
	}

	// 检查快照是否比当前状态更新
	currentSnap, err := s.LoadNewestSnapshot()
	if err != nil {
		return fmt.Errorf("failed to load current snapshot: %v", err)
	}

	if currentSnap != nil {
		if snapshot.Metadata.Index < currentSnap.Metadata.Index {
			return fmt.Errorf("snapshot index [%d] is older than current snapshot [%d]",
				snapshot.Metadata.Index, currentSnap.Metadata.Index)
		}
		if snapshot.Metadata.Term < currentSnap.Metadata.Term {
			return fmt.Errorf("snapshot term [%d] is older than current snapshot [%d]",
				snapshot.Metadata.Term, currentSnap.Metadata.Term)
		}
	}

	// 恢复状态机状态
	if err := s.fsm.RestoreSnapshot(snapshot.Data); err != nil {
		return fmt.Errorf("failed to restore state machine: %v", err)
	}

	// 序列化快照
	data, err := snapshot.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal snapshot: %v", err)
	}

	// 保存快照
	err = s.db.Update(func(txn *badger.Txn) error {
		// 删除旧的快照
		err := txn.Delete(keySnapshot)
		if err != nil && err != badger.ErrKeyNotFound {
			return fmt.Errorf("failed to delete old snapshot: %v", err)
		}

		// 保存新的快照
		err = txn.Set(keySnapshot, data)
		if err != nil {
			return fmt.Errorf("failed to save snapshot: %v", err)
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to save snapshot: %v", err)
	}

	fmt.Printf("Successfully saved snapshot with index: %d\n", snapshot.Metadata.Index)
	return nil
}

// LoadNewestSnapshot 加载最新的快照
func (s *Snapshotter) LoadNewestSnapshot() (*raftpb.Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var snapshot raftpb.Snapshot
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(keySnapshot)
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			return snapshot.Unmarshal(val)
		})
	})

	if err != nil {
		return nil, err
	}

	if IsEmptySnap(snapshot) {
		return nil, nil
	}

	return &snapshot, nil
}

// GetFSM 获取状态机
func (s *Snapshotter) GetFSM() *fsm.KVStateMachine {
	return s.fsm
}

// // IsEmptySnap 检查快照是否为空
// func IsEmptySnap(snap raftpb.Snapshot) bool {
// 	return snap.Metadata.Index == 0
// }

// // logKey 生成日志条目的键
// func logKey(index uint64) []byte {
// 	buf := make([]byte, 8)
// 	binary.BigEndian.PutUint64(buf, index)
// 	return append(prefixLogEntry, buf...)
// }

// snapshotKey 生成快照的键
func snapshotKey(index uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, index)
	return append(keySnapshot, buf...)
}

// parseLogKey 从日志键中解析索引
func parseLogKey(key []byte) uint64 {
	return binary.BigEndian.Uint64(key[len(prefixLogEntry):])
}

// parseSnapshotKey 从快照键中解析索引
func parseSnapshotKey(key []byte) uint64 {
	return binary.BigEndian.Uint64(key[len(keySnapshot):])
}
