package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"qklzl/fsm"

	"github.com/dgraph-io/badger/v3"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
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
	fsm           fsm.KVStateMachine

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
func NewSnapshotter(dir string, db *badger.DB, config *SnapshotConfig, fsm *fsm.KVStateMachine) (*Snapshotter, error) {
	if fsm == nil {
		return nil, fmt.Errorf("状态机不能为空")
	}

	if db == nil {
		return nil, fmt.Errorf("数据库实例不能为空")
	}

	if config == nil {
		config = &SnapshotConfig{
			CheckInterval: defaultSnapshotCheckInterval,
			SnapshotCount: defaultSnapshotCount,
		}
	}

	// 验证配置参数
	if config.CheckInterval < time.Second {
		return nil, fmt.Errorf("检查间隔不能小于1秒")
	}
	if config.SnapshotCount < 10 {
		return nil, fmt.Errorf("快照计数不能小于10")
	}

	s := &Snapshotter{
		dir:           dir,
		db:            db,
		config:        config,
		stopCh:        make(chan struct{}),
		fsm:           *fsm,
		snapshotCh:    make(chan struct{}, snapshotChannelBuffer),
		logEntryCount: 0,
	}

	// 初始化日志条目计数
	if err := s.initLogEntryCount(); err != nil {
		return nil, fmt.Errorf("初始化日志条目计数失败: %v", err)
	}

	// 启动自动快照检查
	go s.autoCheckSnapshot()
	// 启动异步快照处理
	go s.asyncSnapshotHandler()

	fmt.Printf("成功创建快照管理器，目录: %s\n", dir)
	return s, nil
}

// initLogEntryCount 初始化日志条目计数
func (s *Snapshotter) initLogEntryCount() error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	count, err := s.countLogEntries()
	if err != nil {
		return fmt.Errorf("计算日志条目数量失败: %v", err)
	}

	atomic.StoreUint64(&s.logEntryCount, count)
	fmt.Printf("初始化日志条目计数: %d\n", count)
	return nil
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
	s.mu.Lock()
	defer s.mu.Unlock()

	select {
	case <-s.stopCh:
		// 通道已经关闭，直接返回
		return
	default:
		close(s.stopCh)
	}
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

// checkAndCreateSnapshot 检查并创建快照
func (s *Snapshotter) checkAndCreateSnapshot() error {
	defer atomic.StoreInt32(&s.isCreatingSnapshot, 0)

	// 获取当前日志条目数
	currentEntryCount := atomic.LoadUint64(&s.logEntryCount)
	if currentEntryCount < s.config.SnapshotCount {
		return nil
	}

	fmt.Printf("开始创建快照，当前日志条目数: %d\n", currentEntryCount)

	// 使用互斥锁保护快照创建过程
	s.mu.Lock()
	defer s.mu.Unlock()

	// 再次检查计数，避免并发问题
	if atomic.LoadUint64(&s.logEntryCount) < s.config.SnapshotCount {
		return nil
	}

	// 获取状态机的完整状态
	stateData, err := s.fsm.SaveSnapshot()
	if err != nil {
		return fmt.Errorf("保存状态机快照失败: %v", err)
	}

	if len(stateData) == 0 {
		return fmt.Errorf("状态机返回了空的快照数据")
	}

	fmt.Printf("成功创建状态机快照，数据大小: %d 字节\n", len(stateData))

	var lastIndex, firstIndex, committedIndex uint64
	err = s.db.View(func(txn *badger.Txn) error {
		// 获取硬状态以获取已提交的索引
		item, err := txn.Get(keyHardState)
		if err != nil && err != badger.ErrKeyNotFound {
			return fmt.Errorf("获取硬状态失败: %v", err)
		}
		if err == nil {
			var hs raftpb.HardState
			err = item.Value(func(val []byte) error {
				return hs.Unmarshal(val)
			})
			if err != nil {
				return fmt.Errorf("解析硬状态失败: %v", err)
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
		return fmt.Errorf("获取日志范围失败: %v", err)
	}

	// 验证快照点的有效性
	if committedIndex == 0 {
		committedIndex = lastIndex // 如果没有提交索引，使用最后的索引
	}
	if committedIndex < firstIndex || committedIndex > lastIndex {
		return fmt.Errorf("无效的提交索引 %d (first: %d, last: %d)", committedIndex, firstIndex, lastIndex)
	}

	// 获取配置状态
	var confState raftpb.ConfState
	err = s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(keyConfState)
		if err != nil && err != badger.ErrKeyNotFound {
			return fmt.Errorf("获取配置状态失败: %v", err)
		}
		if err == nil {
			return item.Value(func(val []byte) error {
				return confState.Unmarshal(val)
			})
		}
		return nil
	})
	if err != nil {
		return err
	}

	// 创建快照
	snapshot := raftpb.Snapshot{
		Data: stateData,
		Metadata: raftpb.SnapshotMetadata{
			Index:     committedIndex,
			Term:      s.getCurrentTerm(),
			ConfState: confState,
		},
	}

	// 保存快照
	if err := s.SaveSnapshot(snapshot); err != nil {
		return fmt.Errorf("保存快照失败: %v", err)
	}

	// 压缩日志
	if err := s.db.Update(func(txn *badger.Txn) error {
		// 删除已压缩的日志条目
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefixLogEntry
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		deleted := 0
		for it.Seek(prefixLogEntry); it.Valid(); it.Next() {
			key := it.Item().Key()
			if !bytes.HasPrefix(key, prefixLogEntry) {
				break
			}
			index := binary.BigEndian.Uint64(key[len(prefixLogEntry):])
			if index <= committedIndex {
				if err := txn.Delete(key); err != nil {
					return fmt.Errorf("删除已压缩的日志条目失败: %v", err)
				}
				deleted++
			}
		}
		fmt.Printf("成功删除 %d 个已压缩的日志条目\n", deleted)

		// 更新第一个索引
		entry := raftpb.Entry{Index: committedIndex + 1}
		data, err := entry.Marshal()
		if err != nil {
			return fmt.Errorf("序列化第一个索引失败: %v", err)
		}
		if err := txn.Set(keyFirstIndex, data); err != nil {
			return fmt.Errorf("保存第一个索引失败: %v", err)
		}

		return nil
	}); err != nil {
		return fmt.Errorf("压缩日志失败: %v", err)
	}

	// 重置日志条目计数
	atomic.StoreUint64(&s.logEntryCount, 0)
	fmt.Printf("快照创建完成，重置日志条目计数\n")

	return nil
}

// SaveSnapshot 保存快照
func (s *Snapshotter) SaveSnapshot(snapshot raftpb.Snapshot) error {
	if raft.IsEmptySnap(snapshot) {
		fmt.Println("[SaveSnapshot] 空快照，跳过保存")
		return nil
	}

	fmt.Println("[SaveSnapshot] 开始保存快照")
	// 不再加锁，避免死锁

	// 确保快照目录存在
	if err := os.MkdirAll(s.dir, 0755); err != nil {
		fmt.Printf("[SaveSnapshot] 创建快照目录失败: %v\n", err)
		return fmt.Errorf("创建快照目录失败: %v", err)
	}

	// 创建临时文件
	tmpPath := filepath.Join(s.dir, fmt.Sprintf("tmp-%016x", snapshot.Metadata.Index))
	fmt.Printf("[SaveSnapshot] 创建临时快照文件: %s\n", tmpPath)
	fp, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		fmt.Printf("[SaveSnapshot] 创建临时文件失败: %v\n", err)
		return fmt.Errorf("创建临时文件失败: %v", err)
	}
	defer fp.Close()

	// 序列化快照数据
	data, err := snapshot.Marshal()
	if err != nil {
		fmt.Printf("[SaveSnapshot] 序列化快照失败: %v\n", err)
		return fmt.Errorf("序列化快照失败: %v", err)
	}

	// 写入数据到临时文件
	if _, err := fp.Write(data); err != nil {
		os.Remove(tmpPath) // 清理临时文件
		fmt.Printf("[SaveSnapshot] 写入快照数据失败: %v\n", err)
		return fmt.Errorf("写入快照数据失败: %v", err)
	}

	// 确保数据写入磁盘
	if err := fp.Sync(); err != nil {
		os.Remove(tmpPath) // 清理临时文件
		fmt.Printf("[SaveSnapshot] 同步快照数据失败: %v\n", err)
		return fmt.Errorf("同步快照数据失败: %v", err)
	}

	// 构建最终文件名
	finalPath := filepath.Join(s.dir, fmt.Sprintf("%016x-%016x%s", snapshot.Metadata.Term, snapshot.Metadata.Index, snapshotSuffix))
	fmt.Printf("[SaveSnapshot] 最终快照文件: %s\n", finalPath)

	// 重命名临时文件
	if err := os.Rename(tmpPath, finalPath); err != nil {
		os.Remove(tmpPath) // 清理临时文件
		fmt.Printf("[SaveSnapshot] 重命名快照文件失败: %v\n", err)
		return fmt.Errorf("重命名快照文件失败: %v", err)
	}

	// 同时保存到数据库
	err = s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(keySnapshot, data)
	})
	if err != nil {
		os.Remove(finalPath) // 如果数据库写入失败，删除文件
		fmt.Printf("[SaveSnapshot] 保存快照到数据库失败: %v\n", err)
		return fmt.Errorf("保存快照到数据库失败: %v", err)
	}

	// 删除旧的快照文件
	if err := s.purgeOldSnapshots(); err != nil {
		fmt.Printf("[SaveSnapshot] 警告：清理旧快照失败: %v\n", err)
	}

	fmt.Printf("[SaveSnapshot] 成功保存快照，索引: %d，任期: %d\n", snapshot.Metadata.Index, snapshot.Metadata.Term)
	return nil
}

// LoadSnapshot 加载最新的快照
func (s *Snapshotter) LoadSnapshot() (*raftpb.Snapshot, error) {
	// 获取最新的快照文件
	names, err := s.ListSnapshots()
	if err != nil {
		return nil, err
	}
	if len(names) == 0 {
		return nil, nil
	}

	filename := names[len(names)-1] // 最新的快照
	filepath := filepath.Join(s.dir, filename)

	// 读取快照文件
	data, err := os.ReadFile(filepath)
	if err != nil {
		return nil, err
	}

	// 反序列化快照
	var snapshot raftpb.Snapshot
	if err := snapshot.Unmarshal(data); err != nil {
		return nil, fmt.Errorf("反序列化快照失败: %v", err)
	}

	return &snapshot, nil
}

// ApplySnapshot 应用快照到状态机
func (s *Snapshotter) ApplySnapshot(snapshot raftpb.Snapshot) error {
	fmt.Printf("[ApplySnapshot] 收到快照: Index=%d, Term=%d, DataLen=%d\n", snapshot.Metadata.Index, snapshot.Metadata.Term, len(snapshot.Data))
	// 先检查索引
	if snapshot.Metadata.Index == 0 {
		fmt.Println("[ApplySnapshot] 检测到无效快照索引，返回错误")
		return fmt.Errorf("无效的快照索引")
	}
	// 再判断空快照
	if raft.IsEmptySnap(snapshot) {
		return nil
	}
	// 检查是否为过期快照
	currentSnap, _ := s.LoadNewestSnapshot()
	if currentSnap != nil && snapshot.Metadata.Index <= currentSnap.Metadata.Index {
		fmt.Printf("[ApplySnapshot] 拒绝过期快照: 当前=%d, 传入=%d\n", currentSnap.Metadata.Index, snapshot.Metadata.Index)
		return fmt.Errorf("早于当前快照")
	}

	fmt.Printf("开始应用快照，索引: %d，任期: %d\n", snapshot.Metadata.Index, snapshot.Metadata.Term)

	// 检查快照数据
	if len(snapshot.Data) == 0 {
		fmt.Println("警告：快照数据为空")
		return fmt.Errorf("快照数据不能为空")
	}

	fmt.Println("开始解析快照数据...")

	// 使用互斥锁保护状态机操作
	s.mu.Lock()
	defer s.mu.Unlock()

	// 开始批量写入
	fmt.Println("开始批量写入操作...")
	wb := s.db.NewWriteBatch()
	defer wb.Cancel()

	// 清除现有数据
	fmt.Println("删除现有的键值对...")
	deleteCount := 0
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			if err := wb.Delete(it.Item().Key()); err != nil {
				return err
			}
			deleteCount++
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("删除现有数据失败: %v", err)
	}
	fmt.Printf("删除 %d 个现有键值对...\n", deleteCount)

	// 恢复快照数据
	fmt.Printf("恢复 %d 字节的快照数据...\n", len(snapshot.Data))
	if err := s.fsm.RestoreSnapshot(snapshot.Data); err != nil {
		return fmt.Errorf("恢复状态机失败: %v", err)
	}

	// 提交批量写入
	fmt.Println("提交批量写入...")
	if err := wb.Flush(); err != nil {
		return fmt.Errorf("提交批量写入失败: %v", err)
	}

	fmt.Println("成功恢复状态机状态")
	fmt.Printf("成功应用快照，状态机已更新到索引 %d\n", snapshot.Metadata.Index)

	// ----------- 新增：写快照文件 -----------
	data, err := snapshot.Marshal()
	if err != nil {
		return fmt.Errorf("快照序列化失败: %v", err)
	}
	if err := os.MkdirAll(s.dir, 0755); err != nil {
		return fmt.Errorf("创建快照目录失败: %v", err)
	}
	fileName := fmt.Sprintf("%016x-%016x%s", snapshot.Metadata.Term, snapshot.Metadata.Index, snapshotSuffix)
	filePath := filepath.Join(s.dir, fileName)
	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("写快照文件失败: %v", err)
	}
	fmt.Printf("[ApplySnapshot] 已写入快照文件: %s\n", filePath)
	// ----------- END -----------

	return nil
}

// LoadNewestSnapshot 加载最新的快照
func (s *Snapshotter) LoadNewestSnapshot() (*raftpb.Snapshot, error) {
	fmt.Println("[LoadNewestSnapshot] 进入函数体")
	// 不再加锁，避免死锁

	fmt.Println("[LoadNewestSnapshot] 尝试从数据库加载快照...")
	var snapshot raftpb.Snapshot

	// 首先尝试从数据库加载
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(keySnapshot)
		if err == nil {
			return item.Value(func(val []byte) error {
				if len(val) == 0 {
					return fmt.Errorf("快照数据为空")
				}
				return snapshot.Unmarshal(val)
			})
		}
		if err != badger.ErrKeyNotFound {
			return err
		}
		return nil
	})

	if err == nil && !raft.IsEmptySnap(snapshot) {
		fmt.Printf("[LoadNewestSnapshot] 成功从数据库加载快照，索引: %d\n", snapshot.Metadata.Index)
		return &snapshot, nil
	}

	fmt.Println("[LoadNewestSnapshot] 数据库无快照，尝试从文件系统加载...")
	// 如果数据库中没有找到，尝试从文件系统加载
	names, err := s.ListSnapshots()
	if err != nil {
		fmt.Printf("[LoadNewestSnapshot] 列出快照文件失败: %v\n", err)
		return nil, fmt.Errorf("列出快照文件失败: %v", err)
	}
	if len(names) == 0 {
		fmt.Println("[LoadNewestSnapshot] Info: No snapshot found in storage")
		return nil, nil
	}

	filename := names[len(names)-1] // 最新的快照
	filepath := filepath.Join(s.dir, filename)
	fmt.Printf("[LoadNewestSnapshot] 读取快照文件: %s\n", filepath)

	// 读取快照文件
	data, err := os.ReadFile(filepath)
	if err != nil {
		fmt.Printf("[LoadNewestSnapshot] 读取快照文件失败: %v\n", err)
		return nil, fmt.Errorf("读取快照文件失败: %v", err)
	}

	// 反序列化快照
	if err := snapshot.Unmarshal(data); err != nil {
		fmt.Printf("[LoadNewestSnapshot] 反序列化快照失败: %v\n", err)
		return nil, fmt.Errorf("反序列化快照失败: %v", err)
	}

	// 验证快照数据
	if snapshot.Metadata.Index == 0 {
		fmt.Println("[LoadNewestSnapshot] 无效的快照索引")
		return nil, fmt.Errorf("无效的快照索引")
	}

	// 如果从文件系统成功加载，同步到数据库
	err = s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(keySnapshot, data)
	})
	if err != nil {
		fmt.Printf("[LoadNewestSnapshot] 警告：同步快照到数据库失败: %v\n", err)
	}

	fmt.Printf("[LoadNewestSnapshot] 成功加载快照，索引: %d，任期: %d\n", snapshot.Metadata.Index, snapshot.Metadata.Term)
	return &snapshot, nil
}

// GetFSM 获取状态机
func (s *Snapshotter) GetFSM() *fsm.KVStateMachine {
	return &s.fsm
}

// snapshotKey 生成快照键
func snapshotKey(index uint64) []byte {
	buf := make([]byte, len(prefixSnapshot)+8)
	copy(buf, prefixSnapshot)
	binary.BigEndian.PutUint64(buf[len(prefixSnapshot):], index)
	return buf
}

// parseSnapshotKey 从快照键中解析索引
func parseSnapshotKey(key []byte) uint64 {
	if len(key) < len(prefixSnapshot)+8 {
		return 0
	}
	return binary.BigEndian.Uint64(key[len(prefixSnapshot):])
}

// ListSnapshots 列出所有快照文件
func (s *Snapshotter) ListSnapshots() ([]string, error) {
	fmt.Printf("[ListSnapshots] 进入函数体: %s\n", s.dir)
	// 不再加锁，避免死锁

	dir, err := os.Open(s.dir)
	if err != nil {
		fmt.Printf("[ListSnapshots] 打开快照目录失败: %v\n", err)
		return nil, err
	}
	defer dir.Close()

	names, err := dir.Readdirnames(-1)
	if err != nil {
		fmt.Printf("[ListSnapshots] 读取快照目录文件名失败: %v\n", err)
		return nil, err
	}
	fmt.Printf("[ListSnapshots] 快照目录下文件: %v\n", names)

	var snaps []string
	for _, name := range names {
		if strings.HasSuffix(name, snapshotSuffix) {
			snaps = append(snaps, name)
		}
	}

	sort.Strings(snaps)
	fmt.Printf("[ListSnapshots] 有效快照文件: %v\n", snaps)
	return snaps, nil
}

// purgeOldSnapshots 删除旧的快照文件，只保留最新的几个
func (s *Snapshotter) purgeOldSnapshots() error {
	snaps, err := s.ListSnapshots()
	if err != nil {
		return err
	}

	// 如果快照数量超过限制，删除旧的快照
	if len(snaps) > maxSnapshots {
		for _, snap := range snaps[:len(snaps)-maxSnapshots] {
			if err := os.Remove(filepath.Join(s.dir, snap)); err != nil {
				return err
			}
		}
	}

	return nil
}

// getCurrentTerm 获取当前任期
func (s *Snapshotter) getCurrentTerm() uint64 {
	var term uint64
	s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(keyHardState)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var hs raftpb.HardState
			if err := hs.Unmarshal(val); err != nil {
				return err
			}
			term = hs.Term
			return nil
		})
	})
	return term
}
