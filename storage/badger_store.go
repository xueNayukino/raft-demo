package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"qklzl/fsm"

	"github.com/dgraph-io/badger/v3"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// 实现 etcd Raft 的 Storage 接口

var (
	// 键前缀定义
	prefixLogEntry = []byte("log/")
	keyHardState   = []byte("hardstate") //硬件状态
	keyConfState   = []byte("confstate") //配置状态
	keySnapshot    = []byte("snapshot")
	keyFirstIndex  = []byte("firstindex")

	prefixSnapshot = []byte("snapshot/")
	// 错误定义
	ErrKeyNotFound  = errors.New("key not found")
	ErrLogCompacted = errors.New("log compacted") //已压缩
)

const (
	// 默认 GC 间隔
	defaultGCInterval = 30 * time.Minute
	// 默认 GC 丢弃比率
	defaultGCDiscardRatio = 0.7
	// 默认快照检查间隔
	defaultSnapshotInterval = 30 * time.Minute
	// 快照创建通道缓冲区大小
	snapshotChannelBuffer = 10

	// 默认快照检查间隔
	defaultSnapshotCheckInterval = 30 * time.Minute
	// 默认快照阈值（日志条目数）
	defaultSnapshotCount = 10000
	// 快照文件后缀和最大数量
	snapshotSuffix = ".snap"
	maxSnapshots   = 5 // 保留的最大快照数量
)

// BadgerConfig 存储配置
type BadgerConfig struct {
	// GC 间隔
	GCInterval time.Duration
	// GC 丢弃比率
	GCDiscardRatio float64
	// 快照检查间隔
	SnapshotInterval time.Duration
	// 触发快照的日志条目数
	SnapshotCount uint64
}

// BadgerStorage 基于 BadgerDB 的存储实现
type BadgerStorage struct {
	db             *badger.DB
	config         BadgerConfig
	stopCh         chan struct{}
	snapshotCh     chan struct{}
	fsm            *fsm.KVStateMachine
	logEntryCount  uint64
	isSnapshotting int32
	mu             sync.RWMutex // 用于保护关闭操作
	fsmMu          sync.RWMutex // 用于保护FSM操作
	firstIndex     uint64       // 第一个可用的日志索引
	appliedIndex   uint64       // 最后应用的日志索引
}

// NewBadgerStorage 创建新的 BadgerDB 存储
func NewBadgerStorage(dir string, config *BadgerConfig, fsm *fsm.KVStateMachine) (*BadgerStorage, error) {
	if fsm == nil {
		return nil, fmt.Errorf("FSM不能为空")
	}

	if config == nil {
		config = &BadgerConfig{
			GCInterval:       defaultGCInterval,
			GCDiscardRatio:   defaultGCDiscardRatio,
			SnapshotInterval: defaultSnapshotInterval,
			SnapshotCount:    10000, // 默认快照阈值
		}
	}

	// 验证配置参数
	if config.GCInterval < time.Second {
		return nil, fmt.Errorf("GC间隔不能小于1秒")
	}
	if config.SnapshotInterval < time.Second {
		return nil, fmt.Errorf("快照检查间隔不能小于1秒")
	}
	if config.SnapshotCount < 10 {
		return nil, fmt.Errorf("快照计数不能小于10")
	}

	opts := badger.DefaultOptions(dir)
	opts.Logger = nil // 禁用 Badger 的日志输出

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	s := &BadgerStorage{
		db:             db,
		config:         *config,
		stopCh:         make(chan struct{}),
		snapshotCh:     make(chan struct{}, snapshotChannelBuffer),
		fsm:            fsm,
		logEntryCount:  0,
		isSnapshotting: 0,
	}

	// 初始化 firstIndex
	if err := s.initFirstIndex(); err != nil {
		db.Close()
		return nil, err
	}

	// 初始化日志条目计数
	if err := s.initLogEntryCount(); err != nil {
		db.Close()
		return nil, err
	}

	// 启动GC协程
	go s.runGC()
	// 启动快照检查协程
	go s.autoCheckSnapshot()
	// 启动异步快照处理
	go s.asyncSnapshotHandler()

	return s, nil
}

// Stop 停止存储服务
func (s *BadgerStorage) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	select {
	case <-s.stopCh:
		// 通道已经关闭，直接返回
		return
	default:
		close(s.stopCh)
		if s.db != nil {
			s.db.Close()
			// 添加短暂延迟，确保文件锁被完全释放
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// runGC 运行垃圾回收
func (s *BadgerStorage) runGC() {
	ticker := time.NewTicker(s.config.GCInterval) //定时器
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			err := s.db.RunValueLogGC(s.config.GCDiscardRatio) //检查哪些 Value Log 文件中的废弃数据比例 ≥ discardRatio（比如 0.7 表示 70% 的数据可丢弃）。 重写这些文件，只保留有效数据，释放磁盘空间，BadgerDB 的 Value Log 是追加写入的，旧数据可能被新数据覆盖或删除，但文件不会自动收缩。
			if err != nil && err != badger.ErrNoRewrite {
				fmt.Printf("BadgerDB GC failed: %v\n", err)
			}
		case <-s.stopCh: //当调用 Stop() 方法时，会执行 close(s.stopCh)，这会向所有监听它的协程发送一个"关闭信号"。
			return
		}
	}
}

// 日志条目键格式: "log/<index>"，键 = 固定前缀 + 大端序编码的 uint64 索引
func logKey(index uint64) []byte {
	buf := make([]byte, len(prefixLogEntry)+8)
	copy(buf, prefixLogEntry)
	binary.BigEndian.PutUint64(buf[len(prefixLogEntry):], index)
	return buf
}

// InitialState 返回初始的硬状态和配置状态
// 作用：返回 Raft 节点的初始状态（硬状态 + 配置状态）。
// 返回值：
// raftpb.HardState：包含当前任期（Term）、投票结果（Vote）、已提交日志索引（Commit）。
// raftpb.ConfState：集群成员配置（Voters/Learners）。
func (s *BadgerStorage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	////存储从 BadgerDB 加载的数据
	var hardState raftpb.HardState
	var confState raftpb.ConfState

	// 加载硬状态
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(keyHardState)
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		//item：BadgerDB 中表示一个键值对的 Item 对象（通过 Get 或迭代器获取）
		//回调函数，所有层级返回 nil，外层 err 为 nil，回调模式的优势在于 零拷贝和更简洁的错误传递
		return item.Value(func(val []byte) error {
			return hardState.Unmarshal(val)
		})
		//hardState 是一个 raftpb.HardState 类型的结构体实例。
		//Unmarshal 方法会修改 hardState 的字段（如 Term、Vote），将二进制数据 val 解析后填充到该结构体中。
	})

	if err != nil {
		return raftpb.HardState{}, raftpb.ConfState{}, err
	}
	//Raft 算法的强一致性要求
	//hardState：包含当前任期（Term）和投票记录（Vote），用于选举和日志一致性。
	//如果加载失败（如数据损坏），可能导致：
	//错误任期号引发脑裂（Split Brain）。
	//丢失投票记录导致重复投票。
	//confState：包含集群成员（Voters），用于决定哪些节点参与共识。
	//如果加载失败（且无法从快照恢复），可能导致：
	//无法识别合法集群成员，破坏安全性。
	//结论：任一状态损坏都会威胁 Raft 的安全性，必须立即报错终止。

	// 加载配置状态
	err = s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(keyConfState)
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			return confState.Unmarshal(val)
		})
	})

	if err != nil {
		return raftpb.HardState{}, raftpb.ConfState{}, err
	}

	// 如果没有配置状态，尝试从快照加载
	if len(confState.Voters) == 0 {
		snapshot, err := s.Snapshot()
		if err == nil && !IsEmptySnap(snapshot) {
			confState = snapshot.Metadata.ConfState
		}
	}

	return hardState, confState, nil
}

// Entries 获取指定范围的日志条目
func (s *BadgerStorage) Entries(lo, hi uint64, maxSize uint64) ([]raftpb.Entry, error) {
	// 检查范围的有效性
	if lo > hi {
		return nil, fmt.Errorf("无效的日志范围: lo %d > hi %d", lo, hi)
	}

	firstIndex, err := s.FirstIndex()
	if err != nil {
		return nil, err
	}
	lastIndex, err := s.LastIndex()
	if err != nil {
		return nil, err
	}

	// raft 规范：lo < firstIndex 返回 ErrLogCompacted
	if lo < firstIndex {
		return nil, ErrLogCompacted
	}
	// raft 规范：hi > lastIndex+1 或 lo > lastIndex 返回 raft.ErrUnavailable
	if hi > lastIndex+1 || lo > lastIndex {
		return nil, raft.ErrUnavailable
	}
	// raft 规范：lo >= hi 返回空切片和 nil
	if lo >= hi {
		return nil, nil
	}

	var entries []raftpb.Entry
	var size uint64

	err = s.db.View(func(txn *badger.Txn) error {
		for i := lo; i < hi; i++ {
			item, err := txn.Get(logKey(i))
			if err == badger.ErrKeyNotFound {
				return ErrLogCompacted
			}
			if err != nil {
				return err
			}

			var entry raftpb.Entry
			err = item.Value(func(val []byte) error {
				return entry.Unmarshal(val)
			})
			if err != nil {
				return err
			}

			size += uint64(entry.Size())
			if maxSize > 0 && size > maxSize && len(entries) > 0 {
				break
			}
			entries = append(entries, entry)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return entries, nil
}

// Term 获取指定索引日志的任期
func (s *BadgerStorage) Term(i uint64) (uint64, error) {
	// Raft 协议要求索引 0 的任期总是 0
	if i == 0 {
		return 0, nil
	}

	// 获取第一个和最后一个可用的日志索引
	firstIndex, err := s.FirstIndex()
	if err != nil {
		return 0, fmt.Errorf("failed to get first index: %v", err)
	}

	lastIndex, err := s.LastIndex()
	if err != nil {
		return 0, fmt.Errorf("failed to get last index: %v", err)
	}

	// 检查索引是否被压缩
	if i < firstIndex {
		// 尝试从快照获取任期
		snapshot, err := s.Snapshot()
		if err != nil {
			return 0, fmt.Errorf("failed to get snapshot: %v", err)
		}

		// 如果快照存在且索引匹配，返回快照的任期
		if !IsEmptySnap(snapshot) && i == snapshot.Metadata.Index {
			return snapshot.Metadata.Term, nil
		}

		// 如果索引小于快照索引，返回压缩错误
		if !IsEmptySnap(snapshot) && i < snapshot.Metadata.Index {
			return 0, ErrLogCompacted
		}

		return 0, ErrLogCompacted
	}

	// 检查索引是否超出范围
	if i > lastIndex {
		return 0, fmt.Errorf("index %d is out of range (first: %d, last: %d)", i, firstIndex, lastIndex)
	}

	var term uint64
	err = s.db.View(func(txn *badger.Txn) error {
		// 尝试从日志条目获取任期
		item, err := txn.Get(logKey(i))
		if err == nil {
			var entry raftpb.Entry
			err = item.Value(func(val []byte) error {
				return entry.Unmarshal(val)
			})
			if err != nil {
				return fmt.Errorf("failed to unmarshal log entry at index %d: %v", i, err)
			}
			term = entry.Term
			return nil
		}

		return fmt.Errorf("failed to get log entry at index %d: %v", i, err)
	})

	if err != nil {
		return 0, err
	}

	return term, nil
}

// FirstIndex 返回第一个日志条目的索引
func (s *BadgerStorage) FirstIndex() (uint64, error) {
	var firstIndex uint64
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(keyFirstIndex)
		if err == nil {
			return item.Value(func(val []byte) error {
				var entry raftpb.Entry
				if err := entry.Unmarshal(val); err != nil {
					return err
				}
				firstIndex = entry.Index
				return nil
			})
		}
		item, err = txn.Get(keySnapshot)
		if err == nil {
			return item.Value(func(val []byte) error {
				var snapshot raftpb.Snapshot
				if err := snapshot.Unmarshal(val); err != nil {
					return err
				}
				firstIndex = snapshot.Metadata.Index + 1
				return nil
			})
		}
		firstIndex = 1
		return nil
	})
	fmt.Printf("[FirstIndex] 返回 firstIndex=%d\n", firstIndex)
	if err != nil {
		return 0, err
	}
	return firstIndex, nil
}

// LastIndex 返回最后一个日志条目的索引
func (s *BadgerStorage) LastIndex() (uint64, error) {
	var lastIndex uint64
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(keySnapshot)
		if err == nil {
			err = item.Value(func(val []byte) error {
				var snapshot raftpb.Snapshot
				if err := snapshot.Unmarshal(val); err != nil {
					return err
				}
				lastIndex = snapshot.Metadata.Index
				return nil
			})
			if err != nil {
				return err
			}
		}
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefixLogEntry
		opts.Reverse = true
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(append(append([]byte{}, prefixLogEntry...), 0xFF)); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()
			if !bytes.HasPrefix(key, prefixLogEntry) {
				break
			}
			index := binary.BigEndian.Uint64(key[len(prefixLogEntry):])
			if index > lastIndex {
				lastIndex = index
			}
		}
		return nil
	})
	fmt.Printf("[LastIndex] 返回 lastIndex=%d\n", lastIndex)
	if err != nil {
		return 0, err
	}
	return lastIndex, nil
}

// Snapshot 获取最新的快照
func (s *BadgerStorage) Snapshot() (raftpb.Snapshot, error) {
	var snapshot raftpb.Snapshot
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(keySnapshot)
		if err == badger.ErrKeyNotFound {
			fmt.Printf("[Snapshot] 没有找到快照\n")
			return ErrKeyNotFound
		}
		if err != nil {
			fmt.Printf("[Snapshot] 获取快照出错: %v\n", err)
			return err
		}
		return item.Value(func(val []byte) error {
			if len(val) == 0 {
				fmt.Printf("[Snapshot] 快照数据为空\n")
				return ErrKeyNotFound
			}
			fmt.Printf("[Snapshot] 反序列化快照，数据长度=%d\n", len(val))
			return snapshot.Unmarshal(val)
		})
	})

	if err == ErrKeyNotFound {
		return raftpb.Snapshot{}, nil
	}
	if err != nil {
		return raftpb.Snapshot{}, err
	}

	fmt.Printf("[Snapshot] 读取到快照 Index=%d, Term=%d\n", snapshot.Metadata.Index, snapshot.Metadata.Term)
	return snapshot, nil
}

// --- 以下方法供上层在应用 Raft 更新时调用 ---

// SaveState 保存硬状态和配置状态
func (s *BadgerStorage) SaveState(hardState raftpb.HardState, confState raftpb.ConfState) error {
	return s.db.Update(func(txn *badger.Txn) error {
		// 保存硬状态
		if !IsEmptyHardState(hardState) {
			hsData, err := hardState.Marshal()
			if err != nil {
				return err
			}
			if err := txn.Set(keyHardState, hsData); err != nil {
				return err
			}
		}

		// 保存配置状态
		if !IsEmptyConfState(confState) {
			csData, err := confState.Marshal()
			if err != nil {
				return err
			}
			if err := txn.Set(keyConfState, csData); err != nil {
				return err
			}
		}

		return nil
	})
}

// SaveEntries 保存日志条目
func (s *BadgerStorage) SaveEntries(entries []raftpb.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// 使用批量写入
	wb := s.db.NewWriteBatch()
	defer wb.Cancel()

	for _, entry := range entries {
		data, err := entry.Marshal()
		if err != nil {
			return fmt.Errorf("序列化日志条目失败: %v", err)
		}

		if err := wb.Set(logKey(entry.Index), data); err != nil {
			return fmt.Errorf("保存日志条目失败: %v", err)
		}
		s.IncrementLogEntryCount()
	}

	if err := wb.Flush(); err != nil {
		return fmt.Errorf("提交批量写入失败: %v", err)
	}

	fmt.Printf("成功保存 %d 个日志条目\n", len(entries))
	return nil
}

// SaveSnapshot 保存快照
func (s *BadgerStorage) SaveSnapshot(snapshot raftpb.Snapshot) error {
	if IsEmptySnap(snapshot) {
		fmt.Println("[SaveSnapshot] 空快照，跳过保存")
		return nil
	}

	fmt.Printf("[SaveSnapshot] 保存快照，Index=%d, Term=%d\n", snapshot.Metadata.Index, snapshot.Metadata.Term)
	data, err := snapshot.Marshal()
	if err != nil {
		fmt.Printf("[SaveSnapshot] Marshal 失败: %v\n", err)
		return err
	}

	// 1. 写入数据库
	err = s.db.Update(func(txn *badger.Txn) error {
		fmt.Println("[SaveSnapshot] 写入数据库 keySnapshot")
		return txn.Set(keySnapshot, data)
	})
	if err != nil {
		return err
	}

	// 2. 写入快照文件（以 Index-Term 命名）
	dir := s.db.Opts().Dir
	if dir == "" {
		dir = "."
	}
	fileName := fmt.Sprintf("%016x-%016x.snap", snapshot.Metadata.Index-snapshot.Metadata.Term+1, snapshot.Metadata.Index)
	filePath := dir + "/" + fileName
	fmt.Printf("[SaveSnapshot] 写入快照文件: %s\n", filePath)
	if err := os.WriteFile(filePath, data, 0644); err != nil {
		fmt.Printf("[SaveSnapshot] 写入快照文件失败: %v\n", err)
		return err
	}

	return nil
}

// CompactLog 压缩指定索引之前的日志
func (s *BadgerStorage) CompactLog(compactIndex uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 获取当前的第一个索引
	firstIndex, err := s.FirstIndex()
	if err != nil {
		return fmt.Errorf("获取第一个索引失败: %v", err)
	}

	// 如果压缩点小于或等于第一个索引，不需要压缩
	if compactIndex <= firstIndex {
		return nil
	}

	// 获取最后一个索引
	lastIndex, err := s.LastIndex()
	if err != nil {
		return fmt.Errorf("获取最后一个索引失败: %v", err)
	}

	// 如果压缩点大于最后一个索引，返回错误
	if compactIndex > lastIndex {
		return fmt.Errorf("压缩索引 %d 大于最后一个索引 %d", compactIndex, lastIndex)
	}

	// 开始批量删除
	wb := s.db.NewWriteBatch()
	defer wb.Cancel()

	// 删除从第一个索引到压缩点之前的所有日志
	deletedCount := uint64(0)
	for i := firstIndex; i < compactIndex; i++ {
		if err := wb.Delete(logKey(i)); err != nil {
			return fmt.Errorf("删除日志条目 %d 失败: %v", i, err)
		}
		s.DecrementLogEntryCount()
		deletedCount++
	}

	// 更新第一个可用的索引
	if err := s.saveFirstIndex(compactIndex); err != nil {
		return fmt.Errorf("更新第一个索引失败: %v", err)
	}

	// 提交批量删除
	if err := wb.Flush(); err != nil {
		return fmt.Errorf("提交批量删除失败: %v", err)
	}

	// 更新内存中的值
	atomic.StoreUint64(&s.firstIndex, compactIndex)

	fmt.Printf("成功压缩日志，删除了 %d 个条目，新的第一个索引: %d\n", deletedCount, compactIndex)
	return nil
}

// parseLogKey 从日志键中解析索引
func parseLogKey(key []byte) uint64 {
	if len(key) < len(prefixLogEntry)+8 {
		return 0
	}
	return binary.BigEndian.Uint64(key[len(prefixLogEntry):])
}

// saveFirstIndex 保存第一个可用的日志索引
func (s *BadgerStorage) saveFirstIndex(index uint64) error {
	// 创建一个特殊的Entry来保存索引
	entry := raftpb.Entry{
		Index: index,
		Term:  0, // 使用0作为特殊标记
	}
	data, err := entry.Marshal()
	if err != nil {
		return fmt.Errorf("序列化索引条目失败: %v", err)
	}

	// 使用原子写入
	err = s.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(keyFirstIndex, data); err != nil {
			return fmt.Errorf("保存第一个索引失败: %v", err)
		}
		return nil
	})

	if err != nil {
		return err
	}

	// 更新内存中的值
	atomic.StoreUint64(&s.firstIndex, index)
	return nil
}

// getLastSnapshotIndex 获取最后一个快照的索引
func (s *BadgerStorage) getLastSnapshotIndex() (uint64, error) {
	var index uint64
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(keySnapshot)
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var snapshot raftpb.Snapshot
			if err := snapshot.Unmarshal(val); err != nil {
				return err
			}
			index = snapshot.Metadata.Index
			return nil
		})
	})
	return index, err
}

// getCurrentTerm 获取当前任期
func (s *BadgerStorage) getCurrentTerm() uint64 {
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

// Append 原子性保存硬状态、配置状态和日志条目
func (s *BadgerStorage) Append(hs *raftpb.HardState, cs *raftpb.ConfState, entries []raftpb.Entry) error {
	if hs == nil && cs == nil && (entries == nil || len(entries) == 0) {
		return nil
	}

	wb := s.db.NewWriteBatch()
	defer wb.Cancel()

	// 保存硬状态
	if hs != nil && !IsEmptyHardState(*hs) {
		data, err := hs.Marshal()
		if err != nil {
			return fmt.Errorf("序列化硬状态失败: %v", err)
		}
		if err := wb.Set(keyHardState, data); err != nil {
			return fmt.Errorf("保存硬状态失败: %v", err)
		}
	}

	// 保存配置状态
	if cs != nil && !IsEmptyConfState(*cs) {
		data, err := cs.Marshal()
		if err != nil {
			return fmt.Errorf("序列化配置状态失败: %v", err)
		}
		if err := wb.Set(keyConfState, data); err != nil {
			return fmt.Errorf("保存配置状态失败: %v", err)
		}
	}

	// 保存日志条目
	if entries != nil && len(entries) > 0 {
		for _, entry := range entries {
			data, err := entry.Marshal()
			if err != nil {
				return fmt.Errorf("序列化日志条目失败: %v", err)
			}
			if err := wb.Set(logKey(entry.Index), data); err != nil {
				return fmt.Errorf("保存日志条目失败: %v", err)
			}
			s.IncrementLogEntryCount()
		}
	}

	// 提交批量写入
	if err := wb.Flush(); err != nil {
		return fmt.Errorf("提交批量写入失败: %v", err)
	}

	return nil
}

// loadConfState 加载配置状态
func (s *BadgerStorage) loadConfState() (raftpb.ConfState, error) {
	var cs raftpb.ConfState
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(keyConfState)
		if err != nil {
			return err
		}

		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}

		return cs.Unmarshal(val)
	})

	if err == badger.ErrKeyNotFound {
		return raftpb.ConfState{}, ErrKeyNotFound
	}

	return cs, err
}

// SaveRaftState 保存 Raft 硬状态
func (s *BadgerStorage) SaveRaftState(hardState raftpb.HardState) error {
	return s.db.Update(func(txn *badger.Txn) error {
		// 序列化硬状态
		data, err := hardState.Marshal()
		if err != nil {
			return fmt.Errorf("序列化硬状态失败: %v", err)
		}

		// 保存硬状态
		return txn.Set(keyHardState, data)
	})
}

// IsEmptyHardState 检查硬状态是否为空
func IsEmptyHardState(st raftpb.HardState) bool {
	return raft.IsEmptyHardState(st)
}

// IsEmptyConfState 检查配置状态是否为空
func IsEmptyConfState(cs raftpb.ConfState) bool {
	return len(cs.Voters) == 0 && len(cs.Learners) == 0
}

// IsEmptySnap 检查快照是否为空
func IsEmptySnap(sp raftpb.Snapshot) bool {
	return raft.IsEmptySnap(sp)
}

// DB 返回底层 BadgerDB 实例
func (s *BadgerStorage) DB() *badger.DB {
	return s.db
}

// SetConfState 设置配置状态
func (s *BadgerStorage) SetConfState(cs *raftpb.ConfState) error {
	// 将 ConfState 序列化为字节
	data, err := cs.Marshal()
	if err != nil {
		return err
	}

	// 写入数据库
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(keyConfState, data)
	})
}

// GetConfState 获取配置状态
func (s *BadgerStorage) GetConfState() (*raftpb.ConfState, error) {
	var cs raftpb.ConfState
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(keyConfState)
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return cs.Unmarshal(val)
		})
	})
	if err != nil {
		return nil, err
	}
	return &cs, nil
}

// SetHardState 设置硬状态
func (s *BadgerStorage) SetHardState(st *raftpb.HardState) error {
	// 将 HardState 序列化为字节
	data, err := st.Marshal()
	if err != nil {
		return err
	}

	// 写入数据库
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(keyHardState, data)
	})
}

// GetHardState 获取硬状态
func (s *BadgerStorage) GetHardState() (*raftpb.HardState, error) {
	var hs raftpb.HardState
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(keyHardState)
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return hs.Unmarshal(val)
		})
	})
	if err != nil {
		return nil, err
	}
	return &hs, nil
}

// writeBatchWithCancel 封装 WriteBatch 的创建和处理
func (s *BadgerStorage) writeBatchWithCancel(fn func(*badger.WriteBatch) error) error {
	wb := s.db.NewWriteBatch()
	var committed bool
	defer func() {
		if !committed {
			wb.Cancel()
		}
	}()

	if err := fn(wb); err != nil {
		return err
	}

	if err := wb.Flush(); err != nil {
		return err
	}

	committed = true
	return nil
}

// initLogEntryCount 初始化日志条目计数
func (s *BadgerStorage) initLogEntryCount() error {
	count, err := s.countLogEntries()
	if err != nil {
		return fmt.Errorf("计算日志条目数量失败: %v", err)
	}

	atomic.StoreUint64(&s.logEntryCount, count)
	fmt.Printf("初始化日志条目计数: %d\n", count)
	return nil
}

// countLogEntries 计算日志条目数量
func (s *BadgerStorage) countLogEntries() (uint64, error) {
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
func (s *BadgerStorage) IncrementLogEntryCount() {
	newCount := atomic.AddUint64(&s.logEntryCount, 1)
	// 检查是否需要触发快照
	if newCount >= s.config.SnapshotCount {
		// 如果已经在创建快照，不重复触发
		if atomic.LoadInt32(&s.isSnapshotting) == 1 {
			return
		}
		select {
		case s.snapshotCh <- struct{}{}:
			// 标记快照创建状态
			atomic.StoreInt32(&s.isSnapshotting, 1)
		default:
			// 如果通道已满，说明已经有快照任务在进行中
		}
	}
}

// DecrementLogEntryCount 减少日志条目计数
func (s *BadgerStorage) DecrementLogEntryCount() {
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

// autoCheckSnapshot 定期检查是否需要创建快照
func (s *BadgerStorage) autoCheckSnapshot() {
	ticker := time.NewTicker(s.config.SnapshotInterval)
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

// asyncSnapshotHandler 异步处理快照创建
func (s *BadgerStorage) asyncSnapshotHandler() {
	for {
		select {
		case <-s.snapshotCh:
			// 异步创建快照
			if err := s.checkAndCreateSnapshot(); err != nil {
				fmt.Printf("异步快照创建失败: %v\n", err)
			}
		case <-s.stopCh:
			return
		}
	}
}

// checkAndCreateSnapshot 检查并创建快照
func (s *BadgerStorage) checkAndCreateSnapshot() error {
	if !atomic.CompareAndSwapInt32(&s.isSnapshotting, 0, 1) {
		return nil // 已经在创建快照
	}
	defer atomic.StoreInt32(&s.isSnapshotting, 0)

	// 获取当前日志条目数
	currentEntryCount := atomic.LoadUint64(&s.logEntryCount)
	if currentEntryCount < s.config.SnapshotCount {
		return nil
	}

	s.fsmMu.RLock() // 保护FSM操作
	defer s.fsmMu.RUnlock()

	// 创建快照
	snapshot, err := s.fsm.SaveSnapshot()
	if err != nil {
		return fmt.Errorf("创建状态机快照失败: %v", err)
	}

	// 获取当前应用的索引
	lastIndex, err := s.LastIndex()
	if err != nil {
		return fmt.Errorf("获取最后索引失败: %v", err)
	}

	// 保存快照
	if err := s.SaveSnapshot(raftpb.Snapshot{
		Data: snapshot,
		Metadata: raftpb.SnapshotMetadata{
			Index: lastIndex,
			Term:  s.getCurrentTerm(),
		},
	}); err != nil {
		return fmt.Errorf("保存快照失败: %v", err)
	}

	return nil
}

// initFirstIndex 初始化第一个可用的日志索引
func (s *BadgerStorage) initFirstIndex() error {
	return s.db.View(func(txn *badger.Txn) error {
		// 先尝试从专门的键中获取
		item, err := txn.Get(keyFirstIndex)
		if err == nil {
			return item.Value(func(val []byte) error {
				var entry raftpb.Entry
				if err := entry.Unmarshal(val); err != nil {
					return err
				}
				s.firstIndex = entry.Index
				return nil
			})
		}

		// 如果找不到，尝试从快照中获取
		item, err = txn.Get(keySnapshot)
		if err == nil {
			return item.Value(func(val []byte) error {
				var snapshot raftpb.Snapshot
				if err := snapshot.Unmarshal(val); err != nil {
					return err
				}
				s.firstIndex = snapshot.Metadata.Index + 1
				return nil
			})
		}

		// 如果都没有，设置为1
		s.firstIndex = 1
		return nil
	})
}
