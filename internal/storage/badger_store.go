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

	"qklzl/internal/fsm"

	"github.com/dgraph-io/badger/v3"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// 实现 etcd Raft 的 Storage 接口

var (
	// 键前缀定义 区分不同类型的数据
	prefixLogEntry = []byte("log/")
	keyHardState   = []byte("hardstate")  //硬件状态，存储HardState（当前任期、投票等信息）的键
	keyConfState   = []byte("confstate")  //配置状态
	keySnapshot    = []byte("snapshot")   //快照
	keyFirstIndex  = []byte("firstindex") //第一个日志索引，用于日志压缩

	prefixSnapshot = []byte("snapshot/") //快照前缀
	// 错误定义
	ErrKeyNotFound  = errors.New("key not found")
	ErrLogCompacted = errors.New("log compacted") //已压缩
)

const (
	// 默认 GC，垃圾回收 间隔
	defaultGCInterval = 30 * time.Minute
	// 默认 GC 丢弃比率，即70%的文件可以被丢弃时才进行GC
	defaultGCDiscardRatio = 0.7
	// 默认快照检查间隔
	defaultSnapshotInterval = 30 * time.Minute
	// 快照创建通道缓冲区大小
	snapshotChannelBuffer = 10

	// 默认快照检查间隔
	defaultSnapshotCheckInterval = 30 * time.Minute
	// 触发快照的日志条目数，默认快照阈值（日志条目数）
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
	db         *badger.DB //实例
	config     BadgerConfig
	stopCh     chan struct{} //用于控制后台协程
	snapshotCh chan struct{} //触发快照的信号通道

	fsm *fsm.KVStateMachine

	logEntryCount  uint64 //快照控制，当前日志条数
	isSnapshotting int32  //原子标记，表示是否正在快照（0表示否，1表示是

	mu    sync.RWMutex // 用于保护关闭操作，防止并发关闭
	fsmMu sync.RWMutex // 用于保护FSM操作，例如应用快照时

	firstIndex   uint64 // 第一个可用的日志索引
	appliedIndex uint64 //  最后被应用到状态机的日志索引
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

	// 确保目录存在
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("创建目录失败: %v", err)
	}

	// 不再尝试删除锁文件

	fmt.Printf("打开BadgerDB,目录: %s\n", dir)
	opts := badger.DefaultOptions(dir)
	opts.Logger = nil            // 禁用 Badger 的日志输出
	opts.DetectConflicts = false // 禁用冲突检测，提高性能
	opts.NumVersionsToKeep = 1   // 只保留一个版本，节约空间
	opts.NumCompactors = 2       // 至少需要2个压缩器，提高效率
	opts.SyncWrites = false      // 异步写入，提高性能

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
			// 不再等待锁释放
		}
	}
}

// runGC 运行垃圾回收
func (s *BadgerStorage) runGC() {
	ticker := time.NewTicker(s.config.GCInterval) //定时器，每隔config.GCInterval时间执行一次垃圾回收
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C: //每隔一段时间自动启动
			err := s.db.RunValueLogGC(s.config.GCDiscardRatio) //垃圾回收方法 RunValueLogGC，BadgerDB 的 Value Log 是追加写入的，旧数据被覆盖或删除后，文件不会自动变小，必须靠 GC 回收
			if err != nil && err != badger.ErrNoRewrite {
				// fmt.Printf("BadgerDB GC failed: %v\n", err)
			}
		case <-s.stopCh: //当调用 Stop() 方法时会向所有监听它的协程发送一个"关闭信号"。
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

func (s *BadgerStorage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	var hs raftpb.HardState
	var cs raftpb.ConfState

	err := s.db.View(func(txn *badger.Txn) error {
		// 1. 获取硬状态
		item, err := txn.Get(keyHardState)
		if err != nil && err != badger.ErrKeyNotFound {
			return err
		}
		if err == nil {
			err = item.Value(func(val []byte) error {
				return hs.Unmarshal(val)
			})
			if err != nil {
				return err
			}
		}

		// 2. 获取配置状态
		item, err = txn.Get(keyConfState)
		if err != nil && err != badger.ErrKeyNotFound {
			return err
		}
		if err == nil {
			err = item.Value(func(val []byte) error {
				return cs.Unmarshal(val)
			})
			if err != nil {
				return err
			}
		}

		// 3. 获取快照，确保硬状态的提交索引不小于快照索引
		item, err = txn.Get(keySnapshot)
		if err != nil && err != badger.ErrKeyNotFound {
			return err
		}
		if err == nil {
			err = item.Value(func(val []byte) error {
				var snapshot raftpb.Snapshot
				if err := snapshot.Unmarshal(val); err != nil {
					return err
				}
				// 确保提交索引不小于快照索引
				if hs.Commit < snapshot.Metadata.Index {
					hs.Commit = snapshot.Metadata.Index
				}
				return nil
			})
			if err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		return hs, cs, err
	}

	return hs, cs, nil
}

// Entries 获取指定范围的日志条目，前闭后开  maxSize：返回条目的最大总字节数（0 表示无限制）
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
	var size uint64 //跟踪当前已加载条目的总大小（用于 maxSize 限制

	err = s.db.View(func(txn *badger.Txn) error {
		for i := lo; i < hi; i++ {
			item, err := txn.Get(logKey(i)) //key + value
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
			if maxSize > 0 && size > maxSize && len(entries) > 0 { //限制从数据库中读取的日志条目的总大小，以避免内存过度使用
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
		item, err := txn.Get(keyFirstIndex) //创建快照并压缩日志时，会更新这个值keyFirstIndex
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
	if err != nil {
		return 0, err
	}
	return firstIndex, nil
}

// LastIndex 返回最后一个日志条目的索引
func (s *BadgerStorage) LastIndex() (uint64, error) {
	var lastIndex uint64
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(keySnapshot) //如果刚好是快照，则直接返回快照的索引
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
		opts.Reverse = true //反向
		it := txn.NewIterator(opts)
		defer it.Close()
		//append([]byte{}, prefixLogEntry...)：复制 "log/" 前缀（避免污染原切片）追加 0xFF：定位到前缀下的最大可能键（BadgerDB 的字节序特性）
		for it.Seek(append(append([]byte{}, prefixLogEntry...), 0xFF)); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()
			if !bytes.HasPrefix(key, prefixLogEntry) {
				break
			}
			index := binary.BigEndian.Uint64(key[len(prefixLogEntry):])
			if index > lastIndex {
				lastIndex = index //通过比较日志中的索引和快照索引，取较大值
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return lastIndex, nil
}

// Snapshot 获取最新的快照
func (s *BadgerStorage) Snapshot() (raftpb.Snapshot, error) {
	// 添加数据库状态检查
	if s.db == nil {
		return raftpb.Snapshot{}, fmt.Errorf("数据库未初始化")
	}

	// 检查数据库是否已关闭
	if s.db.IsClosed() {
		return raftpb.Snapshot{}, fmt.Errorf("数据库已关闭")
	}

	var snapshot raftpb.Snapshot
	err := s.db.View(func(txn *badger.Txn) error {
		// 添加事务检查
		if txn == nil {
			return fmt.Errorf("无效的事务")
		}

		item, err := txn.Get(keySnapshot) //keySnapshot是一个预定义的键，用于存储最新的快照
		if err == badger.ErrKeyNotFound {
			return nil // 返回空快照而不是错误
		}
		if err != nil {
			return fmt.Errorf("获取快照失败: %v", err)
		}

		return item.Value(func(val []byte) error {
			// 添加值检查
			if len(val) == 0 {
				return nil // 返回空快照
			}
			return snapshot.Unmarshal(val)
		})
	})

	if err != nil {
		return raftpb.Snapshot{}, err
	}

	return snapshot, nil
}

// --- 以下方法供上层在应用 Raft 更新时调用 ---

// SaveState 保存硬状态和配置状态,存入数据库里
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

	return nil
}

// SaveSnapshot 保存快照
func (s *BadgerStorage) SaveSnapshot(snapshot raftpb.Snapshot) error {
	if IsEmptySnap(snapshot) {
		return nil
	}

	data, err := snapshot.Marshal()
	if err != nil {
		return err
	}

	// 1. 写入数据库
	return s.db.Update(func(txn *badger.Txn) error {
		// 保存快照数据
		if err := txn.Set(keySnapshot, data); err != nil {
			return err
		}

		// 2. 更新硬状态的提交索引
		item, err := txn.Get(keyHardState)
		if err != nil && err != badger.ErrKeyNotFound {
			return err
		}

		var hs raftpb.HardState
		if err == nil {
			err = item.Value(func(val []byte) error {
				return hs.Unmarshal(val)
			})
			if err != nil {
				return err
			}
		}

		// 更新提交索引
		if hs.Commit < snapshot.Metadata.Index {
			hs.Commit = snapshot.Metadata.Index
			hsData, err := hs.Marshal()
			if err != nil {
				return err
			}
			if err := txn.Set(keyHardState, hsData); err != nil {
				return err
			}
		}

		// 3. 更新 firstIndex
		firstIndex := snapshot.Metadata.Index + 1
		if err := s.saveFirstIndex(firstIndex); err != nil {
			return err
		}

		return nil
	})
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

	// 获取当前硬状态
	var hs raftpb.HardState
	err = s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(keyHardState)
		if err != nil && err != badger.ErrKeyNotFound {
			return err
		}
		if err == nil {
			return item.Value(func(val []byte) error {
				return hs.Unmarshal(val)
			})
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("获取硬状态失败: %v", err)
	}

	// 确保压缩点不大于提交索引，但仅当硬状态不为空且提交索引大于0时才检查
	if !IsEmptyHardState(hs) && hs.Commit > 0 && compactIndex > hs.Commit {
		return fmt.Errorf("压缩索引 %d 大于提交索引 %d", compactIndex, hs.Commit)
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

	return nil
}

// parseLogKey 从日志键中解析索引
func parseLogKey(key []byte) uint64 {
	if len(key) < len(prefixLogEntry)+8 {
		return 0
	}
	return binary.BigEndian.Uint64(key[len(prefixLogEntry):]) //大端，uint64
}

// saveFirstIndex 保存第一个可用的日志索引
func (s *BadgerStorage) saveFirstIndex(index uint64) error {
	// 创建一个特殊的Entry来保存索引
	entry := raftpb.Entry{
		Index: index,
		Term:  0, // 使用0作为特殊标记
	}
	data, err := entry.Marshal() //data是序列化后的一个日志条目
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
		// 获取当前快照索引
		var snapshotIndex uint64
		err := s.db.View(func(txn *badger.Txn) error {
			item, err := txn.Get(keySnapshot)
			if err != nil && err != badger.ErrKeyNotFound {
				return err
			}
			if err == nil {
				return item.Value(func(val []byte) error {
					var snapshot raftpb.Snapshot
					if err := snapshot.Unmarshal(val); err != nil {
						return err
					}
					snapshotIndex = snapshot.Metadata.Index
					return nil
				})
			}
			return nil
		})
		if err != nil {
			return err
		}

		// 确保提交索引不小于快照索引
		if hs.Commit < snapshotIndex {
			hs.Commit = snapshotIndex
		}

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
			wb.Cancel() //在函数退出时检查写入是否已完成，若未提交则取消批量操作
		}
	}()

	if err := fn(wb); err != nil { //执行外部传入的写入逻辑（如添加多个键值对到 WriteBatch）
		return err
	}

	if err := wb.Flush(); err != nil { //将批量操作原子性提交到 BadgerDB
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

	// fmt.Printf("初始化日志条目计数: %d\n", count)
	return nil
}

// countLogEntries 计算日志条目数量
func (s *BadgerStorage) countLogEntries() (uint64, error) {
	var count uint64
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefixLogEntry //设置迭代器只遍历指定前缀的键
		opts.PrefetchValues = false  //不需要获取值，只统计键的数量
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefixLogEntry); it.Valid(); it.Next() { //it.Valid()：判断当前迭代器位置是否有效（即还有数据）
			key := it.Item().Key()
			if !bytes.HasPrefix(key, prefixLogEntry) { //bytes.HasPrefix 判断 key 是否还以 prefixLogEntry 开头（防止遍历到别的类型的数据）。
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
				// 移除调试输出
				// fmt.Printf("异步快照创建失败: %v\n", err)
			}
		case <-s.stopCh:
			return
		}
	}
}

// checkAndCreateSnapshot 检查并创建快照
func (s *BadgerStorage) checkAndCreateSnapshot() error {
	if !atomic.CompareAndSwapInt32(&s.isSnapshotting, 0, 1) { //s.isSnapshotting 是一个标志位，0表示"没有快照正在创建"，1表示"有快照正在创建
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
		// 移除调试输出
		// fmt.Printf("异步快照创建失败: %v\n", err)
		return fmt.Errorf("保存快照失败: %v", err)
	}

	return nil
}

// initFirstIndex 初始化第一个可用的日志索引
func (s *BadgerStorage) initFirstIndex() error {
	return s.db.View(func(txn *badger.Txn) error {
		// 第一条可用日志的key：keyFirstIndex
		item, err := txn.Get(keyFirstIndex)
		if err == nil {
			return item.Value(func(val []byte) error {
				var entry raftpb.Entry
				if err := entry.Unmarshal(val); err != nil {
					return err
				}
				s.firstIndex = entry.Index //代表当前存储中“第一条可用日志”的索引
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
				s.firstIndex = snapshot.Metadata.Index + 1 //取快照元数据中的索引（`snapshot.Metadata.Index`），加1后作为`firstIndex`
				return nil
			})
		}

		// 如果都没有，设置为1
		s.firstIndex = 1
		return nil
	})
}

// NewInMemoryBadgerStorage 创建新的内存模式 BadgerDB 存储
func NewInMemoryBadgerStorage(dir string) (*BadgerStorage, error) {
	// 创建内存模式的BadgerDB
	opts := badger.DefaultOptions("").WithInMemory(true)
	opts.Logger = nil // 禁用日志

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("打开内存数据库失败: %v", err)
	}

	// 创建状态机
	stateMachine := fsm.NewKVStateMachine(db)

	// 创建存储实例
	s := &BadgerStorage{
		db: db,
		config: BadgerConfig{
			GCInterval:       defaultGCInterval,
			GCDiscardRatio:   defaultGCDiscardRatio,
			SnapshotInterval: defaultSnapshotInterval,
			SnapshotCount:    defaultSnapshotCount,
		},
		stopCh:         make(chan struct{}),
		snapshotCh:     make(chan struct{}, snapshotChannelBuffer),
		fsm:            stateMachine,
		logEntryCount:  0,
		isSnapshotting: 0,
		firstIndex:     1, // 设置初始索引为1
	}

	// 启动GC协程
	go s.runGC()
	// 启动快照检查协程
	go s.autoCheckSnapshot()
	// 启动异步快照处理
	go s.asyncSnapshotHandler()

	return s, nil
}

// GetFSM 返回状态机引用
func (s *BadgerStorage) GetFSM() *fsm.KVStateMachine {
	return s.fsm
}

// NewBadgerStorageWithDB 使用已创建的BadgerDB实例创建存储
func NewBadgerStorageWithDB(dir string, db *badger.DB, config *BadgerConfig, fsm *fsm.KVStateMachine) (*BadgerStorage, error) {
	if fsm == nil {
		return nil, fmt.Errorf("FSM不能为空")
	}

	if db == nil {
		return nil, fmt.Errorf("BadgerDB实例不能为空")
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
		return nil, err
	}

	// 初始化日志条目计数
	if err := s.initLogEntryCount(); err != nil {
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

// GetAppliedIndex 获取已应用的日志索引
func (s *BadgerStorage) GetAppliedIndex() uint64 {
	return atomic.LoadUint64(&s.appliedIndex)
}

// SetAppliedIndex 设置已应用的日志索引
func (s *BadgerStorage) SetAppliedIndex(index uint64) {
	atomic.StoreUint64(&s.appliedIndex, index)
}
