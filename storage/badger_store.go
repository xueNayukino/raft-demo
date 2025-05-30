package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

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

	// 错误定义
	ErrKeyNotFound  = errors.New("key not found")
	ErrLogCompacted = errors.New("log compacted") //已压缩
)

const (
	// GC 相关配置
	defaultGCInterval     = 10 * time.Minute
	defaultGCDiscardRatio = 0.5
)

// BadgerConfig BadgerDB 配置
type BadgerConfig struct {
	// GC 检查间隔
	GCInterval time.Duration
	// GC 丢弃比率
	GCDiscardRatio float64
}

// BadgerStorage 实现 etcd Raft 的 Storage 接口
type BadgerStorage struct {
	db     *badger.DB
	config BadgerConfig
	stopCh chan struct{} //停止GC协程的信号通道
}

// NewBadgerStorage 创建新的 Badger 存储适配器，dir 是数据库文件存储的目录路径（如 "./data"）
func NewBadgerStorage(dir string, config *BadgerConfig) (*BadgerStorage, error) {
	if config == nil {
		config = &BadgerConfig{
			GCInterval:     defaultGCInterval,
			GCDiscardRatio: defaultGCDiscardRatio,
		}
	}

	// 配置 BadgerDB
	//默认选项包括了一些合理的默认值，比如：
	//纯Go实现
	//1GB的memtable大小
	//基于文件的value存储等
	opts := badger.DefaultOptions(dir)
	opts.SyncWrites = true // 启用同步写入以保证持久性，每次写入都会调用 fsync 确保数据落盘，保证持久性，但性能会下降。

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	//数据库已经建好了
	s := &BadgerStorage{
		db:     db,
		config: *config,
		stopCh: make(chan struct{}),
	}

	// 启动后台垃圾回收（GC）协程。
	go s.runGC()

	return s, nil
}

// Stop 停止存储服务
func (s *BadgerStorage) Stop() {
	close(s.stopCh)
	s.db.Close()
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
	//将 uint64 按大端序写入字节切片
	binary.BigEndian.PutUint64(buf[len(prefixLogEntry):], index)
	return buf
}

//type Storage interface {
//    InitialState() (pb.HardState, pb.ConfState, error)5
//    Entries(lo, hi, maxSize uint64) ([]pb.Entry, error)4
//    Term(i uint64) (uint64, error)3
//    LastIndex() (uint64, error)2
//    FirstIndex() (uint64, error)1
//    Snapshot() (pb.Snapshot, error)6
//}

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

// Entries 获取指定范围内的日志条目
func (s *BadgerStorage) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	// 检查索引范围的有效性
	if lo > hi {
		return nil, fmt.Errorf("invalid range: lo(%d) > hi(%d)", lo, hi)
	}

	firstIndex, err := s.FirstIndex()
	if err != nil {
		return nil, err
	}

	lastIndex, err := s.LastIndex()
	if err != nil {
		return nil, err
	}

	// 处理压缩的情况
	if lo < firstIndex {
		return nil, ErrLogCompacted
	}

	// 处理越界的情况
	if hi > lastIndex+1 {
		return nil, fmt.Errorf("entries index out of range [%d, %d)", lo, hi)
	}

	// 处理无新条目的情况
	if lo == hi {
		return nil, nil
	}

	var entries []raftpb.Entry
	var entriesSize uint64

	err = s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefixLogEntry
		opts.PrefetchValues = true
		opts.PrefetchSize = int(hi - lo)

		it := txn.NewIterator(opts)
		defer it.Close()

		// 定位到起始索引
		startKey := logKey(lo)
		it.Seek(startKey)

		for ; it.Valid(); it.Next() {
			key := it.Item().Key()
			if !bytes.HasPrefix(key, prefixLogEntry) {
				break
			}

			index := binary.BigEndian.Uint64(key[len(prefixLogEntry):])
			if index >= hi {
				break
			}

			var entry raftpb.Entry
			err := it.Item().Value(func(val []byte) error {
				return entry.Unmarshal(val)
			})
			if err != nil {
				return fmt.Errorf("failed to unmarshal log entry at index %d: %v", index, err)
			}

			// 检查大小限制
			entrySize := uint64(entry.Size())
			if maxSize > 0 && entries != nil && entriesSize+entrySize > maxSize {
				break
			}

			entries = append(entries, entry)
			entriesSize += entrySize
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	// 确保返回至少一个条目（如果存在且未超过大小限制）
	if len(entries) == 0 && lo <= lastIndex {
		return nil, fmt.Errorf("missing log entry [%d]", lo)
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

// LastIndex 获取最后一条日志的索引
func (s *BadgerStorage) LastIndex() (uint64, error) {
	var lastIndex uint64
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions //DefaultIteratorOptions: 获取默认的迭代器配置
		opts.Prefix = prefixLogEntry
		opts.Reverse = true //反向迭代

		it := txn.NewIterator(opts)
		defer it.Close()

		it.Seek(append(prefixLogEntry, 0xFF)) //从eek() 定位到指定位置开始迭代 append(prefixLogEntry, 0xFF): 构造一个理论上最大的键(0xFF是字节最大值)
		if it.Valid() {
			key := it.Item().Key()
			lastIndex = binary.BigEndian.Uint64(key[len(prefixLogEntry):])
			return nil
		}

		return ErrKeyNotFound
	})

	if err == ErrKeyNotFound {
		// 如果没有日志，尝试从快照获取
		snapshot, err := s.Snapshot()
		if err != nil && err != ErrKeyNotFound {
			return 0, err
		}
		if !IsEmptySnap(snapshot) {
			return snapshot.Metadata.Index, nil
		}
		return 0, nil
	}

	return lastIndex, err
}

// FirstIndex 获取第一条日志的索引
func (s *BadgerStorage) FirstIndex() (uint64, error) {
	// 首先尝试从快照获取
	snapshot, err := s.Snapshot()
	if err != nil && err != ErrKeyNotFound {
		return 0, err
	}
	if !IsEmptySnap(snapshot) {
		return snapshot.Metadata.Index + 1, nil //第一条可用日志是快照最后索引的下一条
	}

	// 如果没有快照，从日志中获取
	var firstIndex uint64
	err = s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefixLogEntry //Prefix 设置为日志键前缀

		it := txn.NewIterator(opts) //迭代器
		defer it.Close()

		it.Seek(prefixLogEntry)
		if it.Valid() { //Valid() 检查是否存在数据
			key := it.Item().Key()
			firstIndex = binary.BigEndian.Uint64(key[len(prefixLogEntry):])
			return nil
		}

		return ErrKeyNotFound
	})
	//如果既无快照也无日志，说明是全新存储，返回 1（Raft 日志从索引1开始）
	if err == ErrKeyNotFound {
		return 1, nil // 初始状态第一条日志索引为1
	}

	return firstIndex, err
}

// Snapshot 获取最新的快照
func (s *BadgerStorage) Snapshot() (raftpb.Snapshot, error) {
	var snapshot raftpb.Snapshot
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(keySnapshot)
		if err == badger.ErrKeyNotFound {
			// 日志记录：没有找到快照
			fmt.Printf("Info: No snapshot found in storage\n")
			return ErrKeyNotFound
		}
		if err != nil {
			// 记录其他错误
			fmt.Printf("Error retrieving snapshot: %v\n", err)
			return err
		}

		return item.Value(func(val []byte) error {
			if len(val) == 0 {
				// 记录空快照数据的情况，提供更详细的日志
				fmt.Printf("Warning: Snapshot key exists but value is empty. " +
					"This may indicate an incomplete or corrupted snapshot.\n")
				return ErrKeyNotFound
			}
			return snapshot.Unmarshal(val)
		})
	})

	// 处理不同的错误场景
	if err == ErrKeyNotFound {
		return raftpb.Snapshot{}, nil // 返回空快照而不是错误
	}
	if err != nil {
		return raftpb.Snapshot{}, err
	}

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

	batch := s.db.NewWriteBatch()
	defer batch.Cancel()

	for _, entry := range entries {
		key := logKey(entry.Index)
		data, err := entry.Marshal()
		if err != nil {
			return err
		}
		if err := batch.Set(key, data); err != nil {
			return err
		}
	}

	return batch.Flush()
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

	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(keySnapshot, data)
	})
}

// CompactLog 压缩日志，删除指定索引之前的日志
func (s *BadgerStorage) CompactLog(compactIndex uint64) error {
	firstIndex, err := s.FirstIndex()
	if err != nil {
		return err
	}
	if compactIndex < firstIndex {
		return nil // 已经压缩过
	}

	lastIndex, err := s.LastIndex()
	if err != nil {
		return err
	}
	if compactIndex > lastIndex {
		return fmt.Errorf("compact index %d is out of bound lastindex(%d)", compactIndex, lastIndex)
	}

	// 获取压缩点的日志条目
	var entry raftpb.Entry
	err = s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(logKey(compactIndex))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return entry.Unmarshal(val)
		})
	})
	if err != nil {
		return err
	}

	// 获取当前配置状态
	confState, err := s.loadConfState()
	if err != nil && err != ErrKeyNotFound {
		return err
	}

	// TODO: 这里需要从状态机获取完整的状态数据
	// 注意：这需要状态机实现 GetState() 方法来获取序列化的状态
	// 当前使用空数据作为占位符
	stateData := []byte{}

	// 创建快照
	snapshot := raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index:     compactIndex,
			Term:      entry.Term,
			ConfState: confState,
		},
		Data: stateData, // 应该使用状态机的完整序列化状态
	}

	// 保存快照
	err = s.SaveSnapshot(snapshot)
	if err != nil {
		return err
	}

	// 删除旧的日志条目
	batch := s.db.NewWriteBatch()
	defer batch.Cancel()

	err = s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefixLogEntry
		opts.PrefetchValues = false

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefixLogEntry); it.Valid(); it.Next() {
			key := it.Item().Key()
			index := binary.BigEndian.Uint64(key[len(prefixLogEntry):])
			if index < compactIndex {
				if err := batch.Delete(key); err != nil {
					return err
				}
			}
		}
		return nil
	})

	if err != nil {
		return err
	}

	return batch.Flush()
}

// Append 原子性保存硬状态、配置状态和日志条目
func (s *BadgerStorage) Append(hs *raftpb.HardState, cs *raftpb.ConfState, entries []raftpb.Entry) error {
	return s.db.Update(func(txn *badger.Txn) error {
		// 保存硬状态
		if hs != nil && !IsEmptyHardState(*hs) {
			hsData, err := hs.Marshal()
			if err != nil {
				return err
			}
			if err := txn.Set(keyHardState, hsData); err != nil {
				return err
			}
		}

		// 保存配置状态
		if cs != nil && !IsEmptyConfState(*cs) {
			csData, err := cs.Marshal()
			if err != nil {
				return err
			}
			if err := txn.Set(keyConfState, csData); err != nil {
				return err
			}
		}

		// 保存日志条目
		for _, entry := range entries {
			key := logKey(entry.Index)
			data, err := entry.Marshal()
			if err != nil {
				return err
			}
			if err := txn.Set(key, data); err != nil {
				return err
			}
		}

		return nil
	})
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
