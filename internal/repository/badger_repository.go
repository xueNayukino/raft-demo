package repository

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// 键前缀定义
var (
	prefixLogEntry = []byte("log/")
	keyHardState   = []byte("hardstate")
	keyConfState   = []byte("confstate")
	keySnapshot    = []byte("snapshot")
	keyFirstIndex  = []byte("firstindex")
	prefixKV       = []byte("kv/")
	prefixTxn      = []byte("txn/")
)

// BadgerRepository BadgerDB仓库实现
type BadgerRepository struct {
	db     *badger.DB
	mu     sync.RWMutex
	closed bool
}

// NewBadgerRepository 创建BadgerDB仓库
func NewBadgerRepository(dataDir string) (*BadgerRepository, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("创建目录失败: %v", err)
	}

	opts := badger.DefaultOptions(dataDir)
	opts.Logger = nil
	opts.DetectConflicts = false
	opts.NumVersionsToKeep = 1
	opts.SyncWrites = false

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("打开BadgerDB失败: %v", err)
	}

	return &BadgerRepository{
		db: db,
	}, nil
}

// RaftLogRepository 实现

// GetEntry 获取日志条目
func (r *BadgerRepository) GetEntry(index uint64) (raftpb.Entry, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.closed {
		return raftpb.Entry{}, errors.New("repository已关闭")
	}

	key := r.logKey(index)
	var entry raftpb.Entry

	err := r.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			return entry.Unmarshal(val)
		})
	})

	if err == badger.ErrKeyNotFound {
		return raftpb.Entry{}, raft.ErrUnavailable
	}

	return entry, err
}

// GetEntries 获取日志条目范围
func (r *BadgerRepository) GetEntries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.closed {
		return nil, errors.New("repository已关闭")
	}

	var entries []raftpb.Entry
	var totalSize uint64

	err := r.db.View(func(txn *badger.Txn) error {
		for i := lo; i < hi; i++ {
			key := r.logKey(i)
			item, err := txn.Get(key)
			if err == badger.ErrKeyNotFound {
				continue
			}
			if err != nil {
				return err
			}

			var entry raftpb.Entry
			err = item.Value(func(val []byte) error {
				if maxSize > 0 && totalSize+uint64(len(val)) > maxSize {
					return nil // 达到大小限制
				}
				totalSize += uint64(len(val))
				return entry.Unmarshal(val)
			})
			if err != nil {
				return err
			}

			entries = append(entries, entry)
			if maxSize > 0 && totalSize >= maxSize {
				break
			}
		}
		return nil
	})

	return entries, err
}

// StoreEntries 存储日志条目
func (r *BadgerRepository) StoreEntries(entries []raftpb.Entry) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return errors.New("repository已关闭")
	}

	return r.db.Update(func(txn *badger.Txn) error {
		for _, entry := range entries {
			key := r.logKey(entry.Index)
			val, err := entry.Marshal()
			if err != nil {
				return err
			}
			if err := txn.Set(key, val); err != nil {
				return err
			}
		}
		return nil
	})
}

// FirstIndex 获取第一个日志索引
func (r *BadgerRepository) FirstIndex() (uint64, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.closed {
		return 0, errors.New("repository已关闭")
	}

	var firstIndex uint64 = 1
	err := r.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(keyFirstIndex)
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			if len(val) == 8 {
				firstIndex = binary.BigEndian.Uint64(val)
			}
			return nil
		})
	})

	return firstIndex, err
}

// LastIndex 获取最后一个日志索引
func (r *BadgerRepository) LastIndex() (uint64, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.closed {
		return 0, errors.New("repository已关闭")
	}

	var lastIndex uint64
	err := r.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Reverse = true
		opts.Prefix = prefixLogEntry
		it := txn.NewIterator(opts)
		defer it.Close()

		if it.Rewind(); it.Valid() {
			key := it.Item().Key()
			if len(key) >= len(prefixLogEntry)+8 {
				lastIndex = binary.BigEndian.Uint64(key[len(prefixLogEntry):])
			}
		}
		return nil
	})

	return lastIndex, err
}

// Term 获取指定索引的任期
func (r *BadgerRepository) Term(index uint64) (uint64, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.closed {
		return 0, errors.New("repository已关闭")
	}

	// Handle the dummy entry case: if index is 0, return term 0
	if index == 0 {
		return 0, nil
	}

	var term uint64
	err := r.db.View(func(txn *badger.Txn) error {
		key := r.logKey(index)
		item, err := txn.Get(key)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return ErrEntryNotFound
			}
			return err
		}

		return item.Value(func(val []byte) error {
			var entry raftpb.Entry
			if err := entry.Unmarshal(val); err != nil {
				return err
			}
			term = entry.Term
			return nil
		})
	})

	return term, err
}

// Compact 压缩日志到指定索引
func (r *BadgerRepository) Compact(compactIndex uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return errors.New("repository已关闭")
	}

	// 删除compactIndex之前的日志
	err := r.db.Update(func(txn *badger.Txn) error {
		for i := uint64(1); i < compactIndex; i++ {
			key := r.logKey(i)
			if err := txn.Delete(key); err != nil && err != badger.ErrKeyNotFound {
				return err
			}
		}

		// 更新firstIndex
		val := make([]byte, 8)
		binary.BigEndian.PutUint64(val, compactIndex)
		return txn.Set(keyFirstIndex, val)
	})

	return err
}

// RaftStateRepository 实现

// GetHardState 获取硬状态
func (r *BadgerRepository) GetHardState() (raftpb.HardState, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.closed {
		return raftpb.HardState{}, errors.New("repository已关闭")
	}

	var hs raftpb.HardState
	err := r.db.View(func(txn *badger.Txn) error {
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

	return hs, err
}

// StoreHardState 存储硬状态
func (r *BadgerRepository) StoreHardState(st raftpb.HardState) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return errors.New("repository已关闭")
	}

	val, err := st.Marshal()
	if err != nil {
		return err
	}

	return r.db.Update(func(txn *badger.Txn) error {
		return txn.Set(keyHardState, val)
	})
}

// GetConfState 获取配置状态
func (r *BadgerRepository) GetConfState() (raftpb.ConfState, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.closed {
		return raftpb.ConfState{}, errors.New("repository已关闭")
	}

	var cs raftpb.ConfState
	err := r.db.View(func(txn *badger.Txn) error {
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

	return cs, err
}

// StoreConfState 存储配置状态
func (r *BadgerRepository) StoreConfState(cs raftpb.ConfState) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return errors.New("repository已关闭")
	}

	val, err := cs.Marshal()
	if err != nil {
		return err
	}

	return r.db.Update(func(txn *badger.Txn) error {
		return txn.Set(keyConfState, val)
	})
}

// SnapshotRepository 实现

// GetSnapshot 获取快照
func (r *BadgerRepository) GetSnapshot() (raftpb.Snapshot, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.closed {
		return raftpb.Snapshot{}, errors.New("repository已关闭")
	}

	var snapshot raftpb.Snapshot
	err := r.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(keySnapshot)
		if err == badger.ErrKeyNotFound {
			return raft.ErrSnapshotTemporarilyUnavailable
		}
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			return snapshot.Unmarshal(val)
		})
	})

	return snapshot, err
}

// StoreSnapshot 存储快照
func (r *BadgerRepository) StoreSnapshot(snapshot raftpb.Snapshot) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return errors.New("repository已关闭")
	}

	val, err := snapshot.Marshal()
	if err != nil {
		return err
	}

	return r.db.Update(func(txn *badger.Txn) error {
		return txn.Set(keySnapshot, val)
	})
}

// ApplySnapshot 应用快照
func (r *BadgerRepository) ApplySnapshot(snapshot raftpb.Snapshot) error {
	// 解析快照数据
	var data map[string]string
	if err := json.Unmarshal(snapshot.Data, &data); err != nil {
		return fmt.Errorf("解析快照数据失败: %v", err)
	}

	// 恢复数据
	return r.RestoreFromSnapshot(context.Background(), data)
}

// CreateSnapshot 创建快照
func (r *BadgerRepository) CreateSnapshot(ctx context.Context) (raftpb.Snapshot, error) {
	// 获取所有KV数据
	data, err := r.GetAll(ctx)
	if err != nil {
		return raftpb.Snapshot{}, err
	}

	// 序列化数据
	snapshotData, err := json.Marshal(data)
	if err != nil {
		return raftpb.Snapshot{}, err
	}

	// 创建快照
	snapshot := raftpb.Snapshot{
		Data: snapshotData,
		Metadata: raftpb.SnapshotMetadata{
			Index: 0, // 需要从外部设置
			Term:  0, // 需要从外部设置
		},
	}

	return snapshot, nil
}

// KVRepository 实现

// Get 获取值
func (r *BadgerRepository) Get(ctx context.Context, key string) (string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.closed {
		return "", errors.New("repository已关闭")
	}

	var value string
	err := r.db.View(func(txn *badger.Txn) error {
		kvKey := r.kvKey(key)
		item, err := txn.Get(kvKey)
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			value = string(val)
			return nil
		})
	})

	if err == badger.ErrKeyNotFound {
		return "", errors.New("key not found")
	}

	return value, err
}

// Put 设置值
func (r *BadgerRepository) Put(ctx context.Context, key, value string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return errors.New("repository已关闭")
	}

	return r.db.Update(func(txn *badger.Txn) error {
		kvKey := r.kvKey(key)
		return txn.Set(kvKey, []byte(value))
	})
}

// Delete 删除键
func (r *BadgerRepository) Delete(ctx context.Context, key string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return errors.New("repository已关闭")
	}

	return r.db.Update(func(txn *badger.Txn) error {
		kvKey := r.kvKey(key)
		return txn.Delete(kvKey)
	})
}

// Batch 批量操作
func (r *BadgerRepository) Batch(ctx context.Context, ops []KVOperation) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return errors.New("repository已关闭")
	}

	return r.db.Update(func(txn *badger.Txn) error {
		for _, op := range ops {
			kvKey := r.kvKey(op.Key)
			switch op.Type {
			case "put":
				if err := txn.Set(kvKey, []byte(op.Value)); err != nil {
					return err
				}
			case "delete":
				if err := txn.Delete(kvKey); err != nil {
					return err
				}
			default:
				return fmt.Errorf("不支持的操作类型: %s", op.Type)
			}
		}
		return nil
	})
}

// GetAll 获取所有键值对
func (r *BadgerRepository) GetAll(ctx context.Context) (map[string]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.closed {
		return nil, errors.New("repository已关闭")
	}

	result := make(map[string]string)
	err := r.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefixKV
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.Key()[len(prefixKV):])

			err := item.Value(func(val []byte) error {
				result[key] = string(val)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	return result, err
}

// RestoreFromSnapshot 从快照恢复数据
func (r *BadgerRepository) RestoreFromSnapshot(ctx context.Context, data map[string]string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return errors.New("repository已关闭")
	}

	return r.db.Update(func(txn *badger.Txn) error {
		// 清除现有KV数据
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefixKV
		it := txn.NewIterator(opts)
		defer it.Close()

		var keysToDelete [][]byte
		for it.Rewind(); it.Valid(); it.Next() {
			keysToDelete = append(keysToDelete, it.Item().KeyCopy(nil))
		}

		for _, key := range keysToDelete {
			if err := txn.Delete(key); err != nil {
				return err
			}
		}

		// 恢复数据
		for key, value := range data {
			kvKey := r.kvKey(key)
			if err := txn.Set(kvKey, []byte(value)); err != nil {
				return err
			}
		}

		return nil
	})
}

// TransactionRepository 实现

// BeginTransaction 开始事务
func (r *BadgerRepository) BeginTransaction(ctx context.Context, txnID uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return errors.New("repository已关闭")
	}

	txnData := map[string]interface{}{
		"id":     txnID,
		"state":  "prepare",
		"ops":    []KVOperation{},
		"created": time.Now(),
	}

	data, err := json.Marshal(txnData)
	if err != nil {
		return err
	}

	return r.db.Update(func(txn *badger.Txn) error {
		key := r.txnKey(txnID)
		return txn.Set(key, data)
	})
}

// CommitTransaction 提交事务
func (r *BadgerRepository) CommitTransaction(ctx context.Context, txnID uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return errors.New("repository已关闭")
	}

	return r.db.Update(func(txn *badger.Txn) error {
		key := r.txnKey(txnID)
		item, err := txn.Get(key)
		if err != nil {
			return err
		}

		var txnData map[string]interface{}
		err = item.Value(func(val []byte) error {
			return json.Unmarshal(val, &txnData)
		})
		if err != nil {
			return err
		}

		txnData["state"] = "commit"
		data, err := json.Marshal(txnData)
		if err != nil {
			return err
		}

		return txn.Set(key, data)
	})
}

// RollbackTransaction 回滚事务
func (r *BadgerRepository) RollbackTransaction(ctx context.Context, txnID uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return errors.New("repository已关闭")
	}

	return r.db.Update(func(txn *badger.Txn) error {
		key := r.txnKey(txnID)
		return txn.Delete(key)
	})
}

// GetTransactionState 获取事务状态
func (r *BadgerRepository) GetTransactionState(ctx context.Context, txnID uint64) (string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.closed {
		return "", errors.New("repository已关闭")
	}

	var state string
	err := r.db.View(func(txn *badger.Txn) error {
		key := r.txnKey(txnID)
		item, err := txn.Get(key)
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			var txnData map[string]interface{}
			if err := json.Unmarshal(val, &txnData); err != nil {
				return err
			}
			if s, ok := txnData["state"].(string); ok {
				state = s
			}
			return nil
		})
	})

	if err == badger.ErrKeyNotFound {
		return "", errors.New("transaction not found")
	}

	return state, err
}

// AddTransactionOperation 添加事务操作
func (r *BadgerRepository) AddTransactionOperation(ctx context.Context, txnID uint64, op KVOperation) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return errors.New("repository已关闭")
	}

	return r.db.Update(func(txn *badger.Txn) error {
		key := r.txnKey(txnID)
		item, err := txn.Get(key)
		if err != nil {
			return err
		}

		var txnData map[string]interface{}
		err = item.Value(func(val []byte) error {
			return json.Unmarshal(val, &txnData)
		})
		if err != nil {
			return err
		}

		ops, ok := txnData["ops"].([]interface{})
		if !ok {
			ops = []interface{}{}
		}
		ops = append(ops, op)
		txnData["ops"] = ops

		data, err := json.Marshal(txnData)
		if err != nil {
			return err
		}

		return txn.Set(key, data)
	})
}

// Close 关闭仓库
func (r *BadgerRepository) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil
	}

	r.closed = true
	return r.db.Close()
}

// HealthCheck 健康检查
func (r *BadgerRepository) HealthCheck(ctx context.Context) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.closed {
		return errors.New("repository已关闭")
	}

	// 简单的读取测试
	return r.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte("health_check"))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		return err
	})
}

// 辅助方法

func (r *BadgerRepository) logKey(index uint64) []byte {
	key := make([]byte, len(prefixLogEntry)+8)
	copy(key, prefixLogEntry)
	binary.BigEndian.PutUint64(key[len(prefixLogEntry):], index)
	return key
}

func (r *BadgerRepository) kvKey(key string) []byte {
	result := make([]byte, len(prefixKV)+len(key))
	copy(result, prefixKV)
	copy(result[len(prefixKV):], key)
	return result
}

func (r *BadgerRepository) txnKey(txnID uint64) []byte {
	key := make([]byte, len(prefixTxn)+8)
	copy(key, prefixTxn)
	binary.BigEndian.PutUint64(key[len(prefixTxn):], txnID)
	return key
}