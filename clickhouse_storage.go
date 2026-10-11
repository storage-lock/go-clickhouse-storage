package clickhouse_storage

// ⚠️ NOT FOR DISTRIBUTED LOCK - ClickHouse lacks CAS capability
//
// ClickHouse's ReplacingMergeTree does NOT support:
//   - Unique constraints (concurrent INSERT creates duplicates)
//   - Atomic conditional writes (no IF NOT EXISTS / IF syntax)
//   - Strong consistency (only eventual consistency)
//
// This storage implementation is INCOMPLETE and UNSAFE for production distributed locks.
// It is preserved only as a reference implementation showing the challenges of using
// eventually-consistent storage for distributed locking.
//
// DO NOT USE for production distributed locks. Use storage engines with true CAS support:
//   - SQL databases: MySQL, PostgreSQL, Oracle, etc.
//   - NoSQL with CAS: MongoDB, Cassandra (with LWT), DynamoDB, Redis
//   - Consensus systems: etcd, Zookeeper

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	go_iterator "github.com/golang-infrastructure/go-iterator"
	"github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
)

// ClickHouseStorage 基于 ClickHouse 的存储引擎实现
// ClickHouse 支持 OPTIMIZE FINAL 实现轻量级 CAS
type ClickHouseStorage struct {
	options      *ClickHouseStorageOptions
	conn         clickhouse.Conn
	timeProvider storage.TimeProvider
}

var _ storage.Storage = &ClickHouseStorage{}

func NewClickHouseStorage(ctx context.Context, options *ClickHouseStorageOptions) (*ClickHouseStorage, error) {
	if err := options.Check(); err != nil {
		return nil, err
	}
	s := &ClickHouseStorage{options: options}
	if err := s.Init(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

const ClickHouseStorageName = "clickhouse-storage"

func (x *ClickHouseStorage) GetName() string {
	return ClickHouseStorageName
}

func (x *ClickHouseStorage) Capabilities() []storage.StorageCapability {
	return []storage.StorageCapability{
		storage.CapabilityReliableTime,
		storage.CapabilityWatch,
	}
}

func (x *ClickHouseStorage) Watch(ctx context.Context, lockId string) (<-chan *storage.LockInformation, error) {
	return storage.NewPollingLockWatcher(x, 500*time.Millisecond).Watch(ctx, lockId)
}

func (x *ClickHouseStorage) Init(ctx context.Context) error {
	conn, err := x.options.ConnectionManager.Take(ctx)
	if err != nil {
		return err
	}
	x.conn = conn

	// 创建表
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		lock_id String,
		version UInt64,
		json_data String,
		created_at DateTime
	) ENGINE = ReplacingMergeTree(version) ORDER BY lock_id`, x.options.TableName)
	if err := x.conn.Exec(ctx, query); err != nil {
		return err
	}

	if x.options.TimeProvider != nil {
		x.timeProvider = x.options.TimeProvider
	} else {
		x.timeProvider = &LocalTimeProvider{}
	}
	return nil
}

type LocalTimeProvider struct{}

func (p *LocalTimeProvider) GetTime(ctx context.Context) (time.Time, error) {
	return time.Now(), nil
}

// CreateWithVersion ClickHouse 不支持原子条件写入，无法保证分布式锁所需的互斥性
func (x *ClickHouseStorage) CreateWithVersion(ctx context.Context, lockId string, version storage.Version, lockInformation *storage.LockInformation) error {
	return storage_lock.ErrStorageCapabilityMissing
}

// UpdateWithVersion ClickHouse 不支持原子条件更新，无法保证版本一致性
func (x *ClickHouseStorage) UpdateWithVersion(ctx context.Context, lockId string, expectedVersion, newVersion storage.Version, lockInformation *storage.LockInformation) error {
	return storage_lock.ErrStorageCapabilityMissing
}

// DeleteWithVersion ClickHouse 不支持原子条件删除，无法保证分布式锁的互斥性
func (x *ClickHouseStorage) DeleteWithVersion(ctx context.Context, lockId string, expectedVersion storage.Version, lockInformation *storage.LockInformation) error {
	return storage_lock.ErrStorageCapabilityMissing
}

func (x *ClickHouseStorage) Get(ctx context.Context, lockId string) (string, error) {
	var jsonData string
	query := fmt.Sprintf(`SELECT json_data FROM %s FINAL WHERE lock_id = ?`, x.options.TableName)
	if err := x.conn.QueryRow(ctx, query, lockId).Scan(&jsonData); err != nil {
		return "", storage_lock.ErrLockNotFound
	}
	return jsonData, nil
}

func (x *ClickHouseStorage) GetTime(ctx context.Context) (time.Time, error) {
	return x.timeProvider.GetTime(ctx)
}

func (x *ClickHouseStorage) Close(ctx context.Context) error {
	x.conn = nil
	return nil
}

func (x *ClickHouseStorage) List(ctx context.Context) (go_iterator.Iterator[*storage.LockInformation], error) {
	query := fmt.Sprintf(`SELECT json_data FROM %s FINAL`, x.options.TableName)
	rows, err := x.conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	slice := make([]*storage.LockInformation, 0)
	for rows.Next() {
		var jsonData string
		if err := rows.Scan(&jsonData); err != nil {
			continue
		}
		lockInfo, err := storage.LockInformationFromJsonString(jsonData)
		if err != nil {
			continue
		}
		slice = append(slice, lockInfo)
	}

	return go_iterator.FromSlice(slice), nil
}