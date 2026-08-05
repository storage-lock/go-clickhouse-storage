package clickhouse_storage

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/storage-lock/go-storage"
)

type ClickHouseStorageOptions struct {
	ConnectionManager storage.ConnectionManager[clickhouse.Conn]
	TableName         string
	TimeProvider      storage.TimeProvider
}

func NewClickHouseStorageOptions() *ClickHouseStorageOptions {
	return &ClickHouseStorageOptions{
		TableName: storage.DefaultStorageTableName,
	}
}

func (x *ClickHouseStorageOptions) SetConnectionManager(cm storage.ConnectionManager[clickhouse.Conn]) *ClickHouseStorageOptions {
	x.ConnectionManager = cm
	return x
}

func (x *ClickHouseStorageOptions) SetTableName(tableName string) *ClickHouseStorageOptions {
	x.TableName = tableName
	return x
}

func (x *ClickHouseStorageOptions) SetTimeProvider(tp storage.TimeProvider) *ClickHouseStorageOptions {
	x.TimeProvider = tp
	return x
}

func (x *ClickHouseStorageOptions) Check() error {
	if x.TableName == "" {
		return ErrTableNameEmpty
	}
	if x.ConnectionManager == nil {
		return ErrConnectionManagerNil
	}
	return nil
}
