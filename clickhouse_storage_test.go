package clickhouse_storage

import (
	"context"
	"os"
	"testing"

	storage_test_helper "github.com/storage-lock/go-storage-test-helper"
	"github.com/stretchr/testify/assert"
)

func TestClickHouseStorage(t *testing.T) {
	envName := "STORAGE_LOCK_CLICKHOUSE_ADDR"
	addr := os.Getenv(envName)
	if addr == "" {
		t.Skip("STORAGE_LOCK_CLICKHOUSE_ADDR not set, skipping ClickHouse test")
	}

	database := os.Getenv("STORAGE_LOCK_CLICKHOUSE_DATABASE")
	if database == "" {
		database = "default"
	}
	username := os.Getenv("STORAGE_LOCK_CLICKHOUSE_USERNAME")
	password := os.Getenv("STORAGE_LOCK_CLICKHOUSE_PASSWORD")

	ctx := context.Background()
	cm := NewClickHouseConnectionManagerFromConfig(addr, database, username, password)
	s, err := NewClickHouseStorage(ctx, &ClickHouseStorageOptions{
		ConnectionManager: cm,
		TableName:         storage_test_helper.TestTableName,
		TimeProvider:      &LocalTimeProvider{},
	})
	assert.Nil(t, err)
	storage_test_helper.TestStorage(t, s)
}