package clickhouse_storage

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
	storage_test_helper "github.com/storage-lock/go-storage-test-helper"
)

func TestClickHouseStorage(t *testing.T) {
	addr := os.Getenv("STORAGE_LOCK_CLICKHOUSE_ADDR")
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
	// 构造失败必须立刻中止：后面的断言都要调用存储方法，nil 存储只会空指针崩溃。
	if err != nil {
		t.Fatalf("NewClickHouseStorage: %v", err)
	}

	// ClickHouse 没有原子条件写，三个写方法固定返回能力缺失。
	// 通用契约测试 storage_test_helper.TestStorage 把"写入成功"当成通过条件，
	// 对这个实现必然失败，所以这里按能力分别验证，不跑那一套。
	storage_test_helper.TestStorage_GetName(t, s)
	storage_test_helper.TestStorage_Capabilities(t, s)
	storage_test_helper.TestStorage_Init(t, s)
	storage_test_helper.TestStorage_GetTime(t, s)

	info := storage_test_helper.BuildTestLockInformation(t)
	for _, call := range []struct {
		name string
		fn   func() error
	}{
		{"CreateWithVersion", func() error { return s.CreateWithVersion(ctx, storage_test_helper.TestLockId, info.Version, info) }},
		{"UpdateWithVersion", func() error {
			return s.UpdateWithVersion(ctx, storage_test_helper.TestLockId, info.Version, info.Version+1, info)
		}},
		{"DeleteWithVersion", func() error { return s.DeleteWithVersion(ctx, storage_test_helper.TestLockId, info.Version, info) }},
	} {
		if err := call.fn(); !errors.Is(err, storage_lock.ErrStorageCapabilityMissing) {
			t.Errorf("%s = %v, want ErrStorageCapabilityMissing", call.name, err)
		}
	}

	// 读路径是真实的：不存在的锁返回 ErrLockNotFound，列出当前记录不报错。
	if _, err := s.Get(ctx, "clickhouse-lock-does-not-exist"); !errors.Is(err, storage_lock.ErrLockNotFound) {
		t.Errorf("Get(missing) = %v, want ErrLockNotFound", err)
	}
	it, err := s.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	for it.Next() {
	}

	// 能力声明本身也要钉死，避免以后有人补上一个假的 CAS。
	if storage.HasCapability(s, storage.CapabilityCAS) {
		t.Error("ClickHouse must not declare CapabilityCAS: it has no atomic conditional write")
	}

	// Close 会丢弃连接，必须放在所有读写断言之后。
	storage_test_helper.TestStorage_Close(t, s)
}
