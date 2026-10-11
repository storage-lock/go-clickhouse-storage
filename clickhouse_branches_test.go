package clickhouse_storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
	"github.com/stretchr/testify/assert"
)

type fakeRow struct {
	err   error
	value string
}

func (r fakeRow) Err() error { return r.err }
func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) > 0 {
		if s, ok := dest[0].(*string); ok {
			*s = r.value
		}
	}
	return nil
}
func (r fakeRow) ScanStruct(dest any) error { return r.err }

type fakeRows struct {
	values []string
	index  int
	err    error
}

func (r *fakeRows) Next() bool {
	if r.index >= len(r.values) {
		return false
	}
	r.index++
	return true
}
func (r *fakeRows) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	*(dest[0].(*string)) = r.values[r.index-1]
	return nil
}
func (r *fakeRows) ScanStruct(any) error             { return nil }
func (r *fakeRows) ColumnTypes() []driver.ColumnType { return nil }
func (r *fakeRows) Totals(...any) error              { return nil }
func (r *fakeRows) Columns() []string                { return nil }
func (r *fakeRows) Close() error                     { return nil }
func (r *fakeRows) Err() error                       { return nil }

type fakeConn struct {
	execErr  error
	queryErr error
	rowErr   error
	rowValue string
	rows     *fakeRows
	closed   bool
}

func (c *fakeConn) Contributors() []string { return nil }
func (c *fakeConn) ServerVersion() (*driver.ServerVersion, error) {
	return &proto.ServerHandshake{}, nil
}
func (c *fakeConn) Select(context.Context, any, string, ...any) error { return nil }
func (c *fakeConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	if c.queryErr != nil {
		return nil, c.queryErr
	}
	return c.rows, nil
}
func (c *fakeConn) QueryRow(context.Context, string, ...any) driver.Row {
	return fakeRow{err: c.rowErr, value: c.rowValue}
}
func (c *fakeConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	return nil, errors.New("unsupported")
}
func (c *fakeConn) Exec(context.Context, string, ...any) error              { return c.execErr }
func (c *fakeConn) AsyncInsert(context.Context, string, bool, ...any) error { return nil }
func (c *fakeConn) Ping(context.Context) error                              { return nil }
func (c *fakeConn) Stats() driver.Stats                                     { return driver.Stats{} }
func (c *fakeConn) Close() error {
	return nil
}

type takeErrorManager struct{}

func (takeErrorManager) Name() string { return "take-error" }
func (takeErrorManager) Take(context.Context) (clickhouse.Conn, error) {
	return nil, errors.New("take failed")
}
func (takeErrorManager) Return(context.Context, clickhouse.Conn) error { return nil }
func (takeErrorManager) Shutdown(context.Context) error                { return nil }

func TestClickHouseConnectionManager(t *testing.T) {
	preset := &fakeConn{}
	fromConn := NewClickHouseConnectionManagerFromConn(preset)
	assert.Equal(t, "clickhouse-connection-manager", fromConn.Name())
	got, err := fromConn.Take(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, preset, got)
	assert.NoError(t, fromConn.Return(context.Background(), got))
	assert.NoError(t, fromConn.Shutdown(context.Background()))

	orig := clickHouseOpen
	defer func() { clickHouseOpen = orig }()
	clickHouseOpen = func(*clickhouse.Options) (clickhouse.Conn, error) {
		return nil, errors.New("open failed")
	}
	fromConfig := NewClickHouseConnectionManagerFromConfig("127.0.0.1:1", "db", "u", "p")
	_, err = fromConfig.Take(context.Background())
	assert.EqualError(t, err, "open failed")

	opened := &fakeConn{}
	clickHouseOpen = func(*clickhouse.Options) (clickhouse.Conn, error) { return opened, nil }
	got, err = fromConfig.Take(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, opened, got)
	again, err := fromConfig.Take(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, got, again)
	assert.NoError(t, fromConfig.Shutdown(context.Background()))
	_, err = fromConfig.Take(context.Background())
	assert.NoError(t, err)
}

func TestClickHouseStorage_Branches(t *testing.T) {
	assert.Panics(t, func() { _, _ = NewClickHouseStorage(context.Background(), nil) })
	_, err := NewClickHouseStorage(context.Background(), NewClickHouseStorageOptions())
	assert.ErrorIs(t, err, ErrConnectionManagerNil)
	opts := NewClickHouseStorageOptions().SetConnectionManager(takeErrorManager{}).SetTableName("")
	assert.ErrorIs(t, opts.Check(), ErrTableNameEmpty)

	_, err = NewClickHouseStorage(context.Background(), &ClickHouseStorageOptions{ConnectionManager: takeErrorManager{}, TableName: "locks"})
	assert.EqualError(t, err, "take failed")

	execFailed := &fakeConn{execErr: errors.New("create failed")}
	_, err = NewClickHouseStorage(context.Background(), &ClickHouseStorageOptions{
		ConnectionManager: NewClickHouseConnectionManagerFromConn(execFailed),
		TableName:         "locks",
	})
	assert.EqualError(t, err, "create failed")

	conn := &fakeConn{}
	s, err := NewClickHouseStorage(context.Background(), NewClickHouseStorageOptions().
		SetConnectionManager(NewClickHouseConnectionManagerFromConn(conn)).
		SetTableName("locks").
		SetTimeProvider(&LocalTimeProvider{}))
	assert.NoError(t, err)
	assert.Equal(t, ClickHouseStorageName, s.GetName())
	assert.Len(t, s.Capabilities(), 2)
	assert.ErrorIs(t, s.CreateWithVersion(context.Background(), "lock", 1, &storage.LockInformation{}), storage_lock.ErrStorageCapabilityMissing)
	assert.ErrorIs(t, s.UpdateWithVersion(context.Background(), "lock", 1, 2, &storage.LockInformation{}), storage_lock.ErrStorageCapabilityMissing)
	assert.ErrorIs(t, s.DeleteWithVersion(context.Background(), "lock", 1, &storage.LockInformation{}), storage_lock.ErrStorageCapabilityMissing)
	now, err := s.GetTime(context.Background())
	assert.NoError(t, err)
	assert.False(t, now.IsZero())
	conn.rowErr = errors.New("missing")
	_, err = s.Get(context.Background(), "missing")
	assert.ErrorIs(t, err, storage_lock.ErrLockNotFound)
	conn.rowErr = nil
	conn.rowValue = `{"owner_id":"owner"}`
	got, err := s.Get(context.Background(), "lock")
	assert.NoError(t, err)
	assert.Contains(t, got, "owner")

	info := &storage.LockInformation{OwnerId: "owner"}
	conn.rows = &fakeRows{values: []string{info.ToJsonString(), "bad-json", "scan"}}
	conn.rows.err = nil
	listed, err := s.List(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, listed)

	conn.rows = &fakeRows{values: []string{"ignored"}, err: errors.New("scan failed")}
	listed, err = s.List(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, listed)

	conn.queryErr = errors.New("query failed")
	_, err = s.List(context.Background())
	assert.EqualError(t, err, "query failed")

	defaultTime, err := NewClickHouseStorage(context.Background(), &ClickHouseStorageOptions{
		ConnectionManager: NewClickHouseConnectionManagerFromConn(&fakeConn{}),
		TableName:         "locks",
	})
	assert.NoError(t, err)
	now, err = defaultTime.GetTime(context.Background())
	assert.NoError(t, err)
	assert.False(t, now.IsZero())
	assert.NoError(t, s.Close(context.Background()))
	_, err = (&ClickHouseStorage{}).Watch(context.Background(), "")
	assert.Error(t, err)
	_ = column.Error{}
	_ = time.Second
}
