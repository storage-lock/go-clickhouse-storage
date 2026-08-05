package clickhouse_storage

import (
	"context"
	"sync"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/storage-lock/go-storage"
)

type ClickHouseConnectionManager struct {
	Addr     string
	Database string
	Username string
	Password string

	presetConn clickhouse.Conn

	initMu      sync.Mutex
	initialized bool
	conn        clickhouse.Conn
}

var _ storage.ConnectionManager[clickhouse.Conn] = &ClickHouseConnectionManager{}

func NewClickHouseConnectionManagerFromConfig(addr, database, username, password string) *ClickHouseConnectionManager {
	return &ClickHouseConnectionManager{
		Addr:     addr,
		Database: database,
		Username: username,
		Password: password,
	}
}

func NewClickHouseConnectionManagerFromConn(conn clickhouse.Conn) *ClickHouseConnectionManager {
	return &ClickHouseConnectionManager{
		presetConn: conn,
		initialized: true,
		conn:       conn,
	}
}

func (x *ClickHouseConnectionManager) Name() string {
	return "clickhouse-connection-manager"
}

func (x *ClickHouseConnectionManager) Take(ctx context.Context) (clickhouse.Conn, error) {
	x.initMu.Lock()
	defer x.initMu.Unlock()

	if x.initialized {
		return x.conn, nil
	}

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{x.Addr},
		Auth: clickhouse.Auth{
			Database: x.Database,
			Username: x.Username,
			Password: x.Password,
		},
	})
	if err != nil {
		return nil, err
	}

	x.conn = conn
	x.initialized = true
	return x.conn, nil
}

func (x *ClickHouseConnectionManager) Return(ctx context.Context, connection clickhouse.Conn) error {
	return nil
}

func (x *ClickHouseConnectionManager) Shutdown(ctx context.Context) error {
	x.initMu.Lock()
	defer x.initMu.Unlock()
	if x.conn != nil {
		x.conn.Close()
	}
	x.conn = nil
	x.initialized = false
	return nil
}
