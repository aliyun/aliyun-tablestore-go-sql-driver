package tablestore

import (
	"database/sql"
	"database/sql/driver"
	"errors"
)

var (
	ErrPlaceholderCount = errors.New("tablestore: wrong placeholder count")
	ErrNotSupported     = errors.New("tablestore: operation is not supported")
)

const (
	RetryTimes         = "retryTimes"
	MaxRetryTime       = "maxRetryTime"
	ConnectionTimeout  = "connectionTimeout"
	RequestTimeout     = "requestTimeout"
	MaxIdleConnections = "maxIdleConnections"
)

func init() {
	sql.Register("ots", new(tableStoreDriver))
}

type tableStoreDriver struct {
}

// Open returns a new connection to the database.
// The name is a string in a driver-specific format.
func (ots *tableStoreDriver) Open(dsn string) (driver.Conn, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return newConn(cfg), nil
}
