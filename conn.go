package tablestore

import (
	"database/sql/driver"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
)

type conn struct {
	client *tablestore.TableStoreClient
}

func newConn(cfg *connectionConfig) *conn {
	return &conn{
		client: tablestore.NewClientWithConfig(cfg.endPoint, cfg.instanceName, cfg.accessKeyId, cfg.accessKeySecret, "", cfg.otsConfig),
	}
}

// Prepare returns a prepared statement, bound to this connection.
func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return newStmt(c.client, query), nil
}

// Close invalidates and potentially stops any current
// prepared statements and transactions, marking this
// connection as no longer in use.
func (c *conn) Close() error {
	return nil
}

// Begin starts and returns a new transaction.
func (c *conn) Begin() (driver.Tx, error) {
	return nil, ErrNotSupported
}

// Exec implements the driver.Execer
func (c *conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	stmt, err := c.Prepare(query)
	if err != nil {
		return nil, err
	}
	return stmt.Exec(args)
}

// Query implements the driver.Queryer
func (c *conn) Query(query string, args []driver.Value) (driver.Rows, error) {
	stmt, err := c.Prepare(query)
	if err != nil {
		return nil, err
	}
	return stmt.Query(args)
}
