package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"

	"github.com/bytehouse-cloud/driver-go"
)

// GatewayDriver implements the sql Driver interface
// See https://golang.org/pkg/database/sql/driver/#Driver
type GatewayDriver struct{}

func init() {
	sql.Register("bytehouse", &GatewayDriver{})
}

// Open returns a new connection to the database.
// The name is a string in a driver-specific format.
//
// OpenConfig may return a cached connection (one previously
// closed), but doing so is unnecessary; the sql package
// maintains a pool of idle connections for efficient re-use.
//
// The returned connection is only used by one goroutine at a
// time.
func (d GatewayDriver) Open(dsn string) (driver.Conn, error) {
	c, err := NewConnector(bytehouse.EmptyConnectionContext, dsn)
	if err != nil {
		return nil, err
	}

	return c.Connect(context.Background())
}

// OpenConnector implements driver.DriverContext.
// sql.DB will call OpenConnector to obtain a Connector and then invoke
// that Connector's Connect method to obtain each needed connection,
// instead of invoking the Driver's OpenConfig method for each connection.
// The two-step sequence allows drivers to parse the name just once
// and also provides access to per-Conn contexts.
func (d GatewayDriver) OpenConnector(dsn string) (driver.Connector, error) {
	return NewConnector(bytehouse.EmptyConnectionContext, dsn)
}
