package sql

import (
	"context"
	"database/sql/driver"

	"github.com/bytehouse-cloud/driver-go"
	"github.com/bytehouse-cloud/driver-go/sdk"
)

// NewConnector returns new driver.Connector.
func NewConnector(connContext *bytehouse.ConnectionContext, dsn string) (driver.Connector, error) {
	return &connector{
		dsn:         dsn,
		connContext: connContext,
	}, nil
}

// connector implements Connector interface from database/sql library
// See https://golang.org/pkg/database/sql/driver/#Connector
type connector struct {
	dsn         string
	connContext *bytehouse.ConnectionContext
}

// Connect returns a connection to the database.
// Connect may return a cached connection (one previously
// closed), but doing so is unnecessary; the sql package
// maintains a pool of idle connections for efficient re-use.
//
// The provided context.Context is for dialing purposes only
// (see net.DialContext) and should not be stored or used for
// other purposes. A default timeout should still be used
// when dialing as a connection pool may call Connect
// asynchronously to any query.
//
// The returned connection is only used by one goroutine at a
// time.
func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	if bhConnCtx, ok := ctx.(*bytehouse.ConnectionContext); ok {
		c.connContext = bhConnCtx
	} else {
		c.connContext.Context = ctx
	}

	gateway, err := sdk.Open(c.connContext, c.dsn)
	if err != nil {
		return nil, err
	}

	cn := &CHConn{
		Gateway: gateway,
	}
	return cn, nil
}

// Driver returns the underlying Driver of the Connector,
// mainly to maintain compatibility with the Driver method
// on sql.DB.
func (c *connector) Driver() driver.Driver {
	return &GatewayDriver{}
}
