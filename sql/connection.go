package sql

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"reflect"
	"time"
	"unsafe"

	"github.com/google/uuid"

	connPackage "github.com/bytehouse-cloud/driver-go/conn"
	"github.com/bytehouse-cloud/driver-go/driver/lib/settings"
	driverErrors "github.com/bytehouse-cloud/driver-go/errors"
	"github.com/bytehouse-cloud/driver-go/sdk"
	"github.com/bytehouse-cloud/driver-go/utils"
)

// CHConn implements Conn interface from database/sql library
type CHConn struct {
	Gateway *sdk.Gateway
}

func (c *CHConn) Begin() (driver.Tx, error) {
	return c, nil
}

func (c *CHConn) Commit() error {
	return nil
}

func (c *CHConn) Rollback() error {
	return nil
}

// ResetSession is called prior to executing a query on the connection
// if the connection has been used before. If the driver returns ErrBadConn
// the connection is discarded.
func (c *CHConn) ResetSession(ctx context.Context) error {
	if c.Gateway.Conn.InQueryingState() {
		return driver.ErrBadConn
	}

	if err := c.Gateway.Ping(); err != nil {
		return driver.ErrBadConn
	}

	return nil
}

// Ping implements Pinger interface
// If CHConn.Ping returns ErrBadConn, DB.Ping and DB.PingContext will remove
// the CHConn from pool.
func (c *CHConn) Ping(ctx context.Context) error {
	if c.Gateway.Conn.InQueryingState() {
		return driver.ErrBadConn
	}

	return c.Gateway.Ping()
}

func (c *CHConn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *CHConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	numArgs := utils.NumArgs(query)

	if !utils.IsInsert(query) {
		return newSelectStmt(numArgs, query, c), nil
	}

	insertQuery, err := utils.ParseInsertQuery(query)
	if err != nil {
		return nil, err
	}

	insertStmt, err := c.Gateway.PrepareInsert(ctx, insertQuery.Query, settings.DEFAULT_BLOCK_SIZE) // todo: make it customizable
	if err != nil {
		return nil, err
	}

	return newInsertStmt(numArgs, insertStmt), nil
}

// Close invalidates and potentially stops any current
// prepared statements and transactions, marking this
// connection as no longer in use.
//
// Because the sql package maintains a free pool of
// connections and only calls Close when there's a surplus of
// idle connections, it shouldn't be necessary for drivers to
// do their own connection caching.
//
// Drivers must ensure all network calls made by Close
// do not block indefinitely (e.g. apply a timeout).
func (c *CHConn) Close() error {
	return c.Gateway.Close()
}

// NamedValueChecker may be optionally implemented by CHConn or Stmt. It provides
// the driver more control to handle Go and database types beyond the default
// Values types allowed.
//
// The sql package checks for value checkers in the following order,
// stopping at the first found match: Stmt.NamedValueChecker, CHConn.NamedValueChecker,
// Stmt.ColumnConverter, DefaultParameterConverter.
//
// If CheckNamedValue returns ErrRemoveArgument, the NamedValue will not be included in
// the final query arguments. This may be used to pass special options to
// the query itself.
//
// If ErrSkip is returned the column converter error checking
// path is used for the argument. Drivers may wish to return ErrSkip after
// they have exhausted their own special cases.
//
// CheckNamedValue is called before passing arguments to the driver
// and is called in place of any ColumnConverter. CheckNamedValue must do type
// validation and conversion as appropriate for the driver.
func (c *CHConn) CheckNamedValue(nv *driver.NamedValue) error {
	switch nv.Value.(type) {
	case nil, []byte, int8, int16, int32, int64, uint8, uint16, uint32, uint64, float32, float64, string, time.Time:
		return nil
	case uuid.UUID:
		return nil
	}
	switch v := nv.Value.(type) {
	case
		[]int, []int8, []int16, []int32, []int64,
		[]uint, []uint8, []uint16, []uint32, []uint64,
		[]float32, []float64,
		[]string:
		return nil
	case net.IP:
		return nil
	case driver.Valuer:
		value, err := v.Value()
		if err != nil {
			return err
		}
		nv.Value = value
	default:
		switch value := reflect.ValueOf(nv.Value); value.Kind() {
		case reflect.Slice:
			return nil
		case reflect.Bool:
			nv.Value = uint8(0)
			if value.Bool() {
				nv.Value = uint8(1)
			}
		case reflect.Int8:
			nv.Value = int8(value.Int())
		case reflect.Int16:
			nv.Value = int16(value.Int())
		case reflect.Int32:
			nv.Value = int32(value.Int())
		case reflect.Int64:
			nv.Value = value.Int()
		case reflect.Uint8:
			nv.Value = uint8(value.Uint())
		case reflect.Uint16:
			nv.Value = uint16(value.Uint())
		case reflect.Uint32:
			nv.Value = uint32(value.Uint())
		case reflect.Uint64:
			nv.Value = value.Uint()
		case reflect.Float32:
			nv.Value = float32(value.Float())
		case reflect.Float64:
			nv.Value = value.Float()
		case reflect.String:
			nv.Value = value.String()
		}
	}
	return nil
}

func (c *CHConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	return c.exec(context.Background(), query, args)
}

func (c *CHConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	dargs, err := namedArgsToArgs(args)
	if err != nil {
		return nil, err
	}

	return c.exec(ctx, query, dargs)
}

func (c *CHConn) exec(ctx context.Context, query string, args []driver.Value) (driver.Result, error) {
	if _, err := c.query(ctx, query, args); err != nil {
		return nil, err
	}

	return emptyResult, nil
}

// QueryContext runs a query and returns it's results
func (c *CHConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	dargs, err := namedArgsToArgs(args)
	if err != nil {
		return nil, err
	}

	return c.query(ctx, query, dargs)
}

// Query makes a sql query
// args are parameters in a prepared statement
func (c *CHConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	return c.query(context.Background(), query, args)
}

func (c *CHConn) query(ctx context.Context, query string, args []driver.Value) (dataRows driver.Rows, err error) {
	handleBadConn := func(connErr error) error {
		if errors.Is(&connPackage.ErrBadConnection{}, connErr) {
			return driver.ErrBadConn
		}
		return connErr
	}

	isInsertQuery := utils.IsInsert(query)

	if isInsertQuery {
		insertQuery, err := utils.ParseInsertQuery(query)
		if err != nil {
			return nil, fmt.Errorf("failed to parse insert query: %s", err)
		}

		// If is insert query with arguments
		if len(args) > 0 {
			iValues := *(*[]interface{})(unsafe.Pointer(&args))
			return emptyRows, c.Gateway.InsertArgs(ctx, insertQuery.Query,
				settings.DEFAULT_BLOCK_SIZE, iValues...,
			)
		}

		// Insert query with no arguments
		qr, err := c.Gateway.InsertWithData(
			ctx, insertQuery.Query, bytes.NewReader([]byte(insertQuery.Values)),
			insertQuery.DataFmt, settings.DEFAULT_BLOCK_SIZE,
		)
		if err != nil {
			return nil, handleBadConn(err)
		}

		defer func() {
			_ = qr.Close()
		}()

		return emptyRows, qr.Exception()
	}

	// If is not insert query
	query, err = bindArgsToQuery(query, args)
	if err != nil {
		return nil, err
	}

	qr, err := c.Gateway.QueryContext(ctx, query)
	if err != nil {
		return nil, handleBadConn(err)
	}

	return &rows{
		queryResult: qr,
	}, qr.Exception()
}

// RunConn runs a query on the raw underlying driver connection
// Use this function for batch inserts or insert with reader
// You must return an error in the callback if there is an error with the query
func RunConn(ctx context.Context, db *sql.DB, callback func(conn sdk.Conn) error) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}

	if err := conn.Raw(func(driverConn interface{}) error {
		rawConn, err := GetConn(driverConn)
		if err != nil {
			return err
		}
		return callback(rawConn)
	}); err != nil {
		return err
	}

	// Note: conn.Close() should only be called if conn.Raw doesn't return an error, otherwise it will hang
	return conn.Close()
}

// GetConn gets the underlying SQL gateway from a database/sql CHConn connection
func GetConn(conn interface{}) (sdk.Conn, error) {
	c, ok := conn.(*CHConn)
	if !ok {
		return nil, driverErrors.ErrorfWithCaller("fail to get connection, error = not underlying sql connection")
	}

	return c.Gateway, nil
}
