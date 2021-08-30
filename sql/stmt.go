package sql

import (
	"context"
	"database/sql/driver"
	"errors"

	connPackage "github.com/bytehouse-cloud/driver-go/conn"
	"github.com/bytehouse-cloud/driver-go/sdk"
)

type stmt struct {
	// Number of expected arguments
	numArgs int
	// Raw select query string
	selectQuery string
	// Connection to send select requests
	c *CHConn

	insertStmt *sdk.InsertStmt
	isInsert   bool
}

func newInsertStmt(numArgs int, insertStmt *sdk.InsertStmt) *stmt {
	return &stmt{
		numArgs:    numArgs,
		insertStmt: insertStmt,
		isInsert:   true,
	}
}

func newSelectStmt(numArgs int, query string, c *CHConn) *stmt {
	return &stmt{
		numArgs:     numArgs,
		selectQuery: query,
		c:           c,
	}
}

// NumInput returns the number of placeholder parameters.
//
// If NumInput returns >= 0, the sql package will sanity check
// argument counts from callers and return errors to the caller
// before the statement's Exec or Query methods are called.
//
// NumInput may also return -1, if the driver doesn't know
// its number of placeholders. In that case, the sql package
// will not sanity check Exec or Query argument counts.
func (s *stmt) NumInput() int {
	return s.numArgs
}

func (s *stmt) Close() error {
	if !s.isInsert {
		return nil
	}

	return s.insertStmt.Close()
}

func toInterfaces(namedArgs []driver.NamedValue) []interface{} {
	result := make([]interface{}, len(namedArgs))
	for i, namedArg := range namedArgs {
		result[i] = namedArg.Value
	}
	return result
}

func (s *stmt) ExecContext(ctx context.Context, namedArgs []driver.NamedValue) (driver.Result, error) {
	if s.isInsert {
		return emptyResult, s.insertStmt.ExecContext(ctx, toInterfaces(namedArgs)...)
	}

	args, err := namedArgsToArgs(namedArgs)
	if err != nil {
		return nil, err
	}

	r, err := s.runSelectQuery(ctx, args)
	if err != nil {
		return nil, err
	}
	_ = r.Close()

	return emptyResult, nil
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	if s.isInsert {
		return emptyResult, s.insertStmt.Exec(args)
	}

	r, err := s.runSelectQuery(context.Background(), args)
	if err != nil {
		return nil, err
	}
	_ = r.Close()

	return emptyResult, nil
}

func (s *stmt) QueryContext(ctx context.Context, namedArgs []driver.NamedValue) (driver.Rows, error) {
	if s.isInsert {
		return emptyRows, s.insertStmt.ExecContext(ctx, toInterfaces(namedArgs)...)
	}

	args, err := namedArgsToArgs(namedArgs)
	if err != nil {
		return nil, err
	}

	return s.runSelectQuery(ctx, args)
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	if s.isInsert {
		return emptyRows, s.insertStmt.Exec(args)
	}

	return s.runSelectQuery(context.Background(), args)
}

func (s *stmt) runSelectQuery(ctx context.Context, args []driver.Value) (driver.Rows, error) {
	selectQuery, err := bindArgsToQuery(s.selectQuery, args)
	if err != nil {
		return nil, err
	}

	qr, err := s.c.Gateway.QueryContext(ctx, selectQuery)
	if err != nil {
		// If it is ErrBadConnection, discard connection by returning ErrBadConn
		if errors.Is(connPackage.ErrBadConnection{}, err) {
			return nil, driver.ErrBadConn
		}
		return nil, err
	}

	return &rows{
		queryResult: qr,
	}, qr.Exception()
}
