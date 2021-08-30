package sql

import (
	"database/sql/driver"
	"io"
	"reflect"

	"github.com/bytehouse-cloud/driver-go/sdk"
)

var emptyRows = &emptyR{}

type emptyR struct{}

func (e emptyR) Columns() []string {
	return []string{}
}

func (e emptyR) Close() error {
	return nil
}

func (e emptyR) Next(dest []driver.Value) error {
	return nil
}

type rows struct {
	columnNames []string
	queryResult *sdk.QueryResult
}

// ColumnTypeDatabaseTypeName returns the
// database system type name without the length. Type names should be uppercase.
// Examples of returned types: "VARCHAR", "NVARCHAR", "VARCHAR2", "CHAR", "TEXT",
// "DECIMAL", "SMALLINT", "INT", "BIGINT", "BOOL", "[]BIGINT", "JSONB", "XML",
// "TIMESTAMP".
func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	return r.queryResult.Columns()[index].CHType()
}

// ColumnTypeScanType returns
// the value type that can be used to scan types into. For example, the database
// column type "bigint" this should return "reflect.TypeOf(int64(0))".
func (r *rows) ColumnTypeScanType(index int) reflect.Type {
	return r.queryResult.Columns()[index].ScanType()
}

// Columns returns the names of the columns. The number of
// columns of the result is inferred from the length of the
// slice. If a particular column name isn't known, an empty
// string should be returned for that entry.
func (r *rows) Columns() []string {
	if r.columnNames == nil {
		r.columnNames = make([]string, len(r.queryResult.Columns()))
		for i, v := range r.queryResult.Columns() {
			r.columnNames[i] = v.Name
		}
	}

	return r.columnNames
}

func (r *rows) Close() error {
	return r.queryResult.Close()
}

// Next is called to populate the next row of data into
// the provided slice. The provided slice will be the same
// size as the Columns() are wide.
//
// Next should return io.EOF when there are no more rows.
//
// The dest should not be written to outside of Next. Care
// should be taken when closing Rows not to modify
// a buffer held in dest.
func (r *rows) Next(dest []driver.Value) error {
	rowValues, ok := r.queryResult.NextRow()
	if !ok {
		return io.EOF
	}

	for i := 0; i < len(r.queryResult.Columns()); i++ {
		dest[i] = driver.Value(rowValues[i])
	}

	return nil
}
