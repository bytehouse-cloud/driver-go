package sdk

import (
	"io"

	"github.com/bytehouse-cloud/driver-go/driver/lib/data/column"
)

type ExternalTable struct {
	name        string
	values      [][]interface{}
	columnNames []string
	columnTypes []column.CHColumnType
}

// ExternalTableFromReader creates an external table
// columnTypes are the clickhouse column type for each column in the table, e.g. UInt8, Uint32, etc
// values are the table values
// The type of each column in values table must correspond to the columnTypes specified
func NewExternalTable(name string, values [][]interface{}, columnNames []string, columnTypes []column.CHColumnType) *ExternalTable {
	return &ExternalTable{name: name, values: values, columnNames: columnNames, columnTypes: columnTypes}
}

type ExternalTableReader struct {
	name        string
	reader      io.Reader
	columnNames []string
	columnTypes []column.CHColumnType
	fileType    string
}

// NewExternalTableReader creates an external table from a file
// It parses the data according to the fileType
// The type of each column in the file must correspond to the columnTypes specified
// Column types are the clickhouse column type for each column in the table, e.g. UInt8, Uint32, etc
// Supported fileType values are CSV, CSVWithNames, JSON, VALUES
func NewExternalTableReader(name string, reader io.Reader, columnNames []string, columnTypes []column.CHColumnType, fileType string) *ExternalTableReader {
	return &ExternalTableReader{name: name, reader: reader, columnNames: columnNames, columnTypes: columnTypes, fileType: fileType}
}
