package sdk

import (
	"fmt"
	"io"

	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data/column"
	"github.com/bytehouse-cloud/driver-go/utils"
)

type ExternalTable struct {
	name        string
	values      [][]interface{}
	columnNames []string
	columnTypes []column.CHColumnType
}

func (e *ExternalTable) ToSingleBlockStream() (<-chan *data.Block, error) {
	if e == nil {
		return nil, nil
	}

	blockStream := make(chan *data.Block, 1)
	defer close(blockStream)

	newBlock, err := data.NewBlock(e.columnNames, e.columnTypes, len(e.values))
	if err != nil {
		return nil, err
	}

	columnValues := utils.TransposeMatrix(e.values)
	for i, col := range newBlock.Columns {
		rowsRead, err := col.Data.ReadFromValues(columnValues[i])
		if err != nil {
			return nil, fmt.Errorf("error reading external table, col_idx: %v, row_idx: %v, err: %v", col, rowsRead, err)
		}
	}

	blockStream <- newBlock
	return blockStream, nil
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
