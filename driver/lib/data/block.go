package data

import (
	"strings"
	"unicode/utf8"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data/column"
	"github.com/bytehouse-cloud/driver-go/errors"
)

type Block struct {
	info       *blockInfo
	NumColumns int
	NumRows    int
	Columns    []*column.CHColumn
}

func NewBlock(colNames []string, colTypes []column.CHColumnType, numRows int) (*Block, error) {
	numCols := len(colNames)
	if numCols != len(colTypes) {
		return nil, errors.ErrorfWithCaller("len don't match: %s, %s", colNames, colTypes)
	}

	cols := make([]*column.CHColumn, len(colTypes))
	for i, colType := range colTypes {
		gen, err := column.GenerateColumnDataFactory(colType)
		if err != nil {
			return nil, err
		}

		cols[i] = &column.CHColumn{
			Name:           colNames[i],
			Type:           colType,
			GenerateColumn: gen,
		}
		cols[i].Data = cols[i].GenerateColumn(numRows)
	}

	return &Block{
		NumColumns: len(cols),
		NumRows:    numRows,
		Columns:    cols,
	}, nil
}

func ReadBlockFromDecoder(decoder *ch_encoding.Decoder) (*Block, error) {
	var (
		block Block
		i     uint64
		err   error
	)
	block.info, err = readBlockInfo(decoder)
	if err != nil {
		return nil, err
	}
	i, err = decoder.Uvarint()
	if err != nil {
		return nil, err
	}
	block.NumColumns = int(i)
	i, err = decoder.Uvarint()
	if err != nil {
		return nil, err
	}
	block.NumRows = int(i)
	block.Columns = make([]*column.CHColumn, block.NumColumns)
	for j := range block.Columns {
		if block.Columns[j], err = column.ReadColumnFromDecoder(decoder, block.NumRows); err != nil {
			return nil, err
		}
	}
	return &block, nil
}

func WriteBlockToEncoder(encoder *ch_encoding.Encoder, b *Block) error {
	if b.info == nil {
		b.info = &blockInfo{}
	}
	if err := writeBlockInfo(encoder, b.info); err != nil {
		return err
	}
	if err := encoder.Uvarint(uint64(b.NumColumns)); err != nil {
		return err
	}
	if err := encoder.Uvarint(uint64(b.NumRows)); err != nil {
		return err
	}
	for _, col := range b.Columns {
		if err := column.WriteColumnToEncoder(encoder, col); err != nil {
			return err
		}
	}
	return nil
}

// StructureCopy copies the metadata of Block, with n rows
func (b *Block) StructureCopy(numRows int) *Block {
	newColumns := make([]*column.CHColumn, len(b.Columns))
	for i, c := range b.Columns {
		newColumns[i] = c.StructureCopy(numRows)
	}

	return &Block{
		info:       b.info,
		NumColumns: len(newColumns),
		Columns:    newColumns,
		NumRows:    numRows,
	}
}

// NewStringFrame returns an empty 2 dimensional string
// with dimensions same as the NumRows and NumColumns of the block.
func (b *Block) NewStringFrame() [][]string {
	frame := make([][]string, b.NumRows)
	for i := range frame {
		frame[i] = make([]string, b.NumColumns)
	}
	return frame
}

// NewValuesFrame returns an empty 2 dimensional interface
// with dimensions same as the NumRows and NumColumns of the block.
func (b *Block) NewValuesFrame() [][]interface{} {
	frame := make([][]interface{}, b.NumRows)
	for i := range frame {
		frame[i] = make([]interface{}, b.NumColumns)
	}
	return frame
}

// ReadFromColumnTexts attempts to read columnTexts into current block.
// return numbers of rows read, columns read and error if any.
// if err occurs during read, return the first total num of rows before error occur for that column instead.
func (b *Block) ReadFromColumnTexts(columnTexts [][]string) (rowsRead, columnsRead int, err error) {
	if len(columnTexts) == 0 {
		return 0, 0, nil
	}
	if len(columnTexts) != b.NumColumns {
		return 0, 0, errors.ErrorfWithCaller("incorrect number of column, given: %v, expected: %v", len(columnTexts), b.NumColumns)
	}

	var col []string
	for rowsRead, col = range columnTexts {
		if columnsRead, err = b.Columns[rowsRead].Data.ReadFromTexts(col); err != nil {
			return columnsRead, rowsRead, err
		}
	}

	return columnsRead, len(columnTexts), nil
}

func (b *Block) ReadFromColumnValues(colValues [][]interface{}) (rowsRead, columnsRead int, err error) {
	if len(colValues) == 0 {
		return 0, 0, nil
	}
	if len(colValues) != b.NumColumns {
		return 0, 0, errors.ErrorfWithCaller("incorrect number of column, given: %v, expected: %v", len(colValues), b.NumColumns)
	}

	var col []interface{}
	for rowsRead, col = range colValues {
		if columnsRead, err = b.Columns[rowsRead].Data.ReadFromValues(col); err != nil {
			return columnsRead, rowsRead, err
		}
	}

	return columnsRead, len(colValues), nil
}

func (b *Block) Close() error {
	for i := range b.Columns {
		if err := b.Columns[i].Close(); err != nil {
			return err
		}
	}
	return nil
}

func (b *Block) WriteToValues(values [][]interface{}) {
	for i := 0; i < b.NumRows; i++ {
		row := values[i]
		for j := range row {
			row[j] = b.Columns[j].Data.GetValue(i)
		}
	}
	return
}

// WriteToStrings attempts to read to dataframe, panics if numRows and numCols exceed the dimension of dataframe.
// Callers are expected to check dimension of dataframe passed in.
func (b *Block) WriteToStrings(frame [][]string) {
	for i := 0; i < b.NumRows; i++ {
		row := frame[i]
		for j := range row {
			row[j] = b.Columns[j].Data.GetString(i)
		}
	}
	return
}

// WriteToStringsV2  is similar to WriteToStrings but using the boundaries of the given
// frame instead of boundaries of block.
func (b *Block) WriteToStringsV2(frame [][]string) {
	for i := 0; i < b.NumRows; i++ {
		row := frame[i]
		for j := range row {
			row[j] = b.Columns[j].Data.GetString(i)
		}
	}
	return
}

func (b *Block) WriteRowToValues(record []interface{}, row int) {
	_ = record[b.NumColumns-1]

	for i := range b.Columns {
		record[i] = b.Columns[i].Data.GetValue(row)
	}
}

func (b *Block) WriteRowToStrings(record []string, row int) {
	_ = record[b.NumColumns-1]

	for i := range b.Columns {
		record[i] = b.Columns[i].Data.GetString(row)
	}
}

func (b *Block) strFmtInfo() ([][]string, []string, []int) {
	frame := make([][]string, b.NumRows)

	maxColumnStringLen := make([]int, b.NumColumns) // max len of the strings in each column
	columnNames := b.ColumnNames()
	for i := range columnNames {
		maxColumnStringLen[i] = len(columnNames[i])
	}

	for i := range frame {
		row := make([]string, b.NumColumns)
		for j := range row {
			s := b.Columns[j].Data.GetString(i)
			if utf8.RuneCountInString(s) > maxColumnStringLen[j] {
				maxColumnStringLen[j] = utf8.RuneCountInString(s)
			}
			row[j] = s
		}
		frame[i] = row
	}

	return frame, columnNames, maxColumnStringLen
}

func (b *Block) ColumnNames() []string {
	if b == nil {
		return []string{}
	}

	result := make([]string, b.NumColumns)
	for i := range result {
		result[i] = b.Columns[i].Name
	}
	return result
}

func (b *Block) PrettyFmtBuild(builder *strings.Builder) {
	p := NewBlocksPrinter(500)
	p.Print(b, builder)
}
