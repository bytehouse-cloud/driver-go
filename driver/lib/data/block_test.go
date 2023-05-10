package data

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data/column"
)

func TestBlock_StructureCopy(t *testing.T) {
	int32Sample, err := NewBlock(
		[]string{"a", "b"},
		[]column.CHColumnType{
			column.INT32,
			column.INT32,
		},
		0,
	)
	require.NoError(t, err)

	tests := []struct {
		name    string
		block   *Block
		numRows int
	}{
		{
			name:    "Can struct copy with new number of rows",
			block:   int32Sample,
			numRows: 10,
		},
		{
			name:    "Can struct copy with new number of rows",
			block:   int32Sample,
			numRows: 30,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.block.StructureCopy(tt.numRows)
			require.Equal(t, got.NumColumns, tt.block.NumColumns)
			require.Equal(t, got.NumRows, tt.numRows)
		})
	}
}

func TestBlock_ReadFromColumnTexts(t *testing.T) {
	type args struct {
		columnTexts [][]string
	}
	// Values for specific row
	type wantRowValues struct {
		row    int
		values []interface{}
	}
	// Values for specific row
	type wantRowStrings struct {
		row    int
		values []string
	}

	int32Sample, err := NewBlock(
		[]string{"a", "b"},
		[]column.CHColumnType{
			column.INT32,
			column.INT32,
		},
		0,
	)
	require.NoError(t, err)

	tests := []struct {
		name             string
		sample           *Block
		args             args
		wantTableValues  [][]interface{}
		wantTableStrings [][]string
		wantRowValues    *wantRowValues
		wantRowStrings   *wantRowStrings
		numRows          int
		wantRowsRead     int
		wantColsRead     int
		wantErr          bool
	}{
		{
			name:    "Can read right number of rows",
			sample:  int32Sample,
			numRows: 2,
			args: args{columnTexts: [][]string{
				{
					"1", "3",
				},
				{
					"1", "3",
				},
			}},
			wantTableValues: [][]interface{}{
				{int32(1), int32(1)},
				{int32(3), int32(3)},
			},
			wantTableStrings: [][]string{
				{
					"1", "1",
				},
				{
					"3", "3",
				},
			},
			wantRowValues: &wantRowValues{
				row:    0,
				values: []interface{}{int32(1), int32(1)},
			},
			wantRowStrings: &wantRowStrings{
				row:    0,
				values: []string{"1", "1"},
			},
			wantRowsRead: 2,
			wantColsRead: 2,
		},
		{
			name:    "Can read right number of rows",
			sample:  int32Sample,
			numRows: 3,
			args: args{columnTexts: [][]string{
				{
					"3", "3", "3",
				},
				{
					"1", "3", "100",
				},
			}},
			wantTableValues: [][]interface{}{
				{int32(3), int32(1)},
				{int32(3), int32(3)},
				{int32(3), int32(100)},
			},
			wantTableStrings: [][]string{
				{
					"3", "1",
				},
				{
					"3", "3",
				},
				{
					"3", "100",
				},
			},
			wantRowValues: &wantRowValues{
				row:    2,
				values: []interface{}{int32(3), int32(100)},
			},
			wantRowStrings: &wantRowStrings{
				row:    1,
				values: []string{"3", "3"},
			},
			wantRowsRead: 3,
			wantColsRead: 2,
		},
		{
			name:    "Can throw err with right col, rows read if wrong value",
			sample:  int32Sample,
			numRows: 2,
			args: args{columnTexts: [][]string{
				{
					"1", "3",
				},
				{
					"1", "fewf",
				},
			}},
			wantErr:      true,
			wantRowsRead: 1,
			wantColsRead: 1,
		},
		{
			name:    "Can throw err if wrong number of cols",
			sample:  int32Sample,
			numRows: 2,
			args: args{columnTexts: [][]string{
				{
					"1", "3",
				},
				{
					"1", "1",
				},
				{
					"1", "1",
				},
			}},
			wantErr:      true,
			wantRowsRead: 0,
			wantColsRead: 0,
		},
		{
			name:         "Wont throw err if no cols",
			sample:       int32Sample,
			numRows:      2,
			args:         args{columnTexts: [][]string{}},
			wantErr:      false,
			wantRowsRead: 0,
			wantColsRead: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			newBlock := tt.sample.StructureCopy(tt.numRows)
			require.Equal(t, newBlock.NumColumns, tt.sample.NumColumns)
			require.Equal(t, newBlock.NumRows, tt.numRows)

			row, col, err := newBlock.ReadFromColumnTexts(tt.args.columnTexts)
			require.Equal(t, tt.wantRowsRead, row)
			require.Equal(t, tt.wantColsRead, col)

			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			if tt.wantTableValues != nil {
				tableValues := newBlock.NewValuesFrame()
				newBlock.WriteToValues(tableValues)
				require.Equal(t, tt.wantTableValues, tableValues)
			}

			if tt.wantTableStrings != nil {
				tableStrings := newBlock.NewStringFrame()
				newBlock.WriteToStrings(tableStrings)
				require.Equal(t, tableStrings, tt.wantTableStrings)
			}

			if tt.wantRowValues != nil {
				rowValues := make([]interface{}, tt.sample.NumColumns)
				newBlock.WriteRowToValues(rowValues, tt.wantRowValues.row)
				require.Equal(t, tt.wantRowValues.values, rowValues)
			}

			if tt.wantRowStrings != nil {
				rowStrings := make([]string, tt.sample.NumColumns)
				newBlock.WriteRowToStrings(rowStrings, tt.wantRowStrings.row)
				require.Equal(t, tt.wantRowStrings.values, rowStrings)
			}

			require.NoError(t, newBlock.Close())
			require.NoError(t, tt.sample.Close())
		})
	}
}

func TestPrettyFmtBuild(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "Can pretty fmt build normal block",
			test: func(t *testing.T) {
				int32Sample, err := NewBlock(
					[]string{"a", "b"},
					[]column.CHColumnType{
						column.INT32,
						column.INT32,
					},
					0,
				)
				require.NoError(t, err)

				var sb strings.Builder
				int32Sample.PrettyFmtBuild(&sb)
				require.Equal(t, "┌─\u001B[1ma\u001B[0m─┬─\u001B[1mb\u001B[0m─┐\n└───┴───┘\n", sb.String())
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestBlockEncoderDecoder(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "Test Write/Read Empty Blocks",
			test: func(t *testing.T) {
				var buffer bytes.Buffer

				encoder := ch_encoding.NewEncoder(&buffer)
				decoder := ch_encoding.NewDecoder(&buffer)

				int32Sample, err := NewBlock(
					[]string{"a", "b"},
					[]column.CHColumnType{
						column.INT32,
						column.INT32,
					},
					0,
				)
				require.NoError(t, err)

				err = WriteBlockToEncoder(encoder, int32Sample)
				require.NoError(t, err)

				outBlock, err := ReadBlockFromDecoder(decoder)
				require.NoError(t, err)

				var originalSb strings.Builder
				int32Sample.PrettyFmtBuild(&originalSb)

				var outSb strings.Builder
				outBlock.PrettyFmtBuild(&outSb)

				require.Equal(t, originalSb.String(), outSb.String())
			},
		},
		{
			name: "Test Write/Read Blocks with Values",
			test: func(t *testing.T) {
				var buffer bytes.Buffer

				encoder := ch_encoding.NewEncoder(&buffer)
				decoder := ch_encoding.NewDecoder(&buffer)

				int32Sample, err := NewBlock(
					[]string{"a", "b"},
					[]column.CHColumnType{
						column.INT32,
						column.INT32,
					},
					3,
				)
				require.NoError(t, err)

				_, _, err = int32Sample.ReadFromColumnTexts([][]string{
					{"1", "32", "2"}, {"2", "-32", "3"},
				})
				require.NoError(t, err)

				err = WriteBlockToEncoder(encoder, int32Sample)
				require.NoError(t, err)

				outBlock, err := ReadBlockFromDecoder(decoder)
				require.NoError(t, err)

				var originalSb strings.Builder
				int32Sample.PrettyFmtBuild(&originalSb)

				var outSb strings.Builder
				outBlock.PrettyFmtBuild(&outSb)

				require.Equal(t, originalSb.String(), outSb.String())
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestReadFromColumnValues_MoreColumnsThanRows_Success(t *testing.T) {
	numRows := 1
	colNames := []string{"a1", "a2", "a3", "a4", "a5", "a6"}
	colTypes := []column.CHColumnType{"Int64", "Int64", "Int64", "Int64", "Int64", "Int64"}
	block, err := NewBlock(colNames, colTypes, numRows)
	if err != nil {
		panic(err)
	}
	rowsRead, colsRead, err := block.ReadFromColumnValues([][]interface{}{
		{1},
		{2},
		{3},
		{4},
		{5},
		{6},
	})

	assert.NoError(t, err)
	assert.Equal(t, rowsRead, 1)
	assert.Equal(t, colsRead, 6)
}

func TestReadFromColumnValues_MoreColumnsThanRows_FifthColGotError(t *testing.T) {
	numRows := 1
	colNames := []string{"a1", "a2", "a3", "a4", "a5", "a6"}
	colTypes := []column.CHColumnType{"Int64", "Int64", "Int64", "Int64", "Int64", "Int64"}
	block, err := NewBlock(colNames, colTypes, numRows)
	if err != nil {
		panic(err)
	}
	rowsRead, colsRead, err := block.ReadFromColumnValues([][]interface{}{
		{1},
		{2},
		{3},
		{4},
		{"abc"},
		{6},
	})

	assert.Error(t, err)
	assert.Equal(t, rowsRead, 0)
	assert.Equal(t, colsRead, 4)
}

func TestReadFromColumnValues_MoreRowsThanColumns_Success(t *testing.T) {
	numRows := 10
	colNames := []string{"a1", "a2", "a3"}
	colTypes := []column.CHColumnType{"Int64", "Int64", "Int64"}
	block, err := NewBlock(colNames, colTypes, numRows)
	if err != nil {
		panic(err)
	}
	rowsRead, colsRead, err := block.ReadFromColumnValues([][]interface{}{
		{1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
		{1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
		{1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
	})

	assert.NoError(t, err)
	assert.Equal(t, rowsRead, 10)
	assert.Equal(t, colsRead, 3)
}

func TestReadFromColumnValues_MoreRowsThanColumns_SecondColGotError(t *testing.T) {
	numRows := 10
	colNames := []string{"a1", "a2", "a3"}
	colTypes := []column.CHColumnType{"Int64", "Int64", "Int64"}
	block, err := NewBlock(colNames, colTypes, numRows)
	if err != nil {
		panic(err)
	}
	rowsRead, colsRead, err := block.ReadFromColumnValues([][]interface{}{
		{1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
		{1, 1, 1, "abc", 1, 1, 1, 1, 1, 1},
		{1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
	})

	assert.Error(t, err)
	assert.Equal(t, rowsRead, 3)
	assert.Equal(t, colsRead, 1)
}

func TestReadFromColumnsValueToBlock_ThenWriteToEncoder_ThenDecodeBack(t *testing.T) {
	type args struct {
		data     [][]interface{}
		colNames []string
		colTypes []column.CHColumnType
		numRows  int
	}
	type testCase struct {
		name string
		args args
	}

	testCases := []testCase{
		{
			name: "IF Empty map data THEN no error happens",
			args: args{
				data:     [][]interface{}{},
				colNames: []string{"map_col"},
				colTypes: []column.CHColumnType{
					column.CHColumnType("Map(String, String)"),
				},
				numRows: 0,
			},
		},
		{
			name: "IF non empty map data THEN no error happens",
			args: args{
				data: [][]interface{}{
					{
						map[string]string{},
						map[string]string{"tai": "nmba"},
					},
				},
				colNames: []string{"map_col"},
				colTypes: []column.CHColumnType{
					column.CHColumnType("Map(String, String)"),
				},
				numRows: 2,
			},
		},
		{
			name: "IF empty array data THEN no error happens",
			args: args{
				data:     [][]interface{}{},
				colNames: []string{"array_col"},
				colTypes: []column.CHColumnType{
					column.CHColumnType("Array(String)"),
				},
				numRows: 0,
			},
		},
		{
			name: "IF non-empty array data THEN no error happens",
			args: args{
				data: [][]interface{}{
					{
						[]string{
							"tai",
							"nmba",
						},
						[]string{
							"26072000",
							"25042003",
						},
					},
				},
				colNames: []string{"array_col"},
				colTypes: []column.CHColumnType{
					column.CHColumnType("Array(String)"),
				},
				numRows: 2,
			},
		},
		{
			name: "IF empty low cardinality data THEN no error happens",
			args: args{
				data:     [][]interface{}{},
				colNames: []string{"low_cardinality_col"},
				colTypes: []column.CHColumnType{
					column.CHColumnType("LowCardinality(String)"),
				},
				numRows: 0,
			},
		},
		{
			name: "IF non-empty low cardinality data THEN no error happens",
			args: args{
				data: [][]interface{}{
					{
						"tai",
						"nmba",
					},
				},
				colNames: []string{"low_cardinality_col"},
				colTypes: []column.CHColumnType{
					column.CHColumnType("LowCardinality(String)"),
				},
				numRows: 2,
			},
		},
		{
			name: "IF empty nullable map column data THEN no error happens",
			args: args{
				data:     [][]interface{}{},
				colNames: []string{"nullable_map_col"},
				colTypes: []column.CHColumnType{
					column.CHColumnType("Nullable(Map(String, UInt8))"),
				},
				numRows: 0,
			},
		},
		{
			name: "IF non-empty nullable map column data THEN no error happens",
			args: args{
				data: [][]interface{}{
					{
						nil,
						map[interface{}]interface{}{
							"tai":  uint64(26072000),
							"nmba": uint64(25042003),
						},
					},
				},
				colNames: []string{"nullable_map_col"},
				colTypes: []column.CHColumnType{
					column.CHColumnType("Nullable(Map(String, UInt64))"),
				},
				numRows: 2,
			},
		},
		{
			name: "IF empty nullable array column data THEN no error happens",
			args: args{
				data:     [][]interface{}{},
				colNames: []string{"nullable_array_col"},
				colTypes: []column.CHColumnType{
					column.CHColumnType("Nullable(Array(UInt8))"),
				},
				numRows: 0,
			},
		},
		{
			name: "IF non-empty nullable array column data THEN no error happens",
			args: args{
				data: [][]interface{}{
					{
						nil,
						[]uint8{
							uint8(1),
							uint8(1),
						},
					},
				},
				colNames: []string{"nullable_array_col"},
				colTypes: []column.CHColumnType{
					column.CHColumnType("Nullable(Array(UInt8))"),
				},
				numRows: 2,
			},
		},
		{
			name: "IF empty nullable nested column data type Array(Array(Map(String, UInt64))) THEN no error happens",
			args: args{
				data:     [][]interface{}{},
				colNames: []string{"nullable_nested_array_col"},
				colTypes: []column.CHColumnType{
					column.CHColumnType("Nullable(Array(Array(Map(String, UInt64))))"),
				},
				numRows: 0,
			},
		},
		{
			name: "IF non-empty nullable nested column data type Array(Array(Map(String, UInt64))) THEN no error happens",
			args: args{
				data: [][]interface{}{
					{
						nil,
						[][]map[string]uint64{
							{
								map[string]uint64{
									"tai":  uint64(26072000),
									"nmba": uint64(25042003),
								},
							},
						},
					},
				},
				colNames: []string{"nullable_nested_array_col"},
				colTypes: []column.CHColumnType{
					column.CHColumnType("Nullable(Array(Array(Map(String, UInt64))))"),
				},
				numRows: 2,
			},
		},
	}

	for _, tt := range testCases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			block, err := NewBlock(tt.args.colNames, tt.args.colTypes, tt.args.numRows)
			assert.NoError(t, err)

			actualRowReads, _, err := block.ReadFromColumnValues(tt.args.data)
			assert.NoError(t, err)
			assert.Equal(t, tt.args.numRows, actualRowReads)

			var buf bytes.Buffer
			encoder := ch_encoding.NewEncoder(&buf)
			decoder := ch_encoding.NewDecoder(&buf)

			err = WriteBlockToEncoder(encoder, block)
			assert.NoError(t, err)

			block, err = ReadBlockFromDecoder(decoder)
			assert.NoError(t, err)
			assert.Equal(t, tt.args.numRows, block.NumRows)
		})
	}
}
