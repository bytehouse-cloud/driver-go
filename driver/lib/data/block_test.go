package data

import (
	"bytes"
	"strings"
	"testing"

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
