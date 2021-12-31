package format

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data/column"
)

func TestValuesBlockStreamFmtReader_BlockStreamFmtRead(t *testing.T) {
	c := context.Background()
	int32Sample := &data.Block{
		NumColumns: 2,
		NumRows:    0,
		Columns: []*column.CHColumn{
			{
				Name:           "'a'",
				Type:           "Int32",
				Data:           column.MustMakeColumnData(column.INT32, 0),
				GenerateColumn: column.MustGenerateColumnDataFactory(column.INT32),
			},
			{
				Name:           "'b'",
				Type:           "Int32",
				Data:           column.MustMakeColumnData(column.INT32, 0),
				GenerateColumn: column.MustGenerateColumnDataFactory(column.INT32),
			},
		},
	}
	stringSample := &data.Block{
		NumColumns: 2,
		NumRows:    0,
		Columns: []*column.CHColumn{
			{
				Name:           "'a'",
				Type:           "String",
				Data:           column.MustMakeColumnData(column.STRING, 0),
				GenerateColumn: column.MustGenerateColumnDataFactory(column.STRING),
			},
			{
				Name:           "'b'",
				Type:           "String",
				Data:           column.MustMakeColumnData(column.STRING, 0),
				GenerateColumn: column.MustGenerateColumnDataFactory(column.STRING),
			},
		},
	}
	arraySample := &data.Block{
		NumColumns: 2,
		NumRows:    0,
		Columns: []*column.CHColumn{
			{
				Name:           "'a'",
				Type:           "Array(String)",
				Data:           column.MustMakeColumnData("Array(String)", 0),
				GenerateColumn: column.MustGenerateColumnDataFactory("Array(String)"),
			},
			{
				Name:           "'b'",
				Type:           "Array(String)",
				Data:           column.MustMakeColumnData("Array(String)", 0),
				GenerateColumn: column.MustGenerateColumnDataFactory("Array(String)"),
			},
		},
	}
	arrayMapSample := &data.Block{
		NumColumns: 2,
		NumRows:    0,
		Columns: []*column.CHColumn{
			{
				Name:           "'a'",
				Type:           "Array(Map(String, String))",
				Data:           column.MustMakeColumnData("Array(Map(String, String))", 0),
				GenerateColumn: column.MustGenerateColumnDataFactory("Array(Map(String, String))"),
			},
			{
				Name:           "'b'",
				Type:           "Array(Map(String, String))",
				Data:           column.MustMakeColumnData("Array(Map(String, String))", 0),
				GenerateColumn: column.MustGenerateColumnDataFactory("Array(Map(String, String))"),
			},
		},
	}
	emptySettings := map[string]interface{}{}

	type args struct {
		ctx       context.Context
		sample    *data.Block
		blockSize int
	}
	tests := []struct {
		name              string
		blockStreamReader BlockStreamFmtReader
		args              args
		wantBlocksRead    int
		wantRowsRead      int
		wantErr           bool
	}{
		{
			name: "Can parse block into block stream, with correct block size for each block",
			blockStreamReader: func() BlockStreamFmtReader {
				b, _ := BlockStreamFmtReaderFactory(Formats[VALUES], bytes.NewReader([]byte("(1,2), (2,3), (3,4)")), emptySettings)
				return b
			}(),
			args: args{
				ctx:       c,
				sample:    int32Sample,
				blockSize: 3, // Number of rows for each block
			},
			wantBlocksRead: 1, // Should only have one block since there are 3 rows inserted and blockSize = 3
			wantRowsRead:   3,
		},
		{
			name: "Can parse block into block stream, with correct block size for each block",
			blockStreamReader: func() BlockStreamFmtReader {
				b, _ := BlockStreamFmtReaderFactory(Formats[VALUES], bytes.NewReader([]byte("(1,2), (2,3), (3,4)")), emptySettings)
				return b
			}(),
			args: args{
				ctx:       c,
				sample:    int32Sample,
				blockSize: 1, // Number of rows for each block
			},
			wantBlocksRead: 3, // Should only have one block since there are 3 rows inserted and blockSize = 3
			wantRowsRead:   3,
		},
		{
			name: "Read string unquoted",
			blockStreamReader: func() BlockStreamFmtReader {
				b, _ := BlockStreamFmtReaderFactory(Formats[VALUES], bytes.NewReader([]byte("(1,2), (2,3), (3,4)")), emptySettings)
				return b
			}(),
			args: args{
				ctx:       c,
				sample:    stringSample,
				blockSize: 1,
			},
			wantBlocksRead: 3,
			wantRowsRead:   3,
		},
		{
			name: "Read string quoted",
			blockStreamReader: func() BlockStreamFmtReader {
				b, _ := BlockStreamFmtReaderFactory(Formats[VALUES], bytes.NewReader([]byte("(\"1\",\"2/\"), (2,3), (3,4)")), emptySettings)
				return b
			}(),
			args: args{

				ctx:       c,
				sample:    stringSample,
				blockSize: 1,
			},
			wantBlocksRead: 3,
			wantRowsRead:   3,
		},
		{
			name: "Read string quoted with escaped items",
			blockStreamReader: func() BlockStreamFmtReader {
				b, _ := BlockStreamFmtReaderFactory(Formats[VALUES], bytes.NewReader([]byte(`("1","2/"), ("2","3\u123\/few"), ("\b\n\r\t\v\a\0","4")`)), emptySettings)
				return b
			}(),
			args: args{
				ctx:       c,
				sample:    stringSample,
				blockSize: 1,
			},
			wantBlocksRead: 3,
			wantRowsRead:   3,
		},
		{
			name: "Read string quoted with stop byte",
			blockStreamReader: func() BlockStreamFmtReader {
				b, _ := BlockStreamFmtReaderFactory(Formats[VALUES], bytes.NewReader([]byte(`("1","2/"), ("2","3\u123\/few"), ("3","4\x")`)), emptySettings)
				return b
			}(),
			args: args{
				ctx:       c,
				sample:    stringSample,
				blockSize: 1,
			},
			wantBlocksRead: 2,
			wantErr:        true,
		},
		{
			name: "Read array unquoted",
			blockStreamReader: func() BlockStreamFmtReader {
				b, _ := BlockStreamFmtReaderFactory(Formats[VALUES], bytes.NewReader([]byte("([1, 2, 3], [1, 2, 3])")), emptySettings)
				return b
			}(),
			args: args{
				ctx:       c,
				sample:    arraySample,
				blockSize: 1,
			},
			wantBlocksRead: 1,
			wantRowsRead:   1,
		},
		{
			name: "Read array quoted",
			blockStreamReader: func() BlockStreamFmtReader {
				b, _ := BlockStreamFmtReaderFactory(Formats[VALUES], bytes.NewReader([]byte("(['1', '2', '3'], [1, 2, 3])")), emptySettings)
				return b
			}(),
			args: args{
				ctx:       c,
				sample:    arraySample,
				blockSize: 1,
			},
			wantBlocksRead: 1,
			wantRowsRead:   1,
		},
		{
			name: "Read array empty",
			blockStreamReader: func() BlockStreamFmtReader {
				b, _ := BlockStreamFmtReaderFactory(Formats[VALUES], bytes.NewReader([]byte("(['1', '2', '3'],)")), emptySettings)
				return b
			}(),
			args: args{
				ctx:       c,
				sample:    arraySample,
				blockSize: 1,
			},
			wantBlocksRead: 1,
			wantRowsRead:   1,
		},
		{
			name: "Read array map",
			blockStreamReader: func() BlockStreamFmtReader {
				b, _ := BlockStreamFmtReaderFactory(Formats[VALUES], bytes.NewReader([]byte(`([{'1': '3232'}, {'1': '3232'}], [{'1': '3232'}])`)), emptySettings)
				return b
			}(),
			args: args{
				ctx:       c,
				sample:    arrayMapSample,
				blockSize: 1,
			},
			wantBlocksRead: 1,
			wantRowsRead:   1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blockStream, yield := tt.blockStreamReader.BlockStreamFmtRead(tt.args.ctx, tt.args.sample, tt.args.blockSize)
			var nBlocks int
			for range blockStream {
				nBlocks++
			}
			require.Equal(t, nBlocks, tt.wantBlocksRead)

			nRows, err := yield()
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.wantRowsRead, nRows)
		})
	}
}
