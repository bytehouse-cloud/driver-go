package format

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data/column"
)

func TestValuesBlockStreamFmtWriter_BlockStreamFmtWrite(t *testing.T) {
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
	dateTimeSample := &data.Block{
		NumColumns: 2,
		NumRows:    0,
		Columns: []*column.CHColumn{
			{
				Name:           "'a'",
				Type:           "DateTime",
				Data:           column.MustMakeColumnData(column.DATETIME, 0),
				GenerateColumn: column.MustGenerateColumnDataFactory(column.DATETIME),
			},
			{
				Name:           "'b'",
				Type:           "DateTime",
				Data:           column.MustMakeColumnData(column.DATETIME, 0),
				GenerateColumn: column.MustGenerateColumnDataFactory(column.DATETIME),
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
		blockStreamWriter BlockStreamFmtWriter
		args              args
		wantRowsRead      int
	}{
		{
			name: "Can parse block into block stream, with correct block size for each block",
			blockStreamReader: func() BlockStreamFmtReader {
				b, _ := BlockStreamFmtReaderFactory(Formats[VALUES], bytes.NewReader([]byte("(1,2), (2,3), (3,4)")), emptySettings)
				return b
			}(),
			blockStreamWriter: func() BlockStreamFmtWriter {
				b, _ := BlockStreamFmtWriterFactory(Formats[VALUES], bytepool.NewZBufferDefault(), nil)
				return b
			}(),
			args: args{
				ctx:       c,
				sample:    int32Sample,
				blockSize: 3, // Number of rows for each block
			},
			wantRowsRead: 3, // Should only have one block since there are 3 rows inserted and blockSize = 3
		},
		{
			name: "Can parse block into block stream, with correct block size for each block",
			blockStreamReader: func() BlockStreamFmtReader {
				b, _ := BlockStreamFmtReaderFactory(Formats[VALUES], bytes.NewReader([]byte("(1,2), (2,3), (3,4)")), emptySettings)
				return b
			}(),
			blockStreamWriter: func() BlockStreamFmtWriter {
				b, _ := BlockStreamFmtWriterFactory(Formats[VALUES], bytepool.NewZBufferDefault(), nil)
				return b
			}(),
			args: args{
				ctx:       c,
				sample:    int32Sample,
				blockSize: 1, // Number of rows for each block
			},
			wantRowsRead: 3, // Should only have one block since there are 3 rows inserted and blockSize = 3
		},
		{
			name: "Write string unquoted",
			blockStreamReader: func() BlockStreamFmtReader {
				b, _ := BlockStreamFmtReaderFactory(Formats[VALUES], bytes.NewReader([]byte("(1,2), (2,3), (3,4)")), emptySettings)
				return b
			}(),
			blockStreamWriter: func() BlockStreamFmtWriter {
				b, _ := BlockStreamFmtWriterFactory(Formats[VALUES], bytepool.NewZBufferDefault(), nil)
				return b
			}(),
			args: args{
				ctx:       c,
				sample:    stringSample,
				blockSize: 1,
			},
			wantRowsRead: 3,
		},
		{
			name: "Write string quoted",
			blockStreamReader: func() BlockStreamFmtReader {
				b, _ := BlockStreamFmtReaderFactory(Formats[VALUES], bytes.NewReader([]byte("(\"1\",\"2/\"), (2,3), (3,4)")), emptySettings)
				return b
			}(),
			blockStreamWriter: func() BlockStreamFmtWriter {
				b, _ := BlockStreamFmtWriterFactory(Formats[VALUES], bytepool.NewZBufferDefault(), nil)
				return b
			}(),
			args: args{
				ctx:       c,
				sample:    stringSample,
				blockSize: 1,
			},
			wantRowsRead: 3,
		},
		{
			name: "Write string quoted with escaped items",
			blockStreamReader: func() BlockStreamFmtReader {
				b, _ := BlockStreamFmtReaderFactory(Formats[VALUES], bytes.NewReader([]byte(`("1\"","2/"), ("\"\"2","3\u123\/few"), ("\b\n\r\t\v\a\0","4")`)), emptySettings)
				return b
			}(),
			blockStreamWriter: func() BlockStreamFmtWriter {
				b, _ := BlockStreamFmtWriterFactory(Formats[VALUES], bytepool.NewZBufferDefault(), nil)
				return b
			}(),
			args: args{
				ctx:       c,
				sample:    stringSample,
				blockSize: 1,
			},
			wantRowsRead: 3,
		},
		{
			name: "Write datetime",
			blockStreamReader: func() BlockStreamFmtReader {
				b, _ := BlockStreamFmtReaderFactory(Formats[VALUES], bytes.NewReader([]byte("(2006-01-02 15:04:05, 2006-01-02 15:04:05), (2006-01-02 15:04:05,2006-01-02 15:04:05)")), emptySettings)
				return b
			}(),
			blockStreamWriter: func() BlockStreamFmtWriter {
				b, _ := BlockStreamFmtWriterFactory(Formats[VALUES], bytepool.NewZBufferDefault(), nil)
				return b
			}(),
			args: args{
				ctx:       c,
				sample:    dateTimeSample,
				blockSize: 1,
			},
			wantRowsRead: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blockStream := tt.blockStreamReader.BlockStreamFmtRead(tt.args.ctx, tt.args.sample, tt.args.blockSize)
			tt.blockStreamWriter.BlockStreamFmtWrite(blockStream)
			rows, err := tt.blockStreamWriter.Yield()
			require.NoError(t, err)
			require.Equal(t, rows, tt.wantRowsRead)
		})
	}
}
