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

func TestJSONBlockStreamFmtWriter_BlockStreamFmtWrite(t *testing.T) {
	c := context.Background()
	int32Sample := &data.Block{
		NumColumns: 2,
		NumRows:    0,
		Columns: []*column.CHColumn{
			{
				Name:           "a",
				Type:           "Int32",
				Data:           column.MustMakeColumnData(column.INT32, 0),
				GenerateColumn: column.MustGenerateColumnDataFactory(column.INT32),
			},
			{
				Name:           "b",
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
				Name:           "a",
				Type:           "String",
				Data:           column.MustGenerateColumnDataFactory(column.STRING)(0),
				GenerateColumn: column.MustGenerateColumnDataFactory(column.STRING),
			},
			{
				Name:           "b",
				Type:           "String",
				Data:           column.MustGenerateColumnDataFactory(column.STRING)(0),
				GenerateColumn: column.MustGenerateColumnDataFactory(column.STRING),
			},
		},
	}
	uuidSample := &data.Block{
		NumColumns: 2,
		NumRows:    0,
		Columns: []*column.CHColumn{
			{
				Name:           "a",
				Type:           "UUID",
				Data:           column.MustGenerateColumnDataFactory(column.UUID)(0),
				GenerateColumn: column.MustGenerateColumnDataFactory(column.UUID),
			},
			{
				Name:           "b",
				Type:           "UUID",
				Data:           column.MustGenerateColumnDataFactory(column.UUID)(0),
				GenerateColumn: column.MustGenerateColumnDataFactory(column.UUID),
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
				// Note: Data passed in must have meta field and follow the exact format
				b, _ := BlockStreamFmtReaderFactory(Formats[JSON], bytes.NewReader([]byte(`{"meta": [], "data": [{ "a": "1", "b": "2" }, { "a": "1", "b": "2" }, { "a": "1", "b": "2" }], "rows": 3, "rows_before_limit_at_least": 3}`)), emptySettings)
				return b
			}(),
			blockStreamWriter: func() BlockStreamFmtWriter {
				b, _ := BlockStreamFmtWriterFactory(Formats[JSON], bytepool.NewZBufferDefault(), nil)
				return b
			}(),
			args: args{
				ctx:       c,
				sample:    int32Sample,
				blockSize: 3, // Number of rows for each block
			},
			wantRowsRead: 3,
		},
		{
			name: "Can parse data without meta, can parse block into block stream, with correct block size for each block",
			blockStreamReader: func() BlockStreamFmtReader {
				// Note: Data passed in must have meta field and follow the exact format
				b, _ := BlockStreamFmtReaderFactory(Formats[JSON], bytes.NewReader([]byte(`{"data": [{ "a": "1", "b": "2" }, { "a": "1", "b": "2" }, { "a": "1", "b": "2" }], "rows": 3, "rows_before_limit_at_least": 3}`)), emptySettings)
				return b
			}(),
			blockStreamWriter: func() BlockStreamFmtWriter {
				b, _ := BlockStreamFmtWriterFactory(Formats[JSON], bytepool.NewZBufferDefault(), nil)
				return b
			}(),
			args: args{
				ctx:       c,
				sample:    int32Sample,
				blockSize: 3, // Number of rows for each block
			},
			wantRowsRead: 3,
		},
		{
			name: "Can parse block into block stream, with correct block size for each block",
			blockStreamReader: func() BlockStreamFmtReader {
				b, _ := BlockStreamFmtReaderFactory(Formats[JSON], bytes.NewReader([]byte(`{"meta": [], "data": [{ "a": "1", "b": "2" }, { "a": "1", "b": "2" }, { "a": "1", "b": "2" }], "rows": 3, "rows_before_limit_at_least": 3}`)), emptySettings)
				return b
			}(),
			blockStreamWriter: func() BlockStreamFmtWriter {
				b, _ := BlockStreamFmtWriterFactory(Formats[JSON], bytepool.NewZBufferDefault(), nil)
				return b
			}(),
			args: args{
				ctx:       c,
				sample:    int32Sample,
				blockSize: 1, // Number of rows for each block
			},
			wantRowsRead: 3,
		},
		{
			name: "Can parse block into block stream, with correct block size for each block",
			blockStreamReader: func() BlockStreamFmtReader {
				b, _ := BlockStreamFmtReaderFactory(Formats[JSON], bytes.NewReader([]byte(`{"data": [{ "a": "2bde68e7-3c7e-4032-9217-ff138e5007b3", "b": "2bde68e7-3c7e-4032-9217-ff138e5007b3" }]}`)), emptySettings)
				return b
			}(),
			blockStreamWriter: func() BlockStreamFmtWriter {
				b, _ := BlockStreamFmtWriterFactory(Formats[JSON], bytepool.NewZBufferDefault(), nil)
				return b
			}(),
			args: args{
				ctx:       c,
				sample:    uuidSample,
				blockSize: 1, // Number of rows for each block
			},
			wantRowsRead: 1,
		},
		{
			name: "Can parse block into block stream, with correct block size for each block",
			blockStreamReader: func() BlockStreamFmtReader {
				b, _ := BlockStreamFmtReaderFactory(Formats[JSON], bytes.NewReader([]byte(`{"data": [{ "a": "\u2028", "b": "/" }, { "a": "", "b": "/" }]}`)), emptySettings)
				return b
			}(),
			blockStreamWriter: func() BlockStreamFmtWriter {
				b, _ := BlockStreamFmtWriterFactory(Formats[JSON], bytepool.NewZBufferDefault(), nil)
				return b
			}(),
			args: args{
				ctx:       c,
				sample:    stringSample,
				blockSize: 1, // Number of rows for each block
			},
			wantRowsRead: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blockStream, yield := tt.blockStreamReader.BlockStreamFmtRead(tt.args.ctx, tt.args.sample, tt.args.blockSize)
			tt.blockStreamWriter.BlockStreamFmtWrite(blockStream)
			rows, err := yield()
			require.NoError(t, err)
			require.Equal(t, rows, tt.wantRowsRead)
		})
	}
}
