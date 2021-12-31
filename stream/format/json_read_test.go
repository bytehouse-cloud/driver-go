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

func TestJSONBlockStreamFmtReader_BlockStreamFmtRead(t *testing.T) {
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
				Data:           column.MustMakeColumnData(column.STRING, 0),
				GenerateColumn: column.MustGenerateColumnDataFactory(column.STRING),
			},
			{
				Name:           "b",
				Type:           "String",
				Data:           column.MustMakeColumnData(column.STRING, 0),
				GenerateColumn: column.MustGenerateColumnDataFactory(column.STRING),
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
		wantBlocksRead    int
		wantRowsRead      int
		wantErr           bool
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
			wantBlocksRead: 1, // Should only have one block since there are 3 rows inserted and blockSize = 3
			wantRowsRead:   3,
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
			wantBlocksRead: 1, // Should only have one block since there are 3 rows inserted and blockSize = 3
			wantRowsRead:   3,
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
			wantBlocksRead: 3, // Should only have one block since there are 3 rows inserted and blockSize = 3
			wantRowsRead:   3,
		},
		{
			name: "Can non-quoted json into block streams",
			blockStreamReader: func() BlockStreamFmtReader {
				b, _ := BlockStreamFmtReaderFactory(Formats[JSON], bytes.NewReader([]byte(`{"data": [{ "a": 1, "b": 2 }, { "a": 1, "b": 2 }, { "a": 1, "b": 2 }]}`)), emptySettings)
				return b
			}(),
			args: args{
				ctx:       c,
				sample:    int32Sample,
				blockSize: 1,
			},
			wantBlocksRead: 3, // Should only have one block since there are 3 rows inserted and blockSize = 3
			wantRowsRead:   3,
		},
		{
			name: "Can read quoted string json into block streams",
			blockStreamReader: func() BlockStreamFmtReader {
				b, _ := BlockStreamFmtReaderFactory(Formats[JSON], bytes.NewReader([]byte(`{"data": [{ "a": "1", "b": "2" }, { "a": "1", "b": "2" }, { "a": "1", "b": "2" }]}`)), emptySettings)
				return b
			}(),
			args: args{
				ctx:       c,
				sample:    stringSample,
				blockSize: 1,
			},
			wantBlocksRead: 3, // Should only have one block since there are 3 rows inserted and blockSize = 3
			wantRowsRead:   3,
		},
		{
			name: "Can read non-quoted string json into block streams",
			blockStreamReader: func() BlockStreamFmtReader {
				b, _ := BlockStreamFmtReaderFactory(Formats[JSON], bytes.NewReader([]byte(`{"data": [{ "a": 1, "b": 2 }, { "a": 1, "b": 2 }, { "a": 1, "b": 2 }]}`)), emptySettings)
				return b
			}(),
			args: args{
				ctx:       c,
				sample:    stringSample,
				blockSize: 1,
			},
			wantBlocksRead: 3, // Should only have one block since there are 3 rows inserted and blockSize = 3
			wantRowsRead:   3,
		},
		{
			name: "Can read json batch into block streams",
			blockStreamReader: func() BlockStreamFmtReader {
				b, _ := BlockStreamFmtReaderFactory(Formats[JSON], bytes.NewReader([]byte(`{"data": [{ "a": 1, "b": 2 }, { "a": 1, "b": 2 }`)), emptySettings)
				return b
			}(),
			args: args{
				ctx:       c,
				sample:    stringSample,
				blockSize: 1,
			},
			wantBlocksRead: 2,
			wantRowsRead:   2,
		},
		{
			name: "Should throw error if column name given but not values",
			blockStreamReader: func() BlockStreamFmtReader {
				b, _ := BlockStreamFmtReaderFactory(Formats[JSON], bytes.NewReader([]byte(`{"data": [{ "a": 1, "b": 2 }, { "a": 1, "b"`)), emptySettings)
				return b
			}(),
			args: args{
				ctx:       c,
				sample:    stringSample,
				blockSize: 1,
			},
			wantErr: true,
		},
		{
			name: "Can read empty json into block streams",
			blockStreamReader: func() BlockStreamFmtReader {
				b, _ := BlockStreamFmtReaderFactory(Formats[JSON], bytes.NewReader([]byte(``)), emptySettings)
				return b
			}(),
			args: args{
				ctx:       c,
				sample:    stringSample,
				blockSize: 1,
			},
			wantBlocksRead: 0, // Should only have one block since there are 3 rows inserted and blockSize = 3
			wantRowsRead:   0,
		},
		{
			name: "Should not read any blocks if json format wrong",
			blockStreamReader: func() BlockStreamFmtReader {
				b, _ := BlockStreamFmtReaderFactory(Formats[JSON], bytes.NewReader([]byte(`{"data": [{ "a": "1", "b": "2" , { "a": "1", "b": "2" }, { "a": "1", "b": "2" }], "rows": 3, "rows_before_limit_at_least": 3}`)), emptySettings)
				return b
			}(),
			args: args{
				ctx:       c,
				sample:    int32Sample,
				blockSize: 1,
			},
			wantErr: true,
		},
		{
			name: "Should not read subsequent blocks if json format wrong",
			blockStreamReader: func() BlockStreamFmtReader {
				b, _ := BlockStreamFmtReaderFactory(Formats[JSON], bytes.NewReader([]byte(`{"data": [{ "a": "1", "b": "2" }, { \"a": "1", "b": "2" }, { "a": "1", "b": "2" }], "rows": 3, "rows_before_limit_at_least": 3}`)), emptySettings)
				return b
			}(),
			args: args{
				ctx:       c,
				sample:    int32Sample,
				blockSize: 1,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blockStream, yield := tt.blockStreamReader.BlockStreamFmtRead(tt.args.ctx, tt.args.sample, tt.args.blockSize)
			var nBlocks int
			for range blockStream {
				nBlocks++
			}
			nRows, err := yield()
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.wantRowsRead, nRows)
			require.Equal(t, tt.wantBlocksRead, nBlocks)
		})
	}
}
