package format

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data/column"
)

func TestCSVBlockStreamFmtWriter_BlockStreamFmtWrite(t *testing.T) {
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
		wantRowsRead      int
	}{
		{
			name: "Can parse block into block stream, with correct rows read",
			blockStreamReader: func() BlockStreamFmtReader {
				b, _ := BlockStreamFmtReaderFactory(Formats[CSV], bytes.NewReader([]byte("1, 2\n1,2\n1,2")), emptySettings)
				return b
			}(),
			blockStreamWriter: func() BlockStreamFmtWriter {
				b, _ := BlockStreamFmtWriterFactory(Formats[CSV], bytepool.NewZBufferDefault(), nil)
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
			name: "Can quoted csv into block streams",
			blockStreamReader: func() BlockStreamFmtReader {
				b, _ := BlockStreamFmtReaderFactory(Formats[CSV], bytes.NewReader([]byte("'1', '2'\n'1','2'\n'1','2'")), emptySettings)
				return b
			}(),
			blockStreamWriter: func() BlockStreamFmtWriter {
				b, _ := BlockStreamFmtWriterFactory(Formats[CSV], bytepool.NewZBufferDefault(), nil)
				return b
			}(),
			args: args{
				ctx:       c,
				sample:    int32Sample,
				blockSize: 1,
			},
			wantRowsRead: 3,
		},
		{
			name: "Can read quoted string csv into block streams",
			blockStreamReader: func() BlockStreamFmtReader {
				b, _ := BlockStreamFmtReaderFactory(Formats[CSV], bytes.NewReader([]byte("'1', '2'\n'1','2'\n'1','2'")), emptySettings)
				return b
			}(),
			blockStreamWriter: func() BlockStreamFmtWriter {
				b, _ := BlockStreamFmtWriterFactory(Formats[CSV], bytepool.NewZBufferDefault(), nil)
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
			name: "Can read non-quoted string csv into block streams",
			blockStreamReader: func() BlockStreamFmtReader {
				b, _ := BlockStreamFmtReaderFactory(Formats[CSV], bytes.NewReader([]byte("1, 2\n1,2\n1,2")), emptySettings)
				return b
			}(),
			blockStreamWriter: func() BlockStreamFmtWriter {
				b, _ := BlockStreamFmtWriterFactory(Formats[CSV], bytepool.NewZBufferDefault(), nil)
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
			name: "Should not read any blocks if csv format wrong",
			blockStreamReader: func() BlockStreamFmtReader {
				b, _ := BlockStreamFmtReaderFactory(Formats[CSV], bytes.NewReader([]byte("1")), emptySettings)
				return b
			}(),
			blockStreamWriter: func() BlockStreamFmtWriter {
				b, _ := BlockStreamFmtWriterFactory(Formats[CSV], bytepool.NewZBufferDefault(), nil)
				return b
			}(),
			args: args{
				ctx:       c,
				sample:    int32Sample,
				blockSize: 1,
			},
			wantRowsRead: 0,
		},
		{
			name: "Should not read subsequent blocks if csv format wrong",
			blockStreamReader: func() BlockStreamFmtReader {
				b, _ := BlockStreamFmtReaderFactory(Formats[CSV], bytes.NewReader([]byte("1, 2\n1")), emptySettings)
				return b
			}(),
			blockStreamWriter: func() BlockStreamFmtWriter {
				b, _ := BlockStreamFmtWriterFactory(Formats[CSV], bytepool.NewZBufferDefault(), nil)
				return b
			}(),
			args: args{
				ctx:       c,
				sample:    int32Sample,
				blockSize: 1,
			},
			wantRowsRead: 1,
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

func TestNewCSVBlockStreamFmtWriter(t *testing.T) {
	tests := []struct {
		name        string
		withNames   bool
		settings    map[string]interface{}
		givenBlocks []*data.Block

		fmtResult string
		wantErr   bool
	}{
		{
			name: "if no block then output nothing",
		},
		{
			name:        "if 1 block with names then output csv with names",
			withNames:   true,
			givenBlocks: []*data.Block{makeSampleBlock()},
			fmtResult: `col1,col1,col2
"string1",123
"string2",456`,
		},
		{
			name:        "if 2 blocks with names then output col names only once",
			withNames:   true,
			givenBlocks: []*data.Block{makeSampleBlock(), makeSampleBlock()},
			fmtResult: `col1,col1,col2
"string1",123
"string2",456
"string1",123
"string2",456`,
		},
		{
			name:     "if with invalid delimiter then error",
			settings: map[string]interface{}{"format_csv_delimiter": "|||"},
			wantErr:  true,
		},
		{
			name:        "if with valid delimiter then print with selected delimiter",
			settings:    map[string]interface{}{"format_csv_delimiter": "|"},
			givenBlocks: []*data.Block{makeSampleBlock()},
			withNames:   true,
			fmtResult: `col1|col1|col2
"string1"|123
"string2"|456`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			csvFmtWriter, err := NewCSVBlockStreamFmtWriter(buf, tt.withNames, tt.settings)
			if err != nil {
				if tt.wantErr {
					return
				}
				assert.Nil(t, err)
				return
			}
			bs := toBlockStream(tt.givenBlocks)
			csvFmtWriter.BlockStreamFmtWrite(bs)
			_, err = csvFmtWriter.Yield()
			if !assert.Nil(t, err) {
				return
			}
			if !assert.Equal(t, buf.String(), tt.fmtResult) {
				fmt.Println(buf.String())
			}
		})
	}
}

func makeSampleBlock() *data.Block {
	colNames := []string{"col1", "col2"}
	colTypes := []column.CHColumnType{"String", "Int32"}
	b, err := data.NewBlock(colNames, colTypes, 2)
	if err != nil {
		panic(err)
	}
	if _, err := b.Columns[0].Data.ReadFromValues([]interface{}{"string1", "string2"}); err != nil {
		panic(err)
	}
	if _, err := b.Columns[1].Data.ReadFromValues([]interface{}{int32(123), int32(456)}); err != nil {
		panic(err)
	}
	return b
}
