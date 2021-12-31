package format

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data/column"
)

func TestCSVBlockStreamFmtReader_BlockStreamFmtRead(t *testing.T) {
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
				Name:           "a\"",
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
		args              args
		wantBlocksRead    int
		wantRowsRead      int
		wantErr           bool
	}{
		{
			name: "Can parse block into block stream, with correct block size for each block",
			blockStreamReader: func() BlockStreamFmtReader {
				b, _ := BlockStreamFmtReaderFactory(Formats[CSV], bytes.NewReader([]byte("1, 2\n1,2\n1,2")), emptySettings)
				return b
			}(),
			args: args{
				ctx:       c,
				sample:    int32Sample,
				blockSize: 3, // Number of rows for each block
			},
			wantBlocksRead: 1,
			wantRowsRead:   3,
		},
		{
			name: "Can parse block into block stream, with correct block size for each block",
			blockStreamReader: func() BlockStreamFmtReader {
				b, _ := BlockStreamFmtReaderFactory(Formats[CSV], bytes.NewReader([]byte("1, 2\n1,2\n1,2")), emptySettings)
				return b
			}(),
			args: args{
				ctx:       c,
				sample:    int32Sample,
				blockSize: 1,
			},
			wantBlocksRead: 3,
			wantRowsRead:   3,
		},
		{
			name: "Can quoted csv into block streams",
			blockStreamReader: func() BlockStreamFmtReader {
				b, _ := BlockStreamFmtReaderFactory(Formats[CSV], bytes.NewReader([]byte("'1', '2'\n'1','2'\n'1','2'")), emptySettings)
				return b
			}(),
			args: args{
				ctx:       c,
				sample:    int32Sample,
				blockSize: 1,
			},
			wantBlocksRead: 3,
			wantRowsRead:   3,
		},
		{
			name: "Can quoted csv into block streams with custom delimiter",
			blockStreamReader: func() BlockStreamFmtReader {
				b, _ := BlockStreamFmtReaderFactory(Formats[CSV], bytes.NewReader([]byte("'1'| '2'\n'1'|'2'\n'1'|'2'")), map[string]interface{}{
					csvDelimiterSetting: "|",
				})
				return b
			}(),
			args: args{
				ctx:       c,
				sample:    int32Sample,
				blockSize: 1,
			},
			wantBlocksRead: 3,
			wantRowsRead:   3,
		},
		{
			name: "Can read quoted string csv into block streams",
			blockStreamReader: func() BlockStreamFmtReader {
				b, _ := BlockStreamFmtReaderFactory(Formats[CSV], bytes.NewReader([]byte("'1', '2'\n'1','2'\n'1','2'")), emptySettings)
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
			name: "Can read quoted and non quoted string csv into block streams",
			blockStreamReader: func() BlockStreamFmtReader {
				b, _ := BlockStreamFmtReaderFactory(Formats[CSV], bytes.NewReader([]byte("'1', '2'\n'1','2'\n'''1''',2")), emptySettings)
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
			name: "Can read quoted string with escaped items csv into block streams",
			blockStreamReader: func() BlockStreamFmtReader {
				b, _ := BlockStreamFmtReaderFactory(Formats[CSV], bytes.NewReader([]byte(`"1","2/"\n"2","3\u123\/few"\n"\b\n\r\t\v\a\0",4\"`)), emptySettings)
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
			name: "Can read non-quoted string csv into block streams",
			blockStreamReader: func() BlockStreamFmtReader {
				b, _ := BlockStreamFmtReaderFactory(Formats[CSV], bytes.NewReader([]byte("1, 2\n1,2\n1,2")), emptySettings)
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
			name: "Can read csv with names",
			blockStreamReader: func() BlockStreamFmtReader {
				b, _ := BlockStreamFmtReaderFactory(Formats[CSVWITHNAMES], bytes.NewReader([]byte("jack, ma\n1, 2\n1,2\n1,2")), emptySettings)
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
			name: "Should not read any blocks if csv format wrong",
			blockStreamReader: func() BlockStreamFmtReader {
				b, _ := BlockStreamFmtReaderFactory(Formats[CSV], bytes.NewReader([]byte("1")), emptySettings)
				return b
			}(),
			args: args{
				ctx:       c,
				sample:    int32Sample,
				blockSize: 1,
			},
			wantErr:        true,
			wantBlocksRead: 0,
		},
		{
			name: "Should not read subsequent blocks if csv format wrong",
			blockStreamReader: func() BlockStreamFmtReader {
				b, _ := BlockStreamFmtReaderFactory(Formats[CSV], bytes.NewReader([]byte("1, 2\n1")), emptySettings)
				return b
			}(),
			args: args{
				ctx:       c,
				sample:    int32Sample,
				blockSize: 1,
			},
			wantErr:        true,
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

func Test_resolveCSVDelim(t *testing.T) {
	tests := []struct {
		name          string
		givenSettings map[string]interface{}
		want          byte
		wantErr       bool
	}{
		{
			name: "if no key then comma",
			want: ',',
		},
		{
			name:          "if empty string then error",
			givenSettings: map[string]interface{}{csvDelimiterSetting: ""},
			wantErr:       true,
		},
		{
			name:          "if empty string then error",
			givenSettings: map[string]interface{}{csvDelimiterSetting: ""},
			wantErr:       true,
		},
		{
			name:          "if string of 1 byte then return that byte",
			givenSettings: map[string]interface{}{csvDelimiterSetting: "a"},
			want:          'a',
		},
		{
			name:          "if string of 2 byte no backslash then error",
			givenSettings: map[string]interface{}{csvDelimiterSetting: "aa"},
			wantErr:       true,
		},
		{
			name:          "if string of 2 byte with backslash then return 2nd byte",
			givenSettings: map[string]interface{}{csvDelimiterSetting: "\\u"},
			want:          'u',
		},
		{
			name:          "if string of 2 byte with backslash and special char return special",
			givenSettings: map[string]interface{}{csvDelimiterSetting: "\\a"},
			want:          '\a',
		},
		{
			name:          "if string of 3 bytes then then error",
			givenSettings: map[string]interface{}{csvDelimiterSetting: "\\aa"},
			wantErr:       true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveCSVDelim(tt.givenSettings)
			if (err != nil) != tt.wantErr {
				t.Errorf("resolveCSVDelim() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("resolveCSVDelim() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCSVBlockStreamFmtReader_ArrayRead(t *testing.T) {
	arrBlock := makeArrayBlock()
	input := bytes.NewReader([]byte(`['hello', world], [1, 2, 3]
["foo", bar], [4,5,6]
[baz], [7,8,9]`))
	r, err := NewCSVBlockStreamFmtReader(input, false, nil)
	require.NoError(t, err)
	blockStream, yield := r.BlockStreamFmtRead(context.Background(), arrBlock, 5)
	_, err = yield()
	require.NoError(t, err)

	var output bytes.Buffer
	p := NewPrettyBlockStreamFmtWriter(&output)
	p.BlockStreamFmtWrite(blockStream)
	_, err = p.Yield()
	require.NoError(t, err)
	require.Equal(t, output.String(), `┌─strArr─────────────┬─intArr────┐
│ ['hello', 'world'] │ [1, 2, 3] │
│ ['foo', 'bar']     │ [4, 5, 6] │
│ ['baz']            │ [7, 8, 9] │
└────────────────────┴───────────┘
`)
}

func makeArrayBlock() *data.Block {
	numRows := 3
	colNames := []string{"strArr", "intArr"}
	colTypes := []column.CHColumnType{"Array(String)", "Array(Int32)"}
	block, err := data.NewBlock(colNames, colTypes, numRows)
	if err != nil {
		panic(err)
	}
	if _, _, err := block.ReadFromColumnValues([][]interface{}{
		{
			[]string{"hello", "world"}, []string{"foo", "bar"}, []string{"baz"},
		},
		{
			[]int32{1, 2, 3}, []int32{4, 5, 6}, []int32{7, 8, 9},
		},
	}); err != nil {
		panic(err)
	}
	return block
}
