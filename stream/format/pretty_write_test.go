package format

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data/column"
)

func Test_countMaxLenForEachCol(t *testing.T) {
	tests := []struct {
		name       string
		givenCols  []*column.CHColumn
		givenFrame [][]string
		wantLens   []int
	}{
		{
			name: "given no data take cols len",
			givenCols: []*column.CHColumn{
				{Name: "String"}, {Name: "Int32"},
			},
			wantLens: []int{len("String"), len("Int32")},
		},
		{
			name: "given some data greater than col name then take the len of col data",
			givenCols: []*column.CHColumn{
				{Name: "String"}, {Name: "Int32"},
			},
			givenFrame: [][]string{{
				"Hello World", "2",
			}},
			wantLens: []int{len("Hello World"), len("Int32")},
		},
		{
			name: "given data of varying len take the largest",
			givenCols: []*column.CHColumn{
				{Name: "String"}, {Name: "Int32"},
			},
			givenFrame: [][]string{
				{"Hello World", "2"},
				{"Hello", "123456789"},
				{"World", "8"},
			},
			wantLens: []int{len("Hello World"), len("123456789")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := countMaxLenForEachCol(tt.givenCols, tt.givenFrame, nil); !reflect.DeepEqual(got, tt.wantLens) {
				t.Errorf("countMaxLenForEachCol() = %v, want %v", got, tt.wantLens)
			}
		})
	}
}

func Test_getColNames(t *testing.T) {
	tests := []struct {
		name      string
		givenCols []*column.CHColumn
		want      []string
	}{
		{
			name: "given cols then column names",
			givenCols: []*column.CHColumn{
				{Name: "String"}, {Name: "Int32"},
			},
			want: []string{"String", "Int32"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getColNames(tt.givenCols); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getColNames() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_PrettyWrite(t *testing.T) {

	tests := []struct {
		name        string
		buf         bytes.Buffer
		givenBlocks []*data.Block
		wantString  string
	}{
		{
			name: "give no data then blank written",
			buf:  bytes.Buffer{},
		},
		{
			name: "given 1 block then write 1 box",
			buf:  bytes.Buffer{},
			givenBlocks: []*data.Block{
				makeTestBlock(),
			},
			wantString: `┌─col1──┬─col2─┐
│ hello │ 1    │
│ world │ 2    │
└───────┴──────┘
`,
		},
		{
			name: "given 2 blocks then write 2 box",
			buf:  bytes.Buffer{},
			givenBlocks: []*data.Block{
				makeTestBlock(),
				makeTestBlock(),
			},
			wantString: `┌─col1──┬─col2─┐
│ hello │ 1    │
│ world │ 2    │
└───────┴──────┘
┌─col1──┬─col2─┐
│ hello │ 1    │
│ world │ 2    │
└───────┴──────┘
`,
		},
		{
			name: "given special characters then write properly",
			buf:  bytes.Buffer{},
			givenBlocks: []*data.Block{
				makeTestBlockSpecialChar(),
			},
			wantString: `┌─'你好'─┬─col2─┐
│ 你好   │ 1    │
│ 你好   │ 2    │
└────────┴──────┘
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fmtWriter := NewPrettyBlockStreamFmtWriter(&tt.buf)
			fmtWriter.BlockStreamFmtWrite(toBlockStream(tt.givenBlocks))
			_, err := fmtWriter.Yield()
			if !assert.Nil(t, err) {
				return
			}
			if !assert.Equal(t, tt.wantString, tt.buf.String()) {
				t.Log(tt.buf.String())
			}
		})
	}

}

func toBlockStream(blocks []*data.Block) <-chan *data.Block {
	bs := make(chan *data.Block, 1)
	go func() {
		defer close(bs)
		for _, b := range blocks {
			bs <- b
		}
	}()

	return bs
}

func makeTestBlock() *data.Block {
	colNames := []string{"col1", "col2"}
	colTypes := []column.CHColumnType{"String", "Int32"}

	b, err := data.NewBlock(colNames, colTypes, 2)
	if err != nil {
		panic(err)
	}
	_, err = b.Columns[0].Data.ReadFromValues([]interface{}{"hello", "world"})
	if err != nil {
		panic(err)
	}
	_, err = b.Columns[1].Data.ReadFromValues([]interface{}{int32(1), int32(2)})
	if err != nil {
		panic(err)
	}

	return b
}

func makeTestBlockSpecialChar() *data.Block {
	colNames := []string{"'你好'", "col2"}
	colTypes := []column.CHColumnType{"String", "Int32"}

	b, err := data.NewBlock(colNames, colTypes, 2)
	if err != nil {
		panic(err)
	}
	_, err = b.Columns[0].Data.ReadFromValues([]interface{}{"你好", "你好"})
	if err != nil {
		panic(err)
	}
	_, err = b.Columns[1].Data.ReadFromValues([]interface{}{int32(1), int32(2)})
	if err != nil {
		panic(err)
	}

	return b
}
