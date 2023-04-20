package helper

import (
	"context"
	"fmt"
	"log"
	"runtime/debug"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/bytehouse-cloud/driver-go/driver/lib/data/column"
)

func TestColumnTextsToBlock_1_Block(t *testing.T) {
	ctx := context.Background()
	b := getSampleBlock()
	blockSize := 5

	colTexstStreamer := NewColumnTextsStreamer(b, blockSize, newTestTableReader(5, -1))
	colTextsStream := colTexstStreamer.Start(ctx)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("A runtime panic has occurred with err = [%s],  stacktrace = [%s]\n",
					r,
					string(debug.Stack()))
			}
		}()
		colTexstStreamer.Finish()
	}()

	toBlock := NewColumnTextsToBlock(colTextsStream, b)
	blockOutputStream := toBlock.Start(ctx)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("A runtime panic has occurred with err = [%s],  stacktrace = [%s]\n",
					r,
					string(debug.Stack()))
			}
		}()
		toBlock.Finish()
	}()

	var sb strings.Builder
	for b := range blockOutputStream {
		assert.Equal(t, column.UINT32, b.Columns[0].Type)
		assert.Equal(t, column.STRING, b.Columns[1].Type)
		f := b.NewValuesFrame()
		b.WriteToValues(f)
		sb.WriteString(fmt.Sprintln(f))
	}
	actual := sb.String()
	expected := "[[0 1String] [2 3String] [4 5String] [6 7String] [8 9String]]\n"
	assert.Equal(t, expected, actual)
}

func TestColumnTextsToBlock_100_Rows(t *testing.T) {
	ctx := context.Background()
	b := getSampleBlock()
	blockSize := 5

	colTexstStreamer := NewColumnTextsStreamer(b, blockSize, newTestTableReader(100, -1))
	colTextsStream := colTexstStreamer.Start(ctx)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("A runtime panic has occurred with err = [%s],  stacktrace = [%s]\n",
					r,
					string(debug.Stack()))
			}
		}()
		colTexstStreamer.Finish()
	}()

	toBlock := NewColumnTextsToBlock(colTextsStream, b)
	blockOutputStream := toBlock.Start(ctx)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("A runtime panic has occurred with err = [%s],  stacktrace = [%s]\n",
					r,
					string(debug.Stack()))
			}
		}()
		toBlock.Finish()
	}()

	var sb strings.Builder
	for b := range blockOutputStream {
		assert.Equal(t, column.UINT32, b.Columns[0].Type)
		assert.Equal(t, column.STRING, b.Columns[1].Type)
		f := b.NewValuesFrame()
		b.WriteToValues(f)
		sb.WriteString(fmt.Sprintln(f))
	}
	actual := sb.String()
	expected := "[[0 1String] [2 3String] [4 5String] [6 7String] [8 9String]]\n" +
		"[[10 11String] [12 13String] [14 15String] [16 17String] [18 19String]]\n" +
		"[[20 21String] [22 23String] [24 25String] [26 27String] [28 29String]]\n" +
		"[[30 31String] [32 33String] [34 35String] [36 37String] [38 39String]]\n" +
		"[[40 41String] [42 43String] [44 45String] [46 47String] [48 49String]]\n" +
		"[[50 51String] [52 53String] [54 55String] [56 57String] [58 59String]]\n" +
		"[[60 61String] [62 63String] [64 65String] [66 67String] [68 69String]]\n" +
		"[[70 71String] [72 73String] [74 75String] [76 77String] [78 79String]]\n" +
		"[[80 81String] [82 83String] [84 85String] [86 87String] [88 89String]]\n" +
		"[[90 91String] [92 93String] [94 95String] [96 97String] [98 99String]]\n" +
		"[[100 101String] [102 103String] [104 105String] [106 107String] [108 109String]]\n" +
		"[[110 111String] [112 113String] [114 115String] [116 117String] [118 119String]]\n" +
		"[[120 121String] [122 123String] [124 125String] [126 127String] [128 129String]]\n" +
		"[[130 131String] [132 133String] [134 135String] [136 137String] [138 139String]]\n" +
		"[[140 141String] [142 143String] [144 145String] [146 147String] [148 149String]]\n" +
		"[[150 151String] [152 153String] [154 155String] [156 157String] [158 159String]]\n" +
		"[[160 161String] [162 163String] [164 165String] [166 167String] [168 169String]]\n" +
		"[[170 171String] [172 173String] [174 175String] [176 177String] [178 179String]]\n" +
		"[[180 181String] [182 183String] [184 185String] [186 187String] [188 189String]]\n" +
		"[[190 191String] [192 193String] [194 195String] [196 197String] [198 199String]]\n"
	assert.Equal(t, expected, actual)
}

func TestColumnTextsToBlock_3andHalf_blocks(t *testing.T) {
	ctx := context.Background()
	b := getSampleBlock()
	blockSize := 5

	colTexstStreamer := NewColumnTextsStreamer(b, blockSize, newTestTableReader(16, -1))
	colTextsStream := colTexstStreamer.Start(ctx)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("A runtime panic has occurred with err = [%s],  stacktrace = [%s]\n",
					r,
					string(debug.Stack()))
			}
		}()
		colTexstStreamer.Finish()
	}()

	toBlock := NewColumnTextsToBlock(colTextsStream, b)
	blockOutputStream := toBlock.Start(ctx)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("A runtime panic has occurred with err = [%s],  stacktrace = [%s]\n",
					r,
					string(debug.Stack()))
			}
		}()
		toBlock.Finish()
	}()

	var sb strings.Builder
	for b := range blockOutputStream {
		assert.Equal(t, column.UINT32, b.Columns[0].Type)
		assert.Equal(t, column.STRING, b.Columns[1].Type)
		f := b.NewValuesFrame()
		b.WriteToValues(f)
		sb.WriteString(fmt.Sprintln(f))
	}

	actual := sb.String()
	expected := "[[0 1String] [2 3String] [4 5String] [6 7String] [8 9String]]\n" +
		"[[10 11String] [12 13String] [14 15String] [16 17String] [18 19String]]\n" +
		"[[20 21String] [22 23String] [24 25String] [26 27String] [28 29String]]\n" +
		"[[30 31String]]\n"

	assert.Equal(t, expected, actual)
}

func TestColumnTextsToBlock_3_Rows(t *testing.T) {
	ctx := context.Background()
	b := getSampleBlock()
	blockSize := 5

	colTexstStreamer := NewColumnTextsStreamer(b, blockSize, newTestTableReader(3, -1))
	colTextsStream := colTexstStreamer.Start(ctx)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("A runtime panic has occurred with err = [%s],  stacktrace = [%s]\n",
					r,
					string(debug.Stack()))
			}
		}()
		colTexstStreamer.Finish()
	}()

	toBlock := NewColumnTextsToBlock(colTextsStream, b)
	blockOutputStream := toBlock.Start(ctx)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("A runtime panic has occurred with err = [%s],  stacktrace = [%s]\n",
					r,
					string(debug.Stack()))
			}
		}()
		toBlock.Finish()
	}()

	var sb strings.Builder
	for b := range blockOutputStream {
		assert.Equal(t, column.UINT32, b.Columns[0].Type)
		assert.Equal(t, column.STRING, b.Columns[1].Type)
		f := b.NewValuesFrame()
		b.WriteToValues(f)
		sb.WriteString(fmt.Sprintln(f))
	}

	actual := sb.String()
	expected := "[[0 1String] [2 3String] [4 5String]]\n"

	assert.Equal(t, expected, actual)
}

func TestColumnTextsToBlock_1_Rows(t *testing.T) {
	ctx := context.Background()
	b := getSampleBlock()
	blockSize := 5

	colTexstStreamer := NewColumnTextsStreamer(b, blockSize, newTestTableReader(1, -1))
	colTextsStream := colTexstStreamer.Start(ctx)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("A runtime panic has occurred with err = [%s],  stacktrace = [%s]\n",
					r,
					string(debug.Stack()))
			}
		}()
		colTexstStreamer.Finish()
	}()

	toBlock := NewColumnTextsToBlock(colTextsStream, b)
	blockOutputStream := toBlock.Start(ctx)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("A runtime panic has occurred with err = [%s],  stacktrace = [%s]\n",
					r,
					string(debug.Stack()))
			}
		}()
		toBlock.Finish()
	}()

	var sb strings.Builder
	for b := range blockOutputStream {
		assert.Equal(t, column.UINT32, b.Columns[0].Type)
		assert.Equal(t, column.STRING, b.Columns[1].Type)
		f := b.NewValuesFrame()
		b.WriteToValues(f)
		sb.WriteString(fmt.Sprintln(f))
	}

	actual := sb.String()
	expected := "[[0 1String]]\n"

	assert.Equal(t, expected, actual)
}

func TestColumnTextsToBlock_0_Rows(t *testing.T) {
	ctx := context.Background()
	b := getSampleBlock()
	blockSize := 5

	colTexstStreamer := NewColumnTextsStreamer(b, blockSize, newTestTableReader(0, -1))
	colTextsStream := colTexstStreamer.Start(ctx)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("A runtime panic has occurred with err = [%s],  stacktrace = [%s]\n",
					r,
					string(debug.Stack()))
			}
		}()
		colTexstStreamer.Finish()
	}()

	toBlock := NewColumnTextsToBlock(colTextsStream, b)
	blockOutputStream := toBlock.Start(ctx)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("A runtime panic has occurred with err = [%s],  stacktrace = [%s]\n",
					r,
					string(debug.Stack()))
			}
		}()
		toBlock.Finish()
	}()

	var sb strings.Builder
	for b := range blockOutputStream {
		assert.Equal(t, column.UINT32, b.Columns[0].Type)
		assert.Equal(t, column.STRING, b.Columns[1].Type)
		f := b.NewValuesFrame()
		b.WriteToValues(f)
		sb.WriteString(fmt.Sprintln(f))
	}

	actual := sb.String()
	expected := ""

	assert.Equal(t, expected, actual)
}
