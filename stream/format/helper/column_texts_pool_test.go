package helper

import (
	"fmt"
	"testing"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/stretchr/testify/assert"
)

func TestColumnTextsResults(t *testing.T) {
	numCols := 3
	numRows := 5
	p := NewColumnTextsPool(numCols, numRows)

	fb := bytepool.NewFrameBuffer()
	for i := 0; i < numRows; i++ {
		fb.NewRow()
		for j := 0; j < numCols; j++ {
			fb.NewElem()
			fb.WriteString(fmt.Sprintf("row%v, column%v", i, j))
		}
	}

	expect := [][]string{
		{"row0, column0", "row1, column0", "row2, column0", "row3, column0", "row4, column0"},
		{"row0, column1", "row1, column1", "row2, column1", "row3, column1", "row4, column1"},
		{"row0, column2", "row1, column2", "row2, column2", "row3, column2", "row4, column2"},
	}
	res := p.NewColumnTextsResult(fb)
	assert.Equal(t, expect, res.Get())
	res.Close()

	fb2 := bytepool.NewFrameBuffer()
	res2 := p.NewColumnTextsResult(fb2) // check pool to get back same thing
	assert.Equal(t, expect, res2.colTexts)
	assert.Equal(t, [][]string{{}, {}, {}}, res2.Get()) // return columnTexts with 0 rows
}
