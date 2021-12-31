package helper

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data/column"
	"github.com/stretchr/testify/assert"
)

var delibrateError = errors.New("error made on purpose")

func TestColumnTextsStreamer_1_block(t *testing.T) {
	b := getSampleBlock()
	blockSize := 5
	streamer := NewColumnTextsStreamer(b, blockSize, newTestTableReader(5, -1))
	outStream := streamer.Start(context.Background())
	go streamer.Finish()

	res, ok := <-outStream
	assert.True(t, ok)
	assert.Equal(t, [][]string{
		{"0", "2", "4", "6", "8"},
		{"1String", "3String", "5String", "7String", "9String"},
	}, res.Get())

	res, ok = <-outStream
	assert.False(t, ok)
}

func TestColumnTextsStreamer_2_block(t *testing.T) {
	b := getSampleBlock()
	blockSize := 5
	streamer := NewColumnTextsStreamer(
		b, blockSize, newTestTableReader(10, -1),
	)
	outStream := streamer.Start(context.Background())
	go streamer.Finish()

	// first block get
	res, ok := <-outStream
	assert.True(t, ok)
	assert.Equal(t, [][]string{
		{"0", "2", "4", "6", "8"},
		{"1String", "3String", "5String", "7String", "9String"},
	}, res.Get())

	// second block get
	res, ok = <-outStream
	assert.True(t, ok)
	assert.Equal(t, [][]string{
		{"10", "12", "14", "16", "18"},
		{"11String", "13String", "15String", "17String", "19String"},
	}, res.Get())

	res, ok = <-outStream
	assert.False(t, ok)
}

func TestColumnTextsStreamer_Partial_Block(t *testing.T) {
	b := getSampleBlock()
	blockSize := 5
	streamer := NewColumnTextsStreamer(
		b, blockSize, newTestTableReader(3, -1),
	)
	outStream := streamer.Start(context.Background())
	go streamer.Finish()

	// first block get
	res, ok := <-outStream
	assert.True(t, ok)
	assert.Equal(t, [][]string{
		{"0", "2", "4"},
		{"1String", "3String", "5String"},
	}, res.Get())

	res, ok = <-outStream
	assert.False(t, ok)
}

func TestColumnTextsStreamer_1_Full_Then_Partial_Block(t *testing.T) {
	b := getSampleBlock()
	blockSize := 5
	streamer := NewColumnTextsStreamer(
		b, blockSize, newTestTableReader(8, -1),
	)
	outStream := streamer.Start(context.Background())
	go streamer.Finish()

	// full block
	res, ok := <-outStream
	assert.True(t, ok)
	assert.Equal(t, [][]string{
		{"0", "2", "4", "6", "8"},
		{"1String", "3String", "5String", "7String", "9String"},
	}, res.Get())

	// partial block
	res, ok = <-outStream
	assert.True(t, ok)
	assert.Equal(t, [][]string{
		{"10", "12", "14"},
		{"11String", "13String", "15String"},
	}, res.Get())

	res, ok = <-outStream
	assert.False(t, ok)
}

func TestColumnTextsStreamer_No_Rows(t *testing.T) {
	b := getSampleBlock()
	blockSize := 5
	streamer := NewColumnTextsStreamer(
		b, blockSize, newTestTableReader(0, -1),
	)
	outStream := streamer.Start(context.Background())
	go streamer.Finish()

	_, ok := <-outStream
	assert.False(t, ok)
}

func TestColumnTextsStreamer_Error_at_start(t *testing.T) {
	b := getSampleBlock()
	blockSize := 5
	streamer := NewColumnTextsStreamer(
		b, blockSize, newTestTableReader(100, 0),
	)
	outStream := streamer.Start(context.Background())

	go func() {
		for range outStream {
		}
	}()

	n, err := streamer.Finish()
	if assert.Equal(t, n, 0) {
		assert.True(t, strings.Contains(err.Error(), delibrateError.Error()))
	}
}

func TestColumnTextsStreamer_Error_at_first_row(t *testing.T) {
	b := getSampleBlock()
	blockSize := 5
	streamer := NewColumnTextsStreamer(
		b, blockSize, newTestTableReader(100, 1),
	)
	outStream := streamer.Start(context.Background())

	go func() {
		for range outStream {
		}
	}()

	n, err := streamer.Finish()
	if assert.Equal(t, n, 0) {
		assert.True(t, strings.Contains(err.Error(), delibrateError.Error()))
		assert.True(t, strings.Contains(err.Error(), "1"))
	}
}

func TestColumnTextsStreamer_Error_10th_elem(t *testing.T) {
	b := getSampleBlock()
	blockSize := 5
	streamer := NewColumnTextsStreamer(
		b, blockSize, newTestTableReader(100, 10),
	)
	outStream := streamer.Start(context.Background())

	go func() {
		for range outStream {
		}
	}()

	rowsRead, err := streamer.Finish()
	if assert.Equal(t, 5, rowsRead) {
		assert.True(t, strings.Contains(err.Error(), delibrateError.Error()))
		assert.True(t, strings.Contains(err.Error(), "0"))
	}
}

func TestColumnTextsStreamer_Error_13th_elem(t *testing.T) {
	b := getSampleBlock()
	blockSize := 5
	streamer := NewColumnTextsStreamer(
		b, blockSize, newTestTableReader(100, 13),
	)
	outStream := streamer.Start(context.Background())

	go func() {
		for range outStream {
		}
	}()

	rowsRead, err := streamer.Finish()
	if assert.Equal(t, 6, rowsRead) {
		assert.True(t, strings.Contains(err.Error(), delibrateError.Error()))
		assert.True(t, strings.Contains(err.Error(), "1"))
	}
}

func getSampleBlock() *data.Block {
	colNames := []string{"col_1", "col_2"}
	colTypes := []column.CHColumnType{
		column.UINT32,
		column.STRING,
	}
	b, err := data.NewBlock(colNames, colTypes, 0)
	if err != nil {
		panic(err)
	}
	return b
}

type testTableReader struct {
	rowsLeft int
	errorIdx int
	elemIdx  int
}

func newTestTableReader(rowsLeft, errorAt int) *testTableReader {
	return &testTableReader{
		rowsLeft: rowsLeft,
		errorIdx: errorAt,
	}
}

func (t *testTableReader) ReadFirstColumnTexts(fb *bytepool.FrameBuffer, numRows int, cols []*column.CHColumn) (int, error) {
	return ReadFirstColumnTexts(fb, numRows, cols, t)
}

func (t *testTableReader) ReadColumnTextsCont(fb *bytepool.FrameBuffer, numRows int, cols []*column.CHColumn) (int, error) {
	return ReadColumnTextsCont(fb, numRows, cols, t)
}

func (t *testTableReader) ReadFirstRow(fb *bytepool.FrameBuffer, cols []*column.CHColumn) error {
	return t.ReadRowCont(fb, cols)
}

func (t *testTableReader) ReadRowCont(fb *bytepool.FrameBuffer, cols []*column.CHColumn) error {
	if t.rowsLeft == 0 {
		return io.EOF
	}
	t.rowsLeft--
	return ReadRow(fb, cols, t)
}

func (t *testTableReader) ReadElem(fb *bytepool.FrameBuffer, cols []*column.CHColumn, idx int) error {
	if t.errorIdx == t.elemIdx {
		return delibrateError
	}

	switch cols[idx].Type {
	case column.STRING:
		fb.WriteString(fmt.Sprintf("%vString", t.elemIdx))
	case column.UINT32:
		fb.WriteString(fmt.Sprintf("%v", t.elemIdx))
	}
	t.elemIdx++
	return nil
}
