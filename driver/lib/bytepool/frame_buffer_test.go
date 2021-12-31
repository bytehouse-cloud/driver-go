package bytepool

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFrameBuffer_Empty(t *testing.T) {
	fb := NewFrameBuffer()
	res := make2x2Frame()
	n, elems_read := fb.Export(res)
	assert.Equal(t, n, 0)
	assert.Equal(t, elems_read, []int{})

	fb.NewRow()
	n, elems_read = fb.Export(res)
	assert.Equal(t, 1, n)
	assert.Equal(t, []int{0}, elems_read)
}

func TestFrameBuffer_Export(t *testing.T) {
	fb := NewFrameBuffer()
	res := make2x2Frame()

	fb.NewRow()
	fb.NewElem()
	fb.WriteString("hello")
	fb.NewElem()
	fb.WriteString("world")

	fb.NewRow()
	fb.NewElem()
	fb.WriteString("foo")
	fb.NewElem()
	fb.WriteString("bar")
	fb.NewElem()
	fb.WriteString("baz")

	n, elems_read := fb.Export(res)
	assert.Equal(t, n, 2)
	assert.Equal(t, elems_read, []int{2, 3})
	assert.Equal(t, res, [][]string{
		{"hello", "world"},
		{"foo", "bar", "baz"},
	})
}

func TestFrameBuffer_ReadColumnTexts(t *testing.T) {
	fb := NewFrameBuffer()
	res := make2x2Frame()

	fb.NewRow()
	fb.NewElem()
	fb.WriteString("hello")
	fb.NewElem()
	fb.WriteString("world")

	fb.NewRow()
	fb.NewElem()
	fb.WriteString("foo")
	fb.NewElem()
	fb.WriteString("bar")
	fb.NewElem()
	fb.WriteString("baz")

	n, elems_read := fb.ReadColumnTexts(res)
	assert.Equal(t, n, 2)
	assert.Equal(t, elems_read, []int{2, 3})
	assert.Equal(t, res, [][]string{
		{"hello", "foo"},
		{"world", "bar"},
	})
}

func TestFrameBuffer_DiscardCurrentRow(t *testing.T) {
	fb := NewFrameBuffer()
	res := make2x2Frame()

	fb.NewRow()
	fb.NewElem()
	fb.WriteString("hello")
	fb.NewElem()
	fb.WriteString("world")

	fb.NewRow()
	fb.NewElem()
	fb.WriteString("foo")
	fb.NewElem()
	fb.WriteString("bar")

	fb.DiscardCurrentRow()
	n, elems_read := fb.ReadColumnTexts(res)
	assert.Equal(t, n, 1)
	assert.Equal(t, elems_read, []int{2})
	assert.Equal(t, res, [][]string{
		{"hello", ""},
		{"world", ""},
	})

	fb.DiscardCurrentRow()
	res = make2x2Frame()
	n, elems_read = fb.ReadColumnTexts(res)
	assert.Equal(t, n, 0)
	assert.Equal(t, elems_read, []int{})
	assert.Equal(t, res, [][]string{
		{"", ""},
		{"", ""},
	})

	fb.DiscardCurrentRow()
	res = make2x2Frame()
	n, elems_read = fb.ReadColumnTexts(res)
	assert.Equal(t, n, 0)
	assert.Equal(t, elems_read, []int{})
	assert.Equal(t, res, [][]string{
		{"", ""},
		{"", ""},
	})
}

func TestFrameBuffer_DiscardRightAfterNewRow(t *testing.T) {
	fb := NewFrameBuffer()
	res := make2x2Frame()

	fb.NewRow()
	fb.NewElem()
	fb.WriteString("hello")
	fb.NewElem()
	fb.WriteString("world")

	fb.NewRow()
	fb.NewElem()
	fb.WriteString("foo")
	fb.NewElem()
	fb.WriteString("bar")

	fb.NewRow()
	fb.DiscardCurrentRow()

	n, elems_read := fb.ReadColumnTexts(res)
	assert.Equal(t, n, 2)
	assert.Equal(t, elems_read, []int{2, 2})
	assert.Equal(t, res, [][]string{
		{"hello", "foo"},
		{"world", "bar"},
	})
}

func make2x2Frame() [][]string {
	res := make([][]string, 2)
	for i := range res {
		res[i] = make([]string, 2)
	}
	return res
}
