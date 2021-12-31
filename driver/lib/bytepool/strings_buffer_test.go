package bytepool

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStringsBuffer_Empty(t *testing.T) {
	sb := NewStringsBuffer()
	result := make([]string, 2)
	n := sb.ExportTo(result)
	assert.Equal(t, n, 0)
	assert.Equal(t, result, []string{
		"",
		"",
	})

	sb.NewElem()
	n = sb.ExportTo(result)
	assert.Equal(t, n, 1)
	assert.Equal(t, result, []string{
		"",
		"",
	})
}

func TestStringsBuffer_WriteAndExport(t *testing.T) {
	sb := NewStringsBuffer()
	sb.NewElem()
	sb.Write([]byte("hello"))
	sb.Write([]byte("world"))
	sb.NewElem()
	sb.Write([]byte("foo"))
	sb.Write([]byte("bar"))

	result := make([]string, 2)
	n := sb.ExportTo(result)

	assert.Equal(t, n, 2)
	assert.Equal(t, result, []string{
		"helloworld",
		"foobar",
	})
}

func TestStringsBuffer_WriteStringAndExport(t *testing.T) {
	sb := NewStringsBuffer()
	sb.NewElem()
	sb.WriteString("hello")
	sb.NewElem()
	sb.WriteString("world")

	result := make([]string, 2)
	n := sb.ExportTo(result)

	assert.Equal(t, n, 2)
	assert.Equal(t, result, []string{
		"hello",
		"world",
	})
}

func TestStringsBuffer_WriteByteAndExport(t *testing.T) {
	sb := NewStringsBuffer()
	sb.NewElem()
	sb.WriteByte('a')
	sb.WriteByte('b')
	sb.NewElem()
	sb.WriteByte('c')
	sb.WriteByte('d')

	result := make([]string, 2)
	n := sb.ExportTo(result)

	assert.Equal(t, n, 2)
	assert.Equal(t, result, []string{
		"ab",
		"cd",
	})
}

func TestStringsBuffer_ExportLimted(t *testing.T) {
	sb := NewStringsBuffer()
	sb.NewElem()
	sb.WriteByte('a')
	sb.NewElem()
	sb.WriteByte('b')
	sb.NewElem()
	sb.WriteByte('c')

	result := make([]string, 2)
	n := sb.ExportTo(result)

	assert.Equal(t, n, 2)
	assert.Equal(t, result, []string{
		"a",
		"b",
	})

	result = make([]string, 3)
	n = sb.ExportTo(result)
	assert.Equal(t, n, 3)
	assert.Equal(t, result, []string{
		"a",
		"b",
		"c",
	})

	result = make([]string, 4)
	n = sb.ExportTo(result)
	assert.Equal(t, n, 3)
	assert.Equal(t, result, []string{
		"a",
		"b",
		"c",
		"",
	})
}

func TestStringsBuffer_Truncate(t *testing.T) {
	sb := NewStringsBuffer()
	sb.NewElem()
	sb.WriteString("hello")
	sb.NewElem()
	sb.WriteString("world")
	sb.NewElem()
	sb.WriteString("foo")
	sb.NewElem()
	sb.WriteString("bar")

	sb.TruncateElem(2)
	result := make([]string, 4)
	n := sb.ExportTo(result)
	assert.Equal(t, n, 2)
	assert.Equal(t, []string{
		"hello",
		"world",
		"",
		"",
	}, result)

	sb.TruncateElem(2)
	result = make([]string, 4)
	n = sb.ExportTo(result)
	assert.Equal(t, n, 2)
	assert.Equal(t, []string{
		"hello",
		"world",
		"",
		"",
	}, result)

	sb.TruncateElem(0)
	result = make([]string, 4)
	n = sb.ExportTo(result)
	assert.Equal(t, n, 0)
	assert.Equal(t, []string{
		"",
		"",
		"",
		"",
	}, result)
}

func TestStringsBuffer_SameBufferFromPool(t *testing.T) {
	sb := NewStringsBuffer()
	sb.Close()

	sb2 := NewStringsBuffer()
	sb.NewElem()
	sb.WriteByte('a')
	sb2.WriteByte('b')

	result := make([]string, 1)
	n := sb.ExportTo(result)
	assert.Equal(t, n, 1)
	assert.Equal(t, result, []string{
		"ab",
	})
}
