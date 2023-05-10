package bytepool

import (
	"bytes"
	"io"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/bytehouse-cloud/driver-go/utils/pointer"
	"github.com/stretchr/testify/assert"
)

func TestZBuffer_ReadWrite(t *testing.T) {
	zbuf := NewZBuffer(4, 2)
	write, err := zbuf.Write([]byte("12340987"))
	zbuf.Flush()
	assert.Equal(t, 8, write)
	assert.NoError(t, err)

	buf := make([]byte, 4)
	n, err := zbuf.Read(buf)
	assert.Equal(t, 4, n)
	assert.NoError(t, err)
	assert.Equal(t, "1234", string(buf))
	n, err = zbuf.Read(buf)
	assert.Equal(t, 4, n)
	assert.NoError(t, err)
	assert.Equal(t, "0987", string(buf))
}

func TestZBuffer_ReadWriteByte(t *testing.T) {
	zbuf := NewZBuffer(1, 2)
	zbuf.WriteByte(8)
	zbuf.WriteByte(9)
	zbuf.Flush()

	b, err := zbuf.ReadByte()
	assert.Equal(t, uint8(8), b)
	assert.NoError(t, err)
	b, err = zbuf.ReadByte()
	assert.Equal(t, uint8(9), b)
	assert.NoError(t, err)
}

func TestZBuffer_Close(t *testing.T) {
	zbuf := NewZBuffer(4, 1)
	zbuf.Write([]byte("1234"))
	zbuf.Flush()
	zbuf.Read(make([]byte, 4))
	zbuf.Close()
	_, err := zbuf.Read(make([]byte, 1))
	assert.Equal(t, io.EOF, err)
}

func TestZBuffer_WriteTo(t *testing.T) {
	zbuf := NewZBuffer(3, 2)
	go func() {
		zbuf.Write([]byte("1234567890"))
		zbuf.Flush()
		zbuf.Close()
	}()
	var bbuf bytes.Buffer
	zbuf.WriteTo(&bbuf)
	assert.Equal(t, "1234567890", bbuf.String())
}

func TestZBuffer_ReadFrom(t *testing.T) {
	sr := strings.NewReader("123456789")
	zbuf := NewZBuffer(3, 2)
	go func() {
		zbuf.ReadFrom(sr)
		zbuf.Close()
	}()

	buf := make([]byte, 3)
	{
		read, _ := zbuf.Read(buf)
		assert.Equal(t, 3, read)
		assert.Equal(t, "123", string(buf))
	}
	{
		read, _ := zbuf.Read(buf)
		assert.Equal(t, 3, read)
		assert.Equal(t, "456", string(buf))
	}
	{
		read, _ := zbuf.Read(buf)
		assert.Equal(t, 3, read)
		assert.Equal(t, "789", string(buf))
	}
	{
		read, err := zbuf.Read(buf)
		assert.Equal(t, 0, read)
		assert.Equal(t, err, io.EOF)
	}
}

func TestZBuffer_ZWriter(t *testing.T) {
	zbuf := NewZBuffer(4, 2)
	zWriter := NewZWriter(zbuf, 3, 2)
	go func() {
		zWriter.WriteString("123456789")
		zWriter.Flush()
		zWriter.Close()
	}()
	buf := make([]byte, 3)
	{
		read, _ := zbuf.Read(buf)
		assert.Equal(t, 3, read)
		assert.Equal(t, "123", string(buf))
	}
	{
		read, _ := zbuf.Read(buf)
		assert.Equal(t, 3, read)
		assert.Equal(t, "456", string(buf))
	}
	{
		read, _ := zbuf.Read(buf)
		assert.Equal(t, 3, read)
		assert.Equal(t, "789", string(buf))
	}
	{
		read, err := zbuf.Read(buf)
		assert.Equal(t, 0, read)
		assert.Equal(t, err, io.EOF)
	}
}

func TestZBuffer_ZReader(t *testing.T) {
	zbuf := NewZBuffer(4, 2)
	zReader := NewZReader(pointer.IoReader(zbuf), 3, 2)
	go func() {
		zbuf.Write([]byte("1234567890"))
		zbuf.Flush()
		zbuf.Close()
	}()
	all, err := ioutil.ReadAll(zReader)
	assert.NoError(t, err)
	assert.Equal(t, "1234567890", string(all))
}
