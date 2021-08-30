package ch_encoding

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncoderDecoder(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "Test Write/Read Bytes",
			test: func(t *testing.T) {
				var buffer bytes.Buffer

				encoder := NewEncoder(&buffer)
				decoder := NewDecoder(&buffer)

				data := []byte("Hello")
				b := make([]byte, len(data))

				n, err := encoder.Write(data)
				require.NoError(t, err)
				require.Equal(t, len(data), n)

				require.NoError(t, encoder.Flush())

				n, err = decoder.Read(b)
				require.NoError(t, err)
				require.Equal(t, len(data), n)

				require.Equal(t, data, b)
			},
		},
		{
			name: "Test Write/Read Bytes Compressed",
			test: func(t *testing.T) {
				var buffer bytes.Buffer

				encoder := NewEncoderWithCompress(&buffer)
				encoder.SelectCompress(true)

				decoder := NewDecoderWithCompress(&buffer)
				decoder.SetCompress(true)

				data := []byte("Hello")
				b := make([]byte, len(data))

				n, err := encoder.Write(data)
				require.NoError(t, err)
				require.Equal(t, len(data), n)

				require.NoError(t, encoder.Flush())

				n, err = decoder.Read(b)
				require.NoError(t, err)
				require.Equal(t, len(data), n)

				require.Equal(t, data, b)
			},
		},
		{
			name: "Test Write/Read Bytes Select Compressed",
			test: func(t *testing.T) {
				var buffer bytes.Buffer

				encoder := NewEncoderWithCompress(&buffer)
				encoder.SelectCompress(true)

				decoder := NewDecoderWithCompress(&buffer)
				decoder.SetCompress(true)

				data := []byte("Hello")
				b := make([]byte, len(data))

				n, err := encoder.Write(data)
				require.NoError(t, err)
				require.Equal(t, len(data), n)

				require.NoError(t, encoder.Flush())

				n, err = decoder.Read(b)
				require.NoError(t, err)
				require.Equal(t, len(data), n)

				require.Equal(t, data, b)
			},
		},
		{
			name: "Test Read/Write UInt64",
			test: func(t *testing.T) {
				var buffer bytes.Buffer

				encoder := NewEncoder(&buffer)
				decoder := NewDecoder(&buffer)

				data := uint64(100)

				err := encoder.UInt64(data)
				require.NoError(t, err)

				require.NoError(t, encoder.Flush())

				n, err := decoder.UInt64()
				require.NoError(t, err)
				require.Equal(t, data, n)
			},
		},
		{
			name: "Test Read/Write UInt64 Compressed",
			test: func(t *testing.T) {
				var buffer bytes.Buffer

				encoder := NewEncoderWithCompress(&buffer)
				encoder.SelectCompress(true)

				decoder := NewDecoderWithCompress(&buffer)
				decoder.SetCompress(true)

				data := uint64(100)

				err := encoder.UInt64(data)
				require.NoError(t, err)

				require.NoError(t, encoder.Flush())

				n, err := decoder.UInt64()
				require.NoError(t, err)
				require.Equal(t, data, n)
			},
		},
		{
			name: "Test Read/Write UInt32",
			test: func(t *testing.T) {
				var buffer bytes.Buffer

				encoder := NewEncoder(&buffer)
				decoder := NewDecoder(&buffer)

				data := uint32(100)

				err := encoder.UInt32(data)
				require.NoError(t, err)

				require.NoError(t, encoder.Flush())

				n, err := decoder.UInt32()
				require.NoError(t, err)
				require.Equal(t, data, n)
			},
		},
		{
			name: "Test Read/Write Int32",
			test: func(t *testing.T) {
				var buffer bytes.Buffer

				encoder := NewEncoder(&buffer)
				decoder := NewDecoder(&buffer)

				data := int32(100)

				err := encoder.Int32(data)
				require.NoError(t, err)

				require.NoError(t, encoder.Flush())

				n, err := decoder.Int32()
				require.NoError(t, err)
				require.Equal(t, data, n)
			},
		},
		{
			name: "Test Read/Write String",
			test: func(t *testing.T) {
				var buffer bytes.Buffer

				encoder := NewEncoder(&buffer)
				decoder := NewDecoder(&buffer)

				data := "hello world"

				err := encoder.String(data)
				require.NoError(t, err)

				require.NoError(t, encoder.Flush())

				n, err := decoder.String()
				require.NoError(t, err)
				require.Equal(t, data, n)
			},
		},
		{
			name: "Test Read/Write Bool",
			test: func(t *testing.T) {
				var buffer bytes.Buffer

				encoder := NewEncoder(&buffer)
				decoder := NewDecoder(&buffer)

				data := true

				err := encoder.Bool(data)
				require.NoError(t, err)

				require.NoError(t, encoder.Flush())

				n, err := decoder.Bool()
				require.NoError(t, err)
				require.Equal(t, data, n)
			},
		},
		{
			name: "Test Read/Write UVarInt",
			test: func(t *testing.T) {
				var buffer bytes.Buffer

				encoder := NewEncoder(&buffer)
				decoder := NewDecoder(&buffer)

				data := uint64(100)

				err := encoder.Uvarint(data)
				require.NoError(t, err)

				require.NoError(t, encoder.Flush())

				n, err := decoder.Uvarint()
				require.NoError(t, err)
				require.Equal(t, data, n)
			},
		},
		{
			name: "Test Read/Write UVarInt Compressed",
			test: func(t *testing.T) {
				var buffer bytes.Buffer

				encoder := NewEncoderWithCompress(&buffer)
				decoder := NewDecoderWithCompress(&buffer)

				encoder.SelectCompress(true)
				decoder.SetCompress(true)

				data := uint64(100)

				err := encoder.Uvarint(data)
				require.NoError(t, err)

				require.NoError(t, encoder.Flush())

				n, err := decoder.Uvarint()
				require.NoError(t, err)
				require.Equal(t, data, n)
			},
		},
		{
			name: "Test Write Float64",
			test: func(t *testing.T) {
				buffer := bytes.NewBuffer([]byte(""))
				encoder := NewEncoder(bufio.NewWriter(buffer))
				require.Equal(t, encoder.IsCompressed(), false)

				writeData := float64(-100)
				err := encoder.Float64(writeData)
				require.NoError(t, err)

				err = encoder.Flush()
				require.NoError(t, err)

				require.Equal(t, writeData, math.Float64frombits(binary.LittleEndian.Uint64(buffer.Bytes())))
			},
		},
		{
			name: "Test Write Float32",
			test: func(t *testing.T) {
				buffer := bytes.NewBuffer([]byte(""))
				encoder := NewEncoder(bufio.NewWriter(buffer))
				require.Equal(t, encoder.IsCompressed(), false)

				writeData := float32(-100)
				err := encoder.Float32(writeData)
				require.NoError(t, err)

				err = encoder.Flush()
				require.NoError(t, err)

				require.Equal(t, writeData, math.Float32frombits(binary.LittleEndian.Uint32(buffer.Bytes())))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}
