package ch_encoding

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/bytehouse-cloud/driver-go/utils/pointer"
	"github.com/stretchr/testify/require"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
)

func TestNewDecoder(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "Test Read",
			test: func(t *testing.T) {
				decoder := NewDecoder(bytepool.NewZReader(pointer.IoReader(bytes.NewReader([]byte("5aabcd"))), 100, 100))
				require.Equal(t, decoder.IsCompressed(), false)

				readBytes := make([]byte, 4)
				n, err := decoder.Read(readBytes)
				require.NoError(t, err)
				require.Equal(t, n, 4)
				require.Equal(t, readBytes, []byte("5aab"))
			},
		},
		{
			name: "Test Read UVarint",
			test: func(t *testing.T) {
				decoder := NewDecoder(bytepool.NewZReader(pointer.IoReader(bytes.NewReader([]byte("5aabcd"))), 100, 100))
				require.Equal(t, decoder.IsCompressed(), false)

				n, err := decoder.Uvarint() // Get uvarint ascii code for first char
				require.NoError(t, err)
				require.Equal(t, uint64(53), n) // 53 is ascii code for 5
			},
		},
		{
			name: "Test Read UInt64",
			test: func(t *testing.T) {
				b := make([]byte, 8)
				binary.LittleEndian.PutUint64(b, 10)
				decoder := NewDecoder(bytepool.NewZReader(pointer.IoReader(bytes.NewReader(b)), 100, 100))
				require.Equal(t, decoder.IsCompressed(), false)

				n, err := decoder.UInt64()
				require.NoError(t, err)
				require.Equal(t, uint64(10), n)
			},
		},
		{
			name: "Test Read UInt32",
			test: func(t *testing.T) {
				b := make([]byte, 8)
				binary.LittleEndian.PutUint32(b, 10)
				decoder := NewDecoder(bytepool.NewZReader(pointer.IoReader(bytes.NewReader(b)), 100, 100))
				require.Equal(t, decoder.IsCompressed(), false)

				n, err := decoder.UInt32()
				require.NoError(t, err)
				require.Equal(t, uint32(10), n)
			},
		},
		{
			name: "Test Read Int32",
			test: func(t *testing.T) {
				b := make([]byte, 8)
				binary.LittleEndian.PutUint32(b, 10)
				decoder := NewDecoder(bytepool.NewZReader(pointer.IoReader(bytes.NewReader(b)), 100, 100))
				require.Equal(t, decoder.IsCompressed(), false)

				n, err := decoder.Int32()
				require.NoError(t, err)
				require.Equal(t, int32(10), n)
			},
		},
		{
			name: "Test Read String",
			test: func(t *testing.T) {
				b := make([]byte, 4)
				b[0] = 3
				for i := 1; i < len(b); i++ {
					b[i] = 'h'
				}

				decoder := NewDecoder(bytepool.NewZReader(pointer.IoReader(bytes.NewReader(b)), 100, 100))
				require.Equal(t, decoder.IsCompressed(), false)

				str, err := decoder.String()
				require.NoError(t, err)
				require.Equal(t, "hhh", str)
			},
		},
		{
			name: "Test Read Bool",
			test: func(t *testing.T) {
				b := make([]byte, 1)
				b[0] = 1

				decoder := NewDecoder(bytepool.NewZReader(pointer.IoReader(bytes.NewReader(b)), 100, 100))

				require.Equal(t, decoder.IsCompressed(), false)

				n, err := decoder.Bool()
				require.NoError(t, err)
				require.Equal(t, true, n)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestNewDecoderWithCompress(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "Test can compress",
			test: func(t *testing.T) {
				decoder := NewDecoderWithCompress(bytepool.NewZReader(pointer.IoReader(bytes.NewReader([]byte(""))), 32, 1))
				decoder.SetCompress(true)
				require.Equal(t, decoder.IsCompressed(), true)
			},
		}}
	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}
