package bytepool

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/bytehouse-cloud/driver-go/utils/pointer"
	"github.com/stretchr/testify/require"
)

func TestZReader_ReadAll(t *testing.T) {
	type args struct {
		p []byte
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "Test that read and write same bytes",
			args: args{
				p: []byte("hello world"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewBuffer(tt.args.p)
			z := NewZReader(pointer.IoReader(buf), 100, 1)
			b := make([]byte, len(tt.args.p))
			require.NoError(t, z.ReadFull(b))
			require.Equal(t, tt.args.p, b)
			require.NoError(t, z.Close())
		})
	}
}

func TestZReader_ReadUvarint(t *testing.T) {
	type args struct {
		p []byte
	}
	tests := []struct {
		name     string
		args     args
		expected uint64
	}{
		{
			name: "Can read when have buffer balance < 10",
			args: args{
				p: func() []byte {
					values := make([]byte, 10)
					binary.PutUvarint(values, 10)
					return values
				}(),
			},
			expected: 10,
		},
		{
			name: "Can read when have buffer balance < 10",
			args: args{
				p: func() []byte {
					values := make([]byte, 10)
					binary.PutUvarint(values, 5)
					return values
				}(),
			},
			expected: 5,
		},
		{
			name: "Can read when have buffer balance > 10",
			args: args{
				p: func() []byte {
					values := make([]byte, 1000)
					binary.PutUvarint(values, 1000)
					return values
				}(),
			},
			expected: 1000,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewBuffer(tt.args.p)
			z := NewZReader(pointer.IoReader(buf), 100, 1)
			v, err := z.ReadUvarint()
			require.NoError(t, err)
			require.Equal(t, tt.expected, v)
		})
	}
}

func TestZReader_PrependCurrentBuffer(t *testing.T) {
	type args struct {
		pre []byte
	}
	tests := []struct {
		name               string
		args               args
		initial            []byte
		bytesReadInitially int
		expectedReadFull   []byte
	}{
		{
			name: "Test that can prepend current buffer",
			args: args{
				pre: []byte("1234"),
			},
			initial:            []byte("12345 678"),
			bytesReadInitially: 5,
			expectedReadFull:   []byte("1234 678"),
		},
		{
			name: "Test that can prepend current buffer, making of new buffer",
			args: args{
				// 130 bytes -> will overflow buffer -> make new buffer
				pre: []byte("012346789012346789012346789012346789012346789012346789012346789012346789012346789012346789012346789012346789012346789012346789012346789012346789"),
			},
			initial:            []byte("12345 678"),
			bytesReadInitially: 5,
			expectedReadFull:   []byte("012346789012346789012346789012346789012346789012346789012346789012346789012346789012346789012346789012346789012346789012346789012346789012346789 678"),
		},
		{
			name: "Test that can prepend current buffer, with shifting of buffer",
			args: args{
				pre: []byte("123456789"),
			},
			initial:            []byte("1111111111"),
			bytesReadInitially: 5,
			expectedReadFull:   []byte("12345678911111"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewBuffer(tt.initial)
			z := NewZReader(pointer.IoReader(buf), 100, 1)

			toRead := make([]byte, tt.bytesReadInitially)
			_, err := z.Read(toRead)
			require.NoError(t, err)
			z.PrependCurrentBuffer(tt.args.pre)

			finalRead := make([]byte, len(tt.expectedReadFull))
			_, err = z.Read(finalRead)
			require.NoError(t, err)
			require.Equal(t, string(tt.expectedReadFull), string(finalRead))
		})
	}
}
