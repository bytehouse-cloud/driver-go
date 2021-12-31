package helper

import (
	"bytes"
	"io"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
)

func TestDiscardUntilByteEscaped(t *testing.T) {
	var stop byte = '\n'
	tests := []struct {
		name  string
		input []byte
		want  []byte
		err   error
	}{
		{
			name: "give nil then io.EOF",
			err:  io.EOF,
		},
		{
			name:  "give no stop byte then io.EOF",
			input: []byte("12347890"),
			err:   io.EOF,
		},
		{
			name:  "give byte middle then return want",
			input: []byte("1234\n7890"),
			want:  []byte("7890"),
		},
		{
			name:  "give byte middle with escape then return io.EOF",
			input: []byte("1234\\\n7890"),
			err:   io.EOF,
		},
		{
			name:  "given byte middle with escape then return remains",
			input: []byte("1234\\\n78\n90"),
			want:  []byte("90"),
		},
		{
			name:  "given edge then return nothing",
			input: []byte("123412347890\n"),
			want:  []byte{},
		},
		{
			name:  "given start then return remain",
			input: []byte("\n123412347890"),
			want:  []byte("123412347890"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			zReader := bytepool.NewZReader(bytes.NewReader(tt.input), 3, 2)
			err := DiscardUntilByteEscaped(zReader, stop)
			if !assert.Equal(t, tt.err, err) {
				return
			}
			if err != nil {
				return
			}

			actual, err := ioutil.ReadAll(zReader)
			if !assert.NoError(t, err) {
				return
			}
			assert.Equal(t, tt.want, actual)
		})
	}
}
