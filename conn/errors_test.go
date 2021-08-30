package conn

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsErrBadConnection(t *testing.T) {
	type args struct {
		err error
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "Should return true if it is an ErrBadConnection",
			args: args{
				err: NewErrBadConnection("Mama eats cake"),
			},
			want: true,
		},
		{
			name: "Should return true if it is an ErrBadConnection",
			args: args{
				err: NewErrBadConnection("Papa eats cake"),
			},
			want: true,
		},
		{
			name: "Should return true if it is an ErrBadConnection",
			args: args{
				err: ErrBadConnection{},
			},
			want: true,
		},
		{
			name: "Should return false if it is not an ErrBadConnection",
			args: args{
				err: errors.New("some funny error"),
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.want {
				assert.True(t, ErrBadConnection{}.Is(tt.args.err))
				assert.True(t, errors.Is(ErrBadConnection{}, tt.args.err))
				return
			}

			assert.False(t, errors.Is(tt.args.err, ErrBadConnection{}))
		})
	}
}

func Test_PrintError(t *testing.T) {
	err := NewErrBadConnection("hello world")
	require.Equal(t, err.Error(), "driver-go: ErrBadConnection: hello world")
}
