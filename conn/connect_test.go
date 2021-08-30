package conn

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
)

func TestOpenStrategy_String(t *testing.T) {
	tests := []struct {
		name string
		s    DialStrategy
		want string
	}{
		{
			name: "Should show in_order",
			s:    DialInOrder,
			want: "in_order",
		},
		{
			name: "Should show time_random",
			s:    DialTimeRandom,
			want: "time_random",
		},
		{
			name: "Should show random",
			s:    DialRandom,
			want: "random",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := string(tt.s); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConn(t *testing.T) {
	fConn := &fakeConn{}

	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "Can return eof error at the end of read",
			test: func(t *testing.T) {
				conn := &connect{
					Conn:         fConn,
					logf:         func(s string, i ...interface{}) {},
					ident:        1,
					zReader:      bytepool.NewZReader(fConn, 1024*4, 8),
					readTimeout:  1000,
					writeTimeout: 1000,
				}
				v := make([]byte, 100)
				_, err := conn.Read(v)
				require.Equal(t, err, io.EOF)
				require.NoError(t, conn.Close())
			},
		},
		{
			name: "Can return eof error at the end of read",
			test: func(t *testing.T) {
				conn := &connect{
					Conn:         fConn,
					logf:         func(s string, i ...interface{}) {},
					ident:        1,
					zReader:      bytepool.NewZReader(fConn, 1024*4, 8),
					readTimeout:  1000,
					writeTimeout: 1000,
				}
				_, err := conn.ReadUvarint()
				require.Equal(t, err, io.EOF)
				require.NoError(t, conn.Close())
			},
		},
		{
			name: "Can return error if write fails",
			test: func(t *testing.T) {
				conn := &connect{
					Conn:         fConn,
					logf:         func(s string, i ...interface{}) {},
					ident:        1,
					zReader:      bytepool.NewZReader(fConn, 1024*4, 8),
					readTimeout:  1000,
					writeTimeout: 1000,
				}
				_, err := conn.Write([]byte("hello"))
				require.Error(t, err)
				require.Equal(t, "fakeConn: can't write anything other than dinosaur", err.Error())
				require.NoError(t, conn.Close())
			},
		},
		{
			name: "Can write with right length of bytes written",
			test: func(t *testing.T) {
				conn := &connect{
					Conn:         fConn,
					logf:         func(s string, i ...interface{}) {},
					ident:        1,
					zReader:      bytepool.NewZReader(fConn, 1024*4, 8),
					readTimeout:  1000,
					writeTimeout: 1000,
				}
				data := []byte("dinosaur")
				n, err := conn.Write(data)
				require.NoError(t, err)
				require.Equal(t, len(data), n)
				require.NoError(t, conn.Close())
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}
