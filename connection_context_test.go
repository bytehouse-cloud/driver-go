package bytehouse

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConnectionContext_SetLogf(t *testing.T) {
	type args struct {
		logf func(format string, a ...interface{})
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "Can set logf",
			args: args{
				logf: func(format string, a ...interface{}) {
					fmt.Printf("hi "+format, a...)
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewConnectionContext(func(s string, i ...interface{}) {
			}, func() (string, error) {
				return "", nil
			})
			c.SetLogf(nil)
			require.Nil(t, c.GetLogf())

			c.SetLogf(tt.args.logf)
			require.NotNil(t, c.GetLogf())
		})
	}
}

func TestConnectionContext_SetResolveHost(t *testing.T) {
	type args struct {
		resolveHost func() (string, error)
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "Can set resolveHost",
			args: args{
				resolveHost: func() (string, error) {
					return "", nil
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewConnectionContext(func(s string, i ...interface{}) {
			}, func() (string, error) {
				return "", nil
			})
			c.SetResolveHost(nil)
			require.Nil(t, c.getHost)

			c.SetResolveHost(tt.args.resolveHost)
			require.NotNil(t, c.getHost)
		})
	}
}
