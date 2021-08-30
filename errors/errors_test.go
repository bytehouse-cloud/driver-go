package errors

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestErrorfWithCaller(t *testing.T) {
	type args struct {
		format string
		a      []interface{}
	}
	tests := []struct {
		name string
		args args
		want error
	}{
		{
			name: "Should show caller function",
			args: args{
				format: "good example %s",
				a:      []interface{}{"hi"},
			},
			want: errors.New("driver-go(errors.TestErrorfWithCaller.func1): good example hi"),
		},
		{
			name: "Should remove driver-go prefix if found",
			args: args{
				format: "driver-go(someFunc): good example %s",
				a:      []interface{}{"hi"},
			},
			want: errors.New("driver-go(errors.TestErrorfWithCaller.func1): (someFunc): good example hi"),
		},
		{
			name: "Should remove driver-go prefix if found",
			args: args{
				format: "driver-go: good example %s",
				a:      []interface{}{"hi"},
			},
			want: errors.New("driver-go(errors.TestErrorfWithCaller.func1): good example hi"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want.Error(), ErrorfWithCaller(tt.args.format, tt.args.a...).Error())
		})
	}
}

func TestErrorf(t *testing.T) {
	type args struct {
		format string
		a      []interface{}
	}
	tests := []struct {
		name string
		args args
		want error
	}{
		{
			name: "Should show driver-go prefix",
			args: args{
				format: "good example %s",
				a:      []interface{}{"hi"},
			},
			want: errors.New("driver-go: good example hi"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, Errorf(tt.args.format, tt.args.a...).Error(), tt.want.Error())
		})
	}
}

func TestFunc(t *testing.T) {}

func TestGetFunctionName(t *testing.T) {
	assert.Equal(t, "errors.TestFunc", GetFunctionName(TestFunc))
}
