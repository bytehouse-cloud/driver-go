package column

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_makeMapKeyValue(t *testing.T) {
	type args struct {
		t CHColumnType
	}
	tests := []struct {
		name      string
		args      args
		wantKey   CHColumnType
		wantValue CHColumnType
	}{
		{
			name: "Test that can parse map properly",
			args: args{
				t: "Map(Map(String, UInt8), UInt8)",
			},
			wantKey:   "Map(String, UInt8)",
			wantValue: "UInt8",
		},
		{
			name: "Test that can parse map properly",
			args: args{
				t: "Map(Map(String, UInt8), Tuple(Int8))",
			},
			wantKey:   "Map(String, UInt8)",
			wantValue: "Tuple(Int8)",
		},
		{
			name: "Test that can parse map properly",
			args: args{
				t: "Map(Array(String), UInt8)",
			},
			wantKey:   "Array(String)",
			wantValue: "UInt8",
		},
		{
			name: "Test that can parse map properly",
			args: args{
				t: "Map(Tuple(String, UInt8), UInt8)",
			},
			wantKey:   "Tuple(String, UInt8)",
			wantValue: "UInt8",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotKey, gotValue := makeMapKeyValue(tt.args.t)
			assert.Equal(t, tt.wantKey, gotKey)
			assert.Equal(t, tt.wantValue, gotValue)
		})
	}
}
