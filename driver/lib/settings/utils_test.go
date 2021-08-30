package settings

import (
	"reflect"
	"testing"
)

func Test_parseValueInterface(t *testing.T) {
	tests := []struct {
		name    string
		value   interface{}
		defVal  interface{}
		want    interface{}
		wantErr bool
	}{
		{
			name:    "if uint64 sthen uint64",
			value:   uint64(4),
			defVal:  uint64(1),
			want:    uint64(4),
			wantErr: false,
		},
		{
			name:    "if uint64 string then uint64",
			value:   "4",
			defVal:  uint64(1),
			want:    uint64(4),
			wantErr: false,
		},
		{
			name:    "invalid uint64",
			value:   "aoaoea",
			defVal:  uint64(1),
			want:    uint64(0),
			wantErr: true,
		},
		{
			name:   "valid int64",
			value:  int64(8),
			defVal: int64(0),
			want:   int64(8),
		},
		{
			name:   "valid negative int64",
			value:  int64(-1),
			defVal: int64(0),
			want:   int64(-1),
		},
		{
			name:   "valid int64 string",
			value:  int64(8),
			defVal: int64(0),
			want:   int64(8),
		},
		{
			name:    "invalid int64 string",
			value:   "Oaaaa",
			defVal:  int64(0),
			want:    int64(0),
			wantErr: true,
		},
		{
			name:   "valid float",
			value:  float32(2.3),
			defVal: float32(0),
			want:   "2.3",
		},
		{
			name:   "valid float string",
			value:  "2.3",
			defVal: float32(0),
			want:   "2.3",
		},
		{
			name:    "invalid float string",
			value:   "aaaaa3",
			defVal:  float32(0),
			want:    "",
			wantErr: true,
		},
		{
			name:   "valid bool",
			value:  true,
			defVal: false,
			want:   true,
		},
		{
			name:   "valid bool string",
			value:  "1",
			defVal: false,
			want:   true,
		},
		{
			name:    "invalid bool string",
			value:   "aaaa",
			defVal:  false,
			want:    false,
			wantErr: true,
		},
		{
			name:   "if string then string",
			value:  "Eaeae",
			defVal: "",
			want:   "Eaeae",
		},
		{
			name:   "if unknown type with string then pass with fmt",
			value:  []int{1},
			defVal: "",
			want:   "[1]",
		},
		{
			name:    "if unknown type with int64 then fail",
			value:   []int{1},
			defVal:  int64(0),
			want:    int64(0),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseValueInterface(tt.value, tt.defVal)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseValueInterface() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseValueInterface() got = %v, want %v", got, tt.want)
			}
		})
	}
}
