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

func Test_parseNestedType(t *testing.T) {
	type args struct {
		chColumnType string
		prefix       string
	}
	tests := []struct {
		name    string
		args    args
		want    CHColumnType
		wantErr bool
	}{
		{
			name: "GIVEN 1 simple arg THEN fail",
			args: args{
				chColumnType: "AggregateFunction(int64)",
				prefix:       "AggregateFunction",
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "GIVEN 2 simple args THEN success",
			args: args{
				chColumnType: "AggregateFunction(int8, int64)",
				prefix:       "AggregateFunction",
			},
			want: CHColumnType("int64"),
		},
		{
			name: "GIVEN 3 simple arg THEN fail",
			args: args{
				chColumnType: "AggregateFunction(int64, string, float64)",
				prefix:       "AggregateFunction",
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "GIVEN 1 nested arg THEN fail",
			args: args{
				chColumnType: "AggregateFunction(tuple(int, string))",
				prefix:       "AggregateFunction",
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "GIVEN 2 nested args THEN success",
			args: args{
				chColumnType: "AggregateFunction(func(int, string), tuple(int, string))",
				prefix:       "AggregateFunction",
			},
			want: CHColumnType("tuple(int, string)"),
		},
		{
			name: "GIVEN 2 nested args with multiple layers THEN success",
			args: args{
				chColumnType: "AggregateFunction(func(int, string, tuple(string, int)), tuple(int, string, tuple(string, int)))",
				prefix:       "AggregateFunction",
			},
			want: CHColumnType("tuple(int, string, tuple(string, int))"),
		},
		{
			name: "GIVEN 3 nested arg THEN fail",
			args: args{
				chColumnType: "AggregateFunction(int, tuple(int, string), float64)",
				prefix:       "AggregateFunction",
			},
			want:    "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseNestedType(tt.args.chColumnType, tt.args.prefix)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseNestedType() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("parseNestedType() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDateTime64Param(t *testing.T) {

	tests := []struct {
		name             string
		columnDefinition string
		wantPrecision    int
		wantTZ           string
		wantErr          bool
	}{
		{
			name:             "GIVEN DateTime64 no param THEN return column",
			columnDefinition: "DateTime64()",
			wantErr:          false,
			wantPrecision:    0,
			wantTZ:           "",
		},
		{
			name:             "GIVEN DateTime64 no param with spaces THEN return column",
			columnDefinition: "DateTime64(    )",
			wantErr:          false,
			wantPrecision:    0,
			wantTZ:           "",
		},
		{
			name:             "GIVEN DateTime64 has incorrect datatype for precision THEN return error",
			columnDefinition: "DateTime64('Asia/Istanbul')",
			wantErr:          true,
			wantPrecision:    0,
			wantTZ:           "",
		},
		{
			name:             "GIVEN DateTime64 has precision THEN return column",
			columnDefinition: "DateTime64(3)",
			wantErr:          false,
			wantPrecision:    3,
			wantTZ:           "",
		},
		{
			name:             "GIVEN DateTime64 has precision with spaces THEN return column",
			columnDefinition: "DateTime64(   3  )",
			wantErr:          false,
			wantPrecision:    3,
			wantTZ:           "",
		},
		{
			name:             "GIVEN DateTime64 has precision and TZ  THEN return column",
			columnDefinition: "DateTime64(3, 'Asia')",
			wantErr:          false,
			wantPrecision:    3,
			wantTZ:           "Asia",
		},
		{
			name:             "GIVEN DateTime64 has precision and TZ  THEN return column",
			columnDefinition: "DateTime64(3, 'Asia')",
			wantErr:          false,
			wantPrecision:    3,
			wantTZ:           "Asia",
		},
		{
			name:             "GIVEN DateTime64 has precision and TZ with spaces THEN return column",
			columnDefinition: "DateTime64(  3  ,     'Asia')",
			wantErr:          false,
			wantPrecision:    3,
			wantTZ:           "Asia",
		},
		{
			name:             "GIVEN DateTime64 has precision and TZ with no spaces THEN return column",
			columnDefinition: "DateTime64(3,'Asia')",
			wantErr:          false,
			wantPrecision:    3,
			wantTZ:           "Asia",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			precision, tz, err := parseDateTime64Param(CHColumnType(tt.columnDefinition))

			if !tt.wantErr {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.wantPrecision, precision)
			assert.Equal(t, tt.wantTZ, tz)
		})
	}
}
