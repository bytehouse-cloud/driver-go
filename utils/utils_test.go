package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCountFormat(t *testing.T) {
	tests := []struct {
		name           string
		givenCount     int64
		expectedFormat string
	}{
		{
			name:           "given 0 then 0",
			givenCount:     0,
			expectedFormat: "0",
		},
		{
			name:           "given 3 digits then no comma",
			givenCount:     123,
			expectedFormat: "123",
		},
		{
			name:           "given 4 digits then 1 comma",
			givenCount:     1234,
			expectedFormat: "1,234",
		},
		{
			name:           "given 6 digits then 1 comma",
			givenCount:     123456,
			expectedFormat: "123,456",
		},
		{
			name:           "given 7 digits then 2 comma",
			givenCount:     1234567,
			expectedFormat: "1,234,567",
		},
		{
			name:           "given 8 digits then 2 comma",
			givenCount:     12345678,
			expectedFormat: "12,345,678",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expectedFormat, FormatCount(tt.givenCount))
		})
	}
}

func Benchmark_numInput(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		NumArgs("SELECT * FROM example WHERE os_id in (@os_id,@browser_id) browser_id = @browser_id")
	}
}

func TestTransposeMatrix(t *testing.T) {
	type args struct {
		table [][]interface{}
	}
	tests := []struct {
		name string
		args args
		want [][]interface{}
	}{
		{
			name: "Can transpose table into column values",
			args: args{
				table: [][]interface{}{
					{1, 2, 3, 4},
					{4, 5, 6, 7},
					{7, 8, 9, 19},
				},
			},
			want: [][]interface{}{
				{1, 4, 7},
				{2, 5, 8},
				{3, 6, 9},
				{4, 7, 19},
			},
		},
		{
			name: "Can transpose single column into column values",
			args: args{
				table: [][]interface{}{
					{1},
					{4},
					{79},
				},
			},
			want: [][]interface{}{
				{1, 4, 79},
			},
		},
		{
			name: "Can handle empty table",
			args: args{
				table: [][]interface{}{},
			},
			want: [][]interface{}{},
		},
		{
			name: "Can handle empty columns",
			args: args{
				table: [][]interface{}{{}, {}},
			},
			want: [][]interface{}{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := TransposeMatrix(tt.args.table)
			require.Equal(t, tt.want, got)
		})
	}
}
