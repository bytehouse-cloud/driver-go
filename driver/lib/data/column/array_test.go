package column

import (
	"bytes"
	"testing"

	"github.com/pkg/profile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

func TestArrayColumnData_ReadFromTexts(t *testing.T) {
	type args struct {
		texts []string
	}
	tests := []struct {
		name               string
		columnType         CHColumnType
		args               args
		wantDataWritten    []string
		wantRawDataWritten [][]interface{}
		wantRowsRead       int
		wantErr            bool
	}{
		{
			name:       "Should write data and return number of rows read with no error, 3 rows",
			columnType: "Array(UInt8)",
			args: args{
				texts: []string{"[1, 1]", "[1, 3, 3,44,3]", "[1, 33, 33, 3]"},
			},
			wantDataWritten: []string{"[1, 1]", "[1, 3, 3, 44, 3]", "[1, 33, 33, 3]"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error, 3 rows",
			columnType: "Array(String)",
			args: args{
				texts: []string{"[1, 1]", "[1, 3, 3,44,3]", "[1, 33, 33, 3]"},
			},
			wantDataWritten: []string{"['1', '1']", "['1', '3', '3', '44', '3']", "['1', '33', '33', '3']"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error, 3 rows",
			columnType: "Array(UInt8)",
			args: args{
				texts: []string{"[1,1]", "[1,3,3,44,3]", "[1,33,33,3]"},
			},
			wantDataWritten: []string{"[1, 1]", "[1, 3, 3, 44, 3]", "[1, 33, 33, 3]"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error, nested arrays",
			columnType: "Array(Array(UInt8))",
			args: args{
				texts: []string{"[[1, 1], [1, 3, 3,4]]", "[[1, 1], [1, 3, 3,4]]", "[[1, 1], [1, 3, 3,4]]"},
			},
			wantDataWritten: []string{"[[1, 1], [1, 3, 3, 4]]", "[[1, 1], [1, 3, 3, 4]]", "[[1, 1], [1, 3, 3, 4]]"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name:       "Should throw error if invalid value",
			columnType: "Array(UInt8)",
			args: args{
				texts: []string{"[1, mamamia]", "[-1, 'lalaland']"},
			},
			wantDataWritten: []string{"[1, 0]"},
			wantRowsRead:    0,
			wantErr:         true,
		},
		{
			name:       "Should return empty array",
			columnType: "Array(UInt8)",
			args: args{
				texts: []string{""},
			},
			wantDataWritten:    []string{"[]"},
			wantRawDataWritten: [][]interface{}{{}},
			wantRowsRead:       1,
			wantErr:            false,
		},
		{
			name:       "Should return empty arrays",
			columnType: "Array(UInt8)",
			args: args{
				texts: []string{"", "[1]", ""},
			},
			wantDataWritten:    []string{"[]", "[1]", "[]"},
			wantRawDataWritten: [][]interface{}{{}, {uint8(1)}, {}},
			wantRowsRead:       3,
			wantErr:            false,
		},
		{
			name:       "Should return empty array",
			columnType: "Array(UInt8)",
			args: args{
				texts: []string{"[]"},
			},
			wantDataWritten:    []string{"[]"},
			wantRawDataWritten: [][]interface{}{{}},
			wantRowsRead:       1,
			wantErr:            false,
		},
		{
			name:       "Should return empty array",
			columnType: "Array(UInt8)",
			args: args{
				texts: []string{"[  ]"},
			},
			wantDataWritten: []string{"[]"},
			wantRowsRead:    1,
			wantErr:         false,
		},
		{
			name:       "Should return empty array and some other array",
			columnType: "Array(UInt8)",
			args: args{
				texts: []string{"[]", "[1, 2]", "[]"},
			},
			wantDataWritten: []string{"[]", "[1, 2]", "[]"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name:       "Should throw error if invalid value",
			columnType: "Array(UInt8)",
			args: args{
				texts: []string{"{3}"},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
		{
			name:       "Should throw error if invalid value",
			columnType: "Array(UInt8)",
			args: args{
				texts: []string{"[a]"},
			},
			wantDataWritten: []string{"[0]"},
			wantRowsRead:    0,
			wantErr:         true,
		},
		{
			name:       "Should throw error if invalid value",
			columnType: "Array(UInt8)",
			args: args{
				texts: []string{"[1, 1]", "[-1, 'lalaland']", "[-1, 'lalaland']"},
			},
			wantDataWritten: []string{"[1, 1]"},
			wantRowsRead:    1,
			wantErr:         true,
		},
		{
			name:       "Should throw error if invalid value",
			columnType: "Array(UInt8)",
			args: args{
				texts: []string{"[1,2,3,4,5]", "[1, aa,3]", "[1,2]"},
			},
			wantDataWritten: []string{"[1, 2, 3, 4, 5]"},
			wantRowsRead:    1,
			wantErr:         true,
		},
		{
			name:       "Should throw error if invalid value",
			columnType: "Array(UInt8)",
			args: args{
				texts: []string{"[1, 1]", "[1, 1]", "[1, p]"},
			},
			wantDataWritten: []string{"[1, 1]", "[1, 1]"},
			wantRowsRead:    2,
			wantErr:         true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := MustMakeColumnData(tt.columnType, 1000)

			got, err := i.ReadFromTexts(tt.args.texts)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadFromTexts() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			assert.Equal(t, tt.wantRowsRead, got)

			for index, value := range tt.wantDataWritten {
				assert.Equal(t, value, i.GetString(index))
			}

			if len(tt.wantDataWritten) > 0 {
				for index, value := range tt.wantRawDataWritten {
					assert.Equal(t, value, i.GetValue(index))
				}
			}
		})
	}
}

func TestArrayColumnData_ReadFromValues(t *testing.T) {
	type args struct {
		values []interface{}
	}
	tests := []struct {
		name            string
		args            args
		columnType      CHColumnType
		wantDataWritten []string
		wantRowsRead    int
		wantErr         bool
	}{
		{
			name:       "Should write data and return number of rows read with no error, 0 rows",
			columnType: "Array(UInt8)",
			args: args{
				values: []interface{}{},
			},
			wantRowsRead: 0,
			wantErr:      false,
		},
		{
			name:       "Should write data and return number of rows read with no error, 3 rows",
			columnType: "Array(UInt8)",
			args: args{
				values: []interface{}{[]uint8{1, 1}, []uint8{1, 3, 3, 44, 3}, []uint8{1, 33, 33, 3}},
			},
			wantDataWritten: []string{"[1, 1]", "[1, 3, 3, 44, 3]", "[1, 33, 33, 3]"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error, 3 rows",
			columnType: "Array(UInt8)",
			args: args{
				values: []interface{}{[]interface{}{uint8(1), uint8(1)}, []interface{}{uint8(1), uint8(3), uint8(3), uint8(44), uint8(3)}, []interface{}{uint8(1), uint8(33), uint8(33), uint8(3)}},
			},
			wantDataWritten: []string{"[1, 1]", "[1, 3, 3, 44, 3]", "[1, 33, 33, 3]"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error, nested arrays",
			columnType: "Array(Array(UInt8))",
			args: args{
				values: []interface{}{[][]uint8{{1, 1}, {1, 3, 3, 4}}, [][]uint8{{1, 1}, {1, 3, 3, 4}}, [][]uint8{{1, 1}, {1, 3, 3, 4}}},
			},
			wantDataWritten: []string{"[[1, 1], [1, 3, 3, 4]]", "[[1, 1], [1, 3, 3, 4]]", "[[1, 1], [1, 3, 3, 4]]"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name:       "Should throw error if invalid value",
			columnType: "Array(UInt8)",
			args: args{
				values: []interface{}{[]interface{}{1, "mamamia"}, []interface{}{-1, "lalaland"}},
			},
			wantDataWritten: []string{"[0, 0]"},
			wantRowsRead:    0,
			wantErr:         true,
		},
		{
			name:       "Should return empty array",
			columnType: "Array(UInt8)",
			args: args{
				values: []interface{}{[]interface{}{}},
			},
			wantDataWritten: []string{"[]"},
			wantRowsRead:    1,
			wantErr:         false,
		},
		{
			name:       "Should return empty array if nil",
			columnType: "Array(UInt8)",
			args: args{
				values: []interface{}{nil, nil},
			},
			wantDataWritten: []string{"[]", "[]"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:       "Should return empty array and some other array",
			columnType: "Array(UInt8)",
			args: args{
				values: []interface{}{nil, []uint8{1, 2}, nil},
			},
			wantDataWritten: []string{"[]", "[1, 2]", "[]"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name:       "Should throw error if invalid value",
			columnType: "Array(UInt8)",
			args: args{
				values: []interface{}{[]interface{}{"a"}},
			},
			wantDataWritten: []string{"[0]"},
			wantRowsRead:    0,
			wantErr:         true,
		},
		{
			name:       "Should throw error if inconsistent type",
			columnType: "Array(UInt16)",
			args: args{
				values: []interface{}{[]uint8{2}, 4},
			},
			wantRowsRead: 1,
			wantErr:      true,
		},
		{
			name:       "Should throw error if invalid type",
			columnType: "Array(UInt16)",
			args: args{
				values: []interface{}{[]interface{}{uint16(3), uint16(3)}, 3},
			},
			wantRowsRead: 1,
			wantErr:      true,
		},
		{
			name:       "Should throw error if invalid type",
			columnType: "Array(UInt16)",
			args: args{
				values: []interface{}{3},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
		{
			name:       "Should throw error if invalid value, 3 rows",
			columnType: "Array(UInt8)",
			args: args{
				values: []interface{}{[]uint8{1, 1}, []interface{}{-1, "lalaland"}, []interface{}{-1, "lalaland"}},
			},
			wantDataWritten: []string{"[1, 1]"},
			wantRowsRead:    1,
			wantErr:         true,
		},
		{
			name:       "Should throw error if invalid value",
			columnType: "Array(UInt8)",
			args: args{
				values: []interface{}{[]uint8{1, 2, 3, 4, 5}, []interface{}{uint8(1), "aa", uint8(3)}, []interface{}{1, 2}},
			},
			wantDataWritten: []string{"[1, 2, 3, 4, 5]"},
			wantRowsRead:    1,
			wantErr:         true,
		},
		{
			name:       "Should throw error if invalid value",
			columnType: "Array(UInt8)",
			args: args{
				values: []interface{}{[]uint8{1, 1}, []uint8{1, 1}, []interface{}{uint8(1), 'p'}},
			},
			wantDataWritten: []string{"[1, 1]", "[1, 1]"},
			wantRowsRead:    2,
			wantErr:         true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := MustMakeColumnData(tt.columnType, 1000)

			got, err := i.ReadFromValues(tt.args.values)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadFromValues() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.wantRowsRead {
				t.Errorf("ReadFromValues() got = %v, wantRowsRead %v", got, tt.wantRowsRead)
			}

			for index, value := range tt.wantDataWritten {
				assert.Equal(t, value, i.GetString(index))
			}
		})
	}
}

func Benchmark_ArrayColumnData_ReadFromTexts(b *testing.B) {
	type args struct {
		texts []string
	}
	tests := []struct {
		name            string
		columnType      CHColumnType
		args            args
		wantDataWritten []string
	}{
		{
			name:       "Should write data and return number of rows read with no error, 3 rows", // 5222776040 ns/op // 1784517103 ns/op
			columnType: "Array(UInt8)",
			args: args{
				texts: func() []string {
					str := make([]string, 1e+7)
					for i := 0; i < 1e+7; i++ {
						str[i] = "[1, 3, 3, 44, 3]"
					}
					return str
				}(),
			},
		},
	}
	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			defer profile.Start(profile.CPUProfile, profile.ProfilePath(".")).Stop()
			i := MustMakeColumnData(tt.columnType, 1e+7)

			b.ResetTimer()
			for j := 0; j < b.N; j++ {
				_, _ = i.ReadFromTexts(tt.args.texts)
			}
			b.StopTimer()
		})
	}
}

func TestArrayColumnData_EncoderDecoder(t *testing.T) {
	type args struct {
		texts []string
	}
	tests := []struct {
		name               string
		columnType         CHColumnType
		args               args
		wantDataWritten    []string
		wantRawDataWritten [][]interface{}
		wantRowsRead       int
		wantErr            bool
	}{
		{
			name:       "Should write data and return number of rows read with no error, 3 rows",
			columnType: "Array(UInt8)",
			args: args{
				texts: []string{"[1, 1]", "[1, 3, 3,44,3]", "[1, 33, 33, 3]"},
			},
			wantDataWritten: []string{"[1, 1]", "[1, 3, 3, 44, 3]", "[1, 33, 33, 3]"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error, 3 rows",
			columnType: "Array(UInt8)",
			args: args{
				texts: []string{"[1,1]", "[1,3,3,44,3]", "[1,33,33,3]"},
			},
			wantDataWritten: []string{"[1, 1]", "[1, 3, 3, 44, 3]", "[1, 33, 33, 3]"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error, nested arrays",
			columnType: "Array(Array(UInt8))",
			args: args{
				texts: []string{"[[1, 1], [1, 3, 3,4]]", "[[1, 1], [1, 3, 3,4]]", "[[1, 1], [1, 3, 3,4]]"},
			},
			wantDataWritten: []string{"[[1, 1], [1, 3, 3, 4]]", "[[1, 1], [1, 3, 3, 4]]", "[[1, 1], [1, 3, 3, 4]]"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name:       "Should return empty array",
			columnType: "Array(UInt8)",
			args: args{
				texts: []string{""},
			},
			wantDataWritten:    []string{"[]"},
			wantRawDataWritten: [][]interface{}{{}},
			wantRowsRead:       1,
			wantErr:            false,
		},
		{
			name:       "Should return empty arrays",
			columnType: "Array(UInt8)",
			args: args{
				texts: []string{"", "[1]", ""},
			},
			wantDataWritten:    []string{"[]", "[1]", "[]"},
			wantRawDataWritten: [][]interface{}{{}, {uint8(1)}, {}},
			wantRowsRead:       3,
			wantErr:            false,
		},
		{
			name:       "Should return empty array",
			columnType: "Array(UInt8)",
			args: args{
				texts: []string{"[]"},
			},
			wantDataWritten:    []string{"[]"},
			wantRawDataWritten: [][]interface{}{{}},
			wantRowsRead:       1,
			wantErr:            false,
		},
		{
			name:       "Should return empty array",
			columnType: "Array(UInt8)",
			args: args{
				texts: []string{"[  ]"},
			},
			wantDataWritten: []string{"[]"},
			wantRowsRead:    1,
			wantErr:         false,
		},
		{
			name:       "Should return empty array and some other array",
			columnType: "Array(UInt8)",
			args: args{
				texts: []string{"[]", "[1, 2]", "[]"},
			},
			wantDataWritten: []string{"[]", "[1, 2]", "[]"},
			wantRowsRead:    3,
			wantErr:         false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buffer bytes.Buffer
			encoder := ch_encoding.NewEncoder(&buffer)
			decoder := ch_encoding.NewDecoder(&buffer)

			// Write to encoder
			original := MustMakeColumnData(tt.columnType, len(tt.args.texts))
			got, err := original.ReadFromTexts(tt.args.texts)
			require.NoError(t, err)
			require.Equal(t, got, tt.wantRowsRead)
			require.NoError(t, err)
			err = original.WriteToEncoder(encoder)
			require.NoError(t, err)

			// Read from decoder
			newCopy := MustMakeColumnData(tt.columnType, len(tt.args.texts))
			err = newCopy.ReadFromDecoder(decoder)

			for index, value := range tt.wantDataWritten {
				if !tt.wantErr {
					require.Equal(t, value, newCopy.GetString(index))
				}
			}

			require.Equal(t, newCopy.Len(), original.Len())
			require.Equal(t, newCopy.Zero(), original.Zero())
			require.Equal(t, newCopy.ZeroString(), original.ZeroString())
			require.NoError(t, original.Close())
			require.NoError(t, newCopy.Close())
		})
	}
}
