package column

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"

	"github.com/pkg/profile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

var allInt64Strings = makeAllInt64Strings()

func makeAllInt64Strings() []string {
	var strs []string
	for i := -2147483648; i <= 2147483648*2; i += 100000 {
		str := strconv.Itoa(i)
		strs = append(strs, str)
	}

	return strs
}

var allInt64Values = makeAllInt64Values()

func makeAllInt64Values() []interface{} {
	var values []interface{}
	for i := -2147483648; i <= 2147483648*2; i += 100000 {
		values = append(values, int64(i))
	}

	return values
}

func TestInt64ColumnData_ReadFromTexts(t *testing.T) {
	type args struct {
		texts []string
	}
	tests := []struct {
		name            string
		args            args
		wantDataWritten []int64
		wantRowsRead    int
		wantErr         bool
	}{
		{
			name: "Should write data and return number of rows read with no error, 1 row",
			args: args{
				texts: []string{"122"},
			},
			wantRowsRead: 1,
			wantErr:      false,
		},
		{
			name: "Should write data and return number of rows read with no error, all possible int64",
			args: args{
				texts: allInt64Strings,
			},
			wantRowsRead: len(allInt64Strings),
			wantErr:      false,
		},
		{
			name: "Should write empty string as zero value",
			args: args{
				texts: []string{"", "122"},
			},
			wantDataWritten: []int64{0, 122},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name: "Should throw error if not integer",
			args: args{
				texts: []string{"a"},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
		{
			name: "Should throw error if read text larger than 64 bits",
			args: args{
				texts: []string{"1", "3232323", "9223372036854775808", "3"},
			},
			wantRowsRead: 2,
			wantErr:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := MustMakeColumnData(INT64, 10000000)

			got, err := i.ReadFromTexts(tt.args.texts)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadFromTexts() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.wantRowsRead {
				t.Errorf("ReadFromTexts() got = %v, wantRowsRead %v", got, tt.wantRowsRead)
			}

			if len(tt.wantDataWritten) > 0 {
				for index, value := range tt.wantDataWritten {
					if !tt.wantErr {
						assert.Equal(t, value, i.GetValue(index))
					}
				}
				return
			}

			for index, value := range tt.args.texts {
				if !tt.wantErr && value != i.GetString(index) {
					t.Errorf("ReadFromText(), written data differs")
				}
			}
		})
	}
}

func TestInt64ColumnData_ReadFromValues(t *testing.T) {
	type args struct {
		values []interface{}
	}
	tests := []struct {
		name            string
		args            args
		wantDataWritten []int64
		wantRowsRead    int
		wantErr         bool
	}{
		{
			name: "Should write data and return number of rows read with no error for int64",
			args: args{
				values: []interface{}{},
			},
			wantRowsRead: 0,
			wantErr:      false,
		},
		{
			name: "Should write data and return number of rows read with no error for int64",
			args: args{
				values: []interface{}{int64(122), int64(4)},
			},
			wantRowsRead: 2,
			wantErr:      false,
		},
		{
			name: "Should write data and return number of rows read with no error for int",
			args: args{
				values: []interface{}{int(122), int(4)},
			},
			wantRowsRead: 2,
			wantErr:      false,
		},
		{
			name: "Should write data and return number of rows read with no error for int32",
			args: args{
				values: []interface{}{int32(122), int32(4)},
			},
			wantRowsRead: 2,
			wantErr:      false,
		},
		{
			name: "Should write data and return number of rows read with no error for int8",
			args: args{
				values: []interface{}{int8(122), int8(4)},
			},
			wantRowsRead: 2,
			wantErr:      false,
		},
		{
			name: "Should write data and return number of rows read with no error for int16",
			args: args{
				values: []interface{}{int16(122), int16(4)},
			},
			wantRowsRead: 2,
			wantErr:      false,
		},
		{
			name: "Should write data and return number of rows read with no error, all possible int64",
			args: args{
				values: allInt64Values,
			},
			wantRowsRead: len(allInt64Values),
			wantErr:      false,
		},
		{
			name: "Should throw error with right number of rows read if inconsistent type",
			args: args{
				values: []interface{}{int8(122), int8(122), int16(4)},
			},
			wantRowsRead: 2,
			wantErr:      true,
		},
		{
			name: "Should throw error if read value not int64 or subtypes",
			args: args{
				values: []interface{}{"lol"},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := MustMakeColumnData(INT64, 1000000)

			got, err := i.ReadFromValues(tt.args.values)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadFromValues() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.wantRowsRead {
				t.Errorf("ReadFromValues() got = %v, wantRowsRead %v", got, tt.wantRowsRead)
			}

			for index, refValue := range tt.args.values {
				if !tt.wantErr {
					// Convert int values into string to compare actual value of int instead of the types
					assert.Equal(t, fmt.Sprint(refValue), fmt.Sprint(i.GetValue(index)))
				}
			}
		})
	}
}

func Benchmark_Int64ColumnData_ReadFromTexts(b *testing.B) {
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
			name:       "Should write data and return number of rows read with no error", // strconv 141769791 ns/op fastfloat 75921585  ns/op
			columnType: "Int64",
			args: args{
				texts: func() []string {
					str := make([]string, 1e+7)
					for i := 0; i < 1e+7; i++ {
						str[i] = "222"
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

func TestInt64ColumnData_EncoderDecoder(t *testing.T) {
	type args struct {
		texts []string
	}
	tests := []struct {
		name            string
		args            args
		columnType      CHColumnType
		wantDataWritten []int64
		wantRowsRead    int
		wantErr         bool
	}{
		{
			name: "Should write data and return number of rows read with no error, 1 row",
			args: args{
				texts: []string{"122"},
			},
			columnType:   INT,
			wantRowsRead: 1,
			wantErr:      false,
		},
		{
			name: "Should write data and return number of rows read with no error, 1 row",
			args: args{
				texts: []string{"122"},
			},
			columnType:   INT64,
			wantRowsRead: 1,
			wantErr:      false,
		},
		{
			name: "Should write data and return number of rows read with no error, all possible int64",
			args: args{
				texts: allInt64Strings,
			},
			columnType:   INT64,
			wantRowsRead: len(allInt64Strings),
			wantErr:      false,
		},
		{
			name: "Should write data and return number of rows read with no error, all possible int64",
			args: args{
				texts: allInt64Strings,
			},
			columnType:   INT,
			wantRowsRead: len(allInt64Strings),
			wantErr:      false,
		},
		{
			name: "Should write empty string as zero value",
			args: args{
				texts: []string{"", "122"},
			},
			columnType:      INT64,
			wantDataWritten: []int64{0, 122},
			wantRowsRead:    2,
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
					require.Equal(t, value, newCopy.GetValue(index))
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
