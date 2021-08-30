package column

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

var allInt32Strings = makeAllInt32Strings()

func makeAllInt32Strings() []string {
	var strs []string
	for i := math.MinInt32; i <= math.MaxInt32; i += 100000 {
		str := strconv.Itoa(i)
		strs = append(strs, str)
	}

	return strs
}

var allInt32Values = makeAllInt32Values()

func makeAllInt32Values() []interface{} {
	var values []interface{}
	for i := math.MinInt32; i <= math.MaxInt32; i += 100000 {
		values = append(values, int32(i))
	}

	return values
}

func TestInt32ColumnData_ReadFromTexts(t *testing.T) {
	type args struct {
		texts []string
	}
	tests := []struct {
		name            string
		args            args
		wantDataWritten []int32
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
			name: "Should write data and return number of rows read with no error, all possible int32",
			args: args{
				texts: allInt32Strings,
			},
			wantRowsRead: len(allInt32Strings),
			wantErr:      false,
		},
		{
			name: "Should return values for empty string",
			args: args{
				texts: []string{"", "122"},
			},
			wantDataWritten: []int32{0, 122},
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
			name: "Should throw error if read text larger than 32 bits",
			args: args{
				texts: []string{"5", "2147483649"},
			},
			wantRowsRead: 1,
			wantErr:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := MustMakeColumnData(INT32, 10000000)

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

func TestInt32ColumnData_ReadFromValues(t *testing.T) {
	type args struct {
		values []interface{}
	}
	tests := []struct {
		name            string
		args            args
		wantDataWritten []int32
		wantRowsRead    int
		wantErr         bool
	}{
		{
			name: "Should write data and return number of rows read with no error for int32",
			args: args{
				values: []interface{}{},
			},
			wantRowsRead: 0,
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
			name: "Should write data and return number of rows read with no error, all possible int32",
			args: args{
				values: allInt32Values,
			},
			wantRowsRead: len(allInt32Values),
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
			name: "Should throw error if read value not int32 or subtypes",
			args: args{
				values: []interface{}{int64(3)},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
		{
			name: "Should throw error if read value not int32 or subtypes",
			args: args{
				values: []interface{}{int(3)},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := MustMakeColumnData(INT32, 1000000)

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

func TestInt32ColumnData_EncoderDecoder(t *testing.T) {
	type args struct {
		texts []string
	}
	tests := []struct {
		name            string
		args            args
		wantDataWritten []int32
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
			name: "Should write data and return number of rows read with no error, all possible int32",
			args: args{
				texts: allInt32Strings,
			},
			wantRowsRead: len(allInt32Strings),
			wantErr:      false,
		},
		{
			name: "Should return values for empty string",
			args: args{
				texts: []string{"", "122"},
			},
			wantDataWritten: []int32{0, 122},
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
			original := MustMakeColumnData(INT32, len(tt.args.texts))
			got, err := original.ReadFromTexts(tt.args.texts)
			require.NoError(t, err)
			require.Equal(t, got, tt.wantRowsRead)
			require.NoError(t, err)
			err = original.WriteToEncoder(encoder)
			require.NoError(t, err)

			// Read from decoder
			newCopy := MustMakeColumnData(INT32, len(tt.args.texts))
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
