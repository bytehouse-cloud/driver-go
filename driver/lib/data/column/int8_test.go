package column

import (
	"bytes"
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

var allInt8Strings = makeAllInt8Strings()
var allInt8Values = makeAllInt8Values()

func makeAllInt8Strings() []string {
	var strs []string
	for i := math.MinInt8; i <= math.MaxInt8; i++ {
		str := strconv.Itoa(i)
		strs = append(strs, str)
	}

	return strs
}

func makeAllInt8Values() []interface{} {
	var values []interface{}
	for i := math.MinInt8; i <= math.MaxInt8; i++ {
		values = append(values, int8(i))
	}

	return values
}

func TestInt8ColumnData_ReadFromTexts(t *testing.T) {
	type args struct {
		texts []string
	}
	tests := []struct {
		name            string
		args            args
		wantDataWritten []int8
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
			name: "Should write data and return number of rows read with no error, all possible int 8",
			args: args{
				texts: allInt8Strings,
			},
			wantRowsRead: len(allInt8Strings),
			wantErr:      false,
		},
		{
			name: "Should write data and return number of rows read with no error, 2 rows",
			args: args{
				texts: []string{"", "12"},
			},
			wantDataWritten: []int8{0, 12},
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
			name: "Should throw error if read text larger than 8 bits",
			args: args{
				texts: []string{"2", "3", "128"},
			},
			wantRowsRead: 2,
			wantErr:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := MustMakeColumnData(INT8, 1000)

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

func TestInt8ColumnData_ReadFromValues(t *testing.T) {
	type args struct {
		values []interface{}
	}
	tests := []struct {
		name            string
		args            args
		wantDataWritten []int8
		wantRowsRead    int
		wantErr         bool
	}{
		{
			name: "Should write data and return number of rows read with no error, 1 row",
			args: args{
				values: []interface{}{int8(122)},
			},
			wantRowsRead: 1,
			wantErr:      false,
		},
		{
			name: "Should write data and return number of rows read with no error, all possible int8",
			args: args{
				values: allInt8Values,
			},
			wantRowsRead: len(allInt8Values),
			wantErr:      false,
		},
		{
			name: "Should throw error if inconsistent type",
			args: args{
				values: []interface{}{int8(7), int8(6), int32(3)},
			},
			wantRowsRead: 2,
			wantErr:      true,
		},
		{
			name: "Should throw error if read value not int8",
			args: args{
				values: []interface{}{int32(3)},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := MustMakeColumnData(INT8, 1000)

			got, err := i.ReadFromValues(tt.args.values)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadFromValues() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.wantRowsRead {
				t.Errorf("ReadFromValues() got = %v, wantRowsRead %v", got, tt.wantRowsRead)
			}

			for index, value := range tt.args.values {
				if !tt.wantErr && value != i.GetValue(index) {
					t.Errorf("ReadFromValues(), written data differs")
				}
			}
		})
	}
}

func TestInt8ColumnData_EncoderDecoder(t *testing.T) {
	type args struct {
		texts []string
	}
	tests := []struct {
		name            string
		args            args
		wantDataWritten []int8
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
			name: "Should write data and return number of rows read with no error, all possible int 8",
			args: args{
				texts: allInt8Strings,
			},
			wantRowsRead: len(allInt8Strings),
			wantErr:      false,
		},
		{
			name: "Should write data and return number of rows read with no error, 2 rows",
			args: args{
				texts: []string{"", "12"},
			},
			wantDataWritten: []int8{0, 12},
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
			original := MustMakeColumnData(INT8, len(tt.args.texts))
			got, err := original.ReadFromTexts(tt.args.texts)
			require.NoError(t, err)
			require.Equal(t, got, tt.wantRowsRead)
			require.NoError(t, err)
			err = original.WriteToEncoder(encoder)
			require.NoError(t, err)

			// Read from decoder
			newCopy := MustMakeColumnData(INT8, len(tt.args.texts))
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
