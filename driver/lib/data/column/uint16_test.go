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

var allUInt16Strings = makeAllUInt16Strings()

func makeAllUInt16Strings() []string {
	var strs []string
	for i := 0; i <= math.MaxUint16; i++ {
		str := strconv.Itoa(i)
		strs = append(strs, str)
	}

	return strs
}

var allUInt16Values = makeAllUInt16Values()

func makeAllUInt16Values() []interface{} {
	var values []interface{}
	for i := 0; i <= math.MaxUint16; i++ {
		values = append(values, uint16(i))
	}

	return values
}

func TestUInt16ColumnData_ReadFromTexts(t *testing.T) {
	type args struct {
		texts []string
	}
	tests := []struct {
		name            string
		args            args
		wantDataWritten []uint16
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
			name: "Should write data and return number of rows read with no error, all possible uint16",
			args: args{
				texts: allUInt16Strings,
			},
			wantRowsRead: len(allUInt16Strings),
			wantErr:      false,
		},
		{
			name: "Should write zero value if empty string",
			args: args{
				texts: []string{"", "1", "2"},
			},
			wantDataWritten: []uint16{0, 1, 2},
			wantRowsRead:    3,
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
			name: "Should throw error if read text larger than 16 bits",
			args: args{
				texts: []string{"-1"},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
		{
			name: "Should throw error if read text larger than 16 bits",
			args: args{
				texts: []string{"2", "10", "65536"},
			},
			wantRowsRead: 2,
			wantErr:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := MustMakeColumnData(UINT16, 10000000)

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

func TestUInt16ColumnData_ReadFromValues(t *testing.T) {
	type args struct {
		values []interface{}
	}
	tests := []struct {
		name            string
		args            args
		wantDataWritten []uint16
		wantRowsRead    int
		wantErr         bool
	}{
		{
			name: "Should write data and return number of rows read with no error for uint16",
			args: args{
				values: []interface{}{uint16(122), uint16(4)},
			},
			wantRowsRead: 2,
			wantErr:      false,
		},
		{
			name: "Should write data and return number of rows read with no error for uint8",
			args: args{
				values: []interface{}{uint8(122), uint8(4)},
			},
			wantRowsRead: 2,
			wantErr:      false,
		},
		{
			name: "Should write empty rows",
			args: args{
				values: []interface{}{},
			},
			wantRowsRead: 0,
			wantErr:      false,
		},
		{
			name: "Should write data and return number of rows read with no error, all possible uint16",
			args: args{
				values: allUInt16Values,
			},
			wantRowsRead: len(allUInt16Values),
			wantErr:      false,
		},
		{
			name: "Should throw error if inconsistent type",
			args: args{
				values: []interface{}{uint8(122), uint16(4)},
			},
			wantRowsRead: 1,
			wantErr:      true,
		},
		{
			name: "Should throw error if read value not uint16 or subtypes",
			args: args{
				values: []interface{}{uint32(3)},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := MustMakeColumnData(UINT16, 1000000)

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

func TestUInt16ColumnData_EncoderDecoder(t *testing.T) {
	type args struct {
		texts []string
	}
	tests := []struct {
		name            string
		args            args
		wantDataWritten []uint16
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
			name: "Should write data and return number of rows read with no error, all possible uint16",
			args: args{
				texts: allUInt16Strings,
			},
			wantRowsRead: len(allUInt16Strings),
			wantErr:      false,
		},
		{
			name: "Should write zero value if empty string",
			args: args{
				texts: []string{"", "1", "2"},
			},
			wantDataWritten: []uint16{0, 1, 2},
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
			original := MustMakeColumnData(UINT16, len(tt.args.texts))
			got, err := original.ReadFromTexts(tt.args.texts)
			require.NoError(t, err)
			require.Equal(t, got, tt.wantRowsRead)
			require.NoError(t, err)
			err = original.WriteToEncoder(encoder)
			require.NoError(t, err)

			// Read from decoder
			newCopy := MustMakeColumnData(UINT16, len(tt.args.texts))
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
