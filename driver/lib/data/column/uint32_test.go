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

var uint32TestInterfaces, uint32TestValues, uint32TestStrings = createUInt32TestValues()

func createUInt32TestValues() ([]interface{}, []uint32, []string) {
	var valuesI []interface{}
	var values []uint32
	var strs []string

	for i := 0; i <= math.MaxUint32; i += 100000 {
		str := strconv.Itoa(i)
		strs = append(strs, str)
		values = append(values, uint32(i))
		valuesI = append(valuesI, uint32(i))
	}

	return valuesI, values, strs
}

func TestUInt32ColumnData_ReadFromTexts(t *testing.T) {
	type args struct {
		texts []string
	}
	tests := []struct {
		name            string
		args            args
		wantDataWritten []uint32
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
			name: "Should write data and return number of rows read with no error, all possible uint32",
			args: args{
				texts: uint32TestStrings,
			},
			wantRowsRead: len(uint32TestStrings),
			wantErr:      false,
		},
		{
			name: "Should write zero value if empty string",
			args: args{
				texts: []string{"", "1", "null", "2"},
			},
			wantDataWritten: []uint32{0, 1, 0, 2},
			wantRowsRead:    4,
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
				texts: []string{"-1"},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
		{
			name: "Should throw error if read text larger than 32 bits",
			args: args{
				texts: []string{"4294967296"},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := MustMakeColumnData(UINT32, 10000000)

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

func TestUInt32ColumnData_ReadFromValues(t *testing.T) {
	type args struct {
		values []interface{}
	}
	tests := []struct {
		name            string
		args            args
		wantDataWritten []uint32
		wantRowsRead    int
		wantErr         bool
	}{
		{
			name: "Should write data and return number of rows read with no error for nil",
			args: args{
				values: []interface{}{nil},
			},
			wantRowsRead:    1,
			wantDataWritten: []uint32{0},
			wantErr:         false,
		},
		{
			name: "Should write data and return number of rows read with no error for uint32",
			args: args{
				values: []interface{}{uint32(122), uint32(4)},
			},
			wantDataWritten: []uint32{122, 4},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name: "Should write data and return number of rows read with no error for uint8",
			args: args{
				values: []interface{}{uint8(122), uint8(4)},
			},
			wantDataWritten: []uint32{122, 4},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name: "Should write data and return number of rows read with no error for uint16",
			args: args{
				values: []interface{}{uint16(122), uint16(4)},
			},
			wantDataWritten: []uint32{122, 4},
			wantRowsRead:    2,
			wantErr:         false,
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
			name: "Should write data and return number of rows read with no error, all possible uint32",
			args: args{
				values: uint32TestInterfaces,
			},
			wantDataWritten: uint32TestValues,
			wantRowsRead:    len(uint32TestInterfaces),
			wantErr:         false,
		},
		{
			name: "Should throw error with right number of rows read if inconsistent type",
			args: args{
				values: []interface{}{uint8(122), uint8(122), uint16(4)},
			},
			wantRowsRead: 2,
			wantErr:      true,
		},
		{
			name: "Should throw error if read value not uint32 or subtypes",
			args: args{
				values: []interface{}{uint64(3)},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := MustMakeColumnData(UINT32, 1000000)

			got, err := i.ReadFromValues(tt.args.values)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadFromValues() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.wantRowsRead {
				t.Errorf("ReadFromValues() got = %v, wantRowsRead %v", got, tt.wantRowsRead)
			}

			for index, value := range tt.wantDataWritten {
				if !tt.wantErr {
					require.Equal(t, value, i.GetValue(index))
				}
			}
		})
	}
}

func TestUInt32ColumnData_EncoderDecoder(t *testing.T) {
	type args struct {
		texts []string
	}
	tests := []struct {
		name            string
		args            args
		wantDataWritten []uint32
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
			name: "Should write data and return number of rows read with no error, all possible uint32",
			args: args{
				texts: uint32TestStrings,
			},
			wantRowsRead: len(uint32TestStrings),
			wantErr:      false,
		},
		{
			name: "Should write zero value if empty string",
			args: args{
				texts: []string{"", "1", "", "2"},
			},
			wantDataWritten: []uint32{0, 1, 0, 2},
			wantRowsRead:    4,
			wantErr:         false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buffer bytes.Buffer
			encoder := ch_encoding.NewEncoder(&buffer)
			decoder := ch_encoding.NewDecoder(&buffer)

			// Write to encoder
			original := MustMakeColumnData(UINT32, len(tt.args.texts))
			got, err := original.ReadFromTexts(tt.args.texts)
			require.NoError(t, err)
			require.Equal(t, got, tt.wantRowsRead)
			require.NoError(t, err)
			err = original.WriteToEncoder(encoder)
			require.NoError(t, err)

			// Read from decoder
			newCopy := MustMakeColumnData(UINT32, len(tt.args.texts))
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
