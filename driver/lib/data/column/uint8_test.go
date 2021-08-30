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

var allUInt8Strings = makeAllUInt8Strings()
var allUInt8Values = makeAllUInt8Values()

func makeAllUInt8Strings() []string {
	var strs []string
	for i := 0; i <= math.MaxUint8; i++ {
		str := strconv.Itoa(i)
		strs = append(strs, str)
	}

	return strs
}

func makeAllUInt8Values() []interface{} {
	var values []interface{}
	for i := 0; i <= math.MaxUint8; i++ {
		values = append(values, uint8(i))
	}

	return values
}

func TestUInt8ColumnData_ReadFromTexts(t *testing.T) {
	type args struct {
		texts []string
	}
	tests := []struct {
		name            string
		args            args
		wantDataWritten []uint8
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
				texts: allUInt8Strings,
			},
			wantRowsRead: len(allUInt8Strings),
			wantErr:      false,
		},
		{
			name: "Should write zero value if empty string",
			args: args{
				texts: []string{"", "2", "3"},
			},
			wantDataWritten: []uint8{0, 2, 3},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name: "Should throw error if not integer",
			args: args{
				texts: []string{"mamamia"},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
		{
			name: "Should throw error if read text larger than 8 bits",
			args: args{
				texts: []string{"-1"},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
		{
			name: "Should throw error if read text larger than 8 bits",
			args: args{
				texts: []string{"1", "2", "256"},
			},
			wantRowsRead: 2,
			wantErr:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := MustMakeColumnData(UINT8, 1000)

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

func TestUInt8ColumnData_ReadFromValues(t *testing.T) {
	type args struct {
		values []interface{}
	}
	tests := []struct {
		name            string
		args            args
		wantDataWritten []uint8
		wantRowsRead    int
		wantErr         bool
	}{
		{
			name: "Should write data and return number of rows read with no error, 1 row",
			args: args{
				values: []interface{}{uint8(122)},
			},
			wantRowsRead: 1,
			wantErr:      false,
		},
		{
			name: "Should write data and return number of rows read with no error, all possible uint8",
			args: args{
				values: allUInt8Values,
			},
			wantRowsRead: len(allUInt8Values),
			wantErr:      false,
		},
		{
			name: "Should throw error if inconsistent type",
			args: args{
				values: []interface{}{uint8(7), uint8(6), int32(3)},
			},
			wantRowsRead: 2,
			wantErr:      true,
		},
		{
			name: "Should throw error if read value not uint8",
			args: args{
				values: []interface{}{int32(3)},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := MustMakeColumnData(UINT8, 1000)

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

func TestUInt8ColumnData_EncoderDecoder(t *testing.T) {
	type args struct {
		texts []string
	}
	tests := []struct {
		name            string
		args            args
		wantDataWritten []uint8
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
				texts: allUInt8Strings,
			},
			wantRowsRead: len(allUInt8Strings),
			wantErr:      false,
		},
		{
			name: "Should write zero value if empty string",
			args: args{
				texts: []string{"", "2", "3"},
			},
			wantDataWritten: []uint8{0, 2, 3},
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
			original := MustMakeColumnData(UINT8, len(tt.args.texts))
			got, err := original.ReadFromTexts(tt.args.texts)
			require.NoError(t, err)
			require.Equal(t, got, tt.wantRowsRead)
			require.NoError(t, err)
			err = original.WriteToEncoder(encoder)
			require.NoError(t, err)

			// Read from decoder
			newCopy := MustMakeColumnData(UINT8, len(tt.args.texts))
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
