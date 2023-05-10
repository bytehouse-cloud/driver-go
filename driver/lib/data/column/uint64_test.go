package column

import (
	"bytes"
	"strconv"
	"testing"

	"github.com/pkg/profile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

var uint64TestInterfaces, uint64TestValues, uint64TestStrings = createUInt64TestValues()

func createUInt64TestValues() ([]interface{}, []uint64, []string) {
	var valuesI []interface{}
	var values []uint64
	var strs []string

	for i := 0; i <= 4294967295*2; i += 100000 {
		str := strconv.Itoa(i)
		strs = append(strs, str)
		valuesI = append(valuesI, uint64(i))
		values = append(values, uint64(i))
	}

	return valuesI, values, strs
}

func TestUInt64ColumnData_ReadFromTexts(t *testing.T) {
	type args struct {
		texts []string
	}
	tests := []struct {
		name            string
		args            args
		wantDataWritten []uint64
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
			name: "Should write data and return number of rows read with no error, all possible uint64",
			args: args{
				texts: uint64TestStrings,
			},
			wantRowsRead: len(uint64TestStrings),
			wantErr:      false,
		},
		{
			name: "Should write zero value if empty string",
			args: args{
				texts: []string{"", "3", "4", ""},
			},
			wantDataWritten: []uint64{0, 3, 4, 0},
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
			name: "Should throw error if read text smaller than 64 bits",
			args: args{
				texts: []string{"-1"},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
		{
			name: "Should throw error if read text larger than 64 bits",
			args: args{
				texts: []string{"18446744073709551616"},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := MustMakeColumnData(UINT64, 10000000)

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

func TestUInt64ColumnData_ReadFromValues(t *testing.T) {
	type args struct {
		values []interface{}
	}
	tests := []struct {
		name            string
		args            args
		wantDataWritten []uint64
		wantRowsRead    int
		wantErr         bool
	}{
		{
			name: "Should write data and return number of rows read with no error for nil",
			args: args{
				values: []interface{}{nil},
			},
			wantRowsRead:    1,
			wantDataWritten: []uint64{0},
			wantErr:         false,
		},
		{
			name: "Should write data and return number of rows read with no error for uint64",
			args: args{
				values: []interface{}{uint64(122), uint64(4)},
			},
			wantRowsRead:    2,
			wantDataWritten: []uint64{122, 4},
			wantErr:         false,
		},
		{
			name: "Should write data and return number of rows read with no error for uint",
			args: args{
				values: []interface{}{uint(122), uint(4)},
			},
			wantDataWritten: []uint64{122, 4},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name: "Should write data and return number of rows read with no error for uint32",
			args: args{
				values: []interface{}{uint32(122), uint32(4)},
			},
			wantDataWritten: []uint64{122, 4},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name: "Should write data and return number of rows read with no error for uint8",
			args: args{
				values: []interface{}{uint8(122), uint8(4)},
			},
			wantDataWritten: []uint64{122, 4},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name: "Should write data and return number of rows read with no error for uint16",
			args: args{
				values: []interface{}{uint16(122), uint16(4)},
			},
			wantDataWritten: []uint64{122, 4},
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
			name: "Should write data and return number of rows read with no error, all possible uint64",
			args: args{
				values: uint64TestInterfaces,
			},
			wantRowsRead:    len(uint64TestValues),
			wantDataWritten: uint64TestValues,

			wantErr: false,
		},
		{
			name: "Should throw error with right number of rows read if inconsistent type",
			args: args{
				values: []interface{}{uint8(122), uint8(122), uint16(4)},
			},
			wantRowsRead:    2,
			wantDataWritten: []uint64{122, 122, 4},
			wantErr:         true,
		},
		{
			name: "Should throw error if read value not uint64 or subtypes",
			args: args{
				values: []interface{}{int(70)},
			},
			wantDataWritten: []uint64{70},
			wantRowsRead:    0,
			wantErr:         true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := MustMakeColumnData(UINT64, 1000000)

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

func TestUInt64ColumnData_EncoderDecoder(t *testing.T) {
	type args struct {
		texts []string
	}
	tests := []struct {
		name            string
		args            args
		wantDataWritten []uint64
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
			name: "Should write data and return number of rows read with no error, all possible uint64",
			args: args{
				texts: uint64TestStrings,
			},
			wantRowsRead: len(uint64TestStrings),
			wantErr:      false,
		},
		{
			name: "Should write zero value if empty string",
			args: args{
				texts: []string{"", "3", "4", "null"},
			},
			wantDataWritten: []uint64{0, 3, 4, 0},
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
			original := MustMakeColumnData(UINT64, len(tt.args.texts))
			got, err := original.ReadFromTexts(tt.args.texts)
			require.NoError(t, err)
			require.Equal(t, got, tt.wantRowsRead)
			require.NoError(t, err)
			err = original.WriteToEncoder(encoder)
			require.NoError(t, err)

			// Read from decoder
			newCopy := MustMakeColumnData(UINT64, len(tt.args.texts))
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

func Benchmark_UInt64ColumnData_ReadFromTexts(b *testing.B) {
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
			name:       "Should write data and return number of rows read with no error", // strconv 97351790 ns/op fastfloat 70405891 ns/op
			columnType: "UInt64",
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
