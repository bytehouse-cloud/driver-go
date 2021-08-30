package column

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

func TestNullableColumnData_ReadFromTexts(t *testing.T) {
	type args struct {
		texts []string
	}
	tests := []struct {
		name            string
		columnType      CHColumnType
		args            args
		wantDataWritten []string
		wantRowsRead    int
		wantErr         bool
	}{
		{
			name:       "Should write data and return number of rows read with no error",
			columnType: "Nullable(Int8)",
			args: args{
				texts: []string{"1", "2"},
			},
			wantDataWritten: []string{"1", "2"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error, with null",
			columnType: "Nullable(Int8)",
			args: args{
				texts: []string{"-128", "127", NULL, "0"},
			},
			wantDataWritten: []string{"-128", "127", NULLDisplay, "0"},
			wantRowsRead:    4,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error, with null",
			columnType: "Nullable(String)",
			args: args{
				texts: []string{"-128", "127", "", "''"},
			},
			wantDataWritten: []string{"-128", "127", "", ""},
			wantRowsRead:    4,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error",
			columnType: "Nullable(Float64)",
			args: args{
				texts: []string{"-128", "127"},
			},
			wantDataWritten: []string{"-128", "127"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error, with null",
			columnType: "Nullable(Int8)",
			args: args{
				texts: []string{"-128", "127", NULLSmall, NULL, NULLDisplay},
			},
			wantDataWritten: []string{"-128", "127", NULLDisplay, NULLDisplay, NULLDisplay},
			wantRowsRead:    5,
			wantErr:         false,
		},
		{
			name:       "Should throw error if invalid value",
			columnType: "Nullable(Int8)",
			args: args{
				texts: []string{"-128", "129", NULL},
			},
			wantDataWritten: []string{"-128"},
			wantRowsRead:    1,
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
			if got != tt.wantRowsRead {
				t.Errorf("ReadFromTexts() got = %v, wantRowsRead %v", got, tt.wantRowsRead)
			}

			for index, value := range tt.wantDataWritten {
				assert.Equal(t, value, i.GetString(index))

			}
		})
	}
}

func TestNullableColumnData_ReadFromValues(t *testing.T) {
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
			name:       "Should write data and return number of rows read with no error",
			columnType: "Nullable(String)",
			args: args{
				values: []interface{}{"1", "2"},
			},
			wantDataWritten: []string{"1", "2"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error, with null",
			columnType: "Nullable(String)",
			args: args{
				values: []interface{}{"-128", "127", nil, "0"},
			},
			wantDataWritten: []string{"-128", "127", NULLDisplay, "0"},
			wantRowsRead:    4,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error, with null",
			columnType: "Nullable(UInt8)",
			args: args{
				values: []interface{}{uint8(1), nil},
			},
			wantDataWritten: []string{"1", NULLDisplay},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error, with nil",
			columnType: "Nullable(UInt8)",
			args: args{
				values: []interface{}{uint8(1), nil},
			},
			wantDataWritten: []string{"1", NULLDisplay},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error, with nil",
			columnType: "Nullable(UInt8)",
			args: args{
				values: []interface{}{uint8(1), nil, uint8(1), nil},
			},
			wantDataWritten: []string{"1", NULLDisplay, "1", NULLDisplay},
			wantRowsRead:    4,
			wantErr:         false,
		},
		{
			name:       "Should throw error if invalid type",
			columnType: "Nullable(String)",
			args: args{
				values: []interface{}{"-128", 1, NULL},
			},
			wantDataWritten: []string{"-128"},
			wantRowsRead:    1,
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

func TestNullableColumnData_EncoderDecoder(t *testing.T) {
	type args struct {
		texts []string
	}
	tests := []struct {
		name             string
		columnType       CHColumnType
		args             args
		wantDataWritten  []string
		wantValueWritten []interface{}
		wantRowsRead     int
		wantErr          bool
	}{
		{
			name:       "Should write data and return number of rows read with no error",
			columnType: "Nullable(Int8)",
			args: args{
				texts: []string{"1", "2"},
			},
			wantDataWritten:  []string{"1", "2"},
			wantValueWritten: []interface{}{int8(1), int8(2)},
			wantRowsRead:     2,
			wantErr:          false,
		},
		{
			name:       "Should write data and return number of rows read with no error, with null",
			columnType: "Nullable(Int8)",
			args: args{
				texts: []string{"-128", "127", NULL, "0"},
			},
			wantDataWritten:  []string{"-128", "127", NULLDisplay, "0"},
			wantValueWritten: []interface{}{int8(-128), int8(127), nil, int8(0)},
			wantRowsRead:     4,
			wantErr:          false,
		},
		{
			name:       "Should write data and return number of rows read with no error, with null",
			columnType: "Nullable(String)",
			args: args{
				texts: []string{"-128", "127", NULL, "''"},
			},
			wantDataWritten:  []string{"-128", "127", NULLDisplay, ""},
			wantValueWritten: []interface{}{"-128", "127", nil, ""},
			wantRowsRead:     4,
			wantErr:          false,
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

			for index, value := range tt.wantValueWritten {
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
