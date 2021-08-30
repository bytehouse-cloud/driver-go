package column

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

func TestEnum8ColumnData_ReadFromTexts(t *testing.T) {
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
			name:       "Should write data and return number of rows read with no error, 2 rows",
			columnType: "Enum8('hello' = 1, 'world' = 2)",
			args: args{
				texts: []string{"hello", "world"},
			},
			wantDataWritten: []string{"hello", "world"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error, 2 rows",
			columnType: "Enum8('hello' = -128, 'world' = 127)",
			args: args{
				texts: []string{"-128", "127"},
			},
			wantDataWritten: []string{"hello", "world"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:       "Should throw error if value not in enum",
			columnType: "Enum8('hello' = -128, 'world' = 127)",
			args: args{
				texts: []string{"-128", "1"},
			},
			wantDataWritten: []string{"hello"},
			wantRowsRead:    1,
			wantErr:         true,
		},
		{
			name:       "Should throw error if larger than int8",
			columnType: "Enum8('hello' = -126, 'world' = -129)",
			args: args{
				texts: []string{"-126", "-129"},
			},
			wantDataWritten: []string{"hello"},
			wantRowsRead:    1,
			wantErr:         true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			colGen, err := GenerateColumnDataFactory(tt.columnType)
			if err != nil && tt.wantErr {
				return
			}
			require.NoError(t, err)

			i := colGen(1000)
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

func TestEnum8ColumnData_ReadFromValues(t *testing.T) {
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
			columnType: "Enum8('hello' = 1, 'world' = 2)",
			args: args{
				values: []interface{}{},
			},
			wantRowsRead: 0,
			wantErr:      false,
		},
		{
			name:       "Should write data and return number of rows read with no error, 2 rows",
			columnType: "Enum8('hello' = 1, 'world' = 2)",
			args: args{
				values: []interface{}{int8(1), int8(2)},
			},
			wantDataWritten: []string{"hello", "world"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error, 2 rows",
			columnType: "Enum8('hello' = 1, 'world' = 2)",
			args: args{
				values: []interface{}{"hello", "world"},
			},
			wantDataWritten: []string{"hello", "world"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error, 2 rows",
			columnType: "Enum8('hello' = -128, 'world' = 127)",
			args: args{
				values: []interface{}{int8(-128), int8(127)},
			},
			wantDataWritten: []string{"hello", "world"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:       "Should throw error if value not int8",
			columnType: "Enum8('hello' = -128, 'world' = 127)",
			args: args{
				values: []interface{}{int8(-128), int(127)},
			},
			wantDataWritten: []string{"hello"},
			wantRowsRead:    1,
			wantErr:         true,
		},
		{
			name:       "Should throw error if value not int8",
			columnType: "Enum8('hello' = -128, 'world' = 127)",
			args: args{
				values: []interface{}{127},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
		{
			name:       "Should throw error if inconsistent type",
			columnType: "Enum8('hello' = -128, 'world' = 127)",
			args: args{
				values: []interface{}{int8(-128), "world"},
			},
			wantDataWritten: []string{"hello"},
			wantRowsRead:    1,
			wantErr:         true,
		},
		{
			name:       "Should throw error if inconsistent type",
			columnType: "Enum8('hello' = -128, 'world' = 127)",
			args: args{
				values: []interface{}{"world", int8(-128)},
			},
			wantDataWritten: []string{"world"},
			wantRowsRead:    1,
			wantErr:         true,
		},
		{
			name:       "Should throw error if enum not found",
			columnType: "Enum8('hello' = -128, 'world' = 127)",
			args: args{
				values: []interface{}{int8(-128), int8(9)},
			},
			wantDataWritten: []string{"hello"},
			wantRowsRead:    1,
			wantErr:         true,
		},
		{
			name:       "Should throw error if out of range",
			columnType: "Enum8('hello' = -128, 'world' = 128)",
			args: args{
				values: []interface{}{int8(-128), 128},
			},
			wantDataWritten: []string{"hello"},
			wantRowsRead:    1,
			wantErr:         true,
		},
		{
			name:       "Should throw error if enum not found",
			columnType: "Enum8('hello' = -128, 'world' = 127)",
			args: args{
				values: []interface{}{"hello", "cock"},
			},
			wantDataWritten: []string{"hello"},
			wantRowsRead:    1,
			wantErr:         true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			colGen, err := GenerateColumnDataFactory(tt.columnType)
			if err != nil && tt.wantErr {
				return
			}
			require.NoError(t, err)

			i := colGen(1000)
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

func TestEnum8ColumnData_EncoderDecoder(t *testing.T) {
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
			name:       "Should write data and return number of rows read with no error, 2 rows",
			columnType: "Enum8('hello' = 1, 'world' = 2)",
			args: args{
				texts: []string{"hello", "world"},
			},
			wantDataWritten: []string{"hello", "world"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error, 2 rows",
			columnType: "Enum8('hello' = -128, 'world' = 127)",
			args: args{
				texts: []string{"-128", "127"},
			},
			wantDataWritten: []string{"hello", "world"},
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
			colGen, err := GenerateColumnDataFactory(tt.columnType)
			if err != nil && tt.wantErr {
				return
			}
			require.NoError(t, err)

			original := colGen(len(tt.args.texts))
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
