package column

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

func TestEnum16ColumnData_ReadFromTexts(t *testing.T) {
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
			columnType: "Enum16('A'=1,'B'=2,'C'=3)",
			args: args{
				texts: []string{"1", "2"},
			},
			wantDataWritten: []string{"A", "B"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error, 2 rows",
			columnType: "Enum16('A'=1,'B'=2,'C'=3)",
			args: args{
				texts: []string{"A", "B"},
			},
			wantDataWritten: []string{"A", "B"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error, 2 rows",
			columnType: "Enum16('hello' = -32768, 'world' = 32767)",
			args: args{
				texts: []string{"-32768", "32767"},
			},
			wantDataWritten: []string{"hello", "world"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:       "Should throw error if value not in enum",
			columnType: "Enum16('hello' = -32768, 'world' = 32767)",
			args: args{
				texts: []string{"-32768", "1"},
			},
			wantDataWritten: []string{"hello"},
			wantRowsRead:    1,
			wantErr:         true,
		},
		{
			name:       "Should throw error if larger than int16",
			columnType: "Enum16('hello' = -126, 'world' = 32768)",
			args: args{
				texts: []string{"-126", "32768"},
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

func TestEnum16ColumnData_ReadFromValues(t *testing.T) {
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
			name:       "Should write data and return number of rows read with no error, 2 rows",
			columnType: "Enum16('hello' = 1, 'world' = 2)",
			args: args{
				values: []interface{}{},
			},
			wantRowsRead: 0,
			wantErr:      false,
		},
		{
			name:       "Should write data and return number of rows read with no error, 2 rows",
			columnType: "Enum16('hello' = 1, 'world' = 2)",
			args: args{
				values: []interface{}{"hellofew", "world"},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
		{
			name:       "Should write data and return number of rows read with no error, 2 rows",
			columnType: "Enum16('hello' = 1, 'world' = 2)",
			args: args{
				values: []interface{}{"hello", "world"},
			},
			wantDataWritten: []string{"hello", "world"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error for int16, 2 rows",
			columnType: "Enum16('hello' = -32768, 'world' = 32767)",
			args: args{
				values: []interface{}{int16(-32768), int16(32767)},
			},
			wantDataWritten: []string{"hello", "world"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error for int8, 2 rows",
			columnType: "Enum16('hello' = 1, 'world' = 2)",
			args: args{
				values: []interface{}{int8(1), int8(2)},
			},
			wantDataWritten: []string{"hello", "world"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error for int8, 2 rows",
			columnType: "Enum16('hello' = 1, 'world' = 2)",
			args: args{
				values: []interface{}{12},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
		{
			name:       "Should throw error if value not int16",
			columnType: "Enum16('hello' = -32768, 'world' = 32767)",
			args: args{
				values: []interface{}{int16(-32768), int(32767)},
			},
			wantDataWritten: []string{"hello"},
			wantRowsRead:    1,
			wantErr:         true,
		},
		{
			name:       "Should throw error if inconsistent type",
			columnType: "Enum16('hello' = -32768, 'world' = 32767)",
			args: args{
				values: []interface{}{int16(-32768), "world"},
			},
			wantDataWritten: []string{"hello"},
			wantRowsRead:    1,
			wantErr:         true,
		},
		{
			name:       "Should throw error if inconsistent type",
			columnType: "Enum16('hello' = -32768, 'world' = 32767)",
			args: args{
				values: []interface{}{"world", int16(32767)},
			},
			wantRowsRead: 1,
			wantErr:      true,
		},
		{
			name:       "Should throw error if inconsistent type",
			columnType: "Enum16('hello' = 1, 'world' = 2)",
			args: args{
				values: []interface{}{int8(1), "world"},
			},
			wantDataWritten: []string{"hello"},
			wantRowsRead:    1,
			wantErr:         true,
		},
		{
			name:       "Should throw error if inconsistent type",
			columnType: "Enum16('hello' = 1, 'world' = 2)",
			args: args{
				values: []interface{}{"world", int8(2)},
			},
			wantRowsRead: 1,
			wantErr:      true,
		},
		{
			name:       "Should throw error if enum not found",
			columnType: "Enum16('hello' = 1, 'world' = 2)",
			args: args{
				values: []interface{}{int8(1), int8(9)},
			},
			wantDataWritten: []string{"hello"},
			wantRowsRead:    1,
			wantErr:         true,
		},
		{
			name:       "Should throw error if enum not found",
			columnType: "Enum16('hello' = -32768, 'world' = 32767)",
			args: args{
				values: []interface{}{int16(-32768), int16(9)},
			},
			wantDataWritten: []string{"hello"},
			wantRowsRead:    1,
			wantErr:         true,
		},
		{
			name:       "Should throw error if out of range",
			columnType: "Enum16('hello' = -32768, 'world' = 128)",
			args: args{
				values: []interface{}{int16(-32768), 128},
			},
			wantDataWritten: []string{"hello"},
			wantRowsRead:    1,
			wantErr:         true,
		},
		{
			name:       "Should throw error if enum not found",
			columnType: "Enum16('hello' = -32768, 'world' = 32767)",
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

func TestEnum16ColumnData_EncoderDecoder(t *testing.T) {
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
			columnType: "Enum16('A'=1,'B'=2,'C'=3)",
			args: args{
				texts: []string{"1", "2"},
			},
			wantDataWritten: []string{"A", "B"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error, 2 rows",
			columnType: "Enum16('hello' = -32768, 'world' = 32767)",
			args: args{
				texts: []string{"-32768", "32767"},
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
