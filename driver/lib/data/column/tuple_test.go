package column

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

func TestTupleColumnData_ReadFromTexts(t *testing.T) {
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
			name:       "Should write data and return number of rows read with no error, 3 rows",
			columnType: "Tuple(UInt8, String)",
			args: args{
				texts: []string{"(1, 1)", "(1, 'lalaland')", "(1, 'jack's dog')"},
			},
			wantDataWritten: []string{"(1, '1')", "(1, 'lalaland')", "(1, 'jack's dog')"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error, 0 rows",
			columnType: "Tuple(UInt8, String)",
			args: args{
				texts: []string{},
			},
			wantRowsRead: 0,
			wantErr:      false,
		},
		{
			name:       "Should write data and return number of rows read for square brackets with no error, 3 rows",
			columnType: "Tuple(UInt8, String)",
			args: args{
				texts: []string{"[1, 1]", "[1, 'lalaland']", "[1, 'jack's dog']"},
			},
			wantDataWritten: []string{"(1, '1')", "(1, 'lalaland')", "(1, 'jack's dog')"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error, nested tuples",
			columnType: "Tuple(UInt8, String, Tuple(UInt8, String))",
			args: args{
				texts: []string{"(1, 1, (1, lalaland))", "(1, 'lalaland', (1, lalaland))", "(1, 'jacks dog', (1, lalaland))"},
			},
			wantDataWritten: []string{"(1, '1', (1, 'lalaland'))", "(1, 'lalaland', (1, 'lalaland'))", "(1, 'jacks dog', (1, 'lalaland'))"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name:       "Should throw error if invalid value, 2 rows",
			columnType: "Tuple(UInt8, String)",
			args: args{
				texts: []string{"(1, mamamia)", "(-1, 'lalaland')"},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
		{
			name:       "Should throw error if invalid value, 2 rows",
			columnType: "Tuple(UInt8, String)",
			args: args{
				texts: []string{"(1, mamamia)", "()"},
			},
			wantRowsRead: 1,
			wantErr:      true,
		},
		{
			name:       "Should throw error if invalid value, 2 rows",
			columnType: "Tuple(UInt8, UInt8)",
			args: args{
				texts: []string{"(1, mamamia)", "(-1, 'lalaland')"},
			},
			wantDataWritten: []string{"(1, 0)"},
			wantRowsRead:    0,
			wantErr:         true,
		},
		{
			name:       "Should throw error if invalid value, 2 rows",
			columnType: "Tuple(UInt8, UInt8)",
			args: args{
				texts: []string{"{1, mamamia}", "(-1, 'lalaland')"},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
		{
			name:       "Should throw error if invalid value, 2 rows",
			columnType: "Tuple(UInt8, UInt8)",
			args: args{
				texts: []string{"(1, mamamia]", "(-1, 'lalaland')"},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
		{
			name:       "Should throw error if invalid value, 2 rows",
			columnType: "Tuple(UInt8, UInt8)",
			args: args{
				texts: []string{"(1, 1)", "(1, 'lalaland')"},
			},
			wantRowsRead: 1,
			wantErr:      true,
		},
		{
			name:       "Should throw error if invalid value, 2 rows",
			columnType: "Tuple(UInt8, UInt8)",
			args: args{
				texts: []string{"", "(-1, 'lalaland')"},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
		{
			name:       "Should throw error if invalid value, 2 rows",
			columnType: "Tuple(UInt8, UInt8)",
			args: args{
				texts: []string{"()", "(-1, 'lalaland')"},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
		{
			name:       "Should throw error if invalid value, 2 rows",
			columnType: "Tuple(UInt8, UInt8)",
			args: args{
				texts: []string{"(1)", "(-1, 'lalaland')"},
			},
			wantRowsRead: 0,
			wantErr:      true,
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

			require.Equal(t, got, tt.wantRowsRead)

			for index, value := range tt.wantDataWritten {
				assert.Equal(t, value, i.GetString(index))
			}
		})
	}
}

func TestTupleColumnData_ReadFromValues(t *testing.T) {
	type args struct {
		values []interface{}
	}
	tests := []struct {
		name             string
		args             args
		columnType       CHColumnType
		wantDataWritten  []string
		wantValueWritten []interface{}
		wantRowsRead     int
		wantErr          bool
	}{
		{
			name:       "Should write data and return number of rows read with no error, 3 rows",
			columnType: "Tuple(UInt8, String)",
			args: args{
				values: []interface{}{[]interface{}{uint8(1), "1"}, []interface{}{uint8(1), "lalaland"}, []interface{}{uint8(1), "jack's dog"}},
			},
			wantDataWritten:  []string{"(1, '1')", "(1, 'lalaland')", "(1, 'jack's dog')"},
			wantValueWritten: []interface{}{[]interface{}{uint8(1), "1"}, []interface{}{uint8(1), "lalaland"}, []interface{}{uint8(1), "jack's dog"}},
			wantRowsRead:     3,
			wantErr:          false,
		},
		{
			name:       "Should write data and return number of rows read with no error, zero rows",
			columnType: "Tuple(UInt8, String)",
			args: args{
				values: []interface{}{},
			},
			wantRowsRead: 0,
			wantErr:      false,
		},
		{
			name:       "Should write data and return number of rows read with no error, 3 rows",
			columnType: "Tuple(UInt8, UInt8)",
			args: args{
				values: []interface{}{[]uint8{uint8(1), uint8(2)}, []uint8{uint8(1), uint8(2)}},
			},
			wantDataWritten:  []string{"(1, 2)", "(1, 2)"},
			wantValueWritten: []interface{}{[]interface{}{uint8(1), uint8(2)}, []interface{}{uint8(1), uint8(2)}},
			wantRowsRead:     2,
			wantErr:          false,
		},
		{
			name:       "Should write data and return number of rows read with no error, 3 rows",
			columnType: "Tuple(UInt8, UInt8, UInt8)",
			args: args{
				values: []interface{}{[]uint8{uint8(1), uint8(2), uint8(2)}, []uint8{uint8(1), uint8(2), uint8(2)}},
			},
			wantDataWritten:  []string{"(1, 2, 2)", "(1, 2, 2)"},
			wantValueWritten: []interface{}{[]interface{}{uint8(1), uint8(2), uint8(2)}, []interface{}{uint8(1), uint8(2), uint8(2)}},
			wantRowsRead:     2,
			wantErr:          false,
		},
		{
			name:       "Should throw error if invalid values len, 2 rows",
			columnType: "Tuple(UInt8, String)",
			args: args{
				values: []interface{}{[]interface{}{uint8(1), "mamamia", "haha"}, []interface{}{int8(-1), "lalaland"}},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
		{
			name:       "Should throw error if invalid values len, 3 rows",
			columnType: "Tuple(UInt8, UInt8)",
			args: args{
				values: []interface{}{[]uint8{uint8(1), uint8(2), uint8(3)}, []uint8{uint8(1), uint8(2)}},
			},
			wantDataWritten: []string{"(0, 0)"},
			wantRowsRead:    0,
			wantErr:         true,
		},
		{
			name:       "Should throw error if invalid value, 2 rows",
			columnType: "Tuple(UInt8, String)",
			args: args{
				values: []interface{}{[]interface{}{uint8(1), "mamamia"}, []interface{}{int8(-1), "lalaland"}},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
		{
			name:       "Should throw error if nil",
			columnType: "Tuple(UInt8, String)",
			args: args{
				values: []interface{}{nil},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
		{
			name:       "Should throw error if invalid value, 2 rows",
			columnType: "Tuple(UInt8, UInt8)",
			args: args{
				values: []interface{}{[]interface{}{uint8(1), "mamamia"}, []interface{}{int8(-1), "lalaland"}},
			},
			wantDataWritten: []string{"(1, 0)"},
			wantRowsRead:    0,
			wantErr:         true,
		},
		{
			name:       "Should throw error if invalid value, 2 rows",
			columnType: "Tuple(UInt8, UInt8)",
			args: args{
				values: []interface{}{[]interface{}{uint8(1), uint8(2)}, []interface{}{uint8(1), "lalaland"}},
			},
			wantRowsRead: 1,
			wantErr:      true,
		},
		{
			name:       "Should throw error if invalid value, 2 rows",
			columnType: "Tuple(UInt8, UInt8)",
			args: args{
				values: []interface{}{[]interface{}{uint8(1), "mamamia"}, 1},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
		{
			name:       "Should throw error if invalid value",
			columnType: "Tuple(UInt8, UInt8)",
			args: args{
				values: []interface{}{1},
			},
			wantRowsRead: 0,
			wantErr:      true,
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
				assert.Equal(t, i.GetString(index), value)
			}

			for index, value := range tt.wantValueWritten {
				assert.Equal(t, i.GetValue(index), value)
			}
		})
	}
}

func TestTupleColumnData_EncoderDecoder(t *testing.T) {
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
			name:       "Should write data and return number of rows read with no error, 3 rows",
			columnType: "Tuple(UInt8, String)",
			args: args{
				texts: []string{"(1, 1)", "(1, 'lalaland')", "(1, 'jack's dog')"},
			},
			wantDataWritten: []string{"(1, '1')", "(1, 'lalaland')", "(1, 'jack's dog')"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read for square brackets with no error, 3 rows",
			columnType: "Tuple(UInt8, String)",
			args: args{
				texts: []string{"[1, 1]", "[1, 'lalaland']", "[1, 'jack's dog']"},
			},
			wantDataWritten: []string{"(1, '1')", "(1, 'lalaland')", "(1, 'jack's dog')"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error, nested tuples",
			columnType: "Tuple(UInt8, String, Tuple(UInt8, String))",
			args: args{
				texts: []string{"(1, 1, (1, lalaland))", "(1, 'lalaland', (1, lalaland))", "(1, 'jacks dog', (1, lalaland))"},
			},
			wantDataWritten: []string{"(1, '1', (1, 'lalaland'))", "(1, 'lalaland', (1, 'lalaland'))", "(1, 'jacks dog', (1, 'lalaland'))"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error, nested tuples",
			columnType: "Tuple(UInt8, String, Tuple(UInt8, String))",
			args: args{
				texts: []string{"(1, 1, (1, lalaland))", "(1, 'lalaland', (1, lalaland))", "(1, 'jacks dog', (1, lalaland))"},
			},
			wantDataWritten: []string{"(1, 1, (1, lalaland))", "(1, lalaland, (1, lalaland))", "(1, jacks dog, (1, lalaland))"},
			wantRowsRead:    3,
			wantErr:         true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buffer bytes.Buffer
			encoder := ch_encoding.NewEncoder(&buffer)
			decoder := ch_encoding.NewDecoder(&buffer)

			original := MustMakeColumnData(tt.columnType, len(tt.args.texts))
			_, err := original.ReadFromTexts(tt.args.texts)
			require.NoError(t, err)
			err = original.WriteToEncoder(encoder)
			require.NoError(t, err)

			newCopy := MustMakeColumnData(tt.columnType, len(tt.wantDataWritten))
			err = newCopy.ReadFromDecoder(decoder)
			for index, value := range tt.wantDataWritten {
				if !tt.wantErr {
					assert.Equal(t, value, newCopy.GetString(index))
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

func TestTupleColumnData_CloseFail(t *testing.T) {
	_, err := GenerateColumnDataFactory("Tuple(Unsupported)")
	require.Error(t, err)
}
