package column

import (
	"bytes"
	"fmt"
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

func TestDecimalColumnData_ReadFromTexts(t *testing.T) {
	type args struct {
		texts []string
	}
	tests := []struct {
		name        string
		args        args
		decimalType CHColumnType
		decimalWant struct {
			precision int
			scale     int
		}
		wantRawDataWritten []float64
		wantDataWritten    []string
		wantRowsRead       int
		wantErr            bool
	}{
		{
			name:        "Should write data and return number of rows read with no error, 2 rows",
			decimalType: "Decimal(18,5)",
			decimalWant: struct {
				precision int
				scale     int
			}{precision: 18, scale: 5},
			args: args{
				texts: []string{"122.00000", "1220.00000"},
			},
			wantDataWritten: []string{"122.00000", "1220.00000"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:        "Should throw error if precision/scale not supported",
			decimalType: "Decimal(58,5)",
			decimalWant: struct {
				precision int
				scale     int
			}{precision: 58, scale: 5},
			args: args{
				texts: []string{"122.00000", "1220.00000"},
			},
			wantDataWritten: []string{"122.00000", "1220.00000"},
			wantRowsRead:    0,
			wantErr:         true,
		},
		{
			name:        "Should convert to scale specified, 2 rows",
			decimalType: "Decimal(18,0)",
			decimalWant: struct {
				precision int
				scale     int
			}{precision: 18, scale: 0},
			args: args{
				texts: []string{"122.123453232323", "122.123453232323898"},
			},
			wantDataWritten: []string{"122", "122"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:        "Should convert to scale specified, 2 rows",
			decimalType: "Decimal(2,0)",
			decimalWant: struct {
				precision int
				scale     int
			}{precision: 2, scale: 0},
			args: args{
				texts: []string{"122.123453232323", "122.123453232323898"},
			},
			wantDataWritten: []string{"122", "122"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:        "Should throw error if not decimal",
			decimalType: "Decimal(18,0)",
			decimalWant: struct {
				precision int
				scale     int
			}{precision: 18, scale: 0},
			args: args{
				texts: []string{"", "3.44"},
			},
			wantRawDataWritten: []float64{0},
			wantRowsRead:       2,
			wantErr:            false,
		},
		{
			name:        "Should throw error if not decimal",
			decimalType: "Decimal(18,0)",
			decimalWant: struct {
				precision int
				scale     int
			}{precision: 18, scale: 0},
			args: args{
				texts: []string{"e"},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
		{
			name:        "Should throw error if precision too high",
			decimalType: "Decimal(40,0)",
			decimalWant: struct {
				precision int
				scale     int
			}{precision: 40, scale: 0},
			args: args{
				texts: []string{"1"},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
		{
			name:        "Should throw error if precision too high",
			decimalType: "Decimal(77,0)",
			decimalWant: struct {
				precision int
				scale     int
			}{precision: 77, scale: 0},
			args: args{
				texts: []string{"1"},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
		{
			name:        "Should work for Big Float",
			decimalType: "Decimal(38,15)",
			decimalWant: struct {
				precision int
				scale     int
			}{precision: 38, scale: 15},
			args: args{
				texts: []string{
					"99999999999999999999111.122228888777733",
					"9.012345678987654321",
					"3.141592653589793238",
				},
			},
			wantRowsRead: 3,
			wantErr:      false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := MustMakeColumnData(tt.decimalType, 1000)

			decimalCol, ok := i.(*DecimalColumnData)
			if assert.True(t, ok) {
				assert.Equal(t, tt.decimalWant.precision, decimalCol.precision)
				assert.Equal(t, tt.decimalWant.scale, decimalCol.scale)
			}

			got, err := i.ReadFromTexts(tt.args.texts)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadFromTexts() error = %v, wantErr = %v, got = %v", err, tt.wantErr, got)
				return
			}

			assert.Equal(t, got, tt.wantRowsRead)

			if len(tt.wantRawDataWritten) > 0 {
				for index, value := range tt.wantRawDataWritten {
					if !tt.wantErr {
						assert.Equal(t, value, i.GetValue(index))
					}
				}
				return
			}

			for index, value := range tt.wantDataWritten {
				if !tt.wantErr {
					assert.Equal(t, value, i.GetString(index))
				}
			}
		})
	}
}

func TestDecimalColumnData_ReadFromValues(t *testing.T) {
	type args struct {
		values []interface{}
	}
	tests := []struct {
		name            string
		args            args
		decimalType     CHColumnType
		wantDataWritten []interface{}
		wantValueString []string
		wantRowsRead    int
		wantErr         bool
	}{
		{
			name:        "Should write data and return number of rows read with no error for float64",
			decimalType: "Decimal(18,5)",
			args: args{
				values: []interface{}{},
			},
			wantDataWritten: nil,
			wantValueString: []string{},
			wantRowsRead:    0,
			wantErr:         false,
		},
		{
			name:        "Should write data and return number of rows read with no error for float64",
			decimalType: "Decimal(18,5)",
			args: args{
				values: []interface{}{float64(122), float64(123)},
			},
			wantDataWritten: nil,
			wantValueString: []string{"122", "123"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:        "Should write data and return number of rows read with no error for float32",
			decimalType: "Decimal(18,5)",
			args: args{
				values: []interface{}{float32(122), float32(123)},
			},
			wantValueString: []string{"122", "123"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:        "Should write data and return number of rows read with no error for int8",
			decimalType: "Decimal(18,5)",
			args: args{
				values: []interface{}{int8(122), int8(123)},
			},
			wantValueString: []string{"122", "123"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:        "Should write data and return number of rows read with no error for int16",
			decimalType: "Decimal(18,5)",
			args: args{
				values: []interface{}{int16(122), int16(123)},
			},
			wantValueString: []string{"122", "123"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:        "Should write data and return number of rows read with no error for int32",
			decimalType: "Decimal(18,5)",
			args: args{
				values: []interface{}{int32(122), int32(123)},
			},
			wantValueString: []string{"122", "123"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:        "Should write data and return number of rows read with no error for int",
			decimalType: "Decimal(18,5)",
			args: args{
				values: []interface{}{int(122), int(123)},
			},
			wantValueString: []string{"122", "123"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:        "Should write data and return number of rows read with no error for int64",
			decimalType: "Decimal(18,5)",
			args: args{
				values: []interface{}{int64(122), int64(123)},
			},
			wantRowsRead: 2,
			wantErr:      false,
		},
		{
			name:        "Should write data and return number of rows read with no error for empty data",
			decimalType: "Decimal(18,5)",
			args: args{
				values: []interface{}{},
			},
			wantRowsRead: 0,
			wantErr:      false,
		},
		{
			name:        "Should throw error if precision too big",
			decimalType: "Decimal(111,5)",
			args: args{
				values: []interface{}{float32(122.23), float64(4.33333)},
			},
			wantValueString: []string{"122.23", "4.33333"},
			wantRowsRead:    0,
			wantErr:         true,
		},
		{
			name:        "Should throw error if read value not a decimal",
			decimalType: "Decimal(18,5)",
			args: args{
				values: []interface{}{"baba"},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
		{
			name:        "Should write data from big float",
			decimalType: "Decimal(38,11)",
			args: args{
				values: []interface{}{big.NewFloat(3.14159265359), big.NewFloat(3.14159265359)},
			},
			wantValueString: []string{"3.14159265359", "3.14159265359"},
			wantRowsRead:    2,
			wantErr:         false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := MustMakeColumnData(tt.decimalType, 1000)

			got, err := col.ReadFromValues(tt.args.values)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadFromValues() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.wantRowsRead {
				t.Errorf("ReadFromValues() got = %v, wantRowsRead %v", got, tt.wantRowsRead)
			}

			for idx, wantStr := range tt.wantValueString {
				if !tt.wantErr {
					assert.Equal(t, wantStr, fmt.Sprint(col.GetValue(idx)))
				}
			}
		})
	}
}

func TestDecimalColumnData_EncoderDecoder(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		decimalType CHColumnType
		decimalWant struct {
			precision int
			scale     int
		}
		wantDataWritten []string
		wantRowsRead    int
		wantErr         bool
	}{
		{
			name:        "Should write data and return number of rows read with no error, 2 rows",
			decimalType: "Decimal(18,5)",
			decimalWant: struct {
				precision int
				scale     int
			}{precision: 18, scale: 5},
			args:            []string{"122.00000", "1220.00000"},
			wantDataWritten: []string{"122.00000", "1220.00000"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:        "Should convert to scale specified, 2 rows",
			decimalType: "Decimal(18,0)",
			decimalWant: struct {
				precision int
				scale     int
			}{precision: 18, scale: 0},
			args:            []string{"122.123453232323", "122.123453232323898"},
			wantDataWritten: []string{"122", "122"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:        "Should convert decimal(38,2) with 2 decimal digit",
			decimalType: "Decimal(38,2)",
			decimalWant: struct {
				precision int
				scale     int
			}{precision: 38, scale: 2},
			args:            []string{"9.99999999"},
			wantDataWritten: []string{"9.99"},
			wantRowsRead:    1,
			wantErr:         false,
		},
		{
			name:        "Should convert decimal(38,10) with 10 decimal digit",
			decimalType: "Decimal(38,10)",
			decimalWant: struct {
				precision int
				scale     int
			}{precision: 38, scale: 10},
			args:            []string{"3.141592653589793"},
			wantDataWritten: []string{"3.1415926535"},
			wantRowsRead:    1,
			wantErr:         false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buffer bytes.Buffer
			encoder := ch_encoding.NewEncoder(&buffer)
			decoder := ch_encoding.NewDecoder(&buffer)

			// Write to encoder
			original := MustMakeColumnData(tt.decimalType, len(tt.args))
			got, err := original.ReadFromTexts(tt.args)

			require.NoError(t, err)
			require.Equal(t, got, tt.wantRowsRead)
			require.NoError(t, err)

			err = original.WriteToEncoder(encoder)
			require.NoError(t, err)

			// Read from decoder
			newCopy := MustMakeColumnData(tt.decimalType, len(tt.args))
			err = newCopy.ReadFromDecoder(decoder)

			for index, value := range tt.wantDataWritten {
				if !tt.wantErr {
					require.Equal(t, value, newCopy.GetString(index))
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
