package column

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

func TestFloat64ColumnData_ReadFromTexts(t *testing.T) {
	type args struct {
		texts []string
	}
	tests := []struct {
		name            string
		args            args
		wantDataWritten []float64
		wantRowsRead    int
		wantErr         bool
	}{
		{
			name: "Should write data and return number of rows read with no error, 2 rows",
			args: args{
				texts: []string{"122", "1220"},
			},
			wantRowsRead: 2,
			wantErr:      false,
		},
		{
			name: "Should write data and return number of rows read with no error, 2 rows",
			args: args{
				texts: []string{"", "1.22e+03"},
			},
			wantDataWritten: []float64{0, 1220},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name: "Should throw error if not float64",
			args: args{
				texts: []string{"a"},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := MustMakeColumnData(FLOAT64, 1000)

			got, err := i.ReadFromTexts(tt.args.texts)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadFromTexts() error = %v, wantErr = %v, got = %v", err, tt.wantErr, got)
				return
			}

			assert.Equal(t, got, tt.wantRowsRead)

			if len(tt.wantDataWritten) > 0 {
				for index, value := range tt.wantDataWritten {
					if !tt.wantErr {
						assert.Equal(t, value, i.GetValue(index))
					}
				}
				return
			}

			for index, value := range tt.args.texts {
				if !tt.wantErr {
					assert.Equal(t, value, i.GetString(index))
				}
			}
		})
	}
}

func TestFloat64ColumnData_ReadFromValues(t *testing.T) {
	type args struct {
		values []interface{}
	}
	tests := []struct {
		name            string
		args            args
		wantDataWritten []float64
		wantRowsRead    int
		wantErr         bool
	}{
		{
			name: "Should write data and return number of rows read with no error for float64",
			args: args{
				values: []interface{}{},
			},
			wantRowsRead: 0,
			wantErr:      false,
		},
		{
			name: "Should write data and return number of rows read with no error for float64",
			args: args{
				values: []interface{}{float64(122), float64(123)},
			},
			wantRowsRead: 2,
			wantErr:      false,
		},
		{
			name: "Should write data and return number of rows read with no error for float32",
			args: args{
				values: []interface{}{float32(122), float32(123)},
			},
			wantRowsRead: 2,
			wantErr:      false,
		},
		{
			name: "Should throw error if inconsistent type",
			args: args{
				values: []interface{}{float32(122.23), float64(4.33333)},
			},
			wantRowsRead: 1,
			wantErr:      true,
		},
		{
			name: "Should throw error if read value not float64 or subtypes",
			args: args{
				values: []interface{}{int32(3)},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := MustMakeColumnData(FLOAT64, 1000)

			got, err := i.ReadFromValues(tt.args.values)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadFromValues() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.wantRowsRead {
				t.Errorf("ReadFromValues() got = %v, wantRowsRead %v", got, tt.wantRowsRead)
			}

			for index, value := range tt.args.values {
				if !tt.wantErr {
					assert.Equal(t, fmt.Sprint(value), fmt.Sprint(i.GetValue(index)))
				}
			}
		})
	}
}

func TestFloat64ColumnData_EncoderDecoder(t *testing.T) {
	type args struct {
		texts []string
	}
	tests := []struct {
		name            string
		args            args
		wantDataWritten []float64
		wantRowsRead    int
		wantErr         bool
	}{
		{
			name: "Should write data and return number of rows read with no error, 2 rows",
			args: args{
				texts: []string{"1.22e+02", "1.22e+03"},
			},
			wantDataWritten: []float64{122, 1220},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name: "Should write data and return number of rows read with no error, 2 rows",
			args: args{
				texts: []string{"", "1.22e+03"},
			},
			wantDataWritten: []float64{0, 1220},
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
			original := MustMakeColumnData(FLOAT64, len(tt.args.texts))
			got, err := original.ReadFromTexts(tt.args.texts)
			require.NoError(t, err)
			require.Equal(t, got, tt.wantRowsRead)
			require.NoError(t, err)
			err = original.WriteToEncoder(encoder)
			require.NoError(t, err)

			// Read from decoder
			newCopy := MustMakeColumnData(FLOAT64, len(tt.args.texts))
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
