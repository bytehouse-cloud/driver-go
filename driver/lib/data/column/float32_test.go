package column

import (
	"bytes"
	"testing"

	"github.com/pkg/profile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

func TestFloat32ColumnData_ReadFromTexts(t *testing.T) {
	type args struct {
		texts []string
	}
	tests := []struct {
		name            string
		args            args
		wantDataWritten []float32
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
				texts: []string{"", "1.22e+03", "null"},
			},
			wantDataWritten: []float32{0, 1220, 0},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name: "Should throw error if not float32",
			args: args{
				texts: []string{"a"},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := MustMakeColumnData(FLOAT32, 1000)

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

func TestFloat32ColumnData_ReadFromValues(t *testing.T) {
	type args struct {
		values []interface{}
	}
	tests := []struct {
		name            string
		args            args
		wantDataWritten []float32
		wantRowsRead    int
		wantErr         bool
	}{
		{
			name: "Should write data and return number of rows read with no error, 2 rows",
			args: args{
				values: []interface{}{float32(122), float32(123), nil},
			},
			wantRowsRead: 3,
			wantErr:      false,
		},
		{
			name: "Should throw error if type is not float32",
			args: args{
				values: []interface{}{float32(122), float64(123)},
			},
			wantRowsRead: 1,
			wantErr:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := MustMakeColumnData(FLOAT32, 1000)

			got, err := i.ReadFromValues(tt.args.values)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadFromValues() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.wantRowsRead {
				t.Errorf("ReadFromValues() got = %v, wantRowsRead %v", got, tt.wantRowsRead)
			}

			for index, value := range tt.args.values {
				if value == nil {
					value = float32(0)
				}
				if !tt.wantErr && value != i.GetValue(index) {
					t.Errorf("ReadFromValues(), written data differs")
				}
			}
		})
	}
}

func Benchmark_Float32ColumnData_ReadFromTexts(b *testing.B) {
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
			name:       "Should write data and return number of rows read with no error", // fastfloat 135502480 ns/op
			columnType: "Float32",
			args: args{
				texts: func() []string {
					str := make([]string, 1e+7)
					for i := 0; i < 1e+7; i++ {
						str[i] = "0.122432"
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

func TestFloat32ColumnData_EncoderDecoder(t *testing.T) {
	type args struct {
		texts []string
	}
	tests := []struct {
		name            string
		args            args
		wantDataWritten []float32
		wantRowsRead    int
		wantErr         bool
	}{
		{
			name: "Should write data and return number of rows read with no error, 2 rows",
			args: args{
				texts: []string{"1.22e+02", "1.22e+03"},
			},
			wantRowsRead: 2,
			wantErr:      false,
		},
		{
			name: "Should write data and return number of rows read with no error, 2 rows",
			args: args{
				texts: []string{"", "1.22e+03"},
			},
			wantDataWritten: []float32{0, 1220},
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
			original := MustMakeColumnData(FLOAT32, len(tt.args.texts))
			got, err := original.ReadFromTexts(tt.args.texts)
			require.NoError(t, err)
			require.Equal(t, got, tt.wantRowsRead)
			require.NoError(t, err)
			err = original.WriteToEncoder(encoder)
			require.NoError(t, err)

			// Read from decoder
			newCopy := MustMakeColumnData(FLOAT32, len(tt.args.texts))
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
