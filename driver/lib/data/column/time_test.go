package column

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
)

func TestTimeColumnData_ReadFromTexts(t *testing.T) {
	type args struct {
		texts []string
	}
	tests := []struct {
		name            string
		args            args
		wantDataWritten []string
		wantRowsRead    int
		wantErr         bool
	}{
		{
			name: "Should write data and return number of rows read with no error",
			args: args{
				texts: []string{"15:04:05.123", "15:04:05.222", "00:00:00.333"},
			},
			wantRowsRead: 3,
			wantErr:      false,
		},
		{
			name: "Should write data and return number of rows read with no error, empty string",
			args: args{
				texts: []string{"", "15:04:05.456", "null"},
			},
			wantDataWritten: []string{"00:00:00.000", "15:04:05.456", "00:00:00.000"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name: "Should throw error if invalid time format",
			args: args{
				texts: []string{"15:014:05a", "5:04:05"},
			},
			wantRowsRead: 1,
			wantErr:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := MustMakeColumnData("Time(3)", 1000)

			got, err := i.ReadFromTexts(tt.args.texts)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if got != tt.wantRowsRead {
				t.Errorf("ReadFromTexts() got = %v, wantRowsRead %v", got, tt.wantRowsRead)
			}

			if len(tt.wantDataWritten) > 0 {
				for index, value := range tt.wantDataWritten {
					if !tt.wantErr {
						assert.Equal(t, value, i.GetString(index))
					}
				}
				return
			}

			for index, value := range tt.args.texts {
				if !tt.wantErr {
					// Only check if is same date, ignore time value as there may be time zone differences
					assert.Equal(t, value, i.GetString(index))
				}
			}
		})
	}
}

func TestTimeColumnData_ReadFromValues(t *testing.T) {
	type args struct {
		values []interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    int
		wantErr bool
	}{
		{
			name: "Should return the same time value",
			args: args{
				values: []interface{}{
					time.Now(),
				},
			},
			want:    1,
			wantErr: false,
		},
		{
			name: "Should return the same time value",
			args: args{
				values: []interface{}{
					nil,
				},
			},
			want:    1,
			wantErr: false,
		},
		{
			name: "Should throw error if one of the values is not time.Time",
			args: args{
				values: []interface{}{
					time.Now(), 123,
				},
			},
			want:    1,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := MustMakeColumnData("Time(6)", 1000)
			got, err := d.ReadFromValues(tt.args.values)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadFromValues() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ReadFromValues() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetDecimalFromTimeAndReverse(t *testing.T) {

	testDate := time.Date(1970, 1, 1, 12, 30, 15, int(123*time.Millisecond), time.UTC)

	scale := 3
	val, err := getDecimalFromTime(testDate, scale)
	if err != nil {
		t.Fatal(err)
	}
	f := val.InexactFloat64()
	fmt.Println(f)
	// 45015.123
	decodedTime := getTimeFromDecimal(val)
	assert.Equal(t, testDate.Hour(), decodedTime.Hour())
	assert.Equal(t, testDate.Minute(), decodedTime.Minute())
	assert.Equal(t, testDate.Second(), decodedTime.Second())
	assert.Equal(t, testDate.Nanosecond(), decodedTime.Nanosecond())
}

func TestTimeColumnData_EncoderDecoder(t *testing.T) {
	type args struct {
		texts []string
	}
	tests := []struct {
		name             string
		args             args
		wantDataWritten  []string
		wantRowsRead     int
		wantErr          bool
		columnDefinition string
	}{
		{
			name: "Should write data and return number of rows read with no error",
			args: args{
				texts: []string{"15:04:05.1000", "18:04:05.1000", "00:00:00.1000"},
			},
			wantRowsRead:     3,
			wantErr:          false,
			columnDefinition: "Time(4)",
		},
		{
			name: "Should write data and return number of rows read with no error, empty string",
			args: args{
				texts: []string{"15:04:05.5123"},
			},
			wantDataWritten:  []string{"15:04:05.5123"},
			wantRowsRead:     1,
			wantErr:          false,
			columnDefinition: "Time(4)",
		},
		{
			name: "Should write data and return number of rows read with no error, empty string",
			args: args{
				texts: []string{"15:04:05.3"},
			},
			wantDataWritten:  []string{"15:04:05.3"},
			wantRowsRead:     1,
			wantErr:          false,
			columnDefinition: "Time(1)",
		},
		{
			name: "Should write data and return number of rows read with no error, empty string",
			args: args{
				texts: []string{"15:04:05.003"},
			},
			wantDataWritten:  []string{"15:04:05.003"},
			wantRowsRead:     1,
			wantErr:          false,
			columnDefinition: "Time(3)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buffer bytes.Buffer
			encoder := ch_encoding.NewEncoder(&buffer)
			decoder := ch_encoding.NewDecoder(&buffer)

			// Write to encoder
			original := MustMakeColumnData(CHColumnType(tt.columnDefinition), len(tt.args.texts))
			got, err := original.ReadFromTexts(tt.args.texts)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, got, tt.wantRowsRead)
			require.NoError(t, err)
			err = original.WriteToEncoder(encoder)
			require.NoError(t, err)

			// Read from decoder
			newCopy := MustMakeColumnData(CHColumnType(tt.columnDefinition), len(tt.args.texts))
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
