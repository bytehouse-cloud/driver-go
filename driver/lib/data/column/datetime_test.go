package column

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

func TestDateTimeColumnData_ReadFromTexts(t *testing.T) {
	type args struct {
		texts []string
	}
	tests := []struct {
		name                          string
		args                          args
		wantDataWrittenInOutputFormat []string
		wantRowsRead                  int
		wantErr                       bool
	}{
		{
			name: "Should write data in yyyy-MM-dd HH:mm:ss format and return number of rows read with no error, 1 row",
			args: args{
				texts: []string{"1970-01-02 15:04:05", "2020-01-02 15:04:05", "2019-01-01 00:00:00"},
			},
			wantDataWrittenInOutputFormat: []string{"1970-01-02 15:04:05", "2020-01-02 15:04:05", "2019-01-01 00:00:00"},
			wantRowsRead:                  3,
			wantErr:                       false,
		},
		{
			name: "Should write data in yyyy-MM-dd HH:mm:ss format with 1-9 decimal points and return number of rows read with no error, 1 row",
			args: args{
				texts: []string{"1970-01-02 15:04:05.1", "2020-01-02 15:04:05.123456", "2019-01-01 00:00:00.123456789"},
			},
			wantDataWrittenInOutputFormat: []string{"1970-01-02 15:04:05", "2020-01-02 15:04:05", "2019-01-01 00:00:00"},
			wantRowsRead:                  3,
			wantErr:                       false,
		},
		{
			name: "give inconsistent format then throw error",
			args: args{
				texts: []string{"1970-01-02", "2020-01-02", "2020-01-02 15:04:05"},
			},
			wantDataWrittenInOutputFormat: []string{"1970-01-02 00:00:00", "2020-01-02 00:00:00", "2020-01-02 15:04:05"},
			wantRowsRead:                  3,
			wantErr:                       true,
		},
		{
			name: "Should write data and return number of rows read with no error if empty string",
			args: args{
				texts: []string{"", "1970-01-02 15:04:05", "2020-01-02 15:04:05"},
			},
			wantDataWrittenInOutputFormat: []string{zeroTime.String()[:19], "1970-01-02 15:04:05", "2020-01-02 15:04:05"},
			wantRowsRead:                  3,
			wantErr:                       false,
		},
		{
			name: "Should throw error if invalid time format",
			args: args{
				texts: []string{"1970-01-02 15:04:05", "2020-01-02pp 15:04:05"},
			},
			wantRowsRead: 1,
			wantErr:      true,
		},
		{
			name: "Should throw error if time is earlier than 1970-01-01 00:00:00",
			args: args{
				texts: []string{"1969-01-01 00:00:00", "1970-01-02 00:00:00"},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
		{
			name: "Should throw error if time is later than 2106-02-07 06:28:15",
			args: args{
				texts: []string{"1970-01-01 00:00:01", "2022-08-14 00:00:00", "2106-02-07 06:28:16"},
			},
			wantRowsRead: 2,
			wantErr:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := MustMakeColumnData(DATETIME, 1000)

			got, err := i.ReadFromTexts(tt.args.texts)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if got != tt.wantRowsRead {
				t.Errorf("ReadFromTexts() got = %v, wantRowsRead %v", got, tt.wantRowsRead)
			}

			if len(tt.wantDataWrittenInOutputFormat) > 0 {
				for index, value := range tt.wantDataWrittenInOutputFormat {
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

func TestDateTimeColumnData_ReadFromValues(t *testing.T) {
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
					time.Now(), time.Now(),
				},
			},
			want:    2,
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
			d := MustMakeColumnData(DATETIME, 1000)
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

func TestDateTimeColumnData_EncoderDecoder(t *testing.T) {
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
			name: "Should write data and return number of rows read with no error, 1 row",
			args: args{
				texts: []string{"1970-01-02 15:04:05", "2020-01-02 15:04:05", "2019-01-01 00:00:00"},
			},
			wantRowsRead: 3,
			wantErr:      false,
		},
		{
			name: "Given inconsistent format then throw error",
			args: args{
				texts: []string{"1970-01-02", "2020-01-02", "2020-01-02 15:04:05"},
			},
			wantDataWritten: []string{"1970-01-02 00:00:00", "2020-01-02 00:00:00", "2020-01-02 15:04:05"},
			wantRowsRead:    3,
			wantErr:         true,
		},
		{
			name: "Should write data and return number of rows read with no error if empty string",
			args: args{
				texts: []string{"", "1970-01-02 15:04:05", "2020-01-02 15:04:05"},
			},
			wantDataWritten: []string{zeroTime.String()[:19], "1970-01-02 15:04:05", "2020-01-02 15:04:05"},
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
			original := MustMakeColumnData(DATETIME, len(tt.args.texts))
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
			newCopy := MustMakeColumnData(DATETIME, len(tt.args.texts))
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
