package column

import (
	"bytes"
	"testing"
	"time"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDate32ColumnData_ReadFromTexts(t *testing.T) {
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
			name: "Should write data in yyyy-MM-dd format and return number of rows read with no error",
			args: args{
				texts: []string{"1970-01-02", "2020-01-02", "2019-01-01"},
			},
			wantRowsRead: 3,
			wantErr:      false,
		},
		{
			name: "Should write max and min data in yyyy-MM-dd format and return number of rows read with no error",
			args: args{
				texts: []string{"2299-12-31", "1900-01-01"},
			},
			wantDataWritten: []string{"2299-12-31", "1900-01-01"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name: "Should write data in yyyy-M-d format and return number of rows read with no error",
			args: args{
				texts: []string{"1970-1-2", "2020-1-20", "2019-12-1"},
			},
			wantDataWritten: []string{"1970-01-02", "2020-01-20", "2019-12-01"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name: "Should write data in dd-MM-yyyy format and return number of rows read with no error",
			args: args{
				texts: []string{"02-01-1970", "20-01-2020", "01-12-2019"},
			},
			wantDataWritten: []string{"1970-01-02", "2020-01-20", "2019-12-01"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name: "Should write data in d-M-yyyy format and return number of rows read with no error",
			args: args{
				texts: []string{"2-1-1970", "20-1-2020", "1-12-2019"},
			},
			wantDataWritten: []string{"1970-01-02", "2020-01-20", "2019-12-01"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name: "Should write data in yyyy/MM/dd format and return number of rows read with no error",
			args: args{
				texts: []string{"1970/01/02", "2020/01/20", "2019/12/01"},
			},
			wantDataWritten: []string{"1970-01-02", "2020-01-20", "2019-12-01"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name: "Should write data in yyyy/M/d format and return number of rows read with no error",
			args: args{
				texts: []string{"1970/1/2", "2020/1/20", "2019/12/1"},
			},
			wantDataWritten: []string{"1970-01-02", "2020-01-20", "2019-12-01"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name: "Should write data in dd/MM/yyyy format and return number of rows read with no error",
			args: args{
				texts: []string{"02/01/1970", "20/01/2020", "01/12/2019"},
			},
			wantDataWritten: []string{"1970-01-02", "2020-01-20", "2019-12-01"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name: "Should write data in d/M/yyyy format and return number of rows read with no error",
			args: args{
				texts: []string{"2/1/1970", "20/1/2020", "1/12/2019"},
			},
			wantDataWritten: []string{"1970-01-02", "2020-01-20", "2019-12-01"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name: "Should write data in yyyy-Mon-dd format and return number of rows read with no error",
			args: args{
				texts: []string{"1970-Jan-02", "2020-jan-20", "2019-DEC-01"},
			},
			wantDataWritten: []string{"1970-01-02", "2020-01-20", "2019-12-01"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name: "Should write data in yyyy-Mon-d format and return number of rows read with no error",
			args: args{
				texts: []string{"1970-Jan-2", "2020-jan-20", "2019-DEC-1"},
			},
			wantDataWritten: []string{"1970-01-02", "2020-01-20", "2019-12-01"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name: "Should write data in dd-Mon-yyyy format and return number of rows read with no error",
			args: args{
				texts: []string{"02-Jan-1970", "20-jan-2020", "01-DEC-2019"},
			},
			wantDataWritten: []string{"1970-01-02", "2020-01-20", "2019-12-01"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name: "Should write data in d-Mon-yyyy format and return number of rows read with no error",
			args: args{
				texts: []string{"2-Jan-1970", "20-jan-2020", "1-DEC-2019"},
			},
			wantDataWritten: []string{"1970-01-02", "2020-01-20", "2019-12-01"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name: "Should write data in Mon-dd-yyyy format and return number of rows read with no error",
			args: args{
				texts: []string{"Jan-02-1970", "jan-20-2020", "DEC-01-2019"},
			},
			wantDataWritten: []string{"1970-01-02", "2020-01-20", "2019-12-01"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name: "Should write data in Mon-d-yyyy format and return number of rows read with no error",
			args: args{
				texts: []string{"Jan-2-1970", "jan-20-2020", "DEC-1-2019"},
			},
			wantDataWritten: []string{"1970-01-02", "2020-01-20", "2019-12-01"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name: "Should write data in yyyy/Mon/dd format and return number of rows read with no error",
			args: args{
				texts: []string{"1970/Jan/02", "2020/jan/20", "2019/DEC/01"},
			},
			wantDataWritten: []string{"1970-01-02", "2020-01-20", "2019-12-01"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name: "Should write data in yyyy/Mon/d format and return number of rows read with no error",
			args: args{
				texts: []string{"1970/Jan/2", "2020/jan/20", "2019/DEC/1"},
			},
			wantDataWritten: []string{"1970-01-02", "2020-01-20", "2019-12-01"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name: "Should write data in dd/Mon/yyyy format and return number of rows read with no error",
			args: args{
				texts: []string{"02/Jan/1970", "20/jan/2020", "01/DEC/2019"},
			},
			wantDataWritten: []string{"1970-01-02", "2020-01-20", "2019-12-01"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name: "Should write data in d/Mon/yyyy format and return number of rows read with no error",
			args: args{
				texts: []string{"2/Jan/1970", "20/jan/2020", "1/DEC/2019"},
			},
			wantDataWritten: []string{"1970-01-02", "2020-01-20", "2019-12-01"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name: "Should write data in Mon/dd/yyyy format and return number of rows read with no error",
			args: args{
				texts: []string{"Jan/02/1970", "jan/20/2020", "DEC/01/2019"},
			},
			wantDataWritten: []string{"1970-01-02", "2020-01-20", "2019-12-01"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name: "Should write data in Mon/d/yyyy format and return number of rows read with no error",
			args: args{
				texts: []string{"Jan/2/1970", "jan/20/2020", "DEC/1/2019"},
			},
			wantDataWritten: []string{"1970-01-02", "2020-01-20", "2019-12-01"},
			wantRowsRead:    3,
			wantErr:         false,
		},

		{
			name: "Should write data in yyyy-Month-dd format and return number of rows read with no error",
			args: args{
				texts: []string{"1970-January-02", "2020-january-20", "2019-DECEMBER-01"},
			},
			wantDataWritten: []string{"1970-01-02", "2020-01-20", "2019-12-01"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name: "Should write data in yyyy-Month-d format and return number of rows read with no error",
			args: args{
				texts: []string{"1970-January-2", "2020-january-20", "2019-DECEMBER-1"},
			},
			wantDataWritten: []string{"1970-01-02", "2020-01-20", "2019-12-01"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name: "Should write data in dd-Month-yyyy format and return number of rows read with no error",
			args: args{
				texts: []string{"02-January-1970", "20-january-2020", "01-DECEMBER-2019"},
			},
			wantDataWritten: []string{"1970-01-02", "2020-01-20", "2019-12-01"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name: "Should write data in d-Month-yyyy format and return number of rows read with no error",
			args: args{
				texts: []string{"2-January-1970", "20-january-2020", "1-DECEMBER-2019"},
			},
			wantDataWritten: []string{"1970-01-02", "2020-01-20", "2019-12-01"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name: "Should write data in Month-dd-yyyy format and return number of rows read with no error",
			args: args{
				texts: []string{"January-02-1970", "january-20-2020", "DECEMBER-01-2019"},
			},
			wantDataWritten: []string{"1970-01-02", "2020-01-20", "2019-12-01"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name: "Should write data in Month-d-yyyy format and return number of rows read with no error",
			args: args{
				texts: []string{"January-2-1970", "january-20-2020", "DECEMBER-1-2019"},
			},
			wantDataWritten: []string{"1970-01-02", "2020-01-20", "2019-12-01"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name: "Should write data in yyyy/Month/dd format and return number of rows read with no error",
			args: args{
				texts: []string{"1970/January/02", "2020/january/20", "2019/DECEMBER/01"},
			},
			wantDataWritten: []string{"1970-01-02", "2020-01-20", "2019-12-01"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name: "Should write data in yyyy/Month/d format and return number of rows read with no error",
			args: args{
				texts: []string{"1970/January/2", "2020/january/20", "2019/DECEMBER/1"},
			},
			wantDataWritten: []string{"1970-01-02", "2020-01-20", "2019-12-01"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name: "Should write data in dd/Month/yyyy format and return number of rows read with no error",
			args: args{
				texts: []string{"02/January/1970", "20/january/2020", "01/DECEMBER/2019"},
			},
			wantDataWritten: []string{"1970-01-02", "2020-01-20", "2019-12-01"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name: "Should write data in d/Month/yyyy format and return number of rows read with no error",
			args: args{
				texts: []string{"2/January/1970", "20/january/2020", "1/DECEMBER/2019"},
			},
			wantDataWritten: []string{"1970-01-02", "2020-01-20", "2019-12-01"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name: "Should write data in Month/dd/yyyy format and return number of rows read with no error",
			args: args{
				texts: []string{"January/02/1970", "january/20/2020", "DECEMBER/01/2019"},
			},
			wantDataWritten: []string{"1970-01-02", "2020-01-20", "2019-12-01"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name: "Should write data in Month/d/yyyy format and return number of rows read with no error",
			args: args{
				texts: []string{"January/2/1970", "january/20/2020", "DECEMBER/1/2019"},
			},
			wantDataWritten: []string{"1970-01-02", "2020-01-20", "2019-12-01"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name: "Should write data in yyyyMMdd format and return number of rows read with no error",
			args: args{
				texts: []string{"19700102", "20200120", "20191201"},
			},
			wantDataWritten: []string{"1970-01-02", "2020-01-20", "2019-12-01"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name: "Should write data in ddMMyyyy format and return number of rows read with no error",
			args: args{
				texts: []string{"02011970", "20012020", "01122019"},
			},
			wantDataWritten: []string{"1970-01-02", "2020-01-20", "2019-12-01"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name: "Given inconsistent format then throw error",
			args: args{
				texts: []string{"'1970-01-02'", "2020-01-02", "2020-01-02 15:04:05"},
			},
			wantDataWritten: []string{"1970-01-02", "2020-01-02", "2020-01-02"},
			wantRowsRead:    3,
			wantErr:         true,
		},
		{
			name: "Should write data and return number of rows read with no error if empty string",
			args: args{
				texts: []string{"", "1970-01-02", "2020-01-02", "null"},
			},
			wantDataWritten: []string{"1970-01-01", "1970-01-02", "2020-01-02", "1970-01-01"},
			wantRowsRead:    4,
			wantErr:         false,
		},
		{
			name: "Should throw error if invalid time format",
			args: args{
				texts: []string{"1970-01-02", "2020-01-02pp"},
			},
			wantRowsRead: 1,
			wantErr:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := MustMakeColumnData(DATE32, 1000)

			got, err := i.ReadFromTexts(tt.args.texts)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
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

func TestDateColumnData32_ReadFromValues(t *testing.T) {
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
					time.Now(), time.Now(), nil,
				},
			},
			want:    3,
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
			d := MustMakeColumnData(DATE32, 1000)
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

func TestDateColumnData32_EncoderDecoder(t *testing.T) {
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
				texts: []string{"1970-01-02", "2020-01-02", "2019-01-01"},
			},
			wantDataWritten: []string{"1970-01-02", "2020-01-02", "2019-01-01"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name: "Should write max and min data in yyyy-MM-dd format and return number of rows read with no error",
			args: args{
				texts: []string{"2299-12-31", "1900-01-01"},
			},
			wantDataWritten: []string{"2299-12-31", "1900-01-01"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name: "Given inconsistent format then throw error",
			args: args{
				texts: []string{"'1970-01-02'", "2020-01-02", "2020-01-02 15:04:05"},
			},
			wantDataWritten: []string{"1970-01-02", "2020-01-02", "2020-01-02"},
			wantRowsRead:    3,
			wantErr:         true,
		},
		{
			name: "Should write data and return number of rows read with no error if empty string",
			args: args{
				texts: []string{"", "1970-01-02", "2020-01-02"},
			},
			wantDataWritten: []string{"1970-01-01", "1970-01-02", "2020-01-02"},
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
			original := MustMakeColumnData(DATE32, len(tt.args.texts))
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
			newCopy := MustMakeColumnData(DATE32, len(tt.args.texts))
			err = newCopy.ReadFromDecoder(decoder)

			for index, value := range tt.wantDataWritten {
				if !tt.wantErr {
					require.Equal(t, value, newCopy.GetString(index))
					require.Equal(t, value, newCopy.GetValue(index).(time.Time).String()[:10])
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
