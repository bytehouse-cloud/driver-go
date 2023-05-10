package column

import (
	"bytes"
	"testing"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBoolColumnData_ReadFromTexts(t *testing.T) {
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
				texts: []string{"", "null"},
			},
			wantDataWritten: []string{"0", "0"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name: "Should write data and return number of rows read with no error",
			args: args{
				texts: []string{"1", "0", "true", "false"},
			},
			wantDataWritten: []string{"1", "0", "1", "0"},
			wantRowsRead:    4,
			wantErr:         false,
		},
		{
			name: "Should write data and return number of rows read with no error",
			args: args{
				texts: []string{"'True'", "'T'", "'Y'", "'Yes'", "'On'", "'Enable'", "'Enabled'", "'False'", "'F'", "'N'", "'No'", "'Off'", "'Disable'", "'Disabled'"},
			},
			wantDataWritten: []string{"1", "1", "1", "1", "1", "1", "1", "0", "0", "0", "0", "0", "0", "0", "0"},
			wantRowsRead:    14,
			wantErr:         false,
		},
		{
			name: "Should throw error if invalid Bool format",
			args: args{
				texts: []string{"Tr"},
			},
			wantRowsRead: 1,
			wantErr:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := MustMakeColumnData("Bool", 1000)

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
		})
	}
}

func TestBoolColumnData_ReadFromValues(t *testing.T) {
	type args struct {
		values []interface{}
	}
	tests := []struct {
		name            string
		args            args
		want            int
		wantErr         bool
		wantDataWritten []string
	}{
		{
			name: "Should return the same value",
			args: args{
				values: []interface{}{
					true, false, nil,
				},
			},
			wantDataWritten: []string{"1", "0", "0"},
			want:            3,
			wantErr:         false,
		},
		{
			name: "Should throw error if one of the values is not bool",
			args: args{
				values: []interface{}{
					true, 1,
				},
			},
			want:    1,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := MustMakeColumnData("Bool", 1000)
			got, err := d.ReadFromValues(tt.args.values)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadFromValues() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ReadFromValues() got = %v, want %v", got, tt.want)
			}

			if len(tt.wantDataWritten) > 0 {
				for index, value := range tt.wantDataWritten {
					if !tt.wantErr {
						assert.Equal(t, value, d.GetString(index))
					}
				}
				return
			}
		})
	}
}

func TestBoolColumnData_EncoderDecoder(t *testing.T) {
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
				texts: []string{"1", "0", "true", "false"},
			},
			wantRowsRead: 4,
			wantErr:      false,
		},
		{
			name: "Given different format then no error",
			args: args{
				texts: []string{"'True'", "'T'", "'Y'", "'Yes'", "'On'", "'Enable'", "'Enabled'", "'False'", "'F'", "'N'", "'No'", "'Off'", "'Disable'", "'Disabled'", ""},
			},
			wantDataWritten: []string{"1", "1", "1", "1", "1", "1", "1", "0", "0", "0", "0", "0", "0", "0", "0"},
			wantRowsRead:    15,
			wantErr:         false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buffer bytes.Buffer
			encoder := ch_encoding.NewEncoder(&buffer)
			decoder := ch_encoding.NewDecoder(&buffer)

			// Write to encoder
			original := MustMakeColumnData("Bool", len(tt.args.texts))
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
			newCopy := MustMakeColumnData("Bool", len(tt.args.texts))
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
