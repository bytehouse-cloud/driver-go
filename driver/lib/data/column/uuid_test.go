package column

import (
	"bytes"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

func TestUUIDColumnData_ReadFromTexts(t *testing.T) {
	type args struct {
		texts []string
	}
	tests := []struct {
		name            string
		args            args
		wantDataWritten []uuid.UUID
		wantRowsRead    int
		wantErr         bool
	}{
		{
			name: "Should write data and return number of rows read with no error, 2 rows",
			args: args{
				texts: []string{"123e4567-e89b-12d3-a456-426614174333", "123e4567-e89b-12d3-a456-426614174000"},
			},
			wantRowsRead: 2,
			wantErr:      false,
		},
		{
			name: "Should write data and return number of rows read with no error if empty string, 2 rows",
			args: args{
				texts: []string{"", "123e4567-e89b-12d3-a456-426614174000"},
			},
			wantDataWritten: []uuid.UUID{zeroUUID, uuid.MustParse("123e4567-e89b-12d3-a456-426614174000")},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name: "Should throw error if one uuid wrong format, 1 rows",
			args: args{
				texts: []string{"123e4567-e89b-12d3-a456-426614174333", "123e4567-e89b-12d3-a456-"},
			},
			wantRowsRead: 1,
			wantErr:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := MustMakeColumnData(UUID, 1000)

			got, err := i.ReadFromTexts(tt.args.texts)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadFromTexts() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.wantRowsRead {
				t.Errorf("ReadFromTexts() got = %v, wantRowsRead %v", got, tt.wantRowsRead)
			}

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

func TestUUIDColumnData_ReadFromValues(t *testing.T) {
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
			name: "Should return the same uuids",
			args: args{
				values: []interface{}{
					uuid.New(), uuid.New(),
				},
			},
			want:    2,
			wantErr: false,
		},
		{
			name: "Should throw error if one of the values is not uuid",
			args: args{
				values: []interface{}{
					uuid.New(), 123,
				},
			},
			want:    1,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := MustMakeColumnData(UUID, 1000)
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

func TestUUIDColumnData_EncoderDecoder(t *testing.T) {
	type args struct {
		texts []string
	}
	tests := []struct {
		name            string
		args            args
		wantDataWritten []uuid.UUID
		wantRowsRead    int
		wantErr         bool
	}{
		{
			name: "Should write data and return number of rows read with no error, 2 rows",
			args: args{
				texts: []string{"123e4567-e89b-12d3-a456-426614174333", "123e4567-e89b-12d3-a456-426614174000"},
			},
			wantDataWritten: []uuid.UUID{uuid.MustParse("123e4567-e89b-12d3-a456-426614174333"), uuid.MustParse("123e4567-e89b-12d3-a456-426614174000")},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name: "Should write data and return number of rows read with no error if empty string, 2 rows",
			args: args{
				texts: []string{"", "123e4567-e89b-12d3-a456-426614174000"},
			},
			wantDataWritten: []uuid.UUID{zeroUUID, uuid.MustParse("123e4567-e89b-12d3-a456-426614174000")},
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
			original := MustMakeColumnData(UUID, len(tt.args.texts))
			got, err := original.ReadFromTexts(tt.args.texts)
			require.NoError(t, err)
			require.Equal(t, got, tt.wantRowsRead)
			require.NoError(t, err)
			err = original.WriteToEncoder(encoder)
			require.NoError(t, err)

			// Read from decoder
			newCopy := MustMakeColumnData(UUID, len(tt.args.texts))
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
