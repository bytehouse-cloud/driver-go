package column

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

func TestStringColumnData_ReadFromTexts(t *testing.T) {
	type args struct {
		texts []string
	}
	tests := []struct {
		name            string
		args            args
		wantDataWritten []int8
		wantRead        []string
		wantRowsRead    int
		wantErr         bool
	}{
		{
			name: "Should write data and return number of rows read with no error, 1 row",
			args: args{
				texts: []string{"1970-01-02 15:04:05jfoiejwofjeowijewofijweofjewfoiejwfoe", "2020-01-02 15:04:05"},
			},
			wantRead:     []string{"1970-01-02 15:04:05jfoiejwofjeowijewofijweofjewfoiejwfoe", "2020-01-02 15:04:05"},
			wantRowsRead: 2,
			wantErr:      false,
		},
		{
			name: "Should write data and return number of rows read with no error, 1 row",
			args: args{
				texts: []string{"'1970-01-02 15:04:05jfoiejwofjeowijewofijweofjewfoiejwfoe'", "'2020-01-02 15:04:05'"},
			},
			wantRead:     []string{"1970-01-02 15:04:05jfoiejwofjeowijewofijweofjewfoiejwfoe", "2020-01-02 15:04:05"},
			wantRowsRead: 2,
			wantErr:      false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := MustMakeColumnData(STRING, 1000)

			got, err := i.ReadFromTexts(tt.args.texts)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadFromTexts() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.wantRowsRead {
				t.Errorf("ReadFromTexts() got = %v, wantRowsRead %v", got, tt.wantRowsRead)
			}

			for index, value := range tt.wantRead {
				if !tt.wantErr {
					assert.Equal(t, value, i.GetString(index))
				}
			}
		})
	}
}

func TestStringColumnData_ReadFromValues(t *testing.T) {
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
			name: "Should return the same strings",
			args: args{
				values: []interface{}{
					"fewfweewf", "fewfwe",
				},
			},
			want:    2,
			wantErr: false,
		},
		{
			name: "Should throw error if one of the values is not string",
			args: args{
				values: []interface{}{
					"poooo", 123,
				},
			},
			want:    1,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := MustMakeColumnData(STRING, 1000)
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

func TestStringColumnData_EncoderDecoder(t *testing.T) {
	type args struct {
		texts []string
	}
	tests := []struct {
		name       string
		args       args
		columnType CHColumnType
		wantRead   []string
		numRows    int
		wantErr    bool
	}{
		{
			name:       "Should write data and return number of rows read with no error",
			columnType: STRING,
			args: args{
				texts: []string{"1970-01-02 15:04:05jfoiejwofjeowijewofijweofjewfoiejwfoe", "2020-01-02 15:04:05"},
			},
			wantRead: []string{"1970-01-02 15:04:05jfoiejwofjeowijewofijweofjewfoiejwfoe", "2020-01-02 15:04:05"},
		},
		{
			name:       "Should write data and return number of rows read with no error",
			columnType: STRING,
			args: args{
				texts: []string{"'1970-01-02 15:04:05jfoiejwofjeowijewofijweofjewfoiejwfoe'", "'2020-01-02 15:04:05' "},
			},
			wantRead: []string{"1970-01-02 15:04:05jfoiejwofjeowijewofijweofjewfoiejwfoe", "'2020-01-02 15:04:05' "},
		},
		{
			name:       "Should write data and return number of rows read with no error, 50k rows",
			columnType: STRING,
			args: args{
				texts: func() []string {
					arr := make([]string, 5e4)
					for i := 0; i < 5e4; i++ {
						arr[i] = "hello"
					}
					return arr
				}(),
			},
			wantRead: func() []string {
				arr := make([]string, 5e4)
				for i := 0; i < 5e4; i++ {
					arr[i] = "hello"
				}
				return arr
			}(),
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

			newCopy := MustMakeColumnData(tt.columnType, len(tt.wantRead))
			err = newCopy.ReadFromDecoder(decoder)
			for index, value := range tt.wantRead {
				if !tt.wantErr {
					assert.Equal(t, value, newCopy.GetString(index))
					assert.Equal(t, value, newCopy.GetValue(index))
				}
			}
			require.Equal(t, newCopy.Len(), original.Len())

			require.NoError(t, original.Close())
			require.NoError(t, newCopy.Close())
		})
	}
}
