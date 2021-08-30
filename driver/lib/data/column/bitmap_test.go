package column

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

func TestBitmapColumnData_ReadFromTexts(t *testing.T) {
	type args struct {
		texts []string
	}
	tests := []struct {
		name            string
		columnType      CHColumnType
		args            args
		wantDataWritten [][]uint64
		wantRowsRead    int
		wantErr         bool
	}{
		{
			name:       "Should write data and remove duplicates with no error, 3 rows",
			columnType: BITMAP64,
			args: args{
				texts: []string{"[1, 2, 2]", "[1, 3, 3,44,3]", "[1, 33, 33, 3]"},
			},
			wantDataWritten: [][]uint64{{1, 2}, {1, 3, 44}, {1, 3, 33}},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name:       "Should write data and remove duplicates with no error, 3 rows",
			columnType: BITMAP64,
			args: args{
				texts: []string{"[1000000000, 1, 2, 2, 1000, 1000000000, 1000000000, 10000000000, 10000000000]", "[1, 3, 3,44,3]", "[1, 33, 33, 3]"},
			},
			wantDataWritten: [][]uint64{{1, 2, 1000, 1000000000, 10000000000}, {1, 3, 44}, {1, 3, 33}},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name:       "Should throw error if not uint64",
			columnType: BITMAP64,
			args: args{
				texts: []string{"[1000000000, 1, 2, 2, 1000, 1000000000, 1000000000, 10000000000, 10000000000]", "[1, 3, 3,44,3]", "[-1, 1, 33, 33, 3]"},
			},
			wantDataWritten: [][]uint64{{1, 2, 1000, 1000000000, 10000000000}, {1, 3, 44}},
			wantRowsRead:    2,
			wantErr:         true,
		},
		{
			name:       "Should throw error if not array",
			columnType: BITMAP64,
			args: args{
				texts: []string{"[1000000000, 1, 2, 2, 1000, 1000000000, 1000000000, 10000000000, 10000000000]", "1, 23"},
			},
			wantDataWritten: [][]uint64{{1, 2, 1000, 1000000000, 10000000000}},
			wantRowsRead:    1,
			wantErr:         true,
		},
		{
			name:       "Should return empty array",
			columnType: BITMAP64,
			args: args{
				texts: []string{"[]", "[1, 100]"},
			},
			wantDataWritten: [][]uint64{{}, {1, 100}},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:       "Should return empty arrays",
			columnType: BITMAP64,
			args: args{
				texts: []string{"", "[1, 100]", ""},
			},
			wantDataWritten: [][]uint64{{}, {1, 100}, {}},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name:       "Should return empty array",
			columnType: BITMAP64,
			args: args{
				texts: []string{"[]"},
			},
			wantDataWritten: [][]uint64{{}},
			wantRowsRead:    1,
			wantErr:         false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := MustMakeColumnData(tt.columnType, len(tt.args.texts))

			got, err := i.ReadFromTexts(tt.args.texts)
			fmt.Println(err)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadFromTexts() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			assert.Equal(t, tt.wantRowsRead, got)

			for index, value := range tt.wantDataWritten {
				assert.ElementsMatch(t, value, i.GetValue(index))
			}
		})
	}
}

func TestBitmapColumnData_ReadFromValues(t *testing.T) {
	type args struct {
		values []interface{}
	}
	tests := []struct {
		name            string
		args            args
		columnType      CHColumnType
		wantDataWritten [][]uint64
		wantRowsRead    int
		wantErr         bool
	}{
		{
			name:       "Should write data and remove duplicates with no error, 3 rows",
			columnType: BITMAP64,
			args: args{
				values: []interface{}{[]uint64{1, 2, 2}, []uint64{1, 3, 3, 44, 3}, []uint64{1, 33, 33, 3}},
			},
			wantDataWritten: [][]uint64{{1, 2}, {1, 3, 44}, {1, 3, 33}},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name:       "Should write data and remove duplicates with no error, 3 rows",
			columnType: BITMAP64,
			args: args{
				values: []interface{}{[]uint64{1000000000, 1, 2, 2, 1000, 1000000000, 1000000000, 10000000000, 10000000000}, []uint64{1, 3, 3, 44, 3}, []uint64{1, 33, 33, 3}},
			},
			wantDataWritten: [][]uint64{{1, 2, 1000, 1000000000, 10000000000}, {1, 3, 44}, {1, 3, 33}},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name:       "Should throw error if not uint64",
			columnType: BITMAP64,
			args: args{
				values: []interface{}{[]uint64{1000000000, 1, 2, 2, 1000, 1000000000, 1000000000, 10000000000, 10000000000}, []int{1}},
			},
			wantDataWritten: [][]uint64{{1, 2, 1000, 1000000000, 10000000000}},
			wantRowsRead:    1,
			wantErr:         true,
		},
		{
			name:       "Should throw error if nil",
			columnType: BITMAP64,
			args: args{
				values: []interface{}{nil},
			},
			wantDataWritten: [][]uint64{},
			wantRowsRead:    0,
			wantErr:         true,
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
				assert.ElementsMatch(t, value, i.GetValue(index))
			}
		})
	}
}

func TestBitmapColumnData_EncoderDecoder(t *testing.T) {
	type args struct {
		texts []string
	}
	tests := []struct {
		name              string
		columnType        CHColumnType
		args              args
		wantStringWritten map[string]bool
		wantDataWritten   [][]uint64
		wantRowsRead      int
		wantErr           bool
	}{
		{
			name:       "Should write data and remove duplicates with no error, 3 rows",
			columnType: BITMAP64,
			args: args{
				texts: []string{"[1, 2, 2]", "[1, 3, 3,44,3]", "[1, 33, 33, 3]"},
			},
			wantStringWritten: map[string]bool{
				"[1, 2]": true, "[1, 3, 44]": true, "[1, 3, 33]": true,
			},
			wantDataWritten: [][]uint64{{1, 2}, {1, 3, 44}, {1, 3, 33}},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name:       "Should write data and remove duplicates with no error, 3 rows",
			columnType: BITMAP64,
			args: args{
				texts: []string{"[1000000000, 1, 2, 2, 1000, 1000000000, 1000000000, 10000000000, 10000000000]", "[1, 3, 3,44,3]", "[1, 33, 33, 3]"},
			},
			wantStringWritten: map[string]bool{
				"[1, 2, 1000, 1000000000, 10000000000]": true, "[1, 3, 44]": true, "[1, 3, 33]": true,
			},
			wantDataWritten: [][]uint64{{1, 2, 1000, 1000000000, 10000000000}, {1, 3, 44}, {1, 3, 33}},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name:       "Should return empty array",
			columnType: BITMAP64,
			args: args{
				texts: []string{"[]", "[1, 100]"},
			},
			wantStringWritten: map[string]bool{"[]": true, "[1, 100]": true},
			wantDataWritten:   [][]uint64{{}, {1, 100}},
			wantRowsRead:      2,
			wantErr:           false,
		},
		{
			name:       "Should return empty arrays",
			columnType: BITMAP64,
			args: args{
				texts: []string{"", "[1, 100]", ""},
			},
			wantStringWritten: map[string]bool{"[]": true, "[1, 100]": true},
			wantDataWritten:   [][]uint64{{}, {1, 100}, {}},
			wantRowsRead:      3,
			wantErr:           false,
		},
		{
			name:       "Should return empty array",
			columnType: BITMAP64,
			args: args{
				texts: []string{"[]"},
			},
			wantStringWritten: map[string]bool{"[]": true},
			wantDataWritten:   [][]uint64{{}},
			wantRowsRead:      1,
			wantErr:           false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buffer bytes.Buffer
			encoder := ch_encoding.NewEncoder(&buffer)
			decoder := ch_encoding.NewDecoder(&buffer)

			// Write to encoder
			original := MustMakeColumnData(tt.columnType, len(tt.args.texts))
			got, err := original.ReadFromTexts(tt.args.texts)
			require.NoError(t, err)
			require.Equal(t, got, tt.wantRowsRead)
			require.NoError(t, err)
			err = original.WriteToEncoder(encoder)
			require.NoError(t, err)

			// Read from decoder
			newCopy := MustMakeColumnData(tt.columnType, len(tt.args.texts))
			err = newCopy.ReadFromDecoder(decoder)

			for index, value := range tt.wantDataWritten {
				if !tt.wantErr {
					require.ElementsMatch(t, value, newCopy.GetValue(index))
				}
			}

			for i := range tt.args.texts {
				sorted := sortFormattedStringSlice(newCopy.GetString(i))
				require.True(t, tt.wantStringWritten[sorted])
			}

			require.Equal(t, newCopy.Len(), original.Len())
			require.Equal(t, newCopy.Zero(), original.Zero())
			require.Equal(t, newCopy.ZeroString(), original.ZeroString())
			require.NoError(t, original.Close())
			require.NoError(t, newCopy.Close())
		})
	}
}

// sortFormattedStringSlice sorts [2, 8, 4] into [2, 4, 8]
func sortFormattedStringSlice(s string) string {
	s = s[1 : len(s)-1]
	var ss []string
	if s != "" {
		ss = strings.Split(s, ", ")
	}

	numbers := make([]int, len(ss))
	for i := range numbers {
		numbers[i], _ = strconv.Atoi(ss[i])
	}
	sort.Ints(numbers)
	for i := range ss {
		ss[i] = strconv.Itoa(numbers[i])
	}

	return "[" + strings.Join(ss, ", ") + "]"
}
