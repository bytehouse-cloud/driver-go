package utils

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_numInput(t *testing.T) {
	for query, num := range map[string]int{
		"SELECT * FROM example WHERE os_id = 42":                                                  0,
		"SELECT * FROM example WHERE email = 'name@mail'":                                         0,
		"SELECT * FROM example WHERE email = 'na`me@mail'":                                        0,
		"SELECT * FROM example WHERE email = 'na`m`e@mail'":                                       0,
		"SELECT * FROM example WHERE email = 'na`m`e@m`ail'":                                      0,
		"SELECT * FROM example WHERE os_id = @os_id AND browser_id = @os_id":                      1,
		"SELECT * FROM example WHERE os_id = @os_id AND browser_id = @os_id2":                     2,
		"SELECT * FROM example WHERE os_id in (@os_id,@browser_id) browser_id = @browser_id":      2,
		"SELECT * FROM example WHERE os_id IN (@os_id, @browser_id) AND browser_id = @browser_id": 2,
		"SELECT * FROM example WHERE os_id = ? AND browser_id = ?":                                2,
		"SELECT * FROM example WHERE os_id in (?,?) browser_id = ?":                               3,
		"SELECT * FROM example WHERE os_id IN (?, ?) AND browser_id = ?":                          3,
		"SELECT a ? '+' : '-'": 0,
		"SELECT a ? '+' : '-' FROM example WHERE a = ? AND b IN(?)": 2,
		`SELECT
			a ? '+' : '-'
		FROM example WHERE a = 42 and b in(
			?,
			?,
			?
		)
		`: 3,
		"SELECT * from EXAMPLE LIMIT ?":                                       1,
		"SELECT * from EXAMPLE LIMIT ?, ?":                                    2,
		"SELECT * from EXAMPLE WHERE os_id like ?":                            1,
		"SELECT * FROM example WHERE a BETWEEN ? AND ?":                       2,
		"SELECT * FROM example WHERE a BETWEEN ? AND ? AND b = ?":             3,
		"SELECT * FROM example WHERE a = ? AND b BETWEEN ? AND ?":             3,
		"SELECT * FROM example WHERE a BETWEEN ? AND ? AND b BETWEEN ? AND ?": 4,
	} {
		assert.Equal(t, num, NumArgs(query), query)
	}
}

func TestMakeColumnValues(t *testing.T) {
	type args struct {
		nColumns  int
		blockSize int
	}
	tests := []struct {
		name string
		args args
		want [][]interface{}
	}{
		{
			name: "Should create array with correct size",
			args: args{
				nColumns:  2,
				blockSize: 2,
			},
			want: [][]interface{}{
				{},
				{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := MakeColumnValues(tt.args.nColumns, tt.args.blockSize); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("MakeColumnValues() = %v, want %v", got, tt.want)
			}
		})
	}
}
