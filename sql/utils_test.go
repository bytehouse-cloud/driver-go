package sql

import (
	"database/sql/driver"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func Test_bindArgsToQuery(t *testing.T) {
	type args struct {
		query string
		args  []driver.Value
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "Should replace args of basic types",
			args: args{
				query: "SELECT mums from table where dog = ? and la = ? and hi = ?",
				args: []driver.Value{
					driver.Value("bug"),
					driver.Value(1),
					driver.Value(false),
				},
			},
			want:    "SELECT mums from table where dog = 'bug' and la = 1 and hi = false",
			wantErr: false,
		},
		{
			name: "Should replace args of map",
			args: args{
				query: "SELECT mums from table where dog = ? and la = ?",
				args: []driver.Value{
					driver.Value(map[string]string{"kuku": "malu"}),
					driver.Value(map[string]string{"kuku": "malu"}),
				},
			},
			want:    "SELECT mums from table where dog = {kuku:malu} and la = {kuku:malu}",
			wantErr: false,
		},
		{
			name: "Should parse array",
			args: args{
				query: "SELECT mums from table where dog = ? and la = ? and hi = ?",
				args: []driver.Value{
					[]uint32{1, 2, 3, 4, 5},
					driver.Value(1),
					driver.Value(false),
				},
			},
			want:    "SELECT mums from table where dog = [1,2,3,4,5] and la = 1 and hi = false",
			wantErr: false,
		},
		{
			name: "Should return query if no args",
			args: args{
				query: "SELECT mums from table where ma = 'fa'",
				args:  []driver.Value{},
			},
			want:    "SELECT mums from table where ma = 'fa'",
			wantErr: false,
		},
		{
			name: "Should ignore question marks in quotes '?'",
			args: args{
				query: "SELECT mums from table where ma = '?bub' and ?",
				args: []driver.Value{
					driver.Value("good"),
				},
			},
			want:    "SELECT mums from table where ma = '?bub' and 'good'",
			wantErr: false,
		},
		{
			name: "Should ignore question marks in quotes '?' and escape strings ''",
			args: args{
				query: "SELECT mums from table where ma = '''?bub''' and ?",
				args: []driver.Value{
					driver.Value("good"),
				},
			},
			want:    "SELECT mums from table where ma = '''?bub''' and 'good'",
			wantErr: false,
		},
		{
			name: "Should ignore question marks in quotes '?' and escape strings \\'",
			args: args{
				query: "SELECT mums from table where ma = '\\'?bub\\'hi\\'?' and ?",
				args: []driver.Value{
					driver.Value("good"),
				},
			},
			want:    "SELECT mums from table where ma = '\\'?bub\\'hi\\'?' and 'good'",
			wantErr: false,
		},
		{
			name: "Should ignore question marks in identifiers marked by backtick `",
			args: args{
				query: "SELECT `mums?` from table where hi = ?",
				args: []driver.Value{
					driver.Value("good"),
				},
			},
			want:    "SELECT `mums?` from table where hi = 'good'",
			wantErr: false,
		},
		{
			name: "Should ignore question marks in identifiers marked by double quotes \"",
			args: args{
				query: "SELECT \"mums?\" from table where hi = ?",
				args: []driver.Value{
					driver.Value("good"),
				},
			},
			want:    "SELECT \"mums?\" from table where hi = 'good'",
			wantErr: false,
		},
		{
			name: "Should ignore question marks in identifiers marked by double quotes \"",
			args: args{
				query: "SELECT \"mums?\" from table where \"hi?\" = ?",
				args: []driver.Value{
					driver.Value("good"),
				},
			},
			want:    "SELECT \"mums?\" from table where \"hi?\" = 'good'",
			wantErr: false,
		},
		{
			name: "Should throw errChan if less args than ?",
			args: args{
				query: "SELECT mums from table where dog = ? and la = ? and hi = ? and fa = ?",
				args: []driver.Value{
					driver.Value("bug"),
					driver.Value(1),
					driver.Value(false),
				},
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "Should throw errChan if more args than ?",
			args: args{
				query: "SELECT mums from table where dog = ? and la = ? and hi = ?",
				args: []driver.Value{
					driver.Value("bug"),
					driver.Value(1),
					driver.Value(false),
					driver.Value(true),
				},
			},
			want:    "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := bindArgsToQuery(tt.args.query, tt.args.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("bindArgsToQuery() error = %v, wantExecErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("bindArgsToQuery() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_quote(t *testing.T) {
	type args struct {
		v driver.Value
	}
	tests := []struct {
		name   string
		args   args
		want   string
		assert func(t *testing.T, s string)
	}{
		{
			name: "Should generate correct value for map",
			args: args{
				v: map[string]int{
					"Koo":  1,
					"Baba": 2,
				},
			},
			assert: func(t *testing.T, s string) {
				v1 := "{Koo:1,Baba:2}"
				v2 := "{Baba:2,Koo:1}"
				if s != v1 && s != v2 {
					t.Errorf("expect value = %s to = %s or %s", s, v1, v2)
				}
			},
		},
		{
			name: "Should generate correct value for map",
			args: args{
				v: map[string]int{},
			},
			want: "{}",
		},
		{
			name: "Should generate correct value for slice",
			args: args{
				v: []uint32{
					1, 2, 3, 4, 5,
				},
			},
			want: "[1,2,3,4,5]",
		},
		{
			name: "Should generate correct value for uuid",
			args: args{
				v: func() uuid.UUID {
					v, _ := uuid.Parse("f76adea1-fa8b-4b85-b645-7ef82f85444b")
					return v
				}(),
			},
			want: "f76adea1-fa8b-4b85-b645-7ef82f85444b",
		},
		{
			name: "Should generate correct value for net.IPv4",
			args: args{
				v: net.ParseIP("192.0.2.1"),
			},
			want: "192.0.2.1",
		},
		{
			name: "Should generate correct value for net.IPv6",
			args: args{
				v: net.ParseIP("2001:db8::68"),
			},
			want: "2001:db8::68",
		},
		{
			name: "Should generate correct value for datetime",
			args: args{
				// 1 year in second and 1 year in nanosecond -> should be 2 years
				v: time.Unix(3.154e+7, 3.154e+16),
			},
			want: "toDateTime(63080000)",
		},
		{
			name: "Should generate correct value for datetime",
			args: args{
				v: func() time.Time {
					l, _ := time.LoadLocation("Asia/Singapore")
					return time.Unix(864000+59400, 0).In(l)
				}(),
			},
			want: "toDate(10)",
		},
		{
			name: "Should generate correct value for float",
			args: args{
				v: float64(1.23444),
			},
			want: "1.23444",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.assert != nil {
				tt.assert(t, quote(tt.args.v))
				return
			}

			assert.Equal(t, quote(tt.args.v), tt.want)
		})
	}
}

func Benchmark_quote(b *testing.B) {
	type args struct {
		v driver.Value
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "Should generate correct value for map",
			args: args{
				v: map[string]int{
					"Koo":  1,
					"Baba": 2,
				},
			},
		},
		{
			name: "Should generate correct value for slice",
			args: args{
				v: []uint32{
					1, 2, 3, 4, 5,
				},
			},
		},
	}
	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				quote(tt.args.v)
			}
		})
	}
}

func Test_namedValueToValue(t *testing.T) {
	type args struct {
		named []driver.NamedValue
	}
	tests := []struct {
		name    string
		args    args
		want    []driver.Value
		wantErr bool
	}{
		{
			name: "Should convert named value to value",
			args: args{
				named: []driver.NamedValue{
					{
						Name:    "",
						Ordinal: 0,
						Value:   "hahaha",
					},
					{
						Name:    "",
						Ordinal: 0,
						Value:   "mamama",
					},
				},
			},
			want: []driver.Value{
				"hahaha",
				"mamama",
			},
			wantErr: false,
		},
		{
			name: "Should throw error if have name",
			args: args{
				named: []driver.NamedValue{
					{
						Name:    "lol",
						Ordinal: 0,
						Value:   "hahaha",
					},
					{
						Name:    "",
						Ordinal: 0,
						Value:   "mamama",
					},
				},
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := namedArgsToArgs(tt.args.named)
			if (err != nil) != tt.wantErr {
				t.Errorf("namedArgsToArgs() error = %v, wantExecErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("namedArgsToArgs() got = %v, want %v", got, tt.want)
			}
		})
	}
}
