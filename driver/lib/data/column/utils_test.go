package column

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_splitIgnoreBraces(t *testing.T) {
	type args struct {
		src         string
		separator   byte
		bufferReuse []string
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{
			name: "Should split ignore values in braces",
			args: args{
				src:         "1, 1, {1, lalaland}",
				separator:   ',',
				bufferReuse: nil,
			},
			want: []string{"1", "1", "{1, lalaland}"},
		},
		{
			name: "Should split ignore values in arrays",
			args: args{
				src:         "1, 1, [1, lalaland]",
				separator:   ',',
				bufferReuse: nil,
			},
			want: []string{"1", "1", "[1, lalaland]"},
		},
		{
			name: "Should split ignore values in tuples",
			args: args{
				src:         "1, 1, (1, lalaland)",
				separator:   ',',
				bufferReuse: nil,
			},
			want: []string{"1", "1", "(1, lalaland)"},
		},
		{
			name: "Should preserve leading and trailing spaces of strings",
			args: args{
				src:         "1, '    1  ', (1, lalaland)",
				separator:   ',',
				bufferReuse: nil,
			},
			want: []string{"1", "'    1  '", "(1, lalaland)"},
		},
		{
			name: "Should split with apostrophe and escape",
			args: args{
				src:         "1, 'jack\\'s dog', (1, lalaland)",
				separator:   ',',
				bufferReuse: nil,
			},
			want: []string{"1", "'jack\\'s dog'", "(1, lalaland)"},
		},
		{
			name: "Should split with white space at eof",
			args: args{
				src:         "1, '1' , '(1, lalaland)'   ",
				separator:   ',',
				bufferReuse: nil,
			},
			want: []string{"1", "'1'", "'(1, lalaland)'"},
		},
		{
			name: "Should split nested tuples with a random ) inside",
			args: args{
				src:         "1, 1, (1, 'lala)land' , (1, lalaland))",
				separator:   ',',
				bufferReuse: nil,
			},
			want: []string{"1", "1", "(1, 'lala)land' , (1, lalaland))"},
		},
		{
			name: "Should account for double escapes",
			args: args{
				src:         "1, 1, \\n",
				separator:   ',',
				bufferReuse: nil,
			},
			want: []string{"1", "1", "\\n"},
		},
		{
			name: "Split values by colon, ignoring curly braces",
			args: args{
				src:         "x : { y : x }",
				separator:   ':',
				bufferReuse: nil,
			},
			want: []string{"x", "{ y : x }"},
		},
		{
			name: "Split values by colon, ignoring open quotes",
			args: args{
				src:         "x : { y : 'x:boo' }",
				separator:   ':',
				bufferReuse: nil,
			},
			want: []string{"x", "{ y : 'x:boo' }"},
		},
		{
			name: "Split column types properly, ignoring round braces",
			args: args{
				src:         "Map(String, UInt8), UInt8",
				separator:   ',',
				bufferReuse: nil,
			},
			want: []string{"Map(String, UInt8)", "UInt8"},
		},
		{
			name: "Split column types properly, should account for n substrings",
			args: args{
				src:         "Map(String, UInt8), UInt8, Map(String, String), Koo Koo Koo",
				separator:   ',',
				bufferReuse: nil,
			},
			want: []string{"Map(String, UInt8)", "UInt8", "Map(String, String)", "Koo Koo Koo"},
		},
		{
			name: "Split nested round brackets, square brackets, curly brackets  properly",
			args: args{
				src:         "[[[1,2,3],[1,2]]],[[1,2],[2,3]],[1,4,5,3],  (((1,2,3,4))),  {{'nmba':'25042003','tai':'26072000'}}",
				separator:   ',',
				bufferReuse: nil,
			},
			want: []string{"[[[1,2,3],[1,2]]]", "[[1,2],[2,3]]", "[1,4,5,3]", "(((1,2,3,4)))", "{{'nmba':'25042003','tai':'26072000'}}"},
		},
		{
			name: "Split Array of string with bracket as part of the string",
			args: args{
				src:         "['\\[\\nmba', '\\[\\]25042003'], [['\\]tai', '\\[\\]26072000']]",
				separator:   ',',
				bufferReuse: nil,
			},
			want: []string{"['\\[\\nmba', '\\[\\]25042003']", "[['\\]tai', '\\[\\]26072000']]"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := splitIgnoreBraces(tt.args.src, tt.args.separator, tt.args.bufferReuse)
			assert.Equal(t, tt.want, got)
		})
	}
}

func Test_indexTillNotByteOrEOF(t *testing.T) {
	type args struct {
		s string
		c byte
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "Test that can get first index which is not c",
			args: args{
				s: "  koo koo bird",
				c: ' ',
			},
			want: 2,
		},
		{
			name: "Test that can get first index which is not c",
			args: args{
				s: "aaaaaa ",
				c: 'a',
			},
			want: 6,
		},
		{
			name: "Test get last index if c not found",
			args: args{
				s: "        ",
				c: ' ',
			},
			want: len("        ") - 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := indexTillNotByteOrEOF(tt.args.s, tt.args.c); got != tt.want {
				t.Errorf("indexTillNotByteOrEOF() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_commaIterator(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name    string
		args    args
		wantLen int
		want    []string
	}{
		{
			name: "Test can comma iterate",
			args: args{
				s: "ewe,3232,33",
			},
			want: []string{"ewe", "3232", "33"},
		},
		{
			name: "Test can comma iterate with square bracket",
			args: args{
				s: "ewe,3232,[33,3232],3",
			},
			want: []string{"ewe", "3232", "[33,3232]", "3"},
		},
		{
			name: "Test can comma iterate with round bracket",
			args: args{
				s: "ewe,3232,(33,3232),3",
			},
			want: []string{"ewe", "3232", "(33,3232)", "3"},
		},
		{
			name: "Test can comma iterate with curly bracket",
			args: args{
				s: "ewe,3232,{33,3232},3",
			},
			want: []string{"ewe", "3232", "{33,3232}", "3"},
		},
		{
			name: "Test can comma iterate with doubleQuote",
			args: args{
				s: "ewe,3232,\"33,3232\",3",
			},
			want: []string{"ewe", "3232", "\"33,3232\"", "3"},
		},
		{
			name: "Test can comma iterate with backQuote",
			args: args{
				s: "ewe,3232,`33,3232`,3",
			},
			want: []string{"ewe", "3232", "`33,3232`", "3"},
		},
		{
			name: "Test can comma iterate with escape",
			args: args{
				s: "ewe,3232\\,33,3232,3",
			},
			want: []string{"ewe", "3232\\,33", "3232", "3"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fun := commaIterator(tt.args.s)
			for _, value := range tt.want {
				actual, ok := fun()
				require.True(t, ok)
				require.Equal(t, value, actual)
			}
			_, ok := fun()
			require.False(t, ok)
		})
	}
}

func Test_getDateTimeLocation(t *testing.T) {
	type args struct {
		t CHColumnType
	}
	tests := []struct {
		name string
		args args
		want *time.Location
	}{
		{
			name: "Test if no location will return nil",
			args: args{
				t: "DateTime",
			},
			want: nil,
		},
		{
			name: "Test if have location can get location",
			args: args{
				t: "DateTime('Europe/Moscow')",
			},
			want: func() *time.Location {
				loc, _ := time.LoadLocation("Europe/Moscow")
				return loc
			}(),
		},
		{
			name: "Test if have location can get location",
			args: args{
				t: "DateTime('Asia/Singapore')",
			},
			want: func() *time.Location {
				loc, _ := time.LoadLocation("Asia/Singapore")
				return loc
			}(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getDateTimeLocation(tt.args.t)
			require.NoError(t, err)
			require.Equal(t, got, tt.want)
		})
	}
}

func Test_getDateTime64Param(t *testing.T) {
	type args struct {
		t CHColumnType
	}
	tests := []struct {
		name          string
		args          args
		wantPrecision int
		wantLocation  *time.Location
	}{
		{
			name: "Test if no location will return nil",
			args: args{
				t: "DateTime64(10)",
			},
			wantPrecision: 10,
			wantLocation:  time.UTC,
		},
		{
			name: "Test if have location can get location",
			args: args{
				t: "DateTime64(11, 'Europe/Moscow')",
			},
			wantPrecision: 11,
			wantLocation: func() *time.Location {
				loc, _ := time.LoadLocation("Europe/Moscow")
				return loc
			}(),
		},
		{
			name: "Test if have location can get location",
			args: args{
				t: "DateTime64(12, 'Asia/Singapore')",
			},
			wantPrecision: 12,
			wantLocation: func() *time.Location {
				loc, _ := time.LoadLocation("Asia/Singapore")
				return loc
			}(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			precision, location, err := getDateTime64Param(tt.args.t)
			require.NoError(t, err)
			require.Equal(t, precision, tt.wantPrecision)
			require.Equal(t, location, tt.wantLocation)
		})
	}
}
