package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_parseInsertQuery(t *testing.T) {
	type args struct {
		query string
	}
	tests := []struct {
		name string
		args args
		want *InsertQuery
	}{
		{
			name: "Should parse insert Query",
			args: args{
				query: "INSERT INTO demo_db_one.sample_table Values (4294967295,'RED BLUE YELLOW')",
			},
			want: &InsertQuery{
				DataFmt: "VALUES",
				Query:   "INSERT INTO demo_db_one.sample_table VALUES",
				Values:  "(4294967295,'RED BLUE YELLOW')",
			},
		},
		{
			name: "Should parse insert Query json",
			args: args{
				query: "INSERT INTO demo_db_one.sample_table        json {}",
			},
			want: &InsertQuery{
				DataFmt: "JSON",
				Query:   "INSERT INTO demo_db_one.sample_table JSON",
				Values:  "{}",
			},
		},
		{
			name: "Should parse insert Query json",
			args: args{
				query: "insert into `table_read` FORMAT JSON INFILE 'read.json'",
			},
			want: &InsertQuery{
				DataFmt: "JSON",
				Query:   "insert into `table_read` FORMAT JSON",
				Values:  "INFILE 'read.json'",
			},
		},
		{
			name: "Should parse insert Query csv",
			args: args{
				query: "insert into `table_read` FORMAT CSV INFILE 'read.csv'",
			},
			want: &InsertQuery{
				DataFmt: "CSV",
				Query:   "insert into `table_read` FORMAT CSV",
				Values:  "INFILE 'read.csv'",
			},
		},
		{
			name: "Should parse insert Query csv with name",
			args: args{
				query: "insert into `table_read` FORMAT CSVWITHNAMES INFILE 'read.csv'",
			},
			want: &InsertQuery{
				DataFmt: "CSVWITHNAMES",
				Query:   "insert into `table_read` FORMAT CSVWITHNAMES",
				Values:  "INFILE 'read.csv'",
			},
		},
		{
			name: "Should parse insert Query Values",
			args: args{
				query: "insert into `table_read` FORMAT Values INFILE 'read.Values'",
			},
			want: &InsertQuery{
				DataFmt: "VALUES",
				Query:   "insert into `table_read` FORMAT VALUES",
				Values:  "INFILE 'read.Values'",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseInsertQuery(tt.args.query)
			assert.NoError(t, err)
			assert.Equal(t, tt.want.Query, got.Query)
			assert.Equal(t, tt.want.DataFmt, got.DataFmt)
			assert.Equal(t, tt.want.Values, got.Values)
		})
	}
}

func Test_IsInsert(t *testing.T) {
	type args struct {
		query string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "Should return true if is insert",
			args: args{
				query: "INSERT INTO mytable Values",
			},
			want: true,
		},
		{
			name: "Should return true if is insert",
			args: args{
				query: "insert into mytable Values",
			},
			want: true,
		},
		{
			name: "Should return true if is insert",
			args: args{
				query: "select * from mytable Values",
			},
			want: false,
		},
		{
			name: "Should return true if is insert",
			args: args{
				query: "    iNSERT INTO mytable Values",
			},
			want: true,
		},
		{
			name: "Should return true if is insert",
			args: args{query: "INSERT INTO sample_table VALUES (?, ?), (?, ?)"},
			want: true,
		},
		{
			name: "Should return false if not insert",
			args: args{
				query: "    select INTO mytable Values",
			},
			want: false,
		},
		{
			name: "Should return false if not valid query",
			args: args{
				query: "insert into ",
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, IsInsert(tt.args.query), tt.want)
		})
	}
}

func TestInsertQuery_ColumnsCount(t *testing.T) {
	type fields struct {
		DataFmt string
		Query   string
		Values  string
	}
	tests := []struct {
		name    string
		fields  fields
		want    int
		wantErr bool
	}{
		{
			name: "Get valid column count",
			fields: fields{
				DataFmt: "",
				Query:   "",
				Values:  "(?, ?), (?, ?)",
			},
			want:    2,
			wantErr: false,
		},
		{
			name: "Get valid column count",
			fields: fields{
				DataFmt: "",
				Query:   "",
				Values:  "(?,?,?), (?,",
			},
			want:    3,
			wantErr: false,
		},
		{
			name: "Throw error if values format wrong",
			fields: fields{
				DataFmt: "",
				Query:   "",
				Values:  "(?,?,?",
			},
			want:    0,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			iq := &InsertQuery{
				DataFmt: tt.fields.DataFmt,
				Query:   tt.fields.Query,
				Values:  tt.fields.Values,
			}
			got, err := iq.ColumnsCount()
			if (err != nil) != tt.wantErr {
				t.Errorf("ColumnsCount() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ColumnsCount() got = %v, want %v", got, tt.want)
			}
		})
	}
}
