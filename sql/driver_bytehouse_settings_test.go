package sql

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	bytehouse "github.com/bytehouse-cloud/driver-go"
	"github.com/bytehouse-cloud/driver-go/sdk"
	"github.com/bytehouse-cloud/driver-go/utils"
)

func TestInsertPrepareSettings(t *testing.T) {
	utils.SkipIntegrationTestIfShort(t)

	type opt func(bhCtx *bytehouse.QueryContext) error

	tt := []struct {
		name string
		opts []opt
	}{
		{
			name: "given batch size then success",
			opts: []opt{func(bhCtx *bytehouse.QueryContext) error {
				return bhCtx.AddClientSetting(bytehouse.InsertBlockSize, 50)
			}},
		},
		{
			name: "given connection count then success",
			opts: []opt{func(bhCtx *bytehouse.QueryContext) error {
				return bhCtx.AddClientSetting(bytehouse.InsertConnectionCount, 2)
			}},
		},
		{
			name: "given block process parallelism then success",
			opts: []opt{func(bhCtx *bytehouse.QueryContext) error {
				return bhCtx.AddClientSetting(bytehouse.InsertBlockParallelism, 2)
			}},
		},
	}

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()

			bhCtx := bytehouse.NewQueryContext(ctx)
			for _, opt := range test.opts {
				require.NoError(t, opt(bhCtx))
			}

			db, err := sql.Open("bytehouse", "tcp://localhost:9000?user=default")
			require.NoError(t, err, "sql open error: %s", err)
			defer db.Close()

			_, err = db.Exec("create database if not exists test")
			require.NoError(t, err, "create database error: %s", err)

			_, err = db.Exec("create table if not exists test.insert_test (i Int32, s String) Engine = MergeTree order by tuple()")
			require.NoError(t, err, "create table error: %s", err)
			defer db.Exec("drop table if exists test.insert_test")

			numRows := int(1e5)
			err = RunConn(ctx, db, func(conn sdk.Conn) error {
				stmt, err := conn.PrepareContext(ctx, "insert into table test.insert_test Values")
				if err != nil {
					return err
				}

				for i := 0; i < numRows; i++ {
					err = stmt.ExecContext(ctx, int32(13246), "eee")
					require.NoError(t, err, "execContext error: %s", err)
				}
				return stmt.Close()
			})
			require.NoError(t, err, "run conn error: %s")

			rows, err := db.Query("select count() from test.insert_test")
			require.NoError(t, err, "select count() error: %s", err)
			defer rows.Close()

			rows.Next()
			var count interface{}
			require.NoError(t, rows.Scan(&count))
			require.Equal(t, fmt.Sprint(count), fmt.Sprint(numRows))
		})
		time.Sleep(time.Second)
	}
}
