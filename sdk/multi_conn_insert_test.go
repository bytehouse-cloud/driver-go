package sdk

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bytehouse-cloud/driver-go/utils"
)

func TestMultiInsertStatement(t *testing.T) {
	utils.SkipIntegrationTestIfShort(t)
	g, err := OpenConfig(getConfig(t))
	require.NoError(t, err, "OpenConfig error: %s", err)

	ctx := context.Background()
	var qr *QueryResult
	qr, err = g.Query("create database if not exists test")
	require.NoError(t, err, "create database error: %s", err)
	//_ = qr.Close()

	_, err = g.Query("create table if not exists test.multi_insert_test (i Int32, s String) Engine = MergeTree order by tuple()")
	require.NoError(t, err, "create table: %s", err)
	//_ = qr.Close()
	defer g.Query("drop table if exists test.multi_insert_test")

	stmt, err := g.PrepareMultiConnectionInsert(ctx, "insert into test.multi_insert_test Values", 500, 5)
	require.NoError(t, err, "prepare multi conn insert error:", err)

	numRows := int(1e5)
	for i := 0; i < numRows; i++ {
		err = stmt.ExecContext(ctx, int32(4), "eee")
		require.NoError(t, err, "execContext error:", err)
	}
	err = stmt.Close()
	require.NoError(t, err, "close error:", err)

	qr, err = g.Query("select count() from test.multi_insert_test")
	require.NoError(t, err, "select count() error:", err)
	require.NoError(t, qr.Exception(), "select count() error:", err)

	nr, ok := qr.NextRow()
	if !ok {
		t.Log("did not receive any data from select")
		t.FailNow()
	}
	require.Equal(t, fmt.Sprint(nr[0]), fmt.Sprint(numRows))
}
