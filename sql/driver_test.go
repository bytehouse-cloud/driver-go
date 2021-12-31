package sql

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bytehouse-cloud/driver-go"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data/column"
	"github.com/bytehouse-cloud/driver-go/sdk"
	"github.com/bytehouse-cloud/driver-go/utils"
)

var (
	dsn = "tcp://localhost:9000?user=default&compress=true"
	ctx = context.Background()
)

func assertTrue(t *testing.T, v bool) {
	t.Helper()

	if v {
		return
	}

	t.Fatal("value was not true")
}

func assertEqual(t *testing.T, a interface{}, b interface{}) {
	t.Helper()

	if a == b {
		return
	}

	t.Fatal(fmt.Sprintf("%v != %v", a, b))
}

func assertEqualDate(t *testing.T, a time.Time, b time.Time) {
	t.Helper()

	aString := a.String()[0:10]
	bString := b.String()[0:10]

	if aString == bString {
		return
	}

	t.Fatal(fmt.Sprintf("%v != %v", aString, bString))
}

func assertEqualTime(t *testing.T, a time.Time, b time.Time) {
	t.Helper()

	aString := a.String()[0:19]
	bString := b.String()[0:19]

	if aString == bString {
		return
	}

	t.Fatal(fmt.Sprintf("%v != %v", aString, bString))
}

func assertNoErr(t *testing.T, e error) {
	t.Helper()

	if e == nil {
		return
	}

	t.Fatal(e.Error())
}

func assertExecSQL(t *testing.T, connDB *sql.DB, script ...string) {
	t.Helper()

	for _, cmd := range script {
		_, err := connDB.ExecContext(ctx, cmd)
		assertNoErr(t, err)
	}
}

func assertErr(t *testing.T, err error, errorSubstring string) {
	t.Helper()

	if err == nil {
		t.Fatalf("expected error containing '%s', but there was no error at all", errorSubstring)
	}

	errStr := err.Error()

	if strings.Contains(errStr, errorSubstring) {
		return
	}

	t.Fatalf("expected an error containing '%s', but found '%s'", errorSubstring, errStr)
}

func assertNext(t *testing.T, rows *sql.Rows) {
	t.Helper()

	if !rows.Next() {
		t.Fatal("another row was expected to be available, but wasn't")
	}
}

func assertNoNext(t *testing.T, rows *sql.Rows) {
	t.Helper()

	if rows.Next() {
		t.Fatal("no more rows expected available, but were")
	}
}

func openConnection(t *testing.T, setupScript ...string) *sql.DB {
	connDB, err := sql.Open("bytehouse", dsn)
	assertNoErr(t, err)

	err = connDB.PingContext(ctx)
	assertNoErr(t, err)

	assertExecSQL(t, connDB, utils.AllowMapSQLScript)

	if len(setupScript) > 0 {
		assertExecSQL(t, connDB, setupScript...)
	}

	return connDB
}

func closeConnection(t *testing.T, connDB *sql.DB, teardownScript ...string) {
	if len(teardownScript) > 0 {
		assertExecSQL(t, connDB, teardownScript...)
	}

	assertNoErr(t, connDB.Close())
}

// TestBasicWorkflow test the basic workflow of creating database, table, populating table and reading data
func TestBasicWorkflow(t *testing.T) {
	utils.SkipIntegrationTestIfShort(t)

	connDB := openConnection(t,
		`CREATE DATABASE IF NOT EXISTS demo_db_one`,
		`CREATE TABLE IF NOT EXISTS demo_db_one.sample_table (
		Id UInt32,
		Color String,
		Status FixedString(16),  
		Numbers Int32,
		BigNumbers Int64,
		BiggerNumbers UInt64,  
		ShippingCost Float32,
		ShippingDate TIMESTAMP
	) ENGINE=MergeTree ORDER BY Id`,
	)
	defer closeConnection(t, connDB,
		`DROP TABLE IF EXISTS demo_db_one.sample_table`,
		`DROP DATABASE IF EXISTS demo_db_one`,
	)

	type refRowType struct {
		id            uint32
		color         string
		status        string
		numbers       int32
		bigNumbers    int64
		biggerNumbers uint64
		shippingCost  float32
		shippingDate  time.Time
	}

	// Populate with some data
	refRow := refRowType{math.MaxUint32, "RED BLUE YELLOW", "PENDING", math.MinInt32, math.MaxInt64, math.MaxInt64, math.MaxFloat32 - 0.00289, time.Now()}
	var table = []refRowType{
		refRow,
		refRow,
		refRow,
		refRow,
	}

	for _, row := range table {
		_, err := connDB.ExecContext(ctx, "INSERT INTO demo_db_one.sample_table VALUES (?,?,?,?,?,?,?,?)",
			row.id,
			row.color,
			row.status,
			row.numbers,
			row.bigNumbers,
			row.biggerNumbers,
			row.shippingCost,
			row.shippingDate,
		)
		assertNoErr(t, err)
	}

	// Show table query
	showQuery, err := connDB.QueryContext(ctx, "SHOW CREATE TABLE demo_db_one.sample_table")
	assertNoErr(t, err)

	for showQuery.Next() {
		var val string
		assertNoErr(t, showQuery.Scan(&val))
		log.Printf("show query: %v", val)
	}

	// Read data
	tableRows, err := connDB.QueryContext(ctx, "SELECT * FROM demo_db_one.sample_table")
	assertNoErr(t, err)
	defer assertNoErr(t, tableRows.Close())

	for tableRows.Next() {
		var (
			id            uint32
			color         string
			status        string
			numbers       int32
			bigNumbers    int64
			biggerNumbers uint64
			shippingCost  float32
			shippingDate  time.Time
		)

		assertNoErr(t, tableRows.Scan(
			&id,
			&color,
			&status,
			&numbers,
			&bigNumbers,
			&biggerNumbers,
			&shippingCost,
			&shippingDate,
		))

		assertEqual(t, refRow.id, id)
		assertEqual(t, refRow.color, color)
		assertEqual(t, refRow.status, status)
		assertEqual(t, refRow.numbers, numbers)
		assertEqual(t, refRow.bigNumbers, bigNumbers)
		assertEqual(t, refRow.biggerNumbers, biggerNumbers)
		assertEqual(t, refRow.shippingCost, shippingCost)
		assertEqualTime(t, refRow.shippingDate, shippingDate)
	}
}

func TestLoopingExec(t *testing.T) {
	utils.SkipIntegrationTestIfShort(t)

	connDB := openConnection(t, `DROP TABLE IF EXISTS MyTable`,
		`CREATE TABLE MyTable (id Int64, name varchar(64), PRIMARY KEY(id)) ENGINE=MergeTree`)
	defer closeConnection(t, connDB, `
	DROP TABLE MyTable;
	`)

	for i := 0; i < 100; i++ {
		_, err := connDB.ExecContext(ctx, "INSERT INTO MyTable VALUES (?, ?)", i, "Joe perry")
		assertNoErr(t, err)
	}
}

func TestConcurrentExec(t *testing.T) {
	utils.SkipIntegrationTestIfShort(t)

	connDB := openConnection(t, `DROP TABLE IF EXISTS MyTable`,
		`CREATE TABLE MyTable (id Int64, name varchar(64), PRIMARY KEY(id)) ENGINE=MergeTree`)
	defer closeConnection(t, connDB, `
	DROP TABLE MyTable;
	`)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = connDB.ExecContext(ctx, "INSERT INTO MyTable VALUES (?, ?)", i, "Joe Perry")
		}()
	}

	wg.Wait()
}

func TestBasicArgsQuery(t *testing.T) {
	utils.SkipIntegrationTestIfShort(t)

	connDB := openConnection(t,
		"DROP TABLE IF EXISTS MyTable",
		"CREATE TABLE MyTable(id int, name VARCHAR(64), guitarist UInt8, height FLOAT, birthday DateTime64(5), PRIMARY KEY(id)) ENGINE=MergeTree",
	)
	defer closeConnection(t, connDB, "DROP TABLE IF EXISTS MyTable")

	myTime := time.Now()

	_, err := connDB.ExecContext(ctx, "INSERT INTO MyTable VALUES (?, ?, ?, ?, ?)", int32(21), "Joe Perry", uint8(1), float32(123.45), myTime)
	assertNoErr(t, err)

	//-----------------------------------------------------------------------------------------
	// Make sure we can iterate queries with a string
	//-----------------------------------------------------------------------------------------

	rows, err := connDB.QueryContext(ctx, "SELECT name FROM MyTable WHERE id=?", 21)
	assertNoErr(t, err)
	assertNext(t, rows)

	var nameStr string
	assertNoErr(t, rows.Scan(&nameStr))

	assertEqual(t, nameStr, "Joe Perry")
	assertNoNext(t, rows)

	assertNoErr(t, rows.Close())

	//-----------------------------------------------------------------------------------------
	// Make sure we can run queries with an int, bool and float
	//-----------------------------------------------------------------------------------------

	rows, err = connDB.QueryContext(ctx, "SELECT id, guitarist, height, birthday FROM MyTable WHERE name=?", "Joe Perry")
	assertNoErr(t, err)
	assertNext(t, rows)

	var id int
	var guitarist bool
	var height float64
	var birthday time.Time
	assertNoErr(t, rows.Scan(&id, &guitarist, &height, &birthday))

	assertEqual(t, id, 21)
	assertEqual(t, guitarist, true)
	assertEqual(t, height, 123.45)

	assertEqualTime(t, birthday, myTime) // We gave a timestamp with assumed UTC0, so this is correct.
	assertNoNext(t, rows)

	//-----------------------------------------------------------------------------------------
	// Ensure the 'QueryRowContext()' variant works.
	//-----------------------------------------------------------------------------------------

	err = connDB.QueryRowContext(ctx, "SELECT id FROM MyTable WHERE name=?", "Joe Perry").Scan(&id)
	assertNoErr(t, err)
	assertEqual(t, id, 21)

	assertNoErr(t, rows.Close())
}

func Test_Context_Timeout(t *testing.T) {
	utils.SkipIntegrationTestIfShort(t)
	if connect, err := sql.Open("bytehouse", "tcp://localhost:9000?debug=true"); assert.NoError(t, err) && assert.NoError(t, connect.Ping()) {
		{
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*20)
			defer cancel()
			if row := connect.QueryRowContext(ctx, "SELECT 1, sleep(2)"); assert.NotNil(t, row) {
				var a, b int
				assertErr(t, row.Scan(&a, &b), "")
			}
		}
		{
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if row := connect.QueryRowContext(ctx, "SELECT 1, sleep(0.1)"); assert.NotNil(t, row) {
				var value, value2 int
				if assert.NoError(t, row.Scan(&value, &value2)) {
					assert.Equal(t, int(1), value)
				}
			}
		}
	}
}

func Test_Ping_Context_Timeout(t *testing.T) {
	utils.SkipIntegrationTestIfShort(t)
	if connect, err := sql.Open("bytehouse", "tcp://localhost:9000?debug=true"); assert.NoError(t, err) && assert.NoError(t, connect.Ping()) {
		{
			ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
			defer cancel()
			if err := connect.PingContext(ctx); assert.Error(t, err) {
				assert.Equal(t, context.DeadlineExceeded, err)
			}
		}
		{
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*20)
			defer cancel()
			if row := connect.QueryRowContext(ctx, "SELECT 1, sleep(2)"); assert.NotNil(t, row) {
				var a, b int
				assertErr(t, row.Scan(&a, &b), "")
			}
		}
		{
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if row := connect.QueryRowContext(ctx, "SELECT 1, sleep(0.1)"); assert.NotNil(t, row) {
				var value, value2 int
				if assert.NoError(t, row.Scan(&value, &value2)) {
					assert.Equal(t, int(1), value)
				}
			}
		}
	}
}

func Test_Timeout(t *testing.T) {
	utils.SkipIntegrationTestIfShort(t)
	if connect, err := sql.Open("bytehouse", "tcp://localhost:9000?read_timeout=0.2s"); assert.NoError(t, err) && assert.NoError(t, connect.Ping()) {
		{
			if row := connect.QueryRow("SELECT 1, sleep(2)"); assert.NotNil(t, row) {
				var a, b int
				// always reading progress packet, which refreshes the timeout
				assert.NoError(t, row.Scan(&a, &b), "")
			}
		}
		{
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if row := connect.QueryRowContext(ctx, "SELECT 1, sleep(0.1)"); assert.NotNil(t, row) {
				var value, value2 int
				if assert.NoError(t, row.Scan(&value, &value2)) {
					assert.Equal(t, int(1), value)
				}
			}
		}
	}
}

func TestBasicArgsWithNil(t *testing.T) {
	utils.SkipIntegrationTestIfShort(t)

	connDB := openConnection(t,
		"DROP TABLE IF EXISTS MyTable",
		"CREATE TABLE MyTable(id int, name Nullable(VARCHAR(64)), guitarist UInt8, height FLOAT, birthday TIMESTAMP, PRIMARY KEY(id)) ENGINE=MergeTree",
	)
	defer closeConnection(t, connDB, "DROP TABLE IF EXISTS MyTable")
	var id int
	var name sql.NullString
	var err error

	//-----------------------------------------------------------------------------------------
	// Ensure we can insert naked null values.
	//-----------------------------------------------------------------------------------------
	_, err = connDB.ExecContext(ctx, "INSERT INTO MyTable VALUES (?, ?, ?, ?, ?)", int32(13), nil, uint8(1), float32(123.45), time.Now())
	assertNoErr(t, err)

	err = connDB.QueryRowContext(ctx, "SELECT id, name FROM MyTable WHERE isNull(name)").Scan(&id, &name)
	assertNoErr(t, err)
	assertEqual(t, id, 13)
	assertEqual(t, name.Valid, false)

	// -----------------------------------------------------------------------------------------
	// Ensure we can insert NullString with value
	// -----------------------------------------------------------------------------------------
	var someStr = sql.NullString{
		Valid:  true,
		String: "hello",
	}
	_, err = connDB.ExecContext(ctx, "INSERT INTO MyTable VALUES (?, ?, ?, ?, ?)", int32(15), someStr, uint8(1), float32(456.78), time.Now())
	assertNoErr(t, err)

	err = connDB.QueryRowContext(ctx, "SELECT id, name FROM MyTable WHERE id=?", 15).Scan(&id, &name)
	assertNoErr(t, err)
	assertEqual(t, id, 15)
	assertEqual(t, name.String, "hello")

	// -----------------------------------------------------------------------------------------
	// Ensure we can insert NullString without value
	// -----------------------------------------------------------------------------------------
	var emptyStr = sql.NullString{
		Valid:  false,
		String: "",
	}
	_, err = connDB.ExecContext(ctx, "INSERT INTO MyTable VALUES (?, ?, ?, ?, ?)", int32(16), emptyStr, uint8(1), float32(456.78), time.Now())
	assertNoErr(t, err)

	err = connDB.QueryRowContext(ctx, "SELECT id, name FROM MyTable WHERE id=?", 16).Scan(&id, &name)
	assertNoErr(t, err)
	assertEqual(t, id, 16)
	assertEqual(t, name.Valid, false) // Currently returning true
}

func TestEmptyStatementError(t *testing.T) {
	utils.SkipIntegrationTestIfShort(t)

	connDB := openConnection(t)
	defer closeConnection(t, connDB)

	// Try as exec.
	_, err := connDB.ExecContext(ctx, "")
	assert.Error(t, err)

	// Try as query.
	_, err = connDB.QueryContext(ctx, "")
	assert.Error(t, err)
}

func Test_InsertCommonTypes(t *testing.T) {
	utils.SkipIntegrationTestIfShort(t)

	var err error
	const (
		ddl = `
			CREATE TABLE clickhouse_test_insert (
				int8  Int8,
				int16 Int16,
				int32 Int32,
				int64 Int64,
				uint8  UInt8,
				uint16 UInt16,
				uint32 UInt32,
				uint64 UInt64,
				float32 Float32,
				float64 Float64,
				string  Nullable(String),
				fString FixedString(2),
				date    Date,
				datetime DateTime,
				datetime64 DateTime64,
				ipv4str IPv4,
				ipv6str IPv6
			) Engine=Memory
		`
		dropTable = "DROP TABLE IF EXISTS clickhouse_test_insert"
		dml       = `
			INSERT INTO clickhouse_test_insert (
				int8,
				int16,
				int32,
				int64,
				uint8,
				uint16,
				uint32,
				uint64,
				float32,
				float64,
				string,
				fString,
				date,
				datetime,
				datetime64,
				ipv4str,
				ipv6str
			) VALUES (
				?,
				?,
				?,
				?,
				?,
				?,
				?,
				?,
				?,
				?,
				?,
				?,
				?,
				?,
				?,
				?,
			    ?
			)
		`
		query = `
			SELECT
				int8,
				int16,
				int32,
				int64,
				uint8,
				uint16,
				uint32,
				uint64,
				float32,
				float64,
				string,
				fString,
				date,
				datetime,
				datetime64,
				ipv4str,
				ipv6str
			FROM clickhouse_test_insert
		`
	)

	connDB := openConnection(t, dropTable, ddl)
	defer closeConnection(t, connDB, dropTable)

	type item struct {
		Int8        int8
		Int16       int16
		Int32       int32
		Int64       int64
		UInt8       uint8
		UInt16      uint16
		UInt32      uint32
		UInt64      uint64
		Float32     float32
		Float64     float64
		String      string
		FixedString string
		Date        time.Time
		DateTime    time.Time
		DateTime64  time.Time
		Ipv6        net.IP
		Ipv4        net.IP
	}

	refData := &item{
		Int8:        -1,
		Int16:       -2,
		Int32:       -4,
		Int64:       -8, // int
		UInt8:       uint8(1),
		UInt16:      uint16(2),
		UInt32:      uint32(4),
		UInt64:      uint64(8), // uint
		Float32:     1.32 * float32(10),
		Float64:     1.64 * float64(10), // float
		String:      "mummy",            // string
		FixedString: "RU",               // fixedstring,
		Date:        time.Now(),         // date
		DateTime:    time.Now(),         // datetime
		DateTime64:  time.Now(),         // datetime64
		Ipv4:        net.ParseIP("192.168.5.18"),
		Ipv6:        net.ParseIP("2001:0db8:85a3:0000:0000:8a2e:0370:7334"),
	}

	for i := 1; i <= 10; i++ {
		_, err = connDB.Exec(
			dml,
			refData.Int8,
			refData.Int16,
			refData.Int32,
			refData.Int64, // int
			refData.UInt8,
			refData.UInt16,
			refData.UInt32,
			refData.UInt64, // uint
			refData.Float32,
			refData.Float64,     // float
			refData.String,      // string
			refData.FixedString, // fixedstring
			refData.Date,        // date
			refData.DateTime,    // datetime
			refData.DateTime64,  // datetime64
			refData.Ipv4,
			refData.Ipv6,
		)
		assertNoErr(t, err)
	}

	rowData, err := connDB.Query(query)
	assertNoErr(t, err)
	defer rowData.Close()

	var data item
	for rowData.Next() {
		err := rowData.Scan(
			&data.Int8,
			&data.Int16,
			&data.Int32,
			&data.Int64,
			&data.UInt8,
			&data.UInt16,
			&data.UInt32,
			&data.UInt64,
			&data.Float32,
			&data.Float64,
			&data.String,
			&data.FixedString,
			&data.Date,
			&data.DateTime,
			&data.DateTime64,
			&data.Ipv4,
			&data.Ipv6,
		)
		assertNoErr(t, err)
		assertEqual(t, refData.Int8, data.Int8)
		assertEqual(t, refData.Int16, data.Int16)
		assertEqual(t, refData.Int32, data.Int32)
		assertEqual(t, refData.Int64, data.Int64)
		assertEqual(t, refData.UInt8, data.UInt8)
		assertEqual(t, refData.UInt16, data.UInt16)
		assertEqual(t, refData.UInt32, data.UInt32)
		assertEqual(t, refData.UInt64, data.UInt64)
		assertEqual(t, refData.Float32, data.Float32)
		assertEqual(t, refData.Float64, data.Float64)
		assertEqual(t, refData.String, data.String)
		assertEqual(t, refData.FixedString, data.FixedString)
		assertEqualDate(t, refData.Date, data.Date)
		assertEqualTime(t, refData.DateTime, data.DateTime)
		assertEqualTime(t, refData.DateTime64, data.DateTime64)
		assertTrue(t, refData.Ipv4.Equal(data.Ipv4))
		assertTrue(t, refData.Ipv6.Equal(data.Ipv6))
	}
}

func Test_Open(t *testing.T) {
	utils.SkipIntegrationTestIfShort(t)

	type args struct {
		dsn string
	}
	tests := []struct {
		name    string
		args    args
		want    driver.Conn
		wantErr bool
	}{
		{
			name: "Valid dsn returns valid db",
			args: args{
				dsn: "tcp://localhost:9000?user=default&compress=true",
			},
			wantErr: false,
		},
		{
			name: "Invalid dsn returns errChan",
			args: args{
				dsn: "tcp://localhost:9000poo?user=default&compress=true",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := GatewayDriver{}
			got, err := d.Open(tt.args.dsn)
			if (err != nil) != tt.wantErr {
				t.Errorf("Open() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if _, ok := got.(driver.Conn); !tt.wantErr && !ok {
				t.Errorf("Open() got = %v, return type is not driver.Conn", got)
			}
		})
	}
}

func Test_InsertLarge(t *testing.T) {
	utils.SkipIntegrationTestIfShort(t)

	var err error

	const (
		ddl = `
			CREATE TABLE clickhouse_test_insert_batch (
				int8  Int8,
				int16 Int16,
				int32 Int32,
				int64 Int64,
				uint8  UInt8,
				uint16 UInt16,
				uint32 UInt32,
				uint64 UInt64,
				float32 Float32,
				float64 Float64,
				string  String,
				fString FixedString(2),
				date    Date,
				datetime DateTime,
				arrayString Array(String)
			) Engine=Memory
		`
		dropTable = "DROP TABLE IF EXISTS clickhouse_test_insert_batch"
		dml       = `
			INSERT INTO clickhouse_test_insert_batch (
				int8,
				int16,
				int32,
				int64,
				uint8,
				uint16,
				uint32,
				uint64,
				float32,
				float64,
				string,
				fString,
				date,
				datetime,
				arrayString
			) VALUES (
				?,
				?,
				?,
				?,
				?,
				?,
				?,
				?,
				?,
				?,
				?,
				?,
				?,
				?,
				?
			)
		`
		query = `SELECT COUNT(*) FROM clickhouse_test_insert_batch`
	)

	connDB := openConnection(t, dropTable, ddl)
	defer closeConnection(t, connDB, dropTable)

	for i := 1; i <= 1000; i++ {
		_, err = connDB.Exec(
			dml,
			int8(-1*i), int16(-2*i), int32(-4*i), int64(-8*i), // int
			uint8(1*i), uint16(2*i), uint32(4*i), uint64(8*i), // uint
			1.32*float32(i), 1.64*float64(i), //float
			fmt.Sprintf("string %d ", i), // string
			"RU",                         //fixedstring,
			time.Now(),                   //date
			time.Now(),                   //datetime
			[]string{"A", "B", "C"},
		)
		assertNoErr(t, err)
	}

	rowData, err := connDB.Query(query)
	assertNoErr(t, err)
	defer rowData.Close()

	var count int
	for rowData.Next() {
		err = rowData.Scan(&count)
		assertNoErr(t, err)
		assertEqual(t, 1000, count)
	}
}

func TestOpenWithConnector(t *testing.T) {
	utils.SkipIntegrationTestIfShort(t)

	connecter, err := NewConnector(bytehouse.DefaultConnectionContext, "user=default")
	require.NoError(t, err)

	g := sql.OpenDB(connecter)
	require.NotNil(t, g)

	require.NoError(t, g.Ping())
	_ = g.Close()
}

// Invalid should throw an error
func Test_conn_Invalid_Query(t *testing.T) {
	utils.SkipIntegrationTestIfShort(t)

	c := context.Background()
	db, err := sql.Open("bytehouse", "tcp://localhost:9000?user=default&compress=true")
	if err != nil {
		t.Fatalf("Fail to open db")
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("Ping failed")
	}

	_, err = db.QueryContext(c, "SELECT A")
	assert.Error(t, err)
}

func TestQueryWithByteHouseContext(t *testing.T) {
	utils.SkipIntegrationTestIfShort(t)

	gw, err := sdk.Open(bytehouse.DefaultConnectionContext, "user=default")
	if !assert.Nil(t, err) {
		return
	}
	err = gw.Ping()
	assert.Nil(t, err)

	ctx := bytehouse.NewQueryContext(context.Background())
	err = ctx.AddQuerySetting("send_logs_level", "trace")
	if !assert.Nil(t, err) {
		return
	}
	r, err := gw.QueryContext(ctx, "select 1")
	if !assert.Nil(t, err) {
		return
	}
	r.Close()

	logs := r.GetAllLogs()
	assert.True(t, len(logs) > 0)
}

func TestQueryWithSettingsInDSN(t *testing.T) {
	utils.SkipIntegrationTestIfShort(t)

	gw, err := sdk.Open(bytehouse.DefaultConnectionContext, "user=default&send_logs_level=trace")
	if !assert.Nil(t, err) {
		return
	}
	err = gw.Ping()
	assert.Nil(t, err)

	if !assert.Nil(t, err) {
		return
	}
	r, err := gw.Query("select 1")
	if !assert.Nil(t, err) {
		return
	}

	logs := r.GetAllLogs()
	assert.True(t, len(logs) > 0)
}

// Types tested
// - [ ] BitMap (not tested b/c not supported in community clickhouse server)
// - [x] Map
// - [x] Array
// - [x] Date
// - [x] DateTime
// - [x] DateTime64
// - [x] Decimal
// - [x] LowCardinality
// - [x] Tuple
// - [x] Uuid
// - [x] Enum8
// - [x] Enum16
// - [x] FixedString
// - [x] Float32
// - [x] Float64
// - [x] Ipv4
// - [x] Ipv6
// - [x] Uint8
// - [x] Uint16
// - [x] Uint32
// - [x] Uint64
// - [x] Int8
// - [x] Int16
// - [x] Int32
// - [x] Int64
func TestDriver_Insert(t *testing.T) {
	utils.SkipIntegrationTestIfShort(t)
	type count struct {
		expected int
	}
	type args struct {
		ctx   context.Context
		query string
		args  [][]interface{}
	}
	tests := []struct {
		name          string
		args          args
		setupQuery    string
		teardownQuery string
		selectQuery   string
		compareFunc   func(t *testing.T, expected, result []interface{})
		rowValues     interface{}
		want          driver.Rows
		wantErr       bool
	}{
		{
			name: "Can insert 1 row",
			setupQuery: `CREATE TABLE sample_table (
				Id UInt32,
				Color String
			) ENGINE=MergeTree ORDER BY Id`,
			teardownQuery: "DROP TABLE IF EXISTS sample_table",
			args: args{
				ctx:   context.Background(),
				query: "INSERT INTO sample_table VALUES (?, ?)",
				args:  [][]interface{}{{uint32(1), "red"}},
			},
			compareFunc: func(t *testing.T, expected, result []interface{}) {
				assert.ElementsMatch(t, expected, result)
			},
			rowValues: &struct {
				Id    uint32
				Color string
			}{
				Id:    0,
				Color: "",
			},
			selectQuery: "SELECT * FROM sample_table where Id = 1 and Color = 'red'",
			wantErr:     false,
		},
		{
			name: "Can insert 2 rows",
			setupQuery: `CREATE TABLE sample_table (
				Id UInt32,
				Color String
			) ENGINE=MergeTree ORDER BY Id`,
			teardownQuery: "DROP TABLE IF EXISTS sample_table",
			compareFunc: func(t *testing.T, expected, result []interface{}) {
				assert.ElementsMatch(t, expected, result)
			},
			args: args{
				ctx:   context.Background(),
				query: "INSERT INTO sample_table VALUES (?, ?)",
				args: [][]interface{}{
					{uint32(1), "red"},
					{uint32(2), "re"},
					{uint32(3), "r"},
					{uint32(4), "redd"},
					{uint32(5), "reddd"},
					{uint32(6), "redddd"},
					{uint32(7), "reddddd"},
				},
			},
			selectQuery: "SELECT * FROM sample_table ORDER BY Id",
			wantErr:     false,
		},
		{
			name: "Can insert many rows of different common types",
			setupQuery: `CREATE TABLE clickhouse_test_insert_batch (
				int8  Int8,
				int16 Int16,
				int32 Int32,
				int64 Int64,
				uint8  UInt8,
				uint16 UInt16,
				uint32 UInt32,
				uint64 UInt64,
				float32 Float32,
				float64 Float64,
				string  String,
				fString FixedString(2),
				uuid UUID
			) Engine=MergeTree ORDER BY int8`,
			teardownQuery: "DROP TABLE IF EXISTS clickhouse_test_insert_batch;",
			compareFunc: func(t *testing.T, expected, result []interface{}) {
				assert.ElementsMatch(t, expected, result)
			},
			args: args{
				ctx: context.Background(),
				query: `INSERT INTO clickhouse_test_insert_batch (
				int8,
				int16,
				int32,
				int64,
				uint8,
				uint16,
				uint32,
				uint64,
				float32,
				float64,
				string,
				fString,
				uuid
			) VALUES (
				?,
				?,
				?,
				?,
				?,
				?,
				?,
				?,
				?,
				?,
				?,
				?,
				?
			)`,
				args: func() (ret [][]interface{}) {
					for i := 1; i <= 10; i++ {
						ret = append(ret, []interface{}{
							int8(-1 * i), int16(-2 * i), int32(-4 * i), int64(-8 * i), // int
							uint8(1 * i), uint16(2 * i), uint32(4 * i), uint64(8 * i), // uint
							1.32 * float32(i), 1.64 * float64(i), //float
							fmt.Sprintf("string %d ", i), // string
							"RU",                         //fixedstring
							uuid.New(),
						})
					}
					return ret
				}(),
			},
			selectQuery: `SELECT 	
				int8,
				int16,
				int32,
				int64,
				uint8,
				uint16,
				uint32,
				uint64,
				float32,
				float64,
				string,
				fString, 
				uuid
				FROM clickhouse_test_insert_batch ORDER BY int8 DESC`,
			wantErr: false,
		},
		{
			name: "Can insert many rows of ip types",
			setupQuery: `
			CREATE TABLE clickhouse_test_insert (
				ipv4 IPv4,
				ipv6 IPv6
			) Engine=MergeTree ORDER BY ipv4
			`,
			teardownQuery: "DROP TABLE IF EXISTS clickhouse_test_insert",
			compareFunc: func(t *testing.T, expected, result []interface{}) {
				assert.Equal(t, len(expected), len(result))
				for i := range expected {
					assert.Equal(t, fmt.Sprint(expected[i]), fmt.Sprint(result[i]))
				}
			},
			args: args{
				ctx: context.Background(),
				query: `INSERT INTO clickhouse_test_insert (
				ipv4,
				ipv6
			) VALUES (
				?,
				?
			)`,
				args: func() (ret [][]interface{}) {
					for i := 1; i <= 10; i++ {
						ret = append(ret, []interface{}{
							net.ParseIP("192.0.2.1"),
							net.ParseIP("::ffff:192.0.2.1"),
						})
					}
					return ret
				}(),
			},
			selectQuery: "SELECT * FROM clickhouse_test_insert",
			wantErr:     false,
		},
		{
			name: "Can insert many rows of array types",
			setupQuery: `
			CREATE TABLE clickhouse_test_array (
				int8     Array(Int8),
				int16    Array(Int16),
				int32    Array(Int32),
				int64    Array(Int64),
				uint8    Array(UInt8),
				uint16   Array(UInt16),
				uint32   Array(UInt32),
				uint64   Array(UInt64),
				float32  Array(Float32),
				float64  Array(Float64),
				string   Array(String),
				fString  Array(FixedString(2)),
				enum8    Array(Enum8 ('a' = 1, 'b' = 2)),
				enum16   Array(Enum16('c' = 1, 'd' = 2))
			) Engine=Memory
		`,
			teardownQuery: "DROP TABLE IF EXISTS clickhouse_test_array",
			compareFunc: func(t *testing.T, expected, result []interface{}) {
				interfaceSlice := func(slice interface{}) []interface{} {
					if slice == nil {
						return []interface{}{}
					}

					s := reflect.ValueOf(slice)
					if s.Kind() != reflect.Slice {
						t.Errorf("InterfaceSlice() given a non-slice type")
						return nil
					}

					// Keep the distinction between nil and empty slice input
					if s.IsNil() {
						return nil
					}

					ret := make([]interface{}, s.Len())

					for i := 0; i < s.Len(); i++ {
						ret[i] = s.Index(i).Interface()
					}

					return ret
				}

				assert.Equal(t, len(expected), len(result))

				for i, eValues := range expected {
					eValues := interfaceSlice(eValues)
					assert.NotNil(t, eValues)
					rValues, ok := result[i].([]interface{})
					assert.True(t, ok)
					assert.Equal(t, len(eValues), len(rValues))

					for j, ev := range eValues {
						assert.Equal(t, ev, rValues[j])
					}
				}
			},
			args: args{
				ctx: context.Background(),
				query: `
			INSERT INTO clickhouse_test_array (
				int8,
				int16,
				int32,
				int64,
				uint8,
				uint16,
				uint32,
				uint64,
				float32,
				float64,
				string,
				fString,
				enum8,
				enum16
			) VALUES (
				?,
				?,
				?,
				?,
				?,
				?,
				?,
				?,
				?,
				?,
				?,
				?
				?,
				?
			)
		`,
				args: func() (ret [][]interface{}) {
					for i := 1; i <= 10; i++ {
						ret = append(ret,
							[]interface{}{
								nil,
								[]int16{5, 6, 7},
								[]int32{8, 9, 10},
								[]int64{11, 12, 13},
								[]uint8{14, 15, 16},
								[]uint16{17, 18, 19},
								[]uint32{20, 21, 22},
								[]uint64{23, 24, 25},
								[]float32{32.1, 32.2},
								[]float64{64.1, 64.2},
								[]string{fmt.Sprintf("A"), "B", "C"},
								[]string{"RU", "EN", "DE"},
								[]string{"a", "b"},
								[]string{"c", "d"},
							})
					}
					return ret
				}(),
			},
			selectQuery: `SELECT 				
				int8,
				int16,
				int32,
				int64,
				uint8,
				uint16,
				uint32,
				uint64,
				float32,
				float64,
				string,
				fString,
				enum8,
				enum16 FROM clickhouse_test_array`,
			wantErr: false,
		},
		{
			name: "Can insert many rows of tuple types",
			setupQuery: `
			CREATE TABLE clickhouse_test_tuple (
				t     Tuple(
						Int8, Int16, Int32, Int64,
						UInt8, UInt16, UInt32, UInt64,
						Float32, Float64, String, FixedString(2),
						Enum8 ('a' = 1, 'b' = 2), Enum16('c' = 1, 'd' = 2)
					)
			) Engine=Memory
		`,
			teardownQuery: "DROP TABLE IF EXISTS clickhouse_test_tuple",
			compareFunc: func(t *testing.T, expected, result []interface{}) {
				interfaceSlice := func(slice interface{}) []interface{} {
					s := reflect.ValueOf(slice)
					if s.Kind() != reflect.Slice {
						t.Errorf("InterfaceSlice() given a non-slice type")
						return nil
					}

					// Keep the distinction between nil and empty slice input
					if s.IsNil() {
						return nil
					}

					ret := make([]interface{}, s.Len())

					for i := 0; i < s.Len(); i++ {
						ret[i] = s.Index(i).Interface()
					}

					return ret
				}

				assert.Equal(t, len(expected), len(result))

				for i, eValues := range expected {
					eValues := interfaceSlice(eValues)
					assert.NotNil(t, eValues)
					rValues, ok := result[i].([]interface{})
					assert.True(t, ok)
					assert.Equal(t, len(eValues), len(rValues))

					for j, ev := range eValues {
						assert.Equal(t, ev, rValues[j])
					}
				}
			},
			args: args{
				ctx: context.Background(),
				query: `
			INSERT INTO clickhouse_test_tuple (
				t
			) VALUES (
				?
			)
		`,
				args: func() (ret [][]interface{}) {
					for i := 1; i <= 10; i++ {
						ret = append(ret,
							[]interface{}{
								[]interface{}{
									int8(1),
									int16(5),
									int32(8),
									int64(11),
									uint8(14),
									uint16(17),
									uint32(20),
									uint64(23),
									float32(32.1),
									float64(64.1),
									"A_5",
									"RU",
									"a",
									"c",
								},
							})
					}
					return ret
				}(),
			},
			selectQuery: `SELECT 				
				* FROM clickhouse_test_tuple`,
			wantErr: false,
		},
		{
			name: "Test dates",
			setupQuery: `
			CREATE TABLE clickhouse_test_date (
				date Date,
				datetime DateTime,
				datetime64 DateTime64
			) Engine=Memory
			`,
			teardownQuery: "DROP TABLE IF EXISTS clickhouse_test_date",
			compareFunc: func(t *testing.T, expected, result []interface{}) {
				assert.Equal(t, len(expected), len(result))
				for i := range expected {
					assert.Equal(t, fmt.Sprint(expected[i])[:10], fmt.Sprint(result[i])[:10])
				}
			},
			args: args{
				ctx: context.Background(),
				query: `INSERT INTO clickhouse_test_date (
				date,
				datetime,
				datetime64
			) VALUES (
				?,
				?,
				?
			)`,
				args: func() (ret [][]interface{}) {
					for i := 1; i <= 10; i++ {
						ret = append(ret, []interface{}{
							time.Now(),
							time.Now(),
							time.Now(),
						})
					}
					return ret
				}(),
			},
			selectQuery: "SELECT * FROM clickhouse_test_date",
			wantErr:     false,
		},
		{
			name: "Test decimals",
			setupQuery: `
			CREATE TABLE clickhouse_test_decimal (
				decimal Decimal(18,5)
			) Engine=Memory
			`,
			teardownQuery: "DROP TABLE IF EXISTS clickhouse_test_decimal",
			compareFunc: func(t *testing.T, expected, result []interface{}) {
				assert.Equal(t, len(expected), len(result))
				for i := range expected {
					assert.Equal(t, fmt.Sprint(expected[i]), fmt.Sprint(result[i]))
				}
			},
			args: args{
				ctx: context.Background(),
				query: `INSERT INTO clickhouse_test_decimal (
				decimal
			) VALUES (
				?
			)`,
				args: func() (ret [][]interface{}) {
					for i := 1; i <= 10; i++ {
						ret = append(ret, []interface{}{
							float64(122),
						})
					}
					return ret
				}(),
			},
			selectQuery: "SELECT * FROM clickhouse_test_decimal",
			wantErr:     false,
		},
		{
			name: "Test lowCardinality",
			setupQuery: `
			CREATE TABLE clickhouse_test_lowCardinality (
				lowCardinality1 LowCardinality(String),
				lowCardinality2 LowCardinality(String)
			) Engine=Memory
			`,
			teardownQuery: "DROP TABLE IF EXISTS clickhouse_test_lowCardinality",
			compareFunc: func(t *testing.T, expected, result []interface{}) {
				assert.Equal(t, len(expected), len(result))
				for i := range expected {
					assert.Equal(t, expected[i], result[i])
				}
			},
			args: args{
				ctx: context.Background(),
				query: `INSERT INTO clickhouse_test_lowCardinality (
				lowCardinality1,
				lowCardinality2
			) VALUES (
				?,
				?
			)`,
				args: func() (ret [][]interface{}) {
					for i := 1; i <= 10; i++ {
						ret = append(ret, []interface{}{
							"fewfweewf", "fewfwe",
						})
					}
					return ret
				}(),
			},
			selectQuery: "SELECT * FROM clickhouse_test_lowCardinality",
			wantErr:     false,
		},
		{
			name: "Should throw error if insert arguments less than expected",
			setupQuery: `
			CREATE TABLE clickhouse_test_lowCardinality (
				lowCardinality1 LowCardinality(String),
				lowCardinality2 LowCardinality(String)
			) Engine=Memory
			`,
			teardownQuery: "DROP TABLE IF EXISTS clickhouse_test_lowCardinality",
			compareFunc: func(t *testing.T, expected, result []interface{}) {
				assert.Equal(t, len(expected), len(result))
				for i := range expected {
					assert.Equal(t, expected[i], result[i])
				}
			},
			args: args{
				ctx: context.Background(),
				query: `INSERT INTO clickhouse_test_lowCardinality (
				lowCardinality1,
				lowCardinality2
			) VALUES (
				?,
				?
			)`,
				args: func() (ret [][]interface{}) {
					for i := 1; i <= 10; i++ {
						ret = append(ret, []interface{}{
							"fewfweewf",
						})
					}
					return ret
				}(),
			},
			selectQuery: "SELECT * FROM clickhouse_test_lowCardinality",
			wantErr:     true,
		},
		{
			name: "Test map",
			setupQuery: `
			CREATE TABLE clickhouse_test_map (
				map1 Map(String, Int8),
				map2 Map(String, Int8)
			) Engine=Memory
			`,
			teardownQuery: "DROP TABLE IF EXISTS clickhouse_test_map",
			compareFunc: func(t *testing.T, expected, result []interface{}) {
				assert.Equal(t, len(expected), len(result))
				for i := range expected {
					assert.Equal(t, expected[i], result[i])
				}
			},
			args: args{
				ctx: context.Background(),
				query: `INSERT INTO clickhouse_test_map (
				map1,
				map2
			) VALUES (
				?,
				?
			)`,
				args: func() (ret [][]interface{}) {
					for i := 1; i <= 10; i++ {
						ret = append(ret, []interface{}{
							map[string]int8{
								"baba": int8(1),
								"mama": int8(2),
							},
							map[string]int8{
								"gege":   int8(1),
								"meimei": int8(2),
							},
						})
					}
					return ret
				}(),
			},
			selectQuery: "SELECT * FROM clickhouse_test_map",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup table script
			g := openConnection(t, tt.teardownQuery, tt.setupQuery)
			defer closeConnection(t, g, tt.teardownQuery)

			for _, arg := range tt.args.args {
				_, err := g.ExecContext(tt.args.ctx, tt.args.query, arg...)
				if err != nil {
					if tt.wantErr {
						assert.Error(t, err)
						return
					}

					assert.NoError(t, err)
					return
				}
			}

			r, err := g.QueryContext(tt.args.ctx, tt.selectQuery)
			if err != nil {
				assert.NoError(t, err)
				return
			}

			res := make([]interface{}, len(tt.args.args[0]))
			resPtrs := make([]interface{}, len(tt.args.args[0]))
			for i := range res {
				resPtrs[i] = &res[i]
			}

			i := 0
			for r.Next() {
				err := r.Scan(resPtrs...)
				if err != nil {
					assert.NoError(t, err)
					return
				}
				tt.compareFunc(t, tt.args.args[i], res)
				i++
			}

			assert.Equal(t, len(tt.args.args), i)
		})
	}
}

func Test_InsertString_AndSelect(t *testing.T) {
	utils.SkipIntegrationTestIfShort(t)

	tests := []struct {
		name           string
		setupQuery     string
		teardownQuery  string
		insertQuery    string
		selectQuery    string
		wantErr        bool
		compareFunc    func(t *testing.T, expected, result []interface{})
		expectedValues []interface{}
	}{
		{

			name: "Can insert many rows of int types",
			setupQuery: `CREATE TABLE clickhouse_test_insert_batch (
						int8  Int8,
						int16 Int16,
						int32 Int32,
						int64 Int64
					) Engine=MergeTree ORDER BY int8`,
			teardownQuery: "DROP TABLE IF EXISTS clickhouse_test_insert_batch;",
			insertQuery: `INSERT INTO clickhouse_test_insert_batch (
						int8,
						int16,
						int32,
						int64 
					) VALUES (1,2,3,4)`,
			selectQuery: `SELECT
						int8,
						int16,
						int32,
						int64
						FROM clickhouse_test_insert_batch`,
			expectedValues: []interface{}{int8(1), int16(2), int32(3), int64(4)},
		},
		{

			name: "Can insert empty values of int types",
			setupQuery: `CREATE TABLE clickhouse_test_insert_batch (
						int8  Int8,
						int16 Int16,
						int32 Int32,
						int64 Int64
					) Engine=MergeTree ORDER BY int8`,
			teardownQuery: "DROP TABLE IF EXISTS clickhouse_test_insert_batch;",
			insertQuery: `INSERT INTO clickhouse_test_insert_batch (
						int8,
						int16,
						int32,
						int64 
					) VALUES (,,,)`,
			selectQuery: `SELECT
						int8,
						int16,
						int32,
						int64
						FROM clickhouse_test_insert_batch`,
			expectedValues: []interface{}{int8(0), int16(0), int32(0), int64(0)},
		},
		{

			name: "Can insert many rows of uint types",
			setupQuery: `CREATE TABLE clickhouse_test_insert_batch (
						uint8  UInt8,
						uint16 UInt16,
						uint32 UInt32,
						uint64 UInt64
					) Engine=MergeTree ORDER BY uint8`,
			teardownQuery: "DROP TABLE IF EXISTS clickhouse_test_insert_batch;",
			insertQuery: `INSERT INTO clickhouse_test_insert_batch (
						uint8,
						uint16,
						uint32,
						uint64 
					) VALUES (1,2,3,4)`,
			selectQuery: `SELECT
						uint8,
						uint16,
						uint32,
						uint64
						FROM clickhouse_test_insert_batch`,
			expectedValues: []interface{}{uint8(1), uint16(2), uint32(3), uint64(4)},
		},
		{

			name: "Can insert many empty value rows of uint types",
			setupQuery: `CREATE TABLE clickhouse_test_insert_batch (
						uint8  UInt8,
						uint16 UInt16,
						uint32 UInt32,
						uint64 UInt64
					) Engine=MergeTree ORDER BY uint8`,
			teardownQuery: "DROP TABLE IF EXISTS clickhouse_test_insert_batch;",
			insertQuery: `INSERT INTO clickhouse_test_insert_batch (
						uint8,
						uint16,
						uint32,
						uint64 
					) VALUES (,,,)`,
			selectQuery: `SELECT
						uint8,
						uint16,
						uint32,
						uint64
						FROM clickhouse_test_insert_batch`,
			expectedValues: []interface{}{uint8(0), uint16(0), uint32(0), uint64(0)},
		},
		{

			name: "Can insert many rows of ip types",
			setupQuery: `CREATE TABLE clickhouse_test_insert_batch (
						ipv4 IPv4,
						ipv6 IPv6,
						ipv4a IPv4,
						ipv6a IPv6
					) Engine=MergeTree ORDER BY ipv4`,
			teardownQuery: "DROP TABLE IF EXISTS clickhouse_test_insert_batch;",
			insertQuery: `INSERT INTO clickhouse_test_insert_batch (
						ipv4,
						ipv6,
						ipv4a,
						ipv6a
					) VALUES (100.139.220.39,103.136.220.38,,)`,
			selectQuery: `SELECT
						ipv4,
						ipv6,
						ipv4a,
						ipv6a
						FROM clickhouse_test_insert_batch`,
			expectedValues: []interface{}{net.ParseIP("100.139.220.39"), net.ParseIP("103.136.220.38"), net.IPv4zero, net.IPv6zero},
			compareFunc: func(t *testing.T, expected, result []interface{}) {
				for i, v := range expected {
					expectedV := v.(net.IP)
					resultV := result[i].(net.IP)
					assertTrue(t, expectedV.Equal(resultV))
				}
			},
		},
		{

			name: "Can insert many value rows of float types",
			setupQuery: `CREATE TABLE clickhouse_test_insert_batch (
						float32 Float32,
						float64 Float64,
						decimal Decimal(18, 5),
						float32a Float32,
						float64b Float64,
						decimalc Decimal(18, 5)
					) Engine=MergeTree ORDER BY float32`,
			teardownQuery: "DROP TABLE IF EXISTS clickhouse_test_insert_batch;",
			insertQuery: `INSERT INTO clickhouse_test_insert_batch (
						float32,
						float64,
						decimal,
						float32a,
						float64b,
						decimalc
					) VALUES (32,23,23,,,)`,
			selectQuery: `SELECT
						float32,
						float64,
						decimal,
						float32a,
						float64b,
						decimalc
						FROM clickhouse_test_insert_batch`,
			expectedValues: []interface{}{float32(32), float64(23), float64(23), float32(0), float64(0), float64(0)},
		},
		{

			name: "Can insert many value rows of string types",
			setupQuery: `CREATE TABLE clickhouse_test_insert_batch (
						string  String,
						nullable  Nullable(String),
						lowCardinality LowCardinality(String),
						uuid UUID,
						fString FixedString(1),
						stringA  String,
						nullableA  Nullable(String),
						lowCardinalityA LowCardinality(String),
						uuidA UUID,
						fStringA FixedString(2)
					) Engine=MergeTree ORDER BY string`,
			teardownQuery: "DROP TABLE IF EXISTS clickhouse_test_insert_batch;",
			insertQuery: `INSERT INTO clickhouse_test_insert_batch (
						string,
						nullable,
						lowCardinality,
						uuid,
						fString,
						stringA,
						nullableA,
						lowCardinalityA,
						uuidA,
						fStringA
					) VALUES (a, b, c, 123e4567-e89b-12d3-a456-426614174000, e,,null,,,)`,
			selectQuery: `SELECT
						string,
						nullable,
						lowCardinality,
						uuid,
						fString,
						stringA,
						nullableA,
						lowCardinalityA,
						uuidA,
						fStringA
						FROM clickhouse_test_insert_batch`,
			expectedValues: []interface{}{"a", "b", "c", uuid.MustParse("123e4567-e89b-12d3-a456-426614174000"), "e", "", nil, "", uuid.MustParse("00000000-0000-0000-0000-000000000000"), ""},
		},
		{

			name: "Can insert many value rows of date types",
			setupQuery: `CREATE TABLE clickhouse_test_insert_batch (
						date    Date,
						datetime DateTime,
						datetime64 DateTime64,
						dateA    Date,
						datetimeA DateTime,
						datetime64A DateTime64
					) Engine=MergeTree ORDER BY date`,
			teardownQuery: "DROP TABLE IF EXISTS clickhouse_test_insert_batch;",
			insertQuery: `INSERT INTO clickhouse_test_insert_batch (
						date,
						datetime,
						datetime64,
						dateA,
						datetimeA,
						datetime64A
					) VALUES ('1970-01-02', '1970-01-02', '1970-01-02',,,)`,
			selectQuery: `SELECT
						date,
						datetime,
						datetime64,
						dateA,
						datetimeA,
						datetime64A
						FROM clickhouse_test_insert_batch`,
			expectedValues: []interface{}{"1970-01-02", "1970-01-02", "1970-01-02", "1970-01-01", "1970-01-01", "1970-01-01"},
			compareFunc: func(t *testing.T, expected, result []interface{}) {
				for i, v := range expected {
					expectedV := v
					resultV := result[i].(time.Time)
					assert.Equal(t, expectedV, resultV.String()[:10])
				}
			},
		},
		{
			name: "Can insert rows of array types",
			setupQuery: `CREATE TABLE clickhouse_test_insert_batch (
						int8 Array(Int8),
						uint8  Array(UInt8),
						uint16  Array(UInt16),
						uint32  Array(UInt32),
						uint64  Array(UInt64)
					) Engine=MergeTree ORDER BY uint8`,
			teardownQuery: "DROP TABLE IF EXISTS clickhouse_test_insert_batch;",
			insertQuery: `INSERT INTO clickhouse_test_insert_batch (
						int8,
						uint8,
						uint16,
						uint32,
						uint64
					) VALUES (,[1,2,3,4],,,)`,
			selectQuery: `SELECT
						int8,
						uint8,
						uint16,
						uint32,
						uint64
						FROM clickhouse_test_insert_batch`,
			expectedValues: []interface{}{[]interface{}{}, []interface{}{uint8(1), uint8(2), uint8(3), uint8(4)}, []interface{}{}, []interface{}{}, []interface{}{}},
		},
		{
			name: "Can throw err if wrong array format",
			setupQuery: `CREATE TABLE clickhouse_test_insert_batch (
						int8 Array(Int8),
						uint8  Array(UInt8),
						uint16  Array(UInt16),
						uint32  Array(UInt32),
						uint64  Array(UInt64)
					) Engine=MergeTree ORDER BY uint8`,
			teardownQuery: "DROP TABLE IF EXISTS clickhouse_test_insert_batch;",
			insertQuery: `INSERT INTO clickhouse_test_insert_batch (
						int8,
						uint8,
						uint16,
						uint32,
						uint64
					) VALUES (a,[1,2,3,4],,,)`,
			selectQuery: `SELECT
						int8,
						uint8,
						uint16,
						uint32,
						uint64
						FROM clickhouse_test_insert_batch`,
			wantErr: true,
		},
		{
			name: "Can throw err if syntax incorrect",
			setupQuery: `CREATE TABLE clickhouse_test_insert_batch (
						int8  Int8
					) Engine=MergeTree ORDER BY int8`,
			teardownQuery: "DROP TABLE IF EXISTS clickhouse_test_insert_batch;",
			insertQuery: `INSERT INTO clickhouse_test_insert_batch (
						int8
					) VALUES (1) nietzsche`,
			selectQuery: `SELECT
						int8,
						FROM clickhouse_test_insert_batch`,
			expectedValues: []interface{}{int8(1)},
			wantErr:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := context.Background()
			g := openConnection(t, tt.teardownQuery, tt.setupQuery)
			defer closeConnection(t, g, tt.teardownQuery)

			_, err := g.ExecContext(c, tt.insertQuery)
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			r, err := g.QueryContext(c, tt.selectQuery)
			require.NoError(t, err)

			res := make([]interface{}, len(tt.expectedValues))
			resPtrs := make([]interface{}, len(tt.expectedValues))
			for i := range res {
				resPtrs[i] = &res[i]
			}

			for r.Next() {
				err := r.Scan(resPtrs...)
				require.NoError(t, err)
				if tt.compareFunc != nil {
					tt.compareFunc(t, tt.expectedValues, res)
					continue
				}
				assert.ElementsMatch(t, tt.expectedValues, res)
			}
		})
	}
}

func Test_SelectRowsColumnTypes(t *testing.T) {
	utils.SkipIntegrationTestIfShort(t)

	tests := []struct {
		name           string
		setupQuery     string
		teardownQuery  string
		insertQuery    string
		selectQuery    string
		wantErr        bool
		compareFunc    func(t *testing.T, expected, result []interface{})
		expectedValues []interface{}
	}{
		{

			name: "Can select row type of many in types",
			setupQuery: `CREATE TABLE clickhouse_test_insert_batch (
						int8  Int8,
						int16 Int16,
						int32 Int32,
						int64 Int64
					) Engine=MergeTree ORDER BY int8`,
			teardownQuery: "DROP TABLE IF EXISTS clickhouse_test_insert_batch;",
			insertQuery: `INSERT INTO clickhouse_test_insert_batch (
						int8,
						int16,
						int32,
						int64 
					) VALUES (1,2,3,4)`,
			selectQuery: `SELECT
						int8,
						int16,
						int32,
						int64
						FROM clickhouse_test_insert_batch`,
		},
		{
			name: "Can select row type of many in types",
			setupQuery: `CREATE TABLE clickhouse_test_insert_batch (
						int8  Int8,
						int16 Int16,
						int32 Int32,
						int64 Int64
					) Engine=MergeTree ORDER BY int8`,
			teardownQuery: "DROP TABLE IF EXISTS clickhouse_test_insert_batch;",
			selectQuery: `SELECT
						int8,
						int16,
						int32,
						int64
						FROM clickhouse_test_insert_batch LIMIT 10`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := context.Background()
			g := openConnection(t, tt.teardownQuery, tt.setupQuery)
			defer closeConnection(t, g, tt.teardownQuery)

			if tt.insertQuery != "" {
				_, err := g.ExecContext(c, tt.insertQuery)
				if tt.wantErr {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
			}

			r, err := g.QueryContext(c, tt.selectQuery)
			require.NoError(t, err)

			_, err = r.ColumnTypes()
			require.NoError(t, err)
		})
	}
}

func Test_InsertWithSelect(t *testing.T) {
	utils.SkipIntegrationTestIfShort(t)

	type args struct {
		query string
	}

	tests := []struct {
		name          string
		driver        string
		args          args
		setupQuery    string
		insertQuery   string
		teardownQuery string
		selectQuery   string
		testResults   func(t *testing.T, qr *sql.Rows)
		wantErr       bool
	}{
		{
			name: "Can insert with select query",
			setupQuery: `CREATE TABLE sample_table (
				dog UInt32,
				cat UInt32
			) ENGINE=MergeTree ORDER BY dog`,
			insertQuery:   `INSERT INTO sample_table VALUES (10, 20), (10, 20)`,
			teardownQuery: "DROP TABLE IF EXISTS sample_table",
			args: args{
				query: "INSERT INTO sample_table SELECT * FROM sample_table",
			},
			selectQuery: "SELECT * FROM sample_table",
			testResults: func(t *testing.T, qr *sql.Rows) {
				i := 0
				for qr.Next() {
					var v1 uint32
					var v2 uint32
					err := qr.Scan(&v1, &v2)
					require.NoError(t, err)
					require.Equal(t, v1, uint32(10))
					require.Equal(t, v2, uint32(20))
					i++
				}
				require.Equal(t, 4, i)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(b *testing.T) {
			c := context.Background()
			// Setup table script
			g := openConnection(t, tt.teardownQuery, tt.setupQuery, tt.insertQuery)
			defer closeConnection(t, g, tt.teardownQuery)

			_, err := g.ExecContext(c, tt.args.query)
			if tt.wantErr {
				require.Error(b, err)
				return
			}
			require.NoError(b, err)

			qr, err := g.QueryContext(c, tt.selectQuery)
			tt.testResults(t, qr)
		})
	}
}

func Test_InsertFromReader(t *testing.T) {
	utils.SkipIntegrationTestIfShort(t)

	tests := []struct {
		name                string
		setupQuery          string
		insertQuery         string
		insertQuerySettings map[string]string
		check               func(t *testing.T, g *sql.DB)
		teardownQuery       string
		selectQuery         string
		fileName            string
		wantRowCount        int
	}{
		{
			name: "Can insert csv file",
			setupQuery: `CREATE TABLE sample_table (
				dog UInt32,
				cat UInt32
			) ENGINE=MergeTree ORDER BY dog`,
			insertQuery:   `INSERT INTO sample_table FORMAT CSV`,
			teardownQuery: "DROP TABLE IF EXISTS sample_table",
			check: func(t *testing.T, g *sql.DB) {
				// Run select query ensure has enough rows
				r, err := g.Query("SELECT COUNT(*) FROM sample_table WHERE dog = 1")
				require.NoError(t, err)
				r.Next()
				var rowCount int
				require.NoError(t, r.Scan(&rowCount))
				require.Equal(t, 50, rowCount)

				// Run select query ensure has enough rows
				r, err = g.Query("SELECT COUNT(*) FROM sample_table WHERE cat = 2")
				require.NoError(t, err)
				r.Next()
				require.NoError(t, r.Scan(&rowCount))
				require.Equal(t, 50, rowCount)
			},
			fileName: "./testdata/insert.csv",
		},
		{
			name: "Can insert csv file with custom separator",
			setupQuery: `CREATE TABLE sample_table (
				dog UInt32,
				cat UInt32
			) ENGINE=MergeTree ORDER BY dog`,
			insertQuery: `INSERT INTO sample_table FORMAT CSV`,
			insertQuerySettings: map[string]string{
				"format_csv_delimiter": "|",
			},
			teardownQuery: "DROP TABLE IF EXISTS sample_table",
			fileName:      "./testdata/insert_with_pipes.csv",
			check: func(t *testing.T, g *sql.DB) {
				// Run select query ensure has enough rows
				r, err := g.Query("SELECT COUNT(*) FROM sample_table WHERE dog = 1")
				require.NoError(t, err)
				r.Next()
				var rowCount int
				require.NoError(t, r.Scan(&rowCount))
				require.Equal(t, 50, rowCount)

				// Run select query ensure has enough rows
				r, err = g.Query("SELECT COUNT(*) FROM sample_table WHERE cat = 2")
				require.NoError(t, err)
				r.Next()
				require.NoError(t, r.Scan(&rowCount))
				require.Equal(t, 50, rowCount)
			},
		},
		{
			name: "Can insert csvwithnames file",
			setupQuery: `CREATE TABLE sample_table (
				dog UInt32,
				cat UInt32
			) ENGINE=MergeTree ORDER BY dog`,
			insertQuery:         `INSERT INTO sample_table FORMAT CSVWithNames`,
			insertQuerySettings: nil,
			teardownQuery:       "DROP TABLE IF EXISTS sample_table",
			fileName:            "./testdata/insert_with_names.csv",
			check: func(t *testing.T, g *sql.DB) {
				// Run select query ensure has enough rows
				r, err := g.Query("SELECT COUNT(*) FROM sample_table WHERE dog = 1")
				require.NoError(t, err)
				r.Next()
				var rowCount int
				require.NoError(t, r.Scan(&rowCount))
				require.Equal(t, 50, rowCount)

				// Run select query ensure has enough rows
				r, err = g.Query("SELECT COUNT(*) FROM sample_table WHERE cat = 2")
				require.NoError(t, err)
				r.Next()
				require.NoError(t, r.Scan(&rowCount))
				require.Equal(t, 50, rowCount)
			},
		},
		{
			name: "Can insert json file",
			setupQuery: `CREATE TABLE sample_table (
				dog UInt32,
				cat UInt32
			) ENGINE=MergeTree ORDER BY dog`,
			insertQuery:         `INSERT INTO sample_table FORMAT JSON`,
			insertQuerySettings: nil,
			teardownQuery:       "DROP TABLE IF EXISTS sample_table",
			fileName:            "./testdata/insert.json",
			check: func(t *testing.T, g *sql.DB) {
				// Run select query ensure has enough rows
				r, err := g.Query("SELECT COUNT(*) FROM sample_table WHERE dog = 1")
				require.NoError(t, err)
				r.Next()
				var rowCount int
				require.NoError(t, r.Scan(&rowCount))
				require.Equal(t, 50, rowCount)

				// Run select query ensure has enough rows
				r, err = g.Query("SELECT COUNT(*) FROM sample_table WHERE cat = 2")
				require.NoError(t, err)
				r.Next()
				require.NoError(t, r.Scan(&rowCount))
				require.Equal(t, 50, rowCount)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(b *testing.T) {
			// Setup table script
			g := openConnection(t, tt.teardownQuery, tt.setupQuery, tt.insertQuery)

			// Open file
			file, err := os.Open(tt.fileName)
			require.NoError(b, err)
			defer file.Close()

			ctx := bytehouse.NewQueryContext(ctx)
			for k, v := range tt.insertQuerySettings {
				err = ctx.AddQuerySetting(k, v)
				require.NoError(b, err)
			}

			// Run insert query
			err = RunConn(ctx, g, func(conn sdk.Conn) error {
				return conn.InsertFromReader(ctx, tt.insertQuery, file)
			})
			require.NoError(b, err)

			tt.check(t, g)
		})
	}
}

func Test_ExportToFile(t *testing.T) {
	utils.SkipIntegrationTestIfShort(t)

	tests := []struct {
		name          string
		setupQuery    string
		insertQuery   string
		check         func(t *testing.T, g *sql.DB)
		teardownQuery string
		selectQuery   string
		fileName      string
		fmtType       string
		wantRowCount  int
		expected      string
	}{
		{
			name: "Can export to csv file",
			setupQuery: `CREATE TABLE sample_table (
				dog UInt32,
				cat UInt32
			) ENGINE=MergeTree ORDER BY dog`,
			insertQuery:   `INSERT INTO sample_table VALUES (1, 2), (3, 4), (5, 6)`,
			teardownQuery: "DROP TABLE IF EXISTS sample_table",
			selectQuery:   "SELECT * FROM sample_table",
			fmtType:       "CSV",
			fileName:      "./testdata/select_temp.csv",
			expected: `1,2
3,4
5,6`,
		},
		{
			name: "Can export to json file",
			setupQuery: `CREATE TABLE sample_table (
				dog UInt32,
				cat UInt32
			) ENGINE=MergeTree ORDER BY dog`,
			insertQuery:   `INSERT INTO sample_table VALUES (1, 2), (3, 4), (5, 6)`,
			teardownQuery: "DROP TABLE IF EXISTS sample_table",
			selectQuery:   "SELECT * FROM sample_table",
			fmtType:       "JSON",
			fileName:      "./testdata/select_temp.json",
			expected: `{
	"meta":
	[
		{
			"name": "dog",
			"type": "UInt32"
		},
		{
			"name": "cat",
			"type": "UInt32"
		}
	],

	"data":
	[
		{			
			"dog": 1,			
			"cat": 2
		},
		{			
			"dog": 3,			
			"cat": 4
		},
		{			
			"dog": 5,			
			"cat": 6
		}
	],

	"rows": 3
}
`,
		},
		{
			name: "Can export to json file with map value",
			setupQuery: `CREATE TABLE sample_table (
				dog Map(String, UInt32),
				cat UInt32
			) ENGINE=MergeTree ORDER BY dog`,
			insertQuery:   `INSERT INTO sample_table VALUES ({1: 1}, 2), ({1: 1}, 4), ({1: 1}, 6)`,
			teardownQuery: "DROP TABLE IF EXISTS sample_table",
			selectQuery:   "SELECT * FROM sample_table",
			fmtType:       "JSON",
			fileName:      "./testdata/select_temp.json",
			expected: `{
	"meta":
	[
		{
			"name": "dog",
			"type": "Map(String, UInt32)"
		},
		{
			"name": "cat",
			"type": "UInt32"
		}
	],

	"data":
	[
		{			
			"dog": "{'1': 1}",			
			"cat": 2
		},
		{			
			"dog": "{'1': 1}",			
			"cat": 4
		},
		{			
			"dog": "{'1': 1}",			
			"cat": 6
		}
	],

	"rows": 3
}
`},
		{
			name: "Can export to values file",
			setupQuery: `CREATE TABLE sample_table (
				dog UInt32,
				cat UInt32
			) ENGINE=MergeTree ORDER BY dog`,
			insertQuery:   `INSERT INTO sample_table VALUES (1, 2), (3, 4), (5, 6)`,
			teardownQuery: "DROP TABLE IF EXISTS sample_table",
			selectQuery:   "SELECT * FROM sample_table",
			fmtType:       "VALUES",
			fileName:      "./testdata/select_temp.values",
			expected: `(1, 2),
(3, 4),
(5, 6)`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(b *testing.T) {
			// Remove file if exist
			_ = os.Remove(tt.fileName)

			// Setup table script
			g := openConnection(t, tt.teardownQuery, tt.setupQuery, tt.insertQuery)

			// Open file
			file, err := os.OpenFile(tt.fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			require.NoError(b, err)

			var reader io.Reader
			// Run insert query
			err = RunConn(ctx, g, func(conn sdk.Conn) error {
				qr, err := conn.QueryContext(ctx, tt.selectQuery)
				if err != nil {
					return err
				}

				reader = qr.ExportToReader(tt.fmtType)
				return nil
			})
			require.NoError(b, err)

			_, err = io.Copy(file, reader)
			require.NoError(b, err)
			_ = file.Close()

			out, err := os.ReadFile(tt.fileName)
			require.NoError(b, err)

			require.Equal(b, tt.expected, string(out))
		})
	}
}

func Test_QueryContextWithExternalTableOnly(t *testing.T) {
	utils.SkipIntegrationTestIfShort(t)

	tests := []struct {
		name          string
		setupQuery    string
		insertQuery   string
		externalTable *sdk.ExternalTable
		check         func(t *testing.T, g *sql.DB)
		teardownQuery string
		selectQuery   string
		fileName      string
		fmtType       string
		wantRowCount  int
		expected      string
	}{
		{
			name: "Can select with external table 1",
			setupQuery: `CREATE TABLE sample_table (
				dog UInt32,
				cat UInt32
			) ENGINE=MergeTree ORDER BY dog`,
			insertQuery: `INSERT INTO sample_table VALUES (1, 2), (3, 4), (5, 7)`,
			selectQuery: "SELECT * FROM sample_table WHERE dog IN (SELECT a FROM fish)",
			externalTable: sdk.NewExternalTable(
				"fish",
				[][]interface{}{
					{uint32(1), uint32(4)},
					{uint32(2), uint32(5)},
					{uint32(3), uint32(6)},
				},
				[]string{"a", "b"},
				[]column.CHColumnType{column.UINT32, column.UINT32},
			),
			teardownQuery: "DROP TABLE IF EXISTS sample_table",
			fmtType:       "CSV",
			fileName:      "./testdata/select_temp.csv",
			expected: `1,2
3,4`,
		},
		{
			name: "Can select with external table 2",
			setupQuery: `CREATE TABLE sample_table (
				dog UInt32,
				cat UInt32
			) ENGINE=MergeTree ORDER BY dog`,
			insertQuery: `INSERT INTO sample_table VALUES (1, 2), (3, 4), (5, 6)`,
			selectQuery: "SELECT * FROM sample_table WHERE dog IN (SELECT b FROM fish)",
			externalTable: sdk.NewExternalTable(
				"fish",
				[][]interface{}{
					{uint32(1), uint32(4)},
					{uint32(2), uint32(5)},
					{uint32(3), uint32(6)},
				},
				[]string{"a", "b"},
				[]column.CHColumnType{column.UINT32, column.UINT32},
			),
			teardownQuery: "DROP TABLE IF EXISTS sample_table",
			fmtType:       "CSV",
			fileName:      "./testdata/select_temp.csv",
			expected:      `5,6`,
		},
		{
			name: "Can throw error if external table not found",
			setupQuery: `CREATE TABLE sample_table (
				dog UInt32,
				cat UInt32
			) ENGINE=MergeTree ORDER BY dog`,
			insertQuery: `INSERT INTO sample_table VALUES (1, 2), (3, 4), (5, 6)`,
			selectQuery: "SELECT * FROM sample_table WHERE dog IN (SELECT b FROM sharks)",
			externalTable: sdk.NewExternalTable(
				"fish",
				[][]interface{}{
					{uint32(1), uint32(4)},
					{uint32(2), uint32(5)},
					{uint32(3), uint32(6)},
				},
				[]string{"a", "b"},
				[]column.CHColumnType{column.UINT32, column.UINT32},
			),
			teardownQuery: "DROP TABLE IF EXISTS sample_table",
			fmtType:       "CSV",
			expected:      "",
			fileName:      "./testdata/select_temp.csv",
		},
	}

	for i, tt := range tests {
		if i > 0 {
			continue
		}

		t.Run(tt.name, func(b *testing.T) {
			// Remove file if exist
			_ = os.Remove(tt.fileName)

			// Setup table script
			g := openConnection(t, tt.teardownQuery, tt.setupQuery, tt.insertQuery)

			// Open file
			file, err := os.OpenFile(tt.fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			require.NoError(b, err)

			var reader io.Reader
			// Run insert query
			err = RunConn(ctx, g, func(conn sdk.Conn) error {
				qr, err := conn.QueryContextWithExternalTable(ctx, tt.selectQuery, tt.externalTable)
				if err != nil {
					return err
				}

				reader = qr.ExportToReader(tt.fmtType)
				return nil
			})

			require.NoError(b, err)

			_, err = io.Copy(file, reader)
			require.NoError(b, err)
			_ = file.Close()

			out, err := os.ReadFile(tt.fileName)
			require.NoError(b, err)
			require.Equal(b, tt.expected, string(out))
		})
	}
}

func Test_QueryContextWithExternalTableReader(t *testing.T) {
	utils.SkipIntegrationTestIfShort(t)

	tests := []struct {
		name          string
		setupQuery    string
		insertQuery   string
		externalTable *sdk.ExternalTableReader
		check         func(t *testing.T, g *sql.DB)
		teardownQuery string
		selectQuery   string
		outFileName   string
		outFmtType    string
		wantRowCount  int
		expected      string
	}{
		{
			name: "Can select with external table, bytes.Buffer CSV format",
			setupQuery: `CREATE TABLE sample_table (
				dog UInt32,
				cat UInt32
			) ENGINE=MergeTree ORDER BY dog`,
			insertQuery: `INSERT INTO sample_table VALUES (1, 2), (3, 4), (5, 6)`,
			selectQuery: "SELECT * FROM sample_table WHERE dog IN (SELECT a FROM fish)",
			externalTable: sdk.NewExternalTableReader(
				"fish",
				bytes.NewBuffer([]byte("1,2\n2,5\n3,6\n")),
				[]string{"a", "b"},
				[]column.CHColumnType{column.UINT32, column.UINT32},
				"CSV",
			),
			teardownQuery: "DROP TABLE IF EXISTS sample_table",
			outFmtType:    "CSV",
			outFileName:   "./testdata/select_temp.csv",
			expected: `1,2
3,4`,
		},
		{
			name: "Can select with external table, CSV file",
			setupQuery: `CREATE TABLE sample_table (
				dog UInt32,
				cat UInt32
			) ENGINE=MergeTree ORDER BY dog`,
			insertQuery: `INSERT INTO sample_table VALUES (1, 2), (3, 4), (5, 6)`,
			selectQuery: "SELECT * FROM sample_table WHERE dog IN (SELECT a FROM fish)",
			externalTable: sdk.NewExternalTableReader(
				"fish",
				func() io.Reader {
					f, _ := os.Open("./testdata/external_table.csv")
					return f
				}(),
				[]string{"a", "b"},
				[]column.CHColumnType{column.UINT32, column.UINT32},
				"CSV",
			),
			teardownQuery: "DROP TABLE IF EXISTS sample_table",
			outFmtType:    "CSV",
			outFileName:   "./testdata/select_temp.csv",
			expected: `1,2
3,4`,
		},
		{
			name: "Can select with external table, CSVWithNames file",
			setupQuery: `CREATE TABLE sample_table (
				dog UInt32,
				cat UInt32
			) ENGINE=MergeTree ORDER BY dog`,
			insertQuery: `INSERT INTO sample_table VALUES (1, 2), (3, 4), (5, 6)`,
			selectQuery: "SELECT * FROM sample_table WHERE dog IN (SELECT a FROM fish)",
			externalTable: sdk.NewExternalTableReader(
				"fish",
				func() io.Reader {
					f, _ := os.Open("./testdata/external_table_with_names.csv")
					return f
				}(),
				[]string{"a", "b"},
				[]column.CHColumnType{column.UINT32, column.UINT32},
				"CSVWithNames",
			),
			teardownQuery: "DROP TABLE IF EXISTS sample_table",
			outFmtType:    "CSV",
			outFileName:   "./testdata/select_temp.csv",
			expected: `1,2
3,4`,
		},
		{
			name: "Can select with external table, JSON file",
			setupQuery: `CREATE TABLE sample_table (
				dog UInt32,
				cat UInt32
			) ENGINE=MergeTree ORDER BY dog`,
			insertQuery: `INSERT INTO sample_table VALUES (1, 2), (3, 4), (5, 6)`,
			selectQuery: "SELECT * FROM sample_table WHERE dog IN (SELECT a FROM fish)",
			externalTable: sdk.NewExternalTableReader(
				"fish",
				func() io.Reader {
					f, _ := os.Open("./testdata/external_table.json")
					return f
				}(),
				[]string{"a", "b"},
				[]column.CHColumnType{column.UINT32, column.UINT32},
				"JSON",
			),
			teardownQuery: "DROP TABLE IF EXISTS sample_table",
			outFmtType:    "CSV",
			outFileName:   "./testdata/select_temp.csv",
			expected: `1,2
3,4`,
		},
		{
			name: "Can throw error if external table not found",
			setupQuery: `CREATE TABLE sample_table (
				dog UInt32,
				cat UInt32
			) ENGINE=MergeTree ORDER BY dog`,
			insertQuery: `INSERT INTO sample_table VALUES (1, 2), (3, 4), (5, 6)`,
			selectQuery: "SELECT * FROM sample_table WHERE dog IN (SELECT b FROM sharks)",
			externalTable: sdk.NewExternalTableReader(
				"fish",
				func() io.Reader {
					f, _ := os.Open("./testdata/external_table.json")
					return f
				}(),
				[]string{"a", "b"},
				[]column.CHColumnType{column.UINT32, column.UINT32},
				"JSON",
			),
			teardownQuery: "DROP TABLE IF EXISTS sample_table",
			outFmtType:    "CSV",
			expected:      "",
			outFileName:   "./testdata/select_temp.csv",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(b *testing.T) {
			// Remove file if exist
			_ = os.Remove(tt.outFileName)

			// Setup table script
			g := openConnection(t, tt.teardownQuery, tt.setupQuery, tt.insertQuery)

			// Open file
			file, err := os.OpenFile(tt.outFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			require.NoError(b, err)

			var reader io.Reader
			// Run insert query
			err = RunConn(ctx, g, func(conn sdk.Conn) error {
				qr, err := conn.QueryContextWithExternalTableReader(ctx, tt.selectQuery, tt.externalTable)
				if err != nil {
					return err
				}

				reader = qr.ExportToReader(tt.outFmtType)
				return nil
			})

			require.NoError(b, err)

			_, err = io.Copy(file, reader)
			require.NoError(b, err)
			_ = file.Close()

			out, err := os.ReadFile(tt.outFileName)
			require.NoError(b, err)
			require.Equal(b, tt.expected, string(out))
		})
	}
}

func Test_BatchInsert(t *testing.T) {
	utils.SkipIntegrationTestIfShort(t)

	type args struct {
		query string
		args  []interface{}
	}

	tests := []struct {
		name          string
		driver        string
		size          int
		args          args
		setupQuery    string
		teardownQuery string
		selectQuery   string
		testResults   func(t *testing.T, qr *sql.Rows)
		wantErr       bool
	}{
		{
			name: "Can batch insert same type",
			setupQuery: `CREATE TABLE sample_table (
				dog UInt32,
				cat UInt32
			) ENGINE=MergeTree ORDER BY dog`,
			size:          2,
			teardownQuery: "DROP TABLE IF EXISTS sample_table",
			args: args{
				query: "INSERT INTO sample_table VALUES (?, ?), (?, ?)",
				args:  []interface{}{uint32(10), uint32(200), uint32(10), uint32(200)},
			},
			selectQuery: "SELECT * FROM sample_table",
			testResults: func(t *testing.T, qr *sql.Rows) {
				for qr.Next() {
					var v1 uint32
					var v2 uint32
					err := qr.Scan(&v1, &v2)
					require.NoError(t, err)
					require.Equal(t, v1, uint32(10))
					require.Equal(t, v2, uint32(200))
				}
			},
		},
		{
			name: "batch insert fails if number of args wrong",
			setupQuery: `CREATE TABLE sample_table (
				dog UInt32,
				cat UInt32
			) ENGINE=MergeTree ORDER BY dog`,
			size:          2,
			teardownQuery: "DROP TABLE IF EXISTS sample_table",
			args: args{
				query: "INSERT INTO sample_table VALUES (?, ?, ?), (?, ?, ?)",
				args:  []interface{}{uint32(10), uint32(200), uint32(10)},
			},
			selectQuery: "SELECT * FROM sample_table",
			testResults: func(t *testing.T, qr *sql.Rows) {
				for qr.Next() {
					var v1 uint32
					var v2 uint32
					err := qr.Scan(&v1, &v2)
					require.NoError(t, err)
					require.Equal(t, v1, uint32(10))
					require.Equal(t, v2, uint32(200))
				}
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(b *testing.T) {
			c := context.Background()
			// Setup table script
			g := openConnection(t, tt.teardownQuery, tt.setupQuery)
			defer closeConnection(t, g, tt.teardownQuery)

			_, err := g.ExecContext(c, tt.args.query, tt.args.args...)
			if tt.wantErr {
				require.Error(b, err)
				return
			}
			require.NoError(b, err)

			qr, err := g.QueryContext(c, tt.selectQuery)
			tt.testResults(t, qr)
		})
	}
}

func TestBH_InsertLargeRows(t *testing.T) {
	utils.SkipIntegrationTestIfShort(t)

	type args struct {
		query string
		args  []interface{}
	}

	tests := []struct {
		name          string
		driver        string
		size          int
		args          args
		setupQuery    string
		teardownQuery string
		selectQuery   string
	}{
		{
			driver: "bytehouse",
			name:   "Bytehouse insert large rows",
			setupQuery: `CREATE TABLE sample_table (
				dog UInt32,
				cat UInt32
			) ENGINE=MergeTree ORDER BY dog`,
			selectQuery:   "SELECT COUNT(*) FROM sample_table",
			size:          1e+7,
			teardownQuery: "DROP TABLE IF EXISTS sample_table",
			args: args{
				query: "INSERT INTO sample_table VALUES (?, ?)",
				args:  []interface{}{uint32(10), uint32(200)},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(b *testing.T) {
			c := context.Background()
			g := openConnection(b, tt.teardownQuery, tt.setupQuery)
			defer closeConnection(b, g, tt.teardownQuery)

			tx, err := g.Begin()
			if err != nil {
				return
			}
			s, err := tx.PrepareContext(c, tt.args.query)
			if err != nil {
				return
			}
			for i := 0; i < tt.size; i++ {
				_, err = s.ExecContext(c, tt.args.args...)
				if err != nil {
					return
				}
			}
			if err = s.Close(); err != nil {
				return
			}
			if err = tx.Commit(); err != nil {
				return
			}

			r, err := g.Query(tt.selectQuery)
			require.NoError(t, err)
			r.Next()
			var x int
			require.NoError(t, r.Scan(&x))
			require.Equal(t, tt.size, x)
			require.NoError(t, r.Close())
		})
	}
}

// Types tested
// - [ ] bitmap (not tested b/c not supported in community clickhouse server)
// - [x] map
// - [x] array
// - [x] date
// - [x] datetime
// - [x] datetime64
// - [x] decimal
// - [x] low cardinality
// - [x] tuple
// - [x] uuid
// - [x] enum8
// - [x] enum16
// - [x] fixed_string
// - [x] float32
// - [x] float64
// - [x] ipv4
// - [x] ipv6
// - [x] uint8
// - [x] uint16
// - [x] uint32
// - [x] uint64
// - [x] int8
// - [x] int16
// - [x] int32
// - [x] int64
func Test_Prepare(t *testing.T) {
	utils.SkipIntegrationTestIfShort(t)

	type args struct {
		ctx   context.Context
		query string
		args  [][]interface{}
	}
	tests := []struct {
		name           string
		args           args
		setupQuery     string
		teardownQuery  string
		selectQuery    string
		compareFunc    func(t *testing.T, expected, result []interface{})
		rowValues      interface{}
		want           driver.Rows
		wantExecErr    bool
		wantPrepareErr bool
	}{
		{
			name: "Can insert 1 row",
			setupQuery: `CREATE TABLE sample_table (
				Id UInt32,
				Color String
			) ENGINE=MergeTree ORDER BY Id`,
			teardownQuery: "DROP TABLE IF EXISTS sample_table",
			args: args{
				ctx:   context.Background(),
				query: "INSERT INTO sample_table VALUES (?, ?)",
				args:  [][]interface{}{{uint32(1), "red"}},
			},
			compareFunc: func(t *testing.T, expected, result []interface{}) {
				assert.ElementsMatch(t, expected, result)
			},
			rowValues: &struct {
				Id    uint32
				Color string
			}{
				Id:    0,
				Color: "",
			},
			selectQuery: "SELECT * FROM sample_table where Id = 1 and Color = 'red'",
			wantExecErr: false,
		},
		{
			name: "Can insert 2 rows",
			setupQuery: `CREATE TABLE sample_table (
				Id UInt32,
				Color String
			) ENGINE=MergeTree ORDER BY Id`,
			teardownQuery: "DROP TABLE IF EXISTS sample_table",
			compareFunc: func(t *testing.T, expected, result []interface{}) {
				assert.ElementsMatch(t, expected, result)
			},
			args: args{
				ctx:   context.Background(),
				query: "INSERT INTO sample_table VALUES (?, ?)",
				args: [][]interface{}{
					{uint32(1), "red"},
					{uint32(2), "re"},
					{uint32(3), "r"},
					{uint32(4), "redd"},
					{uint32(5), "reddd"},
					{uint32(6), "redddd"},
					{uint32(7), "reddddd"},
				},
			},
			selectQuery: "SELECT * FROM sample_table ORDER BY Id",
			wantExecErr: false,
		},
		{
			name: "Can insert many rows of different common types",
			setupQuery: `CREATE TABLE clickhouse_test_insert_batch (
				int8  Int8,
				int16 Int16,
				int32 Int32,
				int64 Int64,
				uint8  UInt8,
				uint16 UInt16,
				uint32 UInt32,
				uint64 UInt64,
				float32 Float32,
				float64 Float64,
				string  String,
				fString FixedString(2),
				uuid UUID
			) Engine=MergeTree ORDER BY int8`,
			teardownQuery: "DROP TABLE IF EXISTS clickhouse_test_insert_batch;",
			compareFunc: func(t *testing.T, expected, result []interface{}) {
				assert.ElementsMatch(t, expected, result)
			},
			args: args{
				ctx: context.Background(),
				query: `INSERT INTO clickhouse_test_insert_batch (
				int8,
				int16,
				int32,
				int64,
				uint8,
				uint16,
				uint32,
				uint64,
				float32,
				float64,
				string,
				fString,
				uuid
			) VALUES (
				?,
				?,
				?,
				?,
				?,
				?,
				?,
				?,
				?,
				?,
				?,
				?,
				?
			)`,
				args: func() (ret [][]interface{}) {
					for i := 1; i <= 10; i++ {
						ret = append(ret, []interface{}{
							int8(-1 * i), int16(-2 * i), int32(-4 * i), int64(-8 * i), // int
							uint8(1 * i), uint16(2 * i), uint32(4 * i), uint64(8 * i), // uint
							1.32 * float32(i), 1.64 * float64(i), //float
							fmt.Sprintf("string %d ", i), // string
							"RU",                         //fixedstring
							uuid.New(),
						})
					}
					return ret
				}(),
			},
			selectQuery: `SELECT 	
				int8,
				int16,
				int32,
				int64,
				uint8,
				uint16,
				uint32,
				uint64,
				float32,
				float64,
				string,
				fString, 
				uuid
				FROM clickhouse_test_insert_batch ORDER BY int8 DESC`,
			wantExecErr: false,
		},
		{
			name: "Can insert many rows of ip types",
			setupQuery: `
			CREATE TABLE clickhouse_test_insert (
				ipv4 IPv4,
				ipv6 IPv6
			) Engine=MergeTree ORDER BY ipv4
			`,
			teardownQuery: "DROP TABLE IF EXISTS clickhouse_test_insert",
			compareFunc: func(t *testing.T, expected, result []interface{}) {
				assert.Equal(t, len(expected), len(result))
				for i := range expected {
					assert.Equal(t, fmt.Sprint(expected[i]), fmt.Sprint(result[i]))
				}
			},
			args: args{
				ctx: context.Background(),
				query: `INSERT INTO clickhouse_test_insert (
				ipv4,
				ipv6
			) VALUES (
				?,
				?
			)`,
				args: func() (ret [][]interface{}) {
					for i := 1; i <= 10; i++ {
						ret = append(ret, []interface{}{
							net.ParseIP("192.0.2.1"),
							net.ParseIP("::ffff:192.0.2.1"),
						})
					}
					return ret
				}(),
			},
			selectQuery: "SELECT * FROM clickhouse_test_insert",
			wantExecErr: false,
		},
		{
			name: "Can insert many rows of array types",
			setupQuery: `
			CREATE TABLE clickhouse_test_array (
				int8     Array(Int8),
				int16    Array(Int16),
				int32    Array(Int32),
				int64    Array(Int64),
				uint8    Array(UInt8),
				uint16   Array(UInt16),
				uint32   Array(UInt32),
				uint64   Array(UInt64),
				float32  Array(Float32),
				float64  Array(Float64),
				string   Array(String),
				fString  Array(FixedString(2)),
				enum8    Array(Enum8 ('a' = 1, 'b' = 2)),
				enum16   Array(Enum16('c' = 1, 'd' = 2))
			) Engine=Memory
		`,
			teardownQuery: "DROP TABLE IF EXISTS clickhouse_test_array",
			compareFunc: func(t *testing.T, expected, result []interface{}) {
				interfaceSlice := func(slice interface{}) []interface{} {
					if slice == nil {
						return []interface{}{}
					}

					s := reflect.ValueOf(slice)
					if s.Kind() != reflect.Slice {
						t.Errorf("InterfaceSlice() given a non-slice type")
						return nil
					}

					// Keep the distinction between nil and empty slice input
					if s.IsNil() {
						return nil
					}

					ret := make([]interface{}, s.Len())

					for i := 0; i < s.Len(); i++ {
						ret[i] = s.Index(i).Interface()
					}

					return ret
				}

				assert.Equal(t, len(expected), len(result))

				for i, eValues := range expected {
					eValues := interfaceSlice(eValues)
					assert.NotNil(t, eValues)
					rValues, ok := result[i].([]interface{})
					assert.True(t, ok)
					assert.Equal(t, len(eValues), len(rValues))

					for j, ev := range eValues {
						assert.Equal(t, ev, rValues[j])
					}
				}
			},
			args: args{
				ctx: context.Background(),
				query: `
			INSERT INTO clickhouse_test_array (
				int8,
				int16,
				int32,
				int64,
				uint8,
				uint16,
				uint32,
				uint64,
				float32,
				float64,
				string,
				fString,
				enum8,
				enum16
			) VALUES (
				?,
				?,
				?,
				?,
				?,
				?,
				?,
				?,
				?,
				?,
				?,
				?
				?,
				?
			)
		`,
				args: func() (ret [][]interface{}) {
					for i := 1; i <= 10; i++ {
						ret = append(ret,
							[]interface{}{
								nil,
								[]int16{5, 6, 7},
								[]int32{8, 9, 10},
								[]int64{11, 12, 13},
								[]uint8{14, 15, 16},
								[]uint16{17, 18, 19},
								[]uint32{20, 21, 22},
								[]uint64{23, 24, 25},
								[]float32{32.1, 32.2},
								[]float64{64.1, 64.2},
								[]string{fmt.Sprintf("A"), "B", "C"},
								[]string{"RU", "EN", "DE"},
								[]string{"a", "b"},
								[]string{"c", "d"},
							})
					}
					return ret
				}(),
			},
			selectQuery: `SELECT 				
				int8,
				int16,
				int32,
				int64,
				uint8,
				uint16,
				uint32,
				uint64,
				float32,
				float64,
				string,
				fString,
				enum8,
				enum16 FROM clickhouse_test_array`,
			wantExecErr: false,
		},
		{
			name: "Can insert many rows of tuple types",
			setupQuery: `
			CREATE TABLE clickhouse_test_tuple (
				t     Tuple(
						Int8, Int16, Int32, Int64,
						UInt8, UInt16, UInt32, UInt64,
						Float32, Float64, String, FixedString(2),
						Enum8 ('a' = 1, 'b' = 2), Enum16('c' = 1, 'd' = 2)
					)
			) Engine=Memory
		`,
			teardownQuery: "DROP TABLE IF EXISTS clickhouse_test_tuple",
			compareFunc: func(t *testing.T, expected, result []interface{}) {
				interfaceSlice := func(slice interface{}) []interface{} {
					s := reflect.ValueOf(slice)
					if s.Kind() != reflect.Slice {
						t.Errorf("InterfaceSlice() given a non-slice type")
						return nil
					}

					// Keep the distinction between nil and empty slice input
					if s.IsNil() {
						return nil
					}

					ret := make([]interface{}, s.Len())

					for i := 0; i < s.Len(); i++ {
						ret[i] = s.Index(i).Interface()
					}

					return ret
				}

				assert.Equal(t, len(expected), len(result))

				for i, eValues := range expected {
					eValues := interfaceSlice(eValues)
					assert.NotNil(t, eValues)
					rValues, ok := result[i].([]interface{})
					assert.True(t, ok)
					assert.Equal(t, len(eValues), len(rValues))

					for j, ev := range eValues {
						assert.Equal(t, ev, rValues[j])
					}
				}
			},
			args: args{
				ctx: context.Background(),
				query: `
			INSERT INTO clickhouse_test_tuple (
				t
			) VALUES (
				?
			)
		`,
				args: func() (ret [][]interface{}) {
					for i := 1; i <= 10; i++ {
						ret = append(ret,
							[]interface{}{
								[]interface{}{
									int8(1),
									int16(5),
									int32(8),
									int64(11),
									uint8(14),
									uint16(17),
									uint32(20),
									uint64(23),
									float32(32.1),
									float64(64.1),
									"A_5",
									"RU",
									"a",
									"c",
								},
							})
					}
					return ret
				}(),
			},
			selectQuery: `SELECT 				
				* FROM clickhouse_test_tuple`,
			wantExecErr: false,
		},
		{
			name: "Test dates",
			setupQuery: `
			CREATE TABLE clickhouse_test_date (
				date Date,
				datetime DateTime,
				datetime64 DateTime64
			) Engine=Memory
			`,
			teardownQuery: "DROP TABLE IF EXISTS clickhouse_test_date",
			compareFunc: func(t *testing.T, expected, result []interface{}) {
				assert.Equal(t, len(expected), len(result))
				for i := range expected {
					assert.Equal(t, fmt.Sprint(expected[i])[:10], fmt.Sprint(result[i])[:10])
				}
			},
			args: args{
				ctx: context.Background(),
				query: `INSERT INTO clickhouse_test_date (
				date,
				datetime,
				datetime64
			) VALUES (
				?,
				?,
				?
			)`,
				args: func() (ret [][]interface{}) {
					for i := 1; i <= 10; i++ {
						ret = append(ret, []interface{}{
							time.Now(),
							time.Now(),
							time.Now(),
						})
					}
					return ret
				}(),
			},
			selectQuery: "SELECT * FROM clickhouse_test_date",
			wantExecErr: false,
		},
		{
			name: "Test decimals",
			setupQuery: `
			CREATE TABLE clickhouse_test_decimal (
				decimal Decimal(18,5)
			) Engine=Memory
			`,
			teardownQuery: "DROP TABLE IF EXISTS clickhouse_test_decimal",
			compareFunc: func(t *testing.T, expected, result []interface{}) {
				assert.Equal(t, len(expected), len(result))
				for i := range expected {
					assert.Equal(t, fmt.Sprint(expected[i]), fmt.Sprint(result[i]))
				}
			},
			args: args{
				ctx: context.Background(),
				query: `INSERT INTO clickhouse_test_decimal (
				decimal
			) VALUES (
				?
			)`,
				args: func() (ret [][]interface{}) {
					for i := 1; i <= 10; i++ {
						ret = append(ret, []interface{}{
							float64(122),
						})
					}
					return ret
				}(),
			},
			selectQuery: "SELECT * FROM clickhouse_test_decimal",
			wantExecErr: false,
		},
		{
			name: "Test lowCardinality",
			setupQuery: `
			CREATE TABLE clickhouse_test_lowCardinality (
				lowCardinality1 LowCardinality(String),
				lowCardinality2 LowCardinality(String)
			) Engine=Memory
			`,
			teardownQuery: "DROP TABLE IF EXISTS clickhouse_test_lowCardinality",
			compareFunc: func(t *testing.T, expected, result []interface{}) {
				assert.Equal(t, len(expected), len(result))
				for i := range expected {
					assert.Equal(t, expected[i], result[i])
				}
			},
			args: args{
				ctx: context.Background(),
				query: `INSERT INTO clickhouse_test_lowCardinality (
				lowCardinality1,
				lowCardinality2
			) VALUES (
				?,
				?
			)`,
				args: func() (ret [][]interface{}) {
					for i := 1; i <= 10; i++ {
						ret = append(ret, []interface{}{
							"fewfweewf", "fewfwe",
						})
					}
					return ret
				}(),
			},
			selectQuery: "SELECT * FROM clickhouse_test_lowCardinality",
			wantExecErr: false,
		},
		{
			name: "Test lowCardinality large 1",
			setupQuery: `
			CREATE TABLE clickhouse_test_lowCardinality (
				lowCardinality1 LowCardinality(String)
			) Engine=Memory
			`,
			teardownQuery: "DROP TABLE IF EXISTS clickhouse_test_lowCardinality",
			compareFunc: func(t *testing.T, expected, result []interface{}) {
				assert.Equal(t, len(expected), len(result))
				for i := range expected {
					assert.Equal(t, expected[i], result[i])
				}
			},
			args: args{
				ctx: context.Background(),
				query: `INSERT INTO clickhouse_test_lowCardinality (
				lowCardinality1
			) VALUES (
				?
			)`,
				args: func() (ret [][]interface{}) {
					ret = make([][]interface{}, math.MaxUint8+1)
					for i := range ret {
						ret[i] = []interface{}{fmt.Sprint(i)}
					}
					return ret
				}(),
			},
			selectQuery: "SELECT * FROM clickhouse_test_lowCardinality",
			wantExecErr: false,
		},
		{
			name: "Test lowCardinality large 2",
			setupQuery: `
			CREATE TABLE clickhouse_test_lowCardinality (
				lowCardinality1 LowCardinality(String)
			) Engine=Memory
			`,
			teardownQuery: "DROP TABLE IF EXISTS clickhouse_test_lowCardinality",
			compareFunc: func(t *testing.T, expected, result []interface{}) {
				assert.Equal(t, len(expected), len(result))
				for i := range expected {
					assert.Equal(t, expected[i], result[i])
				}
			},
			args: args{
				ctx: context.Background(),
				query: `INSERT INTO clickhouse_test_lowCardinality (
				lowCardinality1
			) VALUES (
				?
			)`,
				args: func() (ret [][]interface{}) {
					ret = make([][]interface{}, math.MaxUint16+1)
					for i := range ret {
						ret[i] = []interface{}{fmt.Sprint(i)}
					}
					return ret
				}(),
			},
			selectQuery: "SELECT * FROM clickhouse_test_lowCardinality",
			wantExecErr: false,
		},
		{
			name: "Test map",
			setupQuery: `
			CREATE TABLE clickhouse_test_map (
				map1 Map(String, Int8),
				map2 Map(String, Int8)
			) Engine=Memory
			`,
			teardownQuery: "DROP TABLE IF EXISTS clickhouse_test_map",
			compareFunc: func(t *testing.T, expected, result []interface{}) {
				assert.Equal(t, len(expected), len(result))
				for i := range expected {
					assert.Equal(t, expected[i], result[i])
				}
			},
			args: args{
				ctx: context.Background(),
				query: `INSERT INTO clickhouse_test_map (
				map1,
				map2
			) VALUES (
				?,
				?
			)`,
				args: func() (ret [][]interface{}) {
					for i := 1; i <= 10; i++ {
						ret = append(ret, []interface{}{
							map[string]int8{
								"baba": int8(1),
								"mama": int8(2),
							},
							map[string]int8{
								"gege":   int8(1),
								"meimei": int8(2),
							},
						})
					}
					return ret
				}(),
			},
			selectQuery: "SELECT * FROM clickhouse_test_map",
			wantExecErr: false,
		},
		{
			name: "Should throw error if insert arguments less than expected",
			setupQuery: `
			CREATE TABLE clickhouse_test_lowCardinality (
				lowCardinality1 LowCardinality(String),
				lowCardinality2 LowCardinality(String)
			) Engine=Memory
			`,
			teardownQuery: "DROP TABLE IF EXISTS clickhouse_test_lowCardinality",
			compareFunc: func(t *testing.T, expected, result []interface{}) {
				assert.Equal(t, len(expected), len(result))
				for i := range expected {
					assert.Equal(t, expected[i], result[i])
				}
			},
			args: args{
				ctx: context.Background(),
				query: `INSERT INTO clickhouse_test_lowCardinality (
				lowCardinality1,
				lowCardinality2
			) VALUES (
				?,
				?
			)`,
				args: func() (ret [][]interface{}) {
					for i := 1; i <= 10; i++ {
						ret = append(ret, []interface{}{
							"fewfweewf",
						})
					}
					return ret
				}(),
			},
			selectQuery: "SELECT * FROM clickhouse_test_lowCardinality",
			wantExecErr: true,
		},
		{
			name: "Should throw error if number of insert arguments not multiple of number of columns",
			setupQuery: `
			CREATE TABLE clickhouse_test_lowCardinality (
				lowCardinality1 LowCardinality(String),
				lowCardinality2 LowCardinality(String)
			) Engine=Memory
			`,
			teardownQuery: "DROP TABLE IF EXISTS clickhouse_test_lowCardinality",
			compareFunc: func(t *testing.T, expected, result []interface{}) {
				assert.Equal(t, len(expected), len(result))
				for i := range expected {
					assert.Equal(t, expected[i], result[i])
				}
			},
			args: args{
				ctx: context.Background(),
				query: `INSERT INTO clickhouse_test_lowCardinality (
				lowCardinality1,
				lowCardinality2
			) VALUES (
				?,
				?
			)`,
				args: func() (ret [][]interface{}) {
					for i := 1; i <= 10; i++ {
						ret = append(ret, []interface{}{
							"fewfweewf", "fewfweewf", "fewfweewf", "fewfweewf",
							"fewfweewf", "fewfweewf", "fewfweewf",
						})
					}
					return ret
				}(),
			},
			selectQuery: "SELECT * FROM clickhouse_test_lowCardinality",
			wantExecErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name+" fast", func(t *testing.T) {
			g := openConnection(t, tt.teardownQuery, tt.setupQuery)
			defer closeConnection(t, g, tt.teardownQuery)

			err := RunConn(tt.args.ctx, g, func(conn sdk.Conn) error {
				stmt, err := conn.PrepareContext(tt.args.ctx, tt.args.query)
				if tt.wantPrepareErr {
					require.Error(t, err)
					return err
				}
				if err != nil {
					return err
				}
				defer stmt.Close()

				for _, arg := range tt.args.args {
					err = stmt.ExecContext(tt.args.ctx, arg...)
					if tt.wantExecErr {
						require.Error(t, err)
						return err
					}
					if err != nil {
						return err
					}
				}

				return err
			})

			if tt.wantExecErr || tt.wantPrepareErr {
				return
			}

			require.NoError(t, err)
			r, err := g.QueryContext(tt.args.ctx, tt.selectQuery)
			require.NoError(t, err)

			res := make([]interface{}, len(tt.args.args[0]))
			resPtrs := make([]interface{}, len(tt.args.args[0]))
			for i := range res {
				resPtrs[i] = &res[i]
			}

			i := 0
			for r.Next() {
				err := r.Scan(resPtrs...)
				require.NoError(t, err)
				tt.compareFunc(t, tt.args.args[i], res)
				i++
			}

			require.Equal(t, len(tt.args.args), i)
			err = r.Close()
			require.NoError(t, err)
		})

		t.Run(tt.name+" slow", func(t *testing.T) {
			// Setup table script
			g := openConnection(t, tt.teardownQuery, tt.setupQuery)
			defer closeConnection(t, g, tt.teardownQuery)

			tx, err := g.Begin()
			require.NoError(t, err)

			st, err := tx.PrepareContext(tt.args.ctx, tt.args.query)
			if tt.wantPrepareErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			for _, arg := range tt.args.args {
				_, err = st.ExecContext(tt.args.ctx, arg...)
				if tt.wantExecErr {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
			}
			err = st.Close()
			if tt.wantExecErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			err = tx.Commit()
			require.NoError(t, err)

			r, err := g.QueryContext(tt.args.ctx, tt.selectQuery)
			require.NoError(t, err)

			res := make([]interface{}, len(tt.args.args[0]))
			resPtrs := make([]interface{}, len(tt.args.args[0]))
			for i := range res {
				resPtrs[i] = &res[i]
			}

			i := 0
			for r.Next() {
				err := r.Scan(resPtrs...)
				require.NoError(t, err)
				tt.compareFunc(t, tt.args.args[i], res)
				i++
			}

			require.Equal(t, len(tt.args.args), i)

			err = r.Close()
			require.NoError(t, err)
		})
	}
}

func TestNullableColumnTypesGet(t *testing.T) {
	utils.SkipIntegrationTestIfShort(t)

	db, err := sql.Open("bytehouse", "tcp://localhost:9000")
	assert.NoError(t, err)
	rows, err := db.QueryContext(context.Background(), "SELECT toNullable(toUInt64(0))")
	assert.NoError(t, err)
	_, err = rows.ColumnTypes()
	assert.NoError(t, err)
}
