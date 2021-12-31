package sdk

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"reflect"
	"regexp"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	bytehouse "github.com/bytehouse-cloud/driver-go"
	"github.com/bytehouse-cloud/driver-go/driver/lib/settings"
	"github.com/bytehouse-cloud/driver-go/utils"
)

// -- INTEGRATION TESTS -- //
func getConfig(t *testing.T) *Config {
	config, err := ParseDSN("tcp://localhost:9000?user=default", nil, nil)
	if err != nil {
		t.Fatalf("[setup] Failed to parse config, error = %v", err)
		return nil
	}
	return config
}

func TestSDKSimple(t *testing.T) {
	utils.SkipIntegrationTestIfShort(t)

	gateway := OpenConfig(getConfig(t))

	_, err := gateway.Query("set send_logs_level = 'trace'")
	result, err := gateway.Query("select * from numbers(2) as x, numbers(2) as y")
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}

	for {
		row, ok := result.NextRow()
		if !ok {
			break
		}
		fmt.Println(row)
	}

	serverMetas := result.GetAllMeta()
	require.True(t, len(serverMetas) > 0)
	logs := result.GetAllLogs()
	require.True(t, len(logs) > 0)
}

func TestSDKInsert(t *testing.T) {
	utils.SkipIntegrationTestIfShort(t)

	gateway := OpenConfig(getConfig(t))

	_, err := gateway.Query("create database if not exists zx_test")
	_, err = gateway.Query("create table if not exists zx_test.number (f Int64) Engine = Log")
	_, err = gateway.Query("truncate table zx_test.number")

	result, err := gateway.Query("insert into zx_test.number Values (4564)")

	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}

	for {
		row, ok := result.NextRow()
		if !ok {
			break
		}
		fmt.Println(row)
	}

	result2, err := gateway.Query("select * from zx_test.number")

	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}

	for {
		row, ok := result2.NextRow()
		if !ok {
			break
		}
		fmt.Println(row)
	}
}

func TestSDKCancel(t *testing.T) {
	utils.SkipIntegrationTestIfShort(t)

	gateway := OpenConfig(getConfig(t))

	_, err := gateway.Query("set send_logs_level = 'trace'")
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second))
	defer cancel()
	result, err := gateway.QueryContext(ctx, "select * from system.numbers limit 1000000000")
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}

	cntRow := 0
	for {
		_, ok := result.NextRow()
		if !ok {
			break
		}
		cntRow++
	}

	require.NotEqual(t, cntRow, 1000000000)
}

func TestSDKQueryWithSetting(t *testing.T) {
	utils.SkipIntegrationTestIfShort(t)

	gateway := OpenConfig(getConfig(t))

	ctx := bytehouse.NewQueryContext(context.Background())
	err := ctx.AddQuerySetting("send_logs_level", "trace")
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	result, err := gateway.QueryContext(ctx, "select * from numbers(2) as x, numbers(2) as y")
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}

	for {
		row, ok := result.NextRow()
		if !ok {
			break
		}
		fmt.Println(row)
	}
}

func TestQueryResult_ExportToReader(t *testing.T) {
	utils.SkipIntegrationTestIfShort(t)

	gateway := OpenConfig(getConfig(t))
	if err := gateway.Ping(); err != nil {
		fmt.Println(err)
		t.FailNow()
	}

	result, err := gateway.Query("select * from numbers(3) as x, numbers(4) as y")
	require.Nil(t, err)

	r := result.ExportToReader("csv")

	bs, err := ioutil.ReadAll(r)
	require.Nil(t, err)

	expected := `0,0
0,1
0,2
0,3
1,0
1,1
1,2
1,3
2,0
2,1
2,2
2,3`

	require.Equal(t, expected, string(bs))
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
func TestGateway_Insert(t *testing.T) {
	utils.SkipIntegrationTestIfShort(t)

	type args struct {
		ctx         context.Context
		setupScript string
		query       string
		args        [][]interface{}
	}
	tests := []struct {
		name          string
		args          args
		setupQuery    string
		teardownQuery string
		selectQuery   string
		compareFunc   func(t *testing.T, expected, result []interface{})
		want          *QueryResult
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
				require.ElementsMatch(t, expected, result)
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
				require.ElementsMatch(t, expected, result)
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
			selectQuery: "SELECT * FROM sample_table",
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
				require.ElementsMatch(t, expected, result)
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
				require.Equal(t, len(expected), len(result))
				for i := range expected {
					require.Equal(t, fmt.Sprint(expected[i]), fmt.Sprint(result[i]))
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

				require.Equal(t, len(expected), len(result))

				for i, eValues := range expected {
					eValues := interfaceSlice(eValues)
					require.NotNil(t, eValues)
					rValues, ok := result[i].([]interface{})
					require.True(t, ok)
					require.Equal(t, len(eValues), len(rValues))

					for j, ev := range eValues {
						require.Equal(t, ev, rValues[j])
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
								[]string{fmt.Sprintf("A_%d", i), "B", "C"},
								[]string{"RU", "EN", "DE"},
								[]string{"a", "b"},
								[]string{"c", "d"},
							})

						ret = append(ret,
							[]interface{}{
								[]int8{100, 101, 102, 103, 104, 105},
								[]int16{200, 201},
								[]int32{300, 301, 302, 303},
								[]int64{400, 401, 402},
								[]uint8{250, 251, 252, 253, 254},
								[]uint16{1000, 1001, 1002, 1003, 1004},
								[]uint32{2001, 2002},
								[]uint64{3000},
								[]float32{1000.1, 100.1, 2000},
								[]float64{640, 8, 650.9, 703.5, 800},
								[]string{fmt.Sprintf("D_%d", i), "E", "F", "G"},
								[]string{"UA", "GB"},
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

				require.Equal(t, len(expected), len(result))

				for i, eValues := range expected {
					eValues := interfaceSlice(eValues)
					require.NotNil(t, eValues)
					rValues, ok := result[i].([]interface{})
					require.True(t, ok)
					require.Equal(t, len(eValues), len(rValues))

					for j, ev := range eValues {
						require.Equal(t, ev, rValues[j])
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
									fmt.Sprintf("A_%d", i),
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
				require.Equal(t, len(expected), len(result))
				for i := range expected {
					require.Equal(t, fmt.Sprint(expected[i])[:10], fmt.Sprint(result[i])[:10])
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
				require.Equal(t, len(expected), len(result))
				for i := range expected {
					require.Equal(t, fmt.Sprint(expected[i]), fmt.Sprint(result[i]))
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
				require.Equal(t, len(expected), len(result))
				for i := range expected {
					require.Equal(t, expected[i], result[i])
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
			name: "Test map",
			setupQuery: `
			CREATE TABLE clickhouse_test_map (
				map1 Map(String, Int8),
				map2 Map(String, Int8)
			) Engine=Memory
			`,
			teardownQuery: "DROP TABLE IF EXISTS clickhouse_test_map",
			compareFunc: func(t *testing.T, expected, result []interface{}) {
				require.Equal(t, len(expected), len(result))
				for i := range expected {
					require.Equal(t, expected[i], result[i])
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
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup table script
			g := OpenConfig(getConfig(t))
			require.NotNil(t, g)

			_, err := g.QueryContext(tt.args.ctx, utils.AllowMapSQLScript)
			require.NoError(t, err)

			err = g.Ping()
			require.NoError(t, err)
			defer g.Close()

			defer func() {
				qr, err := g.Query(tt.teardownQuery)
				requireQrNoError(t, qr, err)
				_ = qr.Close()
			}()

			qr, err := g.Query(tt.teardownQuery)
			requireQrNoError(t, qr, err)
			_ = qr.Close()

			qr, err = g.Query(tt.setupQuery)
			requireQrNoError(t, qr, err)
			_ = qr.Close()

			err = g.InsertTable(tt.args.ctx, tt.args.query, tt.args.args, settings.DEFAULT_BLOCK_SIZE)
			if !tt.wantErr {
				require.NoError(t, err)
			}

			qr, err = g.QueryContext(tt.args.ctx, tt.selectQuery)
			requireQrNoError(t, qr, err)
			i := 0
			for {
				rowValues, ok := qr.NextRow()
				if !ok {
					break
				}
				tt.compareFunc(t, tt.args.args[i], rowValues)
				i++
			}
			_ = qr.Close()

			if !tt.wantErr {
				require.Equal(t, len(tt.args.args), i)
			}
		})
	}
}

func TestGateway_InsertWithDataReader(t *testing.T) {
	utils.SkipIntegrationTestIfShort(t)

	type args struct {
		ctx   context.Context
		query string
	}
	tests := []struct {
		name          string
		args          args
		setupQuery    string
		teardownQuery string
		selectQuery   string
		insertValues  string
		compareFunc   func(t *testing.T, expected string, result []interface{})
		want          *QueryResult
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
				query: "INSERT INTO sample_table VALUES ",
			},
			insertValues: "(1,red), (2,blue), (3,koo)",
			compareFunc: func(t *testing.T, expected string, result []interface{}) {
				out := ""
				for i, v := range result {
					out += fmt.Sprint(v)
					if i != len(result)-1 {
						out += ","
					}
				}

				require.Equal(t, expected, "("+out+")")
			},
			selectQuery: "SELECT * FROM sample_table",
			wantErr:     false,
		},
		{
			name: "Can throw ioErr if wrong array format",
			setupQuery: `CREATE TABLE clickhouse_test_insert_batch (
						int8 Array(Int8),
						uint8  Array(UInt8),
						uint16  Array(UInt16),
						uint32  Array(UInt32),
						uint64  Array(UInt64)
					) Engine=MergeTree ORDER BY uint8`,
			teardownQuery: "DROP TABLE IF EXISTS clickhouse_test_insert_batch;",
			args: args{
				ctx: context.Background(),
				query: `INSERT INTO clickhouse_test_insert_batch (
						int8,
						uint8,
						uint16,
						uint32,
						uint64
					) VALUES`,
			},
			insertValues: "(a,[1,2,3,4],,,)",
			selectQuery: `SELECT
						int8,
						uint8,
						uint16,
						uint32,
						uint64
						FROM clickhouse_test_insert_batch`,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup table script
			g := OpenConfig(getConfig(t))

			qr, err := g.Query(tt.teardownQuery)
			require.NoError(t, err)
			require.NoError(t, qr.Exception())

			qr, err = g.Query(tt.setupQuery)
			require.NoError(t, err)
			require.NoError(t, qr.Exception())

			qr, err = g.InsertWithDataFormatAuto(tt.args.ctx, tt.args.query, bytes.NewReader([]byte(tt.insertValues)))
			require.NotNil(t, qr)
			if err != nil || qr.Exception() != nil {
				if tt.wantErr {
					if err != nil {
						require.Error(t, err)
					} else {
						require.Error(t, qr.Exception(), "Should either have ioErr or query result exception")
					}
					return
				}

				t.Errorf("error = %v, query result exception = %v", err, qr.Exception())
				return
			}

			defer func() {
				qr, err := g.Query(tt.teardownQuery)
				require.NoError(t, err)
				require.NoError(t, qr.Exception())
			}()

			qr, err = g.QueryContext(tt.args.ctx, tt.selectQuery)
			require.NoError(t, err)
			splitBracketRe := regexp.MustCompile(`\(.*?\)`)

			iv := splitBracketRe.FindAllString(tt.insertValues, -1)
			i := 0
			for {
				rowValues, ok := qr.NextRow()
				if !ok {
					break
				}
				tt.compareFunc(t, iv[i], rowValues)
				i++
			}
		})
	}
}

func TestGateway_Query(t *testing.T) {
	utils.SkipIntegrationTestIfShort(t)

	type args struct {
		query string
	}
	tests := []struct {
		name          string
		setupQuery    string
		teardownQuery string
		args          args
		want          *QueryResult
		wantErr       bool
	}{
		{
			name:          "Should not throw ioErr if empty query result",
			setupQuery:    "CREATE TABLE m ( x Int ) Engine=Memory",
			teardownQuery: "drop table if exists m",
			args: args{
				query: "select * from m",
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "Should not throw ioErr if empty query result",
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
			args: args{
				query: `SELECT 				
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
			},
			want:    nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gateway := OpenConfig(getConfig(t))
			require.NotNil(t, gateway)

			err := gateway.Ping()
			require.NoError(t, err)
			defer gateway.Close()

			qr, err := gateway.Query(tt.teardownQuery)
			requireQrNoError(t, qr, err)
			_ = qr.Close()

			qr, err = gateway.Query(tt.setupQuery)
			requireQrNoError(t, qr, err)
			_ = qr.Close()

			defer func() {
				qr, err := gateway.Query(tt.teardownQuery)
				requireQrNoError(t, qr, err)
				_ = qr.Close()
			}()

			qr, err = gateway.Query(tt.args.query)
			requireQrNoError(t, qr, err)
			_ = qr.Close()
		})
	}
}

func requireQrNoError(t *testing.T, qr *QueryResult, err error) {
	require.NoError(t, err)
	require.NotNil(t, qr)
	require.NoError(t, qr.Exception())
}
