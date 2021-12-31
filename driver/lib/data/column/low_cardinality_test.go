package column

import (
	"bytes"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

func TestLowCardinalityColumnData_ReadFromTexts(t *testing.T) {
	type args struct {
		texts []string
	}
	tests := []struct {
		name            string
		columnType      CHColumnType
		args            args
		wantDataWritten []string
		wantRowsRead    int
		wantErr         bool
	}{
		{
			name:       "Should write data and return number of rows read with no error, 0 rows",
			columnType: "LowCardinality(String)",
			args: args{
				texts: []string{},
			},
			wantRowsRead: 0,
			wantErr:      false,
		},
		{
			name:       "Should write data and return number of rows read with no error, 2 rows",
			columnType: "LowCardinality(String)",
			args: args{
				texts: []string{"hello", "good morning", "good morning", "good morning"},
			},
			wantDataWritten: []string{"hello", "good morning", "good morning", "good morning"},
			wantRowsRead:    4,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error, 2 rows",
			columnType: "LowCardinality(String)",
			args: args{
				texts: []string{"hello", "father", "mother", "daughter", "son", "k", "w", "x"},
			},
			wantDataWritten: []string{"hello", "father", "mother", "daughter", "son", "k", "w", "x"},
			wantRowsRead:    8,
			wantErr:         false,
		},
		{
			name:       "Should write empty string and return number of rows read with no error",
			columnType: "LowCardinality(Int64)",
			args: args{
				texts: []string{"1", "", "3"},
			},
			wantDataWritten: []string{"1", "0", "3"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error, 2 rows",
			columnType: "LowCardinality(String)",
			args: args{
				texts: []string{"hello"},
			},
			wantDataWritten: []string{"hello"},
			wantRowsRead:    1,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error",
			columnType: "LowCardinality(Date)",
			args: args{
				texts: []string{"1970-01-02", "2020-01-02"},
			},
			wantDataWritten: []string{"1970-01-02", "2020-01-02"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:       "Should throw error if invalid time format",
			columnType: "LowCardinality(Date)",
			args: args{
				texts: []string{"1970-01-02", "2020-01-02pp"},
			},
			wantRowsRead:    1,
			wantDataWritten: []string{"1970-01-01"},
			wantErr:         true,
		},
		// Disable tests for wrapped complex types since not supported
		// See https://clickhouse.tech/docs/en/sql-reference/data-types/lowcardinality/ for supported types
		//{
		//	name:       "Should write data and return number of rows read with no error, 2 rows",
		//	columnType: "LowCardinality(Map(Int8, String))",
		//	args: args{
		//		texts: []string{"{1: 2}", "{1    : 2}", "{1: 2}", "{1: 2}"},
		//	},
		//	wantDataWritten: []string{"{1: 2}", "{1: 2}", "{1: 2}", "{1: 2}"},
		//	wantRowsRead:    4,
		//	wantErr:         false,
		//},
		//{
		//	name:       "Should write data and return number of rows read with no error, 3 rows",
		//	columnType: "LowCardinality(Array(UInt8))",
		//	args: args{
		//		texts: []string{"[1, 1]", "[1, 3, 3,44,3]", "[1, 33, 33, 3]"},
		//	},
		//	wantDataWritten: []string{"[1, 1]", "[1, 3, 3, 44, 3]", "[1, 33, 33, 3]"},
		//	wantRowsRead:    3,
		//	wantErr:         false,
		//},
		//{
		//	name:       "Should write data and return number of rows read with no error, nested arrays",
		//	columnType: "LowCardinality(Array(LowCardinality(Array(UInt8))))",
		//	args: args{
		//		texts: []string{"[[1, 1], [1, 3, 3,4]]", "[[1, 1], [1, 3, 3,4]]", "[[1, 1], [1, 3, 3,4]]"},
		//	},
		//	wantDataWritten: []string{"[[1, 1], [1, 3, 3, 4]]", "[[1, 1], [1, 3, 3, 4]]", "[[1, 1], [1, 3, 3, 4]]"},
		//	wantRowsRead:    3,
		//	wantErr:         false,
		//},
		//{
		//	name:       "Should throw error if invalid value, 2 rows",
		//	columnType: "LowCardinality(Array(UInt8))",
		//	args: args{
		//		texts: []string{"[1, mamamia]", "[-1, 'lalaland']"},
		//	},
		//	wantDataWritten: []string{"[]"},
		//	wantRowsRead:    0,
		//	wantErr:         true,
		//},
		//{
		//	name:       "Should return empty array",
		//	columnType: "LowCardinality(Array(UInt8))",
		//	args: args{
		//		texts: []string{"[]"},
		//	},
		//	wantDataWritten: []string{"[]"},
		//	wantRowsRead:    1,
		//	wantErr:         false,
		//},
		//{
		//	name:       "Should return empty array",
		//	columnType: "LowCardinality(Array(UInt8))",
		//	args: args{
		//		texts: []string{"[  ]"},
		//	},
		//	wantDataWritten: []string{"[]"},
		//	wantRowsRead:    1,
		//	wantErr:         false,
		//},
		//{
		//	name:       "Should return empty array and some other array",
		//	columnType: "LowCardinality(Array(UInt8))",
		//	args: args{
		//		texts: []string{"[]", "[1, 2]", "[]"},
		//	},
		//	wantDataWritten: []string{"[]", "[1, 2]", "[]"},
		//	wantRowsRead:    3,
		//	wantErr:         false,
		//},
		//{
		//	name:       "Should throw error if invalid value, 2 rows",
		//	columnType: "LowCardinality(Array(UInt8))",
		//	args: args{
		//		texts: []string{"[a]"},
		//	},
		//	wantDataWritten: []string{"[]"},
		//	wantRowsRead:    0,
		//	wantErr:         true,
		//},
		//{
		//	name:       "Should throw error if invalid value, 2 rows",
		//	columnType: "LowCardinality(Array(UInt8))",
		//	args: args{
		//		texts: []string{"[1, 1]", "[-1, 'lalaland']", "[-1, 'lalaland']"},
		//	},
		//	wantDataWritten: []string{"[]"},
		//	wantRowsRead:    1,
		//	wantErr:         true,
		//},
		//{
		//	name:       "Should throw error if invalid value, 2 rows",
		//	columnType: "LowCardinality(Array(UInt8))",
		//	args: args{
		//		texts: []string{"[1,2,3,4,5]", "[1, aa,3]", "[1,2]"},
		//	},
		//	wantDataWritten: []string{"[]"},
		//	wantRowsRead:    1,
		//	wantErr:         true,
		//},
		//{
		//	name:       "Should throw error if invalid value, 2 rows",
		//	columnType: "LowCardinality(Array(UInt8))",
		//	args: args{
		//		texts: []string{"[1, 1]", "[1, 1]", "[1, p]"},
		//	},
		//	wantDataWritten: []string{"[]"},
		//	wantRowsRead:    2,
		//	wantErr:         true,
		//},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := MustMakeColumnData(tt.columnType, len(tt.args.texts))

			got, err := i.ReadFromTexts(tt.args.texts)

			if (err != nil) != tt.wantErr {
				t.Errorf("ReadFromTexts() error = %v, wantErr = %v", err, tt.wantErr)
				return
			}

			assert.Equal(t, tt.wantRowsRead, got)

			for index, value := range tt.wantDataWritten {
				assert.Equal(t, value, i.GetString(index))
			}
		})
	}
}

func TestLowCardinalityColumnData_ReadFromValues(t *testing.T) {
	someTime := time.Now()
	type args struct {
		values []interface{}
	}
	tests := []struct {
		name            string
		args            args
		columnType      CHColumnType
		wantDataWritten []string
		wantRowsRead    int
		wantEqualValues bool // check if values written are same as args (set true only for non-nested and defined types, since can reliably assert nested types)
		wantErr         bool
	}{
		{
			name:       "Should return the same strings",
			columnType: "LowCardinality(String)",
			args: args{
				values: []interface{}{
					"fewfweewf", "fewfwe",
				},
			},
			wantDataWritten: []string{"fewfweewf", "fewfwe"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:       "Should throw error if one of the values is not string",
			columnType: "LowCardinality(String)",
			args: args{
				values: []interface{}{
					"poooo", 123,
				},
			},
			//wantDataWritten: []string{"poooo"},
			wantRowsRead: 1,
			wantErr:      true,
		},
		{
			name:       "Should return the same time value",
			columnType: "LowCardinality(Date)",
			args: args{
				values: []interface{}{
					someTime, someTime,
				},
			},
			wantDataWritten: []string{someTime.String()[:10], someTime.String()[:10]},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:       "Should throw error if one of the values is not time.Time",
			columnType: "LowCardinality(Date)",
			args: args{
				values: []interface{}{
					someTime, 123,
				},
			},
			wantDataWritten: []string{"1970-01-01"},
			wantRowsRead:    1,
			wantErr:         true,
		},
		{
			name:       "Should throw error if nil",
			columnType: "LowCardinality(Date)",
			args: args{
				values: []interface{}{
					nil,
				},
			},
			wantDataWritten: []string{},
			wantRowsRead:    0,
			wantErr:         true,
		},
		// Disable tests for wrapped complex types since not supported
		// See https://clickhouse.tech/docs/en/sql-reference/data-types/lowcardinality/ for supported types
		//
		//{
		//	name:       "Should write data and return number of rows read with no error",
		//	columnType: "LowCardinality(Map(UInt64, UInt64))",
		//	args: args{
		//		values: []interface{}{map[uint64]uint64{1: 3}},
		//	},
		//	wantEqualValues: true,
		//	wantDataWritten: []string{"{1: 3}"},
		//	wantRowsRead:    1,
		//	wantErr:         false,
		//},
		//{
		//	name:       "Should write data and return number of rows read with no error",
		//	columnType: "LowCardinality(Map(UInt64, UInt64))",
		//	args: args{
		//		values: []interface{}{map[interface{}]interface{}{uint8(1): uint32(3)}, map[interface{}]interface{}{uint8(1): uint32(3)}},
		//	},
		//	wantDataWritten: []string{"{1: 3}", "{1: 3}"},
		//	wantRowsRead:    2,
		//	wantErr:         false,
		//},
		//{
		//	name:       "Should write data and return number of rows read with no error if empty",
		//	columnType: "LowCardinality(Map(UInt8, UInt32))",
		//	args: args{
		//		values: []interface{}{map[uint8]uint32{}},
		//	},
		//	wantDataWritten: []string{"{}"},
		//	wantEqualValues: true,
		//	wantRowsRead:    1,
		//	wantErr:         false,
		//},
		//{
		//	name:       "Should return empty map and some other map",
		//	columnType: "LowCardinality(Map(UInt8, UInt8))",
		//	args: args{
		//		values: []interface{}{map[uint8]uint8{}, map[uint8]uint8{1: 2}, map[uint8]uint8{}},
		//	},
		//	wantDataWritten: []string{"{}", "{1: 2}", "{}"},
		//	wantRowsRead:    3,
		//	wantErr:         false,
		//},
		//{
		//	name:       "Should write data and return number of rows read with no error, nested maps",
		//	columnType: "LowCardinality(Map(UInt8,Map(UInt8, UInt8)))",
		//	args: args{
		//		values: []interface{}{map[uint8]map[uint8]uint8{10: {1: 2}, 5: {3: 4}}, map[uint8]map[uint8]uint8{10: {1: 2}, 5: {3: 4}}},
		//	},
		//	wantDataWritten: []string{}, // don't test this, since maps are unordered, can reliably check if same
		//	wantEqualValues: true,       // test values equality
		//	wantRowsRead:    2,
		//	wantErr:         false,
		//},
		//{
		//	name:       "Should write data and return number of rows read with no error, struct as key",
		//	columnType: "LowCardinality(Map(Map(String, UInt8), UInt8))",
		//	args: args{
		//		values: []interface{}{map[DemoType]uint8{DemoType{Key: 2}: 8}, map[DemoType]uint8{DemoType{Key: 2}: 8}},
		//	},
		//	wantDataWritten: []string{"{{Key: 2}: 8}", "{{Key: 2}: 8}"},
		//	wantRowsRead:    2,
		//	wantErr:         false,
		//},
		//{
		//	name:       "Should convert struct with struct tag, struct as key",
		//	columnType: "LowCardinality(Map(Map(String, UInt8), UInt8))",
		//	args: args{
		//		values: []interface{}{map[DemoTypeWithTag]uint8{DemoTypeWithTag{Key: 2}: 8}, map[DemoTypeWithTag]uint8{DemoTypeWithTag{Key: 2}: 8}},
		//	},
		//	wantDataWritten: []string{"{{key: 2}: 8}", "{{key: 2}: 8}"},
		//	wantRowsRead:    2,
		//	wantErr:         false,
		//},
		//{
		//	name:       "Should throw error, no panic, if struct is unexported",
		//	columnType: "LowCardinality(Map(Map(String, UInt8), UInt8))",
		//	args: args{
		//		values: []interface{}{struct{ name string }{name: "jack"}},
		//	},
		//	wantDataWritten: []string{"{}"},
		//	wantRowsRead:    0,
		//	wantErr:         true,
		//},
		//{
		//	name:       "Should throw error if invalid value, 2 rows",
		//	columnType: "LowCardinality(Map(UInt8, UInt8))",
		//	args: args{
		//		values: []interface{}{map[uint8]interface{}{1: "koo"}, map[uint8]interface{}{1: uint8(1)}},
		//	},
		//	wantDataWritten: []string{"{}"},
		//	wantRowsRead:    0,
		//	wantErr:         true,
		//},
		//{
		//	name:       "Should throw error if invalid value, 2 rows",
		//	columnType: "LowCardinality(Map(UInt8, UInt8))",
		//	args: args{
		//		values: []interface{}{map[interface{}]interface{}{"a": -1}},
		//	},
		//	wantDataWritten: []string{"{}"},
		//	wantRowsRead:    0,
		//	wantErr:         true,
		//},
		//{
		//	name:       "Should throw error if invalid value, 2 rows",
		//	columnType: "LowCardinality(Map(UInt8, UInt8))",
		//	args: args{
		//		values: []interface{}{
		//			map[interface{}]interface{}{uint8(1): uint8(1)},
		//			map[interface{}]interface{}{-1: "laland"},
		//			map[interface{}]interface{}{-1: "laland"},
		//		},
		//	},
		//	wantDataWritten: []string{"{}"},
		//	wantRowsRead:    1,
		//	wantErr:         true,
		//},
		//{
		//	name:       "Should throw error if invalid value, 2 rows",
		//	columnType: "LowCardinality(Map(UInt8, UInt8))",
		//	args: args{
		//		values: []interface{}{
		//			map[uint8]interface{}{
		//				1: uint8(2),
		//			},
		//			map[uint8]interface{}{
		//				1: "2",
		//				2: uint8(2),
		//			},
		//			map[uint8]interface{}{
		//				1: uint8(2),
		//				2: uint8(2),
		//			},
		//		},
		//	},
		//	wantDataWritten: []string{"{}"},
		//	wantRowsRead:    1,
		//	wantErr:         true,
		//},
		//{
		//	name:       "Should throw error if invalid value, 2 rows",
		//	columnType: "LowCardinality(Map(UInt8, LowCardinality(UInt8)))",
		//	args: args{
		//		values: []interface{}{
		//			map[uint8]interface{}{
		//				1: uint8(1),
		//			},
		//			map[uint8]interface{}{
		//				1: uint8(1),
		//			},
		//			map[uint8]interface{}{
		//				1: "p",
		//			},
		//		},
		//	},
		//	wantDataWritten: []string{"{}"},
		//	wantRowsRead:    2,
		//	wantErr:         true,
		//},
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
				assert.Equal(t, value, i.GetString(index))
			}

			if tt.wantEqualValues {
				for index, value := range tt.args.values {
					assert.EqualValues(t, value, i.GetValue(index))
				}
			}
		})
	}
}

func TestLowCardinalityColumnData_EncoderDecoder(t *testing.T) {
	uint16SizeStrings := make([]string, math.MaxUint8+1)
	for i := range uint16SizeStrings {
		uint16SizeStrings[i] = fmt.Sprint(i)
	}

	uint32SizeStrings := make([]string, math.MaxUint16+1)
	for i := range uint32SizeStrings {
		uint32SizeStrings[i] = fmt.Sprint(i)
	}

	type args struct {
		texts []string
	}
	tests := []struct {
		name            string
		columnType      CHColumnType
		args            args
		wantDataWritten []string
		wantRowsRead    int
		wantErr         bool
	}{
		{
			name:       "Should write data and return number of rows read with no error, 0 rows",
			columnType: "LowCardinality(String)",
			args: args{
				texts: []string{},
			},
			wantRowsRead: 0,
			wantErr:      false,
		},
		{
			name:       "Should write data and return number of rows read with no error, 2 rows",
			columnType: "LowCardinality(String)",
			args: args{
				texts: []string{"hello", "good morning", "good morning", "good morning"},
			},
			wantDataWritten: []string{"hello", "good morning", "good morning", "good morning"},
			wantRowsRead:    4,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error, 2 rows",
			columnType: "LowCardinality(String)",
			args: args{
				texts: []string{"hello", "father", "mother", "daughter", "son", "k", "w", "x"},
			},
			wantDataWritten: []string{"hello", "father", "mother", "daughter", "son", "k", "w", "x"},
			wantRowsRead:    8,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error, 2 rows",
			columnType: "LowCardinality(String)",
			args: args{
				texts: []string{"hello"},
			},
			wantDataWritten: []string{"hello"},
			wantRowsRead:    1,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error, uint16Size rows",
			columnType: "LowCardinality(String)",
			args: args{
				texts: uint16SizeStrings,
			},
			wantDataWritten: uint16SizeStrings,
			wantRowsRead:    len(uint16SizeStrings),
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error, uint32Size rows",
			columnType: "LowCardinality(String)",
			args: args{
				texts: uint32SizeStrings,
			},
			wantDataWritten: uint32SizeStrings,
			wantRowsRead:    len(uint32SizeStrings),
			wantErr:         false,
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
			require.Equal(t, tt.wantRowsRead, got)
			require.NoError(t, err)
			err = original.WriteToEncoder(encoder)
			require.NoError(t, err)

			// Read from decoder
			newCopy := MustMakeColumnData(tt.columnType, len(tt.args.texts))
			err = newCopy.ReadFromDecoder(decoder)

			for index, value := range tt.wantDataWritten {
				if !tt.wantErr {
					require.Equal(t, value, newCopy.GetString(index))
					require.Equal(t, value, newCopy.GetValue(index))
				}
			}

			require.Equal(t, newCopy.Len(), original.Len())
			require.Equal(t, newCopy.Zero(), original.Zero())
			require.Equal(t, newCopy.ZeroString(), original.ZeroString())
			require.NoError(t, original.Close())
			require.NoError(t, newCopy.Close())
		})
	}
}
