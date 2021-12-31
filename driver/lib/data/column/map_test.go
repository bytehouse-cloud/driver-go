package column

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

func TestMapColumnData_ReadFromTexts(t *testing.T) {
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
			name:       "Should write data and return number of rows read with no error, 3 rows",
			columnType: "Map(UInt64, UInt64)",
			args: args{
				texts: []string{"{1 : 3}"},
			},
			wantDataWritten: []string{"{1: 3}"},
			wantRowsRead:    1,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error, 3 rows",
			columnType: "Map(String, String)",
			args: args{
				texts: []string{"{1:3}"},
			},
			wantDataWritten: []string{"{'1': '3'}"},
			wantRowsRead:    1,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error, 3 rows",
			columnType: "Map(UInt64, UInt64)",
			args: args{
				texts: []string{"{1 : 3}", "{1 : 3 }"},
			},
			wantDataWritten: []string{"{1: 3}", "{1: 3}"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:       "Should write empty string with no error",
			columnType: "Map(UInt64, UInt64)",
			args: args{
				texts: []string{"", "{1 : 3 }"},
			},
			wantDataWritten: []string{"{}", "{1: 3}"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error, 3 rows",
			columnType: "Map(UInt64, UInt64)",
			args: args{
				texts: []string{"{}"},
			},
			wantDataWritten: []string{"{}"},
			wantRowsRead:    1,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error, 3 rows",
			columnType: "Map(UInt64, UInt64)",
			args: args{
				texts: []string{"{     }"},
			},
			wantDataWritten: []string{"{}"},
			wantRowsRead:    1,
			wantErr:         false,
		},
		{
			name:       "Should return empty map and some other map",
			columnType: "Map(UInt8, UInt8)",
			args: args{
				texts: []string{"{   }", "{1: 2}", "{}"},
			},
			wantDataWritten: []string{"{}", "{1: 2}", "{}"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error, nested maps",
			columnType: "Map(UInt8,Map(UInt8, UInt8))",
			args: args{
				texts: []string{"{10: {1 : 2}, 5: {3 : 4}}", "{10: {1 : 2}, 5: {3 : 4}}"},
			},
			wantDataWritten: []string{"{10: {1: 2}, 5: {3: 4}}", "{10: {1: 2}, 5: {3: 4}}"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:       "Should throw error if invalid value",
			columnType: "Map(UInt8, UInt8)",
			args: args{
				texts: []string{"{1:koo}", "{1:1}"},
			},
			wantDataWritten: []string{"{1: 0}"},
			wantRowsRead:    0,
			wantErr:         true,
		},
		{
			name:       "Should throw error if invalid value",
			columnType: "Map(UInt8, UInt8)",
			args: args{
				texts: []string{"{a: -1}"},
			},
			wantDataWritten: []string{"{0: 0}"},
			wantRowsRead:    0,
			wantErr:         true,
		},
		{
			name:       "Should throw error if invalid value",
			columnType: "Map(UInt8, UInt8)",
			args: args{
				texts: []string{"{1: 1}", "{-1: 'lalaland'}", "{-1: 'lalaland'}"},
			},
			wantDataWritten: []string{"{1: 1}"},
			wantRowsRead:    1,
			wantErr:         true,
		},
		{
			name:       "Should throw error if invalid value",
			columnType: "Map(UInt8, UInt8)",
			args: args{
				texts: []string{"{1: 1}", "{0: -1}", "{-1: 3}"},
			},
			wantDataWritten: []string{"{1: 1}"},
			wantRowsRead:    1,
			wantErr:         true,
		},
		{
			name:       "Should throw error if invalid value",
			columnType: "Map(UInt8, UInt8)",
			args: args{
				texts: []string{"{1: 1: 3}"},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
		{
			name:       "Should throw error if invalid value",
			columnType: "Map(UInt8, UInt8)",
			args: args{
				texts: []string{"{1: 3]"},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
		{
			name:       "Should throw error if invalid value",
			columnType: "Map(UInt8, UInt8)",
			args: args{
				texts: []string{"{1: 1}", "{-1: 3}"},
			},
			wantDataWritten: []string{"{1: 1}"},
			wantRowsRead:    1,
			wantErr:         true,
		},
		{
			name:       "Should throw error if invalid value",
			columnType: "Map(UInt8, UInt8)",
			args: args{
				texts: []string{"{1: 1, 2: 2}", "{1: a, 2: 2}", "{1: 1, 2: 2}"},
			},
			wantDataWritten: []string{"{1: 1, 2: 2}"},
			wantRowsRead:    1,
			wantErr:         true,
		},
		{
			name:       "Should throw error if invalid value",
			columnType: "Map(UInt8, UInt8)",
			args: args{
				texts: []string{"{1: 1}", "{1: 1}", "{1: p}"},
			},
			wantDataWritten: []string{"{1: 1}", "{1: 1}"},
			wantRowsRead:    2,
			wantErr:         true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := MustMakeColumnData(tt.columnType, 1000)

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

type DemoType struct {
	Key uint8
}

type DemoTypeWithTag struct {
	Key uint8 `clickhouse:"key"`
}

func TestMapColumnData_ReadFromValues(t *testing.T) {
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
			name:       "Should write data and return number of rows read with no error",
			columnType: "Map(UInt64, UInt64)",
			args: args{
				values: []interface{}{},
			},
			wantEqualValues: true,
			wantDataWritten: []string{"{}"},
			wantRowsRead:    0,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error",
			columnType: "Map(String, UInt64)",
			args: args{
				values: []interface{}{map[string]uint64{"hi": 3}, nil},
			},
			wantDataWritten: []string{"{'hi': 3}", "{}"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error",
			columnType: "Map(UInt64, UInt64)",
			args: args{
				values: []interface{}{nil, nil},
			},
			wantDataWritten: []string{"{}", "{}"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error",
			columnType: "Map(UInt64, UInt64)",
			args: args{
				values: []interface{}{map[uint64]uint64{1: 3}},
			},
			wantEqualValues: true,
			wantDataWritten: []string{"{1: 3}"},
			wantRowsRead:    1,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error",
			columnType: "Map(UInt64, UInt64)",
			args: args{
				values: []interface{}{map[interface{}]interface{}{uint8(1): uint32(3)}, map[interface{}]interface{}{uint8(1): uint32(3)}},
			},
			wantDataWritten: []string{"{1: 3}", "{1: 3}"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error if empty",
			columnType: "Map(UInt8, UInt32)",
			args: args{
				values: []interface{}{map[uint8]uint32{}},
			},
			wantDataWritten: []string{"{}"},
			wantEqualValues: true,
			wantRowsRead:    1,
			wantErr:         false,
		},
		{
			name:       "Should return empty map if nil",
			columnType: "Map(UInt8, UInt32)",
			args: args{
				values: []interface{}{nil},
			},
			wantDataWritten: []string{"{}"},
			wantEqualValues: false,
			wantRowsRead:    1,
			wantErr:         false,
		},
		{
			name:       "Should return empty map and some other map",
			columnType: "Map(UInt8, UInt8)",
			args: args{
				values: []interface{}{nil, map[uint8]uint8{1: 2}, nil, map[uint8]uint8{1: 2}},
			},
			wantDataWritten: []string{"{}", "{1: 2}", "{}", "{1: 2}"},
			wantRowsRead:    4,
			wantErr:         false,
		},
		{
			name:       "Should return empty map and some other map",
			columnType: "Map(UInt8, UInt8)",
			args: args{
				values: []interface{}{map[uint8]uint8{}, map[uint8]uint8{1: 2}, map[uint8]uint8{}},
			},
			wantDataWritten: []string{"{}", "{1: 2}", "{}"},
			wantRowsRead:    3,
			wantErr:         false,
		},

		{
			name:       "Should write data and return number of rows read with no error, nested maps",
			columnType: "Map(UInt8,Map(UInt8, UInt8))",
			args: args{
				values: []interface{}{map[uint8]map[uint8]uint8{10: {1: 2}, 5: {3: 4}}, map[uint8]map[uint8]uint8{10: {1: 2}, 5: {3: 4}}},
			},
			wantDataWritten: []string{}, // don't test this, since maps are unordered, can't reliably check if same
			wantEqualValues: true,       // test values equality
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error, struct as key",
			columnType: "Map(Map(String, UInt8), UInt8)",
			args: args{
				values: []interface{}{map[DemoType]uint8{{Key: 2}: 8}, map[DemoType]uint8{{Key: 2}: 8}},
			},
			wantDataWritten: []string{"{{'Key': 2}: 8}", "{{'Key': 2}: 8}"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:       "Should convert struct with struct tag, struct as key",
			columnType: "Map(Map(String, UInt8), UInt8)",
			args: args{
				values: []interface{}{map[DemoTypeWithTag]uint8{{Key: 2}: 8}, map[DemoTypeWithTag]uint8{{Key: 2}: 8}},
			},
			wantDataWritten: []string{"{{'key': 2}: 8}", "{{'key': 2}: 8}"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:       "Should throw error, no panic, if struct is unexported",
			columnType: "Map(Map(String, UInt8), UInt8)",
			args: args{
				values: []interface{}{struct{ name string }{name: "jack"}},
			},
			wantDataWritten: []string{"{}"},
			wantRowsRead:    0,
			wantErr:         true,
		},
		{
			name:       "Should throw error if invalid value",
			columnType: "Map(UInt8, UInt8)",
			args: args{
				values: []interface{}{map[uint8]interface{}{1: "koo"}, map[uint8]interface{}{1: uint8(1)}},
			},
			wantDataWritten: []string{"{1: 0}"},
			wantRowsRead:    0,
			wantErr:         true,
		},
		{
			name:       "Should throw error if invalid value",
			columnType: "Map(UInt8, UInt8)",
			args: args{
				values: []interface{}{map[interface{}]interface{}{"a": -1}},
			},
			wantDataWritten: []string{"{0: 0}"},
			wantRowsRead:    0,
			wantErr:         true,
		},
		{
			name:       "Should throw error if invalid value",
			columnType: "Map(UInt8, UInt8)",
			args: args{
				values: []interface{}{
					map[interface{}]interface{}{uint8(1): uint8(1)},
					map[interface{}]interface{}{-1: "laland"},
					map[interface{}]interface{}{-1: "laland"},
				},
			},
			wantDataWritten: []string{"{1: 1}"},
			wantRowsRead:    1,
			wantErr:         true,
		},
		{
			name:       "Should throw error if invalid value",
			columnType: "Map(UInt8, UInt8)",
			args: args{
				values: []interface{}{
					map[uint8]interface{}{
						1: uint8(2),
					},
					map[uint8]interface{}{
						1: "2",
						2: uint8(2),
					},
					map[uint8]interface{}{
						1: uint8(2),
						2: uint8(2),
					},
				},
			},
			wantDataWritten: []string{"{1: 2}"},
			wantRowsRead:    1,
			wantErr:         true,
		},
		{
			name:       "Should throw error if invalid value",
			columnType: "Map(UInt8, UInt8)",
			args: args{
				values: []interface{}{
					map[uint8]interface{}{
						1: uint8(1),
					},
					map[uint8]interface{}{
						1: uint8(1),
					},
					map[uint8]interface{}{
						1: "p",
					},
				},
			},
			wantDataWritten: []string{"{1: 1}", "{1: 1}"},
			wantRowsRead:    2,
			wantErr:         true,
		},
		{
			name:       "Should throw error if invalid value",
			columnType: "Map(String, UInt8)",
			args: args{
				values: []interface{}{
					map[uint8]uint8{
						1: 8,
					},
					DemoType{
						Key: 1,
					},
				},
			},
			wantErr: true,
		},
		{
			name:       "Should throw error if invalid value",
			columnType: "Map(String, UInt8)",
			args: args{
				values: []interface{}{
					DemoType{
						Key: 1,
					},
					map[string]uint8{
						"1": 8,
					},
				},
			},
			wantErr: true,
		},
		{
			name:       "Should throw error if invalid value",
			columnType: "Map(UInt8, UInt8)",
			args: args{
				values: []interface{}{
					map[uint8]uint8{
						1: 8,
					},
					map[uint8]string{
						1: "8",
					},
					map[interface{}]interface{}{
						"1": uint8(8),
					},
				},
			},
			wantRowsRead: 1,
			wantErr:      true,
		},
		{
			name:       "Should throw error if invalid value",
			columnType: "Map(UInt8, UInt8)",
			args: args{
				values: []interface{}{
					map[string]uint8{
						"1": 8,
					},
				},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
		{
			name:       "Should throw error if invalid type",
			columnType: "Map(UInt8, UInt8)",
			args: args{
				values: []interface{}{
					1,
				},
			},
			wantDataWritten: []string{"{}"},
			wantRowsRead:    0,
			wantErr:         true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := MustMakeColumnData(tt.columnType, len(tt.args.values))

			got, err := i.ReadFromValues(tt.args.values)
			if tt.wantErr {
				require.Error(t, err)
				require.Equal(t, got, tt.wantRowsRead)
				return
			}
			require.NoError(t, err)

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

func TestMapColumnData_EncoderDecoder(t *testing.T) {
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
			name:       "Should write data and return number of rows read with no error, 3 rows",
			columnType: "Map(UInt64, UInt64)",
			args: args{
				texts: []string{"{1 : 3}"},
			},
			wantDataWritten: []string{"{1: 3}"},
			wantRowsRead:    1,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error, 3 rows",
			columnType: "Map(String, String)",
			args: args{
				texts: []string{"{1:3}"},
			},
			wantDataWritten: []string{"{'1': '3'}"},
			wantRowsRead:    1,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error, 3 rows",
			columnType: "Map(UInt64, UInt64)",
			args: args{
				texts: []string{"{1 : 3}", "{1 : 3 }"},
			},
			wantDataWritten: []string{"{1: 3}", "{1: 3}"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:       "Should write empty string with no error",
			columnType: "Map(UInt64, UInt64)",
			args: args{
				texts: []string{"", "{1 : 3 }"},
			},
			wantDataWritten: []string{"{}", "{1: 3}"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error, 3 rows",
			columnType: "Map(UInt64, UInt64)",
			args: args{
				texts: []string{"{}"},
			},
			wantDataWritten: []string{"{}"},
			wantRowsRead:    1,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error, 3 rows",
			columnType: "Map(UInt64, UInt64)",
			args: args{
				texts: []string{"{     }"},
			},
			wantDataWritten: []string{"{}"},
			wantRowsRead:    1,
			wantErr:         false,
		},
		{
			name:       "Should return empty map and some other map",
			columnType: "Map(UInt8, UInt8)",
			args: args{
				texts: []string{"{   }", "{1: 2}", "{}"},
			},
			wantDataWritten: []string{"{}", "{1: 2}", "{}"},
			wantRowsRead:    3,
			wantErr:         false,
		},
		{
			name:       "Should write data and return number of rows read with no error, nested maps",
			columnType: "Map(UInt8,Map(UInt8, UInt8))",
			args: args{
				texts: []string{"{10: {1 : 2}, 5: {3 : 4}}", "{10: {1 : 2}, 5: {3 : 4}}"},
			},
			wantDataWritten: []string{"{10: {1: 2}, 5: {3: 4}}", "{10: {1: 2}, 5: {3: 4}}"},
			wantRowsRead:    2,
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
			require.Equal(t, got, tt.wantRowsRead)
			require.NoError(t, err)
			err = original.WriteToEncoder(encoder)
			require.NoError(t, err)

			// Read from decoder
			newCopy := MustMakeColumnData(tt.columnType, len(tt.args.texts))
			err = newCopy.ReadFromDecoder(decoder)

			for index, value := range tt.wantDataWritten {
				if !tt.wantErr {
					require.Equal(t, value, newCopy.GetString(index))
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
