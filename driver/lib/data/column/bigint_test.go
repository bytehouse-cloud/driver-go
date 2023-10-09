package column

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
)

type bigIntTest struct {
	name            string
	dataType        CHColumnType
	textsToReadFrom []string
	wantRowsRead    int
	wantErr         bool
	wantStrings     []string
}

func testBigIntCommon_ReadFromTexts_ThenGetString(t *testing.T, tests []bigIntTest) {
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			col := MustMakeColumnData(tt.dataType, 1000)

			gotReadRows, err := col.ReadFromTexts(tt.textsToReadFrom)
			if tt.wantErr {
				require.Error(t, err, "ReadFromTexts expect to have error, but got <nil>")
			} else {
				require.NoErrorf(t, err, "ReadFromTexts expect no error but got=[%v]", err)
			}

			require.Equalf(t, tt.wantRowsRead, gotReadRows, "ReadFromTexts() gotReadRows = %v, wantRowsRead %v", gotReadRows, tt.wantRowsRead)

			for idx, wantVal := range tt.wantStrings {
				gotVal := col.GetString(idx)
				require.Equalf(t, wantVal, gotVal, "value with idx=%d expected to be=%v but got=%v", idx, wantVal, gotVal)
			}
		})
	}
}

func TestInt128_ReadFromTexts_ThenGetString(t *testing.T) {

	tests := []bigIntTest{
		{
			name:            "single row, Zero",
			dataType:        INT128,
			textsToReadFrom: []string{"0"},
			wantRowsRead:    1,
			wantErr:         false,
			wantStrings:     []string{"0"},
		},
		{
			name:            "5 rows, all Zeros",
			dataType:        INT128,
			textsToReadFrom: []string{"0", "0", "", "", "0"},
			wantRowsRead:    5,
			wantErr:         false,
			wantStrings:     []string{"0", "0", "0", "0", "0"},
		},
		{
			name:     "random numbers, also with largest and smallest Int128",
			dataType: INT128,
			textsToReadFrom: []string{
				"564513746841", "-646030650650561061", "1", "10000002000000300000", "-666644455324891",
				"-1654610320230363103", "564513746841", "-99946030650650561061", "1712199806031998", "10000002000000300000",
				"-170141183460469231731687303715884105728", "170141183460469231731687303715884105727", "208888", "-54388821", "-17121998060142331998",
			},
			wantRowsRead: 15,
			wantErr:      false,
			wantStrings: []string{
				"564513746841", "-646030650650561061", "1", "10000002000000300000", "-666644455324891",
				"-1654610320230363103", "564513746841", "-99946030650650561061", "1712199806031998", "10000002000000300000",
				"-170141183460469231731687303715884105728", "170141183460469231731687303715884105727", "208888", "-54388821", "-17121998060142331998",
			},
		},
		{
			name:            "5 rows, third row is not float or decimal",
			dataType:        INT128,
			textsToReadFrom: []string{"1", "-1", "1.1", "3", "4"},
			wantRowsRead:    2,
			wantErr:         true,
		},
		{
			name:            "5 rows, third row has character",
			dataType:        INT128,
			textsToReadFrom: []string{"0", "1", "1a", "3", "4"},
			wantRowsRead:    2,
			wantErr:         true,
		},
	}

	testBigIntCommon_ReadFromTexts_ThenGetString(t, tests)
}

func TestUInt128_ReadFromTexts_ThenGetString(t *testing.T) {
	tests := []bigIntTest{
		{
			name:            "single row, Zero",
			dataType:        UINT128,
			textsToReadFrom: []string{"0"},
			wantRowsRead:    1,
			wantErr:         false,
			wantStrings:     []string{"0"},
		},
		{
			name:            "5 rows, all Zeros",
			dataType:        UINT128,
			textsToReadFrom: []string{"0", "0", "", "", "null"},
			wantRowsRead:    5,
			wantErr:         false,
			wantStrings:     []string{"0", "0", "0", "0", "0"},
		},
		{
			name:     "random numbers, also with largest UInt128",
			dataType: UINT128,
			textsToReadFrom: []string{
				"564513746841", "1", "10000002000000300000",
				"564513746841", "646030650650561061", "1712199806031998", "10000002000000300000",
				"340282366920938463463374607431768211455", "20", "54321", "1712199806031998",
			},
			wantRowsRead: 11,
			wantErr:      false,
			wantStrings: []string{
				"564513746841", "1", "10000002000000300000",
				"564513746841", "646030650650561061", "1712199806031998", "10000002000000300000",
				"340282366920938463463374607431768211455", "20", "54321", "1712199806031998",
			},
		},
		{
			name:            "5 rows, third row is negative number",
			dataType:        UINT128,
			textsToReadFrom: []string{"0", "1", "-12", "3", "4"},
			wantRowsRead:    2,
			wantErr:         true,
		},
		{
			name:            "5 rows, third row is not float or decimal",
			dataType:        UINT128,
			textsToReadFrom: []string{"0", "1", "1.1", "3", "4"},
			wantRowsRead:    2,
			wantErr:         true,
		},
		{
			name:            "5 rows, third row has character",
			dataType:        UINT128,
			textsToReadFrom: []string{"0", "1", "1a", "3", "4"},
			wantRowsRead:    2,
			wantErr:         true,
		},
	}

	testBigIntCommon_ReadFromTexts_ThenGetString(t, tests)
}

func TestInt256_ReadFromTexts_ThenGetString(t *testing.T) {
	tests := []bigIntTest{
		{
			name:            "single row, Zero",
			dataType:        INT256,
			textsToReadFrom: []string{"0"},
			wantRowsRead:    1,
			wantErr:         false,
			wantStrings:     []string{"0"},
		},
		{
			name:            "5 rows, all Zeros",
			dataType:        INT256,
			textsToReadFrom: []string{"0", "0", "", "", "0"},
			wantRowsRead:    5,
			wantErr:         false,
			wantStrings:     []string{"0", "0", "0", "0", "0"},
		},
		{
			name:     "random numbers, also with largest and smallest Int256",
			dataType: INT256,
			textsToReadFrom: []string{
				"564513746841", "-646030650650561061", "1", "10000002000000300000", "-666644455324891",
				"-1654610320230363103", "564513746841", "-99946030650650561061", "1712199806031998", "10000002000000300000",
				"-170141183460469231731687303715884105728", "170141183460469231731687303715884105727", "208888", "-54388821", "-17121998060142331998",
				"57896044618658097711785492504343953926634992332820282019728792003956564819967", "-57896044618658097711785492504343953926634992332820282019728792003956564819968",
			},
			wantRowsRead: 17,
			wantErr:      false,
			wantStrings: []string{
				"564513746841", "-646030650650561061", "1", "10000002000000300000", "-666644455324891",
				"-1654610320230363103", "564513746841", "-99946030650650561061", "1712199806031998", "10000002000000300000",
				"-170141183460469231731687303715884105728", "170141183460469231731687303715884105727", "208888", "-54388821", "-17121998060142331998",
				"57896044618658097711785492504343953926634992332820282019728792003956564819967", "-57896044618658097711785492504343953926634992332820282019728792003956564819968",
			},
		},
		{
			name:            "5 rows, third row is not float or decimal",
			dataType:        INT256,
			textsToReadFrom: []string{"1", "-1", "1.1", "3", "4"},
			wantRowsRead:    2,
			wantErr:         true,
		},
		{
			name:            "5 rows, third row has character",
			dataType:        INT256,
			textsToReadFrom: []string{"0", "1", "1a", "3", "4"},
			wantRowsRead:    2,
			wantErr:         true,
		},
	}

	testBigIntCommon_ReadFromTexts_ThenGetString(t, tests)
}

func TestUInt256_ReadFromTexts_ThenGetString(t *testing.T) {
	tests := []bigIntTest{
		{
			name:            "single row, Zero",
			dataType:        UINT256,
			textsToReadFrom: []string{"0"},
			wantRowsRead:    1,
			wantErr:         false,
			wantStrings:     []string{"0"},
		},
		{
			name:            "5 rows, all Zeros",
			dataType:        UINT256,
			textsToReadFrom: []string{"0", "0", "", "", "0"},
			wantRowsRead:    5,
			wantErr:         false,
			wantStrings:     []string{"0", "0", "0", "0", "0"},
		},
		{
			name:     "random numbers, also with largest UInt256",
			dataType: UINT256,
			textsToReadFrom: []string{
				"564513746841", "1", "10000002000000300000",
				"564513746841", "646030650650561061", "1712199806031998", "10000002000000300000",
				"340282366920938463463374607431768211455", "20", "54321", "1712199806031998",
				"115792089237316195423570985008687907853269984665640564039457584007913129639935",
			},
			wantRowsRead: 12,
			wantErr:      false,
			wantStrings: []string{
				"564513746841", "1", "10000002000000300000",
				"564513746841", "646030650650561061", "1712199806031998", "10000002000000300000",
				"340282366920938463463374607431768211455", "20", "54321", "1712199806031998",
				"115792089237316195423570985008687907853269984665640564039457584007913129639935",
			},
		},
		{
			name:            "5 rows, third row is negative number",
			dataType:        UINT256,
			textsToReadFrom: []string{"0", "1", "-12", "3", "4"},
			wantRowsRead:    2,
			wantErr:         true,
		},
		{
			name:            "5 rows, third row is not float or decimal",
			dataType:        UINT256,
			textsToReadFrom: []string{"0", "1", "1.1", "3", "4"},
			wantRowsRead:    2,
			wantErr:         true,
		},
		{
			name:            "5 rows, third row has character",
			dataType:        UINT256,
			textsToReadFrom: []string{"0", "1", "1a", "3", "4"},
			wantRowsRead:    2,
			wantErr:         true,
		},
	}

	testBigIntCommon_ReadFromTexts_ThenGetString(t, tests)
}

func TestMakeBigIntDataType(t *testing.T) {
	{
		chCol := MustMakeColumnData("Int128", 3)
		bigInt, ok := chCol.(*BigIntColumnData)
		require.True(t, ok, "Column is not of *BigIntColumnData")
		require.Equal(t, 3*16, len(bigInt.raw), "raw byteBuffer size incorrect")
		require.True(t, bigInt.isSigned, "Int128 must have isSigned=true")
	}
	{
		chCol := MustMakeColumnData("UInt128", 3)
		bigInt, ok := chCol.(*BigIntColumnData)
		require.True(t, ok, "Column is not of *BigIntColumnData")
		require.Equal(t, 3*16, len(bigInt.raw), "raw byteBuffer size incorrect")
		require.False(t, bigInt.isSigned, "UInt128 must have isSigned=false")
	}
	{
		chCol := MustMakeColumnData("Int256", 3)
		bigInt, ok := chCol.(*BigIntColumnData)
		require.True(t, ok, "Column is not of *BigIntColumnData")
		require.Equal(t, 3*32, len(bigInt.raw), "raw byteBuffer size incorrect")
		require.True(t, bigInt.isSigned, "Int256 must have isSigned=true")
	}
	{
		chCol := MustMakeColumnData("UInt256", 3)
		bigInt, ok := chCol.(*BigIntColumnData)
		require.True(t, ok, "Column is not of *BigIntColumnData")
		require.Equal(t, 3*32, len(bigInt.raw), "raw byteBuffer size incorrect")
		require.False(t, bigInt.isSigned, "UInt256 must have isSigned=false")
	}
}

func TestBigInt_ReadFromValues_ThenGetString(t *testing.T) {
	tests := []struct {
		name             string
		dataType         CHColumnType
		valuesToReadFrom []interface{}
		wantRowsRead     int
		wantErr          bool
		wantStrings      []string
	}{
		{
			name:     "UInt256, nulls",
			dataType: UINT256,
			valuesToReadFrom: []interface{}{
				nil,
			},
			wantRowsRead: 1,
			wantStrings: []string{
				"0",
			},
		},
		{
			name:     "UInt256, all built-in integer data types (non-negative only)",
			dataType: UINT256,
			valuesToReadFrom: []interface{}{
				0,
				int(1), int8(127), int16(32767), int32(2147483647), int64(9223372036854775807),
				uint(2), uint8(255), uint16(65535), uint32(4294967295), uint64(18446744073709551615),
				*(new(big.Int).SetUint64(18446744073709551615)), new(big.Int).SetUint64(18446744073709551615),
			},
			wantRowsRead: 13,
			wantStrings: []string{
				"0",
				"1", "127", "32767", "2147483647", "9223372036854775807",
				"2", "255", "65535", "4294967295", "18446744073709551615",
				"18446744073709551615", "18446744073709551615",
			},
		},
		{
			name:             "UInt256, int, negative",
			dataType:         UINT256,
			valuesToReadFrom: []interface{}{int(-1)},
			wantRowsRead:     0,
			wantErr:          true,
		},
		{
			name:             "UInt256, int64, negative",
			dataType:         UINT256,
			valuesToReadFrom: []interface{}{int64(-922337203685477580)},
			wantRowsRead:     0,
			wantErr:          true,
		},
		{
			name:             "UInt256, *big.Int, negative",
			dataType:         UINT256,
			valuesToReadFrom: []interface{}{new(big.Int).SetInt64(-123)},
			wantRowsRead:     0,
			wantErr:          true,
		},
		{
			name:     "Int256, all built-in integer data types",
			dataType: INT256,
			valuesToReadFrom: []interface{}{
				0,
				int(1), int8(127), int16(32767), int32(2147483647), int64(9223372036854775807),
				int(-1), int8(-128), int16(-32768), int32(-2147483648), int64(-9223372036854775808),
				uint(2), uint8(255), uint16(65535), uint32(4294967295), uint64(18446744073709551615),
				*(new(big.Int).SetInt64(1844674407370955161)), new(big.Int).SetInt64(-1844674407370955161),
			},
			wantRowsRead: 18,
			wantStrings: []string{
				"0",
				"1", "127", "32767", "2147483647", "9223372036854775807",
				"-1", "-128", "-32768", "-2147483648", "-9223372036854775808",
				"2", "255", "65535", "4294967295", "18446744073709551615",
				"1844674407370955161", "-1844674407370955161",
			},
		},
		{
			name:             "Int256, value is string at 2nd row",
			dataType:         INT256,
			valuesToReadFrom: []interface{}{0, "abc"},
			wantRowsRead:     1,
			wantErr:          true,
		},
		{
			name:             "UInt256, value is float at 2nd row",
			dataType:         UINT256,
			valuesToReadFrom: []interface{}{0, 321.123},
			wantRowsRead:     1,
			wantErr:          true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			col := MustMakeColumnData(tt.dataType, 1000)

			gotReadRows, err := col.ReadFromValues(tt.valuesToReadFrom)
			if tt.wantErr {
				require.Error(t, err, "ReadFromValues expect to have error, but got <nil>")
			} else {
				require.NoErrorf(t, err, "ReadFromValues expect no error but got=[%v]", err)
			}

			require.Equalf(t, tt.wantRowsRead, gotReadRows, "ReadFromValues() gotReadRows = %v, wantRowsRead %v", gotReadRows, tt.wantRowsRead)

			for idx, wantVal := range tt.wantStrings {
				gotVal := col.GetString(idx)
				require.Equalf(t, wantVal, gotVal, "value with idx=%d expected to be=%v but got=%v", idx, wantVal, gotVal)
			}
		})
	}
}
