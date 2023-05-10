package column

import (
	"bytes"
	"math/big"
	"math/rand"
	"strconv"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

func TestDecimalColumnData_ReadFromValues(t *testing.T) {
	type args struct {
		values []interface{}
	}
	bigDecimal, _ := decimal.NewFromString("12132132132132321321321321321321313")
	smallDecimal, _ := decimal.NewFromString("-12132132132132321321321321321321313")
	mockDecimal, _ := decimal.NewFromString("123456789.12345679")

	dec128_1, _ := decimal.NewFromString("-99999999999999999999111.122228888777733")
	dec128_2, _ := decimal.NewFromString("-9.012345678987654")
	dec128_3, _ := decimal.NewFromString("-3.141592653589793")
	dec128_4, _ := decimal.NewFromString("99999999999999999999999.999999999999999")
	dec128_5, _ := decimal.NewFromString("-99999999999999999999999.999999999999999")
	dec128_exceedbitlen, _ := decimal.NewFromString("300000000000000000000000.0000000000001")
	dec128_exceedprecision, _ := decimal.NewFromString("100000000000000000000000.123")

	dec256_1, _ := decimal.NewFromString("999999.9999999999999999999999999999999999999999999999999999999999999999999999")
	dec256_2, _ := decimal.NewFromString("-999999.9999999999999999999999999999999999999999999999999999999999999999999999")
	dec256_3, _ := decimal.NewFromString("0")
	dec256_4, _ := decimal.NewFromString("-0")
	dec256_5, _ := decimal.NewFromString("3.141592653589793")
	dec256_6, _ := decimal.NewFromString("99999.122228888777733")
	dec256_exceedbitlen, _ := decimal.NewFromString("-1999999.123")
	dec256_exceedprecision, _ := decimal.NewFromString("-1000000.1")

	tests := []struct {
		name            string
		args            args
		decimalType     CHColumnType
		wantDataWritten []interface{}
		wantValueString []string
		wantRowsRead    int
		wantErr         bool
	}{
		{
			name:        "Should throw error if precision too big",
			decimalType: "Decimal(111,5)",
			args: args{
				values: []interface{}{float32(122.23), float64(4.33333)},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
		{
			name:        "Should throw error if read value not a decimal for Decimal32",
			decimalType: "Decimal(1,4)",
			args: args{
				values: []interface{}{"baba"},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
		{
			name:        "Should throw error if read value not a decimal for Decimal64",
			decimalType: "Decimal(18,4)",
			args: args{
				values: []interface{}{"baba"},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
		{
			name:        "Should throw error if read value not a decimal for Decimal128",
			decimalType: "Decimal(19,4)",
			args: args{
				values: []interface{}{"baba"},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
		{
			name:        "Should write data and return number of rows read with no error for Decimal32 for min and max supported",
			decimalType: "Decimal(1,4)",
			args: args{
				values: []interface{}{-99999.9999, 99999.9999, 0, nil},
			},
			wantValueString: []string{"-99999.9999", "99999.9999", "0.0000", "0.0000"},
			wantRowsRead:    4,
			wantErr:         false,
		},
		{
			name:        "Should write error because value is > max range, it will overflow",
			decimalType: "Decimal(1,4)",
			args: args{
				values: []interface{}{999999.9999},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
		{
			name:        "Should write error because value is < min range, it will overflow",
			decimalType: "Decimal(1,4)",
			args: args{
				values: []interface{}{-999999.9999},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
		{
			name:        "Should write data and return number of rows read with no error for empty data",
			decimalType: "Decimal(18,5)",
			args: args{
				values: []interface{}{},
			},
			wantValueString: []string{"0.00000"},
			wantRowsRead:    0,
			wantErr:         false,
		},
		{
			name:        "Should write error because value is > max range, it will overflow",
			decimalType: "Decimal(18, 16)",
			args: args{
				values: []interface{}{decimal.New(1, 3)},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
		{
			name:        "Should write error because value is < min range, it will overflow",
			decimalType: "Decimal(18, 16)",
			args: args{
				values: []interface{}{decimal.New(-1, 3)},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
		{
			name:        "Should write error because value is > max range, it will overflow",
			decimalType: "Decimal(38, 38)",
			args: args{
				values: []interface{}{bigDecimal},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
		{
			name:        "Should write error because value is < min range, it will overflow",
			decimalType: "Decimal(38, 38)",
			args: args{
				values: []interface{}{smallDecimal},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
		{
			name:        "Should write error because value is < min range, it will overflow",
			decimalType: "Decimal(38, 38)",
			args: args{
				values: []interface{}{mockDecimal},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
		{
			name:        "Should write error because value is > max range, it will overflow",
			decimalType: "Decimal(18, 16)",
			args: args{
				values: []interface{}{decimal.NewFromInt(100), decimal.NewFromInt(-100)},
			},
			// ////////////////////////////1234567890123456
			wantValueString: []string{"100.0000000000000000", "-100.0000000000000000"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:        "Should write data and return number of rows read with no error for empty data",
			decimalType: "Decimal(38,38)",
			args: args{
				values: []interface{}{},
			},
			// //////////////////////////12345678901234567890123456789012345678
			wantValueString: []string{"0.00000000000000000000000000000000000000"},
			wantRowsRead:    0,
			wantErr:         false,
		},
		{
			name:        "Should write data and return number of rows read with no error for empty data",
			decimalType: "Decimal(0,0)",
			args: args{
				values: []interface{}{},
			},
			wantValueString: []string{"0"},
			wantRowsRead:    0,
			wantErr:         false,
		},
		{
			name:        "Should write data and return number of rows read with no error for float64",
			decimalType: "Decimal(18,5)",
			args: args{
				values: []interface{}{float64(122), float64(123)},
			},
			wantDataWritten: nil,
			wantValueString: []string{"122.00000", "123.00000"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:        "Should write data and return number of rows read with no error for float32",
			decimalType: "Decimal(18,5)",
			args: args{
				values: []interface{}{float32(122), float32(123)},
			},
			wantValueString: []string{"122.00000", "123.00000"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:        "Should write data and return number of rows read with no error for int8",
			decimalType: "Decimal(18,5)",
			args: args{
				values: []interface{}{int8(122), int8(123)},
			},
			wantValueString: []string{"122.00000", "123.00000"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:        "Should write data and return number of rows read with no error for supported datatypes",
			decimalType: "Decimal(18,5)",
			args: args{
				values: []interface{}{
					int(123),
					int8(123),
					int16(123),
					int32(123),
					int64(123),
					uint(122),
					uint8(122),
					uint16(122),
					uint32(122),
					uint64(122),
					float32(122.23),
					float64(4.33333),
					big.NewFloat(4.33333),
					big.NewInt(1234),
					decimal.NewFromInt(1234),
				},
			},
			wantValueString: []string{
				"123.00000", "123.00000", "123.00000", "123.00000", "123.00000",
				"122.00000", "122.00000", "122.00000", "122.00000", "122.00000",
				"122.23000", "4.33333", "4.33333",
				"1234.00000", "1234.00000",
			},
			wantRowsRead: 15,
			wantErr:      false,
		},
		{
			name:        "Should write data and return number of rows read with no error for supported datatypes edge cases",
			decimalType: "Decimal(38,0)",
			args: args{
				values: []interface{}{
					int(2147483647),
					int8(127),
					int16(32767),
					int32(2147483647),
					int64(9223372036854775807),
					uint(4294967295),
					uint8(255),
					uint16(65535),
					uint32(4294967295),
					uint64(18446744073709551615),
				},
			},
			wantValueString: []string{
				"2147483647", "127", "32767", "2147483647", "9223372036854775807",
				"4294967295", "255", "65535", "4294967295", "18446744073709551615",
			},
			wantRowsRead: 10,
			wantErr:      false,
		},
		{
			name:        "(P,S)=(38,15) and not overflow",
			decimalType: "Decimal(38,15)",
			args: args{
				values: []interface{}{
					dec128_1,
					dec128_2,
					dec128_3,
					dec128_4,
					dec128_5,
				},
			},
			wantValueString: []string{
				"-99999999999999999999111.122228888777733",
				"-9.012345678987654",
				"-3.141592653589793",
				"99999999999999999999999.999999999999999",
				"-99999999999999999999999.999999999999999",
			},
			wantRowsRead: 5,
			wantErr:      false,
		},
		{
			name:        "(P,S)=(38,15) and overflow at 5th row because BitLen > 127",
			decimalType: "Decimal(38,15)",
			args: args{
				values: []interface{}{
					dec128_1,
					dec128_2,
					dec128_3,
					dec128_4,
					dec128_exceedbitlen,
					dec128_5,
				},
			},
			wantRowsRead: 4,
			wantErr:      true,
		},
		{
			name:        "(P,S)=(38,15) and overflow at 1st row because Precision > 38",
			decimalType: "Decimal(38,15)",
			args: args{
				values: []interface{}{
					dec128_exceedprecision,
					dec128_3,
					dec128_4,
					dec128_5,
				},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
		{
			name:        "(P,S)=(76,70) and not overflow",
			decimalType: "Decimal(76,70)",
			args: args{
				values: []interface{}{
					dec256_1,
					dec256_2,
					dec256_3,
					dec256_4,
					dec256_5,
					dec256_6,
				},
			},
			wantValueString: []string{
				"999999.9999999999999999999999999999999999999999999999999999999999999999999999",
				"-999999.9999999999999999999999999999999999999999999999999999999999999999999999",
				"0.0000000000000000000000000000000000000000000000000000000000000000000000",
				"0.0000000000000000000000000000000000000000000000000000000000000000000000",
				"3.1415926535897930000000000000000000000000000000000000000000000000000000",
				"99999.1222288887777330000000000000000000000000000000000000000000000000000000",
			},
			wantRowsRead: 6,
			wantErr:      false,
		},
		{
			name:        "(P,S)=(76,70) and overflow at 3rd row because BitLen > 253",
			decimalType: "Decimal(76,70)",
			args: args{
				values: []interface{}{
					dec256_1,
					dec256_2,
					dec256_exceedbitlen,
					dec256_3,
				},
			},
			wantRowsRead: 2,
			wantErr:      true,
		},
		{
			name:        "(P,S)=(76,70) and overflow at 2nd row because Precision > 76",
			decimalType: "Decimal(76,70)",
			args: args{
				values: []interface{}{
					dec256_1,
					dec256_exceedprecision,
				},
			},
			wantRowsRead: 1,
			wantErr:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := MustMakeColumnData(tt.decimalType, 1000)

			got, err := col.ReadFromValues(tt.args.values)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadFromValues() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.wantRowsRead {
				t.Errorf("ReadFromValues() got = %v, wantRowsRead %v", got, tt.wantRowsRead)
			}

			for idx, wantStr := range tt.wantValueString {
				if !tt.wantErr {
					assert.Equal(t, wantStr, col.GetString(idx))
				}
			}
		})
	}
}

func TestDecimalColumnData_ReadFromTexts(t *testing.T) {
	type args struct {
		texts []string
	}
	tests := []struct {
		name        string
		args        args
		decimalType CHColumnType
		decimalWant struct {
			precision int
			scale     int
		}
		wantRawDataWritten []decimal.Decimal
		wantDataWritten    []string
		wantRowsRead       int
		wantErr            bool
	}{
		{
			name:        "Should write data and return number of rows read with no error, 2 rows",
			decimalType: "Decimal(18,5)",
			decimalWant: struct {
				precision int
				scale     int
			}{precision: 18, scale: 5},
			args: args{
				texts: []string{"", "null"},
			},
			wantDataWritten: []string{"0.00000", "0.00000"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:        "Should write data and return number of rows read with no error, 2 rows",
			decimalType: "Decimal(18,5)",
			decimalWant: struct {
				precision int
				scale     int
			}{precision: 18, scale: 5},
			args: args{
				texts: []string{"122.00000", "1220.00000"},
			},
			wantDataWritten: []string{"122.00000", "1220.00000"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:        "Should throw error if precision/scale not supported",
			decimalType: "Decimal(77,5)",
			decimalWant: struct {
				precision int
				scale     int
			}{precision: 77, scale: 5},
			args: args{
				texts: []string{"122.00000", "1220.00000"},
			},
			wantDataWritten: []string{"122.00000", "1220.00000"},
			wantRowsRead:    0,
			wantErr:         true,
		},
		{
			name:        "Should convert to scale specified, 2 rows",
			decimalType: "Decimal(18,0)",
			decimalWant: struct {
				precision int
				scale     int
			}{precision: 18, scale: 0},
			args: args{
				texts: []string{"122.123453232323", "122.123453232323898"},
			},
			wantDataWritten: []string{"122", "122"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:        "Should convert to scale specified, 2 rows",
			decimalType: "Decimal(2,0)",
			decimalWant: struct {
				precision int
				scale     int
			}{precision: 2, scale: 0},
			args: args{
				texts: []string{"122.123453232323", "122.123453232323898"},
			},
			wantDataWritten: []string{"122", "122"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:        "Should throw error if not decimal",
			decimalType: "Decimal(18,0)",
			decimalWant: struct {
				precision int
				scale     int
			}{precision: 18, scale: 0},
			args: args{
				texts: []string{"", "3.44"},
			},
			wantRawDataWritten: []decimal.Decimal{decimal.NewFromInt32(0)},
			wantRowsRead:       2,
			wantErr:            false,
		},
		{
			name:        "Should throw error if not decimal",
			decimalType: "Decimal(18,0)",
			decimalWant: struct {
				precision int
				scale     int
			}{precision: 18, scale: 0},
			args: args{
				texts: []string{"e"},
			},
			wantRowsRead: 0,
			wantErr:      true,
		},
		{
			name:        "(P,S)=(38,15) and not overflow",
			decimalType: "Decimal(38,15)",
			decimalWant: struct {
				precision int
				scale     int
			}{precision: 38, scale: 15},
			args: args{
				texts: []string{
					"99999999999999999999111.122228888777733",
					"9.012345678987654",
					"3.141592653589793",
					"99999999999999999999999.999999999999999",
					"-99999999999999999999999.999999999999999",
				},
			},
			wantDataWritten: []string{
				"99999999999999999999111.122228888777733",
				"9.012345678987654",
				"3.141592653589793",
				"99999999999999999999999.999999999999999",
				"-99999999999999999999999.999999999999999",
			},
			wantRowsRead: 5,
			wantErr:      false,
		},
		{
			name:        "(P,S)=(38,5) but overflow at 4th row because BitLen > 127",
			decimalType: "Decimal(38,5)",
			decimalWant: struct {
				precision int
				scale     int
			}{precision: 38, scale: 5},
			args: args{
				texts: []string{
					"999999999999999999999999999999999.99999",
					"-999999999999999999999999999999999.99999",
					"3.141592653589793",
					"3000000000000000000000000000000000.00001",
					"99999999999999999999111.122228888777733",
				},
			},
			wantRowsRead: 3,
			wantErr:      true,
		},
		{
			name:        "(P,S)=(38,5) but overflow at 4th row because precision > 38",
			decimalType: "Decimal(38,5)",
			decimalWant: struct {
				precision int
				scale     int
			}{precision: 38, scale: 5},
			args: args{
				texts: []string{
					"999999999999999999999999999999999.99999",
					"-999999999999999999999999999999999.99999",
					"3.141592653589793",
					"1000000000000000000000000000000000.0003",
					"99999999999999999999111.122228888777733",
				},
			},
			wantRowsRead: 3,
			wantErr:      true,
		},
		{
			name:        "(P,S)=(62,0) and not overflow",
			decimalType: "Decimal(62,0)",
			decimalWant: struct {
				precision int
				scale     int
			}{precision: 62, scale: 0},
			args: args{
				texts: []string{
					"-12345678901234567890123456789012345678901234567890123456789012",
					"12345678901234567890123456789012345678901234567890123456789012",
					"66",
					"-66",
					"0",
					"-0",
				},
			},
			wantDataWritten: []string{
				"-12345678901234567890123456789012345678901234567890123456789012",
				"12345678901234567890123456789012345678901234567890123456789012",
				"66",
				"-66",
				"0",
				"0",
			},
			wantRowsRead: 6,
			wantErr:      false,
		},
		{
			name:        "(P,S)=(45,13) and not overflow",
			decimalType: "Decimal(45,13)",
			decimalWant: struct {
				precision int
				scale     int
			}{precision: 45, scale: 13},
			args: args{
				texts: []string{
					"-123456789012345678901234567890123456789012345.4567890123456",
					"123456789012345678901234567890123456789012345.4567890123456",
					"600000000000000000000000000000000.0000000000006",
					"-600000000000000000000000000000000.0000000000006",
				},
			},
			wantDataWritten: []string{
				"-123456789012345678901234567890123456789012345.4567890123456",
				"123456789012345678901234567890123456789012345.4567890123456",
				"600000000000000000000000000000000.0000000000006",
				"-600000000000000000000000000000000.0000000000006",
			},
			wantRowsRead: 4,
			wantErr:      false,
		},
		{
			name:        "(P,S)=(59,58) and not overflow",
			decimalType: "Decimal(59,58)",
			decimalWant: struct {
				precision int
				scale     int
			}{precision: 59, scale: 58},
			args: args{
				texts: []string{
					"-8.0123456789012345678901234567890123456789012345678901234567",
					"8.0123456789012345678901234567890123456789012345678901234567",
					"0.0000000000000000000000000000000000000000000000000000000006",
					"-0.0000000000000000000000000000000000000000000000000000000006",
				},
			},
			wantDataWritten: []string{
				"-8.0123456789012345678901234567890123456789012345678901234567",
				"8.0123456789012345678901234567890123456789012345678901234567",
				"0.0000000000000000000000000000000000000000000000000000000006",
				"-0.0000000000000000000000000000000000000000000000000000000006",
			},
			wantRowsRead: 4,
			wantErr:      false,
		},
		{
			name:        "(P,S)=(76,0) and not overflow",
			decimalType: "Decimal(76,0)",
			decimalWant: struct {
				precision int
				scale     int
			}{precision: 76, scale: 0},
			args: args{
				texts: []string{
					"-1234567890123456789012345678901234567890123456789012345678901234567890123456",
					"1234567890123456789012345678901234567890123456789012345678901234567890123456",
					"600000000000000000000000000000000000000000000000000000000000006",
					"-600000000000000000000000000000000000000000000000000000000000006",
					"0",
					"-0",
				},
			},
			wantDataWritten: []string{
				"-1234567890123456789012345678901234567890123456789012345678901234567890123456",
				"1234567890123456789012345678901234567890123456789012345678901234567890123456",
				"600000000000000000000000000000000000000000000000000000000000006",
				"-600000000000000000000000000000000000000000000000000000000000006",
				"0",
				"0",
			},
			wantRowsRead: 6,
			wantErr:      false,
		},
		{
			name:        "(P,S)=(76,13) and not overflow",
			decimalType: "Decimal(76,13)",
			decimalWant: struct {
				precision int
				scale     int
			}{precision: 76, scale: 13},
			args: args{
				texts: []string{
					"-123456789012345678901234567890123456789012345678901234567890123.4567890123456",
					"123456789012345678901234567890123456789012345678901234567890123.4567890123456",
					"600000000000000000000000000000000000000000000000000000000000000.0000000000006",
					"-600000000000000000000000000000000000000000000000000000000000000.0000000000006",
				},
			},
			wantDataWritten: []string{
				"-123456789012345678901234567890123456789012345678901234567890123.4567890123456",
				"123456789012345678901234567890123456789012345678901234567890123.4567890123456",
				"600000000000000000000000000000000000000000000000000000000000000.0000000000006",
				"-600000000000000000000000000000000000000000000000000000000000000.0000000000006",
			},
			wantRowsRead: 4,
			wantErr:      false,
		},
		{
			name:        "(P,S)=(76,76) and not overflow",
			decimalType: "Decimal(76,76)",
			decimalWant: struct {
				precision int
				scale     int
			}{precision: 76, scale: 76},
			args: args{
				texts: []string{
					"-0.0123456789012345678901234567890123456789012345678901234567890123456789012345",
					"0.0123456789012345678901234567890123456789012345678901234567890123456789012345",
					"0.0000000000000000000000000000000000000000000000000000000000000000000000000006",
					"-0.0000000000000000000000000000000000000000000000000000000000000000000000000006",
				},
			},
			wantDataWritten: []string{
				"-0.0123456789012345678901234567890123456789012345678901234567890123456789012345",
				"0.0123456789012345678901234567890123456789012345678901234567890123456789012345",
				"0.0000000000000000000000000000000000000000000000000000000000000000000000000006",
				"-0.0000000000000000000000000000000000000000000000000000000000000000000000000006",
			},
			wantRowsRead: 4,
			wantErr:      false,
		},
		{
			name:        "(P,S)=(76,70) but overflow at 6th row because BitLen > 253",
			decimalType: "Decimal(76,70)",
			decimalWant: struct {
				precision int
				scale     int
			}{precision: 76, scale: 70},
			args: args{
				texts: []string{
					"999999.9999999999999999999999999999999999999999999999999999999999999999999999",
					"-999999.9999999999999999999999999999999999999999999999999999999999999999999999",
					"0",
					"-0",
					"3.141592653589793",
					"-1999999.123",
					"99999.122228888777733",
				},
			},
			wantRowsRead: 5,
			wantErr:      true,
		},
		{
			name:        "(P,S)=(76,70) but overflow at 6th row because precision > 76",
			decimalType: "Decimal(76,70)",
			decimalWant: struct {
				precision int
				scale     int
			}{precision: 76, scale: 70},
			args: args{
				texts: []string{
					"999999.9999999999999999999999999999999999999999999999999999999999999999999999",
					"-999999.9999999999999999999999999999999999999999999999999999999999999999999999",
					"0",
					"-0",
					"3.141592653589793",
					"-1000000.1",
					"99999.122228888777733",
				},
			},
			wantRowsRead: 5,
			wantErr:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := MustMakeColumnData(tt.decimalType, 1000)

			decimalCol, ok := i.(*DecimalColumnData)
			if assert.True(t, ok) {
				assert.Equal(t, tt.decimalWant.precision, decimalCol.precision)
				assert.Equal(t, tt.decimalWant.scale, decimalCol.scale)
			}

			got, err := i.ReadFromTexts(tt.args.texts)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadFromTexts() error = %v, wantErr = %v, got = %v", err, tt.wantErr, got)
				return
			}

			assert.Equal(t, got, tt.wantRowsRead)

			if len(tt.wantRawDataWritten) > 0 {
				for index, value := range tt.wantRawDataWritten {
					if !tt.wantErr {
						assert.Equal(t, value, i.GetValue(index))
					}
				}
				return
			}

			for index, value := range tt.wantDataWritten {
				if !tt.wantErr {
					assert.Equal(t, value, i.GetString(index))
				}
			}
		})
	}
}

func TestDecimalColumnData_EncoderDecoder(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		decimalType CHColumnType
		decimalWant struct {
			precision int
			scale     int
		}
		wantDataWritten []string
		wantRowsRead    int
		wantErr         bool
	}{
		{
			name:        "Should write data and return number of rows read with no error, 2 rows",
			decimalType: "Decimal(18,5)",
			decimalWant: struct {
				precision int
				scale     int
			}{precision: 18, scale: 5},
			args:            []string{"122.00000", "1220.00000"},
			wantDataWritten: []string{"122.00000", "1220.00000"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:        "Should convert to scale specified, 2 rows",
			decimalType: "Decimal(18,0)",
			decimalWant: struct {
				precision int
				scale     int
			}{precision: 18, scale: 0},
			args:            []string{"122.123453232323", "122.123453232323898"},
			wantDataWritten: []string{"122", "122"},
			wantRowsRead:    2,
			wantErr:         false,
		},
		{
			name:        "Should convert decimal(38,2) with 2 decimal digit",
			decimalType: "Decimal(38,2)",
			decimalWant: struct {
				precision int
				scale     int
			}{precision: 38, scale: 2},
			args:            []string{"9.99999999"},
			wantDataWritten: []string{"9.99"},
			wantRowsRead:    1,
			wantErr:         false,
		},
		{
			name:        "Should convert decimal(38,10) with 10 decimal digit",
			decimalType: "Decimal(38,10)",
			decimalWant: struct {
				precision int
				scale     int
			}{precision: 38, scale: 10},
			args:            []string{"3.141592653589793"},
			wantDataWritten: []string{"3.1415926535"},
			wantRowsRead:    1,
			wantErr:         false,
		},
		{
			name:        "Should convert decimal(38,38) with 38 decimal digit",
			decimalType: "Decimal(38,38)",
			decimalWant: struct {
				precision int
				scale     int
			}{precision: 38, scale: 38},
			args:            []string{"0.141592653589793"},
			wantDataWritten: []string{"0.14159265358979300000000000000000000000"},
			wantRowsRead:    1,
			wantErr:         false,
		},
		{
			name:        "Should convert decimal(38,38) with 38 decimal digit",
			decimalType: "Decimal(38,37)",
			decimalWant: struct {
				precision int
				scale     int
			}{precision: 38, scale: 37},
			args:            []string{"3.141592653589793"},
			wantDataWritten: []string{"3.1415926535897930000000000000000000000"},
			wantRowsRead:    1,
			wantErr:         false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buffer bytes.Buffer
			encoder := ch_encoding.NewEncoder(&buffer)
			decoder := ch_encoding.NewDecoder(&buffer)

			// Write to encoder
			original := MustMakeColumnData(tt.decimalType, len(tt.args))
			got, err := original.ReadFromTexts(tt.args)

			require.NoError(t, err)
			require.Equal(t, got, tt.wantRowsRead)
			require.NoError(t, err)

			err = original.WriteToEncoder(encoder)
			require.NoError(t, err)

			// Read from decoder
			newCopy := MustMakeColumnData(tt.decimalType, len(tt.args))
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

func BenchmarkDecimal(b *testing.B) {
	var buffer bytes.Buffer
	encoder := ch_encoding.NewEncoder(&buffer)
	decoder := ch_encoding.NewDecoder(&buffer)
	arrLen := 50_000_000
	decimalStr := makeRandomDecimalFloat(arrLen)
	b.ResetTimer() // Reset the benchmark timer

	for n := 0; n < b.N; n++ {

		original := MustMakeColumnData("Decimal(38,20)", arrLen)
		_, err := original.ReadFromValues(decimalStr)
		require.NoError(b, err)

		err = original.WriteToEncoder(encoder)
		require.NoError(b, err)

		newCopy := MustMakeColumnData("Decimal(38,20)", arrLen)
		err = newCopy.ReadFromDecoder(decoder)
		require.NoError(b, err)
	}
}

func makeRandomDecimalStrArray(len int) []string {
	arr := make([]string, len)
	for i := 0; i < len; i++ {
		// Generate a random float between 0 and 1
		randomFloat := rand.Float64()

		// Convert the random float to string with 4 decimal places
		randomString := strconv.FormatFloat(randomFloat, 'f', -1, 64)
		arr[i] = randomString
	}
	return arr
}

func makeRandomDecimalFloat(len int) []interface{} {
	arr := make([]interface{}, len)
	for i := 0; i < len; i++ {
		// Generate a random float between 0 and 1
		randomFloat := rand.Float64()
		arr[i] = decimal.NewFromFloat(randomFloat)
	}
	return arr
}
