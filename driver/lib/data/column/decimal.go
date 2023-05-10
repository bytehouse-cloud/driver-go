package column

import (
	"encoding/binary"
	"math/big"
	"strconv"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
	"github.com/bytehouse-cloud/driver-go/errors"
	"github.com/shopspring/decimal"
)

const (
	invalidDecimalStringErr  = "invalid decimal string: %v"
	precisionNotSupportedErr = "precision of %v not supported"
	unreachableExecutionErr  = "unreachable execution path"
	unsupportedPrecisionErr  = "unsupported decimal precision: %v"
	valueOverFlowsErr        = "decimal value overflow because exceed precision=%v"
)

const (
	maxSupportedDecimalBytes     = 32
	maxSupportedDecimalPrecision = 76

	maxBitLenDecimal128 = 127 // corresponding to largest Int128 with 38 digits
	maxBitLenDecimal256 = 253 // corresponding to largest Int256 with 76 digits
)

var (
	biggestInt128With38Digits, _ = new(big.Int).SetString("99999999999999999999999999999999999999", 10)
	biggestInt256With76Digits, _ = new(big.Int).SetString("9999999999999999999999999999999999999999999999999999999999999999999999999999", 10)
)

// DecimalColumnData represents Decimal(P, S) in Clickhouse.
// Decimals are fixed-point numbers that preserve precision for add, sub and mul operations.
//
// For division least significant digits are discarded (not rounded).
// See https://clickhouse.tech/docs/en/sql-reference/data-types/decimal
type DecimalColumnData struct {
	precision int
	scale     int
	byteCount int
	raw       []byte
	isClosed  bool
}

func (d *DecimalColumnData) ReadFromDecoder(decoder *ch_encoding.Decoder) error {
	if d.precision > maxSupportedDecimalPrecision {
		return errors.ErrorfWithCaller(unsupportedPrecisionErr, d.precision)
	}

	_, err := decoder.Read(d.raw)

	return err
}

func (d *DecimalColumnData) WriteToEncoder(encoder *ch_encoding.Encoder) error {
	if d.precision > maxSupportedDecimalPrecision {
		return errors.ErrorfWithCaller(unsupportedPrecisionErr, d.precision)
	}

	_, err := encoder.Write(d.raw)

	return err
}

func (d *DecimalColumnData) ReadFromValues(values []interface{}) (int, error) {
	if len(values) == 0 {
		return 0, nil
	}

	if d.byteCount > maxSupportedDecimalBytes {
		return 0, errors.ErrorfWithCaller(precisionNotSupportedErr, d.precision)
	}

	for i, value := range values {
		if value == nil {
			_ = d.putDecimalIntoBytes(i, 0.0)
			continue
		}

		if err := d.putDecimalIntoBytes(i, value); err != nil {
			return i, err
		}
	}

	return len(values), nil
}

func (d *DecimalColumnData) ReadFromTexts(texts []string) (int, error) {
	if d.byteCount > maxSupportedDecimalBytes {
		return 0, errors.ErrorfWithCaller(precisionNotSupportedErr, d.precision)
	}

	for i, text := range texts {
		if isEmptyOrNull(text) {
			_ = d.putDecimalIntoBytes(i, 0.0)
			continue
		}

		// Attempt parsing to decimal
		v, err := d.parseDecimal(text)
		if err != nil {
			return i, errors.ErrorfWithCaller(invalidDecimalStringErr, text)
		}

		// Put it into little-endian bytes
		if err = d.putDecimalIntoBytes(i, v); err != nil {
			return i, err
		}
	}

	return len(texts), nil
}

func (d *DecimalColumnData) GetValue(row int) interface{} {
	switch d.byteCount {
	case 4:
		return d.decimal32ToDecimal(row)
	case 8:
		return d.decimal64ToDecimal(row)
	case 16:
		return d.decimal128ToDecimal(row)
	case 32:
		return d.decimal256ToDecimal(row)
	default:
		panic(unreachableExecutionErr)
	}
}

func (d *DecimalColumnData) GetString(row int) string {
	switch d.byteCount {
	case 4:
		v := d.decimal32ToDecimal(row)
		return v.StringFixed(int32(d.scale))
	case 8:
		v := d.decimal64ToDecimal(row)
		return v.StringFixed(int32(d.scale))
	case 16:
		v := d.decimal128ToDecimal(row)
		return v.StringFixed(int32(d.scale))
	case 32:
		v := d.decimal256ToDecimal(row)
		return v.StringFixed(int32(d.scale))
	default:
		return "0.0"
	}
}

func (d *DecimalColumnData) Zero() interface{} {
	return float64(0)
}

func (d *DecimalColumnData) ZeroString() string {
	return zeroString
}

func (d *DecimalColumnData) Len() int {
	return len(d.raw) / d.byteCount
}

func (d *DecimalColumnData) Close() error {
	if d.isClosed {
		return nil
	}
	d.isClosed = true
	bytepool.PutBytes(d.raw)
	return nil
}

func (d *DecimalColumnData) decimal32ToDecimal(row int) decimal.Decimal {
	n := bufferRowToUint32(d.raw, row)
	/*
		(1) For Decimal32, Clickhouse Server send us 4 bytes representing Int32 (what Server stores internally)
		(2) int32 and uint32 have same bit-representation in memory so when we read UInt32,
		we can use type conversion to get the original Int32 that Server sends.
		Unsafe.Pointer is another way to convert one type to another type having similar memory layout
	*/
	return decimal.New(int64(int32(n)), int32(-d.scale))
}

func (d *DecimalColumnData) decimal64ToDecimal(row int) decimal.Decimal {
	/*
		(1) For Decimal64, Clickhouse Server send us 8 bytes representing Int64 (what Server stores internally)
		(2) int64 and uint64 have same bit-representation in memory so when we read UInt64,
		we can use type conversion to get the original Int64 that Server sends.
		Unsafe.Pointer is another way to convert one type to another type having similar memory layout
	*/
	n := bufferRowToUint64(d.raw, row)
	return decimal.New(int64(n), int32(-d.scale))
}

func (d *DecimalColumnData) decimal128ToDecimal(row int) decimal.Decimal {
	// For Decimal128, Clickhouse Server send us 16 bytes representing Int128 (what Server stores internally)
	bytes := d.raw[row*16 : (row+1)*16]
	bi := rawToBigInt(bytes, true)
	return decimal.NewFromBigInt(bi, -int32(d.scale))
}

func (d *DecimalColumnData) decimal256ToDecimal(row int) decimal.Decimal {
	// For Decimal128, Clickhouse Server send us 16 bytes representing Int128 (what Server stores internally)
	bytes := d.raw[row*32 : (row+1)*32]
	bi := rawToBigInt(bytes, true)
	return decimal.NewFromBigInt(bi, -int32(d.scale))
}

func (d *DecimalColumnData) parseDecimal(s string) (decimal.Decimal, error) {
	v, err := decimal.NewFromString(s)
	if err != nil {
		return decimal.Decimal{}, err
	}
	return v, nil
}

func (d *DecimalColumnData) putDecimalIntoBytes(i int, val interface{}) error {
	switch d.byteCount {
	case 4: // Decimal32
		v, err := d.getValToDecimal(val)
		if err != nil {
			return err
		}
		part := v.Shift(int32(d.scale)).IntPart()

		// Check for overflow, golang conversion always yields a valid value with no indication of overflow.
		// inline overflow checking reference: https://groups.google.com/g/golang-nuts/c/kPr7wZTAQM4
		a := int32(part)
		if int64(a) != part {
			return errors.ErrorfWithCaller(valueOverFlowsErr, d.precision)
		}

		binary.LittleEndian.PutUint32(d.raw[i*4:], uint32(part)) // serialize to bytes
	case 8: // Decimal64
		v, err := d.getValToDecimal(val)
		if err != nil {
			return err
		}

		// Check for overflow, golang conversion always yields a valid value with no indication of overflow.
		bi := v.Shift(int32(d.scale)).BigInt()
		if !bi.IsInt64() {
			return errors.ErrorfWithCaller(valueOverFlowsErr, d.precision)
		}
		binary.LittleEndian.PutUint64(d.raw[i*8:], uint64(bi.Int64())) // serialize to bytes
	case 16: // Decimal128
		v, err := d.getValToDecimal(val)
		if err != nil {
			return err
		}

		bi := v.Shift(int32(d.scale)).BigInt()

		// the conditional structure is for optimization purpose
		// comparing big.Int is a costly operation
		// while calling BitLen is very fast
		if bi.BitLen() > maxBitLenDecimal128 || (bi.BitLen() == maxBitLenDecimal128 && new(big.Int).Abs(bi).Cmp(biggestInt128With38Digits) > 0) {
			return errors.ErrorfWithCaller(valueOverFlowsErr, d.precision)
		}

		putBigIntLittleEndianImpl(d.raw[i*16:(i+1)*16], bi)
	case 32: // Decimal256
		v, err := d.getValToDecimal(val)
		if err != nil {
			return err
		}

		bi := v.Shift(int32(d.scale)).BigInt()

		// the conditional structure is for optimization purpose
		// comparing big.Int is a costly operation
		// while calling BitLen is very fast
		if bi.BitLen() > maxBitLenDecimal256 || (bi.BitLen() == maxBitLenDecimal256 && new(big.Int).Abs(bi).Cmp(biggestInt256With76Digits) > 0) {
			return errors.ErrorfWithCaller(valueOverFlowsErr, d.precision)
		}

		putBigIntLittleEndianImpl(d.raw[i*32:(i+1)*32], bi)
	default:
		panic(errors.ErrorfWithCaller(unreachableExecutionErr))
	}

	return nil
}

func (d *DecimalColumnData) getValToDecimal(val interface{}) (decimal.Decimal, error) {
	switch v := val.(type) {
	case int: // at least 32 bits in size, It is a distinct type, however, and not an alias for, say, int32.
		return decimal.NewFromInt(int64(v)), nil
	case int8:
		return decimal.NewFromInt32(int32(v)), nil
	case int16:
		return decimal.NewFromInt32(int32(v)), nil
	case int32:
		return decimal.NewFromInt32(v), nil
	case int64:
		return decimal.NewFromInt(v), nil
	case uint: // cast up
		return decimal.NewFromInt(int64(v)), nil
	case uint8: // cast up
		return decimal.NewFromInt32(int32(v)), nil
	case uint16: // cast up
		return decimal.NewFromInt32(int32(v)), nil
	case uint32: // cast up
		return decimal.NewFromInt(int64(v)), nil
	case uint64: // cast up
		return decimal.NewFromString(strconv.FormatUint(v, 10))
	case float64:
		return decimal.NewFromFloat(v), nil
	case float32:
		return decimal.NewFromFloat32(v), nil
	case big.Int:
		return decimal.NewFromString(v.String())
	case decimal.Decimal:
		return v, nil
	case *big.Float:
		return decimal.NewFromString(v.String())
	case *big.Int:
		return decimal.NewFromString(v.String())
	case *decimal.Decimal:
		return *v, nil
	default:
		return decimal.Decimal{}, NewErrInvalidColumnType(v, 0.0)
	}
}

func getByteCountFromPrecision(p int) int {
	var result = 4
	if p <= 9 {
		return result
	}
	result <<= 1
	if p <= 18 {
		return result
	}
	result <<= 1
	if p <= 38 {
		return result
	}
	result <<= 1
	if p <= 76 {
		return result
	}
	return result << 1
}
