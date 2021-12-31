package column

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"strconv"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
	"github.com/bytehouse-cloud/driver-go/errors"
)

const (
	invalidDecimalStringErr  = "invalid decimal string: %v"
	precisionNotSupportedErr = "precision of %v not supported"
	unreachableExecutionErr  = "unreachable execution path"
	unsupportedPrecisionErr  = "unsupported decimal precision: %v"
)

const (
	maxSupportedDecimalBytes     = 16
	maxSupportedDecimalPrecision = 38
	maxMantissaBit128Precision   = 120
)

var factors10 = []float64{
	1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10,
	1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19, 1e20,
}

// DecimalColumnData represents Decimal(P, S) in Clickhouse.
// Decimals are fixed-point numbers that preserve precision for add, sub and mul operations.
//
// For division least significant digits are discarded (not rounded).
// See https://clickhouse.tech/docs/en/sql-reference/data-types/decimal
type DecimalColumnData struct {
	precision   int // support up to 38 digits
	scale       int // support up to 38 decimal values.
	byteCount   int
	raw         []byte
	fmtTemplate string
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
		if text == "" {
			_ = d.putDecimalIntoBytes(i, 0.0)
			continue
		}

		// Attempt parsing the float
		v, err := d.parseDecimal(text)
		if err != nil {
			return i, errors.ErrorfWithCaller(invalidDecimalStringErr, text)
		}

		// Put it into little-endian bytes
		if err := d.putDecimalIntoBytes(i, v); err != nil {
			return i, err
		}
	}

	return len(texts), nil
}

func (d *DecimalColumnData) GetValue(row int) interface{} {
	switch d.byteCount {
	case 4:
		return d.decimal32ToFloat64(row)
	case 8:
		return d.decimal64ToFloat64(row)
	case 16:
		return d.decimal128ToBigFloat(row)
	default:
		panic(unreachableExecutionErr)
	}
}

func (d *DecimalColumnData) GetString(row int) string {
	switch d.byteCount {
	case 4:
		v := d.decimal32ToFloat64(row)
		return fmt.Sprintf(d.fmtTemplate, v)
	case 8:
		v := d.decimal64ToFloat64(row)
		return fmt.Sprintf(d.fmtTemplate, v)
	case 16:
		v := d.decimal128ToBigFloat(row)
		return v.Text('f', d.scale)
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
	bytepool.PutBytes(d.raw)
	return nil
}

func (d *DecimalColumnData) decimal32ToFloat64(row int) float64 {
	n := bufferRowToUint32(d.raw, row)
	val := float64(n)
	return val / factors10[d.scale]
}

func (d *DecimalColumnData) decimal64ToFloat64(row int) float64 {
	n := bufferRowToUint64(d.raw, row)
	val := float64(n)
	return val / factors10[d.scale]
}

func (d *DecimalColumnData) decimal128ToBigFloat(row int) *big.Float {
	// The bytes sent is a 16 bytes int128 in
	// little big endian
	bytes := d.raw[row*16 : (row+1)*16]

	// Reverse to make it big endian, since big
	// pkg uses big endian
	for i, j := 0, len(bytes)-1; i < j; i, j = i+1, j-1 {
		bytes[i], bytes[j] = bytes[j], bytes[i]
	}

	bi := big.NewInt(0).SetBytes(bytes)
	bf := new(big.Float).SetInt(bi)

	factor10 := big.NewFloat(math.Pow10(d.scale))
	bf = bf.Quo(bf, factor10) // division by scale factor

	return bf
}

func (d *DecimalColumnData) parseDecimal(s string) (interface{}, error) {
	switch d.byteCount {
	case 16:
		v, _, err := big.ParseFloat(s, 10, maxMantissaBit128Precision, big.ToNearestEven)
		if err != nil {
			return nil, err
		}
		return v, nil
	default:
		f64, err := strconv.ParseFloat(s, d.byteCount*8)
		if err != nil {
			return nil, err
		}
		return f64, nil
	}
}

func (d *DecimalColumnData) putDecimalIntoBytes(i int, decimal interface{}) error {
	switch d.byteCount {
	case 4:
		v, err := d.getDecimalToFloat64(decimal)
		if err != nil {
			return err
		}

		x := uint32(v * factors10[d.scale])           // apply scale factor
		binary.LittleEndian.PutUint32(d.raw[i*4:], x) // serialize to bytes

	case 8:
		v, err := d.getDecimalToFloat64(decimal)
		if err != nil {
			return err
		}

		x := uint64(v * factors10[d.scale])           // apply scale factor
		binary.LittleEndian.PutUint64(d.raw[i*8:], x) // serialize to bytes

	case 16:
		v, err := d.getDecimalToBigFloat(decimal)
		if err != nil {
			return err
		}

		factor := new(big.Float).SetFloat64(factors10[d.scale])

		v = v.Mul(v, factor) // apply scale factor
		z := new(big.Int)    // serialize as 16-byte integer (int128)
		v.Int(z)

		// NOTE: can panic if value of z >= 10^39 - 1.
		// For value beyond this threshold, it needs more than 16 bytes.
		buff := d.raw[i*16 : (i+1)*16]
		z.FillBytes(buff)

		// Reverse byte-by-byte from big to little endian
		for i, j := 0, len(buff)-1; i < j; i, j = i+1, j-1 {
			buff[i], buff[j] = buff[j], buff[i]
		}

	default:
		panic(errors.ErrorfWithCaller(unreachableExecutionErr))
	}

	return nil
}

func (d *DecimalColumnData) getDecimalToFloat64(decimal interface{}) (float64, error) {
	switch v := decimal.(type) {
	case float64:
		return float64(v), nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	default:
		return 0.0, NewErrInvalidColumnType(v, 0.0)
	}
}

func (d *DecimalColumnData) getDecimalToBigFloat(decimal interface{}) (*big.Float, error) {
	switch v := decimal.(type) {
	case *big.Float:
		return v, nil

	case *big.Int:
		return new(big.Float).SetInt(v), nil

	default:
		f64, err := d.getDecimalToFloat64(decimal)
		if err != nil {
			return nil, err
		}

		return big.NewFloat(f64), nil
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

func makeDecimalFmtTemplate(scale int) string {
	return "%" + fmt.Sprintf(".%vf", scale)
}
