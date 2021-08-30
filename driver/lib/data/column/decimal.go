package column

import (
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
	"github.com/bytehouse-cloud/driver-go/errors"
)

const (
	invalidDecimalString  = "invalid decimal string: %v"
	precisionNotSupported = "precision of %v not supported"
	unreachableExecution  = "unreachable execution path"
	unsupportedPrecision  = "unsupported decimal precision: %v"
)

var factors10 = []float64{
	1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10,
	1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18,
}

// DecimalColumnData represents Decimal(P, S) in Clickhouse.
// Decimals are signed fixed-point numbers that keep precision during add, subtract and multiply operations. For division least significant digits are discarded (not rounded).
// See https://clickhouse.tech/docs/en/sql-reference/data-types/decimal/
type DecimalColumnData struct {
	precision   int // Valid range: [1:18]. Precision is the number of digits in a number.
	scale       int // Valid range: [0:P]. Scale is the number of digits to the right of the decimal point in a number.
	byteCount   int
	raw         []byte
	fmtTemplate string
}

func (d *DecimalColumnData) ReadFromDecoder(decoder *ch_encoding.Decoder) error {
	if d.precision > 18 {
		return errors.ErrorfWithCaller(unsupportedPrecision, d.precision)
	}

	_, err := decoder.Read(d.raw)
	return err
}

func (d *DecimalColumnData) WriteToEncoder(encoder *ch_encoding.Encoder) error {
	if d.precision > 18 {
		return errors.ErrorfWithCaller(unsupportedPrecision, d.precision)
	}

	_, err := encoder.Write(d.raw)
	return err
}

func (d *DecimalColumnData) ReadFromValues(values []interface{}) (int, error) {
	if len(values) == 0 {
		return 0, nil
	}

	if d.byteCount > 8 {
		return 0, errors.ErrorfWithCaller(precisionNotSupported, d.precision)
	}

	var (
		f   float64
		ok  bool
		err error
	)

	readDecimalFunc, err := interpretDecimalType(values[0])
	if err != nil {
		return 0, err
	}

	for i, value := range values {
		f, ok = readDecimalFunc(value)
		if !ok {
			return i, NewErrInvalidColumnType(value, f)
		}

		d.putFloat(d.raw[i*d.byteCount:], f)
	}

	return len(values), nil
}

func (d *DecimalColumnData) ReadFromTexts(texts []string) (int, error) {
	if d.byteCount > 8 {
		return 0, errors.ErrorfWithCaller(precisionNotSupported, d.precision)
	}

	var (
		f   float64
		err error
	)
	for i, text := range texts {
		if text == "" {
			d.putFloat(d.raw[i*d.byteCount:], 0)
			continue
		}

		if f, err = strconv.ParseFloat(text, d.byteCount*8); err != nil {
			return i, errors.ErrorfWithCaller(invalidDecimalString, text)
		}
		d.putFloat(d.raw[i*d.byteCount:], f)
	}
	return len(texts), nil
}

// get returns value at row with correct scale
// Note: get doesn't handle parsing into the correct precision. This is handle by the database
func (d *DecimalColumnData) get(row int) float64 {
	switch d.byteCount {
	case 4:
		return float64(int32(bufferRowToUint32(d.raw, row))) / factors10[d.scale]
	case 8:
		return float64(int64(bufferRowToUint64(d.raw, row))) / factors10[d.scale]
	default:
		panic(unreachableExecution)
	}
}

func (d *DecimalColumnData) GetValue(row int) interface{} {
	return d.get(row)
}

func (d *DecimalColumnData) GetString(row int) string {
	return fmt.Sprintf(d.fmtTemplate, d.get(row))
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

func (d *DecimalColumnData) putFloat(buf []byte, f float64) {
	switch d.byteCount {
	case 4:
		binary.LittleEndian.PutUint32(buf, uint32(f*factors10[d.scale]))
	case 8:
		binary.LittleEndian.PutUint64(buf, uint64(f*factors10[d.scale]))
	default:
		panic(errors.ErrorfWithCaller(unreachableExecution))
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

// interpretDecimalType converts decimals to float64
// interpretDecimalType returns a function to avoid unnecessary switch case computation
// implicitly assumes that all values follow the type of values[0]
func interpretDecimalType(originalValue interface{}) (func(value interface{}) (float64, bool), error) {
	switch value := originalValue.(type) {
	case float64:
		return func(value interface{}) (float64, bool) {
			// Have to recompute value of ok even if match to ensure that subsequent values matches same type
			v, ok := value.(float64)
			return v, ok
		}, nil
	case float32:
		return func(value interface{}) (float64, bool) {
			v, ok := value.(float32)
			return float64(v), ok
		}, nil
	case int:
		return func(value interface{}) (float64, bool) {
			// Have to recompute value of ok even if match to ensure that subsequent values matches same type
			v, ok := value.(int)
			return float64(v), ok
		}, nil
	case int64:
		return func(value interface{}) (float64, bool) {
			// Have to recompute value of ok even if match to ensure that subsequent values matches same type
			v, ok := value.(int64)
			return float64(v), ok
		}, nil
	case int32:
		return func(value interface{}) (float64, bool) {
			v, ok := value.(int32)
			return float64(v), ok
		}, nil
	case int16:
		return func(value interface{}) (float64, bool) {
			v, ok := value.(int16)
			return float64(v), ok
		}, nil
	case int8:
		return func(value interface{}) (float64, bool) {
			v, ok := value.(int8)
			return float64(v), ok
		}, nil
	default:
		return nil, NewErrInvalidColumnType(value, float64(0))
	}
}
