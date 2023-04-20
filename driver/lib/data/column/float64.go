package column

import (
	"encoding/binary"
	"math"
	"strconv"

	"github.com/valyala/fastjson/fastfloat"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

// defaultFloatFormat uses one of the format specified below:
// 'b' (-ddddp±ddd, a binary exponent),
// 'e' (-d.dddde±dd, a decimal exponent),
// 'E' (-d.ddddE±dd, a decimal exponent),
// 'f' (-ddd.dddd, no exponent),
// 'g' ('e' for large exponents, 'f' otherwise),
// 'G' ('E' for large exponents, 'f' otherwise),
// 'x' (-0xd.ddddp±ddd, a hexadecimal fraction and binary exponent), or
// 'X' (-0Xd.ddddP±ddd, a hexadecimal fraction and binary exponent).
const defaultFloatFormat = 'g'
const float64ByteSize = 8

type Float64ColumnData struct {
	raw      []byte
	isClosed bool
}

func (f *Float64ColumnData) ReadFromDecoder(decoder *ch_encoding.Decoder) error {
	_, err := decoder.Read(f.raw)
	return err
}

func (f *Float64ColumnData) WriteToEncoder(encoder *ch_encoding.Encoder) error {
	_, err := encoder.Write(f.raw)
	return err
}

func (f *Float64ColumnData) ReadFromValues(values []interface{}) (int, error) {
	if len(values) == 0 {
		return 0, nil
	}

	var (
		v   float64
		ok  bool
		err error
	)

	readFloat64Func, err := interpretFloat64Type(values[0])
	if err != nil {
		return 0, err
	}

	for idx, value := range values {
		v, ok = readFloat64Func(value)
		if !ok {
			return idx, NewErrInvalidColumnType(value, v)
		}

		binary.LittleEndian.PutUint64(f.raw[idx*float64ByteSize:], math.Float64bits(v))
	}

	return len(values), nil
}

func (f *Float64ColumnData) ReadFromTexts(texts []string) (int, error) {
	var (
		v   float64
		err error
	)

	for i, text := range texts {
		if text == "" {
			binary.LittleEndian.PutUint64(f.raw[i*float64ByteSize:], math.Float64bits(0))
			continue
		}

		v, err = fastfloat.Parse(text)
		if err != nil {
			return i, err
		}
		binary.LittleEndian.PutUint64(f.raw[i*float64ByteSize:], math.Float64bits(v))
	}
	return len(texts), nil
}

func (f *Float64ColumnData) get(row int) float64 {
	return math.Float64frombits(bufferRowToUint64(f.raw, row))
}

func (f *Float64ColumnData) GetValue(row int) interface{} {
	return f.get(row)
}

func (f *Float64ColumnData) GetString(row int) string {
	return strconv.FormatFloat(f.get(row), defaultFloatFormat, -1, 64)
}

func (f *Float64ColumnData) Zero() interface{} {
	return float64(0)
}

func (f *Float64ColumnData) ZeroString() string {
	return zeroString
}

func (f *Float64ColumnData) Len() int {
	return len(f.raw) / 4
}

func (f *Float64ColumnData) Close() error {
	if f.isClosed {
		return nil
	}
	f.isClosed = true
	bytepool.PutBytes(f.raw)
	return nil
}

// interpretFloat64Type converts subsets of float64 to float64
// interpretFloat64Type returns a function to avoid unnecessary switch case computation
// implicitly assumes that all values follow the type of values[0]
func interpretFloat64Type(originalValue interface{}) (func(value interface{}) (float64, bool), error) {
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
	default:
		return nil, NewErrInvalidColumnType(value, float64(0))
	}
}
