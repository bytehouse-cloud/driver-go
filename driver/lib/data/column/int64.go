package column

import (
	"encoding/binary"
	"strconv"

	"github.com/valyala/fastjson/fastfloat"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

const int64ByteSize = 8

type Int64ColumnData struct {
	raw      []byte
	isClosed bool
}

func (i *Int64ColumnData) ReadFromDecoder(decoder *ch_encoding.Decoder) error {
	_, err := decoder.Read(i.raw)
	return err
}

func (i *Int64ColumnData) WriteToEncoder(encoder *ch_encoding.Encoder) error {
	_, err := encoder.Write(i.raw)
	return err
}

func (i *Int64ColumnData) ReadFromValues(values []interface{}) (int, error) {
	if len(values) == 0 {
		return 0, nil
	}

	var (
		v             int64
		ok            bool
		err           error
		readInt64Func func(value interface{}) (int64, bool)
	)

	for idx, value := range values {
		if value == nil {
			binary.LittleEndian.PutUint64(i.raw[idx*int64ByteSize:], 0)
			continue
		}

		if readInt64Func == nil {
			readInt64Func, err = interpretInt64Type(value)
			if err != nil {
				return 0, err
			}
		}

		v, ok = readInt64Func(value)
		if !ok {
			return idx, NewErrInvalidColumnType(value, v)
		}

		binary.LittleEndian.PutUint64(i.raw[idx*int64ByteSize:], uint64(v))
	}

	return len(values), nil
}

func (i *Int64ColumnData) ReadFromTexts(texts []string) (int, error) {
	var (
		v   int64
		err error
	)

	for idx, text := range texts {
		if isEmptyOrNull(text) {
			binary.LittleEndian.PutUint64(i.raw[idx*int64ByteSize:], 0)
			continue
		}

		if v, err = fastfloat.ParseInt64(text); err != nil {
			return idx, err
		}
		binary.LittleEndian.PutUint64(i.raw[idx*int64ByteSize:], uint64(v))
	}
	return len(texts), nil
}

func (i *Int64ColumnData) get(row int) int64 {
	return int64(bufferRowToUint64(i.raw, row))
}

func (i *Int64ColumnData) GetValue(row int) interface{} {
	return i.get(row)
}

func (i *Int64ColumnData) GetString(row int) string {
	return strconv.FormatInt(i.get(row), 10)
}

func (i *Int64ColumnData) Zero() interface{} {
	return int64(0)
}

func (i *Int64ColumnData) ZeroString() string {
	return zeroString
}

func (i *Int64ColumnData) Len() int {
	return len(i.raw) / 8
}

func (i *Int64ColumnData) Close() error {
	if i.isClosed {
		return nil
	}
	i.isClosed = true
	bytepool.PutBytes(i.raw)
	return nil
}

// interpretInt64Type converts subsets of int64 to int64
// interpretInt64Type returns a function to avoid unnecessary switch case computation
// implicitly assumes that all values follow the type of values[0]
func interpretInt64Type(originalValue interface{}) (func(value interface{}) (int64, bool), error) {
	switch value := originalValue.(type) {
	case int64:
		return func(value interface{}) (int64, bool) {
			// Have to recompute value of ok even if match to ensure that subsequent values matches same type
			v, ok := value.(int64)
			return v, ok
		}, nil
	case int:
		return func(value interface{}) (int64, bool) {
			// Have to recompute value of ok even if match to ensure that subsequent values matches same type
			v, ok := value.(int)
			return int64(v), ok
		}, nil
	case int32:
		return func(value interface{}) (int64, bool) {
			v, ok := value.(int32)
			return int64(v), ok
		}, nil
	case int16:
		return func(value interface{}) (int64, bool) {
			v, ok := value.(int16)
			return int64(v), ok
		}, nil
	case int8:
		return func(value interface{}) (int64, bool) {
			v, ok := value.(int8)
			return int64(v), ok
		}, nil
	default:
		return nil, NewErrInvalidColumnType(value, int64(0))
	}
}
