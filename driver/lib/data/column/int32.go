package column

import (
	"encoding/binary"
	"strconv"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

const int32ByteSize = 4

type Int32ColumnData struct {
	raw      []byte
	isClosed bool
}

func (i *Int32ColumnData) ReadFromDecoder(decoder *ch_encoding.Decoder) error {
	_, err := decoder.Read(i.raw)
	return err
}

func (i *Int32ColumnData) WriteToEncoder(encoder *ch_encoding.Encoder) error {
	_, err := encoder.Write(i.raw)
	return err
}

func (i *Int32ColumnData) ReadFromValues(values []interface{}) (int, error) {
	if len(values) == 0 {
		return 0, nil
	}

	var (
		v             int32
		ok            bool
		err           error
		readInt32Func func(value interface{}) (int32, bool)
	)

	for idx, value := range values {
		if value == nil {
			binary.LittleEndian.PutUint32(i.raw[idx*int32ByteSize:], 0)
			continue
		}

		if readInt32Func == nil {
			readInt32Func, err = interpretInt32Type(value)
			if err != nil {
				return 0, err
			}
		}

		v, ok = readInt32Func(value)
		if !ok {
			return idx, NewErrInvalidColumnType(value, v)
		}

		binary.LittleEndian.PutUint32(i.raw[idx*int32ByteSize:], uint32(v))
	}

	return len(values), nil
}

func (i *Int32ColumnData) ReadFromTexts(texts []string) (int, error) {
	var (
		v   int64
		err error
	)

	for idx, text := range texts {
		if isEmptyOrNull(text) {
			binary.LittleEndian.PutUint32(i.raw[idx*int32ByteSize:], 0)
			continue
		}

		if v, err = strconv.ParseInt(text, 10, 32); err != nil {
			return idx, err
		}
		binary.LittleEndian.PutUint32(i.raw[idx*int32ByteSize:], uint32(v))
	}
	return len(texts), nil
}

func (i *Int32ColumnData) get(row int) int32 {
	return int32(bufferRowToUint32(i.raw, row))
}

func (i *Int32ColumnData) GetValue(row int) interface{} {
	return i.get(row)
}

func (i *Int32ColumnData) GetString(row int) string {
	return strconv.FormatInt(int64(i.get(row)), 10)
}

func (i *Int32ColumnData) Zero() interface{} {
	return int32(0)
}

func (i *Int32ColumnData) ZeroString() string {
	return zeroString
}

func (i *Int32ColumnData) Len() int {
	return len(i.raw) / 4
}

func (i *Int32ColumnData) Close() error {
	if i.isClosed {
		return nil
	}
	i.isClosed = true
	bytepool.PutBytes(i.raw)
	return nil
}

// interpretInt32Type converts subsets of int32 to int32
// interpretInt32Type returns a function to avoid unnecessary switch case computation
// implicitly assumes that all values follow the type of values[0]
func interpretInt32Type(originalValue interface{}) (func(value interface{}) (int32, bool), error) {
	switch value := originalValue.(type) {
	case int32:
		return func(value interface{}) (int32, bool) {
			// Have to recompute value of ok even if match to ensure that subsequent values matches same type
			v, ok := value.(int32)
			return v, ok
		}, nil
	case int16:
		return func(value interface{}) (int32, bool) {
			v, ok := value.(int16)
			return int32(v), ok
		}, nil
	case int8:
		return func(value interface{}) (int32, bool) {
			v, ok := value.(int8)
			return int32(v), ok
		}, nil
	default:
		return nil, NewErrInvalidColumnType(value, int32(0))
	}
}
