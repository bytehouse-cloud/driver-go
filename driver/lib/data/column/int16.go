package column

import (
	"encoding/binary"
	"strconv"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

const int16ByteSize = 2

type Int16ColumnData struct {
	raw      []byte
	isClosed bool
}

func (i *Int16ColumnData) ReadFromDecoder(decoder *ch_encoding.Decoder) error {
	_, err := decoder.Read(i.raw)
	return err
}

func (i *Int16ColumnData) WriteToEncoder(encoder *ch_encoding.Encoder) error {
	_, err := encoder.Write(i.raw)
	return err
}

func (i *Int16ColumnData) ReadFromValues(values []interface{}) (int, error) {
	if len(values) == 0 {
		return 0, nil
	}

	var (
		v             int16
		ok            bool
		err           error
		readInt16Func func(value interface{}) (int16, bool)
	)

	for idx, value := range values {
		if value == nil {
			binary.LittleEndian.PutUint16(i.raw[idx*int16ByteSize:], 0)
			continue
		}

		if readInt16Func == nil {
			readInt16Func, err = interpretInt16Type(value)
			if err != nil {
				return 0, err
			}
		}

		v, ok = readInt16Func(value)
		if !ok {
			return idx, NewErrInvalidColumnType(value, v)
		}

		binary.LittleEndian.PutUint16(i.raw[idx*int16ByteSize:], uint16(v))
	}

	return len(values), nil
}

func (i *Int16ColumnData) ReadFromTexts(texts []string) (int, error) {
	var (
		v   int64
		err error
	)

	for idx, text := range texts {
		if isEmptyOrNull(text) {
			binary.LittleEndian.PutUint16(i.raw[idx*int16ByteSize:], 0)
			continue
		}

		if v, err = strconv.ParseInt(text, 10, 16); err != nil {
			return idx, err
		}
		binary.LittleEndian.PutUint16(i.raw[idx*int16ByteSize:], uint16(v))
	}
	return len(texts), nil
}

func (i *Int16ColumnData) get(row int) int16 {
	return int16(bufferRowToUint16(i.raw, row))
}

func (i *Int16ColumnData) GetValue(row int) interface{} {
	return i.get(row)
}

func (i *Int16ColumnData) GetString(row int) string {
	return strconv.FormatInt(int64(i.get(row)), 10)
}

func (i *Int16ColumnData) Zero() interface{} {
	return int16(0)
}

func (i *Int16ColumnData) ZeroString() string {
	return zeroString
}

func (i *Int16ColumnData) Len() int {
	return len(i.raw) / 2
}

func (i *Int16ColumnData) Close() error {
	if i.isClosed {
		return nil
	}
	i.isClosed = true
	bytepool.PutBytes(i.raw)
	return nil
}

// interpretInt16Type converts subsets of int16 to int16
// interpretInt16Type returns a function to avoid unnecessary switch case computation
// implicitly assumes that all values follow the type of values[0]
func interpretInt16Type(originalValue interface{}) (func(value interface{}) (int16, bool), error) {
	switch value := originalValue.(type) {
	case int16:
		return func(value interface{}) (int16, bool) {
			// Have to recompute value of ok even if match to ensure that subsequent values matches same type
			v, ok := value.(int16)
			return v, ok
		}, nil
	case int8:
		return func(value interface{}) (int16, bool) {
			v, ok := value.(int8)
			return int16(v), ok
		}, nil
	default:
		return nil, NewErrInvalidColumnType(value, int16(0))
	}
}
