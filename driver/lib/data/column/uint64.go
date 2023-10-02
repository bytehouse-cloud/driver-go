package column

import (
	"encoding/binary"
	"strconv"

	"github.com/valyala/fastjson/fastfloat"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

type UInt64ColumnData struct {
	raw      []byte
	isClosed bool
}

const uint64ByteSize = 8

func (u *UInt64ColumnData) ReadFromDecoder(decoder *ch_encoding.Decoder) error {
	_, err := decoder.Read(u.raw)
	return err
}

func (u *UInt64ColumnData) WriteToEncoder(encoder *ch_encoding.Encoder) error {
	_, err := encoder.Write(u.raw)
	return err
}

func (u *UInt64ColumnData) ReadFromValues(values []interface{}) (int, error) {
	if len(values) == 0 {
		return 0, nil
	}

	var (
		err            error
		readUInt64Func func(value interface{}) (uint64, bool)
	)

	for idx, value := range values {
		if value == nil {
			binary.LittleEndian.PutUint64(u.raw[idx*uint64ByteSize:], 0)
			continue
		}

		if readUInt64Func == nil {
			readUInt64Func, err = interpretUInt64Type(value)
			if err != nil {
				return 0, err
			}
		}

		v, ok := readUInt64Func(value)
		if !ok {
			return idx, NewErrInvalidColumnType(value, v)
		}

		binary.LittleEndian.PutUint64(u.raw[idx*uint64ByteSize:], v)
	}

	return len(values), nil
}

func (u *UInt64ColumnData) ReadFromTexts(texts []string) (int, error) {
	var (
		v   uint64
		err error
	)

	for i, text := range texts {
		if isEmptyOrNull(text) {
			binary.LittleEndian.PutUint64(u.raw[i*uint64ByteSize:], 0)
			continue
		}

		if v, err = fastfloat.ParseUint64(text); err != nil {
			return i, err
		}
		binary.LittleEndian.PutUint64(u.raw[i*uint64ByteSize:], v)
	}
	return len(texts), nil
}

func (u *UInt64ColumnData) get(row int) uint64 {
	return bufferRowToUint64(u.raw, row)
}

func (u *UInt64ColumnData) GetValue(row int) interface{} {
	return u.get(row)
}

func (u *UInt64ColumnData) GetString(row int) string {
	return strconv.FormatUint(u.get(row), 10)
}

func (u *UInt64ColumnData) Zero() interface{} {
	return uint64(0)
}

func (u *UInt64ColumnData) ZeroString() string {
	return zeroString
}

func (u *UInt64ColumnData) Len() int {
	return len(u.raw) / 8
}

func (u *UInt64ColumnData) Close() error {
	if u.isClosed {
		return nil
	}
	u.isClosed = true
	bytepool.PutBytes(u.raw)
	return nil
}

// interpretUInt64Type converts subsets of uint64 to uint64
// interpretUInt64Type returns a function to avoid unnecessary switch case computation
// implicitly assumes that all values follow the type of values[0]
func interpretUInt64Type(originalValue interface{}) (func(value interface{}) (uint64, bool), error) {
	switch value := originalValue.(type) {
	case uint64:
		return func(value interface{}) (uint64, bool) {
			// Have to recompute value of ok even if match to ensure that subsequent values matches same type
			v, ok := value.(uint64)
			return v, ok
		}, nil
	case uint:
		return func(value interface{}) (uint64, bool) {
			// Have to recompute value of ok even if match to ensure that subsequent values matches same type
			v, ok := value.(uint)
			return uint64(v), ok
		}, nil
	case uint32:
		return func(value interface{}) (uint64, bool) {
			v, ok := value.(uint32)
			return uint64(v), ok
		}, nil
	case uint16:
		return func(value interface{}) (uint64, bool) {
			v, ok := value.(uint16)
			return uint64(v), ok
		}, nil
	case uint8:
		return func(value interface{}) (uint64, bool) {
			v, ok := value.(uint8)
			return uint64(v), ok
		}, nil
	default:
		return nil, NewErrInvalidColumnType(value, uint64(0))
	}
}
