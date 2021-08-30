package column

import (
	"encoding/binary"
	"strconv"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

const uint32ByteSize = 4

type UInt32ColumnData struct {
	raw []byte
}

func (u *UInt32ColumnData) ReadFromDecoder(decoder *ch_encoding.Decoder) error {
	_, err := decoder.Read(u.raw)
	return err
}

func (u *UInt32ColumnData) WriteToEncoder(encoder *ch_encoding.Encoder) error {
	_, err := encoder.Write(u.raw)
	return err
}

func (u *UInt32ColumnData) ReadFromValues(values []interface{}) (int, error) {
	if len(values) == 0 {
		return 0, nil
	}

	var (
		v   uint32
		ok  bool
		err error
	)

	readUInt32Func, err := interpretUInt32Type(values[0])
	if err != nil {
		return 0, err
	}

	for idx, value := range values {
		v, ok = readUInt32Func(value)
		if !ok {
			return idx, NewErrInvalidColumnType(value, v)
		}

		binary.LittleEndian.PutUint32(u.raw[idx*uint32ByteSize:], v)
	}

	return len(values), nil
}

func (u *UInt32ColumnData) ReadFromTexts(texts []string) (int, error) {
	var (
		err error
		v   uint64
	)

	for i, text := range texts {
		if text == "" {
			binary.LittleEndian.PutUint32(u.raw[i*uint32ByteSize:], 0)
			continue
		}

		if v, err = strconv.ParseUint(text, 10, 32); err != nil {
			return i, err
		}
		binary.LittleEndian.PutUint32(u.raw[i*uint32ByteSize:], uint32(v))
	}
	return len(texts), nil
}

func (u *UInt32ColumnData) get(row int) uint32 {
	return bufferRowToUint32(u.raw, row)
}

func (u *UInt32ColumnData) GetValue(row int) interface{} {
	return u.get(row)
}

func (u *UInt32ColumnData) GetString(row int) string {
	return strconv.FormatUint(uint64(u.get(row)), 10)
}

func (u *UInt32ColumnData) Zero() interface{} {
	return uint32(0)
}

func (u *UInt32ColumnData) ZeroString() string {
	return zeroString
}

func (u *UInt32ColumnData) Len() int {
	return len(u.raw) / 4
}

func (u *UInt32ColumnData) Close() error {
	bytepool.PutBytes(u.raw)
	return nil
}

// interpretUInt32Type converts subsets of uint32 to uint32
// interpretUInt32Type returns a function to avoid unnecessary switch case computation
// implicitly assumes that all values follow the type of values[0]
func interpretUInt32Type(originalValue interface{}) (func(value interface{}) (uint32, bool), error) {
	switch value := originalValue.(type) {
	case uint32:
		return func(value interface{}) (uint32, bool) {
			// Have to recompute value of ok even if match to ensure that subsequent values matches same type
			v, ok := value.(uint32)
			return v, ok
		}, nil
	case uint16:
		return func(value interface{}) (uint32, bool) {
			v, ok := value.(uint16)
			return uint32(v), ok
		}, nil
	case uint8:
		return func(value interface{}) (uint32, bool) {
			v, ok := value.(uint8)
			return uint32(v), ok
		}, nil
	default:
		return nil, NewErrInvalidColumnType(value, uint32(0))
	}
}
