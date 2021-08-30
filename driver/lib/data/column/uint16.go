package column

import (
	"encoding/binary"
	"strconv"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

const uint16ByteSize = 2

type UInt16ColumnData struct {
	raw []byte
}

func (u *UInt16ColumnData) ReadFromDecoder(decoder *ch_encoding.Decoder) error {
	_, err := decoder.Read(u.raw)
	return err
}

func (u *UInt16ColumnData) WriteToEncoder(encoder *ch_encoding.Encoder) error {
	_, err := encoder.Write(u.raw)
	return err
}

func (i *UInt16ColumnData) ReadFromValues(values []interface{}) (int, error) {
	if len(values) == 0 {
		return 0, nil
	}

	var (
		v   uint16
		ok  bool
		err error
	)

	readUInt16Func, err := interpretUInt16Type(values[0])
	if err != nil {
		return 0, err
	}

	for idx, value := range values {
		v, ok = readUInt16Func(value)
		if !ok {
			return idx, NewErrInvalidColumnType(value, v)
		}

		binary.LittleEndian.PutUint16(i.raw[idx*uint16ByteSize:], v)
	}

	return len(values), nil
}

func (u *UInt16ColumnData) ReadFromTexts(texts []string) (int, error) {
	var (
		v   uint64
		err error
	)

	for i, text := range texts {
		if text == "" {
			binary.LittleEndian.PutUint16(u.raw[i*uint16ByteSize:], 0)
			continue
		}

		if v, err = strconv.ParseUint(text, 10, 16); err != nil {
			return i, err
		}
		binary.LittleEndian.PutUint16(u.raw[i*uint16ByteSize:], uint16(v))
	}
	return len(texts), nil
}

func (u *UInt16ColumnData) get(row int) uint16 {
	return bufferRowToUint16(u.raw, row)
}

func (u *UInt16ColumnData) GetValue(row int) interface{} {
	return u.get(row)
}

func (u *UInt16ColumnData) GetString(row int) string {
	return strconv.FormatUint(uint64(u.get(row)), 10)
}

func (u *UInt16ColumnData) Zero() interface{} {
	return uint16(0)
}

func (u *UInt16ColumnData) ZeroString() string {
	return zeroString
}

func (u *UInt16ColumnData) Len() int {
	return len(u.raw) / 2
}

func (u *UInt16ColumnData) Close() error {
	bytepool.PutBytes(u.raw)
	return nil
}

// interpretUInt16Type converts subsets of uint16 to uint16
// interpretUInt16Type returns a function to avoid unnecessary switch case computation
// implicitly assumes that all values follow the type of values[0]
func interpretUInt16Type(originalValue interface{}) (func(value interface{}) (uint16, bool), error) {
	switch value := originalValue.(type) {
	case uint16:
		return func(value interface{}) (uint16, bool) {
			// Have to recompute value of ok even if match to ensure that subsequent values matches same type
			v, ok := value.(uint16)
			return v, ok
		}, nil
	case uint8:
		return func(value interface{}) (uint16, bool) {
			v, ok := value.(uint8)
			return uint16(v), ok
		}, nil
	default:
		return nil, NewErrInvalidColumnType(value, uint16(0))
	}
}
