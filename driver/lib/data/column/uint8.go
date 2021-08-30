package column

import (
	"strconv"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
	"github.com/bytehouse-cloud/driver-go/errors"
)

type UInt8ColumnData struct {
	raw []byte
}

func (u *UInt8ColumnData) ReadFromDecoder(decoder *ch_encoding.Decoder) error {
	_, err := decoder.Read(u.raw)
	return err
}

func (u *UInt8ColumnData) WriteToEncoder(encoder *ch_encoding.Encoder) error {
	_, err := encoder.Write(u.raw)
	return err
}

func (u *UInt8ColumnData) ReadFromValues(values []interface{}) (int, error) {
	var (
		v  uint8
		ok bool
	)

	for idx, value := range values {
		v, ok = value.(uint8)
		if !ok {
			return idx, NewErrInvalidColumnType(value, v)
		}

		u.raw[idx] = v
	}

	return len(values), nil
}

func (u *UInt8ColumnData) ReadFromTexts(texts []string) (int, error) {
	var (
		v   uint64
		err error
	)

	for i, text := range texts {
		if text == "" {
			u.raw[i] = byte(0)
			continue
		}

		if v, err = strconv.ParseUint(text, 10, 8); err != nil {
			return i, errors.ErrorfWithCaller("%v", err)
		}
		u.raw[i] = byte(v)
	}
	return len(texts), nil
}

func (u *UInt8ColumnData) Zero() interface{} {
	return uint8(0)
}

func (u *UInt8ColumnData) ZeroString() string {
	return zeroString
}

func (u *UInt8ColumnData) get(row int) uint8 {
	return u.raw[row]
}

func (u *UInt8ColumnData) GetValue(row int) interface{} {
	return u.get(row)
}

func (u *UInt8ColumnData) GetString(row int) string {
	return strconv.FormatUint(uint64(u.get(row)), 10)
}

func (u *UInt8ColumnData) Len() int {
	return len(u.raw)
}

func (u *UInt8ColumnData) Close() error {
	bytepool.PutBytes(u.raw)
	return nil
}
