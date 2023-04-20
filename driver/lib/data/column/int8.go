package column

import (
	"strconv"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

const zeroString = "0"

type Int8ColumnData struct {
	raw      []byte
	isClosed bool
}

func (i *Int8ColumnData) ReadFromDecoder(decoder *ch_encoding.Decoder) error {
	_, err := decoder.Read(i.raw)
	return err
}

func (i *Int8ColumnData) WriteToEncoder(encoder *ch_encoding.Encoder) error {
	_, err := encoder.Write(i.raw)
	return err
}

func (i *Int8ColumnData) ReadFromValues(values []interface{}) (int, error) {
	var (
		v  int8
		ok bool
	)

	for idx, value := range values {
		v, ok = value.(int8)
		if !ok {
			return idx, NewErrInvalidColumnType(value, v)
		}

		i.raw[idx] = byte(v)
	}

	return len(values), nil
}

func (i *Int8ColumnData) ReadFromTexts(texts []string) (int, error) {
	var (
		v   int64
		err error
	)

	for idx, text := range texts {
		if text == "" {
			i.raw[idx] = 0
			continue
		}

		if v, err = strconv.ParseInt(text, 10, 8); err != nil {
			return idx, err
		}
		i.raw[idx] = byte(v)
	}
	return len(texts), nil
}

func (i *Int8ColumnData) get(row int) int8 {
	return int8(i.raw[row])
}

func (i *Int8ColumnData) GetValue(row int) interface{} {
	return i.get(row)
}

func (i *Int8ColumnData) GetString(row int) string {
	return strconv.FormatInt(int64(i.get(row)), 10)
}

func (i *Int8ColumnData) Zero() interface{} {
	return int8(0)
}

func (i *Int8ColumnData) ZeroString() string {
	return zeroString
}

func (i *Int8ColumnData) Len() int {
	return len(i.raw)
}

func (i *Int8ColumnData) Close() error {
	if i.isClosed {
		return nil
	}
	i.isClosed = true
	bytepool.PutBytes(i.raw)
	return nil
}
