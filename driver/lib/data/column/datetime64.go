package column

import (
	"encoding/binary"
	"math"
	"time"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

const (
	dateTime64Len    = 8
	dateTime64Format = "2006-01-02 15:04:05.999"
)

type DateTime64ColumnData struct {
	precision int
	timeZone  *time.Location
	raw       []byte
}

func (d *DateTime64ColumnData) ReadFromDecoder(decoder *ch_encoding.Decoder) error {
	_, err := decoder.Read(d.raw)
	return err
}

func (d *DateTime64ColumnData) WriteToEncoder(encoder *ch_encoding.Encoder) error {
	_, err := encoder.Write(d.raw)
	return err
}

func (d *DateTime64ColumnData) ReadFromValues(values []interface{}) (int, error) {
	var (
		t  time.Time
		ok bool
	)

	for i, value := range values {
		t, ok = value.(time.Time)
		if !ok {
			return i, NewErrInvalidColumnType(value, t)
		}
		n := t.UnixNano() / int64(math.Pow10(9-d.precision))
		binary.LittleEndian.PutUint64(d.raw[i*dateTime64Len:], uint64(n))
	}

	return len(values), nil
}

func (d *DateTime64ColumnData) ReadFromTexts(texts []string) (int, error) {
	var (
		t   time.Time
		err error
	)

	parseTime := interpretTimeFormat([]string{dateFormat, dateTimeFormat, dateTime64Format}, texts, time.Local)

	for i, text := range texts {
		if text == "" {
			binary.LittleEndian.PutUint64(d.raw[i*dateTime64Len:], 0)
			continue
		}

		t, err = parseTime(text)
		if err != nil {
			return i, err
		}
		n := t.UnixNano() / int64(math.Pow10(9-d.precision))
		binary.LittleEndian.PutUint64(d.raw[i*dateTime64Len:], uint64(n))
	}
	return len(texts), nil
}

func (d *DateTime64ColumnData) get(row int) time.Time {
	timeValue := int64(bufferRowToUint64(d.raw, row))
	timeValue *= int64(math.Pow10(9 - d.precision))
	return time.Unix(0, timeValue).In(d.timeZone)
}

func (d *DateTime64ColumnData) GetValue(row int) interface{} {
	return d.get(row)
}

func (d *DateTime64ColumnData) GetString(row int) string {
	return d.get(row).In(d.timeZone).Format(dateTime64Format)
}

func (d *DateTime64ColumnData) Zero() interface{} {
	return zeroTime
}

func (d *DateTime64ColumnData) ZeroString() string {
	return zeroTime.Format(dateTime64Format)
}

func (d *DateTime64ColumnData) Len() int {
	return len(d.raw) / dateTime64Len
}

func (d *DateTime64ColumnData) Close() error {
	bytepool.PutBytes(d.raw)
	return nil
}
