package column

import (
	"encoding/binary"
	"math"
	"strings"
	"time"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

const (
	dateTime64Len          = 8
	dateTime64FormatPrefix = "2006-01-02 15:04:05."
)

type DateTime64ColumnData struct {
	precision int
	timeZone  *time.Location
	raw       []byte
	isClosed  bool
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
		if value == nil {
			binary.LittleEndian.PutUint64(d.raw[i*dateTime64Len:], 0)
			continue
		}

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

	for i, text := range texts {
		if isEmptyOrNull(text) {
			binary.LittleEndian.PutUint64(d.raw[i*dateTime64Len:], 0)
			continue
		}

		t, err = parseDateTime64Format(supportedDateTimeFormats, text, d.precision, d.timeZone)
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
	if d.timeZone != nil {
		return time.Unix(0, timeValue).In(d.timeZone)
	}
	return time.Unix(0, timeValue)
}

func (d *DateTime64ColumnData) GetValue(row int) interface{} {
	return d.get(row)
}

func (d *DateTime64ColumnData) GetString(row int) string {
	return d.get(row).Format(d.getDateTime64Format())
}

func (d *DateTime64ColumnData) Zero() interface{} {
	return zeroTime
}

func (d *DateTime64ColumnData) ZeroString() string {
	return zeroTime.Format(d.getDateTime64Format())
}

func (d *DateTime64ColumnData) Len() int {
	return len(d.raw) / dateTime64Len
}

func (d *DateTime64ColumnData) Close() error {
	if d.isClosed {
		return nil
	}
	d.isClosed = true
	bytepool.PutBytes(d.raw)
	return nil
}

func (d *DateTime64ColumnData) getDateTime64Format() string {
	var sb strings.Builder
	sb.WriteString(dateTime64FormatPrefix)
	for i := 0; i < d.precision; i++ {
		sb.WriteString("0")
	}
	return sb.String()
}
