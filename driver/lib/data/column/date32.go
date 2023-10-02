package column

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

const (
	date32Len                        = 4      // int32
	maxSupportedDaysSinceEpoch int64 = 120529 // 2299-12-31
	minSupportedDaysSinceEpoch int64 = -25567 // 1900-01-01
)

type Date32ColumnData struct {
	raw      []byte
	isClosed bool
}

func (d *Date32ColumnData) ReadFromDecoder(decoder *ch_encoding.Decoder) error {
	_, err := decoder.Read(d.raw)
	return err
}

func (d *Date32ColumnData) WriteToEncoder(encoder *ch_encoding.Encoder) error {
	_, err := encoder.Write(d.raw)
	return err
}

func (d *Date32ColumnData) ReadFromValues(values []interface{}) (int, error) {
	var (
		t  time.Time
		ok bool
	)

	for i, value := range values {
		if value == nil {
			binary.LittleEndian.PutUint32(d.raw[i*date32Len:], 0)
			continue
		}

		t, ok = value.(time.Time)
		if !ok {
			return i, NewErrInvalidColumnType(value, t)
		}
		binary.LittleEndian.PutUint32(d.raw[i*date32Len:], uint32((t.Unix())/dayHours/hourSeconds))
	}

	return len(values), nil
}

func (d *Date32ColumnData) ReadFromTexts(texts []string) (int, error) {
	var (
		t   time.Time
		err error
	)

	// Use UTC for location to prevent date from being modified
	parseTime := interpretTimeFormat(supportedStringDateFormats, texts, time.UTC)

	for i, text := range texts {
		if isEmptyOrNull(text) {
			binary.LittleEndian.PutUint32(d.raw[i*date32Len:], 0)
			continue
		}

		t, err = parseTime(text)
		if err != nil {
			return i, err
		}
		parsedDateInInt64 := t.Unix() / dayHours / hourSeconds
		if parsedDateInInt64 > maxSupportedDaysSinceEpoch || parsedDateInInt64 < minSupportedDaysSinceEpoch {
			return i, fmt.Errorf(
				"invalid value for type Date32, given %s, but supported range is 1900-01-01 - 2299-12-31",
				text,
			)
		}
		binary.LittleEndian.PutUint32(d.raw[i*date32Len:], uint32(parsedDateInInt64))
	}
	return len(texts), nil
}

func (d *Date32ColumnData) get(row int) time.Time {
	daysSinceEpoch := bufferRowToUint32(d.raw, row)
	// Get negative values
	secondsSinceEpoch := int64(int32(daysSinceEpoch)) * dayHours * hourSeconds
	return time.Unix(secondsSinceEpoch, 0)
}

func (d *Date32ColumnData) GetValue(row int) interface{} {
	return d.get(row)
}

func (d *Date32ColumnData) GetString(row int) string {
	return d.get(row).Format(defaultDateFormat)
}

func (d *Date32ColumnData) Zero() interface{} {
	return zeroTime
}

func (d *Date32ColumnData) ZeroString() string {
	return zeroTime.Format(defaultDateFormat)
}

func (d *Date32ColumnData) Len() int {
	return len(d.raw) / date32Len
}

func (d *Date32ColumnData) Close() error {
	if d.isClosed {
		return nil
	}
	d.isClosed = false
	bytepool.PutBytes(d.raw)
	return nil
}
