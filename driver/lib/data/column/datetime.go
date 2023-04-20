package column

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

const (
	dateTimeLen           = 4
	defaultDateTimeFormat = "2006-01-02 15:04:05" // todo: allow client to set format
)

var supportedDateTimeFormats = []string{
	defaultDateFormat,
	defaultDateTimeFormat,
	"2006-01-02T15:04:05-0700",
	time.RFC3339,
}

type DateTimeColumnData struct {
	timeZone *time.Location
	raw      []byte
	isClosed bool
}

func (d *DateTimeColumnData) ReadFromDecoder(decoder *ch_encoding.Decoder) error {
	_, err := decoder.Read(d.raw)
	return err
}

func (d *DateTimeColumnData) WriteToEncoder(encoder *ch_encoding.Encoder) error {
	_, err := encoder.Write(d.raw)
	return err
}

func (d *DateTimeColumnData) ReadFromValues(values []interface{}) (int, error) {
	var (
		t  time.Time
		ok bool
	)

	for i, value := range values {
		t, ok = value.(time.Time)
		if !ok {
			return i, NewErrInvalidColumnType(value, t)
		}
		binary.LittleEndian.PutUint32(d.raw[i*dateTimeLen:], uint32(t.Unix()))
	}

	return len(values), nil
}

func (d *DateTimeColumnData) ReadFromTexts(texts []string) (int, error) {
	var (
		t   time.Time
		err error
	)

	parseTime := interpretTimeFormat(supportedDateTimeFormats, texts, d.timeZone)

	for i, text := range texts {
		if text == "" {
			binary.LittleEndian.PutUint32(d.raw[i*dateTimeLen:], 0)
			continue
		}

		t, err = parseTime(text)
		if err != nil {
			return i, err
		}
		parsedTimeInSecondsInt64 := t.Unix()
		var supportedTimeInSecondsMax int64 = 4294967295 // 2106-02-07 06:28:15
		var supportedDateInSecondsMin int64 = 0          // 1970-01-01 00:00:00
		if parsedTimeInSecondsInt64 > supportedTimeInSecondsMax || parsedTimeInSecondsInt64 < supportedDateInSecondsMin {
			return i, fmt.Errorf(
				"invalid value for type DateTime, given %s, but supported range is %s - %s in UTC",
				text, "1970-01-01 00:00:00", "2106-02-07 06:28:15",
			)
		}
		binary.LittleEndian.PutUint32(d.raw[i*dateTimeLen:], uint32(parsedTimeInSecondsInt64))
	}

	return len(texts), nil
}

func (d *DateTimeColumnData) get(row int) time.Time {
	secondsSinceEpoch := bufferRowToUint32(d.raw, row)
	if d.timeZone != nil {
		// TODO: remove this if branch, function should be decided at the
		// time when column in initialized
		return time.Unix(int64(secondsSinceEpoch), 0).In(d.timeZone)
	}
	return time.Unix(int64(secondsSinceEpoch), 0)
}

func (d *DateTimeColumnData) GetValue(row int) interface{} {
	return d.get(row)
}

func (d *DateTimeColumnData) GetString(row int) string {
	return d.get(row).Format(defaultDateTimeFormat)
}

func (d *DateTimeColumnData) Zero() interface{} {
	return zeroTime
}

func (d *DateTimeColumnData) ZeroString() string {
	return zeroTime.Format(defaultDateTimeFormat)
}

func (d *DateTimeColumnData) Len() int {
	return len(d.raw) / dateTimeLen
}

func (d *DateTimeColumnData) Close() error {
	if d.isClosed {
		return nil
	}
	d.isClosed = true
	bytepool.PutBytes(d.raw)
	return nil
}
