package column

import (
	"encoding/binary"
	"time"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

const (
	dateTimeLen    = 4
	dateTimeFormat = "2006-01-02 15:04:05"
)

type DateTimeColumnData struct {
	timeZone *time.Location
	raw      []byte
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

	parseTime := interpretTimeFormat([]string{dateFormat, dateTimeFormat}, texts, time.Local)

	for i, text := range texts {
		if text == "" {
			binary.LittleEndian.PutUint32(d.raw[i*dateTimeLen:], 0)
			continue
		}

		t, err = parseTime(text)
		if err != nil {
			return i, err
		}
		binary.LittleEndian.PutUint32(d.raw[i*dateTimeLen:], uint32(t.Unix()))
	}

	return len(texts), nil
}

func (d *DateTimeColumnData) get(row int) time.Time {
	secondsSinceEpoch := bufferRowToUint32(d.raw, row)
	return time.Unix(int64(secondsSinceEpoch), 0).In(d.timeZone)
}

func (d *DateTimeColumnData) GetValue(row int) interface{} {
	return d.get(row)
}

func (d *DateTimeColumnData) GetString(row int) string {
	return d.get(row).In(d.timeZone).Format(dateTimeFormat)
}

func (d *DateTimeColumnData) Zero() interface{} {
	return zeroTime
}

func (d *DateTimeColumnData) ZeroString() string {
	return zeroTime.Format(dateTimeFormat)
}

func (d *DateTimeColumnData) Len() int {
	return len(d.raw) / dateTimeLen
}

func (d *DateTimeColumnData) Close() error {
	bytepool.PutBytes(d.raw)
	return nil
}
