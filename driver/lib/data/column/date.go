package column

import (
	"encoding/binary"
	"time"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

const (
	dateLen     = 2
	dateFormat  = "2006-01-02"
	dayHours    = 24
	hourSeconds = 3600
)

var zeroTime = time.Unix(0, 0)

var offset = func() int64 {
	_, offset := time.Unix(0, 0).Zone()
	return int64(offset)
}()

type DateColumnData struct {
	raw []byte
}

func (d *DateColumnData) ReadFromDecoder(decoder *ch_encoding.Decoder) error {
	_, err := decoder.Read(d.raw)
	return err
}

func (d *DateColumnData) WriteToEncoder(encoder *ch_encoding.Encoder) error {
	_, err := encoder.Write(d.raw)
	return err
}

func (d *DateColumnData) ReadFromValues(values []interface{}) (int, error) {
	var (
		t  time.Time
		ok bool
	)

	for i, value := range values {
		t, ok = value.(time.Time)
		if !ok {
			return i, NewErrInvalidColumnType(value, t)
		}
		binary.LittleEndian.PutUint16(d.raw[i*dateLen:], uint16((t.Unix()+offset)/dayHours/hourSeconds))
	}

	return len(values), nil
}

func (d *DateColumnData) ReadFromTexts(texts []string) (int, error) {
	var (
		t   time.Time
		err error
	)

	// Use UTC for location to prevent date from being modified
	parseTime := interpretTimeFormat([]string{dateFormat, dateTimeFormat}, texts, time.UTC)

	for i, text := range texts {
		if text == "" {
			binary.LittleEndian.PutUint16(d.raw[i*dateLen:], 0)
			continue
		}

		t, err = parseTime(text)
		if err != nil {
			return i, err
		}
		binary.LittleEndian.PutUint16(d.raw[i*dateLen:], uint16((t.Unix())/dayHours/hourSeconds))
	}
	return len(texts), nil
}

func (d *DateColumnData) get(row int) time.Time {
	daysSinceEpoch := bufferRowToUint16(d.raw, row)
	secondsSinceEpoch := int64(daysSinceEpoch) * dayHours * hourSeconds
	return time.Unix(secondsSinceEpoch-offset, 0).In(time.Local)
}

func (d *DateColumnData) GetValue(row int) interface{} {
	return d.get(row)
}

func (d *DateColumnData) GetString(row int) string {
	return d.get(row).Format(dateFormat)
}

func (d *DateColumnData) Zero() interface{} {
	return zeroTime
}

func (d *DateColumnData) ZeroString() string {
	return zeroTime.Format(dateFormat)
}

func (d *DateColumnData) Len() int {
	return len(d.raw) / dateLen
}

func (d *DateColumnData) Close() error {
	bytepool.PutBytes(d.raw)
	return nil
}
