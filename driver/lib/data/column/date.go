package column

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

const (
	dateLen           = 2
	dayHours          = 24
	hourSeconds       = 3600
	defaultDateFormat = "2006-01-02" // todo: allow client to specify format for date
)

var supportedNumericalDateFormats = []string{
	"20060102", // yyyyMMdd
	"02012006", // ddMMyyyy
}

var supportedStringDateFormats = []string{
	"2006-1-2",       // yyyy-M-d, can cover yyyy-MM-dd case too
	"2-1-2006",       // d-M-yyyy, can cover dd-MM-yyyy case too, MM-dd-yyyy may conflict with dd-MM-yyyy, thus MM-dd-yyyy is not supported
	"2006/1/2",       // yyyy/M/d, can cover yyyy/MM/dd case too
	"2/1/2006",       // d/M/yyyy, can cover dd/MM/yyyy case too, MM/dd/yyyy may conflict with dd/MM/yyyy, thus MM/dd/yyyy is not supported
	"2006-Jan-2",     // yyyy-Mon-d, can cover yyyy-Mon-dd case too
	"2-Jan-2006",     // d-Mon-yyyy, can cover dd-Mon-yyyy case too
	"Jan-2-2006",     // Mon-d-yyyy, can cover Mon-dd-yyyy case too
	"2006/Jan/2",     // yyyy/Mon/d, can cover yyyy/Mon/dd case too
	"2/Jan/2006",     // d/Mon/yyyy, can cover dd/Mon/yyyy case too
	"Jan/2/2006",     // Mon/d/yyyy, can cover Mon/dd/yyyy case too
	"2006-January-2", // yyyy-Month-d, can cover yyyy-Month-dd case too
	"2-January-2006", // d-Month-yyyy, can cover dd-Month-yyyy case too
	"January-2-2006", // Month-d-yyyy, can cover Month-dd-yyyy case too
	"2006/January/2", // yyyy/Month/d, can cover yyyy/Month/dd case too
	"2/January/2006", // d/Month/yyyy, can cover dd/Month/yyyy case too
	"January/2/2006", // Month/d/yyyy, can cover Month/dd/yyyy case too
}

var zeroTime = time.Unix(0, 0)

var offset = func() int64 {
	_, offset := time.Unix(0, 0).Zone()
	return int64(offset)
}()

type DateColumnData struct {
	dayOffset int
	raw       []byte
	isClosed  bool
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
	parseTime := interpretTimeFormat(supportedStringDateFormats, texts, time.UTC)

	for i, text := range texts {
		if text == "" {
			binary.LittleEndian.PutUint16(d.raw[i*dateLen:], 0)
			continue
		}

		t, err = parseTime(text)
		if err != nil {
			return i, err
		}
		parsedDateInInt64 := t.Unix() / dayHours / hourSeconds
		var supportedDateMax int64 = 65535 // 2149-06-06
		var supportedDateMin int64 = 0     // 1970-01-01
		if parsedDateInInt64 > supportedDateMax || parsedDateInInt64 < supportedDateMin {
			return i, fmt.Errorf(
				"invalid value for type Date, given %s, but supported range is 1970-01-01 - 2149-06-06",
				text,
			)
		}
		binary.LittleEndian.PutUint16(d.raw[i*dateLen:], uint16(parsedDateInInt64))
	}
	return len(texts), nil
}

func (d *DateColumnData) get(row int) time.Time {
	daysSinceEpoch := bufferRowToUint16(d.raw, row)
	secondsSinceEpoch := int64(daysSinceEpoch) * dayHours * hourSeconds
	return time.Unix(secondsSinceEpoch, 0)
}

func (d *DateColumnData) GetValue(row int) interface{} {
	return d.get(row)
}

func (d *DateColumnData) GetString(row int) string {
	return d.get(row).Format(defaultDateFormat)
}

func (d *DateColumnData) Zero() interface{} {
	return zeroTime
}

func (d *DateColumnData) ZeroString() string {
	return zeroTime.Format(defaultDateFormat)
}

func (d *DateColumnData) Len() int {
	return len(d.raw) / dateLen
}

func (d *DateColumnData) Close() error {
	if d.isClosed {
		return nil
	}
	d.isClosed = false
	bytepool.PutBytes(d.raw)
	return nil
}
