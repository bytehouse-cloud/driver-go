package column

import (
	"time"

	"github.com/shopspring/decimal"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

type TimeColumnData struct {
	scale      int
	baseColumn *DecimalColumnData
}

func (d *TimeColumnData) ReadFromDecoder(decoder *ch_encoding.Decoder) error {
	return d.baseColumn.ReadFromDecoder(decoder)
}

func (d *TimeColumnData) WriteToEncoder(encoder *ch_encoding.Encoder) error {
	return d.baseColumn.WriteToEncoder(encoder)
}

func (d *TimeColumnData) ReadFromValues(values []interface{}) (int, error) {
	var (
		t  time.Time
		ok bool
	)

	for i, value := range values {
		if value == nil {
			_ = d.baseColumn.putDecimalIntoBytes(i, 0.0)
			continue
		}

		t, ok = value.(time.Time)
		if !ok {
			return i, NewErrInvalidColumnType(value, t)
		}
		decimalValue, err := getDecimalFromTime(t, d.scale)
		if err != nil {
			return i, err
		}
		if err := d.baseColumn.putDecimalIntoBytes(i, decimalValue); err != nil {
			return i, err
		}
	}

	return len(values), nil
}

func (d *TimeColumnData) ReadFromTexts(texts []string) (int, error) {
	var (
		t   decimal.Decimal
		err error
	)

	for i, text := range texts {
		if isEmptyOrNull(text) {
			_ = d.baseColumn.putDecimalIntoBytes(i, 0.0)
			continue
		}

		t, err = parseDecimalTimeFromString(text, d.scale)
		if err != nil {
			return i, err
		}
		if err := d.baseColumn.putDecimalIntoBytes(i, t); err != nil {
			return i, err
		}
	}
	return len(texts), nil
}

func (d *TimeColumnData) get(row int) time.Time {
	decimalValue, ok := d.baseColumn.GetValue(row).(decimal.Decimal)
	if !ok {
		return time.Time{}
	}
	return getTimeFromDecimal(decimalValue)
}

func (d *TimeColumnData) GetValue(row int) interface{} {
	return d.get(row)
}

func (d *TimeColumnData) GetString(row int) string {

	timeVal := d.get(row)
	return timeVal.Format(GetTimeFormat(d.scale))
}

func (d *TimeColumnData) Zero() interface{} {
	return zeroTime
}

func (d *TimeColumnData) ZeroString() string {
	return zeroTime.Format(GetTimeFormat(d.scale))
}

func (d *TimeColumnData) Len() int {
	return d.baseColumn.Len()
}

func (d *TimeColumnData) Close() error {
	return d.baseColumn.Close()
}
