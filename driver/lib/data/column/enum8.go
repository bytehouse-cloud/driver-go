package column

import (
	"fmt"
	"strconv"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
	"github.com/bytehouse-cloud/driver-go/errors"
)

const unknownInt8Value = "unknown int8 value: %v, expects one of key in map: %v"

type Enum8ColumnData struct {
	raw  []byte
	itoa map[int8]string
	atoi map[string]int8
}

func (e *Enum8ColumnData) ReadFromDecoder(decoder *ch_encoding.Decoder) error {
	_, err := decoder.Read(e.raw)
	return err
}

func (e *Enum8ColumnData) WriteToEncoder(encoder *ch_encoding.Encoder) error {
	_, err := encoder.Write(e.raw)
	return err
}

func (e *Enum8ColumnData) ReadFromValues(values []interface{}) (int, error) {
	if len(values) == 0 {
		return 0, nil
	}

	var (
		v   int8
		err error
	)

	readEnum8Func, err := e.interpretEnum8Type(values[0])
	if err != nil {
		return 0, err
	}

	for i, value := range values {
		if v, err = readEnum8Func(value); err != nil {
			return i, err
		}
		e.raw[i] = byte(v)
	}
	return len(values), nil
}

func (e *Enum8ColumnData) ReadFromTexts(texts []string) (int, error) {
	var (
		v   int8
		err error
	)
	for i, text := range texts {
		if v, err = e.getInt8FromText(text); err != nil {
			return i, err
		}
		e.raw[i] = byte(v)
	}
	return len(texts), nil
}

func (e *Enum8ColumnData) get(row int) int8 {
	return int8(e.raw[row])
}

func (e *Enum8ColumnData) GetValue(row int) interface{} {
	return e.GetString(row)
}

func (e *Enum8ColumnData) GetString(row int) string {
	return e.itoa[e.get(row)]
}

func (e *Enum8ColumnData) Zero() interface{} {
	return emptyString
}

func (e *Enum8ColumnData) ZeroString() string {
	return zeroString
}

func (e *Enum8ColumnData) Len() int {
	return len(e.raw)
}

func (e *Enum8ColumnData) Close() error {
	bytepool.PutBytes(e.raw)
	return nil
}

// getInt8FromText returns the int8 for the enum
// If s is the string value of the enum, it returns the int8 the string value is mapped to
// If s is an int8 value, int8(s) is returned
func (e *Enum8ColumnData) getInt8FromText(s string) (int8, error) {
	s = processString(s)
	v, ok := e.atoi[s]
	if !ok { //possible to be Int8 string
		i, err := strconv.ParseInt(s, 10, 8)
		if err != nil {
			return 0, errors.ErrorfWithCaller(unknownStringValue, s, e.atoi)
		}
		if _, ok := e.itoa[int8(i)]; !ok {
			return 0, errors.ErrorfWithCaller(unknownInt8Value, s, e.itoa)
		}
		return int8(i), nil
	}
	return v, nil
}

// interpretEnum8Type converts the string or int8 to the int8 in the enum
// interpretEnum8Type returns a function to avoid unnecessary switch case computation
func (e *Enum8ColumnData) interpretEnum8Type(value interface{}) (func(value interface{}) (int8, error), error) {
	switch value := value.(type) {
	case string:
		return func(value interface{}) (int8, error) {
			str, ok := value.(string)
			if !ok {
				return 0, NewErrInvalidColumnType(value, str)
			}

			v, ok := e.atoi[str]
			if !ok {
				return 0, errors.ErrorfWithCaller(unknownStringValue, str, e.atoi)
			}
			return v, nil
		}, nil
	case int8:
		return func(value interface{}) (int8, error) {
			// Have to recompute value of ok even if match to ensure that subsequent values matches same type
			v, ok := value.(int8)
			if !ok {
				return v, NewErrInvalidColumnType(value, v)
			}

			if _, ok = e.itoa[v]; !ok {
				return 0, errors.ErrorfWithCaller(unknownInt8Value, v, e.itoa)
			}

			return v, nil
		}, nil
	default:
		return nil, NewErrInvalidColumnTypeCustomText(fmt.Sprintf("invalid column data type, current = %T, expected = %T or %T", value, int8(0), ""))
	}
}
