package column

import (
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
	"github.com/bytehouse-cloud/driver-go/errors"
)

const (
	unknownInt16Value  = "unknown int16 value: %v, expects one of key in map: %v"
	unknownStringValue = "unknown string value: %v, expects one of key in map: %v"
)

type Enum16ColumnData struct {
	raw  []byte
	itoa map[int16]string
	atoi map[string]int16
}

func (e *Enum16ColumnData) ReadFromDecoder(decoder *ch_encoding.Decoder) error {
	_, err := decoder.Read(e.raw)
	return err
}

func (e *Enum16ColumnData) WriteToEncoder(encoder *ch_encoding.Encoder) error {
	_, err := encoder.Write(e.raw)
	return err
}

func (e *Enum16ColumnData) ReadFromValues(values []interface{}) (int, error) {
	if len(values) == 0 {
		return 0, nil
	}

	var (
		v   int16
		err error
	)

	readEnum16Func, err := e.interpretEnum16Type(values[0])
	if err != nil {
		return 0, err
	}

	for i, value := range values {
		if v, err = readEnum16Func(value); err != nil {
			return i, err
		}
		binary.LittleEndian.PutUint16(e.raw[i*uint16ByteSize:], uint16(v))
	}
	return len(values), nil
}

func (e *Enum16ColumnData) ReadFromTexts(texts []string) (int, error) {
	var (
		v   int16
		err error
	)
	for i, text := range texts {
		if v, err = e.getInt16FromString(text); err != nil {
			return i, err
		}
		binary.LittleEndian.PutUint16(e.raw[i*uint16ByteSize:], uint16(v))
	}
	return len(texts), nil
}

func (e *Enum16ColumnData) get(row int) int16 {
	return int16(bufferRowToUint16(e.raw, row))
}

func (e *Enum16ColumnData) GetValue(row int) interface{} {
	return e.GetString(row)
}

func (e *Enum16ColumnData) GetString(row int) string {
	return e.itoa[e.get(row)]
}

func (e *Enum16ColumnData) Zero() interface{} {
	return emptyString
}

func (e *Enum16ColumnData) ZeroString() string {
	return zeroString
}

func (e *Enum16ColumnData) Len() int {
	return len(e.raw) / 2
}

func (e *Enum16ColumnData) Close() error {
	bytepool.PutBytes(e.raw)
	return nil
}

func (e *Enum16ColumnData) getInt16FromString(s string) (int16, error) {
	s = processString(s)
	v, ok := e.atoi[s]
	// If not alr in atoi -> then s might be an int16 of the enum value
	// Parse s to get int16
	if !ok {
		i, err := strconv.ParseInt(s, 10, 16)
		if err != nil {
			return 0, errors.ErrorfWithCaller(unknownStringValue, s, e.atoi)
		}
		if _, ok := e.itoa[int16(i)]; !ok {
			return 0, errors.ErrorfWithCaller(unknownInt16Value, s, e.itoa)
		}
		return int16(i), nil
	}
	return v, nil
}

// interpretEnum16Type converts the string or int8 or in16 to the int16 in the enum
// interpretEnum16Type returns a function to avoid unnecessary switch case computation
func (e *Enum16ColumnData) interpretEnum16Type(value interface{}) (func(value interface{}) (int16, error), error) {
	switch value := value.(type) {
	case string:
		return func(value interface{}) (int16, error) {
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
	case int16:
		return func(value interface{}) (int16, error) {
			// Have to recompute value of ok even if match to ensure that subsequent values matches same type
			v16, ok := value.(int16)
			if !ok {
				return 0, NewErrInvalidColumnType(value, v16)
			}

			if _, ok = e.itoa[v16]; !ok {
				return 0, errors.ErrorfWithCaller(unknownInt16Value, v16, e.itoa)
			}

			return v16, nil
		}, nil
	case int8:
		return func(value interface{}) (int16, error) {
			// Have to recompute value of ok even if match to ensure that subsequent values matches same type
			v8, ok := value.(int8)
			if !ok {
				return 0, NewErrInvalidColumnType(value, v8)
			}

			v16 := int16(v8)
			if _, ok = e.itoa[v16]; !ok {
				return 0, errors.ErrorfWithCaller(unknownInt16Value, v16, e.itoa)
			}

			return v16, nil
		}, nil
	default:
		return nil, NewErrInvalidColumnTypeCustomText(fmt.Sprintf("invalid column data type, current = %T, expected = %T or %T", value, int16(0), ""))
	}
}
