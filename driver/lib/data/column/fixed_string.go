package column

import (
	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
	"github.com/bytehouse-cloud/driver-go/errors"
)

const (
	stringLenTooLong = "data for fixed string column is too long: %v, expected only %v characters"
)

type FixedStringColumnData struct {
	raw      []byte
	mask     []byte
	isClosed bool
}

func (f *FixedStringColumnData) ReadFromDecoder(decoder *ch_encoding.Decoder) error {
	_, err := decoder.Read(f.raw)
	return err
}

func (f *FixedStringColumnData) WriteToEncoder(encoder *ch_encoding.Encoder) error {
	_, err := encoder.Write(f.raw)
	return err
}

func (f *FixedStringColumnData) ReadFromValues(values []interface{}) (int, error) {
	sLen := len(f.mask)
	var (
		n    int
		diff int
		v    string
		ok   bool
	)

	for i, value := range values {
		v, ok = value.(string)
		if !ok {
			return i, NewErrInvalidColumnType(value, v)
		}
		if len(v) > sLen {
			return i, errors.ErrorfWithCaller(stringLenTooLong, v, sLen)
		}
		n = copy(f.raw[i*sLen:], v)
		diff = sLen - n
		copy(f.raw[i*sLen+n:], f.mask[:diff])
	}

	return len(values), nil
}

func (f *FixedStringColumnData) ReadFromTexts(texts []string) (int, error) {
	sLen := len(f.mask)
	var (
		n    int
		diff int
	)

	for i, text := range texts {
		text = processString(text)
		if len(text) > sLen {
			return i, errors.ErrorfWithCaller(stringLenTooLong, text, sLen)
		}
		n = copy(f.raw[i*sLen:], text)
		diff = sLen - n
		copy(f.raw[i*sLen+n:], f.mask[:diff])
	}

	return len(texts), nil
}

func (f *FixedStringColumnData) GetValue(row int) interface{} {
	return f.GetString(row)
}

func (f *FixedStringColumnData) GetString(row int) string {
	rowBytes := getRowRaw(f.raw, row, len(f.mask))
	rowBytes = removeNullBytes(rowBytes)
	return string(rowBytes)
}

func (f *FixedStringColumnData) Zero() interface{} {
	return emptyString
}

func (f *FixedStringColumnData) ZeroString() string {
	return emptyString
}

func (f *FixedStringColumnData) Len() int {
	return len(f.raw) / len(f.mask)
}

func (f *FixedStringColumnData) Close() error {
	if f.isClosed {
		return nil
	}
	f.isClosed = true
	bytepool.PutBytes(f.mask)
	bytepool.PutBytes(f.raw)
	return nil
}

func removeNullBytes(buf []byte) []byte {
	end := len(buf) - 1
	for end >= 0 {
		if buf[end] != 0 {
			break
		}
		end--
	}
	return buf[:end+1]
}

// func (f *FixedStringColumnData) processFixedString(s string) (string, error) {
//	if len(s) > f.stringLen {
//		return s[:f.stringLen], nil // todo: solve this temporary fix
//	}
//	return s, nil
// }
