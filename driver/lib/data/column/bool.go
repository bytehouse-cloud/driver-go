package column

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
	derrors "github.com/bytehouse-cloud/driver-go/errors"
)

var supportedTrueBool = map[string]struct{}{
	"1":         {},
	"'1'":       {},
	"true":      {},
	"'true'":    {},
	"'t'":       {},
	"'y'":       {},
	"'yes'":     {},
	"'on'":      {},
	"'enable'":  {},
	"'enabled'": {},
}

var supportedFalseBool = map[string]struct{}{
	"0":          {},
	"'0'":        {},
	"false":      {},
	"'false'":    {},
	"'f'":        {},
	"'n'":        {},
	"'no'":       {},
	"'off'":      {},
	"'disable'":  {},
	"'disabled'": {},
}

type BoolColumnData struct {
	raw      []byte
	isClosed bool
}

func (u *BoolColumnData) ReadFromDecoder(decoder *ch_encoding.Decoder) error {
	_, err := decoder.Read(u.raw)
	return err
}

func (u *BoolColumnData) WriteToEncoder(encoder *ch_encoding.Encoder) error {
	_, err := encoder.Write(u.raw)
	return err
}

func (u *BoolColumnData) ReadFromValues(values []interface{}) (int, error) {
	var (
		v  bool
		ok bool
	)

	for idx, value := range values {
		if value == nil {
			u.raw[idx] = 0
			continue
		}

		v, ok = value.(bool)
		if !ok {
			return idx, NewErrInvalidColumnType(value, v)
		}

		var valUInt uint8
		if v {
			valUInt = 1
		}
		u.raw[idx] = valUInt
	}
	return len(values), nil
}

func (u *BoolColumnData) ReadFromTexts(texts []string) (int, error) {
	for i, text := range texts {
		if isEmptyOrNull(text) {
			u.raw[i] = 0
			continue
		}

		var b uint8
		key := strings.ToLower(text)
		if _, ok := supportedTrueBool[key]; ok {
			b = 1
		} else if _, ok = supportedFalseBool[key]; ok {
			b = 0
		} else {
			return i, derrors.ErrorfWithCaller("%v", fmt.Errorf("cannot parse boolean value here: '%s', should be true/false, 1/0 or True/False/T/F/Y/N/Yes/No/On/Off/Enable/Disable/Enabled/Disabled/1/0 in quotes", text))
		}
		u.raw[i] = b
	}
	return len(texts), nil
}

func (u *BoolColumnData) Zero() interface{} {
	return 0
}

func (u *BoolColumnData) ZeroString() string {
	return "0"
}

func (u *BoolColumnData) get(row int) uint8 {
	return u.raw[row]
}

func (u *BoolColumnData) GetValue(row int) interface{} {
	return u.get(row)
}

func (u *BoolColumnData) GetString(row int) string {
	return strconv.FormatUint(uint64(u.get(row)), 10)
}

func (u *BoolColumnData) Len() int {
	return len(u.raw)
}

func (u *BoolColumnData) Close() error {
	if u.isClosed {
		return nil
	}
	u.isClosed = true
	bytepool.PutBytes(u.raw)
	return nil
}
