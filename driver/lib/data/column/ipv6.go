package column

import (
	"net"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
	"github.com/bytehouse-cloud/driver-go/errors"
)

const (
	invalidIPv6String = "invalid IPv6 string: %s, expected format: 2001:0db8:85a3:0000:0000:8a2e:0370:7334"
	zeroIPv6String    = "::"
)

type IPv6ColumnData struct {
	raw      []byte
	isClosed bool
}

func (i *IPv6ColumnData) ReadFromDecoder(decoder *ch_encoding.Decoder) error {
	_, err := decoder.Read(i.raw)
	return err
}

func (i *IPv6ColumnData) WriteToEncoder(encoder *ch_encoding.Encoder) error {
	_, err := encoder.Write(i.raw)
	return err
}

func (i *IPv6ColumnData) ReadFromValues(values []interface{}) (int, error) {
	var (
		v  net.IP
		ok bool
	)

	// Assign rest of values
	for idx, value := range values {
		if value == nil {
			copy(i.raw[idx*net.IPv6len:], net.IPv6zero)
			continue
		}

		v, ok = value.(net.IP)
		if !ok {
			return idx, NewErrInvalidColumnType(value, v)
		}
		copy(i.raw[idx*net.IPv6len:], v)
	}

	return len(values), nil
}

func (i *IPv6ColumnData) ReadFromTexts(texts []string) (int, error) {
	for idx, text := range texts {
		if isEmptyOrNull(text) {
			copy(i.raw[idx*net.IPv6len:], net.IPv6zero)
			continue
		}

		text = processString(text)
		ip := net.ParseIP(text).To16()
		if ip == nil {
			return idx, errors.ErrorfWithCaller(invalidIPv6String, text)
		}
		copy(i.raw[idx*net.IPv6len:], ip)
	}
	return len(texts), nil
}

func (i *IPv6ColumnData) get(row int) net.IP {
	return getRowRaw(i.raw, row, net.IPv6len)
}

func (i *IPv6ColumnData) GetValue(row int) interface{} {
	return i.get(row)
}

func (i *IPv6ColumnData) GetString(row int) string {
	return i.get(row).String()
}

func (i *IPv6ColumnData) Zero() interface{} {
	return net.IPv6zero
}

func (i *IPv6ColumnData) ZeroString() string {
	return zeroIPv6String
}

func (i *IPv6ColumnData) Len() int {
	return len(i.raw) / net.IPv6len
}

func (i *IPv6ColumnData) Close() error {
	if i.isClosed {
		return nil
	}
	i.isClosed = true
	bytepool.PutBytes(i.raw)
	return nil
}
