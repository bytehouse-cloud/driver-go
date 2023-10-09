package column

import (
	"net"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
	"github.com/bytehouse-cloud/driver-go/errors"
)

const (
	invalidIPv4String = "invalid IPv4 string: %s, expected format: 39.109.234.162"
	zeroIPv4String    = "0.0.0.0"
)

type IPv4ColumnData struct {
	raw      []byte
	isClosed bool
}

func (i *IPv4ColumnData) ReadFromDecoder(decoder *ch_encoding.Decoder) error {
	_, err := decoder.Read(i.raw)
	return err
}

func (i *IPv4ColumnData) WriteToEncoder(encoder *ch_encoding.Encoder) error {
	_, err := encoder.Write(i.raw)
	return err
}

func (i *IPv4ColumnData) ReadFromValues(values []interface{}) (int, error) {
	var (
		v  net.IP
		ok bool
	)

	// Assign rest of values
	for idx, value := range values {
		if value == nil {
			copy(i.raw[idx*net.IPv4len:], net.IPv4zero)
			continue
		}

		v, ok = value.(net.IP)
		if !ok {
			return idx, NewErrInvalidColumnType(value, v)
		}
		v4Address := v.To4()
		if v4Address == nil {
			return 0, NewErrInvalidColumnTypeCustomText("expected ipv4, current is net.IP but not ipv4")
		}
		copy(i.raw[idx*net.IPv4len:], v4Address)
	}

	return len(values), nil
}

func (i *IPv4ColumnData) ReadFromTexts(texts []string) (int, error) {
	var ip net.IP

	for idx, text := range texts {
		if isEmptyOrNull(text) {
			copy(i.raw[idx*net.IPv4len:], net.IPv4zero)
			continue
		}

		text = processString(text)
		ip = net.ParseIP(text).To4()
		if ip == nil {
			return idx, errors.ErrorfWithCaller(invalidIPv4String, text)
		}
		copy(i.raw[idx*net.IPv4len:], ip)
	}

	return len(texts), nil
}

func (i *IPv4ColumnData) get(row int) net.IP {
	return getRowRaw(i.raw, row, net.IPv4len)
}

func (i *IPv4ColumnData) GetValue(row int) interface{} {
	return i.get(row)
}

func (i *IPv4ColumnData) GetString(row int) string {
	return i.get(row).String()
}

func (i *IPv4ColumnData) Zero() interface{} {
	return net.IPv4zero
}

func (i *IPv4ColumnData) ZeroString() string {
	return zeroIPv4String
}

func (i *IPv4ColumnData) Len() int {
	return len(i.raw) / net.IPv4len
}

func (i *IPv4ColumnData) Close() error {
	if i.isClosed {
		return nil
	}
	i.isClosed = true
	bytepool.PutBytes(i.raw)
	return nil
}
