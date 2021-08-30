package column

import (
	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

type NothingColumnData struct {
	raw []byte
}

func (n *NothingColumnData) ReadFromValues(values []interface{}) (int, error) {
	return 0, nil
}

func (n *NothingColumnData) ReadFromTexts(texts []string) (int, error) {
	return 0, nil
}

func (n *NothingColumnData) ReadFromDecoder(decoder *ch_encoding.Decoder) error {
	_, err := decoder.Read(n.raw)
	return err
}

func (n *NothingColumnData) WriteToEncoder(encoder *ch_encoding.Encoder) error {
	_, err := encoder.Write(n.raw)
	return err
}

func (n *NothingColumnData) GetValue(row int) interface{} {
	return nil
}

func (n *NothingColumnData) GetString(row int) string {
	return emptyString
}

func (n *NothingColumnData) Zero() interface{} {
	return nil
}

func (n *NothingColumnData) ZeroString() string {
	return emptyString
}

func (n *NothingColumnData) Len() int {
	return len(n.raw)
}

func (n *NothingColumnData) Close() error {
	bytepool.PutBytes(n.raw)
	return nil
}
