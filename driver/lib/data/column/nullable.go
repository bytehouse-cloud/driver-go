package column

import (
	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

const (
	NULLDisplay = "ᴺᵁᴸᴸ"
	NULL        = "NULL"
	NULLSmall   = "null"
	NULLAlt     = "\\N"
)

type NullableColumnData struct {
	mask            []byte
	innerColumnData CHColumnData
	isClosed        bool
}

func (n *NullableColumnData) ReadFromDecoder(decoder *ch_encoding.Decoder) error {
	if _, err := decoder.Read(n.mask); err != nil {
		return err
	}
	return n.innerColumnData.ReadFromDecoder(decoder)
}

func (n *NullableColumnData) WriteToEncoder(encoder *ch_encoding.Encoder) error {
	if _, err := encoder.Write(n.mask); err != nil {
		return err
	}
	return n.innerColumnData.WriteToEncoder(encoder)
}

func (n *NullableColumnData) ReadFromValues(values []interface{}) (int, error) {
	valuesCopy := make([]interface{}, len(values))
	copy(valuesCopy, values)

	dummyValue := n.innerColumnData.Zero()
	for i, value := range valuesCopy {
		if value == nil {
			n.mask[i] = 1
			valuesCopy[i] = dummyValue
		}
	}

	return n.innerColumnData.ReadFromValues(valuesCopy)
}

func (n *NullableColumnData) ReadFromTexts(texts []string) (int, error) {
	textsCopy := make([]string, len(texts))
	copy(textsCopy, texts)

	dummyString := n.innerColumnData.ZeroString()
	for i, text := range textsCopy {
		if isNull(text) {
			n.mask[i] = 1
			textsCopy[i] = dummyString
		}
	}

	return n.innerColumnData.ReadFromTexts(textsCopy)
}

func (n *NullableColumnData) GetValue(row int) interface{} {
	if n.mask[row] == 0 {
		return n.innerColumnData.GetValue(row)
	}
	return nil
}

func (n *NullableColumnData) GetString(row int) string {
	if n.mask[row] == 0 {
		return n.innerColumnData.GetString(row)
	}
	return NULLDisplay
}

func (n *NullableColumnData) GetInnerColumnData() CHColumnData {
	return n.innerColumnData
}

func (n *NullableColumnData) Zero() interface{} {
	return n.innerColumnData.Zero()
}

func (n *NullableColumnData) ZeroString() string {
	return emptyString
}

func (n *NullableColumnData) Len() int {
	return len(n.mask)
}

func (n *NullableColumnData) Close() error {
	if n.isClosed {
		return nil
	}
	n.isClosed = true
	bytepool.PutBytes(n.mask)
	return n.innerColumnData.Close()
}

func isNull(text string) bool {
	switch text {
	case NULL, NULLSmall, NULLAlt, NULLDisplay:
		return true
	}
	return false
}

func isEmptyOrNull(text string) bool {
	return text == "" || isNull(text)
}
