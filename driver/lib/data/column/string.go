package column

import (
	"github.com/jfcg/sixb"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

const initialBufferLen = 4096

type StringColumnData struct {
	buf      [][]byte
	raw      [][]byte
	isClosed bool
}

func (s *StringColumnData) ReadFromDecoder(decoder *ch_encoding.Decoder) error {
	s.buf = [][]byte{bytepool.GetBytesWithLen(initialBufferLen)}
	var (
		bufferOffset int
		bufferIndex  int
		lastRead     int
		stringLen    int
	)

	for i := range s.raw {
		u, err := decoder.Uvarint()
		if err != nil {
			return err
		}
		stringLen = int(u)
		if stringLen > len(s.buf[bufferIndex])-bufferOffset {
			newSize := 2 * len(s.buf[bufferIndex])
			for stringLen > newSize {
				newSize *= 2
			}
			s.buf = append(s.buf, bytepool.GetBytesWithLen(newSize))
			bufferOffset = 0
			bufferIndex++
		}

		s.raw[i] = s.buf[bufferIndex][bufferOffset : bufferOffset+stringLen]
		lastRead, err = decoder.Read(s.raw[i])
		if err != nil {
			return err
		}
		bufferOffset += lastRead
	}
	return nil
}

func (s *StringColumnData) WriteToEncoder(encoder *ch_encoding.Encoder) error {
	var err error
	for _, b := range s.raw {
		if err = encoder.Uvarint(uint64(len(b))); err != nil {
			return err
		}
		if _, err = encoder.Write(b); err != nil {
			return err
		}
	}
	return nil
}

func (s *StringColumnData) ReadFromValues(values []interface{}) (int, error) {
	for i, value := range values {
		v, ok := value.(string)
		if !ok {
			return i, NewErrInvalidColumnType(value, v)
		}

		s.raw[i] = sixb.StoB(v)
	}

	return len(values), nil
}

func (s *StringColumnData) ReadFromTexts(texts []string) (int, error) {
	for i, text := range texts {
		text = processString(text)
		s.raw[i] = make([]byte, len(text))
		copy(s.raw[i], text)
	}
	return len(texts), nil
}

func (s *StringColumnData) GetValue(row int) interface{} {
	return s.GetString(row)
}

func (s *StringColumnData) GetString(row int) string {
	return string(s.raw[row])
}

func (s *StringColumnData) Zero() interface{} {
	return emptyString
}

func (s *StringColumnData) ZeroString() string {
	return emptyString
}

func (s *StringColumnData) Len() int {
	return len(s.raw)
}

func (s *StringColumnData) Close() error {
	if s.isClosed {
		return nil
	}
	s.isClosed = true
	for _, b := range s.buf {
		bytepool.PutBytes(b)
	}
	return nil
}
