package column

import (
	"encoding/binary"
	"math"
	"strconv"

	"github.com/valyala/fastjson/fastfloat"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

const float32ByteSize = 4

// Float32ColumnData handles float32 column types
// Float32ColumnData doesn't guarantee precision of values larger then MaxFloat32
type Float32ColumnData struct {
	raw      []byte
	isClosed bool
}

func (f *Float32ColumnData) ReadFromDecoder(decoder *ch_encoding.Decoder) error {
	_, err := decoder.Read(f.raw)
	return err
}

func (f *Float32ColumnData) WriteToEncoder(encoder *ch_encoding.Encoder) error {
	_, err := encoder.Write(f.raw)
	return err
}

func (f *Float32ColumnData) ReadFromValues(values []interface{}) (int, error) {
	var (
		v  float32
		ok bool
	)

	for i, value := range values {
		v, ok = value.(float32)
		if !ok {
			return i, NewErrInvalidColumnType(value, v)
		}
		binary.LittleEndian.PutUint32(f.raw[i*float32ByteSize:], math.Float32bits(v))
	}

	return len(values), nil
}

func (f *Float32ColumnData) ReadFromTexts(texts []string) (int, error) {
	var (
		v   float64
		err error
	)

	for i, text := range texts {
		if text == "" {
			binary.LittleEndian.PutUint32(f.raw[i*float32ByteSize:], math.Float32bits(0))
			continue
		}

		v, err = fastfloat.Parse(text)
		if err != nil {
			return i, err
		}
		binary.LittleEndian.PutUint32(f.raw[i*float32ByteSize:], math.Float32bits(float32(v)))
	}
	return len(texts), nil
}

func (f *Float32ColumnData) get(row int) float32 {
	return math.Float32frombits(bufferRowToUint32(f.raw, row))
}

func (f *Float32ColumnData) GetValue(row int) interface{} {
	return f.get(row)
}

func (f *Float32ColumnData) GetString(row int) string {
	return strconv.FormatFloat(float64(f.get(row)), defaultFloatFormat, -1, 32)
}

func (f *Float32ColumnData) Zero() interface{} {
	return float32(0)
}

func (f *Float32ColumnData) ZeroString() string {
	return zeroString
}

func (f *Float32ColumnData) Len() int {
	return len(f.raw) / 4
}

func (f *Float32ColumnData) Close() error {
	if f.isClosed {
		return nil
	}
	f.isClosed = true
	bytepool.PutBytes(f.raw)
	return nil
}
