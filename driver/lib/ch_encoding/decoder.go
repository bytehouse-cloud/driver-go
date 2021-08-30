package ch_encoding

import (
	"encoding/binary"
	"io"

	"github.com/jfcg/sixb"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
)

type Decoder struct {
	compress          bool
	input             io.Reader
	compressInput     io.Reader
	uvarInput         UvarintReader
	compressUvarInput UvarintReader
	scratch           [binary.MaxVarintLen64]byte
}

func NewDecoder(input io.Reader) *Decoder {
	uvarInput, ok := input.(UvarintReader)
	if !ok {
		// slowUvarintReader is used if input doesn't implement it's own UVarintReader
		uvarInput = newSlowUvarintReader(input)
	}

	return &Decoder{
		input:     input,
		uvarInput: uvarInput,
	}
}

func NewDecoderWithCompress(input io.Reader) *Decoder {
	cr := NewCompressReader(input)

	uvarInput, ok := input.(UvarintReader)
	if !ok {
		// slowUvarintReader is used if input doesn't implement it's own UVarintReader
		uvarInput = newSlowUvarintReader(input)
	}

	return &Decoder{
		input:             input,
		compressInput:     cr,
		uvarInput:         uvarInput,
		compressUvarInput: cr,
	}
}

// Read reads up to len(p) bytes into input reader
func (dec *Decoder) Read(b []byte) (int, error) {
	return dec.GetInput().Read(b)
}

func (dec *Decoder) GetInput() io.Reader {
	if dec.compress && dec.compressInput != nil {
		return dec.compressInput
	}
	return dec.input
}

func (dec *Decoder) GetUvarInput() UvarintReader {
	if dec.compress && dec.compressUvarInput != nil {
		return dec.compressUvarInput
	}
	return dec.uvarInput
}

func (dec *Decoder) IsCompressed() bool {
	return dec.compress
}

func (dec *Decoder) SetCompress(compress bool) {
	dec.compress = compress
}

func (dec *Decoder) Uvarint() (uint64, error) {
	return dec.GetUvarInput().ReadUvarint()
}

func (dec *Decoder) String() (string, error) {
	n, err := dec.Uvarint()
	if err != nil {
		return emptyString, err
	}
	b := bytepool.GetBytesWithLen(int(n))
	_, err = dec.Read(b)
	if err != nil {
		return emptyString, err
	}

	return sixb.BtS(b), nil
}

func (dec *Decoder) UInt32() (uint32, error) {
	if _, err := dec.GetInput().Read(dec.scratch[:4]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(dec.scratch[:4]), nil
}

func (dec *Decoder) Bool() (bool, error) {
	if _, err := dec.GetInput().Read(dec.scratch[:1]); err != nil {
		return false, err
	}
	return dec.scratch[0] > 0, nil
}

func (dec *Decoder) Int32() (int32, error) {
	i, err := dec.UInt32()
	return int32(i), err
}

func (dec *Decoder) UInt64() (uint64, error) {
	if _, err := dec.GetInput().Read(dec.scratch[:8]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(dec.scratch[:8]), nil
}

type UvarintReader interface {
	ReadUvarint() (uint64, error)
}

const (
	emptyString = ""
)
