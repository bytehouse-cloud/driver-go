package ch_encoding

import (
	"encoding/binary"
	"io"
	"math"

	"github.com/jfcg/sixb"
)

type Encoder struct {
	compress       bool
	output         io.Writer
	compressOutput io.Writer
	scratch        [binary.MaxVarintLen64]byte
}

func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{
		output: w,
	}
}

func NewEncoderWithCompress(w io.Writer) *Encoder {
	return &Encoder{
		output:         w,
		compressOutput: NewCompressWriter(w),
	}
}

// Write writes len(p) bytes from p to the output data stream
func (enc *Encoder) Write(p []byte) (n int, err error) {
	return enc.GetOutput().Write(p)
}

func (enc *Encoder) GetOutput() io.Writer {
	if enc.compress && enc.compressOutput != nil {
		return enc.compressOutput
	}
	return enc.output
}

func (enc *Encoder) SelectCompress(compress bool) {
	if enc.compressOutput == nil {
		return
	}
	if enc.compress && !compress {
		_ = enc.Flush()
	}
	enc.compress = compress
}

func (enc *Encoder) IsCompressed() bool {
	return enc.compress
}

func (enc *Encoder) Uvarint(i uint64) error {
	n := binary.PutUvarint(enc.scratch[:], i)
	_, err := enc.GetOutput().Write(enc.scratch[:n])
	return err
}

func (enc *Encoder) String(s string) error {
	if err := enc.Uvarint(uint64(len(s))); err != nil {
		return err
	}
	if _, err := enc.GetOutput().Write(sixb.StB(s)); err != nil {
		return err
	}
	return nil
}

func (enc *Encoder) Flush() error {
	if f, ok := enc.GetOutput().(Flusher); ok {
		return f.Flush()
	}
	return nil
}

type Flusher interface {
	Flush() error
}

func (enc *Encoder) Bool(b bool) error {
	enc.scratch[0] = 0
	if b {
		enc.scratch[0]++
	}
	_, err := enc.Write(enc.scratch[:1])
	return err
}

func (enc *Encoder) Float64(f float64) error {
	return enc.UInt64(math.Float64bits(f))
}

func (enc *Encoder) Float32(f float32) error {
	return enc.UInt32(math.Float32bits(f))
}

func (enc *Encoder) UInt64(v uint64) error {
	binary.LittleEndian.PutUint64(enc.scratch[:8], v)
	_, err := enc.GetOutput().Write(enc.scratch[:8])
	return err
}

func (enc *Encoder) UInt32(v uint32) error {
	binary.LittleEndian.PutUint32(enc.scratch[:4], v)
	_, err := enc.GetOutput().Write(enc.scratch[:4])
	return err
}

func (enc *Encoder) Int32(v int32) error {
	return enc.UInt32(uint32(v))
}

type WriteFlusher interface {
	Flush() error
}
