package ch_encoding

import (
	"encoding/binary"
	"io"

	"github.com/dennwc/varint"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/lz4"
	"github.com/bytehouse-cloud/driver-go/errors"
)

type compressReader struct {
	reader io.Reader
	// data uncompressed
	data []byte
	// data position
	pos int
	// data compressed
	zdata []byte
	// lz4 headers
	header []byte
}

// NewCompressReader wrap the io.Reader
func NewCompressReader(r io.Reader) *compressReader {
	p := &compressReader{
		reader: r,
		header: bytepool.GetBytesWithLen(HeaderSize),
	}
	p.data = bytepool.GetBytes(0, BlockMaxSize)

	zlen := lz4.CompressBound(BlockMaxSize) + HeaderSize
	p.zdata = bytepool.GetBytes(0, zlen)

	p.pos = len(p.data)
	return p
}

func (cr *compressReader) ReadUvarint() (uint64, error) {
	if len(cr.data)-cr.pos < varint.MaxLen64 {
		return binary.ReadUvarint(cr)
	}
	uVar, n := varint.Uvarint(cr.data[cr.pos:])
	cr.pos += n
	return uVar, nil
}

func (cr *compressReader) Read(buf []byte) (n int, err error) {
	var bytesRead = 0
	n = len(buf)

	if cr.pos < len(cr.data) {
		copiedSize := copy(buf, cr.data[cr.pos:])

		bytesRead += copiedSize
		cr.pos += copiedSize
	}

	for bytesRead < n {
		if err = cr.readCompressedData(); err != nil {
			return bytesRead, err
		}
		copiedSize := copy(buf[bytesRead:], cr.data)

		bytesRead += copiedSize
		cr.pos = copiedSize
	}
	return n, nil
}

func (cr *compressReader) ReadByte() (byte, error) {
	if cr.pos >= len(cr.data) {
		if err := cr.readCompressedData(); err != nil {
			return 0, err
		}
	}

	b := cr.data[cr.pos]
	cr.pos++
	return b, nil
}

func (cr *compressReader) readCompressedData() (err error) {
	cr.pos = 0
	var n int
	n, err = cr.reader.Read(cr.header)
	if err != nil {
		return
	}
	if n != len(cr.header) {
		return errors.ErrorfWithCaller("lz4 decompression header EOF")
	}

	compressedSize := int(binary.LittleEndian.Uint32(cr.header[17:])) - 9
	decompressedSize := int(binary.LittleEndian.Uint32(cr.header[21:]))

	if compressedSize > cap(cr.zdata) {
		bytepool.PutBytes(cr.zdata)
		cr.zdata = bytepool.GetBytes(0, compressedSize)
	}
	if decompressedSize > cap(cr.data) {
		bytepool.PutBytes(cr.data)
		cr.data = bytepool.GetBytes(0, decompressedSize)
	}

	cr.zdata = cr.zdata[:compressedSize]
	cr.data = cr.data[:decompressedSize]

	// @TODO checksum
	if cr.header[16] == LZ4 {
		n, err = cr.reader.Read(cr.zdata)
		if err != nil {
			return
		}

		if n != len(cr.zdata) {
			return errors.ErrorfWithCaller("decompress read size does not match")
		}

		_, err = lz4.Decode(cr.data, cr.zdata)
		if err != nil {
			return
		}
	} else {
		return errors.ErrorfWithCaller("unknown compression method: 0x%02x ", cr.header[16])
	}

	return nil
}
