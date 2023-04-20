package bytepool

import (
	"encoding/binary"
	"io"

	"github.com/dennwc/varint"
)

const (
	defaultBufferSize  = 4096
	defaultChannelSize = 8
)

type ZReader struct {
	r          io.Reader
	buffer     []byte
	offset     int
	forReceive chan []byte
	forRead    chan []byte
	exception  error
}

func NewZReaderDefault(r io.Reader) *ZReader {
	return NewZReader(r, defaultBufferSize, defaultChannelSize)
}

func NewZReader(r io.Reader, bufferSize, channelSize int) *ZReader {
	z := &ZReader{
		r:          r,
		buffer:     GetBytes(0, bufferSize),
		forReceive: make(chan []byte, channelSize+1),
		forRead:    make(chan []byte, channelSize+1),
	}
	for i := 0; i < channelSize; i++ {
		z.forReceive <- GetBytesWithLen(bufferSize)
	}

	go z.startRead()

	return z
}

func (z *ZReader) startRead() {
	defer close(z.forRead)

	if zBuffer, ok := z.r.(*ZBuffer); ok {
		z.startReadZBuffer(zBuffer)
		return
	}

	for buf := range z.forReceive {
		n, err := z.r.Read(buf)
		if err != nil {
			if n > 0 {
				z.forRead <- buf[:n]
			}
			z.exception = err
			return
		}
		z.forRead <- buf[:n]
	}
}

func (z *ZReader) Read(p []byte) (int, error) {
	if err := z.fillBufferIfEmpty(); err != nil {
		return 0, err
	}

	n := copy(p, z.buffer[z.offset:])
	z.offset += n
	return n, nil
}

func (z *ZReader) ReadFull(p []byte) error {
	var totalRead int
	for totalRead < len(p) {
		if err := z.fillBufferIfEmpty(); err != nil {
			return err
		}

		n := copy(p[totalRead:], z.buffer[z.offset:])
		z.offset += n
		totalRead += n
	}
	return nil
}

func (z *ZReader) ReadUvarint() (uint64, error) {
	if z.bufferBalance() < binary.MaxVarintLen64 {
		return binary.ReadUvarint(z)
	}
	uVar, n := varint.Uvarint(z.buffer[z.offset:])
	z.offset += n
	return uVar, nil
}

// fillBufferIfEmpty attempts to fill the buffer once
func (z *ZReader) fillBufferIfEmpty() error {
	if z.bufferBalance() > 0 { //buffer is not empty
		return nil
	}

	buf, ok := <-z.forRead
	if !ok {
		return z.exception
	}
	z.forReceive <- z.buffer[:cap(z.buffer)]
	z.buffer = buf
	z.offset = 0
	return nil
}

// bufferBalance reports if the number of bytes remaining in the current buffer
func (z *ZReader) bufferBalance() int {
	return len(z.buffer) - z.offset
}

func (z *ZReader) ReadByte() (byte, error) {
	if err := z.fillBufferIfEmpty(); err != nil {
		return 0, err
	}
	b := z.buffer[z.offset]
	z.offset++
	return b, nil
}

// ReadNextBuffer returns the remaining buffer if not empty, else attempts to read from the next buffer in channel
func (z *ZReader) ReadNextBuffer() ([]byte, error) {
	if err := z.fillBufferIfEmpty(); err != nil {
		return nil, err
	}
	r := z.buffer[z.offset:]
	z.offset = len(z.buffer)
	return r, nil
}

func (z *ZReader) UnreadCurrentBuffer(n int) {
	z.offset -= n
}

func (z *ZReader) PrependCurrentBuffer(pre []byte) {
	// If have enough space behind offset, copy over pre to before offset
	if len(pre) <= z.offset {
		z.offset -= copy(z.buffer[z.offset-len(pre):z.offset], pre)
		return
	}

	// Else flush original data all the way to right
	z.shiftDataInBufferRight()
	if len(pre) <= z.offset {
		z.offset -= copy(z.buffer[z.offset-len(pre):z.offset], pre)
		return
	}

	// If still not enough space make a new buffer
	newBuffer := make([]byte, len(pre)+z.bufferBalance())
	copy(newBuffer, pre)
	copy(newBuffer[len(pre):], z.buffer[z.offset:])
	z.buffer = newBuffer
	z.offset = 0
}

// shiftDataInBufferRight shifts data in the buffer to the rightmost if buffer has additional capacity
// It also updates the offset accordingly
func (z *ZReader) shiftDataInBufferRight() {
	b := z.bufferBalance()
	l := len(z.buffer)
	z.buffer = z.buffer[:cap(z.buffer)]
	newOffset := len(z.buffer) - b
	copy(z.buffer[newOffset:], z.buffer[z.offset:l])
	z.offset = newOffset
}

func (z *ZReader) Close() error {
	close(z.forReceive)

	if closer, ok := z.r.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func (z *ZReader) startReadZBuffer(zBuf *ZBuffer) {
	for waste := range z.forReceive {
		buf, err := zBuf.directRead(waste)
		if err != nil {
			z.exception = err
			return
		}
		z.forRead <- buf
	}
}
