package bytepool

import (
	"io"
)

// ZBuffer attempts to reuse all byte bufferStream without wastage,
// aims to have zero []byte being garbage collected.
type ZBuffer struct {
	bufSize      int
	bufferStream chan []byte
	wasteStream  chan []byte

	readBuffer  []byte
	readOffset  int
	writeBuffer []byte
	writeOffset int
	isClosed    bool
}

func NewZBufferDefault() *ZBuffer {
	return NewZBuffer(defaultBufferSize, defaultChannelSize)
}

// NewZBuffer returns a *ZBuffer with given fixed buffer size and max buffer count
func NewZBuffer(bufferSize, channelSize int) *ZBuffer {
	newZBuf := &ZBuffer{
		bufSize:      bufferSize,
		writeBuffer:  GetBytesWithLen(bufferSize),
		readBuffer:   GetBytesWithLen(bufferSize),
		readOffset:   bufferSize,
		bufferStream: make(chan []byte, channelSize),
		wasteStream:  make(chan []byte, channelSize),
	}

	for i := 0; i < channelSize; i++ {
		newZBuf.wasteStream <- GetBytesWithLen(bufferSize)
	}
	return newZBuf
}

// Write appends contents of src to buffer, increase the size of buffer when needed.
// Blocks if buffer is full.
func (z *ZBuffer) Write(src []byte) (int, error) {
	var totalWrite int
	for totalWrite < len(src) {
		n, err := z.writeToBuffer(src[totalWrite:])
		totalWrite += n
		if err != nil {
			return totalWrite, err
		}
	}
	return totalWrite, nil
}

func (z *ZBuffer) writeToBuffer(src []byte) (int, error) {
	if z.hasFullWriteBuffer() {
		if err := z.Flush(); err != nil {
			return 0, err
		}
	}

	n := copy(z.writeBuffer[z.writeOffset:], src)
	z.writeOffset += n
	return n, nil
}

// WriteByte appends b to buffer, increase the size of buffer when needed.
func (z *ZBuffer) WriteByte(b byte) error {
	_, err := z.writeToBuffer([]byte{b})
	return err
}

// ReadFrom reads from r until io.EOF or error, expands the buffer when needed
func (z *ZBuffer) ReadFrom(r io.Reader) (int64, error) {
	defer z.Flush()

	var totalRead int64
	for {
		n, err := z.writeBufferFrom(r)
		totalRead += int64(n)
		if err == io.EOF {
			return totalRead, io.EOF
		}
		if err != nil {
			return totalRead, err
		}
	}
}

func (z *ZBuffer) writeBufferFrom(r io.Reader) (int, error) {
	if z.hasFullWriteBuffer() {
		if err := z.Flush(); err != nil {
			return 0, nil
		}
	}

	balance := z.writeBuffer[z.writeOffset:]
	n, err := r.Read(balance)
	z.writeOffset += n
	return n, err
}

// Read transfer content to des, draining the buffer.
// If buffer is completely drained, io.EOF is returned
func (z *ZBuffer) Read(des []byte) (int, error) {
	if err := z.checkReadBuffer(); err != nil {
		return 0, err
	}

	n := copy(des, z.readBuffer[z.readOffset:])
	z.readOffset += n
	return n, nil
}

// ReadByte reads 1 byte from buffer
// If buffer is drained, io.EOF is returned
func (z *ZBuffer) ReadByte() (byte, error) {
	if err := z.checkReadBuffer(); err != nil {
		return 0, err
	}
	b := z.readBuffer[z.readOffset]
	z.readOffset++
	return b, nil
}

// WriteTo writes to writer until buffer is closed
func (z *ZBuffer) WriteTo(w io.Writer) (int64, error) {
	var totalWrite int64
	for {
		n, err := z.readBufferTo(w)
		totalWrite += int64(n)
		if err == io.EOF {
			return totalWrite, io.EOF
		}
		if err != nil {
			return totalWrite, err
		}
	}
}

func (z *ZBuffer) readBufferTo(w io.Writer) (int, error) {
	if err := z.checkReadBuffer(); err != nil {
		return 0, err
	}

	n, err := w.Write(z.readBuffer[z.readOffset:])
	z.readOffset += n
	return n, err
}

// Close cleaned up the buffer, any operations after calling close will not be safe
func (z *ZBuffer) Close() error {
	if err := z.Flush(); err != nil {
		return err
	}

	z.isClosed = true

	go func() {
		close(z.bufferStream)
		putBytes(z.writeBuffer)

		close(z.wasteStream)
		PutBytes(z.readBuffer)
		PutBytesStream(z.wasteStream)
	}()

	return nil
}

func (z *ZBuffer) Flush() error {
	if z.writeOffset == 0 {
		return nil
	}
	return z.flushWriteBuffer()
}

func (z *ZBuffer) flushWriteBuffer() error {
	if z.isClosed {
		return io.ErrClosedPipe
	}

	buf := <-z.wasteStream
	z.bufferStream <- z.writeBuffer[:z.writeOffset]
	z.writeBuffer = buf
	z.writeOffset = 0
	return nil
}

func (z *ZBuffer) hasFullWriteBuffer() bool {
	return len(z.writeBuffer) == z.writeOffset
}

func (z *ZBuffer) fillReadBuffer() error {
	buf, ok := <-z.bufferStream
	if !ok {
		return io.EOF
	}

	z.wasteStream <- z.readBuffer
	z.readBuffer = buf
	z.readOffset = 0
	return nil
}

func (z *ZBuffer) checkReadBuffer() error {
	if z.hasEmptyReadBuffer() {
		return z.fillReadBuffer()
	}
	return nil
}

func (z *ZBuffer) hasEmptyReadBuffer() bool {
	return len(z.readBuffer) == z.readOffset
}

func (z *ZBuffer) directWrite(buf []byte) ([]byte, error) {
	if z.isClosed {
		return nil, io.ErrClosedPipe
	}

	waste := <-z.wasteStream
	z.bufferStream <- buf
	return waste, nil
}

func (z *ZBuffer) directRead(waste []byte) ([]byte, error) {
	buf, ok := <-z.bufferStream
	if !ok {
		return nil, io.EOF
	}

	z.wasteStream <- waste
	return buf, nil
}
