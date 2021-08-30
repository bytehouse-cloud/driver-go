package bytepool

import (
	"io"

	"github.com/jfcg/sixb"
)

type ZWriter struct {
	w          io.Writer
	buffer     []byte
	offset     int
	forWrite   chan []byte
	forReceive chan []byte
	exception  error
	finish     chan struct{}
}

func NewZWriterDefault(w io.Writer) *ZWriter {
	return NewZWriter(w, defaultBufferSize, defaultChannelSize)
}

func NewZWriter(w io.Writer, bufferSize, channelSize int) *ZWriter {
	z := &ZWriter{
		w:          w,
		buffer:     GetBytesWithLen(bufferSize),
		forWrite:   make(chan []byte, channelSize+1),
		forReceive: make(chan []byte, channelSize+1),
		finish:     make(chan struct{}, 1),
	}

	for i := 0; i < channelSize; i++ {
		z.forReceive <- GetBytesWithLen(bufferSize)
	}

	go z.startWrite()

	return z
}

func (z *ZWriter) Write(p []byte) (int, error) {
	var totalWrite, currentWrite int
	for totalWrite < len(p) {
		currentWrite = copy(z.buffer[z.offset:], p[totalWrite:])
		totalWrite += currentWrite
		z.offset += currentWrite
		if z.hasFullBuffer() {
			z.sendBufferForWrite()
		}
		if z.exception != nil {
			return totalWrite, z.exception
		}
	}
	return totalWrite, nil
}

func (z *ZWriter) hasFullBuffer() bool {
	return z.offset == len(z.buffer)
}

func (z *ZWriter) startWrite() {
	defer z.endWrite()

	if zBuffer, ok := z.w.(*ZBuffer); ok {
		z.startWriteZBuffer(zBuffer)
		return
	}

	for buffer := range z.forWrite {
		if _, err := z.w.Write(buffer); err != nil {
			z.exception = err
			return
		}
		z.forReceive <- buffer
	}
}

func (z *ZWriter) Flush() error {
	if z.offset > 0 {
		z.sendBufferForWrite()
	}
	if flusher, ok := z.w.(flusher); ok {
		if err := flusher.Flush(); err != nil {
			return err
		}
	}
	return z.flush()
}

func (z *ZWriter) sendBufferForWrite() {
	z.forWrite <- z.buffer[:z.offset]
	z.buffer = <-z.forReceive
	z.offset = 0
	return
}

func (z *ZWriter) WriteByte(b byte) error {
	z.buffer[z.offset] = b
	z.offset++
	if z.hasFullBuffer() {
		z.sendBufferForWrite()
	}
	return z.exception
}

func (z *ZWriter) WriteString(s string) error {
	_, err := z.Write(sixb.StB(s))
	return err
}

func (z *ZWriter) Close() error {
	close(z.forWrite)
	if closer, ok := z.w.(io.Closer); ok {
		if err := closer.Close(); err != nil {
			return err
		}
	}
	return z.exception
}

func (z *ZWriter) flush() error {
	close(z.forWrite)
	z.waitEnd()
	z.forWrite = make(chan []byte, cap(z.forWrite))
	go z.startWrite()
	return z.exception
}

func (z *ZWriter) waitEnd() {
	<-z.finish
}

func (z *ZWriter) endWrite() {
	z.finish <- struct{}{}
}

func (z *ZWriter) startWriteZBuffer(zBuf *ZBuffer) {
	for buf := range z.forWrite {
		waste, err := zBuf.directWrite(buf)
		if err != nil {
			z.exception = err
			return
		}
		z.forReceive <- waste
	}
}

type flusher interface {
	Flush() error
}
