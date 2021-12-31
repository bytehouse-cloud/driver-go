package bytepool

import (
	"bytes"
)

var stringsBufferPool = make(chan *StringsBuffer, 10)

func getStringsBufferFromPool() *StringsBuffer {
	select {
	case buf := <-stringsBufferPool:
		buf.buffer.Reset()
		buf.offsets = buf.offsets[:0]
		return buf
	default:
	}

	return &StringsBuffer{
		buffer:  &bytes.Buffer{},
		offsets: make([]int, 0),
	}
}

func putStringsBufferToPool(buf *StringsBuffer) {
	select {
	case stringsBufferPool <- buf:
	default:
	}
}
