package bytepool

import (
	"bytes"

	"github.com/jfcg/sixb"
)

type StringsBuffer struct {
	result  []string
	buffer  *bytes.Buffer
	offsets []int
}

func NewStringsBuffer() *StringsBuffer {
	return getStringsBufferFromPool()
}

func (s *StringsBuffer) Write(p []byte) (int, error) {
	return s.buffer.Write(p)
}

func (s *StringsBuffer) WriteByte(p byte) error {
	return s.buffer.WriteByte(p)
}

func (s *StringsBuffer) WriteString(p string) (int, error) {
	return s.buffer.WriteString(p)
}

// NewEmptyResult returns an empty slice of string, which guarantees
// that when used as argument to call Export, will
// always export all data in the buffer
func (s *StringsBuffer) NewEmptyResult() []string {
	return make([]string, len(s.offsets))
}

// NewElem starts a new section to write another string
func (s *StringsBuffer) NewElem() {
	s.offsets = append(s.offsets, s.buffer.Len())
}

// ExportTo attempts to put the
func (s *StringsBuffer) ExportTo(result []string) int {
	if len(result) == 0 || len(s.offsets) == 0 {
		return 0
	}

	raw := s.buffer.Bytes()

	offsets := append(s.offsets[1:], s.buffer.Len())

	var (
		n          int
		lastOffset int
	)
	for n < len(result) && n < len(offsets) {
		currentOffset := offsets[n]
		result[n] = sixb.BtoS(raw[lastOffset:currentOffset])
		lastOffset = currentOffset
		n++
	}

	return n
}

// Export return the current state of the StringsBuffer,
// safe to use until the next write or close operation
func (s *StringsBuffer) Export() []string {
	neededLen := len(s.offsets)
	if cap(s.result) < len(s.offsets) {
		s.result = make([]string, neededLen)
	} else {
		s.result = s.result[:neededLen]
	}

	s.ExportTo(s.result)
	return s.result
}

// TruncateElem discard all but first n strings
// panics if there
func (s *StringsBuffer) TruncateElem(n int) {
	if n == len(s.offsets) {
		return
	}

	if n == 0 {
		s.buffer.Truncate(0)
		s.offsets = s.offsets[:0]
		return
	}

	s.buffer.Truncate(s.offsets[n])
	s.offsets = s.offsets[:n]
}

func (s *StringsBuffer) Len() int {
	return len(s.offsets)
}

func (s *StringsBuffer) Close() {
	putStringsBufferToPool(s)
}
