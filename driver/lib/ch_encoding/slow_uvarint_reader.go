package ch_encoding

import (
	"encoding/binary"
	"io"
)

type slowUvarintReader struct {
	reader io.Reader
	buffer []byte
}

func (s *slowUvarintReader) ReadByte() (byte, error) {
	if _, err := s.reader.Read(s.buffer); err != nil {
		return 0, err
	}

	return s.buffer[0], nil
}

func (s *slowUvarintReader) ReadUvarint() (uint64, error) {
	return binary.ReadUvarint(s)
}

func newSlowUvarintReader(reader io.Reader) UvarintReader {
	return &slowUvarintReader{reader: reader, buffer: make([]byte, 1)}
}
