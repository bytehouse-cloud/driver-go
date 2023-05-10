package mocks

import (
	"io"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
)

type fakedReader struct{}

func (reader *fakedReader) Read(p []byte) (n int, err error) {
	return 0, nil
}

func NewFakedZReader() *bytepool.ZReader {
	var reader io.Reader = &fakedReader{}
	return bytepool.NewZReaderDefault(&reader)
}
