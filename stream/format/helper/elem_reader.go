package helper

import (
	"fmt"
	"io"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data/column"
)

type ElemReader interface {
	ReadElem(fb *bytepool.FrameBuffer, cols []*column.CHColumn, idx int) error
}

func ReadRow(fb *bytepool.FrameBuffer, cols []*column.CHColumn, e ElemReader) error {
	for i := range cols {
		fb.NewElem()
		if err := e.ReadElem(fb, cols, i); err != nil {
			if err == io.EOF && i == 0 { // already ended, cannot read next row
				return io.EOF
			}
			return fmt.Errorf("error reading elem at idx %v: %s", i, err)
		}
	}

	return nil
}
