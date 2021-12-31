package helper

import (
	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data/column"
)

type RowReader interface {
	ReadFirstRow(fb *bytepool.FrameBuffer, cols []*column.CHColumn) error
	ReadRowCont(fb *bytepool.FrameBuffer, cols []*column.CHColumn) error
}

func ReadFirstColumnTexts(fb *bytepool.FrameBuffer, numRows int, cols []*column.CHColumn, rReader RowReader) (int, error) {
	if numRows == 0 {
		return 0, nil
	}

	fb.NewRow()
	if err := rReader.ReadFirstRow(fb, cols); err != nil {
		fb.DiscardCurrentRow()
		return 0, err
	}

	totalRead := 1
	for i := 1; i < numRows; i++ {
		fb.NewRow()
		if err := rReader.ReadRowCont(fb, cols); err != nil {
			fb.DiscardCurrentRow()
			return totalRead, err
		}
		totalRead++
	}

	return totalRead, nil
}

func ReadColumnTextsCont(fb *bytepool.FrameBuffer, numRows int, cols []*column.CHColumn, rReader RowReader) (int, error) {
	var totalRead int
	for i := 0; i < numRows; i++ {
		fb.NewRow()
		if err := rReader.ReadRowCont(fb, cols); err != nil {
			fb.DiscardCurrentRow()
			return totalRead, err
		}
		totalRead++
	}
	return totalRead, nil
}
