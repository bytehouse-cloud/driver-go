package format

import (
	"context"
	"io"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data/column"
	"github.com/bytehouse-cloud/driver-go/stream/format/helper"
)

const (
	errExpectedByteButGot = "error: expected byte: %q, got: %q"
	errReadingByte        = "error while trying to read %q, %s"
	errReadElem           = "error reading %v for column %v at index %v: %s"
)

func NewValuesBlockStreamReader(r io.Reader) *ValuesBlockStreamFmtReader {
	return &ValuesBlockStreamFmtReader{
		zReader: bytepool.NewZReaderDefault(r),
	}
}

type ValuesBlockStreamFmtReader struct {
	zReader *bytepool.ZReader
}

func (v *ValuesBlockStreamFmtReader) BlockStreamFmtRead(ctx context.Context, sample *data.Block, blockSize int,
) (blockStream <-chan *data.Block, yield func() (int, error)) {
	return helper.TableToBlockStream(ctx, sample, blockSize, v)
}

func (v *ValuesBlockStreamFmtReader) ReadFirstColumnTexts(fb *bytepool.FrameBuffer, numRows int, cols []*column.CHColumn) (int, error) {
	return helper.ReadFirstColumnTexts(fb, numRows, cols, v)
}

func (v *ValuesBlockStreamFmtReader) ReadColumnTextsCont(fb *bytepool.FrameBuffer, numRows int, cols []*column.CHColumn) (int, error) {
	return helper.ReadColumnTextsCont(fb, numRows, cols, v)
}

func (v *ValuesBlockStreamFmtReader) readRow(fb *bytepool.FrameBuffer, cols []*column.CHColumn) error {
	if err := helper.AssertNextByteEqual(v.zReader, '('); err != nil {
		return err
	}
	if err := helper.ReadRow(fb, cols, v); err != nil {
		return err
	}
	return helper.AssertNextByteEqual(v.zReader, ')')
}

func (v *ValuesBlockStreamFmtReader) ReadFirstRow(fb *bytepool.FrameBuffer, cols []*column.CHColumn) error {
	return v.readRow(fb, cols)
}

func (v *ValuesBlockStreamFmtReader) ReadRowCont(fb *bytepool.FrameBuffer, cols []*column.CHColumn) error {
	if err := helper.AssertNextByteEqual(v.zReader, ','); err != nil {
		if err != io.EOF {
			v.zReader.UnreadCurrentBuffer(1)
		}
	}
	return v.readRow(fb, cols)
}

func (v *ValuesBlockStreamFmtReader) ReadElem(fb *bytepool.FrameBuffer, cols []*column.CHColumn, idx int) error {
	if idx > 0 {
		if err := helper.AssertNextByteEqual(v.zReader, ','); err != nil {
			return err
		}
	}
	return v.readElem(fb, cols[idx], (len(cols)-1 == idx))
}

func (v *ValuesBlockStreamFmtReader) readElem(fb *bytepool.FrameBuffer, col *column.CHColumn, last bool) error {
	if last {
		return helper.ReadCHElemTillStop(fb, v.zReader, col.Data, ')')
	}
	return helper.ReadCHElemTillStop(fb, v.zReader, col.Data, ',')
}
