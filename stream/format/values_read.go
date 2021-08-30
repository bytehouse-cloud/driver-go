package format

import (
	"context"
	"io"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data/column"
	"github.com/bytehouse-cloud/driver-go/errors"
	"github.com/bytehouse-cloud/driver-go/stream/format/helper"
)

const (
	expectedByteButGot = "expected character: %q, got: %q"
	errorReadingByte   = "error while trying to read %q, %s"
	readElemErr        = "error reading %v for column %v at index %v: %s"
)

func NewValuesBlockStreamReader(r io.Reader) *ValuesBlockStreamFmtReader {
	return &ValuesBlockStreamFmtReader{
		zReader: bytepool.NewZReaderDefault(r),
	}
}

type ValuesBlockStreamFmtReader struct {
	zReader *bytepool.ZReader

	totalRowsRead int
	exception     error
	done          chan struct{}
}

func (v *ValuesBlockStreamFmtReader) BlockStreamFmtRead(ctx context.Context, sample *data.Block, blockSize int) <-chan *data.Block {
	v.done = make(chan struct{}, 1)
	return helper.ReadColumnTextsToBlockStream(ctx, sample, blockSize, v, &v.exception, &v.totalRowsRead, func() {
		v.done <- struct{}{}
	})
}

func (v *ValuesBlockStreamFmtReader) Yield() (int, error) {
	<-v.done
	return v.totalRowsRead, v.exception
}

func (v *ValuesBlockStreamFmtReader) ReadFirstColumnTexts(colTexts [][]string, cols []*column.CHColumn) (int, error) {
	return helper.ReadFirstColumnTexts(colTexts, cols, v)
}

func (v *ValuesBlockStreamFmtReader) ReadColumnTextsCont(colTexts [][]string, cols []*column.CHColumn) (int, error) {
	return helper.ReadColumnTextsCont(colTexts, cols, v)
}

func (v *ValuesBlockStreamFmtReader) readRow(colTexts [][]string, rowIdx int, cols []*column.CHColumn) error {
	if err := v.readOpenBracket(); err != nil {
		if err != io.EOF {
			err = errors.ErrorfWithCaller("error reading open bracket: %s", err)
		}
		return err
	}

	for colIdx := 0; colIdx < len(colTexts)-1; colIdx++ {
		s, err := v.readElem(cols[colIdx], false)
		if err != nil {
			return errors.ErrorfWithCaller(readElemErr, cols[colIdx].Type, cols[colIdx].Name, colIdx, err)
		}
		colTexts[colIdx][rowIdx] = s

		if err := v.readComma(); err != nil {
			return errors.ErrorfWithCaller(errorReadingByte, ',', err)
		}
	}

	lastCol := cols[len(cols)-1]
	s, err := v.readElem(lastCol, true)
	if err != nil {
		return errors.ErrorfWithCaller(readElemErr, lastCol.Type, lastCol.Name, len(cols)-1, err)
	}
	colTexts[len(cols)-1][rowIdx] = s

	if err := v.readCloseBracket(); err != nil {
		return errors.ErrorfWithCaller("error reading close bracket: %s", err)
	}

	return nil
}

func (v *ValuesBlockStreamFmtReader) ReadFirstRow(colTexts [][]string, cols []*column.CHColumn) error {
	return v.readRow(colTexts, 0, cols)
}

func (v *ValuesBlockStreamFmtReader) ReadRowCont(colTexts [][]string, rowIdx int, cols []*column.CHColumn) error {
	v.readOptionalComma()
	return v.readRow(colTexts, rowIdx, cols)
}

func (v *ValuesBlockStreamFmtReader) readOpenBracket() error {
	b, err := helper.ReadNextNonSpaceByte(v.zReader)
	if err != nil {
		return err
	}
	if b != '(' {
		return errors.ErrorfWithCaller(expectedByteButGot, '(', b)
	}
	return nil
}

func (v *ValuesBlockStreamFmtReader) readCloseBracket() error {
	b, err := helper.ReadNextNonSpaceByte(v.zReader)
	if err != nil {
		return err
	}
	if b != ')' {
		return errors.ErrorfWithCaller(expectedByteButGot, ')', b)
	}
	return nil
}

func (v *ValuesBlockStreamFmtReader) readComma() error {
	b, err := helper.ReadNextNonSpaceByte(v.zReader)
	if err != nil {
		return err
	}
	if b != ',' {
		return errors.ErrorfWithCaller(expectedByteButGot, ',', b)
	}
	return nil
}

func (v *ValuesBlockStreamFmtReader) readOptionalComma() {
	b, err := helper.ReadNextNonSpaceByte(v.zReader)
	if err != nil {
		return
	}
	if b != ',' {
		v.zReader.UnreadCurrentBuffer(1)
	}
}

func (v *ValuesBlockStreamFmtReader) readElem(col *column.CHColumn, last bool) (string, error) {
	if last {
		return helper.ReadCHElem(v.zReader, col, ')')
	}
	return helper.ReadCHElem(v.zReader, col, ',')
}
