package helper

import (
	"context"
	"io"

	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data/column"
	"github.com/bytehouse-cloud/driver-go/errors"
)

type TableReader interface {
	ReadFirstColumnTexts(colTexts [][]string, cols []*column.CHColumn) (int, error)
	ReadColumnTextsCont(colTexts [][]string, cols []*column.CHColumn) (int, error)
}

func ReadColumnTextsToBlockStream(ctx context.Context, sample *data.Block, blockSize int, tReader TableReader, errPtr *error, totalPtr *int, finish func()) <-chan *data.Block {
	fmtLoadStream := make(chan *fmtLoadWithLen, 1)
	pool := newColTextsPool(sample.NumColumns, blockSize)

	go func() {
		defer close(fmtLoadStream)

		var totalRowsRead int

		// First Block
		n, err := columnTextsRead(pool.get, fmtLoadStream, func(colTexts [][]string) (int, error) {
			return tReader.ReadFirstColumnTexts(colTexts, sample.Columns)
		})
		totalRowsRead += n
		if err != nil {
			if err != io.EOF {
				*errPtr = errors.ErrorfWithCaller("error reading texts for row %v: %s", totalRowsRead, err)
			}
			return
		}

		// Subsequent Block
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			n, err = columnTextsRead(pool.get, fmtLoadStream, func(colTexts [][]string) (int, error) {
				return tReader.ReadColumnTextsCont(colTexts, sample.Columns)
			})
			totalRowsRead += n
			if err != nil {
				if err != io.EOF {
					*errPtr = errors.ErrorfWithCaller("error reading texts for row %v: %s", totalRowsRead, err)
				}
				return
			}
		}
	}()

	return asyncColumnTextsToBlock(sample, fmtLoadStream, errPtr, totalPtr, pool.put, finish)
}

type readColumnTexts func([][]string) (int, error)

// columnTextsRead attempt to read column texts.
// if successful, pushes the columnTexts into load stream for processing into blocks.
// if io.EOF, also pushes read result into load stream, and will return io.EOF.
// Any other error will not result in pushing the result into the load stream
func columnTextsRead(getColumnTexts func() [][]string, fmtLoadStream chan<- *fmtLoadWithLen, read readColumnTexts) (int, error) {
	newColumnTexts := getColumnTexts()
	n, err := read(newColumnTexts)
	switch err {
	case nil:
		fmtLoadStream <- &fmtLoadWithLen{
			columnTexts: newColumnTexts,
			numRows:     n,
		}
		return n, nil
	case io.EOF:
		if n > 0 {
			fmtLoadStream <- &fmtLoadWithLen{
				columnTexts: newColumnTexts,
				numRows:     n,
			}
			return n, nil
		}
		return 0, io.EOF
	default:
		return n, err
	}
}
