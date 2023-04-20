package helper

import (
	"context"
	"fmt"
	"io"
	"log"
	"runtime/debug"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data/column"
)

type TableReader interface {
	ReadFirstColumnTexts(fb *bytepool.FrameBuffer, numRows int, cols []*column.CHColumn) (int, error)
	ReadColumnTextsCont(fb *bytepool.FrameBuffer, numRows int, cols []*column.CHColumn) (int, error)
}

type ReadColumnTexts func(fb *bytepool.FrameBuffer, rows int, cols []*column.CHColumn) (int, error)

type ColumnTextsStreamer struct {
	cols      []*column.CHColumn
	blockSize int
	tReader   TableReader
	ctPool    *ColumnTextsPool

	rowRead  int
	err      error
	canceled bool
	done     chan struct{}
}

func NewColumnTextsStreamer(sample *data.Block, blockSize int, tReader TableReader) *ColumnTextsStreamer {
	return &ColumnTextsStreamer{
		cols:      sample.Columns,
		blockSize: blockSize,
		tReader:   tReader,
		ctPool:    NewColumnTextsPool(sample.NumColumns, blockSize),
	}
}

func (c *ColumnTextsStreamer) Start(ctx context.Context) <-chan *ColumnTextsResult {
	go c.watchCtx(ctx)
	c.done = make(chan struct{}, 1)

	outputStream := make(chan *ColumnTextsResult, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("A runtime panic has occurred with err = [%s],  stacktrace = [%s]\n",
					r,
					string(debug.Stack()))
			}
		}()
		defer close(c.done)
		defer close(outputStream)

		var err error
		defer func() {
			if err != nil {
				if err == io.EOF {
					return
				}
				c.err = fmt.Errorf("error reading row at index %v: %s", c.rowRead, err)
			}
		}()

		c.rowRead, err = c.readFirst(ctx, outputStream)
		if err != nil {
			return
		}

		var n int
		for {
			if c.canceled {
				return
			}

			n, err = c.readCont(ctx, outputStream)
			c.rowRead += n
			if err != nil {
				return
			}
		}
	}()

	return outputStream
}

func (c *ColumnTextsStreamer) watchCtx(ctx context.Context) {
	select {
	case <-ctx.Done():
		c.canceled = true
		c.err = ctx.Err()
	case <-c.done:
		return
	}
}

func (c *ColumnTextsStreamer) Finish() (int, error) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("A runtime panic has occurred with err = [%s],  stacktrace = [%s]\n",
				r,
				string(debug.Stack()))
		}
	}()
	<-c.done
	return c.rowRead, c.err
}

func (c *ColumnTextsStreamer) readFirst(ctx context.Context, des chan<- *ColumnTextsResult) (int, error) {
	return c.read(ctx, des, c.tReader.ReadFirstColumnTexts)
}

func (c *ColumnTextsStreamer) readCont(ctx context.Context, des chan<- *ColumnTextsResult) (int, error) {
	return c.read(ctx, des, c.tReader.ReadColumnTextsCont)
}

func (c *ColumnTextsStreamer) read(ctx context.Context, des chan<- *ColumnTextsResult, readColumnTexts ReadColumnTexts) (int, error) {
	if c.canceled {
		return 0, c.err
	}

	fb := bytepool.NewFrameBuffer()
	n, err := readColumnTexts(fb, c.blockSize, c.cols)

	if err != nil {
		if err != io.EOF {
			return n, err
		}
		if n == 0 {
			return n, io.EOF
		}
	}

	select {
	case des <- c.ctPool.NewColumnTextsResult(fb):
		return n, nil
	case <-ctx.Done():
		return n, context.Canceled
	}
}
