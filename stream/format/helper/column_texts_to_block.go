package helper

import (
	"context"
	"fmt"
	"strings"

	"golang.org/x/sync/errgroup"

	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
)

type RecycleColumnTexts func(columnTexts [][]string)

var RecycleColumnTextsNoOp RecycleColumnTexts = func(columnTexts [][]string) {}

type ColumnTextsToBlock struct {
	sample        *data.Block
	ctStream      <-chan *ColumnTextsResult
	rowsProcessed int
	parallelism   int //TODO implement parallelism if it becomes bottleneck
	errGroup      *errgroup.Group
	err           error
	done          chan struct{}
}

func NewColumnTextsToBlock(ctStream <-chan *ColumnTextsResult, sample *data.Block) *ColumnTextsToBlock {
	process := &ColumnTextsToBlock{
		sample:   sample,
		ctStream: ctStream,
	}

	return process
}

func (a *ColumnTextsToBlock) Start(ctx context.Context) <-chan *data.Block {
	outputStream := make(chan *data.Block, 1)
	a.errGroup, ctx = errgroup.WithContext(ctx)
	a.done = make(chan struct{})

	go func() {
		defer close(outputStream)
		defer close(a.done)
		a.parallelism = 1
		for i := 0; i < a.parallelism; i++ {
			a.errGroup.Go(func() error {
				for {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case result, ok := <-a.ctStream:
						if !ok {
							return nil
						}

						columnTexts := result.Get()
						colTextsTrimSpace(columnTexts)
						numRows := len(columnTexts[0])
						newBlock := a.sample.StructureCopy(numRows)
						rowsRead, colsRead, err := newBlock.ReadFromColumnTexts(columnTexts)

						a.rowsProcessed += rowsRead
						if err != nil {
							return fmt.Errorf(
								"reading into block error. row_idx: %v, col_idx: %v, name: %v, type: %v, given: %v, err: %s",
								rowsRead, colsRead, newBlock.Columns[colsRead].Name, newBlock.Columns[colsRead].Type, columnTexts[colsRead][rowsRead], err,
							)
						}
						outputStream <- newBlock
						result.Close()
					}
				}
			})
		}

		a.err = a.errGroup.Wait()
	}()

	return outputStream
}

func (a *ColumnTextsToBlock) Finish() (rowsProcessed int, err error) {
	<-a.done
	return a.rowsProcessed, a.err
}

func (a *ColumnTextsToBlock) Error() error {
	return a.err
}

func colTextsTrimSpace(colTexts [][]string) {
	for _, row := range colTexts {
		for j, s := range row {
			row[j] = strings.TrimSpace(s)
		}
	}
}
