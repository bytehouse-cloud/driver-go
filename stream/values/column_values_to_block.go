package values

import (
	"context"
	"fmt"
	"log"
	"runtime/debug"

	"golang.org/x/sync/errgroup"

	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
)

type RecycleColumnValues func(columnValues [][]interface{})

var recycleColumnValuesNoOp RecycleColumnValues = func(columnValues [][]interface{}) {}

type ColumnValuesToBlock struct {
	sample        *data.Block
	cvStream      <-chan [][]interface{}
	rowsProcessed int
	parallelism   int
	errGroup      *errgroup.Group
	recycle       RecycleColumnValues
	err           error
	done          chan struct{}
}

func NewColumnValuesToBlock(cvStream <-chan [][]interface{}, sample *data.Block) *ColumnValuesToBlock {
	process := &ColumnValuesToBlock{
		sample:      sample,
		cvStream:    cvStream,
		parallelism: defaultParallelismCount,
		recycle:     recycleColumnValuesNoOp,
	}

	return process
}

func (a *ColumnValuesToBlock) Start(ctx context.Context) <-chan *data.Block {
	outputStream := make(chan *data.Block, 1)
	a.errGroup, ctx = errgroup.WithContext(ctx)
	a.done = make(chan struct{})

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("A runtime panic has occurred with err = [%s],  stacktrace = [%s]\n",
					r,
					string(debug.Stack()))
			}
		}()
		defer close(outputStream)
		defer close(a.done)

		for i := 0; i < a.parallelism; i++ {
			a.errGroup.Go(func() error {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("A runtime panic has occurred with err = [%s],  stacktrace = [%s]\n",
							r,
							string(debug.Stack()))
					}
				}()
				for {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case columns, ok := <-a.cvStream:
						if !ok {
							return nil
						}

						numRows := len(columns[0])
						newBlock := a.sample.StructureCopy(numRows)
						rowsRead, colsRead, err := newBlock.ReadFromColumnValues(columns)
						a.rowsProcessed += rowsRead
						outputStream <- newBlock
						if err != nil {
							return fmt.Errorf(
								"reading into block error. row_idx: %v, col_idx: %v, name: %v, type: %v, given: %v, err: %s",
								rowsRead, colsRead, newBlock.Columns[colsRead].Name, newBlock.Columns[colsRead].Type, columns[colsRead][rowsRead], err,
							)
						}
						a.recycle(columns)
					}
				}
			})
		}

		a.err = a.errGroup.Wait()
	}()

	return outputStream
}

func (a *ColumnValuesToBlock) Finish() (rowsProcessed int, err error) {
	<-a.done
	return a.rowsProcessed, a.err
}

func (a *ColumnValuesToBlock) setParallelism(n int) {
	a.parallelism = n
}

func (a *ColumnValuesToBlock) setRecycleColumnValues(recycle RecycleColumnValues) {
	a.recycle = recycle
}

func (a *ColumnValuesToBlock) Error() error {
	return a.err
}
