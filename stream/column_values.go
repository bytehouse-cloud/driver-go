package stream

import (
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"

	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
)

type AsyncColumnValuesToBlock struct {
	sample        *data.Block
	cvStream      <-chan [][]interface{}
	rowsProcessed int
	parallelism   int
	errGroup      *errgroup.Group
	recycle       RecycleColumnValues
	err           error
	done          chan struct{}
}

func (a *AsyncColumnValuesToBlock) Start(ctx context.Context) <-chan *data.Block {
	outputStream := make(chan *data.Block, 1)
	a.errGroup, ctx = errgroup.WithContext(ctx)
	a.done = make(chan struct{})

	go func() {
		defer close(outputStream)
		defer close(a.done)

		for i := 0; i < a.parallelism; i++ {
			a.errGroup.Go(func() error {
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
							return fmt.Errorf("reading into block error. row_idx: %v, col_idx: %v, name: %v, type: %v, given: %v",
								rowsRead, colsRead, newBlock.Columns[colsRead].Name, newBlock.Columns[colsRead].Type, columns[rowsRead][colsRead])
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

func (a *AsyncColumnValuesToBlock) Finish() (rowsProcessed int, err error) {
	<-a.done
	return a.rowsProcessed, err
}

func (a *AsyncColumnValuesToBlock) setParallelism(n int) {
	a.parallelism = n
}

func (a *AsyncColumnValuesToBlock) setRecycle(recycle RecycleColumnValues) {
	a.recycle = recycle
}

func NewAsyncColumnValuesToBlock(cvStream <-chan [][]interface{}, sample *data.Block, opts ...AsyncOption) *AsyncColumnValuesToBlock {
	process := &AsyncColumnValuesToBlock{
		sample:      sample,
		cvStream:    cvStream,
		parallelism: defaultParallelismCount,
		recycle:     recycleNoOpt,
	}

	for _, opt := range opts {
		opt(process)
	}
	return process
}

func (a *AsyncColumnValuesToBlock) Error() error {
	return a.err
}
