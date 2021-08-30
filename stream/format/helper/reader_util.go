package helper

import (
	"strings"
	"sync"

	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
	"github.com/bytehouse-cloud/driver-go/errors"
)

func asyncColumnTextsToBlock(sample *data.Block, loadStream <-chan *fmtLoadWithLen, errPtr *error, totalRowsPtr *int, recycle func([][]string), finish func()) <-chan *data.Block {
	loadStreamWithOrder := orderLoadStream(loadStream, sample)
	blockStream := make(chan *data.Block, 1)

	go func() {
		defer close(blockStream)
		defer finish()

		//threads := runtime.GOMAXPROCS(0)/8 + 1
		threads := 1
		var wg sync.WaitGroup
		wg.Add(threads)

		var queueNumber int

		for i := 0; i < threads; i++ {
			go func() {
				defer wg.Done()
				for orderedLoad := range loadStreamWithOrder {
					colTextsTrimSpace(orderedLoad.columnTexts)
					rowsRead, columnsRead, err := orderedLoad.block.ReadFromColumnTexts(orderedLoad.columnTexts)
					*totalRowsPtr += rowsRead
					if err != nil {
						*errPtr = errors.ErrorfWithCaller("error reading into columns for row_idx %v at col_idx %v: %s with column name: %v", *totalRowsPtr+1, columnsRead, err, orderedLoad.block.Columns[columnsRead].Name)
						return
					}

					for orderedLoad.queueNo != queueNumber {
					}
					blockStream <- orderedLoad.block
					queueNumber++

					recycle(orderedLoad.columnTexts)
				}
			}()
		}

		wg.Wait()
	}()

	return blockStream
}

func colTextsTrimSpace(colTexts [][]string) {
	for _, row := range colTexts {
		for j, s := range row {
			row[j] = strings.TrimSpace(s)
		}
	}
}

func shrinkColumnTexts(texts [][]string, size int) [][]string {
	for i := range texts {
		texts[i] = texts[i][:size]
	}
	return texts
}

func orderLoadStream(loadStream <-chan *fmtLoadWithLen, sample *data.Block) <-chan *fmtLoadWithQueueNumber {
	newLoadStreamWithOrder := make(chan *fmtLoadWithQueueNumber, 1)

	go func() {
		defer close(newLoadStreamWithOrder)

		var i int
		for load := range loadStream {
			newLoadStreamWithOrder <- &fmtLoadWithQueueNumber{
				queueNo:     i,
				columnTexts: shrinkColumnTexts(load.columnTexts, load.numRows),
				block:       sample.StructureCopy(load.numRows),
			}
			i++
		}
	}()

	return newLoadStreamWithOrder
}

type fmtLoadWithQueueNumber struct {
	columnTexts [][]string
	block       *data.Block
	queueNo     int
}

type fmtLoadWithLen struct {
	columnTexts [][]string
	numRows     int
}
