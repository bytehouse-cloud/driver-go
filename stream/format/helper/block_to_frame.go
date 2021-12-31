package helper

import (
	"runtime"
	"sync"

	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data/column"
)

type framePool struct {
	pool    *sync.Pool
	numCols int
}

func newFramePool(numCols int) *framePool {
	return &framePool{
		pool: &sync.Pool{
			New: func() interface{} {
				return make([][]string, 0)
			},
		},
		numCols: numCols,
	}
}

func (f *framePool) getFrame(numRows int) [][]string {
	currentFrame := f.pool.Get().([][]string)
	if cap(currentFrame) < numRows {
		return f.makeNewFrame(numRows)
	}
	return currentFrame[:numRows]
}

func (f *framePool) makeNewFrame(numRows int) [][]string {
	newFrame := make([][]string, numRows)
	for i := range newFrame {
		newFrame[i] = make([]string, f.numCols)
	}
	return newFrame
}

func (f *framePool) putFrame(used [][]string) {
	_ = used[0][f.numCols-1] // at least 1 row with at least numCols columns
	f.pool.Put(used[:cap(used)])
}

// BlockToFrame converts all blocks to respective string frame in order.
// assumes that blocks coming at least have 1 row.
// returns false if channel is closed initially.
func BlockToFrame(blockStream <-chan *data.Block) (<-chan [][]string, []*column.CHColumn, func([][]string), bool) {
	frameStream := make(chan [][]string, 1)

	// process first block because there is no way to determine the numCols without getting first block
	frame, cols, ok := processFmtFirstBlock(blockStream)
	if !ok {
		return nil, nil, nil, false
	}
	frameStream <- frame

	fPool := newFramePool(len(cols))
	blockWithQueueNumberStream := orderBlockStream(blockStream)

	go func() {
		defer close(frameStream)

		threads := runtime.GOMAXPROCS(0)/8 + 1 // todo: investigate max performance from number of CPUS
		var wg sync.WaitGroup
		wg.Add(threads)
		var currentQ int
		for i := 0; i < threads; i++ {

			go func() {
				defer wg.Done()
				for b := range blockWithQueueNumberStream {
					frame := fPool.getFrame(b.block.NumRows)
					b.block.WriteToStrings(frame)
					for b.queueNo != currentQ {
					}
					frameStream <- frame
					currentQ++
					b.block.Close()
				}
			}()
		}
		wg.Wait()
	}()

	return frameStream, cols, fPool.putFrame, true
}

func processFmtFirstBlock(blockStream <-chan *data.Block) ([][]string, []*column.CHColumn, bool) {
	firstBlock, ok := <-blockStream
	if !ok {
		return nil, nil, false
	}
	defer firstBlock.Close()

	if firstBlock.NumRows == 0 {
		return processFmtFirstBlock(blockStream)
	}
	firstFrame := firstBlock.NewStringFrame()
	firstBlock.WriteToStrings(firstFrame)
	return firstFrame, firstBlock.Columns, true
}

type blockWithQueueNumber struct {
	queueNo int
	block   *data.Block
}

func orderBlockStream(blockStream <-chan *data.Block) <-chan *blockWithQueueNumber {
	blockWithQueueNumberStream := make(chan *blockWithQueueNumber)

	go func() {
		defer close(blockWithQueueNumberStream)
		var i int
		for b := range blockStream {
			blockWithQueueNumberStream <- &blockWithQueueNumber{
				queueNo: i,
				block:   b,
			}
			i++
		}
	}()

	return blockWithQueueNumberStream
}
