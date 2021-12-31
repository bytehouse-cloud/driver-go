package helper

import (
	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
)

func NewColumnTextsPool(numCols, numRows int) *ColumnTextsPool {
	newPool := make(chan [][]string, 10)

	return &ColumnTextsPool{
		pool:    newPool,
		numCols: numCols,
		numRows: numRows,
	}
}

type ColumnTextsPool struct {
	pool    chan [][]string
	numCols int
	numRows int
}

type ColumnTextsResult struct {
	fb       *bytepool.FrameBuffer
	colTexts [][]string
	pool     *ColumnTextsPool
}

func (c *ColumnTextsPool) NewColumnTextsResult(fb *bytepool.FrameBuffer) *ColumnTextsResult {
	// get from existing pool or make new
	var ct [][]string
	select {
	case ct = <-c.pool:
		// make sure to max out capacity after getting from pool
		for i, col := range ct {
			ct[i] = col[:cap(col)]
		}
	default:
		ct = make([][]string, c.numCols, c.numCols)
		for i := range ct {
			ct[i] = make([]string, c.numRows, c.numRows)
		}
	}

	return &ColumnTextsResult{
		fb:       fb,
		colTexts: ct,
		pool:     c,
	}
}

func (c *ColumnTextsResult) Get() [][]string {
	n, _ := c.fb.ReadColumnTexts(c.colTexts)
	for i, col := range c.colTexts {
		c.colTexts[i] = col[:n]
	}
	return c.colTexts
}

func (c *ColumnTextsResult) Close() {
	// optionally put it to pool, if there's space
	select {
	case c.pool.pool <- c.colTexts:
	default:
	}

	c.fb.Close()
}
