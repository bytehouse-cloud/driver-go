package helper

import "sync"

func newColTextsPool(numCols, numRows int) *colTextsPool {
	columnTextPool := sync.Pool{
		New: func() interface{} {
			newColumnTexts := make([][]string, numCols)
			for i := range newColumnTexts {
				newColumnTexts[i] = make([]string, numRows)
			}
			return newColumnTexts
		}}

	return &colTextsPool{
		pool: &columnTextPool,
	}
}

type colTextsPool struct {
	pool *sync.Pool
}

func (c *colTextsPool) get() [][]string {
	return c.pool.Get().([][]string)
}

func (c *colTextsPool) put(colTexts [][]string) {
	c.pool.Put(colTexts)
}
