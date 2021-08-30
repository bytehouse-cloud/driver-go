package stream

import "sync"

func NewColumnValuesPool(numCols, numRows int) *ColumnValuesPool {
	d := dimension{numColumns: numCols, numRows: numRows}
	cache, ok := columnValuesPoolMap[d]
	if ok {
		return &ColumnValuesPool{pool: cache}
	}

	newPool := sync.Pool{
		New: func() interface{} {
			cv := make([][]interface{}, numCols)
			for i := range cv {
				cv[i] = make([]interface{}, numRows)
			}
			return cv
		},
	}
	columnValuesPoolMap[d] = &newPool

	return &ColumnValuesPool{
		pool: &newPool,
	}
}

type dimension struct {
	numColumns, numRows int
}

var columnValuesPoolMap = map[dimension]*sync.Pool{}

type ColumnValuesPool struct {
	pool *sync.Pool
}

func (c *ColumnValuesPool) Get() [][]interface{} {
	return c.pool.Get().([][]interface{})
}

func (c *ColumnValuesPool) Put(colTexts [][]interface{}) {
	c.pool.Put(colTexts)
}
