package stream

import (
	"context"

	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
)

const defaultParallelismCount = 2

type RecycleColumnValues func(columnValues [][]interface{})

var recycleNoOpt RecycleColumnValues = func(columnValues [][]interface{}) {}

type AsyncToBlockProcess interface {
	Start(ctx context.Context) <-chan *data.Block
	Finish() (rowsProcess int, err error)
	Error() error
}
