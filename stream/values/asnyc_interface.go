package values

import (
	"context"

	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
)

const defaultParallelismCount = 2

type BlockProcess interface {
	Start(ctx context.Context) <-chan *data.Block
	Finish() (rowsProcess int, err error)
	Error() error
}
