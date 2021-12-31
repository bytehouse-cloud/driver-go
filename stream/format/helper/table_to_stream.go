package helper

import (
	"context"
	"fmt"

	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
	"golang.org/x/sync/errgroup"
)

func TableToBlockStream(ctx context.Context, sample *data.Block, blockSize int, tReader TableReader,
) (blockStream <-chan *data.Block, yield func() (int, error)) {

	eg, ctx := errgroup.WithContext(ctx)

	colTextsStreamer := NewColumnTextsStreamer(sample, blockSize, tReader)
	colTextsStream := colTextsStreamer.Start(ctx)

	toBlockProcess := NewColumnTextsToBlock(colTextsStream, sample)
	blockStream = toBlockProcess.Start(ctx)
	return blockStream, YieldTableStream(eg, colTextsStreamer, toBlockProcess)
}

func YieldTableStream(eg *errgroup.Group, colTextsStreamer *ColumnTextsStreamer, toBlockProcess *ColumnTextsToBlock,
) func() (int, error) {
	return func() (int, error) {

		var numRowTexts, numRowBlocks int

		eg.Go(func() (err error) {
			numRowBlocks, err = toBlockProcess.Finish()
			return
		})

		eg.Go(func() (err error) {
			numRowTexts, err = colTextsStreamer.Finish()
			return
		})

		if err := eg.Wait(); err != nil {
			return numRowBlocks, err
		}

		if numRowTexts != numRowBlocks {
			return numRowBlocks, fmt.Errorf(
				"short rows to blocks, rows read: %v, rows processed: %v",
				numRowTexts, numRowBlocks,
			)
		}

		return numRowBlocks, nil
	}
}
