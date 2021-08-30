package helper

import (
	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data/column"
)

type TableWriter interface {
	// WriteFirstFrame writes the first frame generated from the first block with data from server.
	// meant for writing Headers if any (eg. JSON)
	// return number of rows written and error if any.
	WriteFirstFrame(frame [][]string, cols []*column.CHColumn) (int, error)
	// WriteFrameCont is same as WriteFirstFrame but not for first row
	WriteFrameCont(frame [][]string, cols []*column.CHColumn) (int, error)
	// Flush indicates all blocks are read and caller should write any last data and do clean up
	Flush() error
}

type tableEndWriter interface {
	WriteEnd() error
}

func WriteBlockSteamToFrame(blockStream <-chan *data.Block, tWriter TableWriter) (int, error) {
	frameStream, cols, recycleFrame, ok := asyncBlockToFrame(blockStream)
	if !ok {
		return 0, nil
	}

	defer func() {
		for range frameStream {
		}
	}()

	var (
		totalRowsWrite int
		currentRead    int
		err            error
	)

	// Read first block
	for frame := range frameStream {
		currentRead, err = tWriter.WriteFirstFrame(frame, cols)
		totalRowsWrite += currentRead
		if err != nil {
			return totalRowsWrite, err
		}
		recycleFrame(frame)
		break
	}

	// Read subsequent block
	for frame := range frameStream {
		currentRead, err = tWriter.WriteFrameCont(frame, cols)
		totalRowsWrite += currentRead
		if err != nil {
			return totalRowsWrite, err
		}
		recycleFrame(frame)
	}

	if endWriter, ok := tWriter.(tableEndWriter); ok {
		if err := endWriter.WriteEnd(); err != nil {
			return totalRowsWrite, err
		}
	}

	return totalRowsWrite, tWriter.Flush()
}
