package format

import (
	"context"
	"io"
	"strings"

	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
	"github.com/bytehouse-cloud/driver-go/errors"
)

const (
	PRETTY = iota
	CSVWITHNAMES
	CSV
	VALUES
	JSON
	TOML
)

var Formats = map[int]string{
	CSVWITHNAMES: "CSVWITHNAMES",
	CSV:          "CSV",
	VALUES:       "VALUES",
	JSON:         "JSON",
	TOML:         "TOML",
	PRETTY:       "PRETTY",
}

type BlockStreamFmtReader interface {
	// BlockStreamFmtRead starts reading blocks, return stream of blocks to be consumed.
	// if ctx is cancelled, read will stop asap and channel will be closed
	BlockStreamFmtRead(ctx context.Context, sample *data.Block, blockSize int) <-chan *data.Block
	// Yield blocks until the last BlockStreamFmtRead is completed
	// return total rows notFirstRow and error if any
	Yield() (int, error)
}

func BlockStreamFmtReaderFactory(fmtType string, r io.Reader, settings map[string]interface{}) (BlockStreamFmtReader, error) {
	switch strings.ToUpper(fmtType) {
	case Formats[CSVWITHNAMES]:
		return NewCSVBlockStreamFmtReader(r, true, settings)
	case Formats[CSV]:
		return NewCSVBlockStreamFmtReader(r, false, settings)
	case Formats[VALUES]:
		return NewValuesBlockStreamReader(r), nil
	case Formats[JSON]:
		return NewJSONBlockStreamFmtReader(r), nil

	default:
		return nil, errors.ErrorfWithCaller("unrecognised input format: [%s]\n", fmtType)
	}
}

// BlockStreamFmtWriter writes data of block to it's respective format of it's concrete type
type BlockStreamFmtWriter interface {
	// BlockStreamFmtWrite starts writing stream of blocks into respective format, non blocking
	BlockStreamFmtWrite(blockStream <-chan *data.Block)
	// Yield blocks until all blocks are consumed from BlockStreamFmtWrite
	// returns total rows written and error if any
	Yield() (int, error)
}

func BlockStreamFmtWriterFactory(fmtType string, w io.Writer, settings map[string]interface{}) (BlockStreamFmtWriter, error) {
	switch strings.ToUpper(fmtType) {
	case Formats[PRETTY]:
		return NewPrettyBlockStreamFmtWriter(w), nil
	case Formats[CSVWITHNAMES]:
		return NewCSVBlockStreamFmtWriter(w, true, settings)
	case Formats[CSV]:
		return NewCSVBlockStreamFmtWriter(w, false, settings)
	case Formats[VALUES]:
		return NewValuesBlockStreamFmtWriter(w), nil
	case Formats[JSON]:
		return NewJSONBlockStreamFmtWriter(w), nil
	default:
		return nil, errors.ErrorfWithCaller("unrecognised input format: [%s]\n", fmtType)
	}
}
