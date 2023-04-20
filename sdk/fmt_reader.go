package sdk

import (
	"io"
	"log"
	"runtime/debug"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
	"github.com/bytehouse-cloud/driver-go/stream/format"
)

type resultFmtReader struct {
	reader io.Reader
	err    error
}

func newResultFmtReader(fmtType string, blockStream <-chan *data.Block) *resultFmtReader {
	var fmtReader resultFmtReader

	zBuf := bytepool.NewZBufferDefault()
	fmtWriter, err := format.BlockStreamFmtWriterFactory(fmtType, zBuf, nil)
	if err != nil {
		fmtReader.err = err
		return &fmtReader
	}
	fmtReader.reader = zBuf

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("A runtime panic has occurred with err = [%s],  stacktrace = [%s]\n",
					r,
					string(debug.Stack()))
			}
		}()
		defer func() {
			if err := zBuf.Close(); err != nil {
				if fmtReader.err != nil {
					fmtReader.err = err
				}
			}
		}()

		fmtWriter.BlockStreamFmtWrite(blockStream)
		if _, err := fmtWriter.Yield(); err != nil {
			fmtReader.err = err
		}
	}()

	return &fmtReader
}

func (r *resultFmtReader) Read(buf []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	return r.reader.Read(buf)
}
