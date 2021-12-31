package format

import (
	"io"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data/column"
	"github.com/bytehouse-cloud/driver-go/stream/format/helper"
)

func NewValuesBlockStreamFmtWriter(w io.Writer) *ValuesBlockStreamFmtWriter {
	return &ValuesBlockStreamFmtWriter{
		zWriter: bytepool.NewZWriterDefault(w),
	}
}

type ValuesBlockStreamFmtWriter struct {
	zWriter *bytepool.ZWriter

	done           chan struct{}
	totalRowsWrite int
	exception      error
}

func (v *ValuesBlockStreamFmtWriter) BlockStreamFmtWrite(blockStream <-chan *data.Block) {
	v.done = make(chan struct{}, 1)
	go v.blockStreamFmtWrite(blockStream)
}

func (v *ValuesBlockStreamFmtWriter) blockStreamFmtWrite(blockStream <-chan *data.Block) {
	defer func() {
		v.done <- struct{}{}
	}()
	v.totalRowsWrite, v.exception = helper.WriteTableFromBlockStream(blockStream, v)
}

func (v *ValuesBlockStreamFmtWriter) Yield() (int, error) {
	<-v.done
	return v.totalRowsWrite, v.exception
}

func (v *ValuesBlockStreamFmtWriter) WriteFirstFrame(frame [][]string, cols []*column.CHColumn) (int, error) {
	return helper.WriteFirstFrame(frame, cols, v)
}

func (v *ValuesBlockStreamFmtWriter) WriteFrameCont(frame [][]string, cols []*column.CHColumn) (int, error) {
	return helper.WriteFrameCont(frame, cols, v)
}

func (v *ValuesBlockStreamFmtWriter) Flush() error {
	return v.zWriter.Flush()
}

func (v *ValuesBlockStreamFmtWriter) WriteFirstRow(record []string, cols []*column.CHColumn) error {
	return v.writeRow(record, cols)
}

func (v *ValuesBlockStreamFmtWriter) WriteRowCont(record []string, cols []*column.CHColumn) error {
	if err := v.zWriter.WriteString(",\n"); err != nil {
		return err
	}
	return v.writeRow(record, cols)
}

func (v *ValuesBlockStreamFmtWriter) writeRow(record []string, cols []*column.CHColumn) error {
	if err := v.zWriter.WriteByte('('); err != nil {
		return err
	}

	if err := helper.WriteCHElemString(v.zWriter, record[0], cols[0]); err != nil {
		return err
	}

	for i := 1; i < len(record); i++ {
		if err := v.zWriter.WriteString(", "); err != nil {
			return err
		}
		if err := helper.WriteCHElemString(v.zWriter, record[i], cols[i]); err != nil {
			return err
		}
	}

	if err := v.zWriter.WriteByte(')'); err != nil {
		return err
	}
	return nil
}
