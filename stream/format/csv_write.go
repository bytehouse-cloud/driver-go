package format

import (
	"io"
	"log"
	"runtime/debug"
	"strings"

	"github.com/jfcg/sixb"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data/column"
	"github.com/bytehouse-cloud/driver-go/stream/format/helper"
)

type CSVBlockStreamFmtWriter struct {
	zWriter        *bytepool.ZWriter
	delimiterBytes byte

	totalRowsWrite int
	exception      error
	done           chan struct{}
	withNames      bool
}

func NewCSVBlockStreamFmtWriter(w io.Writer, withNames bool, settings map[string]interface{}) (*CSVBlockStreamFmtWriter, error) {
	delim, err := resolveCSVDelim(settings)
	if err != nil {
		return nil, err
	}
	newWriter := &CSVBlockStreamFmtWriter{
		zWriter:        bytepool.NewZWriterDefault(w),
		delimiterBytes: delim,
		withNames:      withNames,
	}
	return newWriter, nil
}

func (c *CSVBlockStreamFmtWriter) BlockStreamFmtWrite(blockStream <-chan *data.Block) {
	c.done = make(chan struct{}, 1)
	go c.blockStreamFmtWrite(blockStream)
}

func (c *CSVBlockStreamFmtWriter) blockStreamFmtWrite(blockStream <-chan *data.Block) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("A runtime panic has occurred with err = [%s],  stacktrace = [%s]\n",
				r,
				string(debug.Stack()))
		}
	}()
	defer func() {
		c.done <- struct{}{}
	}()
	c.totalRowsWrite, c.exception = helper.WriteTableFromBlockStream(blockStream, c)
}

func (c *CSVBlockStreamFmtWriter) Yield() (int, error) {
	<-c.done
	return c.totalRowsWrite, c.exception
}

func (c *CSVBlockStreamFmtWriter) WriteFrameCont(frame [][]string, cols []*column.CHColumn) (int, error) {
	return helper.WriteFrameCont(frame, cols, c)
}

func (c *CSVBlockStreamFmtWriter) WriteFirstFrame(frame [][]string, cols []*column.CHColumn) (int, error) {
	if c.withNames {
		if err := c.zWriter.WriteString(cols[0].Name); err != nil {
			return 0, err
		}
		for i := 1; i < len(cols); i++ {
			if err := c.zWriter.WriteByte(c.delimiterBytes); err != nil {
				return 0, err
			}
			if err := c.zWriter.WriteString(cols[i].Name); err != nil {
				return 0, err
			}
		}
		if err := c.zWriter.WriteByte('\n'); err != nil {
			return 0, nil
		}
	}

	return helper.WriteFirstFrame(frame, cols, c)
}

func (c *CSVBlockStreamFmtWriter) Flush() error {
	return c.zWriter.Flush()
}

func (c *CSVBlockStreamFmtWriter) WriteFirstRow(record []string, cols []*column.CHColumn) error {
	return c.writeRow(record, cols)
}

func (c *CSVBlockStreamFmtWriter) WriteRowCont(record []string, cols []*column.CHColumn) error {
	if err := c.zWriter.WriteByte('\n'); err != nil {
		return err
	}
	return c.writeRow(record, cols)
}

func (c *CSVBlockStreamFmtWriter) writeRow(record []string, cols []*column.CHColumn) error {
	if err := c.writeColumn(record[0], cols[0]); err != nil {
		return err
	}
	for i := 1; i < len(cols); i++ {
		if err := c.zWriter.WriteByte(c.delimiterBytes); err != nil {
			return err
		}
		if err := c.writeColumn(record[i], cols[i]); err != nil {
			return err
		}
	}
	c.totalRowsWrite++
	return nil
}

func (c *CSVBlockStreamFmtWriter) writeColumn(s string, col *column.CHColumn) error {
	switch col.Data.(type) {
	case *column.StringColumnData, *column.FixedStringColumnData:
		if err := c.zWriter.WriteByte('"'); err != nil {
			return err
		}
		if err := c.writeColumnWithCheck(s); err != nil {
			return err
		}
		if err := c.zWriter.WriteByte('"'); err != nil {
			return err
		}
		return nil
	case *column.DateColumnData, *column.DateTimeColumnData, *column.DateTime64ColumnData:
		if err := c.zWriter.WriteByte('"'); err != nil {
			return err
		}
		if err := c.writeString(s); err != nil {
			return err
		}
		if err := c.zWriter.WriteByte('"'); err != nil {
			return err
		}
		return nil
	default:
		return c.writeString(s)
	}
}

func (c *CSVBlockStreamFmtWriter) writeColumnWithCheck(s string) error {
	for i := strings.IndexByte(s, '"'); i >= 0; i = strings.IndexByte(s, '"') {
		if err := c.writeString(s[:i]); err != nil {
			return err
		}
		if _, err := c.zWriter.Write([]byte("\"\"")); err != nil {
			return err
		}
		s = s[i+1:]
	}

	if err := c.writeString(s); err != nil {
		return err
	}

	return nil
}

func (c *CSVBlockStreamFmtWriter) writeString(s string) error {
	_, err := c.zWriter.Write(sixb.StoB(s))
	return err
}
