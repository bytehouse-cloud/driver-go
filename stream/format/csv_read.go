package format

import (
	"bytes"
	"context"
	"io"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data/column"
	"github.com/bytehouse-cloud/driver-go/errors"
	"github.com/bytehouse-cloud/driver-go/stream/format/helper"
)

const (
	csvDelimiterSetting = "format_csv_delimiter"
	errReadDelimiterFmt = "error reading delimiter: %s"
)

type CSVBlockStreamFmtReader struct {
	zReader   *bytepool.ZReader
	delimiter byte
	withNames bool
}

func NewCSVBlockStreamFmtReader(
	input io.Reader, withNames bool, settings map[string]interface{},
) (*CSVBlockStreamFmtReader, error) {
	delim, err := resolveCSVDelim(settings)
	if err != nil {
		return nil, err
	}

	return &CSVBlockStreamFmtReader{
		zReader:   bytepool.NewZReaderDefault(&input),
		delimiter: delim,
		withNames: withNames,
	}, nil
}

func (c *CSVBlockStreamFmtReader) BlockStreamFmtRead(
	ctx context.Context, sample *data.Block, blockSize int,
) (blockStream <-chan *data.Block, yield func() (int, error)) {
	return helper.TableToBlockStream(ctx, sample, blockSize, c)
}

func (c *CSVBlockStreamFmtReader) ReadFirstRow(fb *bytepool.FrameBuffer, cols []*column.CHColumn) error {
	if c.withNames {
		if err := helper.DiscardUntilByteEscaped(c.zReader, '\n'); err != nil {
			return err
		}
	}

	return helper.ReadRow(fb, cols, c)
}

func (c *CSVBlockStreamFmtReader) ReadRowCont(fb *bytepool.FrameBuffer, cols []*column.CHColumn) error {
	return helper.ReadRow(fb, cols, c)
}

func (c *CSVBlockStreamFmtReader) ReadFirstColumnTexts(
	fb *bytepool.FrameBuffer, numRows int, cols []*column.CHColumn,
) (int, error) {
	return helper.ReadFirstColumnTexts(fb, numRows, cols, c)
}

func (c *CSVBlockStreamFmtReader) ReadColumnTextsCont(
	fb *bytepool.FrameBuffer, numRows int, cols []*column.CHColumn,
) (int, error) {
	return helper.ReadColumnTextsCont(fb, numRows, cols, c)
}

func (c *CSVBlockStreamFmtReader) ReadElem(fb *bytepool.FrameBuffer, cols []*column.CHColumn, idx int) error {
	if idx > 0 {
		if err := helper.AssertNextByteEqual(c.zReader, c.delimiter); err != nil {
			return err
		}
	}
	isSingleCol := len(cols) == 1
	isLast := (len(cols) - 1) == idx
	return c.readElem(fb, cols[idx], isLast, isSingleCol)
}

func (c *CSVBlockStreamFmtReader) readElem(
	fb *bytepool.FrameBuffer, col *column.CHColumn, last bool, singleCol bool,
) error {
	var (
		b   byte
		err error
	)

	if !singleCol && last {
		b, err = helper.ReadNextNonSpaceExceptNewLineByte(c.zReader)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	} else {
		b, err = helper.ReadNextNonSpaceByte(c.zReader)
		if err != nil {
			return err
		}
	}

	var quoted bool
	switch b {
	case '"', '\'', '`':
		quoted = true
	}

	if quoted {
		return c.readElemUntilQuote(fb, b, col)
	}

	c.zReader.UnreadCurrentBuffer(1)
	return c.readElemWithoutQuote(fb, col, last)
}

// readElemUntilQuote notFirstRow from underlying reader until quote is found, return the string notFirstRow excluding quote
func (c *CSVBlockStreamFmtReader) readElemUntilQuote(fb *bytepool.FrameBuffer, quote byte, col *column.CHColumn) error {
	switch col.Data.(type) {
	case *column.FixedStringColumnData, *column.StringColumnData:
		return c.readStringUntilQuote(fb, quote)
	}

	_, err := helper.ReadStringUntilByte(fb, c.zReader, quote)
	return err
}

// readStringUntilQuoteCont assumes that the previous buf has no quote desired
func (c *CSVBlockStreamFmtReader) readStringUntilQuoteCont(fb *bytepool.FrameBuffer, quote byte) error {
	buf, err := c.zReader.ReadNextBuffer()
	if err != nil {
		return err
	}

	i := bytes.IndexByte(buf, quote)
	if i < 0 {
		fb.Write(buf)
		return c.readStringUntilQuoteCont(fb, quote)
	}

	if i == len(buf)-1 {
		fb.Write(buf[:i])
		return c.readStringQuotedCheck(fb, quote)
	}

	return c.readStringQuoteIndexed(fb, buf, i, quote)
}

// readStringQuotedCheck assumes that the last byte of last buf contains the end quote,
// hence need to check the next buf if the quote is actually escaped.
func (c *CSVBlockStreamFmtReader) readStringQuotedCheck(fb *bytepool.FrameBuffer, quote byte) error {
	buf, err := c.zReader.ReadNextBuffer()
	if err != nil {
		return err
	}

	if buf[0] == quote {
		fb.WriteByte(buf[0])
		c.zReader.UnreadCurrentBuffer(len(buf) - 1)
		return c.readStringUntilQuoteCont(fb, quote)
	}

	c.zReader.UnreadCurrentBuffer(len(buf))
	return nil
}

// readStringQuoteIndexed assumes that current buffer from zReader contain the quote at i index,
// and i is not the last index of current buf
func (c *CSVBlockStreamFmtReader) readStringQuoteIndexed(
	fb *bytepool.FrameBuffer, buf []byte, i int, quote byte,
) error {
	if buf[i+1] == quote { // current quote at index i is escaped
		fb.Write(buf[:i+1]) // append everything until including quote
		c.zReader.UnreadCurrentBuffer(len(buf) - i - 2)
		return c.readStringUntilQuoteCont(fb, quote)
	}

	fb.Write(buf[:i])
	c.zReader.UnreadCurrentBuffer(len(buf) - i - 1)
	return nil
}

// readElemWithoutQuote read from buf until Delimiter or newline is reached
func (c *CSVBlockStreamFmtReader) readElemWithoutQuote(
	fb *bytepool.FrameBuffer, col *column.CHColumn, last bool,
) (err error) {
	if last {
		return c.readLastElemWithoutQuote(fb)
	}

	return helper.ReadCHElemTillStop(fb, c.zReader, col.Data, c.delimiter)
}

func (c *CSVBlockStreamFmtReader) readLastElemWithoutQuote(fb *bytepool.FrameBuffer) error {
	_, err := helper.ReadStringUntilByte(fb, c.zReader, '\n')
	switch err {
	case nil:
		//c.zReader.UnreadCurrentBuffer(1)
		return nil
	case io.EOF:
		return nil
	default:
		return err
	}
}

// readStringUntilQuote reads with CSV escape protocol: if quote appears consecutively, it's escaped.
func (c *CSVBlockStreamFmtReader) readStringUntilQuote(fb *bytepool.FrameBuffer, quote byte) error {
	buf, err := c.zReader.ReadNextBuffer()
	if err != nil {
		return err
	}

	i := bytes.IndexByte(buf, quote)

	if i < 0 { // quote byte does not appear in buf
		fb.Write(buf)
		return c.readStringUntilQuoteCont(fb, quote)
	}

	if i == len(buf)-1 { // quote byte reside at last of buf
		fb.Write(buf[:len(buf)-1])
		err := c.readStringQuotedCheck(fb, quote)
		if err == io.EOF { // EOF right after ending quote, marked as successful read
			err = nil
		}
		return err
	}

	return c.readStringQuoteIndexed(fb, buf, i, quote)
}

func resolveCSVDelim(settings map[string]interface{}) (byte, error) {
	delim, ok := settings[csvDelimiterSetting]
	if !ok {
		return ',', nil
	}

	switch delim := delim.(type) {
	case string:
		if len(delim) > 1 {
			return getEscapedDelimiter([]byte(delim))
		}
		if len(delim) == 0 {
			return 0, errors.ErrorfWithCaller("%v settings found, but empty", csvDelimiterSetting)
		}
		return delim[0], nil
	case byte:
		return delim, nil
	default:
		return 0, errors.ErrorfWithCaller("expected type: byte/string for %v, got: %T", csvDelimiterSetting, delim)
	}
}

func getEscapedDelimiter(b []byte) (byte, error) {
	if len(b) > 2 || b[0] != '\\' {
		return 0, errors.ErrorfWithCaller("delimiter should only be 1 byte, got %s", b)
	}

	// The following escape sequences have a corresponding special value: \b, \f, \r, \n, \t, \0, \a, \v
	switch b[1] {
	case 'a':
		return '\a', nil
	case 'b':
		return '\b', nil
	case 'f':
		return '\f', nil
	case 'r':
		return '\r', nil
	case 't':
		return '\t', nil
	case '0':
		return '\000', nil
	case 'n':
		return '\n', nil
	case 'v':
		return '\v', nil
	default:
		return b[1], nil
	}
}
