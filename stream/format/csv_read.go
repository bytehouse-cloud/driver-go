package format

import (
	"bytes"
	"context"
	"io"
	"strings"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data/column"
	"github.com/bytehouse-cloud/driver-go/errors"
	"github.com/bytehouse-cloud/driver-go/stream/format/helper"
)

const csvDelimiterSetting = "format_csv_delimiter"

type CSVBlockStreamFmtReader struct {
	zReader   *bytepool.ZReader
	delimiter byte

	totalRowsRead int
	exception     error
	done          chan struct{}
	withNames     bool
}

func NewCSVBlockStreamFmtReader(r io.Reader, withNames bool, settings map[string]interface{}) (*CSVBlockStreamFmtReader, error) {
	delim, err := resolveCSVDelim(settings)
	if err != nil {
		return nil, err
	}

	return &CSVBlockStreamFmtReader{
		zReader:   bytepool.NewZReaderDefault(r),
		delimiter: delim,
		withNames: withNames,
	}, nil
}

func (c *CSVBlockStreamFmtReader) ReadFirstRow(colTexts [][]string, cols []*column.CHColumn) error {
	if c.withNames {
		if err := helper.DiscardUntilByteEscaped(c.zReader, '\n'); err != nil {
			return err
		}
	}
	return c.readRow(colTexts, 0, cols)
}

func (c *CSVBlockStreamFmtReader) ReadRowCont(colTexts [][]string, rowIdx int, cols []*column.CHColumn) error {
	return c.readRow(colTexts, rowIdx, cols)
}

func (c *CSVBlockStreamFmtReader) ReadFirstColumnTexts(colTexts [][]string, cols []*column.CHColumn) (int, error) {
	return helper.ReadFirstColumnTexts(colTexts, cols, c)
}

func (c *CSVBlockStreamFmtReader) ReadColumnTextsCont(colTexts [][]string, cols []*column.CHColumn) (int, error) {
	return helper.ReadColumnTextsCont(colTexts, cols, c)
}

func (c *CSVBlockStreamFmtReader) BlockStreamFmtRead(ctx context.Context, sample *data.Block, blockSize int) <-chan *data.Block {
	c.done = make(chan struct{}, 1)
	return helper.ReadColumnTextsToBlockStream(ctx, sample, blockSize, c, &c.exception, &c.totalRowsRead, func() {
		c.done <- struct{}{}
	})
}

func (c *CSVBlockStreamFmtReader) Yield() (int, error) {
	<-c.done
	return c.totalRowsRead, c.exception
}

func (c *CSVBlockStreamFmtReader) readRow(colTexts [][]string, rowIdx int, cols []*column.CHColumn) error {
	for colIdx := 0; colIdx < len(colTexts)-1; colIdx++ {
		s, err := c.readElem(cols[colIdx], false)
		switch err {
		case nil:
		case io.EOF:
			if colIdx > 0 { //if not reading first row and EOF
				return errors.ErrorfWithCaller(readElemErr, cols[colIdx].Type, cols[colIdx].Name, colIdx, err)
			}
			if s == "" {
				return io.EOF
			}
		default:
			return err
		}
		colTexts[colIdx][rowIdx] = s

		if err := c.readDelimiter(); err != nil {
			return errors.ErrorfWithCaller("read delimiter error: %s", err)
		}
	}

	lastColIdx := len(colTexts) - 1
	s, err := c.readElem(cols[len(colTexts)-1], true)
	if err == io.EOF && len(colTexts) == 1 { // the first elem is not allowed to be nothing
		return err
	}
	if err != nil && err != io.EOF { // last elem permitted to be nothing
		return errors.ErrorfWithCaller(readElemErr, cols[lastColIdx].Type, cols[lastColIdx].Name, lastColIdx, err)
	}
	colTexts[lastColIdx][rowIdx] = s

	return nil
}

func (c *CSVBlockStreamFmtReader) readElem(col *column.CHColumn, last bool) (string, error) {
	var (
		b   byte
		err error
	)

	b, err = helper.ReadNextNonSpaceByte(c.zReader)

	if err != nil {
		return "", err
	}

	var quoted bool
	switch b {
	case '"', '\'', '`':
		quoted = true
	}

	if quoted {
		return c.readElemUntilQuote(b, col)
	}

	c.zReader.UnreadCurrentBuffer(1)
	return c.readElemWithoutQuote(col, last)
}

// readElemUntilQuote notFirstRow from underlying reader until quote is found, return the string notFirstRow excluding quote
func (c *CSVBlockStreamFmtReader) readElemUntilQuote(quote byte, col *column.CHColumn) (string, error) {
	switch col.Data.(type) {
	case *column.FixedStringColumnData, *column.StringColumnData:
		return c.readStringUntilQuote(quote)
	}

	return helper.ReadStringUntilByte(c.zReader, quote)
}

func (c *CSVBlockStreamFmtReader) readDelimiter() error {
	b, err := helper.ReadNextNonSpaceByte(c.zReader)
	if err != nil {
		return err
	}

	if b != c.delimiter {
		return errors.ErrorfWithCaller(expectedByteButGot, c.delimiter, b)
	}

	return nil
}

// readStringUntilQuoteCont assumes that the previous buf has no quote desired
func (c *CSVBlockStreamFmtReader) readStringUntilQuoteCont(builder *strings.Builder, quote byte) (string, error) {
	buf, err := c.zReader.ReadNextBuffer()
	if err != nil {
		return "", err
	}

	i := bytes.IndexByte(buf, quote)
	if i < 0 {
		builder.Write(buf)
		return c.readStringUntilQuoteCont(builder, quote)
	}

	if i == len(buf)-1 {
		builder.Write(buf[:i])
		return c.readStringQuotedCheck(builder, quote)
	}

	return c.readStringQuoteIndexed(builder, buf, i, quote)
}

// readStringQuotedCheck assumes that the last byte of last buf contains the end quote,
// hence need to check the next buf if the quote is actually escaped.
func (c *CSVBlockStreamFmtReader) readStringQuotedCheck(builder *strings.Builder, quote byte) (string, error) {
	buf, err := c.zReader.ReadNextBuffer()
	if err != nil {
		return "", err
	}

	if buf[0] == quote {
		builder.WriteByte(buf[0])
		c.zReader.UnreadCurrentBuffer(len(buf) - 1)
		return c.readStringUntilQuoteCont(builder, quote)
	}

	c.zReader.UnreadCurrentBuffer(len(buf))
	return builder.String(), nil
}

// readStringQuoteIndexed assumes that current buffer from zReader contain the quote at i index,
// and i is not the last index of current buf
func (c *CSVBlockStreamFmtReader) readStringQuoteIndexed(builder *strings.Builder, buf []byte, i int, quote byte) (string, error) {
	if buf[i+1] == quote { // current quote at index i is escaped
		builder.Write(buf[:i+1]) // append everything until including quote
		c.zReader.UnreadCurrentBuffer(len(buf) - i - 2)
		return c.readStringUntilQuoteCont(builder, quote)
	}

	builder.Write(buf[:i])
	c.zReader.UnreadCurrentBuffer(len(buf) - i - 1)
	return builder.String(), nil
}

// readElemWithoutQuote read from buf until Delimiter or newline is reached
func (c *CSVBlockStreamFmtReader) readElemWithoutQuote(col *column.CHColumn, last bool) (string, error) {
	if last {
		return c.readLastElemWithoutQuote()
	}

	return helper.ReadCHElem(c.zReader, col, c.delimiter)
}

func (c *CSVBlockStreamFmtReader) readLastElemWithoutQuote() (string, error) {
	s, err := helper.ReadStringUntilByte(c.zReader, '\n')
	switch err {
	case nil:
		c.zReader.UnreadCurrentBuffer(1)
		return s, nil
	case io.EOF:
		return s, nil
	default:
		return s, err
	}
}

// readStringUntilQuote reads with CSV escape protocol: if quote appears consecutively, it's escaped.
func (c *CSVBlockStreamFmtReader) readStringUntilQuote(quote byte) (string, error) {
	var builder strings.Builder

	buf, err := c.zReader.ReadNextBuffer()
	if err != nil {
		return "", err
	}

	i := bytes.IndexByte(buf, quote)

	if i < 0 { // quote byte does not appear in buf
		builder.Write(buf)
		return c.readStringUntilQuoteCont(&builder, quote)
	}

	if i == len(buf)-1 { // edge case where the quote byte reside at last of buf
		builder.Write(buf[:len(buf)-1])
		return c.readStringQuotedCheck(&builder, quote)
	}

	return c.readStringQuoteIndexed(&builder, buf, i, quote)
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

	//The following escape sequences have a corresponding special value: \b, \f, \r, \n, \t, \0, \a, \v
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
