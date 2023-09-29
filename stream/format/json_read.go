package format

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data/column"
	"github.com/bytehouse-cloud/driver-go/stream/format/helper"
)

//TODO: add option to be able to read \uXXXX as an escaped character

type JSONBlockStreamFmtReader struct {
	zReader      *bytepool.ZReader
	colIdxByName map[string]int
}

func NewJSONBlockStreamFmtReader(r io.Reader) *JSONBlockStreamFmtReader {
	return &JSONBlockStreamFmtReader{
		zReader: bytepool.NewZReaderDefault(r),
	}
}

func (j *JSONBlockStreamFmtReader) ReadFirstRow(fb *bytepool.FrameBuffer, cols []*column.CHColumn) (err error) {
	//return j.readRow(fb, cols)
	if err := j.readRow(fb, cols); err != nil {
		return fmt.Errorf("error in first row %s", err)
	}
	return nil
}

func (j *JSONBlockStreamFmtReader) ReadRowCont(fb *bytepool.FrameBuffer, cols []*column.CHColumn) error {
	b, err := helper.ReadNextNonSpaceByte(j.zReader)
	if err != nil {
		return err
	}

	switch b {
	case ',':
		return j.readRow(fb, cols)
	case ']': // end of more data, rest of data are not insert values
		helper.FlushZReader(j.zReader)
		return io.EOF
	default:
		return fmt.Errorf("json: read row error: expect: ',' or ']', got: %q", b)
	}
}

// readRow reads a row of data
// Expected row format: {col:val, col:val, col:val}
func (j *JSONBlockStreamFmtReader) readRow(fb *bytepool.FrameBuffer, cols []*column.CHColumn) error {
	if err := helper.AssertNextByteEqual(j.zReader, '{'); err != nil {
		return err
	}

	// store the order of columns read
	order := make([]int, len(cols))
	sb := bytepool.NewStringsBuffer()

	for i := range cols {
		if i > 0 {
			if err := helper.AssertNextByteEqual(j.zReader, ','); err != nil {
				return err
			}
		}

		columnName, err := j.readColumnName()
		if err != nil {
			return fmt.Errorf("error reading column name at idx %v: %s", i, err)
		}

		colIdx, ok := j.colIdxByName[columnName]
		if !ok {
			return fmt.Errorf("key error at idx %v, no key found for: %v", i, columnName)
		}
		order[i] = colIdx

		if err := helper.AssertNextByteEqual(j.zReader, ':'); err != nil {
			return fmt.Errorf("read colon error at idx %v: %s", i, err)
		}

		sb.NewElem()
		if err := j.readElem(sb, cols[colIdx], i == len(cols)-1); err != nil {
			return fmt.Errorf(errReadElem, cols[colIdx].Type, cols[colIdx].Name, i, err)
		}
	}

	// get results (order based on user)
	unordered_result := make([]string, len(cols))
	sb.ExportTo(unordered_result)

	// order based on sample data block from server
	result := make([]string, len(cols))
	for _, i := range order {
		result[i] = unordered_result[order[i]]
	}

	for _, s := range result {
		fb.NewElem()
		fb.WriteString(s)
	}

	return helper.AssertNextByteEqual(j.zReader, '}')
}

func (j *JSONBlockStreamFmtReader) readElem(w helper.Writer, col *column.CHColumn, last bool) error {
	if err := helper.AssertNextByteEqual(j.zReader, '"'); err != nil {
		if err == io.EOF {
			return io.EOF
		}

		j.zReader.UnreadCurrentBuffer(1)
		return j.readElemUnquoted(w, col, last)
	}

	_, err := helper.ReadStringUntilByte(w, j.zReader, '"')
	return err
}

func (j *JSONBlockStreamFmtReader) readElemUnquoted(w helper.Writer, col *column.CHColumn, last bool) error {
	if last {
		return helper.ReadCHElemTillStop(w, j.zReader, col.Data, '}')
	}
	return helper.ReadCHElemTillStop(w, j.zReader, col.Data, ',')
}

func (j *JSONBlockStreamFmtReader) ReadFirstColumnTexts(fb *bytepool.FrameBuffer, numRows int, cols []*column.CHColumn) (int, error) {
	if err := j.skipMeta(); err != nil {
		return 0, err
	}
	j.mapNameIdx(cols)
	return helper.ReadFirstColumnTexts(fb, numRows, cols, j)
}

func (j *JSONBlockStreamFmtReader) ReadColumnTextsCont(fb *bytepool.FrameBuffer, numRows int, cols []*column.CHColumn) (int, error) {
	return helper.ReadColumnTextsCont(fb, numRows, cols, j)
}

func (j *JSONBlockStreamFmtReader) BlockStreamFmtRead(ctx context.Context, sample *data.Block, blockSize int,
) (blockStream <-chan *data.Block, yield func() (int, error)) {
	return helper.TableToBlockStream(ctx, sample, blockSize, j)
}

func (j *JSONBlockStreamFmtReader) readColumnName() (string, error) {
	if err := helper.AssertNextByteEqual(j.zReader, '"'); err != nil {
		return "", err
	}

	var sb strings.Builder
	if _, err := helper.ReadStringUntilByte(&sb, j.zReader, '"'); err != nil {
		return "", err
	}

	return sb.String(), nil
}

// readStringUntilQuoteCont assumes that the previous columnTextsPool has no quote desired
func (j *JSONBlockStreamFmtReader) readStringUntilQuoteCont(fb *bytepool.FrameBuffer, quote byte) error {
	buf, err := j.zReader.ReadNextBuffer()
	if err != nil {
		return err
	}

	i := bytes.IndexByte(buf, quote)
	if i < 0 {
		fb.Write(buf)
		return j.readStringUntilQuoteCont(fb, quote)
	}

	if i == len(buf)-1 {
		fb.Write(buf[:i])
		return j.readStringQuotedCheck(fb, quote)
	}

	return j.readStringQuoteIndexed(fb, buf, i, quote)
}

// readStringQuotedCheck assumes that the last byte of last columnTextsPool contains the end quote,
// hence need to check the next columnTextsPool if the quote is actually escaped.
func (j *JSONBlockStreamFmtReader) readStringQuotedCheck(fb *bytepool.FrameBuffer, quote byte) error {
	buf, err := j.zReader.ReadNextBuffer()
	if err != nil {
		return err
	}

	if buf[0] == quote {
		fb.WriteByte(buf[0])
		j.zReader.UnreadCurrentBuffer(len(buf) - 1)
		return j.readStringUntilQuoteCont(fb, quote)
	}

	j.zReader.UnreadCurrentBuffer(len(buf))
	return nil
}

// readStringQuoteIndexed assumes that current buffer from zReader contain the quote at i index,
// and i is not the last index of current columnTextsPool
func (j *JSONBlockStreamFmtReader) readStringQuoteIndexed(fb *bytepool.FrameBuffer, buf []byte, i int, quote byte) error {
	if buf[i+1] == quote { // current quote at index i is escaped
		fb.Write(buf[:i+1]) // append everything until including quote
		j.zReader.UnreadCurrentBuffer(len(buf) - i - 2)
		return j.readStringUntilQuoteCont(fb, quote)
	}

	fb.Write(buf[:i])
	j.zReader.UnreadCurrentBuffer(len(buf) - i - 1)
	return nil
}

func (j *JSONBlockStreamFmtReader) readStringUntilQuote(fb *bytepool.FrameBuffer, quote byte) error {
	buf, err := j.zReader.ReadNextBuffer()
	if err != nil {
		return err
	}

	i := bytes.IndexByte(buf, quote)

	if i < 0 { // quote byte does not appear in buf
		fb.Write(buf)
		return j.readStringUntilQuoteCont(fb, quote)
	}

	if i == len(buf)-1 { // edge case where the quote byte is at the last index of buf
		fb.Write(buf[:len(buf)-1])
		return j.readStringQuotedCheck(fb, quote)
	}

	return j.readStringQuoteIndexed(fb, buf, i, quote)
}

// skipMeta skips meta field of the json data until the first square bracket in the data field
// It assumes we either
// 1. Do not have meta field e.g. {"data": [...]} OR
// 2. Have meta field e.g. {"meta": [.................], "data": [...]}
// Either ways it will skip to the first square brace in the data field
// e.g. {..., "data": [...]}
//                    ^ here
func (j *JSONBlockStreamFmtReader) skipMeta() error {
	// the second encountered [ denotes the start of data
	var countRead int
	var exitNextSquareOpenBrace bool
	for {
		b, err := helper.ReadNextNonSpaceByte(j.zReader)
		if err != nil {
			return err
		}
		countRead++

		// If third char is not 'm' -> means no meta
		// Exit next time we encounter square open brace
		// e.g. {"data": [...]}
		//               ^ here
		if countRead == 3 && b != 'm' {
			exitNextSquareOpenBrace = true
			continue
		}

		if b == '[' && exitNextSquareOpenBrace {
			break
		}

		// If encounter first square open brace, exit when reach next square open brave
		// e.g. {"meta": [.................], "data": [...]}
		//               ^ currently here             ^ exit here
		if b == '[' {
			exitNextSquareOpenBrace = true
		}
	}

	return nil
}

func (j *JSONBlockStreamFmtReader) mapNameIdx(cols []*column.CHColumn) {
	j.colIdxByName = make(map[string]int)
	for i, col := range cols {
		j.colIdxByName[col.Name] = i
	}
}

func (j *JSONBlockStreamFmtReader) checkOptionalSquareClosingBracket() error {
	b, err := helper.ReadNextNonSpaceByte(j.zReader)
	if err != nil {
		return err
	}
	if b != ']' {
		j.zReader.UnreadCurrentBuffer(1)
		return nil
	}
	return io.EOF
}
