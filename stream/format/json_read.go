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

type JSONBlockStreamFmtReader struct {
	zReader *bytepool.ZReader

	totalRowsRead int
	exception     error
	done          chan struct{}
	mapColNameIdx map[string]int
}

func (j *JSONBlockStreamFmtReader) ReadFirstRow(colTexts [][]string, cols []*column.CHColumn) error {
	return j.readRow(colTexts, 0, cols)
}

func (j *JSONBlockStreamFmtReader) ReadRowCont(colTexts [][]string, rowIdx int, cols []*column.CHColumn) error {
	j.readOptionalComma()
	return j.readRow(colTexts, rowIdx, cols)
}

func (j *JSONBlockStreamFmtReader) ReadFirstColumnTexts(colTexts [][]string, cols []*column.CHColumn) (int, error) {
	if err := j.skipMeta(); err != nil {
		return 0, err
	}
	j.mapNameIdx(cols)
	return helper.ReadFirstColumnTexts(colTexts, cols, j)
}

func (j *JSONBlockStreamFmtReader) ReadColumnTextsCont(colTexts [][]string, cols []*column.CHColumn) (int, error) {
	return helper.ReadColumnTextsCont(colTexts, cols, j)
}

func NewJSONBlockStreamFmtReader(r io.Reader) *JSONBlockStreamFmtReader {
	return &JSONBlockStreamFmtReader{
		zReader: bytepool.NewZReaderDefault(r),
	}
}

func (j *JSONBlockStreamFmtReader) BlockStreamFmtRead(ctx context.Context, sample *data.Block, blockSize int) <-chan *data.Block {
	j.done = make(chan struct{}, 1)
	return helper.ReadColumnTextsToBlockStream(ctx, sample, blockSize, j, &j.exception, &j.totalRowsRead, func() {
		j.done <- struct{}{}
	})
}

func (j *JSONBlockStreamFmtReader) Yield() (int, error) {
	<-j.done
	return j.totalRowsRead, j.exception
}

// readRow reads a row of data
// Expected row format: {col:val, col:val, col:val}
func (j *JSONBlockStreamFmtReader) readRow(colTexts [][]string, rowIdx int, cols []*column.CHColumn) error {

	// 1st check if encounter the closing square bracket, which denotes the end of data
	err := j.checkOptionalSquareClosingBracket()
	if err != nil {
		return err
	}

	if err := j.readOpenCurlyBracket(); err != nil {
		return err
	}

	// Read first n - 1 columns
	for i := 0; i < len(colTexts)-1; i++ {
		colName, err := j.readElem(cols[i], false)
		if err != nil {
			return err
		}
		if err := j.readColon(); err != nil {
			return err
		}
		colValue, err := j.readElem(cols[i], false)
		colIdx := j.mapColNameIdx[colName]
		if err != nil {
			if i > 0 && err != io.EOF { //if not reading first row and EOF
				return errors.ErrorfWithCaller(readElemErr, cols[colIdx].Type, cols[colIdx].Name, colIdx, err)
			}
			return err
		}
		colTexts[colIdx][rowIdx] = colValue

		if err := j.readDelimiter(); err != nil {
			return errors.ErrorfWithCaller("read delimiter error: %s", err)
		}
	}

	// Read nth column
	colName, err := j.readElem(cols[len(colTexts)-1], false)
	if err != nil {
		return err
	}
	if err := j.readColon(); err != nil {
		return err
	}
	colValue, err := j.readElem(cols[len(colTexts)-1], true)
	colIdx := j.mapColNameIdx[colName]

	if err != nil && err != io.EOF { // last col permitted to be nothing
		return errors.ErrorfWithCaller(readElemErr, cols[colIdx].Type, colName, colIdx, err)
	}
	colTexts[colIdx][rowIdx] = colValue

	if err := j.readCloseCurlyBracket(); err != nil {
		return errors.ErrorfWithCaller("error reading close curly bracket: %s", err)
	}

	j.readOptionalComma()

	return nil
}

func (j *JSONBlockStreamFmtReader) readElem(col *column.CHColumn, last bool) (string, error) {
	var (
		b   byte
		err error
	)

	b, err = helper.ReadNextNonSpaceByte(j.zReader)

	if err != nil {
		return "", err
	}

	var quoted bool
	if b == '"' {
		quoted = true
	}

	if quoted {
		return j.readElemUntilQuote(b, col)
	}

	j.zReader.UnreadCurrentBuffer(1)
	return j.readElemWithoutQuote(col, last)
}

// readElemUntilQuote notFirstRow from underlying reader until quote is found, return the string notFirstRow excluding quote
func (j *JSONBlockStreamFmtReader) readElemUntilQuote(quote byte, col *column.CHColumn) (string, error) {
	switch col.Data.(type) {
	case *column.FixedStringColumnData, *column.StringColumnData:
		return j.readStringUntilQuote(quote)
	}

	return helper.ReadStringUntilByte(j.zReader, quote)
}

func (j *JSONBlockStreamFmtReader) readDelimiter() error {
	b, err := helper.ReadNextNonSpaceByte(j.zReader)
	if err != nil {
		return err
	}

	if b != ',' {
		return errors.ErrorfWithCaller(expectedByteButGot, ',', b)
	}

	return nil
}

// readStringUntilQuoteCont assumes that the previous columnTextsPool has no quote desired
func (j *JSONBlockStreamFmtReader) readStringUntilQuoteCont(builder *strings.Builder, quote byte) (string, error) {
	buf, err := j.zReader.ReadNextBuffer()
	if err != nil {
		return "", err
	}

	i := bytes.IndexByte(buf, quote)
	if i < 0 {
		builder.Write(buf)
		return j.readStringUntilQuoteCont(builder, quote)
	}

	if i == len(buf)-1 {
		builder.Write(buf[:i])
		return j.readStringQuotedCheck(builder, quote)
	}

	return j.readStringQuoteIndexed(builder, buf, i, quote)
}

// readStringQuotedCheck assumes that the last byte of last columnTextsPool contains the end quote,
// hence need to check the next columnTextsPool if the quote is actually escaped.
func (j *JSONBlockStreamFmtReader) readStringQuotedCheck(builder *strings.Builder, quote byte) (string, error) {
	buf, err := j.zReader.ReadNextBuffer()
	if err != nil {
		return "", err
	}

	if buf[0] == quote {
		builder.WriteByte(buf[0])
		j.zReader.UnreadCurrentBuffer(len(buf) - 1)
		return j.readStringUntilQuoteCont(builder, quote)
	}

	j.zReader.UnreadCurrentBuffer(len(buf))
	return builder.String(), nil
}

// readStringQuoteIndexed assumes that current buffer from zReader contain the quote at i index,
// and i is not the last index of current columnTextsPool
func (j *JSONBlockStreamFmtReader) readStringQuoteIndexed(builder *strings.Builder, buf []byte, i int, quote byte) (string, error) {
	if buf[i+1] == quote { // current quote at index i is escaped
		builder.Write(buf[:i+1]) // append everything until including quote
		j.zReader.UnreadCurrentBuffer(len(buf) - i - 2)
		return j.readStringUntilQuoteCont(builder, quote)
	}

	builder.Write(buf[:i])
	j.zReader.UnreadCurrentBuffer(len(buf) - i - 1)
	return builder.String(), nil
}

// readElemWithoutQuote notFirstRow from columnTextsPool until Delimiter or newline is reached
func (j *JSONBlockStreamFmtReader) readElemWithoutQuote(col *column.CHColumn, last bool) (string, error) {
	if last {
		return j.readLastElemWithoutQuote()
	}

	return helper.ReadCHElem(j.zReader, col, ',')
}

func (j *JSONBlockStreamFmtReader) readLastElemWithoutQuote() (string, error) {
	s, err := helper.ReadStringUntilByte(j.zReader, '}')
	switch err {
	case nil:
		j.zReader.UnreadCurrentBuffer(1)
		return s, nil
	case io.EOF:
		return s, nil
	default:
		return s, err
	}
}

func (j *JSONBlockStreamFmtReader) readStringUntilQuote(quote byte) (string, error) {
	var builder strings.Builder

	buf, err := j.zReader.ReadNextBuffer()
	if err != nil {
		return "", err
	}

	i := bytes.IndexByte(buf, quote)

	if i < 0 { // quote byte does not appear in buf
		builder.Write(buf)
		return j.readStringUntilQuoteCont(&builder, quote)
	}

	if i == len(buf)-1 { // edge case where the quote byte is at the last index of buf
		builder.Write(buf[:len(buf)-1])
		return j.readStringQuotedCheck(&builder, quote)
	}

	return j.readStringQuoteIndexed(&builder, buf, i, quote)
}

func (j *JSONBlockStreamFmtReader) readOpenCurlyBracket() error {
	b, err := helper.ReadNextNonSpaceByte(j.zReader)
	if err != nil {
		return err
	}
	if b != '{' {
		return errors.ErrorfWithCaller(expectedByteButGot, '{', b)
	}
	return nil
}

func (j *JSONBlockStreamFmtReader) readCloseCurlyBracket() error {
	b, err := helper.ReadNextNonSpaceByte(j.zReader)
	if err != nil {
		return err
	}
	if b != '}' {
		return errors.ErrorfWithCaller(expectedByteButGot, '}', b)
	}
	return nil
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
	j.mapColNameIdx = make(map[string]int)
	for i := range cols {
		j.mapColNameIdx[cols[i].Name] = i
	}
}

func (j *JSONBlockStreamFmtReader) readOptionalComma() {
	b, err := helper.ReadNextNonSpaceByte(j.zReader)
	if err != nil {
		return
	}
	if b != ',' {
		j.zReader.UnreadCurrentBuffer(1)
	}
}

func (j *JSONBlockStreamFmtReader) readColon() error {
	b, err := helper.ReadNextNonSpaceByte(j.zReader)
	if err != nil {
		return err
	}
	if b != ':' {
		return errors.ErrorfWithCaller(expectedByteButGot, ':', b)
	}
	return nil
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

	// encountered ], then skip the rest of the stream
	for {
		if _, err := helper.ReadNextNonSpaceByte(j.zReader); err != nil {
			return err
		}
	}
}
