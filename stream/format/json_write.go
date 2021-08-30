package format

import (
	"io"
	"strconv"
	"strings"

	"github.com/jfcg/sixb"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data/column"
	"github.com/bytehouse-cloud/driver-go/stream/format/helper"
)

var (
	dataSegmentBegin                          = []byte("\n\t\"data\":\n\t[\n")
	metaSegmentBegin                          = []byte("\n\t\"meta\":\n\t[")
	firstMetaPairBegin                        = []byte("\n\t\t{\n\t\t\t\"name\": \"")
	metaPairBetweenNameAndCol                 = []byte("\",\n\t\t\t\"type\": \"")
	metaPairEnd                               = []byte{'"', '\n', '\t', '\t', '}'}
	contMetaPairBegin                         = []byte(",\n\t\t{\n\t\t\t\"name\": \"")
	jsonEnd                                   = []byte{'\n', '}', '\n'}
	rowsInfo                                  = []byte("\n\t\"rows\": ")
	segmentEnd                                = []byte{'\n', '\t', ']', ',', '\n'}
	segmentBegin                              = []byte{'\t', '\t', '{'}
	threeIndentation                          = []byte{'\t', '\t', '\t'}
	newlineCloseCurlyBracesWithTwoIndentation = []byte{'\n', '\t', '\t', '}'}
	newLineThreeIndentationDoubleQuote        = []byte{'\n', '\t', '\t', '\t', '"'}
	doubleQuoteColonSpace                     = []byte("\": ")
)

const (
	escapeBackslash        = "\\"
	doubleEscapeBackslash  = "\\\\"
	slash                  = "/"
	escapeBacklashAndSlash = "\\/"
	escapeDoubleQuotes     = "\"\""
	doubleQuote            = "\""
)

// for the convenience of development:
// some useful commands:
// select * from table1 into outfile 'out1.json' format JSON
// select * from table2 into outfile 'out2.json' format JSON
// select * from table3 into outfile 'out3.json' format JSON
// select * from arr into outfile 'outarr.json' format JSON
// select * from str into outfile 'outstr.json' format JSON
// go tool pprof -http=:5000 cpu.pprof

type JSONBlockStreamFmtWriter struct {
	zWriter        *bytepool.ZWriter
	delimiterBytes []byte

	totalRowsWrite int
	exception      error
	done           chan struct{}
}

func NewJSONBlockStreamFmtWriter(w io.Writer) *JSONBlockStreamFmtWriter {
	newWriter := &JSONBlockStreamFmtWriter{
		zWriter:        bytepool.NewZWriterDefault(w),
		delimiterBytes: []byte{','},
	}
	return newWriter
}

func (j *JSONBlockStreamFmtWriter) BlockStreamFmtWrite(blockStream <-chan *data.Block) {
	j.done = make(chan struct{}, 1)
	go j.blockStreamFmtWrite(blockStream)
}

func (j *JSONBlockStreamFmtWriter) blockStreamFmtWrite(blockStream <-chan *data.Block) {
	defer func() {
		j.done <- struct{}{}
	}()
	j.totalRowsWrite, j.exception = helper.WriteBlockSteamToFrame(blockStream, j)
}

func (j *JSONBlockStreamFmtWriter) Yield() (int, error) {
	<-j.done
	return j.totalRowsWrite, j.exception
}

func (j *JSONBlockStreamFmtWriter) WriteFirstRow(record []string, cols []*column.CHColumn) error {
	err := j.writeRow(record, cols)
	if err != nil {
		return err
	}
	return err
}

func (j *JSONBlockStreamFmtWriter) WriteRowCont(record []string, cols []*column.CHColumn) error {
	if err := j.zWriter.WriteString(",\n"); err != nil {
		return err
	}
	return j.writeRow(record, cols)
}

func (j *JSONBlockStreamFmtWriter) WriteFirstFrame(frame [][]string, cols []*column.CHColumn) (int, error) {
	if err := j.writeJSONHead(); err != nil {
		return 0, err
	}
	if err := j.writeMeta(cols); err != nil {
		return 0, err
	}
	if count, err := j.writeDataHeader(); err != nil {
		return count, err
	}
	return helper.WriteFirstFrame(frame, cols, j)
}

func (j *JSONBlockStreamFmtWriter) WriteFrameCont(frame [][]string, cols []*column.CHColumn) (int, error) {
	count, err := helper.WriteFrameCont(frame, cols, j)
	return count, err
}

func (j *JSONBlockStreamFmtWriter) Flush() error {
	return j.zWriter.Flush()
}

func (j *JSONBlockStreamFmtWriter) writeRow(record []string, cols []*column.CHColumn) error {
	if _, err := j.zWriter.Write(segmentBegin); err != nil {
		return err
	}
	for i := 0; i < len(cols)-1; i++ {
		if _, err := j.zWriter.Write(threeIndentation); err != nil {
			return err
		}
		if err := j.writeColumn(record[i], cols[i]); err != nil {
			return err
		}
		if _, err := j.zWriter.Write(j.delimiterBytes); err != nil {
			return err
		}
	}
	if _, err := j.zWriter.Write(threeIndentation); err != nil {
		return err
	}
	if err := j.writeColumn(record[len(cols)-1], cols[len(cols)-1]); err != nil {
		return err
	}
	if _, err := j.zWriter.Write(newlineCloseCurlyBracesWithTwoIndentation); err != nil {
		return err
	}
	j.totalRowsWrite++
	return nil
}

func (j *JSONBlockStreamFmtWriter) writeColumn(s string, col *column.CHColumn) error {
	// json data format: "col name": value,
	if err := j.writeColumnName(col); err != nil {
		return err
	}
	switch col.Data.(type) {
	case *column.ArrayColumnData, *column.DecimalColumnData, *column.IPv4ColumnData, *column.IPv6ColumnData,
		*column.UInt8ColumnData, *column.UInt16ColumnData, *column.UInt32ColumnData, *column.Int8ColumnData,
		*column.Int16ColumnData, *column.Int32ColumnData:
		if err := j.zWriter.WriteString(s); err != nil {
			return err
		}
		return nil
	default:
		if err := j.zWriter.WriteByte('"'); err != nil {
			return err
		}

		escapedString := j.escape(s)
		if err := j.zWriter.WriteString(escapedString); err != nil {
			return err
		}
		if err := j.zWriter.WriteByte('"'); err != nil {
			return err
		}
		return nil
	}
}

func (j *JSONBlockStreamFmtWriter) writeColumnName(col *column.CHColumn) error {
	// "col name":
	if _, err := j.zWriter.Write(newLineThreeIndentationDoubleQuote); err != nil {
		return err
	}
	if err := j.zWriter.WriteString(col.Name); err != nil {
		return err
	}
	if _, err := j.zWriter.Write(doubleQuoteColonSpace); err != nil {
		return err
	}
	return nil
}

func (j *JSONBlockStreamFmtWriter) writeColumnWithCheck(s string) error {
	for i := strings.IndexByte(s, '"'); i >= 0; i = strings.IndexByte(s, '"') {
		if err := j.writeString(s[:i]); err != nil {
			return err
		}
		if _, err := j.zWriter.Write([]byte("\"\"")); err != nil {
			return err
		}
		s = s[i+1:]
	}

	if err := j.writeString(s); err != nil {
		return err
	}

	return nil
}

func (j *JSONBlockStreamFmtWriter) writeString(s string) error {
	_, err := j.zWriter.Write(sixb.StB(s))
	return err
}

func (j *JSONBlockStreamFmtWriter) WriteEnd() error {
	if _, err := j.zWriter.Write(segmentEnd); err != nil {
		return err
	}
	if err := j.writeJSONTail(); err != nil {
		return err
	}
	return nil
}

func (j *JSONBlockStreamFmtWriter) writeDataHeader() (int, error) {
	if _, err := j.zWriter.Write(dataSegmentBegin); err != nil {
		return 0, err
	}

	return 0, nil
}

func (j *JSONBlockStreamFmtWriter) writeMeta(cols []*column.CHColumn) error {
	if err := j.writeMetaHeader(); err != nil {
		return err
	}
	if err := j.writeMetaContent(cols); err != nil {
		return err
	}
	if err := j.writeMetaEnd(); err != nil {
		return err
	}
	return nil
}

func (j *JSONBlockStreamFmtWriter) writeMetaHeader() error {
	if _, err := j.zWriter.Write(metaSegmentBegin); err != nil {
		return err
	}
	return nil
}

func (j *JSONBlockStreamFmtWriter) writeMetaContent(cols []*column.CHColumn) error {
	if err := j.writeFirstMetaPair(cols[0]); err != nil {
		return err
	}
	if err := j.writeMetaPairCont(cols); err != nil {
		return err
	}
	return nil
}

func (j *JSONBlockStreamFmtWriter) writeFirstMetaPair(col *column.CHColumn) error {
	colName := col.Name
	colType := string(col.Type)
	// head
	if _, err := j.zWriter.Write(firstMetaPairBegin); err != nil {
		return err
	}
	// col name
	if err := j.zWriter.WriteString(colName); err != nil {
		return err
	}
	if _, err := j.zWriter.Write(metaPairBetweenNameAndCol); err != nil {
		return err
	}
	// col type
	if err := j.zWriter.WriteString(colType); err != nil {
		return err
	}
	//tail
	if _, err := j.zWriter.Write(metaPairEnd); err != nil {
		return err
	}
	return nil
}

func (j *JSONBlockStreamFmtWriter) writeMetaPairCont(cols []*column.CHColumn) error {
	for i := 1; i < len(cols); i++ {
		colName := cols[i].Name
		colType := string(cols[i].Type)
		// head
		if _, err := j.zWriter.Write(contMetaPairBegin); err != nil {
			return err
		}
		// col name
		if err := j.zWriter.WriteString(colName); err != nil {
			return err
		}
		if _, err := j.zWriter.Write(metaPairBetweenNameAndCol); err != nil {
			return err
		}
		// col type
		if err := j.zWriter.WriteString(colType); err != nil {
			return err
		}
		//tail
		if _, err := j.zWriter.Write(metaPairEnd); err != nil {
			return err
		}
	}

	return nil
}

func (j *JSONBlockStreamFmtWriter) writeJSONHead() error {
	if err := j.zWriter.WriteByte('{'); err != nil {
		return err
	}
	return nil
}

func (j *JSONBlockStreamFmtWriter) writeJSONTail() error {
	if err := j.writeEndingInfo(); err != nil {
		return err
	}
	if _, err := j.zWriter.Write(jsonEnd); err != nil {
		return err
	}
	return nil
}

func (j *JSONBlockStreamFmtWriter) writeEndingInfo() error {
	if _, err := j.zWriter.Write(rowsInfo); err != nil {
		return err
	}
	if err := j.zWriter.WriteString(strconv.Itoa(j.totalRowsWrite)); err != nil {
		return err
	}
	return nil
}

func (j *JSONBlockStreamFmtWriter) writeMetaEnd() error {
	if _, err := j.zWriter.Write(segmentEnd); err != nil {
		return err
	}
	return nil
}

func (j *JSONBlockStreamFmtWriter) escape(s string) string {
	// escape all special characters
	var builder strings.Builder
	length := len(s)
	for i := 0; i < length; i++ {
		thisString := s[i : i+1]
		switch thisString {
		case escapeBackslash:
			builder.WriteString(doubleEscapeBackslash)
			// check if is \u2028 or \u2029
			if (i+5) < length && s[i+1] == 'u' && s[i+2] == '2' && s[i+3] == '0' && s[i+4] == '2' {
				if s[i+5] == '8' || s[i+5] == '9' {
					builder.WriteString("uxxxx")
				}
			}
		case slash:
			builder.WriteString(escapeBacklashAndSlash)
		case doubleQuote:
			builder.WriteString(escapeDoubleQuotes)
		default:
			builder.WriteString(thisString)
		}
	}
	return builder.String()
}
