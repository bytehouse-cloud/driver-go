package format

import (
	"io"
	"unicode/utf8"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data/column"
	"github.com/bytehouse-cloud/driver-go/stream/format/helper"

	"golang.org/x/text/width"
)

const (
	vertBar                   = "│"
	vertBarWithNewLine        = "│\n"
	topLeftCorner             = "┌─"
	bottomLeftCorner          = "└─"
	topRightCornerWithNewLine = "─┐\n"
	bottomRightCorner         = "─┘\n"
	dash                      = "─"
	bottomSeparator           = "─┴─"
	topSeparator              = "─┬─"
)

type PrettyBlockStreamFmtWriter struct {
	zWriter       *bytepool.ZWriter
	maxColumnLens []int

	totalRowsWrite int
	exception      error
	done           chan struct{}
}

func NewPrettyBlockStreamFmtWriter(w io.Writer) *PrettyBlockStreamFmtWriter {
	return &PrettyBlockStreamFmtWriter{
		zWriter: bytepool.NewZWriterDefault(w),
	}
}

func (p *PrettyBlockStreamFmtWriter) BlockStreamFmtWrite(blockStream <-chan *data.Block) {
	p.done = make(chan struct{})
	go func() {
		defer close(p.done)
		p.totalRowsWrite, p.exception = helper.WriteTableFromBlockStream(blockStream, p)
	}()
}

func (p *PrettyBlockStreamFmtWriter) Yield() (int, error) {
	for range p.done {
	}
	return p.totalRowsWrite, p.exception
}

func (p *PrettyBlockStreamFmtWriter) WriteFirstFrame(frame [][]string, cols []*column.CHColumn) (int, error) {
	return p.WriteFrameCont(frame, cols)
}

func (p *PrettyBlockStreamFmtWriter) WriteFrameCont(frame [][]string, cols []*column.CHColumn) (int, error) {
	p.maxColumnLens = countMaxLenForEachCol(cols, frame, p.maxColumnLens)
	if err := writeBlockHeader(p.zWriter, getColNames(cols), p.maxColumnLens); err != nil {
		return 0, err
	}
	n, err := helper.WriteFrameCont(frame, cols, p)
	if err != nil {
		return n, err
	}
	return n, writeBlockFooter(p.zWriter, p.maxColumnLens)
}

func (p *PrettyBlockStreamFmtWriter) Flush() error {
	return p.zWriter.Flush()
}

func (p *PrettyBlockStreamFmtWriter) WriteFirstRow(record []string, cols []*column.CHColumn) error {
	return p.WriteRowCont(record, cols)
}

func (p *PrettyBlockStreamFmtWriter) WriteRowCont(record []string, cols []*column.CHColumn) (err error) {
	if err = p.zWriter.WriteString(vertBar); err != nil {
		return err
	}
	defer func() {
		err = p.zWriter.WriteString(vertBarWithNewLine)
	}()

	if len(record) == 0 {
		return
	}
	if err = writePrettyField(p.zWriter, p.zWriter, record[0], p.maxColumnLens[0], " "); err != nil {
		return err
	}
	for i := 1; i < len(record); i++ {
		if err = p.zWriter.WriteString(vertBar); err != nil {
			return err
		}
		if err = writePrettyField(p.zWriter, p.zWriter, record[i], p.maxColumnLens[i], " "); err != nil {
			return err
		}
	}

	return
}

func writePrettyField(br io.ByteWriter, w stringWriter, field string, colLen int, excess string) error {
	var err error
	if err = br.WriteByte(' '); err != nil {
		return err
	}
	if err = writePrettyWithOffset(w, field, colLen, excess); err != nil {
		return err
	}
	return br.WriteByte(' ')
}

func countMaxLenForEachCol(cols []*column.CHColumn, frame [][]string, result []int) []int {
	result = result[:0]

	// get all len of col names first
	for _, col := range cols {
		result = append(result, spaceCount(col.Name))
	}

	// check cols lens of each row for each col, assigning to max if greater than current value
	var charCount int
	for _, rowData := range frame {
		for j, field := range rowData {
			charCount = utf8.RuneCountInString(field)
			if charCount > result[j] {
				result[j] = charCount
			}
		}
	}

	return result
}

func getColNames(cols []*column.CHColumn) []string {
	result := make([]string, len(cols))
	for i, col := range cols {
		result[i] = col.Name
	}
	return result
}

func writeBlockHeader(w stringWriter, colNames []string, colLens []int) error {
	if len(colNames) == 0 {
		return nil
	}

	var err error
	if err = w.WriteString(topLeftCorner); err != nil {
		return err
	}

	if err = writePrettyWithOffset(w, colNames[0], colLens[0], dash); err != nil {
		return err
	}
	for i := 1; i < len(colNames); i++ {
		if err = w.WriteString(topSeparator); err != nil {
			return err
		}
		if err = writePrettyWithOffset(w, colNames[i], colLens[i], dash); err != nil {
			return err
		}
	}
	return w.WriteString(topRightCornerWithNewLine)
}

func writePrettyWithOffset(w stringWriter, tgt string, colLen int, excess string) error {
	var err error
	if err = w.WriteString(tgt); err != nil {
		return err
	}
	diff := colLen - spaceCount(tgt)

	for j := 0; j < diff; j++ {
		if err = w.WriteString(excess); err != nil {
			return err
		}
	}
	return nil
}

func writeBlockFooter(w stringWriter, lens []int) error {
	if len(lens) == 0 {
		return nil
	}
	var err error

	if err = w.WriteString(bottomLeftCorner); err != nil {
		return nil
	}

	for i := 0; i < lens[0]; i++ {
		if err = w.WriteString(dash); err != nil {
			return err
		}
	}
	for i := 1; i < len(lens); i++ {
		if err = w.WriteString(bottomSeparator); err != nil {
			return err
		}
		for j := 0; j < lens[i]; j++ {
			if err = w.WriteString(dash); err != nil {
				return err
			}
		}
	}
	return w.WriteString(bottomRightCorner)
}

type stringWriter interface {
	WriteString(string string) error
}

// spaceCount counts the spacing needed to print the characters into terminal.
func spaceCount(s string) int {
	var result int
	scratch := make([]byte, 4)
	for _, r := range s {
		n := utf8.EncodeRune(scratch, r)
		if isWide(scratch[:n]) {
			result += 2
			continue
		}
		result += 1
	}
	return result
}

// isWide checks if the character takes up 2 spacing instead of 1
// when printed in terminal.
// Chinese Characters like '好' takes up 2 character space as oppose to 'å' which
// only takes up 1 character space.
func isWide(b []byte) bool {
	p, _ := width.Lookup(b)
	switch p.Kind() {
	case width.EastAsianWide,
		width.EastAsianFullwidth:
		return true
	default:
		return false
	}
}
