package data

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

const (
	// Deprecated Box Drawing Characters
	//topLeftCorner             = "╭─"
	//bottomLeftCorner          = "╰─"
	//dash                      = "─"
	//bottomRightCorner         = "─╯"
	//bottomSeparator           = "─┴─"
	//topRightCornerWithNewLine = "─╮\n"
	//leftSeparator   = "├─"
	//rightSeparator  = "─┤\n"
	//centerSeparator = "─┼─"

	newLine                   = '\n'
	vertBar                   = "│"
	vertBarWithNewLine        = "│\n"
	topLeftCorner             = "┌─"
	bottomLeftCorner          = "└─"
	topRightCornerWithNewLine = "─┐\n"
	bottomRightCorner         = "─┘"
	dash                      = "─"
	bottomSeparator           = "─┴─"
	topSeparator              = "─┬─"

	bold   = "\u001B[1m"
	reset  = "\u001B[0m"
	bgGrey = "\u001B[100m"

	limitMessage = "... %v more rows\n"
)

type blocksPrinter struct {
	balanceRow               int
	hasPrintedBalanceReached bool
}

// NewBlocksPrinter creates a new blockPrinter instance which used to print blocks
// balanceRow is the number of rows you want to print for the query
// If no more balanceRow, block data won't be printed
// Example usage:
// var sb strings.Builder
// printer := NewBlocksPrinter(100)
// printer.Print(block1, &sb)
// printer.Print(block2, &sb)
// fmt.Println(sb.String())
func NewBlocksPrinter(balanceRow int) *blocksPrinter {
	return &blocksPrinter{balanceRow: balanceRow}
}

// Print prints blocks data into the string builder passed into this function
// balanceRow will decrease by the number of rows in the block
// If balanceRow = 0, a notice that there are a few more rows not printed will be shown (e.g. 3 more rows)
func (p *blocksPrinter) Print(b *Block, builder *strings.Builder) {
	if p.balanceRow <= 0 && p.hasPrintedBalanceReached {
		return
	}

	if p.balanceRow <= 0 {
		builder.WriteString("Extra blocks not printed: No balance row left\n")
		p.hasPrintedBalanceReached = true
		return
	}

	dataFrame, columnNames, maxColumnLens := b.strFmtInfo()
	p.buildDataFrame(builder, columnNames, dataFrame, maxColumnLens)
}

func (p *blocksPrinter) buildDataFrame(builder *strings.Builder, columnNames []string, dataFrame [][]string, columnLens []int) {
	p.buildHeader(builder, columnNames, columnLens)
	excess := len(dataFrame) - p.balanceRow
	p.buildData(builder, dataFrame, columnLens)
	if excess > 0 {
		p.buildCont(builder, excess)
	}
	p.buildBase(builder, columnLens)
}

func (p *blocksPrinter) buildCont(buf *strings.Builder, excess int) {
	buf.WriteString(fmt.Sprintf(limitMessage, excess))
}

func (p *blocksPrinter) buildData(buf *strings.Builder, stringSlice [][]string, size []int) {
	if len(stringSlice) > p.balanceRow {
		stringSlice = stringSlice[:p.balanceRow]
	}

	for i := range stringSlice {
		highlight := i%2 == 1
		for j := range stringSlice[i] {
			buf.WriteString(vertBar)
			if highlight {
				buf.WriteString(bgGrey)
			}
			buf.WriteByte(space)
			buf.WriteString(stringSlice[i][j])
			buf.WriteByte(space)
			diff := size[j] - utf8.RuneCountInString(stringSlice[i][j])
			for k := 0; k < diff; k++ {
				buf.WriteByte(space)
			}
			buf.WriteString(reset)
		}
		buf.WriteString(vertBarWithNewLine)
	}

	p.balanceRow -= len(stringSlice)
}

func (p *blocksPrinter) buildBase(buf *strings.Builder, size []int) {
	buf.WriteString(bottomLeftCorner)
	for i := 0; i < len(size)-1; i++ {
		for j := 0; j < size[i]; j++ {
			buf.WriteString(dash)
		}
		buf.WriteString(bottomSeparator)
	}
	{
		lastIndex := len(size) - 1
		for j := 0; j < size[lastIndex]; j++ {
			buf.WriteString(dash)
		}
	}
	buf.WriteString(bottomRightCorner)
	buf.WriteRune(newLine)
}

func (p *blocksPrinter) buildHeader(buf *strings.Builder, stringSlice []string, size []int) {
	buf.WriteString(topLeftCorner)
	for i := 0; i < len(stringSlice)-1; i++ {
		buf.WriteString(bold)
		buf.WriteString(stringSlice[i])
		buf.WriteString(reset)
		diff := size[i] - len(stringSlice[i])
		for j := 0; j < diff; j++ {
			buf.WriteString(dash)
		}
		buf.WriteString(topSeparator)
	}
	{
		lastIndex := len(stringSlice) - 1
		buf.WriteString(bold)
		buf.WriteString(stringSlice[lastIndex])
		buf.WriteString(reset)
		diff := size[lastIndex] - len(stringSlice[lastIndex])
		for j := 0; j < diff; j++ {
			buf.WriteString(dash)
		}
	}
	buf.WriteString(topRightCornerWithNewLine)
}

//func buildSeparator(buf *strings.Builder, size []int) {
//	buf.WriteString(leftSeparator)
//	for i := 0; i < len(size)-1; i++ {
//		for j := 0; j < size[i]; j++ {
//			buf.WriteString(dash)
//		}
//		buf.WriteString(centerSeparator)
//	}
//	for j := 0; j < size[len(size)-1]; j++ {
//		buf.WriteString(dash)
//	}
//	buf.WriteString(rightSeparator)
//}
