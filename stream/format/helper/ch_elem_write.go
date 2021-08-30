package helper

import (
	"io"
	"strings"

	"github.com/jfcg/sixb"

	"github.com/bytehouse-cloud/driver-go/driver/lib/data/column"
)

func WriteCHElemString(w io.Writer, s string, col *column.CHColumn) error {
	switch col.Data.(type) {
	case *column.StringColumnData, *column.FixedStringColumnData:
		return writeStringWithDoubleQuoteEscaped(w, s)
	case *column.DateColumnData, *column.DateTimeColumnData, *column.DateTime64ColumnData:
		return writeStringWithDoubleQuote(w, s)
	default:
		_, err := w.Write(sixb.StB(s))
		return err
	}
}

func writeStringWithDoubleQuoteEscaped(w io.Writer, s string) error {
	if _, err := w.Write(doubleQuoteBytes); err != nil {
		return err
	}

	for i := strings.IndexByte(s, doubleQuote); i >= 0; i = strings.IndexByte(s, doubleQuote) {
		if _, err := w.Write(sixb.StB(s[:i])); err != nil {
			return err
		}
		if _, err := w.Write(doubleQuoteEscapedBytes); err != nil {
			return err
		}
		s = s[i+1:]
	}
	if _, err := w.Write(sixb.StB(s)); err != nil {
		return err
	}

	if _, err := w.Write(doubleQuoteBytes); err != nil {
		return err
	}
	return nil
}

func writeStringWithDoubleQuote(w io.Writer, s string) error {
	if _, err := w.Write(doubleQuoteBytes); err != nil {
		return err
	}
	if _, err := w.Write(sixb.StB(s)); err != nil {
		return err
	}
	if _, err := w.Write(doubleQuoteBytes); err != nil {
		return err
	}
	return nil
}
