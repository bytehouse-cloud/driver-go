package helper

import (
	"bytes"
	"encoding/hex"
	"strings"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data/column"
)

const backSlash = '\\'

func ReadCHElem(z *bytepool.ZReader, col *column.CHColumn, stop byte) (string, error) {
	switch col.Data.(type) {
	case *column.StringColumnData, *column.FixedStringColumnData:
		return readString(z, stop)
	case *column.ArrayColumnData:
		return readArray(z)
	default:
		defer z.UnreadCurrentBuffer(1)
		return ReadStringUntilByte(z, stop)
	}
}

func readString(z *bytepool.ZReader, stop byte) (string, error) {
	b, err := ReadNextNonSpaceByte(z)
	if err != nil {
		return emptyString, err
	}
	switch b {
	case backTick, doubleQuote, singleQuote:
		return readStringUntilByteEscaped(z, b)
	}

	z.UnreadCurrentBuffer(1)
	defer z.UnreadCurrentBuffer(1)
	return ReadStringUntilByte(z, stop)
}

func readArray(z *bytepool.ZReader) (string, error) {
	b, err := ReadNextNonSpaceByte(z)
	if b != squareOpenBrace {
		z.UnreadCurrentBuffer(1) // if not open square bracket then current value should be empty string ""
		return emptyString, err
	}

	var sb strings.Builder
	sb.WriteByte(squareOpenBrace)
	squareBraceCount := 1
	for {
		b, err = z.ReadByte()
		switch b {
		case backTick, doubleQuote, singleQuote:
			sb.WriteByte(b)
			s, err := readStringUntilByteEscaped(z, b)
			if err != nil {
				return emptyString, err
			}
			sb.WriteString(s)
		case curlyOpenBrace:
			sb.WriteByte(b)
			b = curlyCloseBrace
			s, err := readStringUntilByteEscaped(z, b)
			if err != nil {
				return emptyString, err
			}
			sb.WriteString(s)
		case squareOpenBrace:
			squareBraceCount++
		case squareCloseBrace:
			squareBraceCount--
		}
		sb.WriteByte(b)

		if squareBraceCount == 0 {
			break
		}
	}
	return sb.String(), nil
}

// readStringUntilByteEscaped is the same as ReadStringUntilByte, however backslash character is handled as escaped
func readStringUntilByteEscaped(z *bytepool.ZReader, b byte) (string, error) {
	var yieldFromBuilder func(builder *strings.Builder) (string, error)
	yieldFromBuilder = func(builder *strings.Builder) (string, error) {
		buf, err := z.ReadNextBuffer()
		if err != nil {
			return emptyString, err
		}

		pos, isBackSlash := findBytePositionOrBackSlash(buf, b)
		if pos < 0 {
			builder.Write(buf)
			return yieldFromBuilder(builder)
		}

		z.UnreadCurrentBuffer(len(buf) - pos - 1) // pause reading for processing of stop characters

		if isBackSlash {
			escapedBuf := make([]byte, 3)
			n, ok := readBytesAfterEscape(z, escapedBuf, b)
			resultBuf := escapedBuf[:n]
			if !ok {
				if j := bytes.IndexByte(escapedBuf, b); j > 0 { // rare edge case where stop byte appears in escape buf, eg. \x',
					builder.Write(resultBuf[:j])
					defer z.PrependCurrentBuffer(resultBuf[j:])
					return builder.String(), nil
				}
			}
			builder.Write(escapedBuf[:n])
			return yieldFromBuilder(builder) //  read again after handling escape
		}

		builder.Write(buf[:pos])
		return builder.String(), nil
	}

	return yieldFromBuilder(&strings.Builder{})
}

//// findNonSpaceOrNewLine is similar to findNonSpace but does not include new line character as space
//func findNonSpaceOrNewLine(buf []byte) int {
//	var i int
//	for i = 0; i < len(buf); i++ {
//		switch buf[i] {
//		case '\t', '\f', '\r', '\v', ' ':
//			continue
//		}
//
//		break
//	}
//	return i
//}

// attempts to read escaped character after backslash.
// return number of bytes read and if operation is successful
func readBytesAfterEscape(z *bytepool.ZReader, buf []byte, stop byte) (int, bool) {
	b, err := z.ReadByte()
	if err != nil {
		return 0, false
	}

	switch b {
	case 'b':
		buf[0] = '\b'
	case 'n':
		buf[0] = '\n'
	case 'r':
		buf[0] = '\r'
	case 't':
		buf[0] = '\t'
	case 'v':
		buf[0] = '\v'
	case 'a':
		buf[0] = '\a'
	case '0':
		buf[0] = '\000'
	case 'x':
		n, ok := readHexEscaped(z, buf)
		if !ok {
			copy(buf[1:], buf)
			buf[0] = 'x'
			return n + 1, false
		}
		return 1, true
	case stop:
		buf[0] = b
	default:
		buf[0] = b
		return 1, false
	}
	return 1, true
}

func findBytePositionOrBackSlash(src []byte, tgt byte) (int, bool) {
	for i, b := range src {
		switch b {
		case tgt:
			return i, false
		case backSlash:
			return i, true
		}
	}
	return -1, false
}

func readHexEscaped(z *bytepool.ZReader, buf []byte) (int, bool) {
	hexBuf := make([]byte, 2)

	var totalRead int
	for totalRead < len(hexBuf) {
		n, err := z.Read(buf[totalRead:])
		totalRead += n
		if err != nil {
			return copy(buf, hexBuf[:totalRead]), false
		}
	}

	n, err := hex.Decode(buf, hexBuf)
	if err != nil {
		return copy(buf, hexBuf[:totalRead]), false
	}
	return n, true
}

// ReadStringUntilByte reads content of buffer until until given byte appear.
// Returns string read (not including the byte), and error if any
func ReadStringUntilByte(z *bytepool.ZReader, b byte) (string, error) {
	buf, err := z.ReadNextBuffer()
	if err != nil {
		return emptyString, err
	}

	var (
		builder strings.Builder
		i       int
	)
	for i = bytes.IndexByte(buf, b); i == -1; i = bytes.IndexByte(buf, b) {
		builder.Write(buf)
		if buf, err = z.ReadNextBuffer(); err != nil {
			return builder.String(), err
		}
	}
	builder.Write(buf[:i])

	z.UnreadCurrentBuffer(len(buf) - i - 1)
	return builder.String(), nil
}

// Unused function
//func readNextNonSpaceByteOrNewline(z *bytepool.ZReader) (byte, error) {
//	buf, err := z.ReadNextBuffer()
//	if err != nil {
//		return 0, err
//	}
//
//	var i int
//	for i = findNonSpaceOrNewLine(buf); i == len(buf); i = findNonSpaceOrNewLine(buf) {
//		if buf, err = z.ReadNextBuffer(); err != nil {
//			return 0, err
//		}
//	}
//
//	z.UnreadCurrentBuffer(len(buf) - i - 1)
//	return buf[i], nil
//}

func ReadNextNonSpaceByte(z *bytepool.ZReader) (byte, error) {
	buf, err := z.ReadNextBuffer()
	if err != nil {
		return 0, err
	}

	var i int
	for i = findNonSpace(buf); i == len(buf); i = findNonSpace(buf) {
		if buf, err = z.ReadNextBuffer(); err != nil {
			return 0, err
		}
	}

	z.UnreadCurrentBuffer(len(buf) - i - 1)
	return buf[i], nil
}

// findNonSpace attempts to find the index of the first non space byte that occurs the columnTextsPool.
// returns len(buf) buf only contains space.
// definition of space byte is any of the following:  `\t` `\v` `\f` `\n` `\zReader` ` `
func findNonSpace(buf []byte) int {
	var i int
	for i = 0; i < len(buf); i++ {
		switch buf[i] {
		case '\n', '\t', '\f', '\r', '\v', ' ':
			continue
		}

		break
	}
	return i
}

func DiscardUntilByteEscaped(z *bytepool.ZReader, stop byte) error {
	buf, err := z.ReadNextBuffer()
	if err != nil {
		return err
	}

	var i int
	for i < len(buf) {
		switch buf[i] {
		case stop:
			z.UnreadCurrentBuffer(len(buf) - i - 1)
			return nil
		case '\\':
			i++
		}
		i++
	}

	return DiscardUntilByteEscaped(z, stop)
}
