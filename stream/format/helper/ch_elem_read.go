package helper

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/data/column"
)

const backSlash = '\\'

const (
	errReadArrayMatchCloseSquareBracket = "fail to find match close square brackets"
	errReadMapMatchCloseCurlyBracket    = "fail to find match close curly brackets"
)

func ReadCHElemTillStop(w Writer, z *bytepool.ZReader, col column.CHColumnData, stop byte) error {
	switch data := col.(type) {
	case *column.StringColumnData, *column.FixedStringColumnData:
		return readString(w, z, stop)
	case *column.ArrayColumnData:
		return readArray(w, z, stop)
	case *column.MapColumnData:
		return readMap(w, z, stop)
	case *column.NullableColumnData:
		return ReadCHElemTillStop(w, z, data.GetInnerColumnData(), stop)
	default:
		return readRawTillStop(w, z, stop)
	}
}

// readRawTillStop reads until(excluding) stop byte or EOF
// writes content into w, excluding stop byte
func readRawTillStop(w Writer, z *bytepool.ZReader, stop byte) error {
	_, err := ReadStringUntilByte(w, z, stop)
	if err != nil {
		if err == io.EOF {
			return nil
		}
		return err
	}

	z.UnreadCurrentBuffer(1)
	return nil
}

func readString(w Writer, z *bytepool.ZReader, stop byte) error {
	b, err := ReadNextNonSpaceByte(z)
	if err != nil {
		return err
	}
	switch b {
	case backTick, doubleQuote, singleQuote:
		return readStringUntilByteEscaped(w, z, b)
	default:
		z.UnreadCurrentBuffer(1) // next byte not a quote
	}

	// begin reading unquoted string
	if _, err := ReadStringUntilByte(w, z, stop); err != nil {
		return err
	}

	z.UnreadCurrentBuffer(1)
	return nil
}

func readArray(w Writer, z *bytepool.ZReader, stop byte) error {
	b, err := ReadNextNonSpaceByte(z)
	if b != squareOpenBrace {
		z.UnreadCurrentBuffer(1) // if not open square bracket then current value should be empty string "" or null
		if err != nil {
			return err
		}
		return readRawTillStop(w, z, stop)
	}

	w.WriteByte(squareOpenBrace)
	squareBraceCount := 1
	for {
		b, err = z.ReadByte()
		if err != nil {
			if err == io.EOF {
				err = errors.New(errReadArrayMatchCloseSquareBracket)
			}

			return err
		}

		switch b {
		case backTick, doubleQuote, singleQuote:
			w.WriteByte(b)
			err := readStringUntilByteEscaped(w, z, b)
			if err != nil {
				return err
			}
		case curlyOpenBrace:
			w.WriteByte(b)
			b = curlyCloseBrace
			err := readStringUntilByteEscaped(w, z, b)
			if err != nil {
				return err
			}
		case squareOpenBrace:
			squareBraceCount++
		case squareCloseBrace:
			squareBraceCount--
		}
		w.WriteByte(b)

		if squareBraceCount == 0 {
			break
		}
	}
	return nil
}

func readMap(w Writer, z *bytepool.ZReader, stop byte) error {
	b, err := ReadNextNonSpaceByte(z)
	if b != curlyOpenBrace {
		z.UnreadCurrentBuffer(1) // if not curly square bracket then current value should be empty string "" or null
		if err != nil {
			return err
		}
		return readRawTillStop(w, z, stop)
	}

	w.WriteByte(curlyOpenBrace)
	curlyBraceCount := 1
	for {
		b, err = z.ReadByte()
		if err != nil {
			if err == io.EOF {
				err = errors.New(errReadMapMatchCloseCurlyBracket)
			}

			return err
		}

		switch b {
		case backTick, doubleQuote, singleQuote:
			w.WriteByte(b)
			err := readStringUntilByteEscaped(w, z, b)
			if err != nil {
				return err
			}
		case curlyOpenBrace:
			curlyBraceCount++
		case curlyCloseBrace:
			curlyBraceCount--
		}
		w.WriteByte(b)

		if curlyBraceCount == 0 {
			break
		}
	}
	return nil
}

// readStringUntilByteEscaped is the same as ReadStringUntilByte, however backslash character is handled as escaped
func readStringUntilByteEscaped(w Writer, z *bytepool.ZReader, b byte) error {
	var yieldFromBuilder func(w Writer) error
	yieldFromBuilder = func(w Writer) error {
		buf, err := z.ReadNextBuffer()
		if err != nil {
			return err
		}

		pos, isBackSlash := findBytePositionOrBackSlash(buf, b)
		if pos < 0 {
			w.Write(buf)
			return yieldFromBuilder(w)
		}

		z.UnreadCurrentBuffer(len(buf) - pos - 1) // pause reading for processing of stop characters

		if isBackSlash {
			// this will store the buffer until last backslash
			bufUntilLastBackSlash := buf[:pos]

			escapedBuf := make([]byte, 3)
			n, ok := readBytesAfterEscape(z, escapedBuf, b)
			resultBuf := escapedBuf[:n]
			if !ok {
				if j := bytes.IndexByte(escapedBuf, b); j > 0 { // rare edge case where stop byte appears in escape buf, eg. \x',
					w.Write(resultBuf[:j])
					defer z.PrependCurrentBuffer(resultBuf[j:])
					return nil
				}
			}

			// lets not forget to append the content until last backslash
			w.Write(bufUntilLastBackSlash)

			// write the escape byte
			w.Write(escapedBuf[:n])
			return yieldFromBuilder(w) //  read again after handling escape
		}

		w.Write(buf[:pos])
		return nil
	}

	return yieldFromBuilder(w)
}

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

// ReadStringUntilByte reads content of buffer until including given byte appear or EOF
// Write the bytes into Writer
// Returns number of bytes (excluding quote) written and error
func ReadStringUntilByte(w Writer, z *bytepool.ZReader, b byte) (int, error) {
	var totalRead int

	buf, err := z.ReadNextBuffer()
	if err != nil {
		return 0, err
	}

	var i int
	for i = bytes.IndexByte(buf, b); i == -1; i = bytes.IndexByte(buf, b) {
		n, _ := w.Write(buf)
		totalRead += n
		if buf, err = z.ReadNextBuffer(); err != nil {
			return totalRead, err
		}
	}
	n, _ := w.Write(buf[:i])
	totalRead += n

	z.UnreadCurrentBuffer(len(buf) - i - 1)
	return totalRead, nil
}

// Unused function
// func readNextNonSpaceByteOrNewline(z *bytepool.ZReader) (byte, error) {
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
// }

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

func ReadNextNonSpaceExceptNewLineByte(z *bytepool.ZReader) (byte, error) {
	buf, err := z.ReadNextBuffer()
	if err != nil {
		return 0, err
	}

	var i int
	for i = findNonSpaceExceptNewLine(buf); i == len(buf); i = findNonSpaceExceptNewLine(buf) {
		if buf, err = z.ReadNextBuffer(); err != nil {
			return 0, err
		}
	}

	z.UnreadCurrentBuffer(len(buf) - i - 1)
	return buf[i], nil
}

// findNonSpaceExceptNewLine attempts to find the index of the first non space byte that occurs the columnTextsPool.
// returns len(buf) buf only contains space.
// definition of space byte is any of the following:  `\t` `\v` `\f` `\zReader` ` `
func findNonSpaceExceptNewLine(buf []byte) int {
	var i int
	for i = 0; i < len(buf); i++ {
		switch buf[i] {
		case '\t', '\f', '\r', '\v', ' ':
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

// AssertNextByteEqual reads the next non space byte from z
// throws error if byte is not same as expect
func AssertNextByteEqual(z *bytepool.ZReader, expect byte) error {
	next, err := ReadNextNonSpaceByte(z)
	if err != nil {
		return err
	}
	if next != expect {
		return fmt.Errorf("expect byte: %q, but got: %q", expect, next)
	}
	return nil
}

func FlushZReader(z *bytepool.ZReader) {
	var err error
	for err == nil {
		_, err = z.ReadNextBuffer()
	}
}
