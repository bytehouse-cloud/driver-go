package response

import (
	"strconv"
	"strings"

	"github.com/jfcg/sixb"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

type ExceptionPacket struct {
	Code       uint32
	Name       string
	Message    string
	StackTrace string
	Nested     *ExceptionPacket
}

func (s *ExceptionPacket) Close() error {
	return nil
}

func (s *ExceptionPacket) String() string {
	return s.Error()
}

func readExceptionPacket(decoder *ch_encoding.Decoder) (*ExceptionPacket, error) {
	var (
		exception ExceptionPacket
		err       error
	)
	if exception.Code, err = decoder.UInt32(); err != nil {
		return nil, err
	}
	if exception.Name, err = decoder.String(); err != nil {
		return nil, err
	}
	if exception.Message, err = decoder.String(); err != nil {
		return nil, err
	}
	if exception.StackTrace, err = decoder.String(); err != nil {
		return nil, err
	}
	{
		var hasNested bool
		if hasNested, err = decoder.Bool(); err != nil {
			return nil, err
		}
		if hasNested {
			exception.Nested, err = readExceptionPacket(decoder)
			if err != nil {
				return nil, err
			}
		}
	}
	return &exception, nil
}

func writeExceptionPacket(exception *ExceptionPacket, encoder *ch_encoding.Encoder) (err error) {
	if err = encoder.UInt32(exception.Code); err != nil {
		return err
	}
	if err = encoder.String(exception.Name); err != nil {
		return err
	}
	if err = encoder.String(exception.Message); err != nil {
		return err
	}
	if err = encoder.String(exception.StackTrace); err != nil {
		return err
	}
	{
		hasNested := exception.Nested != nil
		if err = encoder.Bool(hasNested); err != nil {
			return err
		}
		if hasNested {
			if err = writeExceptionPacket(exception.Nested, encoder); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *ExceptionPacket) packet() {}

func (s *ExceptionPacket) Error() string {
	var b strings.Builder
	formatServerException(&b, s, 0)
	return b.String()
}

func formatServerException(b *strings.Builder, s *ExceptionPacket, indentLevel int) {
	b.WriteByte(squareOpenBracket)
	b.WriteString(code)
	b.WriteString(strconv.FormatUint(uint64(s.Code), 10))
	b.WriteString(commaSep)
	b.WriteString(name)
	b.WriteString(s.Name)
	b.WriteString(commaSep)
	b.WriteString(s.Message)
	b.WriteString(commaSep)
	b.WriteString(stackTrace)
	writeIndentedStackTrace(b, s.StackTrace, indentLevel)
	if s.Nested != nil {
		newlineWithIndent(b, indentLevel)
		b.WriteString(nested)
		indentLevel++
		newlineWithIndent(b, indentLevel)
		formatServerException(b, s.Nested, indentLevel)
		indentLevel--
		newlineWithIndent(b, indentLevel)
	}
	b.WriteByte(squareCloseBracket)
}

func writeIndentedStackTrace(b *strings.Builder, st string, indentLevel int) {
	replacement := make([]byte, indentLevel+1)
	var i int
	replacement[0] = newline
	for i = 1; i < len(replacement); i++ {
		replacement[i] = tab
	}
	replacementString := sixb.BtoS(replacement)
	st = strings.ReplaceAll(st, string(newline), replacementString)
	b.WriteString(st)
}

func newlineWithIndent(b *strings.Builder, indentLevel int) {
	b.WriteByte(newline)
	for i := 0; i < indentLevel; i++ {
		b.WriteByte(tab)
	}
}

const (
	code       = "code: "
	name       = "name: "
	stackTrace = "stack trace: "
	nested     = "nested: "

	tab                = '\t'
	squareOpenBracket  = '['
	squareCloseBracket = ']'
	newline            = '\n'

	commaSep = ", "
	mapSep   = ": "
)
