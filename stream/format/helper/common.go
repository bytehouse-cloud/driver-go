package helper

const (
	squareOpenBrace  = '['
	squareCloseBrace = ']'
	curlyOpenBrace   = '{'
	curlyCloseBrace  = '}'
	singleQuote      = '\''
	doubleQuote      = '"'
	backTick         = '`'
	emptyString      = ""
)

var (
	doubleQuoteEscapedBytes = []byte{'"', '"'}
	doubleQuoteBytes        = []byte{'"'}
)
