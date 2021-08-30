package column

import (
	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

type CHColumnData interface {
	// ReadFromDecoder fills the CHColumnData with input from decoder
	// Used when we receive data from clickhouse server
	ReadFromDecoder(decoder *ch_encoding.Decoder) error

	// WriteToEncoder write the data from CHColumnData to encoder
	// Used when we send data from clickhouse server
	WriteToEncoder(encoder *ch_encoding.Encoder) error

	// GetValue returns the value of CHColumnData at given row
	GetValue(row int) interface{}

	// GetString returns the string representation of value of CHColumnData at given row
	GetString(row int) string

	//Zero returns zero value of the CHColumnData
	Zero() interface{}

	//ZeroString return string representation of Zero of CHColumnData
	ZeroString() string

	// Len returns the number of rows of CHColumnData
	Len() int

	// Close recycles the CHColumnData, rendering it unusable
	Close() error

	// ReadFromTexts reads from slice of string into column. len of slice
	// must not exceed the len of column.
	// return total rows written and error if any
	ReadFromTexts(texts []string) (int, error)

	// ReadFromValues reads from slice of golang values into column. len of slice
	// must not exceed the len of column.
	// return total rows written and error if any
	ReadFromValues(values []interface{}) (int, error)
}
