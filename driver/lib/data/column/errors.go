package column

import (
	"fmt"
	"reflect"

	"github.com/bytehouse-cloud/driver-go/errors"
)

// ErrInvalidColumnType is a connection err
// Returning this err will cause upstream sql driver to return ErrBadConn, causing the connection to be discarded.
type ErrInvalidColumnType struct {
	value string
}

func NewErrInvalidColumnType(current, expected interface{}) *ErrInvalidColumnType {
	return &ErrInvalidColumnType{value: fmt.Sprintf("invalid column data type, current = %T, expected = %T", current, expected)}
}

func NewErrInvalidColumnTypeCustomText(text string) *ErrInvalidColumnType {
	return &ErrInvalidColumnType{value: fmt.Sprintf("invalid column data type, %s", text)}
}

func (e ErrInvalidColumnType) Error() string {
	return fmt.Sprintf("%s: ErrInvalidColumnType: %s", errors.DriverGoErrorPrefix, e.value)
}

// Is returns true if target is of ErrInvalidColumnType
func (e ErrInvalidColumnType) Is(target error) bool {
	targetType := reflect.TypeOf(target)
	if targetType.Kind() == reflect.Ptr {
		return reflect.TypeOf(e) == targetType.Elem()
	}

	return reflect.TypeOf(e) == reflect.TypeOf(target)
}
