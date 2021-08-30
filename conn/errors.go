package conn

import (
	"fmt"
	"reflect"

	"github.com/bytehouse-cloud/driver-go/errors"
)

// ErrBadConnection is a connection err
// Returning this err will cause upstream sql driver to return ErrBadConn, causing the connection to be discarded.
type ErrBadConnection struct {
	value string
}

func NewErrBadConnection(value string) *ErrBadConnection {
	return &ErrBadConnection{value: value}
}

func (e ErrBadConnection) Error() string {
	return fmt.Sprintf("%s: ErrBadConnection: %s", errors.DriverGoErrorPrefix, e.value)
}

// Is returns true if target is of ErrBadConnection
func (e ErrBadConnection) Is(target error) bool {
	targetType := reflect.TypeOf(target)
	if targetType.Kind() == reflect.Ptr {
		return reflect.TypeOf(e) == targetType.Elem()
	}

	return reflect.TypeOf(e) == reflect.TypeOf(target)
}
