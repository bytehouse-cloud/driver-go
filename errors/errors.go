package errors

import (
	"fmt"
	"reflect"
	"runtime"
	"strings"
)

// GetFunctionName returns the name of the function `i`.
// Should be passed a function value.
func GetFunctionName(i interface{}) string {
	longName := runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
	arr := strings.Split(longName, "/")
	return arr[len(arr)-1]
}

// GetCallerFunctionName gets the name of the calling function
// skip is the number of levels up from this function
// e.g. GetCallerFunctionName is called in main.Test() which in called in main.Main
// if skip == 1 -> main.Test is returned
// if skip == 2 -> main.Main is returned
func GetCallerFunctionName(skip int) string {
	pc, _, _, ok := runtime.Caller(skip)
	function := runtime.FuncForPC(pc)
	if ok && function != nil {
		longName := function.Name()
		arr := strings.Split(longName, "/")
		return arr[len(arr)-1]
	}

	return "Unknown Function"
}

const DriverGoErrorPrefix = "driver-go"

// Errorf returns an error with driver-go's error prefix
func Errorf(format string, a ...interface{}) error {
	return fmt.Errorf("%s: %s", DriverGoErrorPrefix, fmt.Sprintf(format, a...))
}

// ErrorfWithCaller returns a formatted error with the caller function as prefix
// e.g. driver-go(callerFunctionName): format
// To prevent the error from chaining and becoming too long:
// If format starts with "driver-go: ", remove "driver-go: " prefix from format
// If format starts with "driver-go(someFuncName):", remove "driver-go" prefix from format
func ErrorfWithCaller(format string, a ...interface{}) error {
	plainDriverGoPrefix := fmt.Sprintf("%s: ", DriverGoErrorPrefix)
	if len(format) >= len(plainDriverGoPrefix) && format[:len(plainDriverGoPrefix)] == plainDriverGoPrefix {
		format = format[len(plainDriverGoPrefix):]
	} else if len(format) >= len(DriverGoErrorPrefix) && format[:len(DriverGoErrorPrefix)] == DriverGoErrorPrefix {
		format = format[len(DriverGoErrorPrefix):]
	}

	return fmt.Errorf("%s(%s): %s", DriverGoErrorPrefix, GetCallerFunctionName(2), fmt.Sprintf(format, a...))
}
