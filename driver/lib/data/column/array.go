package column

import (
	"encoding/binary"
	"reflect"
	"strings"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
	"github.com/bytehouse-cloud/driver-go/errors"
)

const (
	invalidArrayFmt = "invalid array string: %v"
	emptyArray      = "[]"
)

// ArrayColumnData's byte encoding
// A column of arrays is converted into a single long array in ReadFromTexts/ReadFromValues
// The length of each array are stored in offsetsRaw
// These values are known as "offset"
// To get back the values for each array, used the offsets to get the right slice from the single long array
type ArrayColumnData struct {
	generateInnerData func(numRows int) CHColumnData // because we don't know how many rows there will be until we read the last offset
	offsetsRaw        []byte
	innerColumnData   CHColumnData
}

func (a *ArrayColumnData) ReadFromDecoder(decoder *ch_encoding.Decoder) error {
	n, err := decoder.Read(a.offsetsRaw)
	if err != nil {
		return err
	}

	var lastOffset uint64
	if n > 0 {
		lastOffset = binary.LittleEndian.Uint64(a.offsetsRaw[n-uint64ByteSize:])
	}

	a.innerColumnData = a.generateInnerData(int(lastOffset))
	return a.innerColumnData.ReadFromDecoder(decoder)
}

func (a *ArrayColumnData) WriteToEncoder(encoder *ch_encoding.Encoder) error {
	if _, err := encoder.Write(a.offsetsRaw); err != nil {
		return err
	}
	return a.innerColumnData.WriteToEncoder(encoder)
}

func (a *ArrayColumnData) ReadFromValues(values []interface{}) (v int, err error) {
	if len(values) == 0 {
		return 0, nil
	}

	var (
		row    []interface{}
		offset uint64 // offset is also the length of the flattened array
		ok     bool
	)

	interpret, err := interpretArrayType(values)
	if err != nil {
		return 0, err
	}

	for idx, value := range values {
		if value == nil {
			binary.LittleEndian.PutUint64(a.offsetsRaw[idx*uint64ByteSize:], offset)
			continue
		}

		// Convert value into slice
		row, ok = interpret(value)
		if !ok {
			return idx, NewErrInvalidColumnType(value, row)
		}

		offset += uint64(len(row))
		binary.LittleEndian.PutUint64(a.offsetsRaw[idx*uint64ByteSize:], offset)
	}

	flattened := make([]interface{}, 0, offset)
	for _, value := range values {
		if value == nil {
			continue
		}

		row, _ = interpret(value) // no need check for ok again since check before in previous loop
		flattened = append(flattened, row...)
	}

	a.innerColumnData = a.generateInnerData(int(offset))
	if n, err := a.innerColumnData.ReadFromValues(flattened); err != nil {
		lower, _ := a.findRowIdx(n, 0, len(values))
		return lower, err
	}

	return len(values), nil
}

func (a *ArrayColumnData) ReadFromTexts(texts []string) (int, error) {
	var (
		err    error
		row    []string
		offset uint64
	)

	for i, text := range texts {
		if isEmptyOrNull(text) {
			binary.LittleEndian.PutUint64(a.offsetsRaw[i*uint64ByteSize:], offset)
			continue
		}

		if text, err = removeSquareBraces(text); err != nil {
			return 0, err
		}

		// Don't update offset if array is empty
		if strings.TrimSpace(text) == "" {
			binary.LittleEndian.PutUint64(a.offsetsRaw[i*uint64ByteSize:], offset)
			continue
		}

		row = splitIgnoreBraces(text, comma, row)
		offset += uint64(len(row))
		binary.LittleEndian.PutUint64(a.offsetsRaw[i*uint64ByteSize:], offset)
	}

	flattened := make([]string, 0, offset)
	for _, text := range texts {
		// Parsing text again b/c parsing text again is less expensive (in terms of time)
		// than allocating new memory to store previously parsed text (Last measured 3-4x faster)
		// For actual time taken, check benchmark in test
		text, _ = removeSquareBraces(text) // no need check for error since alr checked in previous loop
		if strings.TrimSpace(text) == "" {
			continue
		}

		row = splitIgnoreBraces(text, comma, row)
		flattened = append(flattened, row...)
	}

	a.innerColumnData = a.generateInnerData(int(offset))
	if n, err := a.innerColumnData.ReadFromTexts(flattened); err != nil {
		lower, _ := a.findRowIdx(n, 0, len(texts))
		return lower, errors.ErrorfWithCaller("%v", err)
	}
	return len(texts), nil
}

func (a *ArrayColumnData) GetValue(row int) interface{} {
	return getColumnValuesUsingOffset(a.findOffset(row-1), a.findOffset(row), a.innerColumnData)
}

func (a *ArrayColumnData) GetString(row int) string {
	if a.Len() == 0 {
		return emptyArray
	}

	array := getColumnStringsUsingOffset(a.findOffset(row-1), a.findOffset(row), a.innerColumnData)
	innerKind := reflect.ValueOf(a.innerColumnData.Zero()).Type().Kind()

	var builder strings.Builder
	builder.WriteByte(squareOpenBracket)
	if len(array) > 0 {
		builderWriteKind(&builder, array[0], innerKind)
	}
	for i := 1; i < len(array); i++ {
		builder.WriteString(listSeparator)
		builderWriteKind(&builder, array[i], innerKind)
	}
	builder.WriteByte(squareCloseBracket)
	return builder.String()
}

func (a *ArrayColumnData) Zero() interface{} {
	innerType := reflect.ValueOf(a.generateInnerData(0).Zero()).Type()
	sliceType := reflect.SliceOf(innerType)
	emptySlice := reflect.MakeSlice(sliceType, 0, 0)
	return emptySlice.Interface()
}

func (a *ArrayColumnData) ZeroString() string {
	return emptyArray
}

func (a *ArrayColumnData) Len() int {
	return len(a.offsetsRaw) / uint64ByteSize
}

func (a *ArrayColumnData) Close() error {
	return a.innerColumnData.Close()
}

// findOffset returns the offset of a row index
func (a *ArrayColumnData) findOffset(rowIdx int) int {
	if rowIdx == -1 {
		return 0
	}
	return int(binary.LittleEndian.Uint64(a.offsetsRaw[rowIdx*uint64ByteSize:]))
}

func removeSquareBraces(s string) (string, error) {
	sLen := len(s)
	if sLen < 2 || s[0] != squareOpenBracket || s[sLen-1] != squareCloseBracket {
		return emptyString, errors.ErrorfWithCaller(invalidArrayFmt, s)
	}
	return s[1 : sLen-1], nil
}

// findRowIdx returns the the upper and lower row index of a byte offset
// Example
// For [0, 1, 2, 3, 4] [5, 6, 7] [8, 9]
// findRowIdx(6, 0, 3)
// - 6 is at rowIdx = 1
// - Hence, lowerRowIdx = 1, upperRowIdx = 2
func (a *ArrayColumnData) findRowIdx(offset, lowerRowIdx, upperRowIdx int) (int, int) {
	// Keep read upper and lower until the offset is equal
	if upperRowIdx-lowerRowIdx == 1 {
		return lowerRowIdx, upperRowIdx
	}

	midRowIdx := (upperRowIdx + lowerRowIdx) / 2
	beforeMidInnerColIdx := a.findOffset(midRowIdx - 1)

	if offset >= beforeMidInnerColIdx {
		return a.findRowIdx(offset, midRowIdx, upperRowIdx)
	}

	return a.findRowIdx(offset, lowerRowIdx, midRowIdx)
}

func interpretArrayType(values []interface{}) (func(value interface{}) ([]interface{}, bool), error) {
	// Get first non nil value
	var firstNonNilValue interface{}
	for _, rawV := range values {
		if rawV != nil {
			firstNonNilValue = rawV
			break
		}
	}

	// If all values are nil, return default func
	if firstNonNilValue == nil {
		return func(value interface{}) ([]interface{}, bool) {
			out, ok := value.([]interface{})
			return out, ok
		}, nil
	}

	v, ok := firstNonNilValue.([]interface{})
	if ok {
		return func(value interface{}) ([]interface{}, bool) {
			out, ok := value.([]interface{})
			return out, ok
		}, nil
	}

	// Check if it's a slice/array of other types
	switch rt := reflect.TypeOf(firstNonNilValue).Kind(); rt {
	case reflect.Slice, reflect.Array:
		var temp []interface{}
		return func(value interface{}) ([]interface{}, bool) {
			if rt != reflect.TypeOf(value).Kind() {
				return nil, false
			}
			temp = temp[:0]
			// Convert to []interface{} and assign to row if it is
			vals := reflect.ValueOf(value)
			for i := 0; i < vals.Len(); i++ {
				temp = append(temp, vals.Index(i).Interface())
			}
			return temp, true
		}, nil

	default:
		return nil, NewErrInvalidColumnType(firstNonNilValue, v)
	}
}
