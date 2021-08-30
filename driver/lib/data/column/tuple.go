package column

import (
	"reflect"
	"strings"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
	"github.com/bytehouse-cloud/driver-go/errors"
)

const (
	emptyTuple = "()"
)

func NewErrInvalidTupleElemCount(currentTuple interface{}, currentNumberOfElems, expectedNumberOfElems int) error {
	return errors.ErrorfWithCaller("invalid number of tuple element, current tuple = %v, current no. of elems = %d,  expected no. of elems = %d", currentTuple, currentNumberOfElems, expectedNumberOfElems)
}

type TupleColumnData struct {
	innerColumnsData []CHColumnData
}

func (t *TupleColumnData) ReadFromDecoder(decoder *ch_encoding.Decoder) error {
	for i := range t.innerColumnsData {
		if err := t.innerColumnsData[i].ReadFromDecoder(decoder); err != nil {
			return err
		}
	}
	return nil
}

func (t *TupleColumnData) WriteToEncoder(encoder *ch_encoding.Encoder) error {
	for i := range t.innerColumnsData {
		if err := t.innerColumnsData[i].WriteToEncoder(encoder); err != nil {
			return err
		}
	}
	return nil
}

func (t *TupleColumnData) ReadFromValues(values []interface{}) (int, error) {
	if len(values) == 0 {
		return 0, nil
	}

	var (
		row []interface{}
		ok  bool
	)
	numCol := len(t.innerColumnsData)
	interpret, err := interpretArrayType(values)
	if err != nil {
		return 0, err
	}

	// Initialise column table
	columnValues := make([][]interface{}, numCol)
	for idx := range columnValues {
		columnValues[idx] = make([]interface{}, len(values))
	}

	for idx, value := range values {
		if value == nil {
			return 0, NewErrInvalidColumnType(value, row)
		}

		// Convert value into slice
		row, ok = interpret(value)
		if !ok {
			return 0, NewErrInvalidColumnType(value, row)
		}

		if len(row) != numCol {
			return 0, NewErrInvalidTupleElemCount(value, len(row), len(t.innerColumnsData))
		}

		for colIdx, colValue := range columnValues {
			colValue[idx] = row[colIdx]
		}
	}

	for colIdx, col := range t.innerColumnsData {
		if n, err := col.ReadFromValues(columnValues[colIdx]); err != nil {
			err = errors.ErrorfWithCaller("read fail, row = %d, col = %d, coltype = %T, error = %v", n, colIdx, col.Zero(), err)
			// Return n for rows read only for the last column
			// B/c only for the last column can we verify the entire row is read
			if colIdx == len(t.innerColumnsData)-1 {
				return n, err
			}

			return 0, err
		}
	}

	return len(values), nil
}

func (t *TupleColumnData) ReadFromTexts(texts []string) (int, error) {
	if len(texts) == 0 {
		return 0, nil
	}

	removeBraces, err := interpretBraces(texts[0])
	if err != nil {
		return 0, err
	}

	buffer := make([]string, len(t.innerColumnsData))
	numCol := len(t.innerColumnsData)

	// Initialise column table
	columnTexts := make([][]string, numCol)
	for i := range columnTexts {
		columnTexts[i] = make([]string, len(texts))
	}

	for i, text := range texts {
		if text, err = removeBraces(text); err != nil {
			return i, err
		}
		row := splitIgnoreBraces(text, comma, buffer)
		if len(row) != numCol {
			if text == "" {
				return i, NewErrInvalidTupleElemCount(text, 0, len(t.innerColumnsData))
			}

			return i, NewErrInvalidTupleElemCount(text, len(row), len(t.innerColumnsData))
		}

		for colIdx, colText := range columnTexts {
			colText[i] = row[colIdx]
		}
	}

	for colIdx, col := range t.innerColumnsData {
		if n, err := col.ReadFromTexts(columnTexts[colIdx]); err != nil {
			err = errors.ErrorfWithCaller("read fail, row = %d, col = %d, coltype = %T error = %v", n, colIdx, col.Zero(), err)
			// Return n for rows read only for the last column
			if colIdx == len(t.innerColumnsData)-1 {
				return n, err
			}

			return 0, err
		}
	}

	return len(texts), nil
}

func (t *TupleColumnData) GetValue(row int) interface{} {
	result := make([]interface{}, len(t.innerColumnsData))
	for i, innerColumnData := range t.innerColumnsData {
		result[i] = innerColumnData.GetValue(row)
	}
	return result
}

func (t *TupleColumnData) GetString(row int) string {
	if t.Len() == 0 {
		return emptyTuple
	}

	var builder strings.Builder

	builder.WriteByte(roundOpenBracket)
	for i, innerColumnData := range t.innerColumnsData {
		if i != 0 {
			builder.WriteString(listSeparator)
		}

		builderWriteKind(&builder, innerColumnData.GetString(row), reflect.ValueOf(innerColumnData.Zero()).Type().Kind())
	}
	builder.WriteByte(roundCloseBracket)
	return builder.String()
}

func (t *TupleColumnData) Zero() interface{} {
	result := make([]interface{}, len(t.innerColumnsData))
	for i := range t.innerColumnsData {
		result[i] = t.innerColumnsData[i].Zero()
	}
	return result
}

func (t *TupleColumnData) ZeroString() string {
	return emptyTuple
}

func (t *TupleColumnData) Len() int {
	return len(t.innerColumnsData)
}

func (t *TupleColumnData) Close() error {
	for i := range t.innerColumnsData {
		if err := t.innerColumnsData[i].Close(); err != nil {
			return err
		}
	}
	return nil
}

func interpretBraces(text string) (func(string) (string, error), error) {
	if len(text) == 0 {
		return nil, errors.ErrorfWithCaller("tuple string should not be empty")
	}

	// check if first char is [ or (
	switch text[0] {
	case squareOpenBracket:
		return removeSquareBraces, nil
	case roundOpenBracket:
		return removeRoundBraces, nil
	}

	return nil, errors.ErrorfWithCaller("invalid tuple string: %s", text)
}

func removeRoundBraces(s string) (string, error) {
	sLen := len(s)
	if sLen < 2 || s[0] != roundOpenBracket || s[sLen-1] != roundCloseBracket {
		return emptyString, errors.ErrorfWithCaller("invalid tuple string: %s", s)
	}
	return s[1 : sLen-1], nil
}
