package column

import (
	"encoding/binary"
	"reflect"
	"strings"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
	"github.com/bytehouse-cloud/driver-go/errors"
)

const (
	invalidMapFmt       = "invalid string for map: %s, example: {\"key\": value, ...}"
	mapDecipherErrorFmt = "unable to decipher string to map: %s"
	emptyMap            = "{}"
)

// MapColumnData's data representation
// Map is represented with 2 arrays of same length
// One array contains the keys, the other contains the values
// Values of keys can't be another map
// For example:
// For the map below
// {1 : a, 2: b} {3: c, 4: d, 5: e}
// These are the arrays of keys
// [1 2] [3 4 5]
// These are the arrays of values
// [a b] [c d e]
// The key arrays are combined, same with the values arrays
// Hence offsets are also stored to know the boundary index of each map for reconstruction later
// The offsets for the above example are 2 and 5
//
// To retrieve first map:
// 1. offset = 2
// 2. offset of previous = 0
// 3. get from 0 to 2 from of the keys array and values array
// 4. reconstruct map
type MapColumnData struct {
	offsetsRaw []byte
	// for both keys and values, we only know the num of rows to read when we read the last offset
	generateKeys    GenerateColumnData
	generateValues  GenerateColumnData
	keyColumnData   CHColumnData
	valueColumnData CHColumnData
}

func (m *MapColumnData) ReadFromDecoder(decoder *ch_encoding.Decoder) error {
	n, err := decoder.Read(m.offsetsRaw)
	if err != nil {
		return err
	}

	var lastOffset int
	if n > 0 {
		lastOffset = int(binary.LittleEndian.Uint64(m.offsetsRaw[n-8:]))
	}

	m.keyColumnData = m.generateKeys(lastOffset)
	m.valueColumnData = m.generateValues(lastOffset)

	if err = m.keyColumnData.ReadFromDecoder(decoder); err != nil {
		return err
	}
	return m.valueColumnData.ReadFromDecoder(decoder)
}

func (m *MapColumnData) WriteToEncoder(encoder *ch_encoding.Encoder) error {
	if _, err := encoder.Write(m.offsetsRaw); err != nil {
		return err
	}
	if err := m.keyColumnData.WriteToEncoder(encoder); err != nil {
		return err
	}
	return m.valueColumnData.WriteToEncoder(encoder)
}

func (m *MapColumnData) ReadFromValues(values []interface{}) (numRead int, err error) {
	if len(values) == 0 {
		return 0, nil
	}

	defer func() {
		if r := recover(); r != nil {
			err = errors.ErrorfWithCaller("was panic, recovered value: %v", r)
		}
	}()

	var (
		keyValues   []interface{}
		valueValues []interface{}
		row         map[interface{}]interface{}
		n           int
		ok          bool
	)

	interpret, err := interpretMapType(values)
	if err != nil {
		return 0, err
	}

	for idx, value := range values {
		if value == nil {
			binary.LittleEndian.PutUint64(m.offsetsRaw[idx*uint64ByteSize:], uint64(len(keyValues)))
			continue
		}

		row, ok = interpret(value)
		if !ok {
			return 0, NewErrInvalidColumnType(value, row)
		}

		for k, v := range row {
			keyValues = append(keyValues, k)
			valueValues = append(valueValues, v)
		}

		binary.LittleEndian.PutUint64(m.offsetsRaw[idx*uint64ByteSize:], uint64(len(keyValues)))
	}

	m.keyColumnData = m.generateKeys(len(keyValues))
	m.valueColumnData = m.generateValues(len(valueValues))

	var (
		keyLower int
		keyErr   error
	)
	if n, keyErr = m.keyColumnData.ReadFromValues(keyValues); keyErr != nil {
		keyLower, _ = m.findRowIdx(n, 0, len(values))
	}

	var (
		valueLower int
		valueErr   error
	)
	if n, valueErr = m.valueColumnData.ReadFromValues(valueValues); valueErr != nil {
		valueLower, _ = m.findRowIdx(n, 0, len(values))
	}

	// If both have error, return the lower of whichever is smaller
	if keyErr != nil && valueErr != nil {
		err = errors.ErrorfWithCaller("key err = %v, value err = %v", keyErr, valueErr)
		if valueLower < keyLower {
			return valueLower, err
		}

		return keyLower, err
	}

	// Otherwise return whichever has err
	if keyErr != nil {
		return keyLower, keyErr
	}

	if valueErr != nil {
		return valueLower, valueErr
	}

	return len(values), nil
}

func (m *MapColumnData) ReadFromTexts(texts []string) (int, error) {
	var (
		keyTexts   []string
		valueTexts []string
		i          int
		n          int
		text       string
		err        error
	)

	for i, text = range texts {
		if isEmptyOrNull(text) {
			binary.LittleEndian.PutUint64(m.offsetsRaw[i*uint64ByteSize:], uint64(len(keyTexts)))
			continue
		}

		if text, err = removeCurlyBraces(text); err != nil {
			return i, err
		}
		if strings.TrimSpace(text) == "" {
			binary.LittleEndian.PutUint64(m.offsetsRaw[i*uint64ByteSize:], uint64(len(keyTexts)))
			continue
		}

		mapTexts := splitIgnoreBraces(text, comma, nil)
		for _, mapText := range mapTexts {
			mapText = strings.TrimSpace(mapText)
			kvPair := splitIgnoreBraces(mapText, colon, nil)
			if len(kvPair) != 2 {
				return 0, errors.ErrorfWithCaller(mapDecipherErrorFmt, mapText)
			}
			keyTexts = append(keyTexts, strings.TrimSpace(kvPair[0]))
			valueTexts = append(valueTexts, strings.TrimSpace(kvPair[1]))
		}
		binary.LittleEndian.PutUint64(m.offsetsRaw[i*uint64ByteSize:], uint64(len(keyTexts)))
	}

	m.keyColumnData = m.generateKeys(len(keyTexts))
	m.valueColumnData = m.generateValues(len(valueTexts))

	var (
		keyLower int
		keyErr   error
	)
	if n, keyErr = m.keyColumnData.ReadFromTexts(keyTexts); keyErr != nil {
		keyLower, _ = m.findRowIdx(n, 0, len(texts))
	}

	var (
		valueLower int
		valueErr   error
	)
	if n, valueErr = m.valueColumnData.ReadFromTexts(valueTexts); valueErr != nil {
		valueLower, _ = m.findRowIdx(n, 0, len(texts))
	}

	// If both have error, return the lower of whichever is smaller
	if keyErr != nil && valueErr != nil {
		err = errors.ErrorfWithCaller("key err = %v, value err = %v", keyErr, valueErr)
		if valueLower < keyLower {
			return valueLower, err
		}

		return keyLower, err
	}

	// Otherwise return whichever has err
	if keyErr != nil {
		return keyLower, keyErr
	}

	if valueErr != nil {
		return valueLower, valueErr
	}

	return len(texts), nil
}

func (m *MapColumnData) GetValue(row int) interface{} {
	keys := getColumnValuesUsingOffset(m.findOffset(row-1), m.findOffset(row), m.keyColumnData)
	values := getColumnValuesUsingOffset(m.findOffset(row-1), m.findOffset(row), m.valueColumnData)

	keyType := reflect.ValueOf(m.keyColumnData.Zero()).Type()
	valueType := reflect.ValueOf(m.valueColumnData.Zero()).Type()

	resultType := reflect.MapOf(keyType, valueType)
	result := reflect.MakeMapWithSize(resultType, len(keys))

	for i := range keys {
		k := reflect.ValueOf(keys[i])
		v := reflect.ValueOf(values[i])

		if arrI, ok := values[i].([]interface{}); ok {
			v = modifyTypeForArray(arrI, valueType)
		}
		result.SetMapIndex(k, v)
	}

	return result.Interface()
}

func modifyTypeForArray(arrI []interface{}, typ reflect.Type) reflect.Value {
	arrOfDesiredType := reflect.MakeSlice(typ, len(arrI), len(arrI))
	for i, _ := range arrI {
		idx := arrOfDesiredType.Index(i)
		idx.Set(reflect.ValueOf(arrI[i]))
	}
	v := reflect.ValueOf(arrOfDesiredType.Interface())
	return v
}

func (m *MapColumnData) GetString(row int) string {
	if m.Len() == 0 {
		return emptyMap
	}

	var builder strings.Builder

	keys := getColumnStringsUsingOffset(m.findOffset(row-1), m.findOffset(row), m.keyColumnData)
	values := getColumnStringsUsingOffset(m.findOffset(row-1), m.findOffset(row), m.valueColumnData)
	keyKind := reflect.ValueOf(m.keyColumnData.Zero()).Type().Kind()
	valueKind := reflect.ValueOf(m.valueColumnData.Zero()).Type().Kind()

	_ = builder.WriteByte(curlyOpenBracket)
	if len(keys) > 0 {
		builderWriteKind(&builder, keys[0], keyKind)
		builder.WriteString(mapSeparator)
		builder.WriteByte(space)
		builderWriteKind(&builder, values[0], valueKind)
	}
	for i := 1; i < len(keys); i++ {
		builder.WriteString(listSeparator)
		builderWriteKind(&builder, keys[i], keyKind)
		builder.WriteString(mapSeparator)
		builder.WriteByte(space)
		builderWriteKind(&builder, values[i], valueKind)
	}
	_ = builder.WriteByte(curlyCloseBracket)
	return builder.String()
}

func (m *MapColumnData) Zero() interface{} {
	keyType := reflect.ValueOf(m.keyColumnData.Zero()).Type()
	valueType := reflect.ValueOf(m.valueColumnData.Zero()).Type()
	resultType := reflect.MapOf(keyType, valueType)
	resultValue := reflect.MakeMapWithSize(resultType, 0)
	return resultValue.Interface()
}

func (m *MapColumnData) ZeroString() string {
	return emptyMap
}

func (m *MapColumnData) Len() int {
	return len(m.offsetsRaw) / 8
}

func (m *MapColumnData) Close() error {
	_ = m.keyColumnData.Close()
	return m.valueColumnData.Close()
}

func (m *MapColumnData) findOffset(row int) int {
	if row == -1 {
		return 0
	}
	return int(binary.LittleEndian.Uint64(m.offsetsRaw[row*uint64ByteSize:]))
}

func removeCurlyBraces(s string) (string, error) {
	sLen := len(s)
	if sLen < 2 || s[0] != curlyOpenBracket || s[sLen-1] != curlyCloseBracket {
		return emptyString, errors.ErrorfWithCaller(invalidMapFmt, s)
	}
	return s[1 : sLen-1], nil
}

// findRowIdx returns the the upper and lower row index of a byte offset
// Example
// For [0, 1, 2, 3, 4] [5, 6, 7] [8, 9]
// findRowIdx(6, 0, 3)
// - 6 is at rowIdx = 1
// - Hence, lowerRowIdx = 1, upperRowIdx = 2
func (m *MapColumnData) findRowIdx(offset, lowerRowIdx, upperRowIdx int) (int, int) {
	// Keep read upper and lower until the offset is equal
	if upperRowIdx-lowerRowIdx == 1 {
		return lowerRowIdx, upperRowIdx
	}

	midRowIdx := (upperRowIdx + lowerRowIdx) / 2
	beforeMidInnerColIdx := m.findOffset(midRowIdx - 1)

	if offset >= beforeMidInnerColIdx {
		return m.findRowIdx(offset, midRowIdx, upperRowIdx)
	}

	return m.findRowIdx(offset, lowerRowIdx, midRowIdx)
}

func interpretMapType(values []interface{}) (func(value interface{}) (map[interface{}]interface{}, bool), error) {
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
		return func(value interface{}) (map[interface{}]interface{}, bool) {
			out, ok := value.(map[interface{}]interface{})
			return out, ok
		}, nil
	}

	v, ok := firstNonNilValue.(map[interface{}]interface{})
	if ok {
		return func(value interface{}) (map[interface{}]interface{}, bool) {
			out, ok := value.(map[interface{}]interface{})
			return out, ok
		}, nil
	}

	// Check if it's a slice/array of other types
	switch rt := reflect.TypeOf(firstNonNilValue).Kind(); rt {
	case reflect.Map:
		return func(value interface{}) (map[interface{}]interface{}, bool) {
			if rt != reflect.TypeOf(value).Kind() {
				return nil, false
			}
			vals := reflect.ValueOf(value)
			iter := vals.MapRange()
			row := make(map[interface{}]interface{}, vals.Len())
			for iter.Next() {
				row[iter.Key().Interface()] = iter.Value().Interface()
			}
			return row, true
		}, nil
	case reflect.Struct:
		return func(value interface{}) (map[interface{}]interface{}, bool) {
			// Convert struct to map
			valType := reflect.TypeOf(value)
			if rt != valType.Kind() {
				return nil, false
			}
			vals := reflect.ValueOf(value).Convert(valType)
			row := make(map[interface{}]interface{}, vals.NumField())
			for i := 0; i < vals.NumField(); i++ {
				keyField := valType.Field(i)
				// Use tag as key if have tag
				if keyTag := keyField.Tag.Get("clickhouse"); keyTag != "" {
					row[keyTag] = vals.Field(i).Interface()
					continue
				}

				// Else use field name
				row[keyField.Name] = vals.Field(i).Interface()
			}
			return row, true
		}, nil
	default:
		return nil, NewErrInvalidColumnType(firstNonNilValue, v)
	}
}
