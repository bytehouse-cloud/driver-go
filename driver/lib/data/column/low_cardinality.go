package column

import (
	"encoding/binary"
	"math"
	"reflect"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

// LowCardinality
// What is low cardinality: https://altinity.com/blog/2019/3/27/low-cardinality
// Example:
// 3 kinds of strings a, b, c
// 1. Send a, b, c (Can be any order)
// 2. Send 3 indexes
// - uint format, e.g. 4, 5, 7
// 3. Result dictionary
// - {a: 4, b: 5, c: 7}
// 4. Send rows
// - e.g. 4, 5, 5, 5, 7 (equals to a, b, b, b, c)
// 5. Retrieve row at 5
// - Search dictionary to find string -> b
type LowCardinalityColumnData struct {
	// header is a fixed amount of bytes to be read and write when number of rows is at least one.
	// 1st uint64 refers to the version
	// 2nd uint64:
	//		1st 8 bit(uint8) represent number of bytes for each indices,
	// 		bit at 8th idx: flag to indicate if global dictionary is needed,
	// 		bit at 9th idx: flag to indicate if there exist additional key,
	// 		bit at 10th idx: flag to indicate if there is a need to update dictionary.
	// 3rd uint64: number of keys to read
	header          [24]byte
	generateKeys    GenerateColumnData
	keys            CHColumnData
	valueIndicesRaw []byte

	getIndex func(row int) uint
	putIndex func(row int, idx uint)
	numRows  int
}

type indexType uint8

const (
	idxTypeUInt8 indexType = iota
	idxTypeUInt16
	idxTypeUInt32
	idxTypeUInt64
)

// byteSize returns the byte size of the indexType
func (idxType indexType) byteSize() int {
	switch idxType {
	case idxTypeUInt8:
		return 1
	case idxTypeUInt16:
		return uint16ByteSize
	case idxTypeUInt32:
		return uint32ByteSize
	default:
		return uint64ByteSize
	}
}

func indexTypeFromByte(b uint8) indexType {
	switch b {
	case uint8(idxTypeUInt8):
		return idxTypeUInt8
	case uint8(idxTypeUInt16):
		return idxTypeUInt16
	case uint8(idxTypeUInt32):
		return idxTypeUInt32
	default:
		return idxTypeUInt64
	}
}

// indexTypeFromKeySize returns index type
// IndexType is determined by the number of keys
// Will never return UInt64 indexType because nKeys int type can't be larger than largest value of UInt32
func indexTypeFromKeySize(nKeys int) indexType {
	switch {
	case nKeys <= math.MaxUint8:
		return idxTypeUInt8
	case nKeys <= math.MaxUint16:
		return idxTypeUInt16
	default:
		return idxTypeUInt32
	}
}

func (l *LowCardinalityColumnData) ReadFromDecoder(decoder *ch_encoding.Decoder) error {
	if l.numRows == 0 {
		return nil
	}
	if _, err := decoder.Read(l.header[:]); err != nil {
		return err
	}

	numKeys := binary.LittleEndian.Uint64(l.header[16:])
	l.keys = l.generateKeys(int(numKeys))
	if err := l.keys.ReadFromDecoder(decoder); err != nil {
		return err
	}

	numIndices, err := decoder.UInt64()
	if err != nil {
		return err
	}
	// Index type of each value
	idxType := indexTypeFromByte(l.header[8])
	indexByteSize := idxType.byteSize()
	l.valueIndicesRaw = bytepool.GetBytesWithLen(int(numIndices) * indexByteSize)

	l.getIndex = l.generateGetIndex(idxType)

	_, err = decoder.Read(l.valueIndicesRaw)
	return err
}

func (l *LowCardinalityColumnData) WriteToEncoder(encoder *ch_encoding.Encoder) error {
	if l.numRows == 0 {
		return nil
	}
	if _, err := encoder.Write(l.header[:]); err != nil {
		return err
	}
	if err := l.keys.WriteToEncoder(encoder); err != nil {
		return err
	}
	if err := encoder.UInt64(uint64(l.numRows)); err != nil {
		return err
	}
	_, err := encoder.Write(l.valueIndicesRaw)
	return err
}

func (l *LowCardinalityColumnData) GetValue(row int) interface{} {
	if l.getIndex == nil {
		return l.Zero()
	}

	return l.keys.GetValue(int(l.getIndex(row)))
}

func (l *LowCardinalityColumnData) GetString(row int) string {
	if l.getIndex == nil {
		return l.ZeroString()
	}

	return l.keys.GetString(int(l.getIndex(row)))
}

func (l *LowCardinalityColumnData) Zero() interface{} {
	return l.keys.Zero()
}

func (l *LowCardinalityColumnData) ZeroString() string {
	return l.keys.ZeroString()
}

func (l *LowCardinalityColumnData) Len() int {
	return l.numRows
}

func (l *LowCardinalityColumnData) Close() error {
	if l.keys == nil {
		return nil
	}
	bytepool.PutBytes(l.valueIndicesRaw)
	return l.keys.Close()
}

func (l *LowCardinalityColumnData) ReadFromValues(values []interface{}) (int, error) {
	if len(values) == 0 {
		return 0, nil
	}

	var (
		curIndex  uint
		keyValues []interface{}
		// unique values to uint map
		indexByValue map[interface{}]uint
	)

	indices := make([]uint, len(values))
	indexByValue = make(map[interface{}]uint)

	for i, value := range values {
		idx, ok := indexByValue[value]
		if !ok {
			indexByValue[value] = curIndex
			idx = curIndex
			curIndex++
		}
		indices[i] = idx
	}

	// all keys in indexByValue
	keyValues = make([]interface{}, len(indexByValue))
	for value, i := range indexByValue {
		keyValues[i] = value
	}

	l.keys = l.generateKeys(len(keyValues))

	if n, err := l.keys.ReadFromValues(keyValues); err != nil {
		invalid := keyValues[n]
		for i, v := range values {
			if reflect.DeepEqual(v, invalid) {
				return i, err
			}
		}
		return 0, err
	}

	binary.LittleEndian.PutUint64(l.header[16:], uint64(len(keyValues)))
	l.numRows = len(values)
	idxType := indexTypeFromKeySize(len(keyValues))
	l.header[8] = uint8(idxType)
	l.putIndex = l.generatePutIndex(idxType)
	l.getIndex = l.generateGetIndex(idxType)
	l.valueIndicesRaw = bytepool.GetBytesWithLen(l.numRows * idxType.byteSize())

	for i, value := range values {
		l.putIndex(i, indexByValue[value])
	}

	// version
	l.header[0] = 1

	// indicate presence of additional keys
	l.header[9] = 2

	return len(values), nil
}

func (l *LowCardinalityColumnData) ReadFromTexts(texts []string) (int, error) {
	if len(texts) == 0 {
		return 0, nil
	}

	indexByText := make(map[string]uint)
	indices := make([]uint, len(texts))

	var curIndex uint
	for i, text := range texts {
		idx, ok := indexByText[text]
		if !ok {
			indexByText[text] = curIndex
			idx = curIndex
			curIndex++
		}
		indices[i] = idx
	}

	keyTexts := make([]string, len(indexByText))
	for text, i := range indexByText {
		keyTexts[i] = text
	}

	l.keys = l.generateKeys(len(keyTexts))

	if n, err := l.keys.ReadFromTexts(keyTexts); err != nil {
		invalid := keyTexts[n]
		for i, s := range texts {
			if s == invalid {
				return i, err
			}
		}
		return 0, err
	}

	binary.LittleEndian.PutUint64(l.header[16:], uint64(len(keyTexts)))
	l.numRows = len(texts)
	idxType := indexTypeFromKeySize(len(keyTexts))
	l.header[8] = uint8(idxType)
	l.putIndex = l.generatePutIndex(idxType)
	l.getIndex = l.generateGetIndex(idxType)
	l.valueIndicesRaw = bytepool.GetBytesWithLen(l.numRows * idxType.byteSize())

	for i, text := range texts {
		l.putIndex(i, indexByText[text])
	}

	// version
	l.header[0] = 1

	// indicate presence of additional keys
	l.header[9] = 2

	return len(texts), nil
}

func (l *LowCardinalityColumnData) generateGetIndex(idxType indexType) func(row int) uint {
	switch idxType {
	case idxTypeUInt8:
		return func(row int) uint {
			return uint(l.valueIndicesRaw[row])
		}
	case idxTypeUInt16:
		return func(row int) uint {
			return uint(bufferRowToUint16(l.valueIndicesRaw, row))
		}
	case idxTypeUInt32:
		return func(row int) uint {
			return uint(bufferRowToUint32(l.valueIndicesRaw, row))
		}
	default:
		return func(row int) uint {
			return uint(bufferRowToUint64(l.valueIndicesRaw, row))
		}
	}
}

func (l *LowCardinalityColumnData) generatePutIndex(idxType indexType) func(row int, idx uint) {
	switch idxType {
	case idxTypeUInt8:
		return func(row int, idx uint) {
			l.valueIndicesRaw[row] = uint8(idx)
		}
	case idxTypeUInt16:
		return func(row int, idx uint) {
			binary.LittleEndian.PutUint16(l.valueIndicesRaw[row*uint16ByteSize:], uint16(idx))
		}
	case idxTypeUInt32:
		return func(row int, idx uint) {
			binary.LittleEndian.PutUint32(l.valueIndicesRaw[row*uint32ByteSize:], uint32(idx))
		}
	default:
		return func(row int, idx uint) {
			binary.LittleEndian.PutUint64(l.valueIndicesRaw[row*uint64ByteSize:], uint64(idx))
		}
	}
}
