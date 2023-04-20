package column

import (
	"bytes"
	"encoding/binary"
	"strconv"
	"strings"

	"github.com/RoaringBitmap/roaring"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

var bitmapZeroValue = []uint64{}

// BitMapColumnData
// Data representation is an uint64 array
type BitMapColumnData struct {
	raw      [][]byte
	isClosed bool
}

func (b *BitMapColumnData) ReadFromValues(values []interface{}) (int, error) {
	var (
		value64 uint64
		err     error
		row     []uint64
		ok      bool
	)

	valueBuf := new(bytes.Buffer)

	for idx, value := range values {
		row, ok = value.([]uint64)
		if !ok {
			return idx, NewErrInvalidColumnType(value, row)
		}

		// 1. Make keymap
		keymap := make(map[uint32][]uint32)
		for _, value64 = range row {
			high := uint32(value64 >> 32)
			low := uint32(value64)

			_, exist := keymap[high]
			if !exist {
				keymap[high] = []uint32{}
			}
			keymap[high] = append(keymap[high], low)
		}

		// 2. Write size of keymap
		mapSize := uint64(len(keymap))

		// a. Allocate space for b.raw[idx]
		// length = uint64ByteSize initially to allow allocation of mapSize
		// cap = 8 bytes + 4 bytes * number of keys + size of values (max = 4 bytes * len(texts))
		b.raw[idx] = make([]byte, uint64ByteSize, uint64ByteSize+uint32ByteSize*len(keymap)+uint32ByteSize*len(values))
		binary.LittleEndian.PutUint64(b.raw[idx], mapSize)

		// 3. Write key and values
		for key, v := range keymap {
			// a. Write key
			// Grow raw[idx].len by 4 bytes to allow assignment of key
			b.raw[idx] = append(b.raw[idx], 0, 0, 0, 0)
			binary.LittleEndian.PutUint32(b.raw[idx][len(b.raw[idx])-4:], key)

			// b. Push values in roaring bitmap form
			if _, err = roaring.BitmapOf(v...).WriteTo(valueBuf); err != nil {
				return idx, err
			}
			for _, byt := range valueBuf.Bytes() {
				b.raw[idx] = append(b.raw[idx], byt)
			}
			valueBuf.Reset()
		}
	}

	return len(values), nil
}

// ReadFromTexts reads from text and assigns to bitmap data
// Bitmap is represented as array of uint64
// E.g. [1, 3, 4]
func (b *BitMapColumnData) ReadFromTexts(texts []string) (int, error) {
	var (
		value64 uint64
		err     error
		row     []string // row is []uint64 in string representation
	)

	valueBuf := new(bytes.Buffer)

	for idx, text := range texts {
		if text == "" {
			continue
		}

		if text, err = removeSquareBraces(text); err != nil {
			return idx, err
		}

		// If array is empty skip
		if strings.TrimSpace(text) == "" {
			continue
		}

		row = splitIgnoreBraces(text, comma, row)

		// 1. Make keymap
		keymap := make(map[uint32][]uint32)
		for _, value := range row {
			if value64, err = strconv.ParseUint(value, 10, 64); err != nil {
				return idx, NewErrInvalidColumnTypeCustomText("data should be []uint64")
			}
			high := uint32(value64 >> 32)
			low := uint32(value64)

			_, exist := keymap[high]
			if !exist {
				keymap[high] = []uint32{}
			}
			keymap[high] = append(keymap[high], low)
		}

		// 2. Write size of keymap
		mapSize := uint64(len(keymap))

		// a. Allocate space for b.raw[idx]
		// length = uint64ByteSize initially to allow allocation of mapSize
		// cap = 8 bytes + 4 bytes * number of keys + size of values (max = 4 bytes * len(texts))
		b.raw[idx] = make([]byte, uint64ByteSize, uint64ByteSize+uint32ByteSize*len(keymap)+uint32ByteSize*len(texts))
		binary.LittleEndian.PutUint64(b.raw[idx], mapSize)

		// 3. Write key and values
		for key, values := range keymap {
			// a. Write key
			// Grow raw[idx].len by 4 bytes to allow assignment of key
			b.raw[idx] = append(b.raw[idx], 0, 0, 0, 0)
			binary.LittleEndian.PutUint32(b.raw[idx][len(b.raw[idx])-4:], key)

			// b. Push values in roaring bitmap form
			if _, err = roaring.BitmapOf(values...).WriteTo(valueBuf); err != nil {
				return idx, err
			}
			for _, byt := range valueBuf.Bytes() {
				b.raw[idx] = append(b.raw[idx], byt)
			}
			valueBuf.Reset()
		}
	}

	return len(texts), nil
}

// ReadFromDecoder populates data with value from decoder
func (b *BitMapColumnData) ReadFromDecoder(decoder *ch_encoding.Decoder) error {
	for i := range b.raw {
		// Get length of data bytes
		length, err := decoder.Uvarint()
		if err != nil {
			return err
		}

		data := make([]byte, length)
		if _, err = decoder.Read(data); err != nil {
			return err
		}

		b.raw[i] = data
	}

	return nil
}

func (b *BitMapColumnData) WriteToEncoder(encoder *ch_encoding.Encoder) error {
	var err error

	for _, data := range b.raw {
		// Push byte size
		if err = encoder.Uvarint(uint64(len(data))); err != nil {
			return err
		}

		// Push data
		if _, err = encoder.Write(data); err != nil {
			return err
		}
	}

	return nil
}

func (b *BitMapColumnData) GetValue(row int) interface{} {
	data := b.raw[row]
	if data == nil || len(data) == 0 {
		return bitmapZeroValue
	}

	// Bitmap size
	rb := roaring.New()
	mapSize := binary.LittleEndian.Uint64(data[:8])

	// Actual data
	dataBuf := bytes.NewBuffer(data[8:])
	scratch := make([]byte, 4)

	// Read indices - can add a capacity
	var result []uint64
	for i := uint64(0); i < mapSize; i++ {
		if _, err := dataBuf.Read(scratch); err != nil {
			return bitmapZeroValue
		}
		key := binary.LittleEndian.Uint32(scratch)
		if _, err := rb.ReadFrom(dataBuf); err != nil {
			return bitmapZeroValue
		}
		values := rb.ToArray()
		rb.Clear()
		for _, value := range values {
			result = append(result, uint64(key)<<32|uint64(value))
		}
	}

	return result
}

func (b *BitMapColumnData) GetString(row int) string {
	var builder strings.Builder
	array := b.GetValue(row).([]uint64)

	builder.WriteByte(squareOpenBracket)
	if len(array) > 0 {
		builder.WriteString(strconv.Itoa(int(array[0])))
	}

	for i := 1; i < len(array); i++ {
		builder.WriteString(listSeparator)
		builder.WriteString(strconv.Itoa(int(array[i])))
	}
	builder.WriteByte(squareCloseBracket)

	return builder.String()
}

func (b *BitMapColumnData) Zero() interface{} {
	return bitmapZeroValue
}

func (b *BitMapColumnData) ZeroString() string {
	return emptyArray
}
func (b *BitMapColumnData) Len() int {
	return len(b.raw)
}

func (b *BitMapColumnData) Close() error {
	if b.isClosed {
		return nil
	}
	b.isClosed = true
	for _, d := range b.raw {
		bytepool.PutBytes(d)
	}
	return nil
}
