package column

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/bytehouse-cloud/driver-go/driver/lib/bytepool"
)

type CHColumnType string

var (
	NESTED_TYPE_ERROR = fmt.Errorf("[parseNestedType]: failed to pass nested type")
)

const timeMaxScale = 9

const (
	// base type
	INT8         CHColumnType = "Int8"
	INT16        CHColumnType = "Int16"
	INT32        CHColumnType = "Int32"
	INT64        CHColumnType = "Int64"
	INT128       CHColumnType = "Int128"
	INT256       CHColumnType = "Int256"
	UINT8        CHColumnType = "UInt8"
	UINT16       CHColumnType = "UInt16"
	UINT32       CHColumnType = "UInt32"
	UINT64       CHColumnType = "UInt64"
	UINT128      CHColumnType = "UInt128"
	UINT256      CHColumnType = "UInt256"
	FLOAT32      CHColumnType = "Float32"
	FLOAT64      CHColumnType = "Float64"
	STRING       CHColumnType = "String"
	UUID         CHColumnType = "UUID"
	DATE         CHColumnType = "Date"
	DATE32       CHColumnType = "Date32"
	IPV4         CHColumnType = "IPv4"
	IPV6         CHColumnType = "IPv6"
	BITMAP64     CHColumnType = "BitMap64"
	NOTHING      CHColumnType = "Nothing"
	BOOL         CHColumnType = "Bool"
	POINT        CHColumnType = "Point"
	RING         CHColumnType = "Ring"
	POLYGON      CHColumnType = "Polygon"
	MULTIPOLYGON CHColumnType = "MultiPolygon"
	NESTED       CHColumnType = "Nested"

	// complex types with parameters
	NULLABLE       CHColumnType = "Nullable"
	ARRAY          CHColumnType = "Array"
	TUPLE          CHColumnType = "Tuple"
	MAP            CHColumnType = "Map"
	FIXEDSTRING    CHColumnType = "FixedString"
	ENUM8          CHColumnType = "Enum8"
	ENUM16         CHColumnType = "Enum16"
	DECIMAL        CHColumnType = "Decimal"
	DATETIME       CHColumnType = "DateTime"
	DATETIME64     CHColumnType = "DateTime64"
	LOWCARDINALITY CHColumnType = "LowCardinality"
	TIME           CHColumnType = "Time"
	// alias types
	INT CHColumnType = "Int"

	// Aggregate functions
	AGGREGATEFUNCTION      CHColumnType = "AggregateFunction"
	SIMPLEAGGREATEFUNCTION CHColumnType = "SimpleAggregateFunction"
)

// MustMakeColumnData attempts to make column data with give type and row count.
// Panics if not possible
func MustMakeColumnData(t CHColumnType, numRows int) CHColumnData {
	baseImpl, ok := basicDataTypeImpl[t]
	if !ok {
		gen, err := generateComplex(t, nil)
		if err != nil {
			panic(err)
		}
		return gen(numRows)
	}
	return baseImpl(numRows)
}

// GenerateColumnData generates CH column based for numRows
// all rows are initialized to respective zero value
type GenerateColumnData func(numRows int) CHColumnData

func GenerateColumnDataFactory(t CHColumnType) (GenerateColumnData, error) {
	return GenerateColumnDataFactoryWithLocation(t, nil)
}

func GenerateColumnDataFactoryWithLocation(t CHColumnType, location *time.Location) (GenerateColumnData, error) {
	baseImpl, ok := basicDataTypeImpl[t]
	if !ok {
		return generateComplex(t, location)
	}
	return baseImpl, nil
}

func MustGenerateColumnDataFactory(t CHColumnType) GenerateColumnData {
	gen, err := GenerateColumnDataFactory(t)
	if err != nil {
		panic(err)
	}
	return gen
}

func generateComplex(t CHColumnType, location *time.Location) (GenerateColumnData, error) {
	switch {
	case strings.HasPrefix(string(t), string(NULLABLE)):
		return makeNullableColumnData(t, location)
	case strings.HasPrefix(string(t), string(ARRAY)):
		return makeArrayColumnData(t, location)
	case strings.HasPrefix(string(t), string(TUPLE)):
		return makeTupleColumnData(t, location)
	case strings.HasPrefix(string(t), string(MAP)):
		return makeMapColumnData(t, location)
	case strings.HasPrefix(string(t), string(FIXEDSTRING)):
		return makeFixedStringColumnData(t)
	case strings.HasPrefix(string(t), string(ENUM8)):
		return makeEnum8ColumnData(t)
	case strings.HasPrefix(string(t), string(ENUM16)):
		return makeEnum16ColumnData(t)
	case strings.HasPrefix(string(t), string(DECIMAL)):
		return makeDecimalColumnData(t)
	case strings.HasPrefix(string(t), string(DATETIME64)):
		return makeDateTime64ColumnData(t, location)
	case strings.HasPrefix(string(t), string(DATETIME)):
		return makeDateTimeColumnData(t, location)
	case strings.HasPrefix(string(t), string(LOWCARDINALITY)):
		return makeLowCardinality(t, location)
	case strings.HasPrefix(string(t), string(SIMPLEAGGREATEFUNCTION)):
		nestedType, err := parseNestedType(string(t), string(SIMPLEAGGREATEFUNCTION))
		if err != nil {
			return nil, err
		}

		baseImpl, ok := basicDataTypeImpl[nestedType]
		if !ok { // the nestedType might not be simple data type
			columnData, err := generateComplex(nestedType, location)
			if err != nil {
				return nil, err
			}
			return columnData, nil

		}

		return baseImpl, nil

	case strings.HasPrefix(string(t), string(AGGREGATEFUNCTION)):
		nestedType, err := parseNestedType(string(t), string(AGGREGATEFUNCTION))
		if err != nil {
			return nil, err
		}

		baseImpl, ok := basicDataTypeImpl[nestedType]
		if !ok { // the nestedType might not be simple data type
			columnData, err := generateComplex(nestedType, location)
			if err != nil {
				return nil, err
			}
			return columnData, nil
		}

		return baseImpl, nil

	case strings.HasPrefix(string(t), string(TIME)):
		return makeTimeColumnData(t)
	default:
		return nil, fmt.Errorf("unsupported data type: %v", t)
	}
}

var basicDataTypeImpl = map[CHColumnType]func(numRows int) CHColumnData{
	INT8: func(numRows int) CHColumnData {
		return &Int8ColumnData{
			raw: bytepool.GetBytesWithLen(numRows),
		}
	},

	INT16: func(numRows int) CHColumnData {
		return &Int16ColumnData{
			raw: bytepool.GetBytesWithLen(numRows * 2),
		}
	},

	INT32: func(numRows int) CHColumnData {
		return &Int32ColumnData{
			raw: bytepool.GetBytesWithLen(numRows * 4),
		}
	},

	INT64: func(numRows int) CHColumnData {
		return &Int64ColumnData{
			raw: bytepool.GetBytesWithLen(numRows * 8),
		}
	},

	INT128: func(numRows int) CHColumnData {
		return &BigIntColumnData{
			byteCount: 16,
			isSigned:  true,
			raw:       bytepool.GetBytesWithLen(numRows * 16),
		}
	},

	INT256: func(numRows int) CHColumnData {
		return &BigIntColumnData{
			byteCount: 32,
			isSigned:  true,
			raw:       bytepool.GetBytesWithLen(numRows * 32),
		}
	},

	UINT8: func(numRows int) CHColumnData {
		return &UInt8ColumnData{
			raw: bytepool.GetBytesWithLen(numRows),
		}
	},

	UINT16: func(numRows int) CHColumnData {
		return &UInt16ColumnData{
			raw: bytepool.GetBytesWithLen(numRows * 2),
		}
	},

	UINT32: func(numRows int) CHColumnData {
		return &UInt32ColumnData{
			raw: bytepool.GetBytesWithLen(numRows * 4),
		}
	},

	UINT64: func(numRows int) CHColumnData {
		return &UInt64ColumnData{
			raw: bytepool.GetBytesWithLen(numRows * 8),
		}
	},

	UINT128: func(numRows int) CHColumnData {
		return &BigIntColumnData{
			byteCount: 16,
			isSigned:  false,
			raw:       bytepool.GetBytesWithLen(numRows * 16),
		}
	},

	UINT256: func(numRows int) CHColumnData {
		return &BigIntColumnData{
			byteCount: 32,
			isSigned:  false,
			raw:       bytepool.GetBytesWithLen(numRows * 32),
		}
	},

	FLOAT32: func(numRows int) CHColumnData {
		return &Float32ColumnData{
			raw: bytepool.GetBytesWithLen(numRows * 4),
		}
	},

	FLOAT64: func(numRows int) CHColumnData {
		return &Float64ColumnData{
			raw: bytepool.GetBytesWithLen(numRows * 8),
		}
	},

	STRING: func(numRows int) CHColumnData {
		return &StringColumnData{
			raw: make([][]byte, numRows),
		}
	},

	UUID: func(numRows int) CHColumnData {
		return &UUIDColumnData{
			raw: bytepool.GetBytesWithLen(numRows * uuidLen),
		}
	},

	DATE: func(numRows int) CHColumnData {
		var dayOffset int
		// TODO: @Khoi to check why we got this logic below.
		// hourOffset := (time.Now().Hour() * 3600) - int(offset)
		// if hourOffset < 0 {
		// 	if offset > 0 {
		// 		dayOffset++
		// 	} else {
		// 		dayOffset--
		// 	}
		// }

		return &DateColumnData{
			dayOffset: dayOffset,
			raw:       bytepool.GetBytesWithLen(numRows * dateLen),
		}
	},

	DATE32: func(numRows int) CHColumnData {
		return &Date32ColumnData{
			raw: bytepool.GetBytesWithLen(numRows * date32Len),
		}
	},

	IPV4: func(numRows int) CHColumnData {
		return &IPv4ColumnData{
			raw: bytepool.GetBytesWithLen(numRows * net.IPv4len),
		}
	},

	IPV6: func(numRows int) CHColumnData {
		return &IPv6ColumnData{
			raw: bytepool.GetBytesWithLen(numRows * net.IPv6len),
		}
	},

	BITMAP64: func(numRows int) CHColumnData {
		return &BitMapColumnData{
			raw: make([][]byte, numRows),
		}
	},

	BOOL: func(numRows int) CHColumnData {
		return &BoolColumnData{
			raw: bytepool.GetBytesWithLen(numRows),
		}
	},

	NOTHING: func(numRows int) CHColumnData {
		return &NothingColumnData{
			raw: bytepool.GetBytesWithLen(numRows),
		}
	},

	// alias to INT64
	INT: func(numRows int) CHColumnData {
		return &Int64ColumnData{
			raw: bytepool.GetBytesWithLen(numRows * 8),
		}
	},
}

func makeDateTimeColumnData(t CHColumnType, location *time.Location) (GenerateColumnData, error) {
	loc, err := getDateTimeLocation(t)
	if err != nil {
		return nil, err
	}
	if loc != nil {
		location = loc
	}

	if location == nil {
		location = time.Local
	}

	return func(numRows int) CHColumnData {
		return &DateTimeColumnData{
			timeZone: location,
			raw:      bytepool.GetBytesWithLen(numRows * dateTimeLen),
		}
	}, nil
}

func makeDateTime64ColumnData(t CHColumnType, location *time.Location) (GenerateColumnData, error) {
	precision, loc, err := getDateTime64Param(t)
	if err != nil {
		return nil, err
	}
	if loc != nil {
		location = loc
	}

	if location == nil {
		location = time.Local
	}

	return func(numRows int) CHColumnData {
		return &DateTime64ColumnData{
			precision: precision,
			timeZone:  location,
			raw:       bytepool.GetBytesWithLen(numRows * dateTime64Len),
		}
	}, nil
}

func makeTimeColumnData(t CHColumnType) (GenerateColumnData, error) {

	scale, err := getTimeParam(t)
	if err != nil {
		return nil, err
	}

	if scale > timeMaxScale {
		return nil, fmt.Errorf("unsupported scale, maximum is %v", timeMaxScale)
	}
	return func(numRows int) CHColumnData {
		return &TimeColumnData{
			scale: scale,
			baseColumn: &DecimalColumnData{
				precision: 18, // decimal64 precision
				scale:     scale,
				byteCount: 8, // decimal64
				raw:       bytepool.GetBytesWithLen(numRows * 8),
			},
		}
	}, nil

}

func makeDecimalColumnData(t CHColumnType) (GenerateColumnData, error) {
	params := strings.Split(string(t[8:len(t)-1]), ",")
	precisionString := strings.TrimSpace(params[0])
	precision, err := strconv.Atoi(precisionString)
	if err != nil {
		return nil, err
	}
	scaleString := strings.TrimSpace(params[1])
	scale, err := strconv.Atoi(scaleString)
	if err != nil {
		return nil, err
	}

	byteCount := getByteCountFromPrecision(precision)
	return func(numRows int) CHColumnData {
		return &DecimalColumnData{
			precision: precision,
			scale:     scale,
			byteCount: getByteCountFromPrecision(precision),
			raw:       bytepool.GetBytesWithLen(numRows * byteCount),
		}
	}, nil
}

func makeEnum16ColumnData(t CHColumnType) (GenerateColumnData, error) {
	enum16PairsString := t[7 : len(t)-1] // Enum16ColumnData('hello' = 1, 'world' = 2)
	strIter := commaIterator(string(enum16PairsString))
	atoi := make(map[string]int16)
	itoa := make(map[int16]string)
	for {
		s, ok := strIter()
		if !ok {
			break
		}
		enum16StringValuePair := strings.Split(s, enumSeparator)
		enumString := strings.Trim(strings.TrimSpace(enum16StringValuePair[0]), string(singleQuote))
		enum16Value, err := strconv.ParseInt(strings.TrimSpace(enum16StringValuePair[1]), 10, 16)
		if err != nil {
			return nil, err
		}
		atoi[enumString] = int16(enum16Value)
		itoa[int16(enum16Value)] = enumString
	}

	return func(numRows int) CHColumnData {
		return &Enum16ColumnData{
			atoi: atoi,
			itoa: itoa,
			raw:  bytepool.GetBytesWithLen(numRows * uint16ByteSize),
		}
	}, nil
}

func makeEnum8ColumnData(t CHColumnType) (GenerateColumnData, error) {
	enum8PairsString := t[6 : len(t)-1] // Enum8ColumnData('hello' = 1, 'world' = 2)
	strIter := commaIterator(string(enum8PairsString))
	atoi := make(map[string]int8)
	itoa := make(map[int8]string)
	for {
		s, ok := strIter()
		if !ok {
			break
		}
		enum8StringValuePair := strings.Split(s, enumSeparator)
		enumString := strings.Trim(strings.TrimSpace(enum8StringValuePair[0]), string(singleQuote)) // todo: check if needed to trim single quote
		enum8Value, err := strconv.ParseInt(strings.TrimSpace(enum8StringValuePair[1]), 10, 8)
		if err != nil {
			return nil, err
		}
		atoi[enumString] = int8(enum8Value)
		itoa[int8(enum8Value)] = enumString
	}

	return func(numRows int) CHColumnData {
		return &Enum8ColumnData{
			atoi: atoi,
			itoa: itoa,
			raw:  bytepool.GetBytesWithLen(numRows),
		}
	}, nil
}

func makeFixedStringColumnData(t CHColumnType) (GenerateColumnData, error) {
	lenString := t[12 : len(t)-1] // eg. FixedString(256)
	fixedStringLen, err := strconv.ParseUint(string(lenString), 10, 64)
	if err != nil {
		return nil, err
	}
	mask := bytepool.GetBytesWithLen(int(fixedStringLen))
	for i := 0; i < len(mask); i++ {
		mask[i] = 0
	}

	return func(numRows int) CHColumnData {
		return &FixedStringColumnData{
			mask: mask,
			raw:  bytepool.GetBytesWithLen(numRows * int(fixedStringLen)),
		}
	}, nil
}

func makeMapKeyValue(t CHColumnType) (key CHColumnType, value CHColumnType) {
	keyValuePair := splitIgnoreBraces(string(t[4:len(t)-1]), comma, nil) // Map(keyType, valueType) -> keyType, valueType
	key = CHColumnType(strings.TrimSpace(keyValuePair[0]))
	value = CHColumnType(strings.TrimSpace(keyValuePair[1]))
	return key, value
}

func makeMapColumnData(t CHColumnType, location *time.Location) (GenerateColumnData, error) {
	key, value := makeMapKeyValue(t)
	generateKeys, err := generateColumnDataFactoryOptionalTypeName(key, location)
	if err != nil {
		return nil, err
	}
	generateValues, err := generateColumnDataFactoryOptionalTypeName(value, location)
	if err != nil {
		return nil, err
	}

	return func(numRows int) CHColumnData {
		return &MapColumnData{
			offsetsRaw:      bytepool.GetBytesWithLen(numRows * 8),
			generateKeys:    generateKeys,
			generateValues:  generateValues,
			keyColumnData:   generateKeys(numRows),
			valueColumnData: generateValues(numRows),
		}
	}, nil
}

func makeTupleColumnData(t CHColumnType, location *time.Location) (GenerateColumnData, error) {
	tupleElemTypeString := t[6 : len(t)-1] // Tuple(Type1, Type2, ...)
	strIter := commaIterator(string(tupleElemTypeString))
	var generates []GenerateColumnData
	for {
		s, ok := strIter()
		if !ok {
			break
		}
		colDataGen, err := generateColumnDataFactoryOptionalTypeName(CHColumnType(s), location)
		if err != nil {
			return nil, err
		}
		generates = append(generates, colDataGen)
	}

	return func(numRows int) CHColumnData {
		innerColumnDataSlice := make([]CHColumnData, len(generates))
		for i, gen := range generates {
			innerColumnDataSlice[i] = gen(numRows)
		}
		return &TupleColumnData{
			innerColumnsData: innerColumnDataSlice,
		}
	}, nil
}

func makeArrayColumnData(t CHColumnType, location *time.Location) (GenerateColumnData, error) {
	generateInnerData, err := generateColumnDataFactoryOptionalTypeName(t[6:len(t)-1], location) // Array(innerType) -> innerType
	if err != nil {
		return nil, err
	}

	return func(numRows int) CHColumnData {
		return &ArrayColumnData{
			offsetsRaw:        bytepool.GetBytesWithLen(numRows * 8),
			generateInnerData: generateInnerData,
			innerColumnData:   generateInnerData(numRows),
		}
	}, nil
}

func makeNullableColumnData(t CHColumnType, location *time.Location) (GenerateColumnData, error) {
	innerType := t[9 : len(t)-1]
	generateInnerData, err := generateColumnDataFactoryOptionalTypeName(innerType, location) // Nullable(innerType) -> innerType
	if err != nil {
		return nil, err
	}

	return func(numRows int) CHColumnData {
		return &NullableColumnData{
			mask:            make([]byte, numRows),
			innerColumnData: generateInnerData(numRows),
		}
	}, nil
}

func makeLowCardinality(t CHColumnType, location *time.Location) (GenerateColumnData, error) {
	var isNullable bool
	innerType := t[15 : len(t)-1]
	if strings.HasPrefix(string(innerType), string(NULLABLE)) {
		innerType = innerType[9 : len(innerType)-1]
		isNullable = true
	}
	generateKeys, err := generateColumnDataFactoryOptionalTypeName(innerType, location) // LowCardinality(innerType) -> innerType
	if err != nil {
		return nil, err
	}

	return func(numRows int) CHColumnData {
		return &LowCardinalityColumnData{
			// Default keys function's rows = 0 (keys function will be overwritten after reading)
			// Need default keys function to prevent panics when some methods that require it are called
			keys:          generateKeys(0),
			generateKeys:  generateKeys,
			numRows:       numRows,
			isNullableCol: isNullable,
		}
	}, nil
}

// generateColumnDataFactoryOptionalTypeName is similar to GenerateColumnDataFactory
// but allows type name before the type, e.g. "a Int32".
// this will be useful in accepting types like "Array(a Int32)"
func generateColumnDataFactoryOptionalTypeName(t CHColumnType, location *time.Location) (GenerateColumnData, error) {
	gen, err := GenerateColumnDataFactoryWithLocation(t, location)
	if err == nil {
		return gen, nil
	}
	i := strings.IndexByte(string(t), ' ')
	if i == -1 {
		return nil, err
	}

	colTypeTrunc := CHColumnType(strings.TrimSpace(string(t[i:])))
	return GenerateColumnDataFactoryWithLocation(colTypeTrunc, location)
}

func parseNestedType(chColumnType, prefix string) (CHColumnType, error) {
	typeLen, prefLen := len(chColumnType), len(prefix)

	if typeLen < prefLen+2 || chColumnType[prefLen] != roundOpenBracket || chColumnType[typeLen-1] != roundCloseBracket {
		return "", NESTED_TYPE_ERROR
	}

	secondArg, err := returnSecondArg(chColumnType[len(prefix)+1 : typeLen-1])
	if err != nil {
		return "", err
	}

	return CHColumnType(strings.TrimSpace(secondArg)), nil
}

func returnSecondArg(args string) (string, error) {
	// this util should the 2nd argument out of 2 arguments or return error if number of arguments != 2
	counter := 0
	for i, char := range args {
		if char == '(' {
			counter += 1
		}
		if char == ')' {
			counter -= 1
		}
		if char == ',' && counter == 0 {
			for j := i + 1; j < len(args); j++ {
				if args[j] == '(' {
					counter += 1
				}
				if args[j] == ')' {
					counter -= 1
				}
				if args[j] == ',' && counter == 0 {
					return "", NESTED_TYPE_ERROR
				}
			}
			return args[i+1:], nil
		}
	}
	return "", NESTED_TYPE_ERROR
}
