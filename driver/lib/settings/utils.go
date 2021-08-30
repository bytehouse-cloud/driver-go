package settings

import (
	"fmt"
	"strconv"

	"github.com/valyala/fastjson/fastfloat"

	"github.com/bytehouse-cloud/driver-go/errors"
)

func SettingToValue(key string, value interface{}) (interface{}, error) {
	defVal, ok := Default[key]
	if !ok {
		return nil, errors.ErrorfWithCaller("query settings not found: %v", key)
	}
	v, err := parseValueInterface(value, defVal)
	if err != nil {
		return nil, errors.ErrorfWithCaller("parsing error for %v: %s", key, err)
	}
	return v, nil
}

func parseValueInterface(value interface{}, defVal interface{}) (interface{}, error) {
	switch defVal.(type) {
	case bool:
		return tryConvertBool(value)
	case int64:
		return tryConvertInt64(value)
	case uint64:
		return tryConvertUint64(value)
	case float32:
		return tryConvertFloat32(value)
	case string:
		return fmt.Sprint(value), nil
	default:
		return nil, fmt.Errorf("unknown data type %T for %v", defVal, defVal)
	}
}

func tryConvertFloat32(value interface{}) (string, error) {
	switch asserted := value.(type) {
	case float32, float64:
		return fmt.Sprint(value), nil
	case string:
		parsed, err := fastfloat.Parse(asserted)
		if err != nil {
			return "", err
		}
		return fmt.Sprint(parsed), nil
	default:
		return "", fmt.Errorf("data type expected: %T, got: %T", float32(0), value)
	}
}

func tryConvertInt64(value interface{}) (int64, error) {
	switch asserted := value.(type) {
	case int64:
		return asserted, nil
	case int32:
		return int64(asserted), nil
	case int16:
		return int64(asserted), nil
	case int8:
		return int64(asserted), nil
	case int:
		return int64(asserted), nil
	case uint64:
		return int64(asserted), nil
	case uint32:
		return int64(asserted), nil
	case uint16:
		return int64(asserted), nil
	case uint8:
		return int64(asserted), nil
	case uint:
		return int64(asserted), nil
	case string:
		parsed, err := strconv.ParseInt(asserted, 10, 64)
		if err != nil {
			return 0, err
		}
		return int64(parsed), nil
	default:
		return 0, fmt.Errorf("data type expected: %T, got: %T", int64(0), value)
	}
}

func tryConvertUint64(value interface{}) (uint64, error) {
	switch asserted := value.(type) {
	case int64:
		return uint64(asserted), nil
	case int32:
		return uint64(asserted), nil
	case int16:
		return uint64(asserted), nil
	case int8:
		return uint64(asserted), nil
	case int:
		return uint64(asserted), nil
	case uint64:
		return asserted, nil
	case uint32:
		return uint64(asserted), nil
	case uint16:
		return uint64(asserted), nil
	case uint8:
		return uint64(asserted), nil
	case uint:
		return uint64(asserted), nil
	case string:
		parsed, err := strconv.ParseUint(asserted, 10, 64)
		if err != nil {
			return 0, err
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("data type expected: %T, got: %T", uint64(0), value)
	}
}

func tryConvertBool(value interface{}) (bool, error) {
	switch asserted := value.(type) {
	case int:
		return asserted > 0, nil
	case uint:
		return asserted > 0, nil
	case int8:
		return asserted > 0, nil
	case uint8:
		return asserted > 0, nil
	case int16:
		return asserted > 0, nil
	case uint16:
		return asserted > 0, nil
	case int32:
		return asserted > 0, nil
	case uint32:
		return asserted > 0, nil
	case int64:
		return asserted > 0, nil
	case uint64:
		return asserted > 0, nil
	case string:
		parsed, err := strconv.ParseBool(asserted)
		if err != nil {
			return false, err
		}
		return parsed, nil
	case bool:
		return asserted, nil
	default:
		return false, fmt.Errorf("data type expected: %T, got: %T", uint64(0), value)
	}
}
