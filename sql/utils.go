package sql

import (
	"database/sql/driver"
	"fmt"
	"math"
	"net"
	"reflect"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/bytehouse-cloud/driver-go/errors"
)

// bindArgsToQuery binds query's question marks to args
// This function should only be used for select queries
// e.g. (SELECT ?, "Goo") -> "SELECT 'Goo'"
// Returns err if there are not as many ? as args
func bindArgsToQuery(query string, args []driver.Value) (string, error) {
	if len(args) == 0 {
		return query, nil
	}

	var sb strings.Builder
	// Preallocate memory (assume args 10 bytes each)
	sb.Grow(len(query) + len(args)*10)

	var index int
	var openSingleQuote bool
	var openBacktick bool
	var openDoubleQuote bool

	for i, value := range query {
		// If open single quote ', skip question mark replace until next close single quote '
		if value == '\'' {
			// Ignore quotes escaped by \\', check by looking behind
			if i <= 1 || (query[i-1] != '\\' && query[i-2] != '\\') {
				openSingleQuote = !openSingleQuote
			}
		}

		// If open backtick `, skip question mark replace until next close backtick `
		if value == '`' {
			openBacktick = !openBacktick
		}

		// If open double quote ", skip question mark replace until next close double quote "
		if value == '"' {
			openDoubleQuote = !openDoubleQuote
		}

		if value != '?' || openSingleQuote || openBacktick || openDoubleQuote {
			sb.WriteRune(value)
			continue
		}

		// Replace question mark with arg
		if index >= len(args) {
			return "", errors.ErrorfWithCaller("less args then query's ? sign")
		}

		sb.WriteString(quote(args[index]))
		index++
	}

	// index should have advanced past all args
	if index != len(args) {
		return "", errors.ErrorfWithCaller("more args then query's ? sign")
	}

	return sb.String(), nil
}

// quote converts driver.Value into a string used in a sql statement depending on it's type
// this function is copied from clickhouse_go
func quote(v driver.Value) string {
	switch v := v.(type) {
	case string:
		return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(v) + "'"
	case time.Time:
		return formatTime(v)
	case net.IP:
		return v.String()
	case uuid.UUID:
		return v.String()
	}

	switch v := reflect.ValueOf(v); v.Kind() {
	case reflect.Slice:
		var sb strings.Builder
		sb.WriteRune('[')
		for i := 0; ; i++ {
			if i == v.Len()-1 {
				sb.WriteString(quote(v.Index(v.Len() - 1).Interface()))
				break
			}

			sb.WriteString(quote(v.Index(i).Interface()))
			sb.WriteRune(',')
		}
		sb.WriteRune(']')
		return sb.String()
	case reflect.Map:
		var sb strings.Builder
		sb.WriteRune('{')
		iter := v.MapRange()
		if iter.Next() {
			for {
				sb.WriteString(quote(iter.Key()))
				sb.WriteRune(':')
				sb.WriteString(quote(iter.Value()))

				if !iter.Next() {
					break
				}
				sb.WriteRune(',')
			}
		}
		sb.WriteRune('}')
		return sb.String()
	}

	return fmt.Sprint(v)
}

func formatTime(value time.Time) string {
	// toDate() overflows after 65535 days, but toDateTime() only overflows when time.Time overflows (after 9223372036854775807 seconds)
	if days := value.Unix() / 24 / 3600; days <= math.MaxUint16 && (value.Hour()+value.Minute()+value.Second()+value.Nanosecond()) == 0 {
		return fmt.Sprintf("toDate(%d)", days)
	}

	return fmt.Sprintf("toDateTime(%d)", value.Unix())
}

func namedArgsToArgs(namedArgs []driver.NamedValue) ([]driver.Value, error) {
	args := make([]driver.Value, len(namedArgs))
	for n, param := range namedArgs {
		if len(param.Name) > 0 {
			return nil, errors.Errorf("named params not supported")
		}
		args[n] = param.Value
	}
	return args, nil
}
