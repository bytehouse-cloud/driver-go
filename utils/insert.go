package utils

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/bytehouse-cloud/driver-go/errors"
	"github.com/bytehouse-cloud/driver-go/stream/format"
)

var selectRe = regexp.MustCompile(`\s+SELECT\s+`)

func IsInsert(query string) bool {
	if f := strings.Fields(query); len(f) > 2 {
		return strings.EqualFold("INSERT", f[0]) && strings.EqualFold("INTO", f[1]) && !selectRe.MatchString(strings.ToUpper(query))
	}
	return false
}

// InsertQuery is a insert Query disected
// e.g.
// INSERT INTO table VALUES (1); becomes
// {
//   DataFmt: "VALUES",
//   Query: "INSERT INTO table VALUES",
//   Values: "(1)"
// }
type InsertQuery struct {
	DataFmt string
	Query   string
	Values  string
}

// ColumnsCount get number of question marks in a row
// Assumption: No nested brackets
// Returns error if invalid values format
// E.g. (?, ?, ?), (?, ?, ?) -> 3
func (iq *InsertQuery) ColumnsCount() (int, error) {
	var questionMarkCount int
	for _, v := range iq.Values {
		if v == '?' {
			questionMarkCount += 1
			continue
		}
		if v == ')' {
			return questionMarkCount, nil
		}
	}
	return 0, errors.ErrorfWithCaller("invalid query values format = %s", iq.Values)
}

var (
	splitInsertRe = regexp.MustCompile(fmt.Sprintf(`(?i)\s+%s|%s|%s|%s\s*`,
		format.Formats[format.CSVWITHNAMES],
		format.Formats[format.CSV],
		format.Formats[format.VALUES],
		format.Formats[format.JSON],
	))
)

func ParseInsertQuery(query string) (*InsertQuery, error) {
	arr := splitInsertRe.Split(query, 2)
	if len(arr) != 2 {
		return nil, errors.ErrorfWithCaller("cannot parse invalid insert query")
	}

	dataFmt := strings.ToUpper(strings.TrimSpace(splitInsertRe.FindString(query)))
	return &InsertQuery{
		DataFmt: dataFmt,
		Query:   strings.TrimSpace(arr[0]) + " " + dataFmt,
		Values:  strings.TrimSpace(arr[1]),
	}, nil
}
