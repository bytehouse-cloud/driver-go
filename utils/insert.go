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

var (
	// You can test the insert into regex here: https://regex101.com/r/8OW9OC/1
	insertIntoRe   = regexp.MustCompile("(?i)\\bINSERT\\s+INTO\\s+(((`[^`]*`)|([^\\s^\\.]*))\\.)?((`[^`]*`)|([^\\s^(]*))\\s*(\\([^)]*\\))?\\s*")
	insertFormatRe = regexp.MustCompile(fmt.Sprintf(`(?i)\s*\b(%s|%s|%s|%s)\b`,
		format.Formats[format.CSVWITHNAMES],
		format.Formats[format.CSV],
		format.Formats[format.VALUES],
		format.Formats[format.JSON],
	))
)

/*
In this method, we Parse the Insert query into dataFmt (which can be CSV, VALUES, JSON or CSVWITHNAMES),
Query (excluding the values but including the format) and the values.
This is done by first matching the first part of the insert query with our InsertInto regex: INSERT INTO [db.]table [(c1, c2, c3)]

Then we get the format from the remaining string using the insertFormat regex. This is necessary to ensure that
a table, database or column name containing the format key words isn't treated as the format.
*/
func ParseInsertQuery(query string) (*InsertQuery, error) {
	queryPart1 := strings.TrimSpace(insertIntoRe.FindString(query))
	insertIntoArr := insertIntoRe.Split(query, 2)
	if len(insertIntoArr) != 2 {
		return nil, errors.ErrorfWithCaller("cannot parse invalid insert query")
	}
	dataFmt := strings.ToUpper(strings.TrimSpace(insertFormatRe.FindString(insertIntoArr[1])))

	formatArr := insertFormatRe.Split(insertIntoArr[1], 2)
	if len(formatArr) != 2 {
		return nil, errors.ErrorfWithCaller("cannot parse invalid insert query")
	}

	var finalQuery string
	if formatArr[0] == "" {
		finalQuery = queryPart1 + " " + dataFmt
	} else {
		finalQuery = queryPart1 + " " + strings.TrimSpace(formatArr[0]) + " " + dataFmt
	}

	return &InsertQuery{
		DataFmt: dataFmt,
		Query:   finalQuery,
		Values:  strings.TrimSpace(formatArr[1]),
	}, nil
}
