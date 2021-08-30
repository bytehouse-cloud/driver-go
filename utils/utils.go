package utils

import (
	"strconv"
	"strings"
)

func FormatCount(count int64) string {
	s := strconv.FormatInt(count, 10)
	var b strings.Builder
	formatCountString(&b, s)
	return b.String()
}

const comma = ','

func formatCountString(b *strings.Builder, s string) {
	if len(s) > 3 {
		offset := len(s) - 3
		formatCountString(b, s[:offset])
		b.WriteByte(comma)
		b.WriteString(s[offset:])
		return
	}
	b.WriteString(s)
}

// todo: this might be slow, think of another way to "tranpose"
// TransposeMatrix row values to column values
func TransposeMatrix(table [][]interface{}) [][]interface{} {
	if len(table) == 0 {
		return [][]interface{}{}
	}

	if len(table[0]) == 0 {
		// If no columns, return empty too
		return [][]interface{}{}
	}

	columnValues := make([][]interface{}, len(table[0]))
	for col := range columnValues {
		columnValues[col] = make([]interface{}, len(table))
		for row := range columnValues[col] {
			columnValues[col][row] = table[row][col]
		}
	}

	return columnValues
}
