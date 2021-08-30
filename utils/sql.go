package utils

import (
	"bytes"
)

// NumArgs returns the number of arguments in a sql query
// Arguments are denoted by '?'
func NumArgs(query string) int {
	var (
		count         int
		args          = make(map[string]struct{})
		reader        = bytes.NewReader([]byte(query))
		quote, gravis bool
		keyword       bool
		inBetween     bool
		like          = NewMatcher("like")
		limit         = NewMatcher("limit")
		between       = NewMatcher("between")
		and           = NewMatcher("and")
	)
	for {
		if char, _, err := reader.ReadRune(); err == nil {
			switch char {
			case '\'':
				if !gravis {
					quote = !quote
				}
			case '`':
				if !quote {
					gravis = !gravis
				}
			}
			if quote || gravis {
				continue
			}
			switch {
			case char == '?' && keyword:
				count++
			case char == '@':
				if param := ParamParser(reader); len(param) != 0 {
					if _, found := args[param]; !found {
						args[param] = struct{}{}
						count++
					}
				}
			case
				char == '=',
				char == '<',
				char == '>',
				char == '(',
				char == ',',
				char == '[':
				keyword = true
			default:
				if limit.MatchRune(char) || like.MatchRune(char) {
					keyword = true
				} else if between.MatchRune(char) {
					keyword = true
					inBetween = true
				} else if inBetween && and.MatchRune(char) {
					keyword = true
					inBetween = false
				} else {
					keyword = keyword && (char == ' ' || char == '\t' || char == '\n')
				}
			}
		} else {
			break
		}
	}
	return count
}

func ParamParser(reader *bytes.Reader) string {
	var name bytes.Buffer
	for {
		if char, _, err := reader.ReadRune(); err == nil {
			if char == '_' || char >= '0' && char <= '9' || 'a' <= char && char <= 'z' || 'A' <= char && char <= 'Z' {
				name.WriteRune(char)
			} else {
				_ = reader.UnreadRune()
				break
			}
		} else {
			break
		}
	}
	return name.String()
}

func MakeColumnValues(nColumns int, blockSize int) [][]interface{} {
	columnValues := make([][]interface{}, nColumns)
	for i := 0; i < nColumns; i++ {
		rowValues := make([]interface{}, 0, blockSize)
		columnValues[i] = rowValues
	}
	return columnValues
}
