package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type wmTest struct {
	haystack string
	needle   string
	expect   bool
}

func checkMatch(haystack, needle string) bool {
	m := NewMatcher(needle)
	for _, r := range []rune(haystack) {
		if m.MatchRune(r) {
			return true
		}
	}
	return false
}

func TestWordMatcher(t *testing.T) {
	table := []wmTest{
		{"select * from test", "select", true},
		{"select * from test", "*", true},
		{"select * from test", "elect", true},
		{"select * from test", "zelect", false},
		{"select * from test", "sElEct", true},
	}

	for _, test := range table {
		assert.Equal(t, checkMatch(test.haystack, test.needle), test.expect, test.haystack)
	}

}
