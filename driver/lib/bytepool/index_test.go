package bytepool

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetMaxIdx(t *testing.T) {
	tests := []struct {
		name     string
		input    int
		expected int
	}{
		{
			name:     "given 0 then 0",
			input:    0,
			expected: 0,
		},
		{
			name:     "given 1 then 1",
			input:    1,
			expected: 1,
		},
		{
			name:     "given 2 then 2",
			input:    2,
			expected: 2,
		},
		{
			name:     "given 3 then 2",
			input:    3,
			expected: 2,
		},
		{
			name:     "given 4 then 3",
			input:    4,
			expected: 3,
		},
		{
			name:     "given 5 then 3",
			input:    5,
			expected: 3,
		},
		{
			name:     "given 7 then 3",
			input:    7,
			expected: 3,
		},
		{
			name:     "given 8 then 4",
			input:    8,
			expected: 4,
		},
		{
			name:     "given 9 then 5",
			input:    9,
			expected: 4,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := getMaxIdx(test.input)
			assert.Equal(t, test.expected, actual)
		})
	}
}

func TestGetMinIdx(t *testing.T) {
	tests := []struct {
		name     string
		input    int
		expected int
	}{
		{
			name:     "given 0 then 0",
			input:    0,
			expected: 0,
		},
		{
			name:     "given 1 then 1",
			input:    1,
			expected: 1,
		},
		{
			name:     "given 2 then 2",
			input:    2,
			expected: 2,
		},
		{
			name:     "given 3 then 3",
			input:    3,
			expected: 3,
		},
		{
			name:     "given 4 then 3",
			input:    4,
			expected: 3,
		},
		{
			name:     "given 5 then 4",
			input:    5,
			expected: 4,
		},
		{
			name:     "given 7 then 4",
			input:    7,
			expected: 4,
		},
		{
			name:     "given 8 then 4",
			input:    8,
			expected: 4,
		},
		{
			name:     "given 9 then 5",
			input:    9,
			expected: 5,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := getMinIdx(test.input)
			assert.Equal(t, test.expected, actual)
		})
	}
}
