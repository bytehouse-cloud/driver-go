package column

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestInterpretTimeFormat(t *testing.T) {
	defaultDate, _ := time.Parse(dateFormat, dateFormat)
	defaultDateTime, _ := time.Parse(dateTimeFormat, dateTimeFormat)
	timeFromUnix := time.Unix(0, 32154)

	tests := []struct {
		name         string
		givenFormat  []string
		givenTexts   []string
		givenArg     string
		expectedTime time.Time
	}{
		{
			name:         "given no text then default format",
			givenFormat:  []string{dateFormat},
			givenTexts:   nil,
			givenArg:     dateFormat,
			expectedTime: defaultDate,
		},
		{
			name:         "given valid format values[0] then default format",
			givenFormat:  []string{dateFormat},
			givenTexts:   []string{dateFormat},
			givenArg:     dateFormat,
			expectedTime: defaultDate,
		},
		{
			name:         "given multiple format then matching format",
			givenFormat:  []string{dateFormat, dateTimeFormat},
			givenTexts:   []string{dateFormat},
			givenArg:     dateTimeFormat,
			expectedTime: defaultDateTime,
		},
		{
			name:         "given invalid format and invalid unix then default format",
			givenFormat:  []string{dateFormat},
			givenTexts:   []string{"atstrutsOEc"},
			givenArg:     dateFormat,
			expectedTime: defaultDate,
		},
		{
			name:         "given invalid format but valid unix then unix format",
			givenFormat:  []string{dateFormat},
			givenTexts:   []string{"32154"},
			givenArg:     "32154",
			expectedTime: timeFromUnix,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parseFunc := interpretTimeFormat(tt.givenFormat, tt.givenTexts, time.UTC)
			got, _ := parseFunc(tt.givenArg)
			assert.Equal(t, tt.expectedTime, got)
		})
	}
}
