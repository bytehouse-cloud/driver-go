package column

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestInterpretTimeFormat(t *testing.T) {
	defaultDate, _ := time.Parse(defaultDateFormat, defaultDateFormat)
	defaultDateTime, _ := time.Parse(defaultDateTimeFormat, defaultDateTimeFormat)
	timeFromUnix := time.Unix(32154, 0)

	tests := []struct {
		name         string
		givenFormat  []string
		givenTexts   []string
		givenArg     string
		expectedTime time.Time
	}{
		{
			name:         "given no text then default format",
			givenFormat:  []string{defaultDateFormat},
			givenTexts:   nil,
			givenArg:     defaultDateFormat,
			expectedTime: defaultDate,
		},
		{
			name:        "given values[0] is a valid yyyyMMdd format then parse with that format",
			givenFormat: []string{defaultDateFormat},
			givenTexts:  []string{"20220815"},
			givenArg:    "20220815",
			expectedTime: func() time.Time {
				result, _ := time.Parse("20060102", "20220815")
				return result
			}(),
		},
		{
			name:        "given values[0] is a valid ddMMyyyy format then parse with that format",
			givenFormat: []string{defaultDateFormat},
			givenTexts:  []string{"15082022"},
			givenArg:    "15082022",
			expectedTime: func() time.Time {
				result, _ := time.Parse("02012006", "15082022")
				return result
			}(),
		},
		{
			name:         "given valid format values[0] then default format",
			givenFormat:  []string{defaultDateFormat},
			givenTexts:   []string{defaultDateFormat},
			givenArg:     defaultDateFormat,
			expectedTime: defaultDate,
		},
		{
			name:         "given multiple format then matching format",
			givenFormat:  []string{defaultDateFormat, defaultDateTimeFormat},
			givenTexts:   []string{defaultDateFormat},
			givenArg:     defaultDateTimeFormat,
			expectedTime: defaultDateTime,
		},
		{
			name:         "given invalid format and invalid unix then default format",
			givenFormat:  []string{defaultDateFormat},
			givenTexts:   []string{"atstrutsOEc"},
			givenArg:     defaultDateFormat,
			expectedTime: defaultDate,
		},
		{
			name:         "given invalid format but valid unix then unix format",
			givenFormat:  []string{defaultDateFormat},
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

func TestParseUnixTimeStampString(t *testing.T) {
	tests := []struct {
		name         string
		givenText    string
		expectedTime time.Time
		err          bool
	}{
		{
			name:         "given empty string then default time",
			givenText:    "",
			expectedTime: time.Time{},
			err:          true,
		},
		{
			name:         "given invalid string then default time",
			givenText:    "atstrutsOEc",
			expectedTime: time.Time{},
			err:          true,
		},
		{
			name:         "given valid string then unix time",
			givenText:    "32154",
			expectedTime: time.Unix(32154, 0),
		},
		{
			name:         "given valid string then unix time2",
			givenText:    "1546300800",
			expectedTime: time.Unix(1546300800, 0),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseUnixTimeStampString(tt.givenText)
			assert.Equal(t, tt.expectedTime, got)
			fmt.Println(got)
			assert.Equal(t, tt.err, err != nil)
		})
	}
}

func TestTimeToDecimalConversionGivenScaleHigherThanValue(t *testing.T) {

	text := "23:59:59.12"
	scale := 9
	d, err := parseDecimalTimeFromString(text, scale)
	if err != nil {
		t.Fatal(err)
	}

	timeValue := getTimeFromDecimal(d)

	formattedTime := timeValue.Format(GetTimeFormat(scale))
	assert.Equal(t, "23:59:59.120000000", formattedTime)
}

func TestTimeToDecimalConversionGivenScaleLowerThanValue(t *testing.T) {

	text := "23:59:59.1234567899"
	scale := 9
	d, err := parseDecimalTimeFromString(text, scale)
	if err != nil {
		t.Fatal(err)
	}

	timeValue := getTimeFromDecimal(d)

	formattedTime := timeValue.Format(GetTimeFormat(scale))
	assert.Equal(t, "23:59:59.123456789", formattedTime)
}

func TestTimeToDecimalConversionGivenScaleEqualToValue(t *testing.T) {

	text := "23:59:59.123456789"
	scale := 9
	d, err := parseDecimalTimeFromString(text, scale)
	if err != nil {
		t.Fatal(err)
	}

	timeValue := getTimeFromDecimal(d)

	formattedTime := timeValue.Format(GetTimeFormat(scale))
	assert.Equal(t, "23:59:59.123456789", formattedTime)
}
