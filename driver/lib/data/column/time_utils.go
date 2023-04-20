package column

import (
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"
)

func interpretTimeFormat(formats []string, texts []string, location *time.Location) func(string) (time.Time, error) {
	if len(texts) == 0 {
		return makeParseTimeFormat(formats, location)
	}

	for _, numericalDateFormat := range supportedNumericalDateFormats {
		if _, err := time.Parse(numericalDateFormat, texts[0]); err == nil {
			return makeParseTimeFormat([]string{numericalDateFormat}, location)
		}
	}

	if _, err := strconv.ParseInt(texts[0], 10, 64); err == nil {
		return parseUnixTimeStampString
	}

	return makeParseTimeFormat(formats, location)
}

func parseDateTime64Format(formats []string, text string, precision int, location *time.Location) (time.Time, error) {
	if timestamp, err := strconv.ParseInt(text, 10, 64); err == nil {
		return parseIntTimeStampWithPrecision(timestamp, precision)
	}

	if timestamp, err := strconv.ParseFloat(text, 64); err == nil {
		return parseFloatTimeStamp(timestamp, precision)
	}

	parseFunc := makeParseTimeFormat(formats, location)
	return parseFunc(text)
}

func makeParseTimeFormat(formats []string, location *time.Location) func(string) (time.Time, error) {
	if location == nil {
		location = time.Local
	}

	var once sync.Once
	var chosenFormat string

	return func(s string) (time.Time, error) {
		s = processString(s)

		var fmtErr error
		once.Do(func() {
			for _, f := range formats {
				if _, err := time.ParseInLocation(f, s, location); err == nil {
					chosenFormat = f
					return
				}
				if _, err := time.ParseInLocation(s, s, location); err == nil {
					chosenFormat = s
					return
				}
			}
			fmtErr = makeInvalidTimeFormatError(formats, s)
		})
		if fmtErr != nil {
			return time.Time{}, fmtErr
		}

		return time.ParseInLocation(chosenFormat, s, location)
	}
}

func makeInvalidTimeFormatError(formats []string, given string) error {
	return fmt.Errorf("invalid time format, expected = one of %v, got = %v", formats, given)
}

func parseUnixTimeStampString(s string) (time.Time, error) {
	ts, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(ts, 0), nil
}

func parseIntTimeStampWithPrecision(ts int64, precision int) (time.Time, error) {
	seconds := ts / int64(math.Pow10(precision))
	nanoseconds := (ts - seconds*int64(math.Pow10(precision))) * int64(math.Pow10(9-precision))
	return time.Unix(seconds, nanoseconds), nil
}

func parseFloatTimeStamp(ts float64, precision int) (time.Time, error) {
	sec, dec := math.Modf(ts)
	dec = math.Round(dec*math.Pow10(precision)) / math.Pow10(precision)
	return time.Unix(int64(sec), int64(dec*(1e9))), nil
}
