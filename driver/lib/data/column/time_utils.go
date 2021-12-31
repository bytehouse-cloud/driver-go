package column

import (
	"fmt"
	"strconv"
	"sync"
	"time"
)

func interpretTimeFormat(formats []string, texts []string, location *time.Location) func(string) (time.Time, error) {
	if len(texts) == 0 {
		return makeParseTimeFormat(formats, location)
	}

	if _, err := strconv.ParseInt(texts[0], 10, 64); err == nil {
		return parseUnixTimeStampString
	}

	return makeParseTimeFormat(formats, location)
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
	return time.Unix(0, ts), nil
}
