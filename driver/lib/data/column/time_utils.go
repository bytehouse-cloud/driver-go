package column

import (
	"strconv"
	"time"

	"github.com/bytehouse-cloud/driver-go/errors"
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
	return func(s string) (time.Time, error) {
		s = processString(s)

		for _, f := range formats {
			if len(s) == len(f) {
				return time.ParseInLocation(f, s, location)
			}
		}

		return time.Time{}, errors.ErrorfWithCaller("invalid time format, expected = one of %v, got = %v", formats, s)
	}
}

func parseUnixTimeStampString(s string) (time.Time, error) {
	ts, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(0, ts), nil
}
