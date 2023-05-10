package column

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/shopspring/decimal"
)

const TimeFormat = "15:04:05."

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

func getDecimalFromTime(t time.Time, scale int) (decimal.Decimal, error) {
	decimalValue := decimal.NewFromInt(int64(t.Hour()*3600 + t.Minute()*60 + t.Second()))
	if scale > 0 {
		ms := decimal.NewFromInt(int64(t.Nanosecond())).Div(decimal.NewFromInt(int64(time.Second)))
		decimalValue = decimalValue.Add(ms)

	}

	return decimalValue, nil
}

func getTimeFromDecimal(decimalValue decimal.Decimal) time.Time {

	// input = 15:04:05.3

	fraction := decimalValue.Sub(decimal.NewFromInt(decimalValue.IntPart()))
	value := decimalValue.IntPart()

	hour := value / 3600
	minute := value % 3600 / 60
	seconds := value % 60

	nano := fraction.Mul(decimal.NewFromInt(int64(time.Second))).IntPart()
	t := time.Date(0, 0, 0, int(hour), int(minute), int(seconds), int(nano), time.UTC)
	return t
}

func parseDecimalTimeFromString(valueString string, scale int) (decimal.Decimal, error) {
	valueString = processString(valueString)
	components := strings.Split(valueString, ".")
	if len(components) > 2 {
		return decimal.Zero, fmt.Errorf("incorrect format for time, expecting hh:mm:ss.SSSSSS")
	}

	values := strings.Split(components[0], ":")

	if len(values) != 3 {
		return decimal.Zero, fmt.Errorf("unexpected format for time receive, expecting hh:mm:ss but got %s", string(valueString))
	}

	hour, err := strconv.ParseInt(values[0], 10, 64)
	if err != nil || hour >= 24 { // match CNCH which checks if hour >= 24, MySQL can support up to 838
		return decimal.Zero, fmt.Errorf("failed to parse hours value %s err=%s", values[0], err)
	}
	minute, err := strconv.ParseInt(values[1], 10, 64)
	if err != nil || minute >= 60 {
		return decimal.Zero, fmt.Errorf("failed to parse minutes value %s err=%s", values[1], err)
	}
	second, err := strconv.ParseInt(values[2], 10, 64)
	if err != nil || second >= 60 {
		return decimal.Zero, fmt.Errorf("failed to parse seconds value %s err=%s", values[2], err)
	}

	value := decimal.NewFromInt(hour*3600 + minute*60 + second)

	if len(components) > 1 {
		if len(components[1]) > scale { // trim excess
			components[1] = components[1][:scale]
		} else if len(components[1]) < scale {
			components[1] = padRight(components[1], scale-len(components[1]), '0')
		}
		if fraction, err := strconv.ParseInt(components[1], 10, 64); err != nil {
			return decimal.Zero, err
		} else {
			value = value.Add(decimal.NewFromInt(fraction).Div(decimal.NewFromFloat(math.Pow10(scale))))
		}
	}

	return value, nil
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

func padRight(s string, count int, paddingChar rune) string {
	var sb strings.Builder
	sb.WriteString(s)
	for i := 0; i < count; i++ {
		sb.WriteRune(paddingChar)
	}
	return sb.String()
}

func GetTimeFormat(scale int) string {
	return padRight(TimeFormat, scale, '0')
}
