package timeframe

import (
	"fmt"
	"time"
)

const (
	MinuteMS = int64(time.Minute / time.Millisecond)
	HourMS   = int64(time.Hour / time.Millisecond)
	DayMS    = int64(24 * time.Hour / time.Millisecond)
)

var Order = []string{
	"1m", "5m", "15m", "30m",
	"1H", "2H", "4H", "6H", "12H",
	"1D", "2D", "3D", "5D",
	"1W", "2W", "1M", "3M",
}

var fixedMinuteFrames = map[string]int64{
	"1m":  1,
	"5m":  5,
	"15m": 15,
	"30m": 30,
	"1H":  60,
	"2H":  120,
	"4H":  240,
	"6H":  360,
	"12H": 720,
}

var dayFrames = map[string]int64{
	"1D": 1,
	"2D": 2,
	"3D": 3,
	"5D": 5,
}

var weekFrames = map[string]int64{
	"1W": 1,
	"2W": 2,
}

var monthFrames = map[string]int{
	"1M": 1,
	"3M": 3,
}

var index = func() map[string]int {
	out := make(map[string]int, len(Order))
	for i, tf := range Order {
		out[tf] = i
	}
	return out
}()

func Normalize(tf string) (string, error) {
	if _, ok := index[tf]; !ok {
		return "", fmt.Errorf("unsupported timeframe: %s", tf)
	}
	return tf, nil
}

func MustNormalize(tf string) string {
	normalized, err := Normalize(tf)
	if err != nil {
		panic(err)
	}
	return normalized
}

func Index(tf string) int {
	return index[MustNormalize(tf)]
}

func FloorStartMS(tsMS int64, tf string) int64 {
	tf = MustNormalize(tf)
	if minutes, ok := fixedMinuteFrames[tf]; ok {
		duration := minutes * MinuteMS
		return (tsMS / duration) * duration
	}
	dt := time.UnixMilli(tsMS).UTC()
	if days, ok := dayFrames[tf]; ok {
		dayStart := time.Date(dt.Year(), dt.Month(), dt.Day(), 0, 0, 0, 0, time.UTC).UnixMilli()
		duration := days * DayMS
		return (dayStart / duration) * duration
	}
	if weeks, ok := weekFrames[tf]; ok {
		monday := time.Date(dt.Year(), dt.Month(), dt.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -weekdayOffset(dt))
		epochMonday := time.Date(1970, 1, 5, 0, 0, 0, 0, time.UTC)
		baseDays := int64(monday.Sub(epochMonday).Hours() / 24)
		periodDays := weeks * 7
		bucketDays := (baseDays / periodDays) * periodDays
		return epochMonday.AddDate(0, 0, int(bucketDays)).UnixMilli()
	}
	if months, ok := monthFrames[tf]; ok {
		monthIndex := dt.Year()*12 + int(dt.Month()) - 1
		bucketIndex := (monthIndex / months) * months
		year := bucketIndex / 12
		month := time.Month(bucketIndex%12 + 1)
		return time.Date(year, month, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	}
	panic("unreachable timeframe")
}

func NextStartMS(startMS int64, tf string) int64 {
	tf = MustNormalize(tf)
	if minutes, ok := fixedMinuteFrames[tf]; ok {
		return startMS + minutes*MinuteMS
	}
	dt := time.UnixMilli(startMS).UTC()
	if days, ok := dayFrames[tf]; ok {
		return dt.AddDate(0, 0, int(days)).UnixMilli()
	}
	if weeks, ok := weekFrames[tf]; ok {
		return dt.AddDate(0, 0, int(weeks*7)).UnixMilli()
	}
	if months, ok := monthFrames[tf]; ok {
		return dt.AddDate(0, months, 0).UnixMilli()
	}
	panic("unreachable timeframe")
}

func EndMS(startMS int64, tf string) int64 {
	return NextStartMS(startMS, tf) - 1
}

func DurationMS(tf string) int64 {
	tf = MustNormalize(tf)
	if minutes, ok := fixedMinuteFrames[tf]; ok {
		return minutes * MinuteMS
	}
	start := FloorStartMS(time.Now().UnixMilli(), tf)
	return NextStartMS(start, tf) - start
}

func weekdayOffset(dt time.Time) int {
	weekday := int(dt.Weekday())
	if weekday == 0 {
		return 6
	}
	return weekday - 1
}
