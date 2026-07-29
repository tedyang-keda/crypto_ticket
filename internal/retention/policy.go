package retention

import (
	"time"

	"crypto-ticket/internal/timeframe"
)

type Rule struct {
	Timeframe   string
	KeepDays    int
	KeepBars    int
	KeepForever bool
}

func RuleFor(tf string) Rule {
	tf = timeframe.MustNormalize(tf)
	switch tf {
	case "1m":
		return Rule{Timeframe: tf, KeepDays: 7}
	case "5m", "15m", "30m":
		return Rule{Timeframe: tf, KeepDays: 30}
	case "1H", "2H", "4H", "6H", "12H":
		return Rule{Timeframe: tf, KeepDays: 180}
	default:
		return Rule{Timeframe: tf, KeepBars: 300}
	}
}

func DefaultRules() []Rule {
	rules := make([]Rule, 0, len(timeframe.Order))
	for _, tf := range timeframe.Order {
		rules = append(rules, RuleFor(tf))
	}
	return rules
}

func CutoffMS(rule Rule, now time.Time) (int64, bool) {
	if rule.KeepForever || rule.KeepBars > 0 || rule.KeepDays <= 0 {
		return 0, false
	}
	cutoff := now.UTC().AddDate(0, 0, -rule.KeepDays)
	return cutoff.UnixMilli(), true
}

// BarWindowStartMS returns the bucket start for the oldest of the latest
// keepBars completed buckets ending at endMS. It is calendar-aware for day,
// week, and month timeframes.
func BarWindowStartMS(tf string, endMS int64, keepBars int) int64 {
	if keepBars <= 0 {
		return 0
	}
	start := timeframe.FloorStartMS(endMS, tf)
	if timeframe.EndMS(start, tf) > endMS {
		start = timeframe.PreviousStartMS(start, tf)
	}
	for i := 1; i < keepBars; i++ {
		start = timeframe.PreviousStartMS(start, tf)
	}
	return start
}
