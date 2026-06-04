package retention

import (
	"time"

	"crypto-ticket/internal/timeframe"
)

type Rule struct {
	Timeframe   string
	KeepDays    int
	KeepForever bool
}

func RuleFor(tf string) Rule {
	tf = timeframe.MustNormalize(tf)
	switch tf {
	case "1m":
		return Rule{Timeframe: tf, KeepDays: 30}
	case "5m", "15m", "30m":
		return Rule{Timeframe: tf, KeepDays: 90}
	case "1H", "2H", "4H", "6H", "12H":
		return Rule{Timeframe: tf, KeepDays: 180}
	default:
		return Rule{Timeframe: tf, KeepForever: true}
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
	if rule.KeepForever || rule.KeepDays <= 0 {
		return 0, false
	}
	cutoff := now.UTC().AddDate(0, 0, -rule.KeepDays)
	return cutoff.UnixMilli(), true
}
