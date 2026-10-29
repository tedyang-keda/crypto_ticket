package monitoring

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"crypto-ticket/internal/market"
)

const (
	qualityWarningAge  = 2 * time.Minute
	qualityCriticalAge = 5 * time.Minute
	qualityDetailLimit = 20
)

type delayedSymbol struct {
	Symbol SymbolSnapshot
	Age    time.Duration
}

func (s *Service) sendMarketQualityReport(ctx context.Context) error {
	now := s.registry.now()
	body := s.formatMarketQualityReport(now, s.registry.Snapshot(now))
	if reader, ok := s.activityStore.(interface {
		MinuteAuditSummary(context.Context, int64) (market.MinuteAuditSummary, error)
	}); ok {
		queryCtx, cancel := context.WithTimeout(ctx, s.cfg.DatabaseTimeout)
		summary, err := reader.MinuteAuditSummary(queryCtx, now.UnixMilli())
		cancel()
		if err != nil {
			body += "\n**分钟校准待办**: 状态读取失败，当前覆盖情况未知"
		} else {
			body += "\n" + formatMinuteAuditSummary(summary, now.In(s.cfg.DailyLocation))
		}
	}
	return s.alerts.SendMarketQuality(ctx, body)
}

func formatMinuteAuditSummary(summary market.MinuteAuditSummary, now time.Time) string {
	line := fmt.Sprintf("**分钟校准待办**: pending=%d, running=%d, blocked=%d, expired=%d, excluded=%d",
		summary.Pending, summary.Running, summary.Blocked, summary.Expired, summary.Excluded)
	if summary.OldestStartMS > 0 {
		line += ", oldest=" + formatEventTime(now, summary.OldestStartMS)
	}
	if summary.DailyDayStartMS > 0 {
		line += fmt.Sprintf("\n**隔日分钟逐根核验**: UTC %s, completed=%d/%d",
			time.UnixMilli(summary.DailyDayStartMS).UTC().Format("2006-01-02"), summary.DailyCompleted, summary.DailyTotal)
	} else {
		line += "\n**隔日分钟逐根核验**: 尚未登记完成日"
	}
	return line
}

func (s *Service) formatMarketQualityReport(now time.Time, snapshot Snapshot) string {
	lines := []string{
		fmt.Sprintf("**时间**: `%s`", now.In(s.cfg.DailyLocation).Format(time.RFC3339)),
		"**订阅与连接**:",
	}
	for _, runtime := range snapshot.Runtimes {
		status := "离线"
		if runtime.Connected > 0 {
			status = "在线"
		}
		messageAge := ageFromMS(now, runtime.LastMessageMS)
		if runtime.Connected > 0 && messageAge >= s.cfg.WSCriticalAge {
			status = "在线但无消息"
		}
		connectRate := formatSuccessRate(runtime.ConnectSuccesses5m, runtime.ConnectAttempts5m)
		lines = append(lines, fmt.Sprintf("- `%s:%s`: subscribed=%d, connections=%d, %s, last_message=%s, connect_5m=%d/%d (%s), reconnect_5m=%d, parse_error_5m=%d, ingest_error_5m=%d",
			runtime.Exchange, runtime.MarketType, runtime.SubscribedSymbols, runtime.Connected, status, formatAge(messageAge),
			runtime.ConnectSuccesses5m, runtime.ConnectAttempts5m, connectRate, runtime.Reconnects5m, runtime.ParseErrors5m, runtime.IngestErrors5m))
	}
	lines = append(lines, formatSystemQuality(now, snapshot)...)

	delayed, continuousCount := delayedSubscribedSymbols(now, snapshot.Symbols)
	warningCount := 0
	criticalCount := 0
	for _, item := range delayed {
		if item.Age >= qualityCriticalAge {
			criticalCount++
		} else {
			warningCount++
		}
	}
	lines = append(lines,
		fmt.Sprintf("**连续行情延迟**: tracked=%d, 2m~5m=%d, >=5m=%d", continuousCount, warningCount, criticalCount))
	for index, item := range delayed {
		if index >= qualityDetailLimit {
			lines = append(lines, fmt.Sprintf("- 其余 `%d` 个延迟品种已省略", len(delayed)-qualityDetailLimit))
			break
		}
		lines = append(lines, fmt.Sprintf("- `%s:%s`, `%s`, last_final=%s, lag=%s",
			item.Symbol.Exchange, item.Symbol.MarketType, item.Symbol.Symbol,
			formatMS(item.Symbol.LastFinalMS), formatAge(item.Age)))
	}
	if len(delayed) == 0 {
		lines = append(lines, "- 无延迟品种")
	}

	audit := snapshot.Window.GuardianAudit
	if audit.CompletedAtMS > 0 {
		lines = append(lines, fmt.Sprintf("**最近一轮 Guardian**: checked_symbols=%d, checked_bars=%d, failed_symbols=%d, duration=%s",
			audit.CheckedSymbols, audit.CheckedBars, audit.FailedSymbols, audit.Duration.Round(time.Second)))
	} else {
		lines = append(lines, "**最近一轮 Guardian**: 尚未完成")
	}
	lines = append(lines, formatGuardianQuality(now.In(s.cfg.DailyLocation), snapshot.Window.GuardianEvents30m)...)
	if snapshot.Window.Integrity.CheckedAt.IsZero() {
		lines = append(lines, "**最近一轮高周期完整性检查**: 尚未完成")
	} else {
		lines = append(lines, "**最近一轮高周期完整性检查**: "+formatIntegrity(snapshot.Window.Integrity))
	}
	return strings.Join(lines, "\n")
}

func delayedSubscribedSymbols(now time.Time, symbols []SymbolSnapshot) ([]delayedSymbol, int) {
	var delayed []delayedSymbol
	continuousCount := 0
	for _, symbol := range symbols {
		if !symbol.Subscribed || !symbol.Continuous {
			continue
		}
		continuousCount++
		age := ageFromMS(now, symbol.LastFinalMS)
		if age >= qualityWarningAge {
			delayed = append(delayed, delayedSymbol{Symbol: symbol, Age: age})
		}
	}
	sort.Slice(delayed, func(i, j int) bool {
		if delayed[i].Age != delayed[j].Age {
			return delayed[i].Age > delayed[j].Age
		}
		if delayed[i].Symbol.Exchange != delayed[j].Symbol.Exchange {
			return delayed[i].Symbol.Exchange < delayed[j].Symbol.Exchange
		}
		return delayed[i].Symbol.Symbol < delayed[j].Symbol.Symbol
	})
	return delayed, continuousCount
}

func formatGuardianQuality(now time.Time, events []market.KlineGuardianEvent) []string {
	counts := make(map[string]int)
	affected := make(map[string]bool)
	relevant := make([]market.KlineGuardianEvent, 0, len(events))
	for _, event := range events {
		switch event.EventType {
		case "missing_repair", "mismatch_repair", "repair_error", "rest_error", "empty_response", "incomplete_response", "ws_final_conflict":
			counts[event.EventType]++
			relevant = append(relevant, event)
			if event.Symbol != "" {
				affected[event.Exchange+":"+event.Symbol] = true
			}
		}
	}
	sort.Slice(relevant, func(i, j int) bool { return relevant[i].CreatedAtMS > relevant[j].CreatedAtMS })
	lines := []string{fmt.Sprintf(
		"**近30分钟质量**: missing=%d, mismatch=%d, repair_failed=%d, rest_error=%d, affected=%d",
		counts["missing_repair"], counts["mismatch_repair"], counts["repair_error"], counts["rest_error"], len(affected),
	)}
	if counts["empty_response"]+counts["incomplete_response"]+counts["ws_final_conflict"] > 0 {
		lines = append(lines, fmt.Sprintf("- 待核验原因: empty_response=%d, incomplete_response=%d, ws_final_conflict=%d",
			counts["empty_response"], counts["incomplete_response"], counts["ws_final_conflict"]))
	}
	if len(relevant) == 0 {
		return append(lines, "- 无异常")
	}
	for index, event := range relevant {
		if index >= qualityDetailLimit {
			lines = append(lines, fmt.Sprintf("- 其余 `%d` 条异常已省略", len(relevant)-qualityDetailLimit))
			break
		}
		lines = append(lines, fmt.Sprintf("- `%s:%s`, `%s`, `%s`, %s",
			event.Exchange, event.Symbol, event.Timeframe, formatEventTime(now, event.StartMS), guardianEventDescription(event)))
	}
	return lines
}

func guardianEventDescription(event market.KlineGuardianEvent) string {
	switch event.EventType {
	case "empty_response":
		return "官方 REST 返回空数据，尚未完成核验"
	case "incomplete_response":
		return "官方 REST 返回不完整，尚未完成核验"
	case "ws_final_conflict":
		return "已拦截与核验值冲突的 WS final，等待官方核验"
	case "missing_repair":
		return "缺失，已修复"
	case "mismatch_repair":
		return mismatchDescription(event) + "，已修复"
	case "repair_error":
		return "修复失败"
	case "rest_error":
		return "官方 REST 请求失败"
	default:
		return event.EventType
	}
}

func mismatchDescription(event market.KlineGuardianEvent) string {
	var oldBar market.Bar
	var newBar market.Bar
	if json.Unmarshal([]byte(event.OldValueJSON), &oldBar) != nil || json.Unmarshal([]byte(event.NewValueJSON), &newBar) != nil {
		return "行情不一致"
	}
	var fields []string
	if oldBar.OpenPrice != newBar.OpenPrice || oldBar.HighPrice != newBar.HighPrice ||
		oldBar.LowPrice != newBar.LowPrice || oldBar.ClosePrice != newBar.ClosePrice {
		fields = append(fields, "价格")
	}
	if oldBar.Volume != newBar.Volume || oldBar.QuoteVolume != newBar.QuoteVolume ||
		oldBar.ContractVolume != newBar.ContractVolume || oldBar.TradeCount != newBar.TradeCount {
		fields = append(fields, "数量")
	}
	if len(fields) == 0 {
		return "行情不一致"
	}
	return strings.Join(fields, "/") + "不一致"
}

func formatEventTime(now time.Time, value int64) string {
	if value <= 0 {
		return "unknown"
	}
	return time.UnixMilli(value).In(now.Location()).Format(time.RFC3339)
}

func formatAge(age time.Duration) string {
	if age == time.Duration(math.MaxInt64) {
		return "never"
	}
	return age.Round(time.Second).String()
}
