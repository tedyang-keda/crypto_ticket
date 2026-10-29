package monitoring

import (
	"strings"
	"testing"
	"time"

	"crypto-ticket/internal/market"
)

func TestMinuteAuditBacklogRemainsVisibleAfterRollingAuditRecovers(t *testing.T) {
	now := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	text := formatMinuteAuditSummary(market.MinuteAuditSummary{
		Pending: 2, Blocked: 1, Expired: 1, OldestStartMS: now.Add(-2 * time.Hour).UnixMilli(),
		DailyDayStartMS: now.Add(-24 * time.Hour).Truncate(24 * time.Hour).UnixMilli(), DailyCompleted: 12, DailyTotal: 13,
	}, now)
	for _, required := range []string{"pending=2", "blocked=1", "expired=1", "oldest=", "UTC 2026-10-28", "completed=12/13"} {
		if !strings.Contains(text, required) {
			t.Fatalf("missing %q in report: %s", required, text)
		}
	}
}

func TestIncompleteOfficialResponseIsNotReportedAsNoAnomalies(t *testing.T) {
	now := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	lines := formatGuardianQuality(now, []market.KlineGuardianEvent{{
		Exchange: "binance", Symbol: "BTCUSDT", Timeframe: "1m", StartMS: now.UnixMilli(), EventType: "incomplete_response",
	}})
	text := strings.Join(lines, "\n")
	if strings.Contains(text, "无异常") || !strings.Contains(text, "incomplete_response=1") || !strings.Contains(text, "尚未完成核验") {
		t.Fatalf("incomplete response was hidden: %s", text)
	}
}
