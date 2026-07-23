package exchange

import (
	"testing"
	"time"
)

func TestParseOKXDualRebaseAnnouncement(t *testing.T) {
	ann := okxAnnouncement{
		Title: "OKX to execute rebase on OPENAIUSDT and ANTHROPICUSDT pre-market perpetual futures",
		URL:   "https://www.okx.com/help/openai-anthropic-rebase",
		PTime: "1782750000000",
	}
	body := `<table><tr><td>OPENAIUSDT</td><td>10</td><td>07:00 (UTC), June 30, 2026</td></tr>
		<tr><td>ANTHROPICUSDT</td><td>10</td><td>08:06 (UTC), June 30, 2026</td></tr></table>`
	actions := parseOKXCorporateActions(ann, body)
	if len(actions) != 2 {
		t.Fatalf("expected two actions, got %+v", actions)
	}
	wanted := map[string]int64{
		"OPENAI-USDT-SWAP":    mustUTCMS(t, "2026-06-30 07:00:00"),
		"ANTHROPIC-USDT-SWAP": mustUTCMS(t, "2026-06-30 08:06:00"),
	}
	for _, action := range actions {
		if action.Ratio != 10 || action.WindowStartMS != wanted[action.Symbol]-time.Hour.Milliseconds() {
			t.Fatalf("unexpected action: %+v", action)
		}
	}
}

func TestParseOKXRenameRebaseAnnouncement(t *testing.T) {
	if times := parseOKXAnnouncementTimes("07:10:00 (UTC) on June 2, 2026"); len(times) != 1 {
		t.Fatalf("expected one parsed OKX UTC time, got %v", times)
	}
	ann := okxAnnouncement{
		Title: "OKX to execute rebase on SPACEXUSDT pre-market perpetual futures and rename it to SPCXUSDT",
		URL:   "https://www.okx.com/help/spacex-rebase-and-rename",
		PTime: "1780319400000",
	}
	body := `<p>OKX will execute a rebase on SPACEXUSDT at 07:10:00 (UTC) on June 2, 2026.</p>
		<table><tr><td>Adjustment ratio to_ratio (actual / estimated)</td><td>12.52</td></tr></table>`
	actions := parseOKXCorporateActions(ann, body)
	if len(actions) != 1 {
		t.Fatalf("expected one canonical action, got %+v", actions)
	}
	action := actions[0]
	if action.Symbol != "SPCX-USDT-SWAP" || action.PredecessorSymbol != "SPACEX-USDT-SWAP" || action.Ratio != 12.52 {
		t.Fatalf("unexpected rename action: %+v", action)
	}
	if action.WindowStartMS != mustUTCMS(t, "2026-06-02 06:10:00") {
		t.Fatalf("unexpected rename window: %+v", action)
	}
}

func TestOKXListingIsNotCorporateAction(t *testing.T) {
	title := "OKX to list ZHIPUUSDT pre-market perpetual futures"
	if isCorporateActionTitle(title) {
		t.Fatalf("listing must not be classified as a corporate action")
	}
}

func mustUTCMS(t *testing.T, value string) int64 {
	t.Helper()
	parsed, err := time.ParseInLocation("2006-01-02 15:04:05", value, time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	return parsed.UnixMilli()
}
