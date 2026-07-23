package corpaction

import (
	"testing"
	"time"
)

func testRegistry() *Registry {
	return NewRegistry(Config{
		PendingTTL:    30 * time.Minute,
		ResolvedTTL:   26 * time.Hour,
		NeutralizePct: 0.15,
	})
}

func TestAssessBarNoWindow(t *testing.T) {
	r := testRegistry()
	liveRaw, neutralize := r.AssessBar("okx", "MUU-USDT-SWAP", 1000, 1999, 3, now())
	if liveRaw || neutralize {
		t.Fatalf("no window should be inert, got liveRaw=%v neutralize=%v", liveRaw, neutralize)
	}
}

func TestAssessBarPendingMagnitude(t *testing.T) {
	r := testRegistry()
	nowMS := now()
	r.MarkActive("okx", "MUU-USDT-SWAP", nowMS)

	// Corporate-action-scale move: flagged and neutralized.
	liveRaw, neutralize := r.AssessBar("okx", "MUU-USDT-SWAP", nowMS, nowMS+59_999, -95, nowMS)
	if !liveRaw || !neutralize {
		t.Fatalf("large pending move should neutralize, got liveRaw=%v neutralize=%v", liveRaw, neutralize)
	}

	// Ordinary move during the pending window: flagged live_raw but not zeroed.
	liveRaw, neutralize = r.AssessBar("okx", "MUU-USDT-SWAP", nowMS, nowMS+59_999, 2, nowMS)
	if !liveRaw || neutralize {
		t.Fatalf("small pending move should flag but not neutralize, got liveRaw=%v neutralize=%v", liveRaw, neutralize)
	}
}

func TestAssessBarPendingExpiry(t *testing.T) {
	r := testRegistry()
	openedMS := now()
	r.MarkActive("okx", "MUU-USDT-SWAP", openedMS)
	future := openedMS + (31 * time.Minute).Milliseconds()
	liveRaw, neutralize := r.AssessBar("okx", "MUU-USDT-SWAP", openedMS, openedMS+59_999, -95, future)
	if liveRaw || neutralize {
		t.Fatalf("expired pending window should be inert, got liveRaw=%v neutralize=%v", liveRaw, neutralize)
	}
	if r.Active("okx", "MUU-USDT-SWAP", future) {
		t.Fatal("expired window should be pruned")
	}
}

func TestTouchExtendsPendingWindow(t *testing.T) {
	r := testRegistry()
	openedMS := now()
	r.MarkActive("binance", "KORUUSDT", openedMS)
	r.Touch("binance", "KORUUSDT", openedMS+(29*time.Minute).Milliseconds())
	future := openedMS + (31 * time.Minute).Milliseconds()
	if !r.Active("binance", "KORUUSDT", future) {
		t.Fatal("touched lifecycle should remain active across a long halt")
	}
}

func TestAssessBarResolvedCrossing(t *testing.T) {
	r := testRegistry()
	nowMS := now()
	boundary := nowMS
	r.MarkActive("okx", "MUU-USDT-SWAP", nowMS-1000)
	r.Resolve("okx", "MUU-USDT-SWAP", boundary)

	// Bar whose span contains the boundary is the crossing bar.
	liveRaw, neutralize := r.AssessBar("okx", "MUU-USDT-SWAP", boundary, boundary+59_999, -95, nowMS)
	if !liveRaw || !neutralize {
		t.Fatalf("crossing bar should neutralize, got liveRaw=%v neutralize=%v", liveRaw, neutralize)
	}

	// A later bar (both closes post-boundary) is normal, even with a large move.
	liveRaw, neutralize = r.AssessBar("okx", "MUU-USDT-SWAP", boundary+60_000, boundary+119_999, 40, nowMS)
	if liveRaw || neutralize {
		t.Fatalf("post-boundary bar should be normal, got liveRaw=%v neutralize=%v", liveRaw, neutralize)
	}

	// An earlier bar (both closes pre-boundary) is also normal.
	liveRaw, neutralize = r.AssessBar("okx", "MUU-USDT-SWAP", boundary-120_000, boundary-60_001, 40, nowMS)
	if liveRaw || neutralize {
		t.Fatalf("pre-boundary bar should be normal, got liveRaw=%v neutralize=%v", liveRaw, neutralize)
	}
}

func TestAssessBarResolvedExpiry(t *testing.T) {
	r := testRegistry()
	boundary := now()
	r.Resolve("okx", "MUU-USDT-SWAP", boundary)
	future := boundary + (27 * time.Hour).Milliseconds()
	liveRaw, neutralize := r.AssessBar("okx", "MUU-USDT-SWAP", boundary, boundary+59_999, -95, future)
	if liveRaw || neutralize {
		t.Fatalf("expired resolved window should be inert, got liveRaw=%v neutralize=%v", liveRaw, neutralize)
	}
}

func now() int64 {
	return time.Now().UnixMilli()
}
