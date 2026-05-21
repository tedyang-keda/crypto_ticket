package timeframe

import "testing"

func TestFloorAndEndMinute(t *testing.T) {
	ts := int64(1779262261234)
	start := FloorStartMS(ts, "1m")
	if start != 1779262260000 {
		t.Fatalf("unexpected start: %d", start)
	}
	if EndMS(start, "1m") != 1779262319999 {
		t.Fatalf("unexpected end")
	}
}

func TestMonthBucket(t *testing.T) {
	ts := int64(1779262261234)
	start := FloorStartMS(ts, "1M")
	if start != 1777593600000 {
		t.Fatalf("unexpected month start: %d", start)
	}
}
