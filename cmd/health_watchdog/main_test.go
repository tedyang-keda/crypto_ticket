package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

func newWatchdogTest(t *testing.T) (*watchdog, *int32, func()) {
	t.Helper()
	var requests int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&requests, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":0}`))
	}))
	now := time.Date(2026, 7, 28, 1, 0, 0, 0, time.UTC)
	w := newWatchdog(config{WebhookURL: server.URL, StateFile: filepath.Join(t.TempDir(), "state.json")})
	w.now = func() time.Time { return now }
	return w, &requests, func() { server.Close() }
}

func TestWatchdogServiceFailureAndRecovery(t *testing.T) {
	w, requests, closeServer := newWatchdogTest(t)
	defer closeServer()
	ctx := context.Background()
	ready := readyResponse{StartedAtMS: w.now().UnixMilli()}
	err := context.DeadlineExceeded

	w.evaluateService(ctx, w.now(), ready, err)
	w.evaluateService(ctx, w.now(), ready, err)
	if !w.state.ServiceAlert || atomic.LoadInt32(requests) != 1 {
		t.Fatalf("expected one service alert, state=%+v requests=%d", w.state, atomic.LoadInt32(requests))
	}
	w.evaluateService(ctx, w.now(), ready, nil)
	w.evaluateService(ctx, w.now(), ready, nil)
	if w.state.ServiceAlert || atomic.LoadInt32(requests) != 2 {
		t.Fatalf("expected recovery notification, state=%+v requests=%d", w.state, atomic.LoadInt32(requests))
	}
}

func TestWatchdogRestartAndDiskThresholds(t *testing.T) {
	w, requests, closeServer := newWatchdogTest(t)
	defer closeServer()
	ctx := context.Background()
	now := w.now()
	w.state.LastRestartCount = 1
	w.state.RestartCountSet = true
	w.evaluateRestartCount(ctx, now, 4)
	if !w.state.RestartAlert || len(w.state.RestartTimesMS) != 3 {
		t.Fatalf("expected restart storm, state=%+v", w.state)
	}
	w.evaluateDisk(ctx, now, 85)
	w.evaluateDisk(ctx, now, 91)
	w.evaluateDisk(ctx, now, 84)
	w.evaluateDisk(ctx, now, 74)
	if w.state.DiskLevel != "ok" {
		t.Fatalf("expected disk recovery, state=%+v", w.state)
	}
	if got := atomic.LoadInt32(requests); got != 5 {
		t.Fatalf("expected restart plus four disk notifications, got %d", got)
	}
}

func TestWatchdogCountsFirstRestartAfterZeroBaseline(t *testing.T) {
	w, requests, closeServer := newWatchdogTest(t)
	defer closeServer()
	w.evaluateRestartCount(context.Background(), w.now(), 0)
	w.evaluateRestartCount(context.Background(), w.now(), 1)
	if len(w.state.RestartTimesMS) != 1 || atomic.LoadInt32(requests) != 0 {
		t.Fatalf("expected first restart to be recorded without storm alert, state=%+v", w.state)
	}
}

func TestWatchdogCountsManualRestartFromReadinessStartTime(t *testing.T) {
	w, _, closeServer := newWatchdogTest(t)
	defer closeServer()
	ctx := context.Background()
	now := w.now()
	w.evaluateRestarts(ctx, now, readyResponse{StartedAtMS: 100}, false)
	w.evaluateRestarts(ctx, now, readyResponse{StartedAtMS: 200}, false)
	if len(w.state.RestartTimesMS) != 1 || w.state.LastStartedAtMS != 200 {
		t.Fatalf("expected readiness start time restart to be recorded, state=%+v", w.state)
	}
}

func TestWatchdogStateRoundTrip(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "nested", "state.json")
	w := newWatchdog(config{StateFile: stateFile})
	w.state = state{ConsecutiveFailures: 2, ServiceAlert: true, LastStartedAtMS: 123, DiskLevel: "warning"}
	if err := w.saveState(); err != nil {
		t.Fatal(err)
	}
	loaded := newWatchdog(config{StateFile: stateFile})
	if err := loaded.loadState(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(loaded.state, w.state) {
		t.Fatalf("state mismatch after round trip: got=%+v want=%+v", loaded.state, w.state)
	}
	if _, err := os.Stat(stateFile); err != nil {
		t.Fatal(err)
	}
}

func TestWatchdogTestNotification(t *testing.T) {
	w, requests, closeServer := newWatchdogTest(t)
	defer closeServer()
	if err := w.sendTestNotification(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(requests); got != 1 {
		t.Fatalf("expected one test notification, got %d", got)
	}
}
