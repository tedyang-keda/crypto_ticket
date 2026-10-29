package main

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultRateLimitCooldown = 60 * time.Second
	defaultBanCooldown       = 120 * time.Second
	defaultServerCooldown    = time.Second
)

// pacedTransportStats counts actual attempts, not requests canceled while
// waiting. Limited responses are included in Failures as well.
type pacedTransportStats struct {
	Requests int64
	Limited  int64
	Failures int64
}

// pacedTransport belongs to the gapfill command's HTTP boundary. It spaces
// request starts without holding a lock during network I/O and shares server
// cooldowns among all callers using this transport (one per exchange).
type pacedTransport struct {
	base     http.RoundTripper
	interval time.Duration

	mu            sync.Mutex
	nextStart     time.Time
	cooldownUntil time.Time
	stats         pacedTransportStats
}

func newPacedTransport(base http.RoundTripper, interval time.Duration) *pacedTransport {
	if base == nil {
		base = http.DefaultTransport
	}
	if interval < 0 {
		interval = 0
	}
	return &pacedTransport{base: base, interval: interval}
}

func (t *pacedTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if request == nil {
		return nil, errors.New("invalid exchange HTTP request")
	}
	if err := t.waitForStart(request.Context()); err != nil {
		return nil, safeRequestError{cause: err}
	}

	response, err := t.base.RoundTrip(request)
	now := time.Now()
	t.mu.Lock()
	if err != nil || (response != nil && response.StatusCode >= http.StatusBadRequest) {
		t.stats.Failures++
	}
	if response != nil {
		if response.StatusCode == http.StatusTooManyRequests || response.StatusCode == http.StatusTeapot {
			t.stats.Limited++
		}
		if delay := responseCooldown(response, now); delay > 0 {
			until := now.Add(delay)
			if until.After(t.cooldownUntil) {
				t.cooldownUntil = until
			}
		}
	}
	t.mu.Unlock()
	if err != nil {
		// RoundTripper errors can contain the entire URL, including credentials
		// or query parameters. Keep the cause for errors.Is/As, not its text.
		return response, safeRequestError{cause: err}
	}
	return response, nil
}

func (t *pacedTransport) waitForStart(ctx context.Context) error {
	for {
		t.mu.Lock()
		if err := ctx.Err(); err != nil {
			t.mu.Unlock()
			return err
		}
		now := time.Now()
		readyAt := t.nextStart
		if t.cooldownUntil.After(readyAt) {
			readyAt = t.cooldownUntil
		}
		if !now.Before(readyAt) {
			t.nextStart = now.Add(t.interval)
			t.stats.Requests++
			t.mu.Unlock()
			return nil
		}
		delay := readyAt.Sub(now)
		t.mu.Unlock()

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
			// A concurrent response may have extended the cooldown after this
			// timer started, so always recheck under the lock before proceeding.
		}
	}
}

func (t *pacedTransport) Stats() pacedTransportStats {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.stats
}

func responseCooldown(response *http.Response, now time.Time) time.Duration {
	if delay, valid := retryAfterDelay(response.Header.Get("Retry-After"), now); valid {
		return delay
	}
	switch response.StatusCode {
	case http.StatusTooManyRequests:
		return defaultRateLimitCooldown
	case http.StatusTeapot:
		return defaultBanCooldown
	default:
		if response.StatusCode >= 500 && response.StatusCode <= 599 {
			return defaultServerCooldown
		}
		return 0
	}
}

func retryAfterDelay(value string, now time.Time) (time.Duration, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil && seconds >= 0 {
		const maxDuration = time.Duration(1<<63 - 1)
		if seconds > int64(maxDuration/time.Second) {
			return maxDuration, true
		}
		return time.Duration(seconds) * time.Second, true
	}
	if until, err := http.ParseTime(value); err == nil {
		if !until.After(now) {
			return 0, true
		}
		return until.Sub(now), true
	}
	return 0, false
}

type safeRequestError struct {
	cause error
}

func (e safeRequestError) Error() string {
	switch {
	case errors.Is(e.cause, context.Canceled):
		return "exchange HTTP request canceled"
	case errors.Is(e.cause, context.DeadlineExceeded):
		return "exchange HTTP request timed out"
	default:
		return "exchange HTTP request failed"
	}
}

func (e safeRequestError) Unwrap() error { return e.cause }
