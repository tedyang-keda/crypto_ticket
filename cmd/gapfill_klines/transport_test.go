package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func transportTestRequest(ctx context.Context) *http.Request {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://example.invalid/klines?secret=hidden", nil)
	if err != nil {
		panic(err)
	}
	return request
}

func transportTestResponse(status int, retryAfter string) *http.Response {
	response := &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(""))}
	if retryAfter != "" {
		response.Header.Set("Retry-After", retryAfter)
	}
	return response
}

func TestPacedTransportConcurrentStartsAreSpacedWhileRequestsRemainInFlight(t *testing.T) {
	const requestCount = 4
	interval := 15 * time.Millisecond
	starts := make(chan time.Time, requestCount)
	release := make(chan struct{})
	transport := newPacedTransport(roundTripFunc(func(*http.Request) (*http.Response, error) {
		starts <- time.Now()
		<-release
		return transportTestResponse(http.StatusOK, ""), nil
	}), interval)

	var workers sync.WaitGroup
	for i := 0; i < requestCount; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			response, err := transport.RoundTrip(transportTestRequest(context.Background()))
			if err != nil {
				t.Errorf("RoundTrip: %v", err)
				return
			}
			response.Body.Close()
		}()
	}
	defer func() { close(release); workers.Wait() }()

	var previous time.Time
	for i := 0; i < requestCount; i++ {
		select {
		case started := <-starts:
			if !previous.IsZero() && started.Sub(previous) < interval-2*time.Millisecond {
				t.Errorf("start spacing %s is less than interval %s", started.Sub(previous), interval)
			}
			previous = started
		case <-time.After(time.Second):
			t.Fatal("request did not start while earlier requests were still in flight")
		}
	}
	if stats := transport.Stats(); stats.Requests != requestCount || stats.Limited != 0 || stats.Failures != 0 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestPacedTransportCancellationDoesNotCountOrReserveAnAttempt(t *testing.T) {
	transport := newPacedTransport(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return transportTestResponse(http.StatusOK, ""), nil
	}), 30*time.Millisecond)
	response, err := transport.RoundTrip(transportTestRequest(context.Background()))
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Millisecond)
	defer cancel()
	if _, err := transport.RoundTrip(transportTestRequest(ctx)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want context deadline", err)
	}
	if stats := transport.Stats(); stats.Requests != 1 || stats.Failures != 0 {
		t.Fatalf("waiting cancellation was counted: %+v", stats)
	}
	response, err = transport.RoundTrip(transportTestRequest(context.Background()))
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if stats := transport.Stats(); stats.Requests != 2 {
		t.Fatalf("unexpected stats: %+v", stats)
	}

	canceled, stop := context.WithCancel(context.Background())
	stop()
	if _, err := transport.RoundTrip(transportTestRequest(canceled)); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context cancellation", err)
	}
	if stats := transport.Stats(); stats.Requests != 2 {
		t.Fatalf("pre-canceled request was counted: %+v", stats)
	}
}

func TestPacedTransportRateLimitsShareCooldownAndStats(t *testing.T) {
	for _, test := range []struct {
		name       string
		status     int
		retryAfter string
		wantDelay  time.Duration
		limited    int64
	}{
		{name: "429 default", status: http.StatusTooManyRequests, wantDelay: 60 * time.Second, limited: 1},
		{name: "418 default", status: http.StatusTeapot, wantDelay: 120 * time.Second, limited: 1},
		{name: "retry after override", status: http.StatusTooManyRequests, retryAfter: "1", wantDelay: time.Second, limited: 1},
		{name: "503 retry after", status: http.StatusServiceUnavailable, retryAfter: "2", wantDelay: 2 * time.Second},
		{name: "5xx default", status: http.StatusBadGateway, wantDelay: time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Int64
			transport := newPacedTransport(roundTripFunc(func(*http.Request) (*http.Response, error) {
				calls.Add(1)
				return transportTestResponse(test.status, test.retryAfter), nil
			}), time.Millisecond)
			started := time.Now()
			response, err := transport.RoundTrip(transportTestRequest(context.Background()))
			if err != nil {
				t.Fatal(err)
			}
			response.Body.Close()
			transport.mu.Lock()
			delay := transport.cooldownUntil.Sub(started)
			transport.mu.Unlock()
			if delay < test.wantDelay || delay > test.wantDelay+time.Second {
				t.Errorf("cooldown delay %s, want approximately %s", delay, test.wantDelay)
			}

			var workers sync.WaitGroup
			for i := 0; i < 3; i++ {
				workers.Add(1)
				go func() {
					defer workers.Done()
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
					defer cancel()
					if _, err := transport.RoundTrip(transportTestRequest(ctx)); !errors.Is(err, context.DeadlineExceeded) {
						t.Errorf("cooldown request error = %v", err)
					}
				}()
			}
			workers.Wait()
			if calls.Load() != 1 {
				t.Errorf("cooldown allowed %d actual calls", calls.Load())
			}
			if stats := transport.Stats(); stats.Requests != 1 || stats.Failures != 1 || stats.Limited != test.limited {
				t.Fatalf("unexpected stats: %+v", stats)
			}
		})
	}
}

func TestPacedTransportWaitingCallerRechecksNewCooldown(t *testing.T) {
	var calls atomic.Int64
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	transport := newPacedTransport(roundTripFunc(func(*http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			close(firstStarted)
			<-releaseFirst
			return transportTestResponse(http.StatusTooManyRequests, "1"), nil
		}
		return transportTestResponse(http.StatusOK, ""), nil
	}), 25*time.Millisecond)
	firstDone := make(chan error, 1)
	go func() {
		response, err := transport.RoundTrip(transportTestRequest(context.Background()))
		if response != nil {
			response.Body.Close()
		}
		firstDone <- err
	}()
	<-firstStarted

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	secondDone := make(chan error, 1)
	go func() {
		_, err := transport.RoundTrip(transportTestRequest(ctx))
		secondDone <- err
	}()
	// The second caller starts with a short pacing wait. The first response
	// then imposes a longer cooldown before that original wait expires.
	time.Sleep(2 * time.Millisecond)
	close(releaseFirst)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := <-secondDone; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want context deadline", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("cooldown was bypassed: calls=%d", calls.Load())
	}
}

func TestPacedTransportConcurrentResponsesDoNotShortenCooldown(t *testing.T) {
	var calls atomic.Int64
	started := make(chan struct{}, 2)
	releases := []chan struct{}{make(chan struct{}), make(chan struct{})}
	transport := newPacedTransport(roundTripFunc(func(*http.Request) (*http.Response, error) {
		index := int(calls.Add(1)) - 1
		started <- struct{}{}
		<-releases[index]
		if index == 0 {
			return transportTestResponse(http.StatusTooManyRequests, "2"), nil
		}
		return transportTestResponse(http.StatusServiceUnavailable, "1"), nil
	}), 0)
	done := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			response, err := transport.RoundTrip(transportTestRequest(context.Background()))
			if response != nil {
				response.Body.Close()
			}
			done <- err
		}()
	}
	<-started
	<-started
	close(releases[0])
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	transport.mu.Lock()
	firstCooldown := transport.cooldownUntil
	transport.mu.Unlock()
	close(releases[1])
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	transport.mu.Lock()
	lastCooldown := transport.cooldownUntil
	transport.mu.Unlock()
	if !lastCooldown.Equal(firstCooldown) {
		t.Fatalf("shorter concurrent response changed cooldown: first=%s last=%s", firstCooldown, lastCooldown)
	}
	if stats := transport.Stats(); stats.Requests != 2 || stats.Limited != 1 || stats.Failures != 2 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestRetryAfterDelay(t *testing.T) {
	now := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		value string
		want  time.Duration
		valid bool
	}{
		{value: "", valid: false},
		{value: "invalid", valid: false},
		{value: "-1", valid: false},
		{value: "1.5", valid: false},
		{value: "0", want: 0, valid: true},
		{value: " 3 ", want: 3 * time.Second, valid: true},
		{value: now.Add(20 * time.Second).Format(http.TimeFormat), want: 20 * time.Second, valid: true},
		{value: now.Add(-20 * time.Second).Format(http.TimeFormat), want: 0, valid: true},
		{value: "9223372036854775807", want: time.Duration(1<<63 - 1), valid: true},
	} {
		t.Run(test.value, func(t *testing.T) {
			got, valid := retryAfterDelay(test.value, now)
			if got != test.want || valid != test.valid {
				t.Fatalf("retryAfterDelay(%q) = %s, %t; want %s, %t", test.value, got, valid, test.want, test.valid)
			}
		})
	}
}

func TestPacedTransportRetryAfterZeroDoesNotUseDefaultCooldown(t *testing.T) {
	transport := newPacedTransport(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return transportTestResponse(http.StatusTooManyRequests, "0"), nil
	}), time.Millisecond)
	for i := 0; i < 2; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		response, err := transport.RoundTrip(transportTestRequest(ctx))
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
	}
	if stats := transport.Stats(); stats.Requests != 2 || stats.Limited != 2 || stats.Failures != 2 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestPacedTransportErrorsDoNotExposeURLParameters(t *testing.T) {
	cause := errors.New("dial failed: https://example.invalid/klines?secret=hidden")
	transport := newPacedTransport(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, cause
	}), 0)
	_, err := transport.RoundTrip(transportTestRequest(context.Background()))
	if err == nil || strings.Contains(fmt.Sprint(err), "hidden") || strings.Contains(err.Error(), "?") {
		t.Fatalf("unsafe transport error: %v", err)
	}
	if !errors.Is(err, cause) {
		t.Fatal("transport error does not preserve the original cause")
	}
	if stats := transport.Stats(); stats.Requests != 1 || stats.Failures != 1 || stats.Limited != 0 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestNewPacedTransportDefaultsAndNilRequest(t *testing.T) {
	transport := newPacedTransport(nil, -time.Second)
	if transport.base != http.DefaultTransport || transport.interval != 0 {
		t.Fatal("constructor did not normalize defaults")
	}
	if _, err := transport.RoundTrip(nil); err == nil {
		t.Fatal("nil request must fail")
	}
	if stats := transport.Stats(); stats != (pacedTransportStats{}) {
		t.Fatalf("nil request was counted: %+v", stats)
	}
}
