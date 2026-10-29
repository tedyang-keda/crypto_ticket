package guardian

import (
	"context"
	"net/http"
	"time"
)

type guardianExchangeContextKey struct{}
type minuteDayAuditContextKey struct{}

// requestPacingTransport applies the exchange-wide rate limit at the actual
// HTTP boundary, including pagination, retries, redirects, and fallback calls
// performed inside a Fetcher.
type requestPacingTransport struct {
	base            http.RoundTripper
	pacers          map[string]*requestPacer
	minuteDayPacers map[string]*requestPacer
}

func (t requestPacingTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if exchangeName, ok := request.Context().Value(guardianExchangeContextKey{}).(string); ok {
		exchangeName = normalizeExchange(exchangeName)
		if pacer := t.pacers[exchangeName]; pacer != nil {
			var dayPacer *requestPacer
			if enabled, _ := request.Context().Value(minuteDayAuditContextKey{}).(bool); enabled {
				dayPacer = t.minuteDayPacers[exchangeName]
			}
			if err := waitRequestPacers(request.Context(), pacer, dayPacer); err != nil {
				return nil, err
			}
		}
	}
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	return base.RoundTrip(request)
}

// Reserve both budgets at the actual HTTP boundary. Reserving one and then
// waiting on the other would allow delayed requests to bunch up later.
func waitRequestPacers(ctx context.Context, global, background *requestPacer) error {
	if background == nil || background.interval <= 0 {
		return global.Wait(ctx)
	}
	for {
		global.mu.Lock()
		background.mu.Lock()
		now := time.Now()
		ready := global.next
		if background.next.After(ready) {
			ready = background.next
		}
		if !ready.After(now) {
			global.next = now.Add(global.interval)
			background.next = now.Add(background.interval)
			background.mu.Unlock()
			global.mu.Unlock()
			return nil
		}
		background.mu.Unlock()
		global.mu.Unlock()
		timer := time.NewTimer(ready.Sub(now))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
