package guardian

import (
	"net/http"
)

type guardianExchangeContextKey struct{}

// requestPacingTransport applies the exchange-wide rate limit at the actual
// HTTP boundary, including pagination, retries, redirects, and fallback calls
// performed inside a Fetcher.
type requestPacingTransport struct {
	base   http.RoundTripper
	pacers map[string]*requestPacer
}

func (t requestPacingTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if exchangeName, ok := request.Context().Value(guardianExchangeContextKey{}).(string); ok {
		if pacer := t.pacers[normalizeExchange(exchangeName)]; pacer != nil {
			if err := pacer.Wait(request.Context()); err != nil {
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
