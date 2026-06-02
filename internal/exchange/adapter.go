package exchange

import (
	"context"
	"net/http"

	"crypto-ticket/internal/market"
)

type Adapter interface {
	Name() string
	MarketType() string
	RestURL() string
	WSURL() string
	FetchSymbols(ctx context.Context, client *http.Client) ([]market.SymbolInfo, error)
	BuildSubscribePayload(symbols []string, requestID int64) ([]byte, error)
	BuildUnsubscribePayload(symbols []string, requestID int64) ([]byte, error)
	ParseMessage(payload []byte) ([]market.Tick, error)
	ParseKlineMessage(payload []byte) ([]market.Bar, error)
}

type StaticStreamAdapter interface {
	StaticStreamURL(symbols []string) string
}
