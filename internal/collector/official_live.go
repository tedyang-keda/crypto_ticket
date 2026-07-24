package collector

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"crypto-ticket/internal/exchange"
	"crypto-ticket/internal/market"
	"crypto-ticket/internal/realtime"
)

type OfficialLiveSource struct {
	client   *http.Client
	fetchers map[string]exchange.RESTKlineFetcher
}

func NewOfficialLiveSource(fetchers []exchange.RESTKlineFetcher) *OfficialLiveSource {
	byExchange := make(map[string]exchange.RESTKlineFetcher, len(fetchers))
	for _, fetcher := range fetchers {
		if fetcher == nil {
			continue
		}
		name := strings.ToLower(strings.TrimSpace(fetcher.Name()))
		if _, exists := byExchange[name]; !exists {
			byExchange[name] = fetcher
		}
	}
	return &OfficialLiveSource{
		client:   &http.Client{Timeout: 10 * time.Second},
		fetchers: byExchange,
	}
}

func (s *OfficialLiveSource) LatestKline(ctx context.Context, exchangeName string, symbol string, tf string) (*market.Bar, error) {
	fetcher := s.fetchers[strings.ToLower(strings.TrimSpace(exchangeName))]
	if fetcher == nil {
		return nil, fmt.Errorf("official live kline fetcher not found for %s", exchangeName)
	}
	bars, err := fetcher.FetchKlines(ctx, s.client, exchange.KlineRequest{
		Symbol:            symbol,
		Timeframe:         tf,
		Limit:             2,
		ForwardAdjusted:   true,
		IncludeIncomplete: true,
	})
	if err != nil {
		return nil, err
	}
	if len(bars) == 0 {
		return nil, nil
	}
	latest := bars[len(bars)-1]
	return &latest, nil
}

type OfficialLivePublisher interface {
	PublishOfficialLiveKline(ctx context.Context, bar market.Bar) error
}

type OfficialLatestKlineSource interface {
	LatestKline(ctx context.Context, exchange string, symbol string, timeframe string) (*market.Bar, error)
}

type OfficialLiveRunner struct {
	hub       *realtime.Hub
	source    OfficialLatestKlineSource
	publisher OfficialLivePublisher
	interval  time.Duration
	last      map[string]string
}

func NewOfficialLiveRunner(hub *realtime.Hub, source OfficialLatestKlineSource, publisher OfficialLivePublisher, interval time.Duration) *OfficialLiveRunner {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	return &OfficialLiveRunner{hub: hub, source: source, publisher: publisher, interval: interval, last: make(map[string]string)}
}

func (r *OfficialLiveRunner) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		if err := r.poll(ctx); err != nil && ctx.Err() == nil {
			log.Printf("official live kline poll failed: %v", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (r *OfficialLiveRunner) poll(ctx context.Context) error {
	for _, subscription := range r.hub.KlineSubscriptions() {
		if subscription.Timeframe == "1m" {
			continue
		}
		bar, err := r.source.LatestKline(ctx, subscription.Exchange, subscription.Symbol, subscription.Timeframe)
		if err != nil {
			log.Printf("official live kline fetch failed exchange=%s symbol=%s timeframe=%s: %v",
				subscription.Exchange, subscription.Symbol, subscription.Timeframe, err)
			continue
		}
		if bar == nil || bar.IsFinal {
			continue
		}
		key := realtime.KlineChannel(subscription.Exchange, subscription.Symbol, subscription.Timeframe)
		fingerprint := liveBarFingerprint(*bar)
		if r.last[key] == fingerprint {
			continue
		}
		if err := r.publisher.PublishOfficialLiveKline(ctx, *bar); err != nil {
			return err
		}
		r.last[key] = fingerprint
	}
	return nil
}

func liveBarFingerprint(bar market.Bar) string {
	return fmt.Sprintf("%d|%.12g|%.12g|%.12g|%.12g|%.12g|%.12g|%t",
		bar.StartMS, bar.OpenPrice, bar.HighPrice, bar.LowPrice, bar.ClosePrice,
		bar.Volume, bar.QuoteVolume, bar.IsFinal)
}
