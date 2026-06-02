package app

import (
	"context"
	"testing"

	"crypto-ticket/internal/market"
	"crypto-ticket/internal/realtime"
	"crypto-ticket/internal/storage"
)

func TestIngestKlineStoresFinalAndComputesDerivedFields(t *testing.T) {
	ctx := context.Background()
	service := newTestMarketService()
	base := int64(1_710_000_000_000)

	if err := service.IngestKline(ctx, market.Bar{
		Exchange: "binance", Symbol: "BTCUSDT", MarginType: "umargin", Timeframe: "1m",
		StartMS: base, EndMS: base + 59_999,
		OpenPrice: 100, HighPrice: 110, LowPrice: 95, ClosePrice: 105,
		Volume: 2, VolumeUnit: "BTC", QuoteVolume: 210, QuoteUnit: "USDT",
		IsFinal: true,
	}); err != nil {
		t.Fatalf("ingest first final: %v", err)
	}
	if err := service.IngestKline(ctx, market.Bar{
		Exchange: "binance", Symbol: "BTCUSDT", MarginType: "umargin", Timeframe: "1m",
		StartMS: base + 60_000, EndMS: base + 119_999,
		OpenPrice: 105, HighPrice: 120, LowPrice: 100, ClosePrice: 115,
		Volume: 3, VolumeUnit: "BTC", QuoteVolume: 345, QuoteUnit: "USDT",
		IsFinal: true,
	}); err != nil {
		t.Fatalf("ingest second final: %v", err)
	}

	bars, err := service.Klines(ctx, market.KlineQuery{
		Exchange: "binance", Symbol: "BTCUSDT", Timeframe: "1m", Limit: 10, IncludeLive: true,
	})
	if err != nil {
		t.Fatalf("klines: %v", err)
	}
	if len(bars) != 2 {
		t.Fatalf("expected two bars, got %+v", bars)
	}
	second := bars[1]
	if second.PrevClose != 105 || second.Chg != 9.52381 || second.Amp != 20 {
		t.Fatalf("unexpected derived fields: %+v", second)
	}
	if second.Open != second.OpenPrice || second.Quote != second.QuoteVolume || second.StartTS != second.StartMS {
		t.Fatalf("expected decorated aliases: %+v", second)
	}
}

func TestKlinesBuildsHigherTimeframeFromFinalOneMinuteAndLiveOneMinute(t *testing.T) {
	ctx := context.Background()
	service := newTestMarketServiceWithFrames([]string{"1m", "1H"})
	base := int64(1_710_000_000_000)

	if err := service.IngestKline(ctx, market.Bar{
		Exchange: "binance", Symbol: "BTCUSDT", MarginType: "umargin", Timeframe: "1m",
		StartMS: base, EndMS: base + 59_999,
		OpenPrice: 100, HighPrice: 105, LowPrice: 99, ClosePrice: 102,
		Volume: 1, QuoteVolume: 100, IsFinal: true,
	}); err != nil {
		t.Fatalf("ingest final: %v", err)
	}
	if err := service.IngestKline(ctx, market.Bar{
		Exchange: "binance", Symbol: "BTCUSDT", MarginType: "umargin", Timeframe: "1m",
		StartMS: base + 60_000, EndMS: base + 119_999,
		OpenPrice: 102, HighPrice: 110, LowPrice: 101, ClosePrice: 108,
		Volume: 3, QuoteVolume: 300, IsFinal: false,
	}); err != nil {
		t.Fatalf("ingest live: %v", err)
	}

	bars, err := service.Klines(ctx, market.KlineQuery{
		Exchange: "binance", Symbol: "BTCUSDT", Timeframe: "1H", Limit: 10, IncludeLive: true,
	})
	if err != nil {
		t.Fatalf("klines: %v", err)
	}
	if len(bars) != 1 {
		t.Fatalf("expected one partial 1H bar, got %+v", bars)
	}
	bar := bars[0]
	if bar.Timeframe != "1H" || bar.IsFinal {
		t.Fatalf("expected live 1H rollup, got %+v", bar)
	}
	if bar.OpenPrice != 100 || bar.ClosePrice != 108 || bar.Volume != 4 {
		t.Fatalf("unexpected rollup values: %+v", bar)
	}
}

func newTestMarketService() *MarketService {
	return newTestMarketServiceWithFrames([]string{"1m"})
}

func newTestMarketServiceWithFrames(frames []string) *MarketService {
	return NewMarketService(
		storage.NewMemoryHistoricalStore(),
		realtime.NewHub(),
		frames,
		300,
	)
}
