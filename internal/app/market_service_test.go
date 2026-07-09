package app

import (
	"context"
	"testing"
	"time"

	"crypto-ticket/internal/market"
	"crypto-ticket/internal/realtime"
	"crypto-ticket/internal/storage"
	"crypto-ticket/internal/timeframe"
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

func TestKlinesAppliesAdjustmentFactorForAdjustedPriceMode(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryHistoricalStore()
	service := NewMarketService(store, realtime.NewHub(), []string{"1m"}, 300)
	base := int64(1_710_000_000_000)
	if err := store.UpsertAdjustmentFactors(ctx, []market.AdjustmentFactor{{
		Provider:         "vendor",
		ProviderVersion:  "v1",
		Exchange:         "binance",
		SourceMarket:     "binance:um_futures",
		Symbol:           "TSLAUSDT",
		AdjMode:          market.PriceModeBackwardAdjusted,
		EffectiveFromMS:  base,
		EffectiveToMS:    base + 119_999,
		PriceMultiplier:  0.5,
		VolumeMultiplier: 2,
		EventType:        "split",
	}}); err != nil {
		t.Fatalf("upsert factor: %v", err)
	}
	for i := 0; i < 2; i++ {
		start := base + int64(i)*60_000
		if err := service.IngestKline(ctx, market.Bar{
			Exchange: "binance", SourceMarket: "binance:um_futures", Symbol: "TSLAUSDT", Timeframe: "1m",
			StartMS: start, EndMS: start + 59_999,
			OpenPrice: 100, HighPrice: 110, LowPrice: 90, ClosePrice: 104,
			Volume: 10, QuoteVolume: 1000, IsFinal: true,
		}); err != nil {
			t.Fatalf("ingest final: %v", err)
		}
	}

	bars, err := service.Klines(ctx, market.KlineQuery{
		Exchange: "binance", Symbol: "TSLAUSDT", Timeframe: "1m", Limit: 10, PriceMode: market.PriceModeBackwardAdjusted,
	})
	if err != nil {
		t.Fatalf("klines: %v", err)
	}
	if len(bars) != 2 {
		t.Fatalf("expected two bars, got %+v", bars)
	}
	if bars[0].OpenPrice != 50 || bars[0].Volume != 20 || bars[0].RawOpenPrice != 100 {
		t.Fatalf("unexpected adjusted first bar: %+v", bars[0])
	}
	if bars[1].PrevClose != 52 || bars[1].AdjustmentStatus != market.AdjustmentStatusAdjusted {
		t.Fatalf("expected adjusted derived fields, got %+v", bars[1])
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

func TestIngestKlinePublishesKlineAndTickerEvents(t *testing.T) {
	ctx := context.Background()
	hub := realtime.NewHub()
	service := NewMarketService(storage.NewMemoryHistoricalStore(), hub, []string{"1m"}, 300)
	sub := hub.Subscribe()
	defer sub.Close()
	sub.Add(realtime.KlineChannel("binance", "BTCUSDT", "1m"))
	sub.Add(realtime.TickerChannel("binance", "BTCUSDT"))

	if err := service.IngestKline(ctx, market.Bar{
		Exchange: "binance", Symbol: "BTCUSDT", MarginType: "umargin", Timeframe: "1m",
		StartMS: 1_710_000_000_000, EndMS: 1_710_000_059_999,
		OpenPrice: 100, HighPrice: 105, LowPrice: 99, ClosePrice: 102,
		Volume: 1, QuoteVolume: 102, IsFinal: false,
	}); err != nil {
		t.Fatalf("ingest live: %v", err)
	}

	kline := nextTestEvent(t, sub)
	if kline.Type != "kline" || kline.Bar == nil {
		t.Fatalf("expected kline event, got %+v", kline)
	}
	ticker := nextTestEvent(t, sub)
	if ticker.Type != "ticker" || ticker.Tick == nil || ticker.Tick.Price != 102 {
		t.Fatalf("expected ticker event, got %+v", ticker)
	}
}

func TestIngestKlinePublishesEveryLiveUpdate(t *testing.T) {
	ctx := context.Background()
	hub := realtime.NewHub()
	service := NewMarketService(storage.NewMemoryHistoricalStore(), hub, []string{"1m"}, 300)
	sub := hub.Subscribe()
	defer sub.Close()
	sub.Add(realtime.KlineChannel("binance", "BTCUSDT", "1m"))
	sub.Add(realtime.TickerChannel("binance", "BTCUSDT"))
	base := int64(1_710_000_000_000)

	if err := service.IngestKline(ctx, market.Bar{
		Exchange: "binance", Symbol: "BTCUSDT", MarginType: "umargin", Timeframe: "1m",
		StartMS: base, EndMS: base + 59_999,
		OpenPrice: 100, HighPrice: 105, LowPrice: 99, ClosePrice: 102,
		Volume: 1, QuoteVolume: 102, IsFinal: false,
	}); err != nil {
		t.Fatalf("ingest first live: %v", err)
	}
	if err := service.IngestKline(ctx, market.Bar{
		Exchange: "binance", Symbol: "BTCUSDT", MarginType: "umargin", Timeframe: "1m",
		StartMS: base, EndMS: base + 59_999,
		OpenPrice: 100, HighPrice: 106, LowPrice: 98, ClosePrice: 103,
		Volume: 2, QuoteVolume: 206, IsFinal: false,
	}); err != nil {
		t.Fatalf("ingest second live: %v", err)
	}

	_ = nextTestEvent(t, sub)
	_ = nextTestEvent(t, sub)
	kline := nextTestEvent(t, sub)
	if kline.Type != "kline" || kline.Bar == nil || kline.Bar.ClosePrice != 103 {
		t.Fatalf("expected second live kline event, got %+v", kline)
	}
	ticker := nextTestEvent(t, sub)
	if ticker.Type != "ticker" || ticker.Tick == nil || ticker.Tick.Price != 103 {
		t.Fatalf("expected second live ticker event, got %+v", ticker)
	}
}

func TestIngestKlinePublishesLiveHigherTimeframeRollup(t *testing.T) {
	ctx := context.Background()
	hub := realtime.NewHub()
	service := NewMarketService(storage.NewMemoryHistoricalStore(), hub, []string{"1m", "1H"}, 300)
	sub := hub.Subscribe()
	defer sub.Close()
	sub.Add(realtime.KlineChannel("binance", "BTCUSDT", "1H"))
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

	event := nextTestEvent(t, sub)
	if event.Type != "kline" || event.Timeframe != "1H" || event.Bar == nil {
		t.Fatalf("expected live 1H kline event, got %+v", event)
	}
	if event.Bar.IsFinal || event.Bar.OpenPrice != 100 || event.Bar.ClosePrice != 108 || event.Bar.Volume != 4 {
		t.Fatalf("unexpected live 1H rollup: %+v", event.Bar)
	}
}

func TestIngestKlinePublishesSubscribedLiveRollupOutsideConfiguredFinalFrames(t *testing.T) {
	ctx := context.Background()
	hub := realtime.NewHub()
	service := NewMarketService(storage.NewMemoryHistoricalStore(), hub, []string{"1m"}, 300)
	sub := hub.Subscribe()
	defer sub.Close()
	sub.Add(realtime.KlineChannel("binance", "BTCUSDT", "1H"))
	base := int64(1_710_000_000_000)

	if err := service.IngestKline(ctx, market.Bar{
		Exchange: "binance", Symbol: "BTCUSDT", MarginType: "umargin", Timeframe: "1m",
		StartMS: base, EndMS: base + 59_999,
		OpenPrice: 100, HighPrice: 105, LowPrice: 99, ClosePrice: 102,
		Volume: 1, QuoteVolume: 100, IsFinal: false,
	}); err != nil {
		t.Fatalf("ingest live: %v", err)
	}

	event := nextTestEvent(t, sub)
	if event.Type != "kline" || event.Timeframe != "1H" || event.Bar == nil {
		t.Fatalf("expected live 1H kline event, got %+v", event)
	}
	if event.Bar.IsFinal || event.Bar.OpenPrice != 100 || event.Bar.ClosePrice != 102 || event.Bar.Volume != 1 {
		t.Fatalf("unexpected live 1H rollup outside configured frames: %+v", event.Bar)
	}
}

func TestIngestKlineCascadesFinalRollupsThroughIntermediateFrames(t *testing.T) {
	ctx := context.Background()
	service := newTestMarketServiceWithFrames([]string{"1m", "1H"})
	base := timeframe.FloorStartMS(1_710_000_000_000, "1H")

	for i := 0; i < 60; i++ {
		start := base + int64(i)*60_000
		open := float64(100 + i)
		if err := service.IngestKline(ctx, market.Bar{
			Exchange: "binance", Symbol: "BTCUSDT", MarginType: "umargin", Timeframe: "1m",
			StartMS: start, EndMS: start + 59_999,
			OpenPrice: open, HighPrice: open + 2, LowPrice: open - 1, ClosePrice: open + 1,
			Volume: 1, QuoteVolume: 100, IsFinal: true,
		}); err != nil {
			t.Fatalf("ingest final minute %d: %v", i, err)
		}
	}

	bars, err := service.Klines(ctx, market.KlineQuery{
		Exchange: "binance", Symbol: "BTCUSDT", Timeframe: "1H", Limit: 10, IncludeLive: false,
	})
	if err != nil {
		t.Fatalf("klines: %v", err)
	}
	if len(bars) != 1 {
		t.Fatalf("expected one final 1H bar, got %+v", bars)
	}
	bar := bars[0]
	if !bar.IsFinal || bar.Timeframe != "1H" || bar.OpenPrice != 100 || bar.ClosePrice != 160 || bar.Volume != 60 {
		t.Fatalf("unexpected cascaded 1H rollup: %+v", bar)
	}
}

func TestRepairFinalBarsRebuildsContainingRollups(t *testing.T) {
	ctx := context.Background()
	service := newTestMarketServiceWithFrames([]string{"1m", "5m"})
	base := timeframe.FloorStartMS(1_710_000_000_000, "5m")

	for i := 0; i < 5; i++ {
		start := base + int64(i)*60_000
		if err := service.IngestKline(ctx, market.Bar{
			Exchange: "binance", Symbol: "BTCUSDT", MarginType: "umargin", Timeframe: "1m",
			StartMS: start, EndMS: start + 59_999,
			OpenPrice: 100, HighPrice: 105, LowPrice: 99, ClosePrice: 102,
			Volume: 1, QuoteVolume: 100, IsFinal: true,
		}); err != nil {
			t.Fatalf("ingest final minute %d: %v", i, err)
		}
	}

	if err := service.RepairFinalBars(ctx, []market.Bar{{
		Exchange: "binance", Symbol: "BTCUSDT", MarginType: "umargin", Timeframe: "1m",
		StartMS: base + 2*60_000, EndMS: base + 3*60_000 - 1,
		OpenPrice: 102, HighPrice: 130, LowPrice: 98, ClosePrice: 120,
		Volume: 9, QuoteVolume: 900, IsFinal: true, Source: "rest", Reason: "guardian_repair",
	}}); err != nil {
		t.Fatalf("repair final: %v", err)
	}

	bars, err := service.Klines(ctx, market.KlineQuery{
		Exchange: "binance", Symbol: "BTCUSDT", Timeframe: "5m", Limit: 10, IncludeLive: false,
	})
	if err != nil {
		t.Fatalf("klines: %v", err)
	}
	if len(bars) != 1 {
		t.Fatalf("expected one final 5m bar, got %+v", bars)
	}
	bar := bars[0]
	if !bar.IsFinal || bar.HighPrice != 130 || bar.LowPrice != 98 || bar.Volume != 13 || bar.QuoteVolume != 1300 {
		t.Fatalf("expected repaired 5m rollup, got %+v", bar)
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

func nextTestEvent(t *testing.T, sub *realtime.Subscriber) market.Event {
	t.Helper()
	select {
	case event := <-sub.Events():
		return event
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for realtime event")
		return market.Event{}
	}
}
