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

func TestRepairOfficialDailyBarsWithoutHourlySourceRebuildsWeeklyBar(t *testing.T) {
	ctx := context.Background()
	service := newTestMarketServiceWithFrames([]string{"1m", "1H", "1D", "1W"})
	base := timeframe.FloorStartMS(1_710_000_000_000, "1W")
	var daily []market.Bar
	for i := 0; i < 7; i++ {
		start := base + int64(i)*timeframe.DurationMS("1D")
		daily = append(daily, market.Bar{
			Exchange: "binance", Symbol: "BTCUSDT", Timeframe: "1D",
			StartMS: start, EndMS: timeframe.EndMS(start, "1D"),
			OpenPrice: 100, HighPrice: 110, LowPrice: 90, ClosePrice: 105,
			Volume: 2, QuoteVolume: 200, Source: "rest", IsFinal: true,
		})
	}
	if err := service.RepairFinalBars(ctx, daily); err != nil {
		t.Fatalf("repair official daily bars: %v", err)
	}
	for _, tf := range []string{"1m", "1H"} {
		bars, err := service.store.BarsInRange(ctx, "binance", "BTCUSDT", tf, base, timeframe.EndMS(base, "1W"))
		if err != nil || len(bars) != 0 {
			t.Fatalf("daily repair must not fabricate source %s: bars=%v err=%v", tf, bars, err)
		}
	}
	weekly, err := service.store.BarsInRange(ctx, "binance", "BTCUSDT", "1W", base, base)
	if err != nil || len(weekly) != 1 || !weekly[0].IsFinal || weekly[0].Volume != 14 || weekly[0].ClosePrice != 105 {
		t.Fatalf("expected complete weekly rollup from repaired official daily bars: bars=%v err=%v", weekly, err)
	}
}

func TestHistoricalRepairPreservesCurrentLiveBar(t *testing.T) {
	ctx := context.Background()
	service := newTestMarketService()
	base := timeframe.FloorStartMS(1_710_000_000_000, "1m")
	live := market.Bar{
		Exchange: "binance", Symbol: "BTCUSDT", Timeframe: "1m",
		StartMS: base, EndMS: timeframe.EndMS(base, "1m"),
		OpenPrice: 100, HighPrice: 110, LowPrice: 90, ClosePrice: 108,
	}
	if err := service.IngestKline(ctx, live); err != nil {
		t.Fatal(err)
	}
	historical := live
	historical.StartMS -= timeframe.MinuteMS
	historical.EndMS -= timeframe.MinuteMS
	historical.ClosePrice = 101
	if err := service.RepairFinalBars(ctx, []market.Bar{historical}); err != nil {
		t.Fatal(err)
	}
	tick, err := service.LatestTick(ctx, "binance", "BTCUSDT")
	if err != nil || tick == nil || tick.Price != live.ClosePrice {
		t.Fatalf("historical repair discarded current snapshot: tick=%v err=%v", tick, err)
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
