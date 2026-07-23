package app

import (
	"context"
	"testing"
	"time"

	"crypto-ticket/internal/corpaction"
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

func TestApplyCorporateActionGuardNeutralizesCrossingBar(t *testing.T) {
	service := newTestMarketService()
	// Anchor near "now" so the resolved-window TTL (checked against NowMS) is live.
	boundary := market.NowMS()
	registry := corpaction.NewRegistry(corpaction.Config{})
	registry.Resolve("okx", "MUU-USDT-SWAP", boundary)
	service.SetCorporateActionGuard(registry)

	crossing := service.applyCorporateActionGuard(market.Bar{
		Exchange: "okx", Symbol: "MUU-USDT-SWAP", Timeframe: "1m",
		StartMS: boundary, EndMS: boundary + 59_999,
		ClosePrice: 100, PrevClose: 2000, Chg: -95, PriceMode: market.PriceModeRaw,
	})
	if crossing.Chg != 0 || crossing.PrevClose != 0 {
		t.Fatalf("crossing bar should be neutralized, got chg=%f prev=%f", crossing.Chg, crossing.PrevClose)
	}
	if crossing.AdjustmentStatus != market.AdjustmentStatusLiveRaw {
		t.Fatalf("crossing bar should be flagged live_raw, got %q", crossing.AdjustmentStatus)
	}

	// A bar outside the boundary span keeps its real change.
	normal := service.applyCorporateActionGuard(market.Bar{
		Exchange: "okx", Symbol: "MUU-USDT-SWAP", Timeframe: "1m",
		StartMS: boundary + 60_000, EndMS: boundary + 119_999,
		ClosePrice: 130, PrevClose: 100, Chg: 30, PriceMode: market.PriceModeRaw,
	})
	if normal.Chg != 30 || normal.AdjustmentStatus == market.AdjustmentStatusLiveRaw {
		t.Fatalf("post-boundary bar should be untouched, got chg=%f status=%q", normal.Chg, normal.AdjustmentStatus)
	}
}

func TestEnrichBarNeutralizesFakeJumpAcrossRebase(t *testing.T) {
	ctx := context.Background()
	service := newTestMarketService()
	boundary := market.NowMS()
	base := boundary - 60_000
	registry := corpaction.NewRegistry(corpaction.Config{})
	registry.Resolve("okx", "MUU-USDT-SWAP", boundary)
	service.SetCorporateActionGuard(registry)

	// Pre-split final bar establishes the previous close (2000).
	if err := service.IngestKline(ctx, market.Bar{
		Exchange: "okx", Symbol: "MUU-USDT-SWAP", Timeframe: "1m",
		StartMS: base, EndMS: base + 59_999,
		OpenPrice: 2000, HighPrice: 2000, LowPrice: 2000, ClosePrice: 2000,
		IsFinal: true,
	}); err != nil {
		t.Fatalf("ingest pre-split: %v", err)
	}

	enriched, err := service.enrichBar(ctx, market.DecorateBar(market.Bar{
		Exchange: "okx", Symbol: "MUU-USDT-SWAP", Timeframe: "1m",
		StartMS: boundary, EndMS: boundary + 59_999,
		OpenPrice: 100, HighPrice: 100, LowPrice: 100, ClosePrice: 100,
		PriceMode: market.PriceModeRaw,
	}))
	if err != nil {
		t.Fatalf("enrichBar: %v", err)
	}
	if enriched.Chg != 0 {
		t.Fatalf("fake -95%% jump should be neutralized, got chg=%f", enriched.Chg)
	}
	if enriched.AdjustmentStatus != market.AdjustmentStatusLiveRaw {
		t.Fatalf("boundary bar should be live_raw, got %q", enriched.AdjustmentStatus)
	}
}

func TestEnrichBarLeavesBarsUnchangedWithoutGuard(t *testing.T) {
	ctx := context.Background()
	service := newTestMarketService()
	base := int64(1_710_000_000_000)

	if err := service.IngestKline(ctx, market.Bar{
		Exchange: "okx", Symbol: "MUU-USDT-SWAP", Timeframe: "1m",
		StartMS: base, EndMS: base + 59_999,
		OpenPrice: 2000, HighPrice: 2000, LowPrice: 2000, ClosePrice: 2000,
		IsFinal: true,
	}); err != nil {
		t.Fatalf("ingest pre-split: %v", err)
	}
	enriched, err := service.enrichBar(ctx, market.DecorateBar(market.Bar{
		Exchange: "okx", Symbol: "MUU-USDT-SWAP", Timeframe: "1m",
		StartMS: base + 60_000, EndMS: base + 119_999,
		OpenPrice: 100, HighPrice: 100, LowPrice: 100, ClosePrice: 100,
		PriceMode: market.PriceModeRaw,
	}))
	if err != nil {
		t.Fatalf("enrichBar: %v", err)
	}
	if enriched.Chg == 0 {
		t.Fatal("without a guard the raw jump must be preserved")
	}
}

type fixedAliasResolver struct {
	predecessor string
	boundaryMS  int64
}

func (f fixedAliasResolver) Lookup(_ string, _ string) (string, string, int64, bool) {
	return f.predecessor, "okx:SWAP", f.boundaryMS, true
}

func TestKlinesStitchesRenamedPredecessorHistory(t *testing.T) {
	ctx := context.Background()
	service := newTestMarketService()
	store := storage.NewMemoryHistoricalStore()
	// Rebuild the service around a store we can seed directly.
	service = NewMarketService(store, realtime.NewHub(), []string{"1m"}, 300)

	minute := int64(60_000)
	boundary := int64(1_710_000_120_000)

	rawBar := func(symbol string, startMS int64, price float64) market.Bar {
		return market.Bar{
			Exchange: "okx", SourceMarket: "okx:SWAP", Symbol: symbol, Timeframe: "1m",
			StartMS: startMS, EndMS: startMS + 59_999,
			OpenPrice: price, HighPrice: price, LowPrice: price, ClosePrice: price,
			Volume: 5, IsFinal: true,
		}
	}
	// Predecessor OLDX (price ~2504) before boundary; successor NEWX (~200) after.
	if err := store.UpsertBars(ctx, []market.Bar{
		rawBar("OLDX-USDT-SWAP", boundary-2*minute, 2504),
		rawBar("OLDX-USDT-SWAP", boundary-minute, 2504),
		rawBar("NEWX-USDT-SWAP", boundary, 200),
		rawBar("NEWX-USDT-SWAP", boundary+minute, 205),
	}); err != nil {
		t.Fatal(err)
	}
	// The successor's factor timeline: pre-boundary scaled by 200/2504, unit after.
	if err := store.ReplaceAdjustmentFactors(ctx, "okx", "okx:SWAP", "NEWX-USDT-SWAP", market.PriceModeBackwardAdjusted, []market.AdjustmentFactor{
		{Exchange: "okx", SourceMarket: "okx:SWAP", Symbol: "NEWX-USDT-SWAP", AdjMode: market.PriceModeBackwardAdjusted, EffectiveFromMS: 0, EffectiveToMS: boundary - 1, PriceMultiplier: 200.0 / 2504.0, VolumeMultiplier: 2504.0 / 200.0},
		{Exchange: "okx", SourceMarket: "okx:SWAP", Symbol: "NEWX-USDT-SWAP", AdjMode: market.PriceModeBackwardAdjusted, EffectiveFromMS: boundary, EffectiveToMS: 0, PriceMultiplier: 1, VolumeMultiplier: 1},
	}); err != nil {
		t.Fatal(err)
	}
	service.SetAliasResolver(fixedAliasResolver{predecessor: "OLDX-USDT-SWAP", boundaryMS: boundary})

	bars, err := service.Klines(ctx, market.KlineQuery{
		Exchange: "okx", Symbol: "NEWX-USDT-SWAP", Timeframe: "1m", Limit: 10,
		PriceMode: market.PriceModeBackwardAdjusted,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Two predecessor bars stitched ahead of the two successor bars.
	if len(bars) != 4 {
		t.Fatalf("expected 4 stitched bars, got %d", len(bars))
	}
	// Predecessor bars are scaled onto the successor basis: 2504 * (200/2504) = 200.
	if bars[0].StartMS != boundary-2*minute {
		t.Fatalf("history should be prepended in order, first StartMS=%d", bars[0].StartMS)
	}
	if bars[0].ClosePrice < 199.9 || bars[0].ClosePrice > 200.1 {
		t.Fatalf("predecessor close should adjust to ~200, got %f", bars[0].ClosePrice)
	}
	if bars[3].ClosePrice != 205 {
		t.Fatalf("latest successor bar should be unadjusted 205, got %f", bars[3].ClosePrice)
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

func TestKlinesUsesClosingRegimeForBarSpanningAdjustmentBoundary(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryHistoricalStore()
	service := NewMarketService(store, realtime.NewHub(), []string{"1H"}, 300)
	boundary := int64(1_710_003_000_000)
	if err := store.UpsertAdjustmentFactors(ctx, []market.AdjustmentFactor{
		{
			Provider: "vendor", Exchange: "binance", SourceMarket: "binance:um_futures", Symbol: "KORUUSDT",
			AdjMode: market.PriceModeBackwardAdjusted, EffectiveFromMS: 0, EffectiveToMS: boundary - 1,
			PriceMultiplier: 0.05, VolumeMultiplier: 20,
		},
		{
			Provider: "vendor", Exchange: "binance", SourceMarket: "binance:um_futures", Symbol: "KORUUSDT",
			AdjMode: market.PriceModeBackwardAdjusted, EffectiveFromMS: boundary, EffectiveToMS: 0,
			PriceMultiplier: 1, VolumeMultiplier: 1,
		},
	}); err != nil {
		t.Fatalf("upsert factors: %v", err)
	}
	if err := store.UpsertBars(ctx, []market.Bar{{
		Exchange: "binance", SourceMarket: "binance:um_futures", Symbol: "KORUUSDT", Timeframe: "1H",
		StartMS: boundary - 30*60_000, EndMS: boundary + 30*60_000 - 1,
		OpenPrice: 22.68, HighPrice: 23.5, LowPrice: 22.5, ClosePrice: 23.39, Volume: 100, IsFinal: true,
	}}); err != nil {
		t.Fatalf("upsert spanning bar: %v", err)
	}

	bars, err := service.Klines(ctx, market.KlineQuery{
		Exchange: "binance", Symbol: "KORUUSDT", Timeframe: "1H", Limit: 1,
		PriceMode: market.PriceModeBackwardAdjusted,
	})
	if err != nil {
		t.Fatalf("klines: %v", err)
	}
	if len(bars) != 1 || bars[0].PriceMultiplier != 1 || bars[0].OpenPrice != 22.68 {
		t.Fatalf("spanning bar should use post-boundary factor: %+v", bars)
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
