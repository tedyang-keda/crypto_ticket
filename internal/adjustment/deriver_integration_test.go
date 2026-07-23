package adjustment

import (
	"context"
	"testing"
	"time"

	"crypto-ticket/internal/market"
	"crypto-ticket/internal/storage"
)

type fixedAlias struct {
	predecessor string
	boundaryMS  int64
}

func (f fixedAlias) Lookup(_ string, _ string) (string, string, int64, bool) {
	return f.predecessor, "okx:SWAP", f.boundaryMS, true
}

// TestDeriverServesContinuousSeries exercises the full layer-B contract against
// the real memory store: raw bars with a 20:1 split gap -> derived factor ->
// backward-adjusted RecentBars returns a continuous series.
func TestDeriverServesContinuousSeries(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryHistoricalStore()

	rawBar := func(startMS int64, price float64) market.Bar {
		return market.Bar{
			Exchange:     "okx",
			SourceMarket: "okx:SWAP",
			Symbol:       "MUU-USDT-SWAP",
			Timeframe:    "1m",
			StartMS:      startMS,
			EndMS:        startMS + 59_999,
			OpenPrice:    price,
			HighPrice:    price,
			LowPrice:     price,
			ClosePrice:   price,
			Volume:       10,
			IsFinal:      true,
		}
	}
	// Use realistic epoch timestamps near "now" so the deriver's lookback
	// window (relative to event time) actually covers the bars.
	minute := time.Minute.Milliseconds()
	base := market.NowMS() - 5*minute
	boundary := base + 2*minute
	bars := []market.Bar{
		rawBar(base, 2000),
		rawBar(base+minute, 2000),
		rawBar(boundary, 100), // post-split
		rawBar(boundary+minute, 100),
	}
	if err := store.UpsertBars(ctx, bars); err != nil {
		t.Fatal(err)
	}

	deriver := New(store, store, Config{ConfirmDelay: time.Minute, MinMovePct: 0.05})
	_ = deriver.HandleInstrumentEvent(ctx, market.InstrumentChangeEvent{
		Exchange:     "okx",
		SourceMarket: "okx:SWAP",
		Symbol:       "MUU-USDT-SWAP",
		EventType:    market.InstrumentEventRebase,
	})
	deriver.processPending(ctx, market.NowMS()+2*minute)

	adjusted, err := store.RecentBars(ctx, market.KlineQuery{
		Exchange:     "okx",
		SourceMarket: "okx:SWAP",
		Symbol:       "MUU-USDT-SWAP",
		Timeframe:    "1m",
		Limit:        10,
		PriceMode:    market.PriceModeBackwardAdjusted,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(adjusted) != 4 {
		t.Fatalf("expected 4 bars, got %d", len(adjusted))
	}
	for _, b := range adjusted {
		if b.ClosePrice != 100 {
			t.Fatalf("bar %d close = %f, want continuous 100", b.StartMS, b.ClosePrice)
		}
		if b.PriceMode != market.PriceModeBackwardAdjusted || b.AdjustmentStatus != market.AdjustmentStatusAdjusted {
			t.Fatalf("bar %d not marked adjusted: mode=%q status=%q", b.StartMS, b.PriceMode, b.AdjustmentStatus)
		}
	}
}

// TestDeriverRenameCrossSymbol verifies the layer-D rename path: a fresh
// successor instId with no in-symbol gap derives its factor from the
// predecessor's last close vs its own first open.
func TestDeriverRenameCrossSymbol(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryHistoricalStore()
	minute := time.Minute.Milliseconds()
	boundary := market.NowMS() - 2*minute

	rawBar := func(symbol string, startMS int64, price float64) market.Bar {
		return market.Bar{
			Exchange: "okx", SourceMarket: "okx:SWAP", Symbol: symbol, Timeframe: "1m",
			StartMS: startMS, EndMS: startMS + 59_999,
			OpenPrice: price, HighPrice: price, LowPrice: price, ClosePrice: price,
			Volume: 5, IsFinal: true,
		}
	}
	// Predecessor trades near 2504 before the rename; successor near 200 after.
	if err := store.UpsertBars(ctx, []market.Bar{
		rawBar("OLDX-USDT-SWAP", boundary-2*minute, 2504),
		rawBar("OLDX-USDT-SWAP", boundary-minute, 2504),
		rawBar("NEWX-USDT-SWAP", boundary, 200),
		rawBar("NEWX-USDT-SWAP", boundary+minute, 205),
	}); err != nil {
		t.Fatal(err)
	}

	deriver := New(store, store, Config{ConfirmDelay: time.Minute, MinMovePct: 0.05})
	deriver.SetAliasResolver(fixedAlias{predecessor: "OLDX-USDT-SWAP", boundaryMS: boundary})
	_ = deriver.HandleInstrumentEvent(ctx, market.InstrumentChangeEvent{
		Exchange: "okx", SourceMarket: "okx:SWAP", Symbol: "NEWX-USDT-SWAP",
		EventType: market.InstrumentEventRenamed,
	})
	deriver.processPending(ctx, market.NowMS()+2*minute)

	factors, err := store.ListAdjustmentFactors(ctx, "okx", "okx:SWAP", "NEWX-USDT-SWAP", market.PriceModeBackwardAdjusted)
	if err != nil {
		t.Fatal(err)
	}
	if len(factors) != 2 {
		t.Fatalf("expected 2 rename segments, got %d", len(factors))
	}
	pre := factors[0]
	if pre.EffectiveToMS != boundary-1 {
		t.Fatalf("pre segment should end at boundary-1, got %d (boundary=%d)", pre.EffectiveToMS, boundary)
	}
	// price multiplier = successor open / predecessor close = 200 / 2504.
	want := 200.0 / 2504.0
	if pre.PriceMultiplier < want-1e-6 || pre.PriceMultiplier > want+1e-6 {
		t.Fatalf("pre price multiplier = %f, want %f", pre.PriceMultiplier, want)
	}
}
