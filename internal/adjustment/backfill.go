package adjustment

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strings"

	"crypto-ticket/internal/exchange"
	"crypto-ticket/internal/market"
)

const HistoricalEventBinanceContractSize = "binance_contract_size_adjustment"

type HistoricalAction struct {
	Exchange         string
	SourceMarket     string
	Symbol           string
	Ratio            float64
	WindowStartMS    int64
	WindowEndMS      int64
	PublishedMS      int64
	AnnouncementCode string
	Title            string
	Raw              json.RawMessage
}

type HistoricalBackfillStore interface {
	UpsertBars(ctx context.Context, bars []market.Bar) error
	ListAdjustmentFactors(ctx context.Context, exchange string, sourceMarket string, symbol string, priceMode string) ([]market.AdjustmentFactor, error)
	ReplaceAdjustmentFactors(ctx context.Context, exchange string, sourceMarket string, symbol string, priceMode string, factors []market.AdjustmentFactor) error
	UpsertCorporateActionEvent(ctx context.Context, event market.CorporateActionEvent) error
}

type HistoricalKlineSource interface {
	FetchKlines(ctx context.Context, client *http.Client, request exchange.KlineRequest) ([]market.Bar, error)
}

type HistoricalBackfillConfig struct {
	BoundaryTolerancePct float64
	DryRun               bool
}

func (c HistoricalBackfillConfig) withDefaults() HistoricalBackfillConfig {
	if c.BoundaryTolerancePct <= 0 {
		c.BoundaryTolerancePct = 0.25
	}
	return c
}

type HistoricalBackfillResult struct {
	Action        HistoricalAction
	Boundary      Derivation
	Segments      []market.AdjustmentFactor
	AlreadyExists bool
	BarsFetched   int
}

type HistoricalBackfiller struct {
	store  HistoricalBackfillStore
	source HistoricalKlineSource
	client *http.Client
	cfg    HistoricalBackfillConfig
}

func NewHistoricalBackfiller(store HistoricalBackfillStore, source HistoricalKlineSource, client *http.Client, cfg HistoricalBackfillConfig) *HistoricalBackfiller {
	if client == nil {
		client = http.DefaultClient
	}
	return &HistoricalBackfiller{store: store, source: source, client: client, cfg: cfg.withDefaults()}
}

func (b *HistoricalBackfiller) Backfill(ctx context.Context, action HistoricalAction) (HistoricalBackfillResult, error) {
	action.Exchange = strings.ToLower(strings.TrimSpace(action.Exchange))
	action.Symbol = strings.ToUpper(strings.TrimSpace(action.Symbol))
	result := HistoricalBackfillResult{Action: action}
	if action.Exchange != "binance" {
		return result, fmt.Errorf("historical adjustment backfill only supports binance")
	}
	if action.Symbol == "" || action.Ratio <= 0 || action.WindowStartMS <= 0 || action.WindowEndMS <= action.WindowStartMS {
		return result, fmt.Errorf("invalid historical action symbol=%q ratio=%f window=[%d,%d]", action.Symbol, action.Ratio, action.WindowStartMS, action.WindowEndMS)
	}
	bars, err := b.source.FetchKlines(ctx, b.client, exchange.KlineRequest{
		Symbol: action.Symbol, Timeframe: deriverTimeframe,
		StartMS: action.WindowStartMS, EndMS: action.WindowEndMS,
	})
	if err != nil {
		return result, fmt.Errorf("fetch boundary klines: %w", err)
	}
	result.BarsFetched = len(bars)
	boundary, ok := LocateHistoricalBoundary(bars, action.Ratio, b.cfg.BoundaryTolerancePct)
	if !ok {
		return result, fmt.Errorf("no kline boundary matches announced ratio %.8f within %.2f%%", action.Ratio, b.cfg.BoundaryTolerancePct*100)
	}
	result.Boundary = boundary

	existing, err := b.store.ListAdjustmentFactors(ctx, action.Exchange, action.SourceMarket, action.Symbol, market.PriceModeBackwardAdjusted)
	if err != nil {
		return result, fmt.Errorf("list existing adjustment factors: %w", err)
	}
	ledger := ReconstructLedger(existing)
	if HasEventAt(ledger, boundary.BoundaryMS) {
		result.AlreadyExists = true
		result.Segments = existing
		if !b.cfg.DryRun {
			if err := b.store.UpsertBars(ctx, bars); err != nil {
				return result, fmt.Errorf("upsert boundary klines: %w", err)
			}
			if err := b.persistEvent(ctx, action, boundary, historicalEvidence(action, boundary)); err != nil {
				return result, err
			}
		}
		return result, nil
	}
	ledger = append(ledger, LedgerEvent{
		EffectiveMS: boundary.BoundaryMS, PriceMultiplier: 1 / action.Ratio,
		VolumeMultiplier: action.Ratio, EventType: HistoricalEventBinanceContractSize,
	})
	evidence := historicalEvidence(action, boundary)
	base := market.AdjustmentFactor{
		Provider: BinanceProviderName, ProviderVersion: ProviderVersion,
		Exchange: action.Exchange, SourceMarket: action.SourceMarket, Symbol: action.Symbol,
		EventType: HistoricalEventBinanceContractSize, Raw: evidence,
	}
	result.Segments = CumulativeBackwardSegments(base, ledger)
	if b.cfg.DryRun {
		return result, nil
	}
	if err := b.store.UpsertBars(ctx, bars); err != nil {
		return result, fmt.Errorf("upsert boundary klines: %w", err)
	}
	if err := b.store.ReplaceAdjustmentFactors(ctx, action.Exchange, action.SourceMarket, action.Symbol,
		market.PriceModeBackwardAdjusted, result.Segments); err != nil {
		return result, fmt.Errorf("replace adjustment factors: %w", err)
	}
	if err := b.persistEvent(ctx, action, boundary, evidence); err != nil {
		return result, err
	}
	return result, nil
}

func (b *HistoricalBackfiller) persistEvent(ctx context.Context, action HistoricalAction, boundary Derivation, evidence json.RawMessage) error {
	firstSeenMS := action.PublishedMS
	if firstSeenMS <= 0 {
		firstSeenMS = action.WindowStartMS
	}
	if err := b.store.UpsertCorporateActionEvent(ctx, market.CorporateActionEvent{
		ActionID: historicalActionID(action), Exchange: action.Exchange, SourceMarket: action.SourceMarket,
		Symbol: action.Symbol, EventType: HistoricalEventBinanceContractSize, State: market.CorporateActionStateFactor,
		FirstSeenMS: firstSeenMS, LastEventMS: boundary.BoundaryMS, ResumeMS: boundary.BoundaryMS,
		BoundaryMS: boundary.BoundaryMS, AnnouncedRatio: action.Ratio, Raw: evidence, UpdatedAtMS: market.NowMS(),
	}); err != nil {
		return fmt.Errorf("persist historical corporate action: %w", err)
	}
	return nil
}

// LocateHistoricalBoundary finds the adjacent active bars whose observed gap
// is closest to the official ratio. The official ratio remains authoritative;
// the observed gap is accepted only as boundary evidence.
func LocateHistoricalBoundary(bars []market.Bar, officialRatio float64, tolerancePct float64) (Derivation, bool) {
	if officialRatio <= 0 || len(bars) < 2 {
		return Derivation{}, false
	}
	if tolerancePct <= 0 {
		tolerancePct = 0.25
	}
	bestError := math.Inf(1)
	var best Derivation
	previous := -1
	for i := range bars {
		if bars[i].Volume <= 0 || bars[i].OpenPrice <= 0 || bars[i].ClosePrice <= 0 {
			continue
		}
		if previous >= 0 {
			observed := bars[previous].ClosePrice / bars[i].OpenPrice
			relativeError := math.Abs(observed-officialRatio) / officialRatio
			if relativeError < bestError {
				bestError = relativeError
				best = Derivation{
					BoundaryMS: bars[i].StartMS, CloseBefore: bars[previous].ClosePrice, OpenAfter: bars[i].OpenPrice,
					Ratio: observed, PriceMultiplier: 1 / officialRatio, VolumeMultiplier: officialRatio,
				}
			}
		}
		previous = i
	}
	return best, best.BoundaryMS > 0 && bestError <= tolerancePct
}

func historicalEvidence(action HistoricalAction, boundary Derivation) json.RawMessage {
	body, _ := json.Marshal(map[string]any{
		"method": "binance_historical_announcement", "announcement_code": action.AnnouncementCode,
		"announcement_title": action.Title, "official_ratio": action.Ratio,
		"boundary_ms": boundary.BoundaryMS, "observed_ratio": boundary.Ratio,
		"close_before": boundary.CloseBefore, "open_after": boundary.OpenAfter,
		"announcement_raw": json.RawMessage(action.Raw),
	})
	return body
}

func historicalActionID(action HistoricalAction) string {
	code := strings.TrimSpace(action.AnnouncementCode)
	if code == "" {
		code = fmt.Sprint(action.WindowStartMS)
	}
	return fmt.Sprintf("%s|%s|%s|history|%s", action.Exchange, action.SourceMarket, action.Symbol, code)
}
