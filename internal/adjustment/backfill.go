package adjustment

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strings"

	"crypto-ticket/internal/aggregator"
	"crypto-ticket/internal/exchange"
	"crypto-ticket/internal/market"
	"crypto-ticket/internal/timeframe"
)

const (
	HistoricalEventBinanceContractSize = "binance_contract_size_adjustment"
	HistoricalEventOKXRebase           = "okx_rebase"
)

type HistoricalAction struct {
	Exchange          string
	SourceMarket      string
	Symbol            string
	PredecessorSymbol string
	Ratio             float64
	WindowStartMS     int64
	WindowEndMS       int64
	PublishedMS       int64
	AnnouncementCode  string
	Title             string
	Raw               json.RawMessage
}

type HistoricalBackfillStore interface {
	UpsertBars(ctx context.Context, bars []market.Bar) error
	UpsertAdjustedBars(ctx context.Context, bars []market.Bar) error
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
	Action                   HistoricalAction
	Boundary                 Derivation
	Segments                 []market.AdjustmentFactor
	AlreadyExists            bool
	BarsFetched              int
	RawBarsRebuilt           int
	AdjustedBarsMaterialized int
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
	action.PredecessorSymbol = strings.ToUpper(strings.TrimSpace(action.PredecessorSymbol))
	result := HistoricalBackfillResult{Action: action}
	if action.Exchange != "binance" && action.Exchange != "okx" {
		return result, fmt.Errorf("historical adjustment backfill does not support exchange %q", action.Exchange)
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
	officialOneMinute, err := b.fetchMaterializationWindow(ctx, action, boundary.BoundaryMS)
	if err != nil {
		return result, err
	}
	result.BarsFetched = len(officialOneMinute)

	existing, err := b.store.ListAdjustmentFactors(ctx, action.Exchange, action.SourceMarket, action.Symbol, market.PriceModeBackwardAdjusted)
	if err != nil {
		return result, fmt.Errorf("list existing adjustment factors: %w", err)
	}
	ledger := ReconstructLedger(existing)
	if HasEventAt(ledger, boundary.BoundaryMS) {
		result.AlreadyExists = true
		result.Segments = existing
		if !b.cfg.DryRun {
			if err := b.persistBoundaryBars(ctx, action, boundary, officialOneMinute, existing, &result); err != nil {
				return result, err
			}
			if err := b.persistEvent(ctx, action, boundary, historicalEvidence(action, boundary)); err != nil {
				return result, err
			}
		}
		return result, nil
	}
	ledger = append(ledger, LedgerEvent{
		EffectiveMS: boundary.BoundaryMS, PriceMultiplier: 1 / action.Ratio,
		VolumeMultiplier: action.Ratio, EventType: historicalEventType(action.Exchange),
	})
	evidence := historicalEvidence(action, boundary)
	base := market.AdjustmentFactor{
		Provider: historicalProvider(action.Exchange), ProviderVersion: ProviderVersion,
		Exchange: action.Exchange, SourceMarket: action.SourceMarket, Symbol: action.Symbol,
		EventType: historicalEventType(action.Exchange), Raw: evidence,
	}
	result.Segments = CumulativeBackwardSegments(base, ledger)
	if b.cfg.DryRun {
		return result, nil
	}
	if err := b.store.ReplaceAdjustmentFactors(ctx, action.Exchange, action.SourceMarket, action.Symbol,
		market.PriceModeBackwardAdjusted, result.Segments); err != nil {
		return result, fmt.Errorf("replace adjustment factors: %w", err)
	}
	if err := b.persistBoundaryBars(ctx, action, boundary, officialOneMinute, result.Segments, &result); err != nil {
		return result, err
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
		Symbol: action.Symbol, EventType: historicalEventType(action.Exchange), State: market.CorporateActionStateFactor,
		FirstSeenMS: firstSeenMS, LastEventMS: boundary.BoundaryMS, ResumeMS: boundary.BoundaryMS,
		BoundaryMS: boundary.BoundaryMS, AnnouncedRatio: action.Ratio, Raw: evidence, UpdatedAtMS: market.NowMS(),
	}); err != nil {
		return fmt.Errorf("persist historical corporate action: %w", err)
	}
	return nil
}

func (b *HistoricalBackfiller) fetchMaterializationWindow(ctx context.Context, action HistoricalAction, boundaryMS int64) ([]market.Bar, error) {
	startMS := timeframe.FloorStartMS(boundaryMS, "1D")
	endMS := timeframe.EndMS(startMS, "1D")
	bars, err := b.source.FetchKlines(ctx, b.client, exchange.KlineRequest{
		Symbol: action.Symbol, Timeframe: deriverTimeframe, StartMS: startMS, EndMS: endMS,
	})
	if err != nil {
		return nil, fmt.Errorf("fetch official 1m materialization window: %w", err)
	}
	if len(bars) == 0 {
		return nil, fmt.Errorf("official 1m materialization window is empty")
	}
	for i := range bars {
		bars[i].Exchange = action.Exchange
		bars[i].SourceMarket = action.SourceMarket
		bars[i].Symbol = action.Symbol
		bars[i].Timeframe = deriverTimeframe
	}
	sort.Slice(bars, func(i, j int) bool { return bars[i].StartMS < bars[j].StartMS })
	return bars, nil
}

func (b *HistoricalBackfiller) persistBoundaryBars(ctx context.Context, action HistoricalAction, boundary Derivation, rawOneMinute []market.Bar, segments []market.AdjustmentFactor, result *HistoricalBackfillResult) error {
	rawBars, adjustedBars := rebuildBoundaryBars(action, boundary.BoundaryMS, rawOneMinute, segments)
	result.RawBarsRebuilt = len(rawBars)
	result.AdjustedBarsMaterialized = len(adjustedBars)
	if err := b.store.UpsertBars(ctx, rawBars); err != nil {
		return fmt.Errorf("upsert repaired boundary bars: %w", err)
	}
	if err := b.store.UpsertAdjustedBars(ctx, adjustedBars); err != nil {
		return fmt.Errorf("upsert materialized adjusted boundary bars: %w", err)
	}
	return nil
}

func rebuildBoundaryBars(action HistoricalAction, boundaryMS int64, rawOneMinute []market.Bar, segments []market.AdjustmentFactor) ([]market.Bar, []market.Bar) {
	nowMS := market.NowMS()
	rawBars := append([]market.Bar(nil), rawOneMinute...)
	adjustedOneMinute := make([]market.Bar, 0, len(rawOneMinute))
	for _, raw := range rawOneMinute {
		factor := factorAt(segments, market.BarAdjustmentTimestamp(raw))
		if factor == nil {
			continue
		}
		adjustedOneMinute = append(adjustedOneMinute, market.ApplyFactorToBar(raw, *factor))
	}
	adjustedBars := append([]market.Bar(nil), adjustedOneMinute...)

	for _, tf := range boundaryMaterializationTimeframes() {
		bucketStart := timeframe.FloorStartMS(boundaryMS, tf)
		bucketEnd := timeframe.EndMS(bucketStart, tf)
		rawBucket := activeBars(barsWithin(rawOneMinute, bucketStart, bucketEnd))
		adjustedBucket := activeBars(barsWithin(adjustedOneMinute, bucketStart, bucketEnd))
		if len(rawBucket) == 0 || len(adjustedBucket) != len(rawBucket) {
			continue
		}
		rawRollup := aggregator.RollupBars(tf, rawBucket, true, "official_boundary_rebuild", nowMS)
		adjustedRollup := aggregator.RollupBars(tf, adjustedBucket, true, "adjusted_1m_boundary_rebuild", nowMS)
		if rawRollup == nil || adjustedRollup == nil {
			continue
		}
		rawRollup.Exchange = action.Exchange
		rawRollup.SourceMarket = action.SourceMarket
		rawRollup.Symbol = action.Symbol
		adjustedRollup.Exchange = action.Exchange
		adjustedRollup.SourceMarket = action.SourceMarket
		adjustedRollup.Symbol = action.Symbol
		adjustedRollup.PriceMode = market.PriceModeBackwardAdjusted
		adjustedRollup.AdjustmentStatus = market.AdjustmentStatusAdjusted
		adjustedRollup.AdjustmentProvider = historicalProvider(action.Exchange)
		adjustedRollup.AdjustmentProviderVersion = ProviderVersion
		adjustedRollup.AdjustmentEventType = historicalEventType(action.Exchange)
		adjustedRollup.PriceMultiplier = 1
		adjustedRollup.VolumeMultiplier = 1
		adjustedRollup.RawOpenPrice = rawRollup.OpenPrice
		adjustedRollup.RawHighPrice = rawRollup.HighPrice
		adjustedRollup.RawLowPrice = rawRollup.LowPrice
		adjustedRollup.RawClosePrice = rawRollup.ClosePrice
		adjustedRollup.RawVolume = rawRollup.Volume
		adjustedRollup.RawQuoteVolume = rawRollup.QuoteVolume
		rawBars = append(rawBars, market.DecorateBar(*rawRollup))
		adjustedBars = append(adjustedBars, market.DecorateBar(*adjustedRollup))
	}
	return rawBars, adjustedBars
}

func activeBars(bars []market.Bar) []market.Bar {
	active := make([]market.Bar, 0, len(bars))
	for _, bar := range bars {
		if bar.Volume > 0 {
			active = append(active, bar)
		}
	}
	if len(active) == 0 {
		return bars
	}
	return active
}

func boundaryMaterializationTimeframes() []string {
	frames := make([]string, 0)
	for _, tf := range timeframe.Order {
		if tf == "1m" {
			continue
		}
		frames = append(frames, tf)
		if tf == "1D" {
			break
		}
	}
	return frames
}

func barsWithin(bars []market.Bar, startMS int64, endMS int64) []market.Bar {
	out := make([]market.Bar, 0)
	for _, bar := range bars {
		if bar.StartMS >= startMS && bar.StartMS <= endMS {
			out = append(out, bar)
		}
	}
	return out
}

func factorAt(segments []market.AdjustmentFactor, tsMS int64) *market.AdjustmentFactor {
	for i := range segments {
		factor := &segments[i]
		if factor.EffectiveFromMS <= tsMS && (factor.EffectiveToMS == 0 || tsMS <= factor.EffectiveToMS) {
			return factor
		}
	}
	return nil
}

func historicalProvider(exchangeName string) string {
	if strings.EqualFold(exchangeName, "binance") {
		return BinanceProviderName
	}
	return OKXProviderName
}

func historicalEventType(exchangeName string) string {
	if strings.EqualFold(exchangeName, "binance") {
		return HistoricalEventBinanceContractSize
	}
	return HistoricalEventOKXRebase
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
		"method": action.Exchange + "_historical_announcement", "announcement_code": action.AnnouncementCode,
		"announcement_title": action.Title, "official_ratio": action.Ratio,
		"predecessor_symbol": action.PredecessorSymbol,
		"boundary_ms":        boundary.BoundaryMS, "observed_ratio": boundary.Ratio,
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
