package adjustment

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strings"
	"time"

	"crypto-ticket/internal/exchange"
	"crypto-ticket/internal/market"
	"crypto-ticket/internal/timeframe"
)

const (
	boundaryTimeframe                  = "1m"
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
	ReplaceBarsInRange(ctx context.Context, exchange string, symbol string, timeframe string, startMS int64, endMS int64, bars []market.Bar) error
	UpsertCorporateActionEvent(ctx context.Context, event market.CorporateActionEvent) error
}

type HistoricalKlineSource interface {
	FetchKlines(ctx context.Context, client *http.Client, request exchange.KlineRequest) ([]market.Bar, error)
}

type HistoricalBackfillConfig struct {
	BoundaryTolerancePct float64
	RequestDelay         time.Duration
	DryRun               bool
}

func (c HistoricalBackfillConfig) withDefaults() HistoricalBackfillConfig {
	if c.BoundaryTolerancePct <= 0 {
		c.BoundaryTolerancePct = 0.25
	}
	return c
}

type HistoricalBackfillResult struct {
	Action          HistoricalAction
	Boundary        Derivation
	BarsFetched     int
	RawBarsReplaced int
}

type Derivation struct {
	BoundaryMS  int64
	CloseBefore float64
	OpenAfter   float64
	Ratio       float64
}

type historicalOfficialWindow struct {
	oneMinute []market.Bar
	rawBars   []market.Bar
	ranges    map[string]historicalRange
}

type historicalRange struct {
	startMS int64
	endMS   int64
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
		return result, fmt.Errorf("historical corporate-action repair does not support exchange %q", action.Exchange)
	}
	if action.Symbol == "" || action.Ratio <= 0 || action.WindowStartMS <= 0 || action.WindowEndMS <= action.WindowStartMS {
		return result, fmt.Errorf("invalid historical action symbol=%q ratio=%f window=[%d,%d]", action.Symbol, action.Ratio, action.WindowStartMS, action.WindowEndMS)
	}
	bars, err := b.fetchKlinesWithRetry(ctx, exchange.KlineRequest{
		Symbol: action.Symbol, Timeframe: boundaryTimeframe,
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
	officialWindow, err := b.fetchOfficialRepairWindow(ctx, action, boundary.BoundaryMS)
	if err != nil {
		return result, err
	}
	result.BarsFetched = len(officialWindow.rawBars)
	evidence := historicalEvidence(action, boundary)
	if b.cfg.DryRun {
		return result, nil
	}
	if err := b.persistOfficialBars(ctx, action, officialWindow, &result); err != nil {
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
		Symbol: action.Symbol, EventType: historicalEventType(action.Exchange), State: market.CorporateActionStateRawRepaired,
		FirstSeenMS: firstSeenMS, LastEventMS: boundary.BoundaryMS, ResumeMS: boundary.BoundaryMS,
		BoundaryMS: boundary.BoundaryMS, AnnouncedRatio: action.Ratio, Raw: evidence, UpdatedAtMS: market.NowMS(),
	}); err != nil {
		return fmt.Errorf("persist historical corporate action: %w", err)
	}
	return nil
}

func (b *HistoricalBackfiller) fetchOfficialRepairWindow(ctx context.Context, action HistoricalAction, boundaryMS int64) (historicalOfficialWindow, error) {
	startMS := timeframe.FloorStartMS(boundaryMS, "1D")
	endMS := timeframe.EndMS(startMS, "1D")
	frames := officialRepairTimeframes(action.Exchange)
	window := historicalOfficialWindow{ranges: make(map[string]historicalRange, len(frames))}
	for _, tf := range frames {
		rangeStartMS, rangeEndMS := officialRepairRange(boundaryMS, tf, startMS, endMS)
		bars, err := b.fetchKlinesWithRetry(ctx, exchange.KlineRequest{
			Symbol: action.Symbol, Timeframe: tf, StartMS: rangeStartMS, EndMS: rangeEndMS,
			ForwardAdjusted: true,
		})
		if err != nil {
			return historicalOfficialWindow{}, fmt.Errorf("fetch official %s repair window: %w", tf, err)
		}
		if len(bars) == 0 {
			return historicalOfficialWindow{}, fmt.Errorf("official %s repair window is empty", tf)
		}
		for i := range bars {
			bars[i].Exchange = action.Exchange
			bars[i].SourceMarket = action.SourceMarket
			bars[i].Symbol = action.Symbol
			bars[i].Timeframe = tf
		}
		sort.Slice(bars, func(i, j int) bool { return bars[i].StartMS < bars[j].StartMS })
		if tf == boundaryTimeframe {
			window.oneMinute = bars
		}
		window.rawBars = append(window.rawBars, bars...)
		window.ranges[tf] = historicalRange{startMS: rangeStartMS, endMS: rangeEndMS}
		if err := waitHistoricalBackfill(ctx, b.cfg.RequestDelay); err != nil {
			return historicalOfficialWindow{}, err
		}
	}
	return window, nil
}

func officialRepairRange(boundaryMS int64, tf string, dayStartMS int64, dayEndMS int64) (int64, int64) {
	contextStart := timeframe.FloorStartMS(boundaryMS, tf)
	contextEndStart := contextStart
	for i := 0; i < 2; i++ {
		contextStart = timeframe.FloorStartMS(contextStart-1, tf)
		contextEndStart = timeframe.NextStartMS(contextEndStart, tf)
	}
	contextEnd := timeframe.EndMS(contextEndStart, tf)
	if contextStart > dayStartMS {
		contextStart = dayStartMS
	}
	if contextEnd < dayEndMS {
		contextEnd = dayEndMS
	}
	return contextStart, contextEnd
}

func (b *HistoricalBackfiller) fetchKlinesWithRetry(ctx context.Context, request exchange.KlineRequest) ([]market.Bar, error) {
	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		bars, err := b.source.FetchKlines(ctx, b.client, request)
		if err == nil {
			return bars, nil
		}
		lastErr = err
		if attempt == 3 {
			break
		}
		delay := time.Duration(1<<attempt) * 500 * time.Millisecond
		if err := waitHistoricalBackfill(ctx, delay); err != nil {
			return nil, err
		}
	}
	return nil, lastErr
}

func waitHistoricalBackfill(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (b *HistoricalBackfiller) persistOfficialBars(ctx context.Context, action HistoricalAction, window historicalOfficialWindow, result *HistoricalBackfillResult) error {
	result.RawBarsReplaced = len(window.rawBars)
	for _, tf := range officialRepairTimeframes(action.Exchange) {
		repairRange := window.ranges[tf]
		if err := b.store.ReplaceBarsInRange(ctx, action.Exchange, action.Symbol, tf, repairRange.startMS, repairRange.endMS, window.rawBars); err != nil {
			return fmt.Errorf("replace official %s bars in range: %w", tf, err)
		}
	}
	return nil
}

func officialRepairTimeframes(exchangeName string) []string {
	frames := []string{"1m", "5m", "15m", "30m", "1H", "2H", "4H", "6H", "12H", "1D"}
	if strings.EqualFold(exchangeName, "okx") {
		frames = append(frames, "2D")
	}
	// Binance has no official 2D interval, but both exchanges expose 1W.
	frames = append(frames, "1W")
	return frames
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
					Ratio: observed,
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
