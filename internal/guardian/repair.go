package guardian

import (
	"context"
	"fmt"
	"math"
	"sort"

	"crypto-ticket/internal/exchange"
	"crypto-ticket/internal/market"
	"crypto-ticket/internal/timeframe"
)

type timeframeRepairRequest struct {
	Exchange        string
	Symbol          string
	Timeframe       string
	StartMS         int64
	EndMS           int64
	Reason          string
	ExpectData      bool
	ExpectedStartMS int64
	ForceReplay     bool
}

// repairTimeframeRange compares official completed bars directly with their
// stored timeframe. Daily repair therefore does not require complete hourly bars.
func (g *Guardian) repairTimeframeRange(ctx context.Context, fetcher Fetcher, request timeframeRepairRequest) (repairResult, error) {
	request.Exchange = normalizeExchange(request.Exchange)
	request.Symbol = normalizeSymbol(request.Symbol)
	tf, err := timeframe.Normalize(request.Timeframe)
	if err != nil {
		return repairResult{}, err
	}
	request.Timeframe = tf
	request.StartMS = timeframe.FloorStartMS(request.StartMS, tf)
	request.EndMS = timeframe.FloorStartMS(request.EndMS, tf)
	if request.EndMS < request.StartMS {
		return repairResult{}, nil
	}
	state, err := g.store.LoadKlineGuardianState(ctx, request.Exchange, request.Symbol, tf)
	if err != nil {
		return repairResult{}, err
	}
	if state != nil && state.Status == "repair_error" && request.StartMS <= state.LastCheckedEndMS && request.EndMS >= state.LastCheckedStartMS {
		request.ForceReplay = true
	}
	limit := 0
	for start := request.StartMS; start <= request.EndMS; start = timeframe.NextStartMS(start, tf) {
		limit++
	}
	official, err := g.fetchKlines(ctx, fetcher, exchange.KlineRequest{
		Symbol: request.Symbol, Timeframe: tf, StartMS: request.StartMS, EndMS: request.EndMS, Limit: limit,
		Adjustment: guardianAdjustment(request.Exchange),
	})
	if err != nil {
		g.recordRepairFailure(ctx, request, "rest_error", err)
		return repairResult{}, err
	}
	official, err = g.applyCorporateActionFactors(ctx, request.Exchange, request.Symbol, official)
	if err != nil {
		g.recordRepairFailure(ctx, request, "rest_error", err)
		return repairResult{}, err
	}
	local, err := g.store.BarsInRange(ctx, request.Exchange, request.Symbol, tf, request.StartMS, request.EndMS)
	if err != nil {
		return repairResult{}, err
	}
	validOfficial, rejected := filterOfficialBars(official, request, g.now().UnixMilli())
	var responseErr error
	if rejected > 0 || (len(official) > 0 && len(validOfficial) == 0) {
		responseErr = fmt.Errorf("invalid official %s response for %s:%s: accepted=%d rejected=%d", tf, request.Exchange, request.Symbol, len(validOfficial), rejected)
	}
	if len(validOfficial) == 0 {
		if len(local) > 0 || request.ExpectData || request.ForceReplay || responseErr != nil {
			if responseErr == nil {
				responseErr = fmt.Errorf("empty official %s response for %s:%s with existing or expected data", tf, request.Exchange, request.Symbol)
			}
			g.recordRepairFailure(ctx, request, "empty_response", responseErr)
			return repairResult{}, responseErr
		}
		// Before listing and on non-trading days an empty official range can be
		// legitimate, but it is not evidence that local market data is healthy.
		return repairResult{}, g.upsertTimeframeAuditState(ctx, request, "no_data")
	}
	if request.ExpectData {
		expectedStart := request.StartMS
		if request.ExpectedStartMS > expectedStart {
			expectedStart = timeframe.FloorStartMS(request.ExpectedStartMS, request.Timeframe)
		}
		for cursor := expectedStart; cursor <= request.EndMS; cursor = timeframe.NextStartMS(cursor, request.Timeframe) {
			found := false
			for _, bar := range validOfficial {
				if bar.StartMS == cursor {
					found = true
					break
				}
			}
			if !found {
				responseErr = fmt.Errorf("incomplete official %s response for %s:%s: missing start_ms=%d", request.Timeframe, request.Exchange, request.Symbol, cursor)
				break
			}
		}
	}
	unchecked := false
	if !request.ExpectData && request.ExpectedStartMS == 0 {
		// Registry has no listing watermark. A non-empty partial range cannot
		// establish that older buckets were never listed, so keep the state
		// explicitly unchecked instead of reporting it as healthy.
		expected := request.StartMS
		for cursor := expected; cursor <= request.EndMS; cursor = timeframe.NextStartMS(cursor, request.Timeframe) {
			found := false
			for _, bar := range validOfficial {
				if bar.StartMS == cursor {
					found = true
					break
				}
			}
			if !found {
				unchecked = true
				break
			}
		}
	}
	officialStarts := make(map[int64]bool, len(validOfficial))
	for _, bar := range validOfficial {
		officialStarts[bar.StartMS] = true
	}
	for _, bar := range local {
		if !officialStarts[bar.StartMS] {
			responseErr = fmt.Errorf("incomplete official %s response for %s:%s: missing existing start_ms=%d", tf, request.Exchange, request.Symbol, bar.StartMS)
			break
		}
	}
	result := g.diffTimeframeBars(request, validOfficial, local)
	toRepair := make([]market.Bar, 0, len(validOfficial))
	changed := make(map[int64]bool, len(result.Events))
	for _, event := range result.Events {
		changed[event.StartMS] = true
	}
	for _, bar := range validOfficial {
		if !changed[bar.StartMS] && !request.ForceReplay {
			continue
		}
		bar.Source = "rest"
		bar.Reason = "guardian_" + request.Reason
		bar.UpdatedAtMS = g.now().UnixMilli()
		toRepair = append(toRepair, market.DecorateBar(bar))
	}
	if len(toRepair) > 0 {
		if err := g.repairer.RepairFinalBars(ctx, toRepair); err != nil {
			g.recordRepairFailure(ctx, request, "repair_error", err)
			return result, err
		}
	}
	if err := g.insertEvents(ctx, result.Events); err != nil {
		return result, err
	}
	if responseErr != nil {
		g.recordRepairFailure(ctx, request, "incomplete_response", responseErr)
		return result, responseErr
	}
	status := "ok"
	if result.Repaired > 0 || request.ForceReplay {
		status = "repaired"
	}
	if unchecked {
		status = "unchecked"
	}
	return result, g.upsertTimeframeAuditState(ctx, request, status)
}

func filterOfficialBars(bars []market.Bar, request timeframeRepairRequest, nowMS int64) ([]market.Bar, int) {
	byStart := make(map[int64]market.Bar, len(bars))
	rejected := 0
	for _, bar := range bars {
		if bar.StartMS < request.StartMS || bar.StartMS > request.EndMS {
			continue
		}
		if normalizeExchange(bar.Exchange) != request.Exchange || normalizeSymbol(bar.Symbol) != request.Symbol ||
			bar.Timeframe != request.Timeframe || !bar.IsFinal || bar.StartMS <= 0 ||
			bar.StartMS != timeframe.FloorStartMS(bar.StartMS, request.Timeframe) ||
			bar.EndMS != timeframe.EndMS(bar.StartMS, request.Timeframe) || bar.EndMS >= nowMS || !validOfficialOHLC(bar) {
			rejected++
			continue
		}
		bar.Exchange, bar.Symbol = request.Exchange, request.Symbol
		byStart[bar.StartMS] = bar
	}
	accepted := make([]market.Bar, 0, len(byStart))
	for _, bar := range byStart {
		accepted = append(accepted, bar)
	}
	sort.Slice(accepted, func(i, j int) bool { return accepted[i].StartMS < accepted[j].StartMS })
	return accepted, rejected
}

func validOfficialOHLC(bar market.Bar) bool {
	for _, value := range []float64{bar.OpenPrice, bar.HighPrice, bar.LowPrice, bar.ClosePrice} {
		if value <= 0 || math.IsNaN(value) || math.IsInf(value, 0) {
			return false
		}
	}
	return bar.HighPrice >= math.Max(bar.OpenPrice, bar.ClosePrice) &&
		bar.LowPrice <= math.Min(bar.OpenPrice, bar.ClosePrice) && bar.HighPrice >= bar.LowPrice
}

func (g *Guardian) diffTimeframeBars(request timeframeRepairRequest, official []market.Bar, local []market.Bar) repairResult {
	localByStart := make(map[int64]market.Bar, len(local))
	for _, bar := range local {
		localByStart[bar.StartMS] = bar
	}
	result := repairResult{Checked: len(official)}
	for _, officialBar := range official {
		localBar, exists := localByStart[officialBar.StartMS]
		if exists && !barsDiffer(localBar, officialBar, g.cfg.FloatTolerance) {
			continue
		}
		event := market.KlineGuardianEvent{
			Exchange: request.Exchange, Symbol: request.Symbol, Timeframe: request.Timeframe,
			StartMS: officialBar.StartMS, EndMS: officialBar.EndMS, EventType: "missing_repair",
			NewValueJSON: barJSON(officialBar), CreatedAtMS: g.now().UnixMilli(),
		}
		if exists {
			event.EventType = "mismatch_repair"
			event.OldValueJSON = barJSON(localBar)
		}
		result.Repaired++
		result.Events = append(result.Events, event)
	}
	return result
}

func (g *Guardian) recordRepairFailure(ctx context.Context, request timeframeRepairRequest, status string, err error) {
	_ = g.insertEvents(ctx, []market.KlineGuardianEvent{{
		Exchange: request.Exchange, Symbol: request.Symbol, Timeframe: request.Timeframe,
		StartMS: request.StartMS, EndMS: request.EndMS, EventType: status,
		NewValueJSON: quoteError(err), CreatedAtMS: g.now().UnixMilli(),
	}})
	if request.ForceReplay {
		status = "repair_error"
	}
	_ = g.upsertTimeframeAuditState(ctx, request, status)
}

func (g *Guardian) upsertTimeframeAuditState(ctx context.Context, request timeframeRepairRequest, status string) error {
	state, err := g.store.LoadKlineGuardianState(ctx, request.Exchange, request.Symbol, request.Timeframe)
	if err != nil {
		return err
	}
	if state == nil {
		state = &market.KlineGuardianState{Exchange: request.Exchange, Symbol: request.Symbol, Timeframe: request.Timeframe}
	}
	// A successful unrelated range cannot erase a pending upper-rollup retry.
	if isRetryableDailyStatus(state.Status) && !isRetryableDailyStatus(status) &&
		(request.StartMS > state.LastCheckedStartMS || request.EndMS < state.LastCheckedEndMS) {
		return nil
	}
	if isRetryableDailyStatus(state.Status) && isRetryableDailyStatus(status) &&
		(request.StartMS > state.LastCheckedStartMS || request.EndMS < state.LastCheckedEndMS) {
		// Keep the older persisted retry until its own range is revisited. Other
		// failures remain in dailyRetries and will be retried each hourly pass.
		return nil
	}
	state.LastCheckedStartMS = request.StartMS
	state.LastCheckedEndMS = request.EndMS
	state.LastCheckedAtMS = g.now().UnixMilli()
	state.Status = status
	state.UpdatedAtMS = state.LastCheckedAtMS
	return g.store.UpsertKlineGuardianState(ctx, *state)
}

func (g *Guardian) retryFailedRepair(ctx context.Context, fetcher Fetcher, exchangeName string, symbol string, tf string) (repairResult, error) {
	state, err := g.store.LoadKlineGuardianState(ctx, exchangeName, symbol, tf)
	if err != nil || state == nil || state.Status != "repair_error" {
		return repairResult{}, err
	}
	return g.repairTimeframeRange(ctx, fetcher, timeframeRepairRequest{
		Exchange: exchangeName, Symbol: symbol, Timeframe: tf,
		StartMS: state.LastCheckedStartMS, EndMS: state.LastCheckedEndMS,
		Reason: "rollup_retry", ExpectData: true, ForceReplay: true,
	})
}
