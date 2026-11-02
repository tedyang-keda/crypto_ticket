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

// This is a selective fallback for absent daily candles, not a historical
// hourly backfill. Every request still uses Guardian's shared exchange budget.
const maxNoTradeDayChecksPerRange = 7

// confirmNoTradeDays never interprets a weekend, an empty REST response, a
// registry observation time, or today's market status as historical closure.
// Only a complete official UTC day of zero-activity hourly candles is evidence
// to exclude that date from the expectation of a daily candle.
func (g *Guardian) confirmNoTradeDays(ctx context.Context, fetcher Fetcher, request timeframeRepairRequest, official, local []market.Bar) (map[int64]bool, error) {
	confirmed := make(map[int64]bool)
	officialDays := make(map[int64]bool, len(official))
	for _, bar := range official {
		officialDays[bar.StartMS] = true
	}
	localDays := make(map[int64]market.Bar, len(local))
	for _, bar := range local {
		localDays[bar.StartMS] = bar
	}
	checked := 0
	for day := request.StartMS; day <= request.EndMS && checked < maxNoTradeDayChecksPerRange; day = timeframe.NextStartMS(day, "1D") {
		if officialDays[day] {
			continue
		}
		if bar, exists := localDays[day]; exists && (!zeroTradeActivity(bar) || !validOfficialOHLC(bar)) {
			continue
		}
		end := timeframe.EndMS(day, "1D")
		localHours, err := g.store.BarsInRange(ctx, request.Exchange, request.Symbol, "1H", day, end)
		if err != nil {
			return nil, fmt.Errorf("read local hourly no-trade evidence: %w", err)
		}
		contradiction := false
		for _, bar := range localHours {
			if !zeroTradeActivity(bar) || !validOfficialOHLC(bar) {
				contradiction = true
				break
			}
		}
		if contradiction {
			continue
		}
		checked++
		hourRequest := timeframeRepairRequest{
			Exchange: request.Exchange, Symbol: request.Symbol, Timeframe: "1H",
			StartMS: day, EndMS: end - timeframe.DurationMS("1H") + 1,
		}
		hours, err := g.fetchKlines(ctx, fetcher, exchange.KlineRequest{
			Symbol: request.Symbol, Timeframe: "1H", StartMS: hourRequest.StartMS,
			EndMS: hourRequest.EndMS, Limit: 24, Adjustment: guardianAdjustment(request.Exchange),
		})
		if err != nil {
			return nil, fmt.Errorf("fetch official hourly no-trade evidence for %s:%s day=%d: %w", request.Exchange, request.Symbol, day, err)
		}
		// Validate the original cardinality too: duplicate or extra candles must
		// not disappear in filterOfficialBars' normal deduplication step.
		if len(hours) != 24 {
			continue
		}
		valid, rejected := filterOfficialBars(hours, hourRequest, g.now().UnixMilli())
		if rejected != 0 || len(valid) != 24 {
			continue
		}
		completeZeroDay := true
		for i, bar := range valid {
			if bar.StartMS != day+int64(i)*timeframe.DurationMS("1H") || !zeroTradeActivity(bar) {
				completeZeroDay = false
				break
			}
		}
		if completeZeroDay {
			confirmed[day] = true
		}
	}
	return confirmed, nil
}

func zeroTradeActivity(bar market.Bar) bool {
	if bar.TradeCount != 0 {
		return false
	}
	for _, value := range []float64{bar.Volume, bar.QuoteVolume, bar.ContractVolume} {
		if value != 0 || math.IsNaN(value) || math.IsInf(value, 0) {
			return false
		}
	}
	return true
}

func (g *Guardian) noTradeDayEvents(request timeframeRepairRequest, days map[int64]bool) []market.KlineGuardianEvent {
	starts := make([]int64, 0, len(days))
	for start := range days {
		starts = append(starts, start)
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })
	events := make([]market.KlineGuardianEvent, 0, len(starts))
	for _, start := range starts {
		events = append(events, market.KlineGuardianEvent{
			Exchange: request.Exchange, Symbol: request.Symbol, Timeframe: "1D",
			StartMS: start, EndMS: timeframe.EndMS(start, "1D"), EventType: "confirmed_no_trades",
			NewValueJSON: `{"evidence":"official_1H","final_hours":24,"zero_activity":true,"local_activity_conflict":false}`,
			CreatedAtMS:  g.now().UnixMilli(),
		})
	}
	return events
}
