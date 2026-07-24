package exchange

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"crypto-ticket/internal/market"
	"crypto-ticket/internal/timeframe"
)

var ErrUnsupportedKlineInterval = errors.New("unsupported REST kline interval")

const (
	binanceMaxKlineLimit = 1500
	okxRecentKlineLimit  = 300
	okxHistoryKlineLimit = 100
	defaultKlineLimit    = 300
)

type KlineRequest struct {
	Symbol            string
	Timeframe         string
	StartMS           int64
	EndMS             int64
	Limit             int
	ForwardAdjusted   bool
	IncludeIncomplete bool
}

type RESTKlineFetcher interface {
	Name() string
	MarketType() string
	FetchKlines(ctx context.Context, client *http.Client, request KlineRequest) ([]market.Bar, error)
}

func (a *BinanceFuturesAdapter) FetchKlines(ctx context.Context, client *http.Client, request KlineRequest) ([]market.Bar, error) {
	interval, err := binanceKlineInterval(request.Timeframe)
	if err != nil {
		return nil, err
	}
	request.Symbol = strings.ToUpper(strings.TrimSpace(request.Symbol))
	request.Timeframe = timeframe.MustNormalize(request.Timeframe)
	if request.Symbol == "" {
		return nil, nil
	}

	path := "/api/v3/klines"
	if a.marketType == "" || strings.EqualFold(a.marketType, "um_futures") {
		path = "/fapi/v1/klines"
	} else if a.marginType() == "coinmargin" {
		path = "/dapi/v1/klines"
	}

	var all []market.Bar
	remaining := request.Limit
	if remaining <= 0 && request.StartMS == 0 {
		remaining = defaultKlineLimit
	}
	cursorStart := request.StartMS
	cursorEnd := request.EndMS
	if cursorEnd <= 0 {
		cursorEnd = timeframe.FloorStartMS(market.NowMS(), request.Timeframe) - 1
		if request.IncludeIncomplete {
			cursorEnd = market.NowMS()
		}
	}

	for {
		pageLimit := binanceMaxKlineLimit
		if remaining > 0 && remaining < pageLimit {
			pageLimit = remaining
		}
		endpoint, err := url.Parse(a.restURL + path)
		if err != nil {
			return nil, err
		}
		query := endpoint.Query()
		query.Set("symbol", request.Symbol)
		query.Set("interval", interval)
		query.Set("limit", strconv.Itoa(pageLimit))
		if cursorStart > 0 {
			query.Set("startTime", strconv.FormatInt(cursorStart, 10))
		}
		if cursorEnd > 0 {
			query.Set("endTime", strconv.FormatInt(cursorEnd, 10))
		}
		endpoint.RawQuery = query.Encode()

		page, err := a.fetchBinanceKlinePage(ctx, client, endpoint.String(), request.Symbol, request.Timeframe, request.IncludeIncomplete)
		if err != nil {
			return nil, err
		}
		for _, bar := range page {
			if request.StartMS > 0 && bar.StartMS < request.StartMS {
				continue
			}
			if request.EndMS > 0 && bar.StartMS > request.EndMS {
				continue
			}
			all = append(all, bar)
		}
		if len(page) == 0 || request.StartMS == 0 {
			break
		}
		if remaining > 0 {
			remaining -= len(page)
			if remaining <= 0 {
				break
			}
		}
		nextStart := timeframe.NextStartMS(page[len(page)-1].StartMS, request.Timeframe)
		if nextStart > cursorEnd || nextStart <= cursorStart {
			break
		}
		cursorStart = nextStart
		if len(page) < pageLimit {
			break
		}
	}
	sortBars(all)
	return trimBars(all, request.Limit), nil
}

func (a *BinanceFuturesAdapter) fetchBinanceKlinePage(ctx context.Context, client *http.Client, endpoint string, symbol string, tf string, includeIncomplete bool) ([]market.Bar, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("binance klines status %s", resp.Status)
	}

	var rows [][]any
	decoder := json.NewDecoder(resp.Body)
	decoder.UseNumber()
	if err := decoder.Decode(&rows); err != nil {
		return nil, err
	}
	now := market.NowMS()
	bars := make([]market.Bar, 0, len(rows))
	marginType := a.marginType()
	base, quote := binanceSymbolCurrencies(symbol, marginType)
	for _, row := range rows {
		if len(row) < 9 {
			continue
		}
		startMS := intValue(row[0])
		closeMS := intValue(row[6])
		if startMS <= 0 || closeMS <= 0 || (!includeIncomplete && closeMS >= now) {
			continue
		}
		volume := floatValue(row[5])
		quoteVolume := floatValue(row[7])
		volumeUnit := base
		quoteUnit := quote
		contractVolume := float64(0)
		if marginType == "coinmargin" {
			contractVolume = volume
			volumeUnit = "contract"
			quoteUnit = base
		}
		bar := market.Bar{
			Exchange:       a.Name(),
			Symbol:         symbol,
			MarginType:     marginType,
			Timeframe:      tf,
			StartMS:        startMS,
			EndMS:          closeMS,
			OpenPrice:      floatValue(row[1]),
			HighPrice:      floatValue(row[2]),
			LowPrice:       floatValue(row[3]),
			ClosePrice:     floatValue(row[4]),
			Volume:         volume,
			VolumeUnit:     volumeUnit,
			QuoteVolume:    quoteVolume,
			QuoteUnit:      quoteUnit,
			ContractVolume: contractVolume,
			TradeCount:     intValue(row[8]),
			LastTickMS:     closeMS,
			IsFinal:        closeMS < now,
			Source:         "rest",
			Reason:         "exchange_kline_backfill",
			UpdatedAtMS:    now,
		}
		bar = market.ApplyClassificationFieldsToBar(bar, binanceDefaultClassification(a.marketType))
		if validOHLC(bar) {
			bars = append(bars, market.DecorateBar(bar))
		}
	}
	return bars, nil
}

func (a *OKXAdapter) FetchKlines(ctx context.Context, client *http.Client, request KlineRequest) ([]market.Bar, error) {
	bar, err := okxKlineBar(request.Timeframe)
	if err != nil {
		return nil, err
	}
	request.Symbol = strings.ToUpper(strings.TrimSpace(request.Symbol))
	request.Timeframe = timeframe.MustNormalize(request.Timeframe)
	if request.Symbol == "" {
		return nil, nil
	}

	var all []market.Bar
	seen := make(map[int64]bool)
	remaining := request.Limit
	if remaining <= 0 && request.StartMS == 0 {
		remaining = defaultKlineLimit
	}
	after := int64(0)
	if request.EndMS > 0 {
		// OKX may mark the first zero-volume candle below `after` as
		// unconfirmed, even for old history. Look one complete bucket past the
		// requested end so the last requested candle has a stable confirm flag.
		endStartMS := timeframe.FloorStartMS(request.EndMS, request.Timeframe)
		after = timeframe.NextStartMS(timeframe.NextStartMS(endStartMS, request.Timeframe), request.Timeframe)
	}

	useRecentEndpoint := request.StartMS == 0 && request.EndMS == 0
	for {
		pageLimit := okxHistoryKlineLimit
		endpointPath := "/api/v5/market/history-candles"
		if useRecentEndpoint && after == 0 {
			endpointPath = "/api/v5/market/candles"
			pageLimit = okxRecentKlineLimit
		}
		if remaining > 0 && remaining < pageLimit {
			pageLimit = remaining
		}
		page, err := a.fetchOKXKlinePage(ctx, client, endpointPath, request.Symbol, request.Timeframe, bar, after, pageLimit, request.ForwardAdjusted, request.IncludeIncomplete)
		if err != nil {
			return nil, err
		}
		if len(page) == 0 && endpointPath == "/api/v5/market/candles" {
			useRecentEndpoint = false
			continue
		}
		if len(page) == 0 {
			break
		}
		oldest := page[0].StartMS
		for _, item := range page {
			if item.StartMS < oldest {
				oldest = item.StartMS
			}
			if request.StartMS > 0 && item.StartMS < request.StartMS {
				continue
			}
			if request.EndMS > 0 && item.StartMS > request.EndMS {
				continue
			}
			if !seen[item.StartMS] {
				seen[item.StartMS] = true
				all = append(all, item)
			}
		}
		if remaining > 0 {
			remaining -= len(page)
			if remaining <= 0 {
				break
			}
		}
		if request.StartMS > 0 && oldest <= request.StartMS {
			break
		}
		if after == oldest {
			break
		}
		useRecentEndpoint = false
		after = oldest
	}
	sortBars(all)
	return trimBars(all, request.Limit), nil
}

func (a *OKXAdapter) fetchOKXKlinePage(ctx context.Context, client *http.Client, endpointPath string, symbol string, tf string, bar string, after int64, limit int, forwardAdjusted bool, includeIncomplete bool) ([]market.Bar, error) {
	endpoint, err := url.Parse(a.restURL + endpointPath)
	if err != nil {
		return nil, err
	}
	query := endpoint.Query()
	query.Set("instId", symbol)
	query.Set("bar", bar)
	query.Set("limit", strconv.Itoa(limit))
	if forwardAdjusted {
		query.Set("adjust", "forward")
	}
	if after > 0 {
		query.Set("after", strconv.FormatInt(after, 10))
	}
	endpoint.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("okx candles status %s path=%s", resp.Status, endpointPath)
	}
	var payload struct {
		Code string     `json:"code"`
		Msg  string     `json:"msg"`
		Data [][]string `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}
	if payload.Code != "" && payload.Code != "0" {
		return nil, fmt.Errorf("okx candles code=%s msg=%s path=%s", payload.Code, payload.Msg, endpointPath)
	}
	now := market.NowMS()
	bars := make([]market.Bar, 0, len(payload.Data))
	base, quote := inferOKXSymbolCurrencies(symbol)
	spec, ok := a.instrumentSpec(symbol)
	if ok {
		if spec.baseCcy != "" {
			base = spec.baseCcy
		}
		if spec.quoteCcy != "" {
			quote = spec.quoteCcy
		}
	}
	marginType := okxMarginType(symbol, spec)
	for _, row := range payload.Data {
		if len(row) < 9 || (!includeIncomplete && row[8] != "1") {
			continue
		}
		startMS := parseInt(row[0])
		endMS := timeframe.EndMS(startMS, tf)
		if startMS <= 0 || (!includeIncomplete && endMS >= now) {
			continue
		}
		contractVolume := parseFloat(row[5])
		volume := parseFloat(row[6])
		quoteVolume := parseFloat(row[7])
		volumeUnit := base
		quoteUnit := quote
		if marginType == "coinmargin" {
			volume = contractVolume
			volumeUnit = "contract"
			quoteVolume = parseFloat(row[6])
			quoteUnit = base
		}
		bar := market.Bar{
			Exchange:       a.Name(),
			Symbol:         symbol,
			MarginType:     marginType,
			Timeframe:      tf,
			StartMS:        startMS,
			EndMS:          endMS,
			OpenPrice:      parseFloat(row[1]),
			HighPrice:      parseFloat(row[2]),
			LowPrice:       parseFloat(row[3]),
			ClosePrice:     parseFloat(row[4]),
			Volume:         volume,
			VolumeUnit:     volumeUnit,
			QuoteVolume:    quoteVolume,
			QuoteUnit:      quoteUnit,
			ContractVolume: contractVolume,
			TradeCount:     0,
			LastTickMS:     endMS,
			IsFinal:        row[8] == "1",
			Source:         "rest",
			Reason:         "exchange_kline_backfill",
			UpdatedAtMS:    now,
		}
		if ok {
			bar = market.ApplyClassificationFieldsToBar(bar, spec.classification)
		} else {
			bar = market.ApplyClassificationFieldsToBar(bar, okxDefaultClassification(a.instType))
		}
		if validOHLC(bar) {
			bars = append(bars, market.DecorateBar(bar))
		}
	}
	sortBars(bars)
	return bars, nil
}

func binanceKlineInterval(tf string) (string, error) {
	switch timeframe.MustNormalize(tf) {
	case "1m":
		return "1m", nil
	case "5m":
		return "5m", nil
	case "15m":
		return "15m", nil
	case "30m":
		return "30m", nil
	case "1H":
		return "1h", nil
	case "2H":
		return "2h", nil
	case "4H":
		return "4h", nil
	case "6H":
		return "6h", nil
	case "12H":
		return "12h", nil
	case "1D":
		return "1d", nil
	case "3D":
		return "3d", nil
	case "1W":
		return "1w", nil
	case "1M":
		return "1M", nil
	default:
		return "", fmt.Errorf("%w: binance timeframe %s", ErrUnsupportedKlineInterval, tf)
	}
}

func okxKlineBar(tf string) (string, error) {
	switch timeframe.MustNormalize(tf) {
	case "1m":
		return "1m", nil
	case "5m":
		return "5m", nil
	case "15m":
		return "15m", nil
	case "30m":
		return "30m", nil
	case "1H":
		return "1H", nil
	case "2H":
		return "2H", nil
	case "4H":
		return "4H", nil
	case "6H":
		return "6Hutc", nil
	case "12H":
		return "12Hutc", nil
	case "1D":
		return "1Dutc", nil
	case "2D":
		return "2Dutc", nil
	case "3D":
		return "3Dutc", nil
	case "1W":
		return "1Wutc", nil
	case "1M":
		return "1Mutc", nil
	case "3M":
		return "3Mutc", nil
	default:
		return "", fmt.Errorf("%w: okx timeframe %s", ErrUnsupportedKlineInterval, tf)
	}
}

func sortBars(bars []market.Bar) {
	sort.Slice(bars, func(i, j int) bool {
		if bars[i].StartMS == bars[j].StartMS {
			return bars[i].Exchange+bars[i].Symbol+bars[i].Timeframe < bars[j].Exchange+bars[j].Symbol+bars[j].Timeframe
		}
		return bars[i].StartMS < bars[j].StartMS
	})
}

func trimBars(bars []market.Bar, limit int) []market.Bar {
	if limit <= 0 || len(bars) <= limit {
		return bars
	}
	return bars[len(bars)-limit:]
}

func validOHLC(bar market.Bar) bool {
	return bar.OpenPrice > 0 && bar.HighPrice > 0 && bar.LowPrice > 0 && bar.ClosePrice > 0
}
