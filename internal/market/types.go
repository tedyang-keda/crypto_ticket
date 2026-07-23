package market

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	AssetClassCrypto    = "crypto"
	AssetClassEquity    = "equity"
	AssetClassKREquity  = "kr_equity"
	AssetClassCommodity = "commodity"
	AssetClassIndex     = "index"
	AssetClassForex     = "forex"
	AssetClassBonds     = "bonds"
	AssetClassPreMarket = "pre_market"
	AssetClassUnknown   = "unknown"

	RuleNormal    = "normal"
	RulePreMarket = "pre_market"

	PhaseContinuous = "continuous"
	PhasePreMarket  = "pre_market"
	PhasePreopen    = "preopen"
	PhaseRebase     = "rebase"
	PhaseHalt       = "trading_halt"
	PhaseCancelOnly = "trading_cancel_only"
	PhaseSuspend    = "suspend"
	PhaseExpired    = "expired"
	PhaseUnknown    = "unknown"

	PriceModeRaw              = "raw"
	PriceModeForwardAdjusted  = "forward_adjusted"
	PriceModeBackwardAdjusted = "backward_adjusted"

	AdjustmentStatusRaw       = "raw"
	AdjustmentStatusAdjusted  = "adjusted"
	AdjustmentStatusMissing   = "missing_factor"
	AdjustmentStatusLiveRaw   = "live_raw"
	AdjustmentProviderRuntime = "runtime_factor"

	// OKX exposes rebase-eligible contracts via ruleType; rebase itself is a
	// lifecycle state. These are the machine-readable signals for a corporate
	// action (split / pre-IPO share rebase) on equity and pre-market perps.
	RuleRebaseContract = "rebase_contract"

	// Instrument change event types emitted by the instruments-channel monitor.
	InstrumentEventRebase      = "rebase_detected"
	InstrumentEventRebaseArmed = "rebase_contract_armed"
	InstrumentEventSuspended   = "equity_suspended"
	InstrumentEventCancelOnly  = "equity_cancel_only"
	InstrumentEventResumed     = "equity_resumed"
	InstrumentEventDelisted    = "equity_delisted"
	InstrumentEventRenamed     = "instrument_renamed"

	CorporateActionStateDiscovered   = "DISCOVERED"
	CorporateActionStateHalt         = "TRADING_HALT"
	CorporateActionStateCancelOnly   = "TRADING_CANCEL_ONLY"
	CorporateActionStateResumed      = "RESUMED"
	CorporateActionStateFactor       = "FACTOR_WRITTEN"
	CorporateActionStateManualReview = "MANUAL_REVIEW"
)

var ErrUnsupportedPriceMode = errors.New("unsupported price mode")

type Tick struct {
	Exchange         string          `json:"exchange"`
	SourceMarket     string          `json:"source_market,omitempty"`
	Symbol           string          `json:"symbol"`
	InstrumentType   string          `json:"instrument_type,omitempty"`
	AssetClass       string          `json:"asset_class,omitempty"`
	RuleType         string          `json:"rule_type,omitempty"`
	LifecyclePhase   string          `json:"lifecycle_phase,omitempty"`
	PriceMode        string          `json:"price_mode,omitempty"`
	AdjustmentStatus string          `json:"adjustment_status,omitempty"`
	RawPrice         float64         `json:"raw_price,omitempty"`
	AdjustedPrice    float64         `json:"adjusted_price,omitempty"`
	RawSize          float64         `json:"raw_size,omitempty"`
	AdjustedSize     float64         `json:"adjusted_size,omitempty"`
	TsMS             int64           `json:"ts_ms"`
	Price            float64         `json:"price"`
	Size             float64         `json:"size"`
	Side             string          `json:"side,omitempty"`
	TradeID          string          `json:"trade_id,omitempty"`
	EventType        string          `json:"event_type"`
	Source           string          `json:"source"`
	RecvMS           int64           `json:"recv_ms,omitempty"`
	Raw              json.RawMessage `json:"raw,omitempty"`
}

type Bar struct {
	Exchange       string  `json:"exchange"`
	SourceMarket   string  `json:"source_market,omitempty"`
	Symbol         string  `json:"symbol"`
	InstrumentType string  `json:"instrument_type,omitempty"`
	AssetClass     string  `json:"asset_class,omitempty"`
	RuleType       string  `json:"rule_type,omitempty"`
	LifecyclePhase string  `json:"lifecycle_phase,omitempty"`
	MarginType     string  `json:"margin_type,omitempty"`
	Timeframe      string  `json:"timeframe"`
	StartMS        int64   `json:"start_ms"`
	EndMS          int64   `json:"end_ms"`
	StartTS        int64   `json:"startts,omitempty"`
	EndTS          int64   `json:"endts,omitempty"`
	OpenPrice      float64 `json:"open_price"`
	HighPrice      float64 `json:"high_price"`
	LowPrice       float64 `json:"low_price"`
	ClosePrice     float64 `json:"close_price"`
	Open           float64 `json:"open,omitempty"`
	High           float64 `json:"high,omitempty"`
	Low            float64 `json:"low,omitempty"`
	Close          float64 `json:"close,omitempty"`
	PrevClose      float64 `json:"prev_close,omitempty"`
	Chg            float64 `json:"chg,omitempty"`
	Amp            float64 `json:"amp,omitempty"`
	Volume         float64 `json:"volume"`
	VolumeUnit     string  `json:"volume_unit,omitempty"`
	QuoteVolume    float64 `json:"quote_volume"`
	Quote          float64 `json:"quote,omitempty"`
	QuoteUnit      string  `json:"quote_unit,omitempty"`
	ContractVolume float64 `json:"contract_volume,omitempty"`
	TradeCount     int64   `json:"trade_count"`
	LastTickMS     int64   `json:"last_tick_ms"`
	IsFinal        bool    `json:"is_final"`
	Source         string  `json:"source"`
	Reason         string  `json:"reason"`
	UpdatedAtMS    int64   `json:"updated_at_ms"`

	PriceMode                 string  `json:"price_mode,omitempty"`
	AdjustmentStatus          string  `json:"adjustment_status,omitempty"`
	AdjustmentProvider        string  `json:"adjustment_provider,omitempty"`
	AdjustmentProviderVersion string  `json:"adjustment_provider_version,omitempty"`
	AdjustmentEventType       string  `json:"adjustment_event_type,omitempty"`
	PriceMultiplier           float64 `json:"price_multiplier,omitempty"`
	VolumeMultiplier          float64 `json:"volume_multiplier,omitempty"`
	RawOpenPrice              float64 `json:"raw_open_price,omitempty"`
	RawHighPrice              float64 `json:"raw_high_price,omitempty"`
	RawLowPrice               float64 `json:"raw_low_price,omitempty"`
	RawClosePrice             float64 `json:"raw_close_price,omitempty"`
	RawVolume                 float64 `json:"raw_volume,omitempty"`
	RawQuoteVolume            float64 `json:"raw_quote_volume,omitempty"`
}

type SymbolInfo struct {
	Exchange       string          `json:"exchange"`
	SourceMarket   string          `json:"source_market,omitempty"`
	Symbol         string          `json:"symbol"`
	MarketType     string          `json:"market_type"`
	InstrumentType string          `json:"instrument_type,omitempty"`
	AssetClass     string          `json:"asset_class,omitempty"`
	RuleType       string          `json:"rule_type,omitempty"`
	LifecyclePhase string          `json:"lifecycle_phase,omitempty"`
	Status         string          `json:"status"`
	IsActive       bool            `json:"is_active"`
	FirstSeenAtMS  int64           `json:"first_seen_at_ms"`
	LastSeenAtMS   int64           `json:"last_seen_at_ms"`
	UpdatedAtMS    int64           `json:"updated_at_ms"`
	Raw            json.RawMessage `json:"raw,omitempty"`
}

type InstrumentClassification struct {
	SourceMarket   string
	InstrumentType string
	AssetClass     string
	RuleType       string
	LifecyclePhase string
}

type KlineQuery struct {
	Exchange     string
	SourceMarket string
	Symbol       string
	Timeframe    string
	Limit        int
	IncludeLive  bool
	PriceMode    string
}

type AdjustmentFactor struct {
	Provider         string          `json:"provider"`
	ProviderVersion  string          `json:"provider_version"`
	Exchange         string          `json:"exchange"`
	SourceMarket     string          `json:"source_market,omitempty"`
	Symbol           string          `json:"symbol"`
	AdjMode          string          `json:"adj_mode"`
	EffectiveFromMS  int64           `json:"effective_from_ms"`
	EffectiveToMS    int64           `json:"effective_to_ms"`
	PriceMultiplier  float64         `json:"price_multiplier"`
	VolumeMultiplier float64         `json:"volume_multiplier"`
	EventType        string          `json:"event_type,omitempty"`
	Raw              json.RawMessage `json:"raw,omitempty"`
}

type InstrumentChangeEvent struct {
	Exchange     string          `json:"exchange"`
	SourceMarket string          `json:"source_market,omitempty"`
	Symbol       string          `json:"symbol"`
	EventTSMS    int64           `json:"event_ts_ms"`
	EventType    string          `json:"event_type"`
	PreviousHash string          `json:"previous_hash,omitempty"`
	CurrentHash  string          `json:"current_hash,omitempty"`
	PreviousJSON json.RawMessage `json:"previous_json,omitempty"`
	CurrentJSON  json.RawMessage `json:"current_json,omitempty"`
}

// CorporateActionEvent is the durable lifecycle record for one adjustment.
// Raw exchange messages remain attached as evidence because Binance's CMS API
// is public but unversioned.
type CorporateActionEvent struct {
	ActionID       string          `json:"action_id"`
	Exchange       string          `json:"exchange"`
	SourceMarket   string          `json:"source_market,omitempty"`
	Symbol         string          `json:"symbol"`
	EventType      string          `json:"event_type"`
	State          string          `json:"state"`
	FirstSeenMS    int64           `json:"first_seen_ms"`
	LastEventMS    int64           `json:"last_event_ms"`
	ResumeMS       int64           `json:"resume_ms,omitempty"`
	BoundaryMS     int64           `json:"boundary_ms,omitempty"`
	AnnouncedRatio float64         `json:"announced_ratio,omitempty"`
	Attempts       int             `json:"attempts"`
	LastError      string          `json:"last_error,omitempty"`
	Raw            json.RawMessage `json:"raw,omitempty"`
	UpdatedAtMS    int64           `json:"updated_at_ms"`
}

type Event struct {
	Type      string `json:"type"`
	Seq       int64  `json:"seq"`
	Exchange  string `json:"exchange"`
	Symbol    string `json:"symbol"`
	Timeframe string `json:"timeframe,omitempty"`
	Tick      *Tick  `json:"tick,omitempty"`
	Bar       *Bar   `json:"bar,omitempty"`
}

type KlineGuardianState struct {
	Exchange           string `json:"exchange"`
	Symbol             string `json:"symbol"`
	Timeframe          string `json:"timeframe"`
	LastFinalStartMS   int64  `json:"last_final_start_ms"`
	LastFinalRecvMS    int64  `json:"last_final_recv_ms"`
	LastCheckedStartMS int64  `json:"last_checked_start_ms"`
	LastCheckedEndMS   int64  `json:"last_checked_end_ms"`
	LastCheckedAtMS    int64  `json:"last_checked_at_ms"`
	LastGapStartMS     int64  `json:"last_gap_start_ms"`
	LastGapEndMS       int64  `json:"last_gap_end_ms"`
	Status             string `json:"status"`
	UpdatedAtMS        int64  `json:"updated_at_ms"`
}

type KlineGuardianEvent struct {
	ID           int64  `json:"id,omitempty"`
	Exchange     string `json:"exchange"`
	Symbol       string `json:"symbol"`
	Timeframe    string `json:"timeframe"`
	StartMS      int64  `json:"start_ms"`
	EndMS        int64  `json:"end_ms"`
	EventType    string `json:"event_type"`
	OldValueJSON string `json:"old_value_json,omitempty"`
	NewValueJSON string `json:"new_value_json,omitempty"`
	CreatedAtMS  int64  `json:"created_at_ms"`
}

func NowMS() int64 {
	return time.Now().UnixMilli()
}

func DecorateBar(bar Bar) Bar {
	bar.StartTS = bar.StartMS
	bar.EndTS = bar.EndMS
	bar.Open = bar.OpenPrice
	bar.High = bar.HighPrice
	bar.Low = bar.LowPrice
	bar.Close = bar.ClosePrice
	bar.Quote = bar.QuoteVolume
	return bar
}

func SourceMarket(exchange string, marketType string) string {
	exchange = strings.ToLower(strings.TrimSpace(exchange))
	marketType = strings.TrimSpace(marketType)
	if exchange == "" && marketType == "" {
		return ""
	}
	return exchange + ":" + marketType
}

func NormalizePriceMode(value string) (string, error) {
	mode := strings.ToLower(strings.TrimSpace(value))
	if mode == "" {
		mode = PriceModeRaw
	}
	switch mode {
	case PriceModeRaw, PriceModeForwardAdjusted, PriceModeBackwardAdjusted:
		return mode, nil
	default:
		return "", fmt.Errorf("%w: %s", ErrUnsupportedPriceMode, value)
	}
}

func MustNormalizePriceMode(value string) string {
	mode, err := NormalizePriceMode(value)
	if err != nil {
		panic(err)
	}
	return mode
}

func IsAdjustedPriceMode(value string) bool {
	mode, err := NormalizePriceMode(value)
	return err == nil && mode != PriceModeRaw
}

func InstrumentSignature(symbol SymbolInfo) string {
	body, _ := json.Marshal(map[string]any{
		"source_market":   symbol.SourceMarket,
		"instrument_type": symbol.InstrumentType,
		"asset_class":     symbol.AssetClass,
		"rule_type":       symbol.RuleType,
		"lifecycle_phase": symbol.LifecyclePhase,
		"status":          symbol.Status,
		"raw":             relevantRawFields(symbol.Raw),
	})
	sum := sha256.Sum256(body)
	return fmt.Sprintf("%x", sum)
}

func InstrumentChangeEventType(previous SymbolInfo, current SymbolInfo) string {
	switch {
	case previous.RuleType != current.RuleType:
		return "rule_type_changed"
	case previous.LifecyclePhase != current.LifecyclePhase:
		return "lifecycle_phase_changed"
	case previous.AssetClass != current.AssetClass:
		return "asset_class_changed"
	case previous.InstrumentType != current.InstrumentType:
		return "instrument_type_changed"
	default:
		return "metadata_changed"
	}
}

func ApplyFactorToBar(bar Bar, factor AdjustmentFactor) Bar {
	priceMultiplier := factor.PriceMultiplier
	if priceMultiplier == 0 {
		priceMultiplier = 1
	}
	volumeMultiplier := factor.VolumeMultiplier
	if volumeMultiplier == 0 {
		volumeMultiplier = 1
	}
	bar.RawOpenPrice = bar.OpenPrice
	bar.RawHighPrice = bar.HighPrice
	bar.RawLowPrice = bar.LowPrice
	bar.RawClosePrice = bar.ClosePrice
	bar.RawVolume = bar.Volume
	bar.RawQuoteVolume = bar.QuoteVolume
	bar.OpenPrice *= priceMultiplier
	bar.HighPrice *= priceMultiplier
	bar.LowPrice *= priceMultiplier
	bar.ClosePrice *= priceMultiplier
	bar.Volume *= volumeMultiplier
	bar.QuoteVolume *= priceMultiplier * volumeMultiplier
	bar.PriceMode = factor.AdjMode
	bar.AdjustmentStatus = AdjustmentStatusAdjusted
	bar.AdjustmentProvider = factor.Provider
	bar.AdjustmentProviderVersion = factor.ProviderVersion
	bar.AdjustmentEventType = factor.EventType
	bar.PriceMultiplier = priceMultiplier
	bar.VolumeMultiplier = volumeMultiplier
	return DecorateBar(bar)
}

func MarkBarAdjustmentStatus(bar Bar, priceMode string, status string) Bar {
	bar.PriceMode = priceMode
	bar.AdjustmentStatus = status
	if status == AdjustmentStatusRaw {
		bar.RawOpenPrice = 0
		bar.RawHighPrice = 0
		bar.RawLowPrice = 0
		bar.RawClosePrice = 0
		bar.RawVolume = 0
		bar.RawQuoteVolume = 0
	}
	return DecorateBar(bar)
}

func ApplyFactorToTick(tick Tick, factor AdjustmentFactor) Tick {
	priceMultiplier := factor.PriceMultiplier
	if priceMultiplier == 0 {
		priceMultiplier = 1
	}
	volumeMultiplier := factor.VolumeMultiplier
	if volumeMultiplier == 0 {
		volumeMultiplier = 1
	}
	tick.RawPrice = tick.Price
	tick.RawSize = tick.Size
	tick.Price *= priceMultiplier
	tick.Size *= volumeMultiplier
	tick.AdjustedPrice = tick.Price
	tick.AdjustedSize = tick.Size
	tick.PriceMode = factor.AdjMode
	return tick
}

func relevantRawFields(raw json.RawMessage) map[string]any {
	if len(raw) == 0 {
		return nil
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil
	}
	keys := []string{
		"contractType",
		"underlyingType",
		"underlyingSubType",
		"status",
		"instType",
		"instCategory",
		"ruleType",
		"state",
		"preMktSwTime",
		"contTdSwTime",
		"upcChg",
	}
	sort.Strings(keys)
	out := make(map[string]any)
	for _, key := range keys {
		if value, ok := payload[key]; ok {
			out[key] = value
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
