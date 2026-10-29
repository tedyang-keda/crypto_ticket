package market

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

// SymbolSnapshot is the last successful official inventory observation plus
// local subscription policy. Subscribed means selected by local policy; it is
// NOT proof of a live connection or a WebSocket subscription acknowledgement.
// Absence from an inventory is never silently interpreted as delisting.
type SymbolSnapshot struct {
	Exchange            string          `json:"exchange"`
	Symbol              string          `json:"symbol"`
	MarketType          string          `json:"market_type"`
	Subscribed          bool            `json:"subscribed"`
	SubscriptionReason  string          `json:"subscription_reason"`
	OfficialStatus      string          `json:"official_status"`
	OfficialActive      bool            `json:"official_active"`
	Delisted            bool            `json:"delisted"`
	Presence            string          `json:"presence"`
	ContractType        string          `json:"contract_type,omitempty"`
	BaseCurrency        string          `json:"base_currency,omitempty"`
	QuoteCurrency       string          `json:"quote_currency,omitempty"`
	SettleCurrency      string          `json:"settle_currency,omitempty"`
	ListedAtMS          int64           `json:"listed_at_ms,omitempty"`
	DelistAtMS          int64           `json:"delist_at_ms,omitempty"`
	ExpiryAtMS          int64           `json:"expiry_at_ms,omitempty"`
	CapturedAtMS        int64           `json:"captured_at_ms"`
	LastPresentAtMS     int64           `json:"last_present_at_ms,omitempty"`
	FirstAbsentAtMS     int64           `json:"first_absent_at_ms,omitempty"`
	ConsecutiveAbsences int             `json:"consecutive_absences"`
	OfficialRaw         json.RawMessage `json:"official_raw,omitempty"`
}

type SymbolSnapshotSync struct {
	Exchange      string `json:"exchange"`
	MarketType    string `json:"market_type"`
	Status        string `json:"status"` // ok or unavailable; failures preserve the previous inventory.
	LastAttemptMS int64  `json:"last_attempt_ms"`
	LastSuccessMS int64  `json:"last_success_ms"`
	SymbolCount   int    `json:"symbol_count"`
	LastError     string `json:"last_error,omitempty"`
}

type SymbolSnapshotBatch struct {
	Exchange     string           `json:"exchange"`
	MarketType   string           `json:"market_type"`
	CapturedAtMS int64            `json:"captured_at_ms"`
	Symbols      []SymbolSnapshot `json:"symbols"`
}

// SymbolSnapshotRepairSkipReason is shared by automatic and historical repair.
// Returning "" permits repair; missing, stale or unavailable metadata fails
// closed. It does not reinterpret an old official status as current presence.
func SymbolSnapshotRepairSkipReason(item SymbolSnapshot, sync SymbolSnapshotSync, nowMS, maxAgeMS int64) string {
	if reason := item.RepairSkipReason(time.UnixMilli(nowMS), time.Duration(maxAgeMS)*time.Millisecond); reason != "" {
		return reason
	}
	if !strings.EqualFold(sync.Exchange, item.Exchange) || !strings.EqualFold(sync.MarketType, item.MarketType) || sync.Status != "ok" || sync.LastSuccessMS != item.CapturedAtMS {
		return "snapshot_unavailable"
	}
	return ""
}

func (item SymbolSnapshot) RepairSkipReason(now time.Time, maxAge time.Duration) string {
	nowMS, maxAgeMS := now.UnixMilli(), maxAge.Milliseconds()
	if item.Exchange == "" || item.Symbol == "" || item.CapturedAtMS <= 0 {
		return "snapshot_missing"
	}
	if item.Presence != "present" {
		return "official_absent"
	}
	if item.Delisted {
		return "official_delisted"
	}
	if !item.Subscribed {
		if reason := strings.TrimSpace(item.SubscriptionReason); reason != "" {
			return reason
		}
		return "not_subscribed"
	}
	if !item.OfficialActive {
		return "official_inactive"
	}
	if item.ListedAtMS > nowMS {
		return "official_not_listed"
	}
	if maxAgeMS <= 0 || item.CapturedAtMS > nowMS || nowMS-item.CapturedAtMS > maxAgeMS {
		return "snapshot_stale"
	}
	return ""
}

// ReconcileSymbolSnapshot is a pure inventory transition. A successful full
// response can establish absence, not an official delisting or delisting time.
// Empty/partial/error responses must be rejected by the caller before here.
func ReconcileSymbolSnapshot(exchange, marketType string, enabled bool, capturedAtMS int64, official []SymbolInfo, previous []SymbolSnapshot) (SymbolSnapshotBatch, error) {
	exchange = strings.ToLower(strings.TrimSpace(exchange))
	marketType = strings.TrimSpace(marketType)
	batch := SymbolSnapshotBatch{Exchange: exchange, MarketType: marketType, CapturedAtMS: capturedAtMS}
	if exchange == "" || marketType == "" || capturedAtMS <= 0 || len(official) == 0 {
		return batch, fmt.Errorf("invalid or empty official symbol snapshot")
	}
	prior := make(map[string]SymbolSnapshot)
	previousPresent := 0
	for _, item := range previous {
		if strings.EqualFold(item.Exchange, exchange) && strings.EqualFold(item.MarketType, marketType) {
			prior[strings.ToUpper(item.Symbol)] = item
			if item.Presence == "present" {
				previousPresent++
			}
		}
	}
	// Public inventory endpoints have no pagination in these adapters. A severe
	// nonempty truncation is not trustworthy evidence of mass delisting.
	if previousPresent >= 20 && len(official)*2 < previousPresent {
		return batch, fmt.Errorf("official inventory shrank unexpectedly: previous_present=%d received=%d", previousPresent, len(official))
	}
	seen := make(map[string]bool)
	for _, info := range official {
		symbol := strings.ToUpper(strings.TrimSpace(info.Symbol))
		if !strings.EqualFold(info.Exchange, exchange) || !strings.EqualFold(info.MarketType, marketType) || symbol == "" || strings.TrimSpace(info.Status) == "" || seen[symbol] {
			return batch, fmt.Errorf("invalid or duplicate official symbol in %s/%s", exchange, marketType)
		}
		seen[symbol] = true
		old := prior[symbol]
		item := SymbolSnapshot{
			Exchange: exchange, Symbol: symbol, MarketType: marketType,
			Subscribed: enabled && info.IsActive, OfficialStatus: info.Status, OfficialActive: info.OfficialActive,
			Presence: "present", ContractType: info.ContractType, BaseCurrency: info.BaseCurrency,
			QuoteCurrency: info.QuoteCurrency, SettleCurrency: info.SettleCurrency,
			ListedAtMS: info.ListedAtMS, DelistAtMS: info.DelistAtMS, ExpiryAtMS: info.ExpiryAtMS,
			CapturedAtMS: capturedAtMS, LastPresentAtMS: capturedAtMS, OfficialRaw: info.OfficialRaw,
		}
		// Listing/delisting times are only official values, never first_seen.
		if item.ListedAtMS == 0 {
			item.ListedAtMS = old.ListedAtMS
		}
		if item.DelistAtMS == 0 {
			item.DelistAtMS = old.DelistAtMS
		}
		switch strings.ToLower(strings.TrimSpace(info.Status)) {
		case "delisted", "expired", "delivered", "close", "closed":
			item.Delisted = true
		}
		switch {
		case !enabled:
			item.SubscriptionReason = "market_disabled"
		case !info.OfficialActive:
			item.SubscriptionReason = "official_inactive"
		case !info.IsActive:
			item.SubscriptionReason = "contract_filtered"
		default:
			item.SubscriptionReason = "enabled"
		}
		batch.Symbols = append(batch.Symbols, item)
	}
	for symbol, item := range prior {
		if seen[symbol] {
			continue
		}
		item.Presence, item.Subscribed, item.SubscriptionReason = "absent", false, "official_absent"
		item.CapturedAtMS = capturedAtMS
		item.ConsecutiveAbsences++
		if item.FirstAbsentAtMS == 0 {
			item.FirstAbsentAtMS = capturedAtMS
		}
		batch.Symbols = append(batch.Symbols, item)
	}
	sort.Slice(batch.Symbols, func(i, j int) bool { return batch.Symbols[i].Symbol < batch.Symbols[j].Symbol })
	return batch, nil
}
