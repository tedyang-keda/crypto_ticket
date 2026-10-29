package market

import (
	"fmt"
	"testing"
	"time"
)

func TestSymbolSnapshotRejectsSevereNonemptyInventoryTruncation(t *testing.T) {
	previous := make([]SymbolSnapshot, 100)
	for i := range previous {
		previous[i] = SymbolSnapshot{Exchange: "okx", Symbol: fmt.Sprintf("S%d-USDT-SWAP", i), MarketType: "SWAP", Presence: "present"}
	}
	valid := SymbolInfo{Exchange: "okx", Symbol: "BTC-USDT-SWAP", MarketType: "SWAP", Status: "live"}
	if _, err := ReconcileSymbolSnapshot("okx", "SWAP", true, 100, []SymbolInfo{valid}, previous); err == nil {
		t.Fatal("truncated nonempty inventory must preserve previous snapshot")
	}
}

func TestSymbolSnapshotSeparatesOfficialLifecycleAndLocalPolicy(t *testing.T) {
	official := []SymbolInfo{
		{Exchange: "binance", Symbol: "BTCUSDT", MarketType: "um_futures", Status: "TRADING", OfficialActive: true, IsActive: true, ListedAtMS: 50},
		{Exchange: "binance", Symbol: "BTCUSDT_261225", MarketType: "um_futures", Status: "TRADING", OfficialActive: true, IsActive: false},
		{Exchange: "binance", Symbol: "OLDUSDT", MarketType: "um_futures", Status: "CLOSE", OfficialActive: false, IsActive: false},
	}
	batch, err := ReconcileSymbolSnapshot("binance", "um_futures", true, 100, official, nil)
	if err != nil {
		t.Fatal(err)
	}
	bySymbol := make(map[string]SymbolSnapshot)
	for _, item := range batch.Symbols {
		bySymbol[item.Symbol] = item
	}
	if !bySymbol["BTCUSDT"].Subscribed || bySymbol["BTCUSDT"].ListedAtMS != 50 {
		t.Fatalf("bad selected symbol: %+v", bySymbol["BTCUSDT"])
	}
	if !bySymbol["BTCUSDT_261225"].OfficialActive || bySymbol["BTCUSDT_261225"].Subscribed || bySymbol["BTCUSDT_261225"].Delisted || bySymbol["BTCUSDT_261225"].SubscriptionReason != "contract_filtered" {
		t.Fatalf("delivery filtering confused with delisting: %+v", bySymbol["BTCUSDT_261225"])
	}
	if !bySymbol["OLDUSDT"].Delisted || bySymbol["OLDUSDT"].DelistAtMS != 0 {
		t.Fatal("explicit official terminal state must not fabricate a timestamp")
	}
	batch, err = ReconcileSymbolSnapshot("binance", "um_futures", false, 100, official, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range batch.Symbols {
		if item.Subscribed || item.SubscriptionReason != "market_disabled" {
			t.Fatalf("disabled market still selected: %+v", item)
		}
	}
}

func TestSymbolSnapshotAbsentIsNotDelistedAndRecovers(t *testing.T) {
	old := SymbolSnapshot{Exchange: "okx", Symbol: "OLD-USDT-SWAP", MarketType: "SWAP", Presence: "present", OfficialStatus: "live", OfficialActive: true, Subscribed: true, ListedAtMS: 50, CapturedAtMS: 100, LastPresentAtMS: 100}
	live := SymbolInfo{Exchange: "okx", Symbol: "BTC-USDT-SWAP", MarketType: "SWAP", Status: "live", OfficialActive: true, IsActive: true}
	batch, err := ReconcileSymbolSnapshot("okx", "SWAP", true, 200, []SymbolInfo{live}, []SymbolSnapshot{old})
	if err != nil {
		t.Fatal(err)
	}
	var absent SymbolSnapshot
	for _, item := range batch.Symbols {
		if item.Symbol == old.Symbol {
			absent = item
		}
	}
	if absent.Presence != "absent" || absent.Delisted || absent.DelistAtMS != 0 || absent.Subscribed || absent.FirstAbsentAtMS != 200 || absent.LastPresentAtMS != 100 || absent.ConsecutiveAbsences != 1 {
		t.Fatalf("bad absence: %+v", absent)
	}
	returned := live
	returned.Symbol = old.Symbol
	batch, err = ReconcileSymbolSnapshot("okx", "SWAP", true, 300, []SymbolInfo{live, returned}, batch.Symbols)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range batch.Symbols {
		if item.Symbol == old.Symbol && (!item.Subscribed || item.Presence != "present" || item.FirstAbsentAtMS != 0 || item.ConsecutiveAbsences != 0 || item.ListedAtMS != 50) {
			t.Fatalf("reappearance did not restore: %+v", item)
		}
	}
}

func TestSymbolSnapshotRejectsEmptyMalformedAndDuplicateInventory(t *testing.T) {
	valid := SymbolInfo{Exchange: "okx", Symbol: "BTC-USDT-SWAP", MarketType: "SWAP", Status: "live"}
	for _, items := range [][]SymbolInfo{nil, {valid, valid}, {{Exchange: "okx", Symbol: "", MarketType: "SWAP", Status: "live"}}, {{Exchange: "okx", Symbol: "A", MarketType: "SWAP"}}} {
		if _, err := ReconcileSymbolSnapshot("okx", "SWAP", true, 100, items, nil); err == nil {
			t.Fatalf("invalid inventory accepted: %+v", items)
		}
	}
}

func TestSymbolSnapshotRepairRequiresFreshActiveSelectedPresentInventory(t *testing.T) {
	base := SymbolSnapshot{Exchange: "okx", Symbol: "BTC-USDT-SWAP", MarketType: "SWAP", Presence: "present", OfficialActive: true, Subscribed: true, CapturedAtMS: 1000}
	sync := SymbolSnapshotSync{Exchange: "okx", MarketType: "SWAP", Status: "ok", LastSuccessMS: 1000}
	if reason := SymbolSnapshotRepairSkipReason(base, sync, 2000, 2000); reason != "" {
		t.Fatal(reason)
	}
	tests := map[string]func(*SymbolSnapshot){
		"absent":                    func(s *SymbolSnapshot) { s.Presence = "absent" },
		"unsubscribed_empty_reason": func(s *SymbolSnapshot) { s.Subscribed = false },
		"inactive":                  func(s *SymbolSnapshot) { s.OfficialActive = false },
		"delisted":                  func(s *SymbolSnapshot) { s.Delisted = true },
		"stale":                     func(s *SymbolSnapshot) { s.CapturedAtMS = 1 },
		"future":                    func(s *SymbolSnapshot) { s.CapturedAtMS = 5000 },
		"not_listed":                func(s *SymbolSnapshot) { s.ListedAtMS = 5000 },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			item := base
			mutate(&item)
			if item.RepairSkipReason(time.UnixMilli(2000), time.Second) == "" {
				t.Fatal("ineligible inventory allowed")
			}
		})
	}
	for _, state := range []SymbolSnapshotSync{{}, {Exchange: "okx", MarketType: "SWAP", Status: "unavailable", LastSuccessMS: 1000}, {Exchange: "okx", MarketType: "SWAP", Status: "ok", LastSuccessMS: 999}} {
		if SymbolSnapshotRepairSkipReason(base, state, 2000, 2000) == "" {
			t.Fatal("unavailable or inconsistent sync allowed")
		}
	}
}
