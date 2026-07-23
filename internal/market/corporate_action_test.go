package market

import "testing"

func TestIsEquityLikeAssetClass(t *testing.T) {
	equityLike := []string{AssetClassEquity, AssetClassKREquity, AssetClassPreMarket, AssetClassIndex, AssetClassCommodity}
	for _, class := range equityLike {
		if !IsEquityLikeAssetClass(class) {
			t.Fatalf("%q should be equity-like", class)
		}
	}
	for _, class := range []string{AssetClassCrypto, AssetClassForex, AssetClassBonds, AssetClassUnknown, ""} {
		if IsEquityLikeAssetClass(class) {
			t.Fatalf("%q should not be equity-like", class)
		}
	}
}

func TestCorporateActionEventType(t *testing.T) {
	equity := func(phase string, rule string) SymbolInfo {
		return SymbolInfo{AssetClass: AssetClassEquity, LifecyclePhase: phase, RuleType: rule}
	}

	cases := []struct {
		name     string
		previous SymbolInfo
		current  SymbolInfo
		want     string
		wantHit  bool
	}{
		{
			name:     "entered rebase",
			previous: equity(PhaseContinuous, RuleNormal),
			current:  equity(PhaseRebase, RuleNormal),
			want:     InstrumentEventRebase,
			wantHit:  true,
		},
		{
			name:     "armed as rebase contract",
			previous: equity(PhaseContinuous, RuleNormal),
			current:  equity(PhaseContinuous, RuleRebaseContract),
			want:     InstrumentEventRebaseArmed,
			wantHit:  true,
		},
		{
			name:     "suspended from live",
			previous: equity(PhaseContinuous, RuleNormal),
			current:  equity(PhaseSuspend, RuleNormal),
			want:     InstrumentEventSuspended,
			wantHit:  true,
		},
		{
			name:     "binance cancel only",
			previous: equity(PhaseHalt, RuleNormal),
			current:  equity(PhaseCancelOnly, RuleNormal),
			want:     InstrumentEventCancelOnly,
			wantHit:  true,
		},
		{
			name:     "resumed after cancel only",
			previous: equity(PhaseCancelOnly, RuleNormal),
			current:  equity(PhaseContinuous, RuleNormal),
			want:     InstrumentEventResumed,
			wantHit:  true,
		},
		{
			name:     "expired from live",
			previous: equity(PhaseContinuous, RuleNormal),
			current:  equity(PhaseExpired, RuleNormal),
			want:     InstrumentEventDelisted,
			wantHit:  true,
		},
		{
			name:     "crypto rebase ignored",
			previous: SymbolInfo{AssetClass: AssetClassCrypto, LifecyclePhase: PhaseContinuous},
			current:  SymbolInfo{AssetClass: AssetClassCrypto, LifecyclePhase: PhaseRebase},
			want:     "",
			wantHit:  false,
		},
		{
			name:     "unrelated metadata drift",
			previous: equity(PhaseContinuous, RuleNormal),
			current:  equity(PhaseContinuous, RuleNormal),
			want:     "",
			wantHit:  false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, hit := CorporateActionEventType(tc.previous, tc.current)
			if hit != tc.wantHit || got != tc.want {
				t.Fatalf("got (%q, %v), want (%q, %v)", got, hit, tc.want, tc.wantHit)
			}
		})
	}
}
