package config

import "testing"

func TestDisabledThreeDayFilteredFromDefaultAndLegacyConfiguration(t *testing.T) {
	for _, raw := range []string{"", "1m,3D,15m,1D,3M", "3D"} {
		t.Run(raw, func(t *testing.T) {
			t.Setenv("MARKET_TIMEFRAMES", raw)
			t.Setenv("MONITOR_INTEGRITY_TIMEFRAMES", "15m,3D,1D")
			cfg := Load()
			for _, frames := range [][]string{cfg.Timeframes, cfg.MonitorIntegrityTimeframes} {
				for _, tf := range frames {
					if tf == "3D" {
						t.Fatalf("disabled interval survived config filtering: %v", frames)
					}
				}
			}
			if raw != "3D" && len(cfg.Timeframes) == 0 {
				t.Fatal("unrelated configured intervals disappeared")
			}
		})
	}
}

func TestGuardianRepairDefaults(t *testing.T) {
	for _, name := range []string{
		"ENABLE_KLINE_GUARDIAN", "KLINE_GUARDIAN_OBSERVE_FINAL_BARS",
		"KLINE_GUARDIAN_MAX_INFLIGHT_PER_EXCHANGE",
		"KLINE_GUARDIAN_DAILY_AUDIT_ENABLED", "KLINE_GUARDIAN_DAILY_AUDIT_SECONDS",
		"KLINE_GUARDIAN_DAILY_AUDIT_DAYS", "KLINE_GUARDIAN_DAILY_LOOKBACK_DAYS",
		"KLINE_GUARDIAN_MINUTE_DAY_AUDIT_ENABLED", "KLINE_GUARDIAN_MINUTE_DAY_OFFSET_MINUTES",
		"KLINE_GUARDIAN_MINUTE_DAY_RPS", "KLINE_GUARDIAN_MINUTE_RETRY_SECONDS",
	} {
		t.Setenv(name, "")
	}
	cfg := Load()
	if cfg.EnableKlineGuardian || cfg.KlineGuardianObserveFinalBars {
		t.Fatal("Guardian and per-final observations must remain opt-in")
	}
	if !cfg.KlineGuardianDailyAuditEnabled || cfg.KlineGuardianDailyAuditSeconds != 3600 ||
		cfg.KlineGuardianDailyAuditDays != 7 || cfg.KlineGuardianDailyLookbackDays != 300 {
		t.Fatalf("unexpected daily repair defaults: enabled=%t seconds=%d days=%d lookback=%d",
			cfg.KlineGuardianDailyAuditEnabled, cfg.KlineGuardianDailyAuditSeconds,
			cfg.KlineGuardianDailyAuditDays, cfg.KlineGuardianDailyLookbackDays)
	}
	if cfg.KlineGuardianMaxInflightPerExchange != 8 {
		t.Fatalf("unexpected Guardian per-exchange concurrency default: %d", cfg.KlineGuardianMaxInflightPerExchange)
	}
	if !cfg.KlineGuardianMinuteDayAuditEnabled || cfg.KlineGuardianMinuteDayOffsetMinutes != 30 ||
		cfg.KlineGuardianMinuteDayRequestRPS != 1 || cfg.KlineGuardianMinuteRetrySeconds != 15 {
		t.Fatal("durable minute audit defaults must include a throttled next-day audit")
	}
}

func TestGuardianRepairConfiguration(t *testing.T) {
	t.Setenv("ENABLE_KLINE_GUARDIAN", "true")
	t.Setenv("KLINE_GUARDIAN_OBSERVE_FINAL_BARS", "false")
	t.Setenv("KLINE_GUARDIAN_DAILY_AUDIT_ENABLED", "true")
	t.Setenv("KLINE_GUARDIAN_DAILY_AUDIT_SECONDS", "1800")
	t.Setenv("KLINE_GUARDIAN_DAILY_AUDIT_DAYS", "14")
	t.Setenv("KLINE_GUARDIAN_DAILY_LOOKBACK_DAYS", "90")
	t.Setenv("KLINE_GUARDIAN_MAX_INFLIGHT_PER_EXCHANGE", "8")
	t.Setenv("KLINE_GUARDIAN_MINUTE_DAY_AUDIT_ENABLED", "false")
	t.Setenv("KLINE_GUARDIAN_MINUTE_DAY_OFFSET_MINUTES", "45")
	t.Setenv("KLINE_GUARDIAN_MINUTE_DAY_RPS", "2")
	t.Setenv("KLINE_GUARDIAN_MINUTE_RETRY_SECONDS", "30")
	cfg := Load()
	if !cfg.EnableKlineGuardian || cfg.KlineGuardianObserveFinalBars ||
		!cfg.KlineGuardianDailyAuditEnabled || cfg.KlineGuardianDailyAuditSeconds != 1800 ||
		cfg.KlineGuardianDailyAuditDays != 14 || cfg.KlineGuardianDailyLookbackDays != 90 ||
		cfg.KlineGuardianMaxInflightPerExchange != 8 {
		t.Fatal("Guardian repair configuration was not loaded")
	}
	if cfg.KlineGuardianMinuteDayAuditEnabled || cfg.KlineGuardianMinuteDayOffsetMinutes != 45 ||
		cfg.KlineGuardianMinuteDayRequestRPS != 2 || cfg.KlineGuardianMinuteRetrySeconds != 30 {
		t.Fatal("minute audit configuration was not loaded")
	}
}
