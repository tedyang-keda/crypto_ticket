package config

import "testing"

func TestGuardianRepairDefaults(t *testing.T) {
	for _, name := range []string{
		"ENABLE_KLINE_GUARDIAN", "KLINE_GUARDIAN_OBSERVE_FINAL_BARS",
		"KLINE_GUARDIAN_DAILY_AUDIT_ENABLED", "KLINE_GUARDIAN_DAILY_AUDIT_SECONDS",
		"KLINE_GUARDIAN_DAILY_AUDIT_DAYS", "KLINE_GUARDIAN_DAILY_LOOKBACK_DAYS",
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
}

func TestGuardianRepairConfiguration(t *testing.T) {
	t.Setenv("ENABLE_KLINE_GUARDIAN", "true")
	t.Setenv("KLINE_GUARDIAN_OBSERVE_FINAL_BARS", "false")
	t.Setenv("KLINE_GUARDIAN_DAILY_AUDIT_ENABLED", "true")
	t.Setenv("KLINE_GUARDIAN_DAILY_AUDIT_SECONDS", "1800")
	t.Setenv("KLINE_GUARDIAN_DAILY_AUDIT_DAYS", "14")
	t.Setenv("KLINE_GUARDIAN_DAILY_LOOKBACK_DAYS", "90")
	cfg := Load()
	if !cfg.EnableKlineGuardian || cfg.KlineGuardianObserveFinalBars ||
		!cfg.KlineGuardianDailyAuditEnabled || cfg.KlineGuardianDailyAuditSeconds != 1800 ||
		cfg.KlineGuardianDailyAuditDays != 14 || cfg.KlineGuardianDailyLookbackDays != 90 {
		t.Fatal("Guardian repair configuration was not loaded")
	}
}
