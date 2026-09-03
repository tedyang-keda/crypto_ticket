package config

import (
	"os"
	"strconv"
	"strings"

	"crypto-ticket/internal/timeframe"
)

type Config struct {
	HTTPAddr                           string
	RedisURL                           string
	MySQLDSN                           string
	UseMemory                          bool
	RecentCacheLimit                   int
	Timeframes                         []string
	DashboardDir                       string
	EnableMockSymbols                  bool
	EnableCollector                    bool
	EnableKlineGuardian                bool
	KlineGuardianAuditIntervalSeconds  int
	KlineGuardianWindowMinutes         int
	KlineGuardianDelaySeconds          int
	KlineGuardianSymbolsPerRun         int
	KlineGuardianRequestDelayMS        int
	KlineGuardianBinanceRPS            int
	KlineGuardianOKXRPS                int
	KlineGuardianSymbolMaxAgeSeconds   int
	EnableCorporateAction              bool
	CorporateActionPollSeconds         int
	CorporateActionMaxAttempts         int
	CorporateActionRetryBaseSeconds    int
	CorporateActionJobsPerRun          int
	CorporateActionRequestDelayMS      int
	CorporateActionAnchorSeconds       int
	CorporateActionAnchorSymbolsPerRun int
	FeishuWebhookURL                   string
	EnableHealthMonitor                bool
	MonitorP1AlertsEnabled             bool
	MonitorEvaluationSeconds           int
	MonitorDailyReportHour             int
	MonitorMarketReportIntervalMinutes int
	MonitorRedisEnabled                bool
	MonitorIntegrityIntervalSeconds    int
	MonitorIntegritySymbolsPerRun      int
	MonitorIntegrityTimeframes         []string
	MonitorDiskPath                    string
	SymbolRefreshIntervalSeconds       int
	ReconnectBaseDelaySeconds          int
	ReconnectMaxDelaySeconds           int
	CollectorWSPingIntervalSeconds     int
	CollectorWSPongWaitSeconds         int
	Exchanges                          []ExchangeConfig
}

type ExchangeConfig struct {
	Name                  string
	MarketType            string
	RestURL               string
	WSURL                 string
	Enabled               bool
	SubscriptionChunkSize int
}

func Load() Config {
	frames := strings.Split(env("MARKET_TIMEFRAMES", strings.Join(timeframe.Order, ",")), ",")
	outFrames := make([]string, 0, len(frames))
	for _, frame := range frames {
		frame = strings.TrimSpace(frame)
		if frame == "" {
			continue
		}
		outFrames = append(outFrames, timeframe.MustNormalize(frame))
	}
	enableCollector := envBool("ENABLE_COLLECTOR", false)
	return Config{
		HTTPAddr:          env("HTTP_ADDR", "127.0.0.1:8088"),
		RedisURL:          env("REDIS_URL", "redis://127.0.0.1:6379/0"),
		MySQLDSN:          env("MYSQL_DSN", mysqlDSNFromEnv()),
		UseMemory:         envBool("USE_MEMORY_STORE", true),
		RecentCacheLimit:  envInt("RECENT_CACHE_LIMIT", 300),
		Timeframes:        outFrames,
		DashboardDir:      env("DASHBOARD_DIR", "./web/dist"),
		EnableMockSymbols: envBool("ENABLE_MOCK_SYMBOLS", !enableCollector),
		EnableCollector:   enableCollector,
		// Guardian is an optional repair/audit worker. Keep it opt-in so a
		// collector deployment cannot silently add REST repair load and queue
		// noise to the realtime ingestion path.
		EnableKlineGuardian:                envBool("ENABLE_KLINE_GUARDIAN", false),
		KlineGuardianAuditIntervalSeconds:  envInt("KLINE_GUARDIAN_AUDIT_INTERVAL_SECONDS", 120),
		KlineGuardianWindowMinutes:         envInt("KLINE_GUARDIAN_WINDOW_MINUTES", 30),
		KlineGuardianDelaySeconds:          envInt("KLINE_GUARDIAN_DELAY_SECONDS", 120),
		KlineGuardianSymbolsPerRun:         envInt("KLINE_GUARDIAN_SYMBOLS_PER_RUN", 0),
		KlineGuardianRequestDelayMS:        envInt("KLINE_GUARDIAN_REQUEST_DELAY_MS", 100),
		KlineGuardianBinanceRPS:            envInt("KLINE_GUARDIAN_BINANCE_RPS", 8),
		KlineGuardianOKXRPS:                envInt("KLINE_GUARDIAN_OKX_RPS", 5),
		KlineGuardianSymbolMaxAgeSeconds:   envInt("KLINE_GUARDIAN_SYMBOL_MAX_AGE_SECONDS", 600),
		EnableCorporateAction:              envBool("ENABLE_CORPORATE_ACTION", false),
		CorporateActionPollSeconds:         envInt("CORPORATE_ACTION_POLL_SECONDS", 15),
		CorporateActionMaxAttempts:         envInt("CORPORATE_ACTION_MAX_ATTEMPTS", 5),
		CorporateActionRetryBaseSeconds:    envInt("CORPORATE_ACTION_RETRY_BASE_SECONDS", 60),
		CorporateActionJobsPerRun:          envInt("CORPORATE_ACTION_JOBS_PER_RUN", 1),
		CorporateActionRequestDelayMS:      envInt("CORPORATE_ACTION_REQUEST_DELAY_MS", 100),
		CorporateActionAnchorSeconds:       envInt("CORPORATE_ACTION_ANCHOR_SECONDS", 60),
		CorporateActionAnchorSymbolsPerRun: envInt("CORPORATE_ACTION_ANCHOR_SYMBOLS_PER_RUN", 1),
		FeishuWebhookURL:                   env("FEISHU_WEBHOOK_URL", ""),
		EnableHealthMonitor:                envBool("ENABLE_HEALTH_MONITOR", enableCollector),
		MonitorP1AlertsEnabled:             envBool("MONITOR_P1_ALERTS_ENABLED", false),
		MonitorEvaluationSeconds:           envInt("MONITOR_EVALUATION_SECONDS", 15),
		MonitorDailyReportHour:             envInt("MONITOR_DAILY_REPORT_HOUR", 9),
		MonitorMarketReportIntervalMinutes: envInt("MONITOR_MARKET_REPORT_INTERVAL_MINUTES", 30),
		MonitorRedisEnabled:                envBool("MONITOR_REDIS_ENABLED", true),
		MonitorIntegrityIntervalSeconds:    envInt("MONITOR_INTEGRITY_INTERVAL_SECONDS", 600),
		MonitorIntegritySymbolsPerRun:      envInt("MONITOR_INTEGRITY_SYMBOLS_PER_RUN", 50),
		MonitorIntegrityTimeframes:         normalizedTimeframes(env("MONITOR_INTEGRITY_TIMEFRAMES", "15m,30m,1H,4H,1D,2D,1W")),
		MonitorDiskPath:                    env("WATCHDOG_DISK_PATH", "."),
		SymbolRefreshIntervalSeconds:       envInt("SYMBOL_REFRESH_INTERVAL_SECONDS", 120),
		ReconnectBaseDelaySeconds:          envInt("RECONNECT_BASE_DELAY_SECONDS", 1),
		ReconnectMaxDelaySeconds:           envInt("RECONNECT_MAX_DELAY_SECONDS", 60),
		CollectorWSPingIntervalSeconds:     envInt("COLLECTOR_WS_PING_INTERVAL_SECONDS", 20),
		CollectorWSPongWaitSeconds:         envInt("COLLECTOR_WS_PONG_WAIT_SECONDS", 60),
		Exchanges:                          loadExchangeConfigs(),
	}
}

func normalizedTimeframes(raw string) []string {
	var out []string
	seen := make(map[string]bool)
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		normalized := timeframe.MustNormalize(item)
		if !seen[normalized] {
			seen[normalized] = true
			out = append(out, normalized)
		}
	}
	return out
}

func loadExchangeConfigs() []ExchangeConfig {
	enabled := enabledExchangeSet()
	return []ExchangeConfig{
		{
			Name:                  "binance",
			MarketType:            env("BINANCE_KIND", "um_futures"),
			RestURL:               env("BINANCE_REST_URL", "https://fapi.binance.com"),
			WSURL:                 env("BINANCE_WS_URL", "wss://fstream.binance.com/market"),
			Enabled:               enabled["binance"] && envBool("BINANCE_ENABLED", true) && envBool("BINANCE_UM_ENABLED", true),
			SubscriptionChunkSize: envInt("BINANCE_SUBSCRIPTION_CHUNK_SIZE", 50),
		},
		{
			Name:                  "binance",
			MarketType:            env("BINANCE_COIN_KIND", "coin_futures"),
			RestURL:               env("BINANCE_COIN_REST_URL", "https://dapi.binance.com"),
			WSURL:                 env("BINANCE_COIN_WS_URL", "wss://dstream.binance.com/ws"),
			Enabled:               enabled["binance"] && envBool("BINANCE_ENABLED", true) && envBool("BINANCE_COIN_ENABLED", false),
			SubscriptionChunkSize: envInt("BINANCE_COIN_SUBSCRIPTION_CHUNK_SIZE", 50),
		},
		{
			Name:                  "okx",
			MarketType:            strings.ToUpper(env("OKX_KIND", "swap")),
			RestURL:               env("OKX_REST_URL", "https://www.okx.com"),
			WSURL:                 env("OKX_WS_URL", "wss://ws.okx.com:8443/ws/v5/public"),
			Enabled:               enabled["okx"] && envBool("OKX_ENABLED", true),
			SubscriptionChunkSize: envInt("OKX_SUBSCRIPTION_CHUNK_SIZE", 120),
		},
	}
}

func enabledExchangeSet() map[string]bool {
	raw := env("ENABLED_EXCHANGES", "binance,okx")
	out := map[string]bool{}
	for _, item := range strings.Split(raw, ",") {
		name := strings.ToLower(strings.TrimSpace(item))
		if name != "" {
			out[name] = true
		}
	}
	return out
}

func mysqlDSNFromEnv() string {
	user := env("MYSQL_USER", "root")
	password := os.Getenv("MYSQL_PASSWORD")
	host := env("MYSQL_HOST", "127.0.0.1")
	port := env("MYSQL_PORT", "3306")
	database := env("MYSQL_DATABASE", "crypto_ticket")
	return user + ":" + password + "@tcp(" + host + ":" + port + ")/" + database + "?parseTime=true"
}

func env(name string, fallback string) string {
	value := os.Getenv(name)
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func envInt(name string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func envBool(name string, fallback bool) bool {
	value := strings.TrimSpace(strings.ToLower(os.Getenv(name)))
	if value == "" {
		return fallback
	}
	return value == "1" || value == "true" || value == "yes" || value == "on"
}
