package config

import (
	"os"
	"strconv"
	"strings"

	"crypto-ticket/internal/timeframe"
)

type Config struct {
	HTTPAddr                          string
	RedisURL                          string
	MySQLDSN                          string
	UseMemory                         bool
	RecentCacheLimit                  int
	Timeframes                        []string
	DashboardDir                      string
	EnableMockSymbols                 bool
	EnableCollector                   bool
	EnableKlineGuardian               bool
	KlineGuardianAuditIntervalSeconds int
	KlineGuardianWindowMinutes        int
	KlineGuardianDelaySeconds         int
	KlineGuardianSymbolsPerRun        int
	KlineGuardianRequestDelayMS       int
	KlineGuardianSymbolMaxAgeSeconds  int
	SymbolRefreshIntervalSeconds      int
	ReconnectBaseDelaySeconds         int
	ReconnectMaxDelaySeconds          int
	Exchanges                         []ExchangeConfig
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
		HTTPAddr:                          env("HTTP_ADDR", "127.0.0.1:8088"),
		RedisURL:                          env("REDIS_URL", "redis://127.0.0.1:6379/0"),
		MySQLDSN:                          env("MYSQL_DSN", mysqlDSNFromEnv()),
		UseMemory:                         envBool("USE_MEMORY_STORE", true),
		RecentCacheLimit:                  envInt("RECENT_CACHE_LIMIT", 300),
		Timeframes:                        outFrames,
		DashboardDir:                      env("DASHBOARD_DIR", "./web/dist"),
		EnableMockSymbols:                 envBool("ENABLE_MOCK_SYMBOLS", !enableCollector),
		EnableCollector:                   enableCollector,
		EnableKlineGuardian:               envBool("ENABLE_KLINE_GUARDIAN", enableCollector),
		KlineGuardianAuditIntervalSeconds: envInt("KLINE_GUARDIAN_AUDIT_INTERVAL_SECONDS", 60),
		KlineGuardianWindowMinutes:        envInt("KLINE_GUARDIAN_WINDOW_MINUTES", 30),
		KlineGuardianDelaySeconds:         envInt("KLINE_GUARDIAN_DELAY_SECONDS", 120),
		KlineGuardianSymbolsPerRun:        envInt("KLINE_GUARDIAN_SYMBOLS_PER_RUN", 50),
		KlineGuardianRequestDelayMS:       envInt("KLINE_GUARDIAN_REQUEST_DELAY_MS", 100),
		KlineGuardianSymbolMaxAgeSeconds:  envInt("KLINE_GUARDIAN_SYMBOL_MAX_AGE_SECONDS", 600),
		SymbolRefreshIntervalSeconds:      envInt("SYMBOL_REFRESH_INTERVAL_SECONDS", 120),
		ReconnectBaseDelaySeconds:         envInt("RECONNECT_BASE_DELAY_SECONDS", 1),
		ReconnectMaxDelaySeconds:          envInt("RECONNECT_MAX_DELAY_SECONDS", 60),
		Exchanges:                         loadExchangeConfigs(),
	}
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
			Enabled:               enabled["binance"] && envBool("BINANCE_ENABLED", true) && envBool("BINANCE_COIN_ENABLED", true),
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
