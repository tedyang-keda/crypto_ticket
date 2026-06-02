package cache

import (
	"context"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

type RedisMarketCache struct {
	client *redis.Client
}

type KlineCacheClearOptions struct {
	Exchange      string
	Symbol        string
	Timeframe     string
	IncludeRecent bool
	IncludeLive   bool
	ScanCount     int64
}

func NewRedisMarketCache(redisURL string) (*RedisMarketCache, error) {
	options, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, err
	}
	return &RedisMarketCache{client: redis.NewClient(options)}, nil
}

func (r *RedisMarketCache) Close() error {
	return r.client.Close()
}

func (r *RedisMarketCache) ClearKlineCache(ctx context.Context, options KlineCacheClearOptions) (int64, error) {
	if !options.IncludeRecent && !options.IncludeLive {
		return 0, nil
	}
	scanCount := options.ScanCount
	if scanCount <= 0 {
		scanCount = 500
	}
	patterns := make([]string, 0, 3)
	exchange := klinePatternExchange(options.Exchange)
	symbol := klinePatternSymbol(options.Symbol)
	tf := klinePatternTimeframe(options.Timeframe)
	if options.IncludeRecent {
		patterns = append(patterns,
			fmt.Sprintf("kline:idx:%s:%s:%s", exchange, symbol, tf),
			fmt.Sprintf("kline:bar:%s:%s:%s", exchange, symbol, tf),
		)
	}
	if options.IncludeLive {
		patterns = append(patterns, fmt.Sprintf("livebar:%s:%s:%s", exchange, symbol, tf))
	}

	var deleted int64
	seen := make(map[string]bool)
	for _, pattern := range patterns {
		count, err := r.deleteByPattern(ctx, pattern, scanCount, seen)
		if err != nil {
			return deleted, err
		}
		deleted += count
	}
	return deleted, nil
}

func (r *RedisMarketCache) deleteByPattern(ctx context.Context, pattern string, scanCount int64, seen map[string]bool) (int64, error) {
	var cursor uint64
	var deleted int64
	for {
		keys, nextCursor, err := r.client.Scan(ctx, cursor, pattern, scanCount).Result()
		if err != nil {
			return deleted, err
		}
		cursor = nextCursor
		if len(keys) > 0 {
			unique := make([]string, 0, len(keys))
			for _, key := range keys {
				if seen[key] {
					continue
				}
				seen[key] = true
				unique = append(unique, key)
			}
			if len(unique) > 0 {
				count, err := r.client.Del(ctx, unique...).Result()
				if err != nil {
					return deleted, err
				}
				deleted += count
			}
		}
		if cursor == 0 {
			break
		}
	}
	return deleted, nil
}

func klinePatternExchange(exchange string) string {
	exchange = strings.TrimSpace(exchange)
	if exchange == "" {
		return "*"
	}
	return strings.ToLower(exchange)
}

func klinePatternSymbol(symbol string) string {
	symbol = strings.TrimSpace(symbol)
	if symbol == "" {
		return "*"
	}
	return strings.ToUpper(symbol)
}

func klinePatternTimeframe(tf string) string {
	tf = strings.TrimSpace(tf)
	if tf == "" {
		return "*"
	}
	return tf
}
