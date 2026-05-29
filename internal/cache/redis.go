package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"crypto-ticket/internal/market"
)

type RedisMarketCache struct {
	client *redis.Client
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

func (r *RedisMarketCache) SetLatestTick(ctx context.Context, tick market.Tick) error {
	key := fmt.Sprintf("quote:%s:%s", strings.ToLower(tick.Exchange), strings.ToUpper(tick.Symbol))
	fields := map[string]any{
		"exchange":      tick.Exchange,
		"symbol":        tick.Symbol,
		"ts_ms":         tick.TsMS,
		"price":         tick.Price,
		"size":          tick.Size,
		"side":          tick.Side,
		"trade_id":      tick.TradeID,
		"event_type":    tick.EventType,
		"source":        tick.Source,
		"recv_ms":       tick.RecvMS,
		"updated_at_ms": market.NowMS(),
	}
	pipe := r.client.Pipeline()
	pipe.HSet(ctx, key, fields)
	pipe.Expire(ctx, key, 2*time.Minute)
	_, err := pipe.Exec(ctx)
	return err
}

func (r *RedisMarketCache) GetLatestTick(ctx context.Context, exchange string, symbol string) (*market.Tick, error) {
	key := fmt.Sprintf("quote:%s:%s", strings.ToLower(exchange), strings.ToUpper(symbol))
	fields, err := r.client.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, err
	}
	if len(fields) == 0 {
		return nil, nil
	}
	tick := market.Tick{
		Exchange:  fields["exchange"],
		Symbol:    fields["symbol"],
		Side:      fields["side"],
		TradeID:   fields["trade_id"],
		EventType: fields["event_type"],
		Source:    fields["source"],
	}
	tick.TsMS, _ = strconv.ParseInt(fields["ts_ms"], 10, 64)
	tick.Price, _ = strconv.ParseFloat(fields["price"], 64)
	tick.Size, _ = strconv.ParseFloat(fields["size"], 64)
	tick.RecvMS, _ = strconv.ParseInt(fields["recv_ms"], 10, 64)
	return &tick, nil
}

func (r *RedisMarketCache) SetLiveBar(ctx context.Context, bar market.Bar) error {
	payload, err := json.Marshal(bar)
	if err != nil {
		return err
	}
	ttl := liveBarTTL(bar.Timeframe)
	return r.client.Set(ctx, liveBarRedisKey(bar.Exchange, bar.Symbol, bar.Timeframe), payload, ttl).Err()
}

func (r *RedisMarketCache) GetLiveBar(ctx context.Context, exchange string, symbol string, tf string) (*market.Bar, error) {
	raw, err := r.client.Get(ctx, liveBarRedisKey(exchange, symbol, tf)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var bar market.Bar
	if err := json.Unmarshal(raw, &bar); err != nil {
		return nil, err
	}
	return &bar, nil
}

func (r *RedisMarketCache) PutFinalBars(ctx context.Context, bars []market.Bar, maxKeep int) error {
	if len(bars) == 0 {
		return nil
	}
	pipe := r.client.Pipeline()
	trimCommands := make(map[string][]*redis.StringSliceCmd)
	for _, bar := range bars {
		idxKey := recentIdxKey(bar.Exchange, bar.Symbol, bar.Timeframe)
		hashKey := recentHashKey(bar.Exchange, bar.Symbol, bar.Timeframe)
		field := strconv.FormatInt(bar.StartMS, 10)
		payload, err := json.Marshal(bar)
		if err != nil {
			return err
		}
		pipe.ZAdd(ctx, idxKey, redis.Z{Score: float64(bar.StartMS), Member: field})
		pipe.HSet(ctx, hashKey, field, payload)
		pipe.Expire(ctx, idxKey, 24*time.Hour)
		pipe.Expire(ctx, hashKey, 24*time.Hour)
		if maxKeep > 0 {
			trimCommands[hashKey] = append(trimCommands[hashKey], pipe.ZRange(ctx, idxKey, 0, int64(-maxKeep-1)))
			pipe.ZRemRangeByRank(ctx, idxKey, 0, int64(-maxKeep-1))
		}
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}
	for hashKey, commands := range trimCommands {
		oldFields := make([]string, 0)
		seen := map[string]bool{}
		for _, command := range commands {
			fields, err := command.Result()
			if err != nil {
				return err
			}
			for _, field := range fields {
				if !seen[field] {
					seen[field] = true
					oldFields = append(oldFields, field)
				}
			}
		}
		if len(oldFields) > 0 {
			if err := r.client.HDel(ctx, hashKey, oldFields...).Err(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *RedisMarketCache) RecentBars(ctx context.Context, query market.KlineQuery) ([]market.Bar, error) {
	limit := int64(query.Limit)
	if limit <= 0 {
		limit = 300
	}
	idxKey := recentIdxKey(query.Exchange, query.Symbol, query.Timeframe)
	hashKey := recentHashKey(query.Exchange, query.Symbol, query.Timeframe)
	members, err := r.client.ZRevRange(ctx, idxKey, 0, limit-1).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		return nil, nil
	}
	values, err := r.client.HMGet(ctx, hashKey, members...).Result()
	if err != nil {
		return nil, err
	}
	bars := make([]market.Bar, 0, len(values))
	for _, value := range values {
		if value == nil {
			continue
		}
		raw, ok := value.(string)
		if !ok {
			continue
		}
		var bar market.Bar
		if err := json.Unmarshal([]byte(raw), &bar); err == nil {
			bars = append(bars, bar)
		}
	}
	sort.Slice(bars, func(i, j int) bool { return bars[i].StartMS < bars[j].StartMS })
	return bars, nil
}

func liveBarRedisKey(exchange string, symbol string, tf string) string {
	return fmt.Sprintf("livebar:%s:%s:%s", strings.ToLower(exchange), strings.ToUpper(symbol), tf)
}

func recentIdxKey(exchange string, symbol string, tf string) string {
	return fmt.Sprintf("kline:idx:%s:%s:%s", strings.ToLower(exchange), strings.ToUpper(symbol), tf)
}

func recentHashKey(exchange string, symbol string, tf string) string {
	return fmt.Sprintf("kline:bar:%s:%s:%s", strings.ToLower(exchange), strings.ToUpper(symbol), tf)
}

func liveBarTTL(tf string) time.Duration {
	switch tf {
	case "1m":
		return 5 * time.Minute
	case "5m", "15m", "30m":
		return time.Hour
	case "1H", "2H", "4H", "6H", "12H":
		return 48 * time.Hour
	default:
		return 30 * 24 * time.Hour
	}
}
