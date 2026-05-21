package stream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"crypto-ticket/internal/market"
)

type TickMessage struct {
	Stream string
	ID     string
	Tick   market.Tick
}

type TickStream interface {
	EnsureGroup(ctx context.Context, streamName string, group string) error
	AddTick(ctx context.Context, streamName string, tick market.Tick, maxLen int64) (string, error)
	AddTicks(ctx context.Context, streamName string, ticks []market.Tick, maxLen int64) ([]string, error)
	ReadGroup(ctx context.Context, streamName string, group string, consumer string, startID string, count int64, block time.Duration) ([]TickMessage, error)
	Ack(ctx context.Context, streamName string, group string, ids ...string) error
}

type RedisTickStream struct {
	client *redis.Client
}

func NewRedisTickStream(redisURL string) (*RedisTickStream, error) {
	options, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, err
	}
	return &RedisTickStream{client: redis.NewClient(options)}, nil
}

func (r *RedisTickStream) Close() error {
	return r.client.Close()
}

func (r *RedisTickStream) EnsureGroup(ctx context.Context, streamName string, group string) error {
	err := r.client.XGroupCreateMkStream(ctx, streamName, group, "$").Err()
	if err == nil || strings.Contains(err.Error(), "BUSYGROUP") {
		return nil
	}
	return err
}

func (r *RedisTickStream) AddTick(ctx context.Context, streamName string, tick market.Tick, maxLen int64) (string, error) {
	ids, err := r.AddTicks(ctx, streamName, []market.Tick{tick}, maxLen)
	if err != nil {
		return "", err
	}
	if len(ids) == 0 {
		return "", nil
	}
	return ids[0], nil
}

func (r *RedisTickStream) AddTicks(ctx context.Context, streamName string, ticks []market.Tick, maxLen int64) ([]string, error) {
	if len(ticks) == 0 {
		return nil, nil
	}
	pipe := r.client.Pipeline()
	commands := make([]*redis.StringCmd, 0, len(ticks))
	for _, tick := range ticks {
		args := &redis.XAddArgs{
			Stream: streamName,
			Values: tickValues(tick),
		}
		if maxLen > 0 {
			args.MaxLen = maxLen
			args.Approx = true
		}
		commands = append(commands, pipe.XAdd(ctx, args))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(commands))
	for _, command := range commands {
		id, err := command.Result()
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func tickValues(tick market.Tick) map[string]any {
	values := map[string]any{
		"exchange":   strings.ToLower(tick.Exchange),
		"symbol":     strings.ToUpper(tick.Symbol),
		"ts_ms":      tick.TsMS,
		"price":      tick.Price,
		"size":       tick.Size,
		"side":       tick.Side,
		"trade_id":   tick.TradeID,
		"event_type": tick.EventType,
		"source":     tick.Source,
		"recv_ms":    tick.RecvMS,
	}
	if len(tick.Raw) > 0 {
		values["raw"] = string(tick.Raw)
	}
	return values
}

func (r *RedisTickStream) ReadGroup(
	ctx context.Context,
	streamName string,
	group string,
	consumer string,
	startID string,
	count int64,
	block time.Duration,
) ([]TickMessage, error) {
	if startID == "" {
		startID = ">"
	}
	if count <= 0 {
		count = 200
	}
	result, err := r.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{streamName, startID},
		Count:    count,
		Block:    block,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	messages := make([]TickMessage, 0)
	for _, streamResult := range result {
		for _, message := range streamResult.Messages {
			tick, err := TickFromFields(message.Values)
			if err != nil {
				return nil, fmt.Errorf("decode stream %s id %s: %w", streamResult.Stream, message.ID, err)
			}
			messages = append(messages, TickMessage{
				Stream: streamResult.Stream,
				ID:     message.ID,
				Tick:   tick,
			})
		}
	}
	return messages, nil
}

func (r *RedisTickStream) Ack(ctx context.Context, streamName string, group string, ids ...string) error {
	if len(ids) == 0 {
		return nil
	}
	return r.client.XAck(ctx, streamName, group, ids...).Err()
}

func TickFromFields(fields map[string]any) (market.Tick, error) {
	tick := market.Tick{
		Exchange:  strings.ToLower(fieldString(fields, "exchange")),
		Symbol:    strings.ToUpper(fieldString(fields, "symbol")),
		Side:      fieldString(fields, "side"),
		TradeID:   fieldString(fields, "trade_id"),
		EventType: fieldString(fields, "event_type"),
		Source:    fieldString(fields, "source"),
	}
	var err error
	if tick.TsMS, err = fieldInt64(fields, "ts_ms"); err != nil {
		return market.Tick{}, err
	}
	if tick.Price, err = fieldFloat64(fields, "price"); err != nil {
		return market.Tick{}, err
	}
	tick.Size, _ = fieldFloat64(fields, "size")
	tick.RecvMS, _ = fieldInt64(fields, "recv_ms")
	if tick.EventType == "" {
		tick.EventType = "trade"
	}
	if tick.Source == "" {
		tick.Source = "stream"
	}
	if raw := strings.TrimSpace(fieldString(fields, "raw")); raw != "" && json.Valid([]byte(raw)) {
		tick.Raw = json.RawMessage(raw)
	}
	return tick, nil
}

func NameForExchange(exchange string, shard int) string {
	if shard < 0 {
		shard = 0
	}
	return fmt.Sprintf("ticks:%s:%02d", strings.ToLower(strings.TrimSpace(exchange)), shard)
}

func fieldString(fields map[string]any, name string) string {
	value, ok := fields[name]
	if !ok || value == nil {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return typed
	case []byte:
		return string(typed)
	default:
		return fmt.Sprint(typed)
	}
}

func fieldInt64(fields map[string]any, name string) (int64, error) {
	raw := strings.TrimSpace(fieldString(fields, name))
	if raw == "" {
		return 0, nil
	}
	return strconv.ParseInt(raw, 10, 64)
}

func fieldFloat64(fields map[string]any, name string) (float64, error) {
	raw := strings.TrimSpace(fieldString(fields, name))
	if raw == "" {
		return 0, nil
	}
	return strconv.ParseFloat(raw, 64)
}
