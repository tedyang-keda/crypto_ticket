package cache

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"crypto-ticket/internal/monitoring"
)

type RedisMonitor struct {
	client *redis.Client
}

func NewRedisMonitor(redisURL string) (*RedisMonitor, error) {
	options, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, err
	}
	return &RedisMonitor{client: redis.NewClient(options)}, nil
}

func (m *RedisMonitor) Close() error {
	return m.client.Close()
}

func (m *RedisMonitor) RedisStatus(ctx context.Context) (monitoring.RedisServerStatus, monitoring.RedisPoolStatus, time.Duration, error) {
	started := time.Now()
	if err := m.client.Ping(ctx).Err(); err != nil {
		return monitoring.RedisServerStatus{}, redisPoolStatus(m.client.PoolStats()), time.Since(started), err
	}
	pingLatency := time.Since(started)
	body, err := m.client.Info(ctx, "server", "clients", "memory", "stats").Result()
	if err != nil {
		return monitoring.RedisServerStatus{}, redisPoolStatus(m.client.PoolStats()), pingLatency, err
	}
	return parseRedisInfo(body), redisPoolStatus(m.client.PoolStats()), pingLatency, nil
}

func redisPoolStatus(stats *redis.PoolStats) monitoring.RedisPoolStatus {
	if stats == nil {
		return monitoring.RedisPoolStatus{}
	}
	return monitoring.RedisPoolStatus{
		Hits: stats.Hits, Misses: stats.Misses, Timeouts: stats.Timeouts,
		TotalConns: stats.TotalConns, IdleConns: stats.IdleConns, StaleConns: stats.StaleConns,
	}
}

func parseRedisInfo(body string) monitoring.RedisServerStatus {
	values := make(map[string]string)
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, ":")
		if ok {
			values[key] = strings.TrimSpace(value)
		}
	}
	return monitoring.RedisServerStatus{
		ConnectedClients:       int(parseRedisUint(values["connected_clients"])),
		BlockedClients:         int(parseRedisUint(values["blocked_clients"])),
		MaxClients:             int(parseRedisUint(values["maxclients"])),
		UsedMemoryBytes:        parseRedisUint(values["used_memory"]),
		OpsPerSecond:           int(parseRedisUint(values["instantaneous_ops_per_sec"])),
		RejectedConnections:    parseRedisUint(values["rejected_connections"]),
		TotalConnections:       parseRedisUint(values["total_connections_received"]),
		TotalCommandsProcessed: parseRedisUint(values["total_commands_processed"]),
		UptimeSeconds:          parseRedisUint(values["uptime_in_seconds"]),
	}
}

func parseRedisUint(value string) uint64 {
	parsed, _ := strconv.ParseUint(strings.TrimSpace(value), 10, 64)
	return parsed
}
