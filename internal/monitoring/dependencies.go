package monitoring

import (
	"context"
	"database/sql"
	"time"
)

type MySQLServerStatus struct {
	ThreadsConnected   int
	ThreadsRunning     int
	MaxUsedConnections int
	Questions          uint64
	SlowQueries        uint64
	UptimeSeconds      uint64
}

type MySQLSnapshot struct {
	Available          bool
	LastError          string
	PingLatency        time.Duration
	MaxOpenConnections int
	OpenConnections    int
	InUse              int
	Idle               int
	WaitCount          int64
	WaitDuration       time.Duration
	ThreadsConnected   int
	ThreadsRunning     int
	MaxUsedConnections int
	QPS                float64
	SlowQueries        uint64
}

type RedisServerStatus struct {
	ConnectedClients       int
	BlockedClients         int
	MaxClients             int
	UsedMemoryBytes        uint64
	OpsPerSecond           int
	RejectedConnections    uint64
	TotalConnections       uint64
	TotalCommandsProcessed uint64
	UptimeSeconds          uint64
}

type RedisPoolStatus struct {
	Hits       uint32
	Misses     uint32
	Timeouts   uint32
	TotalConns uint32
	IdleConns  uint32
	StaleConns uint32
}

type RedisSnapshot struct {
	Enabled     bool
	Available   bool
	LastError   string
	PingLatency time.Duration
	Server      RedisServerStatus
	Pool        RedisPoolStatus
}

type MySQLStatusProvider interface {
	DBStats() sql.DBStats
	MySQLServerStatus(context.Context) (MySQLServerStatus, error)
}

type RedisStatusProvider interface {
	RedisStatus(context.Context) (RedisServerStatus, RedisPoolStatus, time.Duration, error)
}

type Dependencies struct {
	Redis RedisStatusProvider
}
