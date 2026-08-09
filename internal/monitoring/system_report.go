package monitoring

import (
	"fmt"
	"time"
)

func formatSystemQuality(now time.Time, snapshot Snapshot) []string {
	resources := snapshot.Resources
	lines := []string{
		"**服务器资源**:",
		fmt.Sprintf("- 主机: CPU=%s, memory=%s/%s (%s), disk=%s/%s (%s)",
			formatPercent(resources.HostCPURatio), formatBytes(resources.HostMemoryUsedBytes), formatBytes(resources.HostMemoryTotalBytes), formatPercent(resources.HostMemoryRatio),
			formatBytes(resources.DiskUsedBytes), formatBytes(resources.DiskTotalBytes), formatPercent(resources.DiskRatio)),
		fmt.Sprintf("- marketd: CPU=%s, RSS=%s, FD=%d/%d (%s), goroutines=%d",
			formatPercent(resources.CPURatio), formatBytes(resources.RSSBytes), resources.OpenFDs, resources.FDLimit, formatPercent(resources.FDRatio), resources.Goroutines),
		"**基础设施**:",
		formatMySQLSnapshot(snapshot.MySQL),
		formatRedisSnapshot(snapshot.Redis),
		"**服务流量（近5分钟）**:",
		formatHTTPTraffic(now, snapshot),
		formatWSTraffic(snapshot.Window),
	}
	return lines
}

func formatMySQLSnapshot(snapshot MySQLSnapshot) string {
	if !snapshot.Available {
		status := "未配置"
		if snapshot.LastError != "" {
			status = "不可用: " + snapshot.LastError
		}
		return "- MySQL: " + status
	}
	extra := ""
	if snapshot.LastError != "" {
		extra = ", stats_error=" + snapshot.LastError
	}
	return fmt.Sprintf("- MySQL: 在线, ping=%s, pool=%d open/%d in_use/%d idle/%d max, server=%d connected/%d running, QPS=%.2f, slow_queries_total=%d, pool_wait_total=%d (%s)%s",
		snapshot.PingLatency.Round(time.Millisecond), snapshot.OpenConnections, snapshot.InUse, snapshot.Idle, snapshot.MaxOpenConnections,
		snapshot.ThreadsConnected, snapshot.ThreadsRunning, snapshot.QPS, snapshot.SlowQueries, snapshot.WaitCount, snapshot.WaitDuration.Round(time.Millisecond), extra)
}

func formatRedisSnapshot(snapshot RedisSnapshot) string {
	if !snapshot.Enabled {
		return "- Redis: 未启用监控"
	}
	if !snapshot.Available {
		return "- Redis: 不可用: " + snapshot.LastError
	}
	return fmt.Sprintf("- Redis: 在线, ping=%s, clients=%d/%d, blocked=%d, ops/s=%d, memory=%s, pool=%d total/%d idle, hits_total=%d, misses_total=%d, timeouts_total=%d, rejected_total=%d",
		snapshot.PingLatency.Round(time.Millisecond), snapshot.Server.ConnectedClients, snapshot.Server.MaxClients, snapshot.Server.BlockedClients,
		snapshot.Server.OpsPerSecond, formatBytes(snapshot.Server.UsedMemoryBytes), snapshot.Pool.TotalConns, snapshot.Pool.IdleConns,
		snapshot.Pool.Hits, snapshot.Pool.Misses, snapshot.Pool.Timeouts, snapshot.Server.RejectedConnections)
}

func formatHTTPTraffic(now time.Time, snapshot Snapshot) string {
	windowSeconds := 5 * time.Minute.Seconds()
	if uptime := now.Sub(snapshot.StartedAt).Seconds(); uptime > 0 && uptime < windowSeconds {
		windowSeconds = uptime
	}
	qps := 0.0
	if windowSeconds > 0 {
		qps = float64(snapshot.Window.HTTPRequests5m) / windowSeconds
	}
	successes := snapshot.Window.HTTPRequests5m - snapshot.Window.HTTP5xx5m
	return fmt.Sprintf("- HTTP: connections=%d, requests=%d, QPS=%.2f, non_5xx_success=%s, 5xx=%d, P95=%s",
		snapshot.Window.HTTPConnections, snapshot.Window.HTTPRequests5m, qps,
		formatSuccessRate(successes, snapshot.Window.HTTPRequests5m), snapshot.Window.HTTP5xx5m, snapshot.Window.HTTPP95.Round(time.Millisecond))
}

func formatWSTraffic(window WindowSnapshot) string {
	return fmt.Sprintf("- Public WebSocket: connections=%d, handshake=%d/%d (%s), writes=%d/%d (%s), slow_consumer_drops=%d",
		window.WSConnections, window.WSHandshakeSuccesses5m, window.WSHandshakeAttempts5m,
		formatSuccessRate(window.WSHandshakeSuccesses5m, window.WSHandshakeAttempts5m),
		window.WSWriteSuccesses5m, window.WSWriteAttempts5m, formatSuccessRate(window.WSWriteSuccesses5m, window.WSWriteAttempts5m), window.WSDrops5m)
}

func formatSuccessRate(successes int, total int) string {
	if total <= 0 {
		return "n/a"
	}
	return fmt.Sprintf("%.2f%%", float64(successes)*100/float64(total))
}

func formatPercent(value float64) string {
	return fmt.Sprintf("%.2f%%", value*100)
}

func formatBytes(value uint64) string {
	const unit = 1024
	if value < unit {
		return fmt.Sprintf("%d B", value)
	}
	divisor := uint64(unit)
	units := []string{"KiB", "MiB", "GiB", "TiB"}
	unitIndex := 0
	for value/divisor >= unit && unitIndex < len(units)-1 {
		divisor *= unit
		unitIndex++
	}
	return fmt.Sprintf("%.1f %s", float64(value)/float64(divisor), units[unitIndex])
}
