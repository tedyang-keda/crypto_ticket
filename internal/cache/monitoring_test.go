package cache

import "testing"

func TestParseRedisInfo(t *testing.T) {
	status := parseRedisInfo(`# Server
uptime_in_seconds:123
# Clients
connected_clients:7
blocked_clients:1
maxclients:10000
# Memory
used_memory:1048576
# Stats
instantaneous_ops_per_sec:42
rejected_connections:3
total_connections_received:99
total_commands_processed:1234
`)
	if status.ConnectedClients != 7 || status.BlockedClients != 1 || status.MaxClients != 10000 {
		t.Fatalf("unexpected client status: %+v", status)
	}
	if status.UsedMemoryBytes != 1048576 || status.OpsPerSecond != 42 || status.RejectedConnections != 3 {
		t.Fatalf("unexpected server status: %+v", status)
	}
	if status.TotalConnections != 99 || status.TotalCommandsProcessed != 1234 || status.UptimeSeconds != 123 {
		t.Fatalf("unexpected cumulative status: %+v", status)
	}
}
