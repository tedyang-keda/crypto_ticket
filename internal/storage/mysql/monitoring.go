package mysql

import (
	"context"
	"strconv"
	"strings"

	"crypto-ticket/internal/monitoring"
)

func (s *Store) MySQLServerStatus(ctx context.Context) (monitoring.MySQLServerStatus, error) {
	rows, err := s.db.QueryContext(ctx, `SHOW GLOBAL STATUS WHERE Variable_name IN (
		'Threads_connected', 'Threads_running', 'Max_used_connections',
		'Questions', 'Slow_queries', 'Uptime')`)
	if err != nil {
		return monitoring.MySQLServerStatus{}, err
	}
	defer rows.Close()

	values := make(map[string]uint64)
	for rows.Next() {
		var name string
		var raw string
		if err := rows.Scan(&name, &raw); err != nil {
			return monitoring.MySQLServerStatus{}, err
		}
		value, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
		if err != nil {
			continue
		}
		values[strings.ToLower(name)] = value
	}
	if err := rows.Err(); err != nil {
		return monitoring.MySQLServerStatus{}, err
	}
	return monitoring.MySQLServerStatus{
		ThreadsConnected:   int(values["threads_connected"]),
		ThreadsRunning:     int(values["threads_running"]),
		MaxUsedConnections: int(values["max_used_connections"]),
		Questions:          values["questions"],
		SlowQueries:        values["slow_queries"],
		UptimeSeconds:      values["uptime"],
	}, nil
}
