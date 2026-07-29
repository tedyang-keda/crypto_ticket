package monitoring

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"crypto-ticket/internal/notify"
)

type Severity string

const (
	SeverityWarning  Severity = "warning"
	SeverityCritical Severity = "critical"
)

type Condition struct {
	Key      string
	Title    string
	Severity Severity
	Message  string
	Active   bool
	P1       bool
}

type alertState struct {
	Active         bool
	Severity       Severity
	Title          string
	Message        string
	OpenedAt       time.Time
	LastSentAt     time.Time
	HealthyChecks  int
	ShadowRecorded bool
}

type AlertEngine struct {
	mu        sync.Mutex
	now       func() time.Time
	notifier  *notify.FeishuClient
	registry  *Registry
	p1Enabled bool
	reminder  time.Duration
	states    map[string]*alertState
	wouldFire map[string]uint64
}

func NewAlertEngine(notifier *notify.FeishuClient, registry *Registry, p1Enabled bool) *AlertEngine {
	return newAlertEngine(notifier, registry, p1Enabled, time.Now)
}

func newAlertEngine(notifier *notify.FeishuClient, registry *Registry, p1Enabled bool, now func() time.Time) *AlertEngine {
	return &AlertEngine{
		now: now, notifier: notifier, registry: registry, p1Enabled: p1Enabled,
		reminder: 30 * time.Minute, states: make(map[string]*alertState), wouldFire: make(map[string]uint64),
	}
}

func (e *AlertEngine) Evaluate(ctx context.Context, conditions []Condition) {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := e.now()
	seen := make(map[string]bool, len(conditions))
	for _, condition := range conditions {
		if condition.Key == "" {
			continue
		}
		seen[condition.Key] = true
		state := e.states[condition.Key]
		if state == nil {
			state = &alertState{}
			e.states[condition.Key] = state
		}
		if !condition.Active {
			e.recoverLocked(ctx, condition.Key, state, now)
			continue
		}
		if condition.P1 && !e.p1Enabled {
			if !state.ShadowRecorded {
				e.wouldFire[condition.Key]++
				state.ShadowRecorded = true
				log.Printf("monitoring P1 shadow alert key=%s severity=%s message=%s", condition.Key, condition.Severity, condition.Message)
			}
			state.Title = condition.Title
			state.Message = condition.Message
			continue
		}
		state.ShadowRecorded = false
		shouldSend := !state.Active || severityRank(condition.Severity) > severityRank(state.Severity) || now.Sub(state.LastSentAt) >= e.reminder
		if !state.Active {
			state.OpenedAt = now
		}
		state.Active = true
		state.Severity = condition.Severity
		state.Title = condition.Title
		state.Message = condition.Message
		state.HealthyChecks = 0
		if shouldSend {
			e.sendLocked(ctx, condition.Title, condition.Severity, condition.Message, "active", now)
			state.LastSentAt = now
		}
	}
	for key, state := range e.states {
		if !seen[key] {
			e.recoverLocked(ctx, key, state, now)
		}
	}
	e.updateMetricsLocked()
}

func (e *AlertEngine) recoverLocked(ctx context.Context, key string, state *alertState, now time.Time) {
	state.ShadowRecorded = false
	if !state.Active {
		state.HealthyChecks = 0
		return
	}
	state.HealthyChecks++
	if state.HealthyChecks < 2 {
		return
	}
	duration := now.Sub(state.OpenedAt).Round(time.Second)
	body := fmt.Sprintf("**状态**: 已恢复\n**持续时间**: `%s`\n**原告警**: %s", duration, state.Message)
	e.sendLocked(ctx, state.Title+" 已恢复", state.Severity, body, "recovered", now)
	state.Active = false
	state.HealthyChecks = 0
	state.LastSentAt = now
	log.Printf("monitoring alert recovered key=%s duration=%s", key, duration)
}

func (e *AlertEngine) sendLocked(ctx context.Context, title string, severity Severity, message string, status string, now time.Time) {
	template := "orange"
	if severity == SeverityCritical {
		template = "red"
	}
	if status == "recovered" {
		template = "green"
	}
	body := fmt.Sprintf("**级别**: `%s`\n**时间**: `%s`\n%s", severity, now.Format(time.RFC3339), message)
	if err := e.notifier.SendCard(ctx, notify.Card{Title: title, Template: template, Body: body}); err != nil {
		log.Printf("monitoring notification failed title=%q err=%v", title, err)
	}
}

func (e *AlertEngine) Active() (warning int, critical int, lines []string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for key, state := range e.states {
		if !state.Active {
			continue
		}
		if state.Severity == SeverityCritical {
			critical++
		} else {
			warning++
		}
		lines = append(lines, fmt.Sprintf("- `%s` %s", key, state.Message))
	}
	sort.Strings(lines)
	return warning, critical, lines
}

func (e *AlertEngine) WouldFire() map[string]uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make(map[string]uint64, len(e.wouldFire))
	for key, value := range e.wouldFire {
		out[key] = value
	}
	return out
}

func (e *AlertEngine) SendDaily(ctx context.Context, body string) error {
	return e.notifier.SendCard(ctx, notify.Card{Title: "行情系统每日健康摘要", Template: "blue", Body: strings.TrimSpace(body)})
}

func (e *AlertEngine) updateMetricsLocked() {
	warning := 0
	critical := 0
	for _, state := range e.states {
		if !state.Active {
			continue
		}
		if state.Severity == SeverityCritical {
			critical++
		} else {
			warning++
		}
	}
	e.registry.SetActiveAlerts(warning, critical)
}

func severityRank(severity Severity) int {
	if severity == SeverityCritical {
		return 2
	}
	return 1
}
