package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"crypto-ticket/internal/notify"
)

type config struct {
	ReadyURL    string
	DiskPath    string
	StateFile   string
	Interval    time.Duration
	HTTPTimeout time.Duration
	WebhookURL  string
	ServiceUnit string
	DryRun      bool
	Once        bool
	TestNotify  bool
}

type readyResponse struct {
	OK          bool  `json:"ok"`
	StartedAtMS int64 `json:"started_at_ms"`
}

type state struct {
	ConsecutiveFailures int     `json:"consecutive_failures"`
	HealthyChecks       int     `json:"healthy_checks"`
	ServiceAlert        bool    `json:"service_alert"`
	LastStartedAtMS     int64   `json:"last_started_at_ms"`
	RestartTimesMS      []int64 `json:"restart_times_ms"`
	RestartAlert        bool    `json:"restart_alert"`
	LastRestartCount    uint64  `json:"last_restart_count"`
	RestartCountSet     bool    `json:"restart_count_set"`
	DiskLevel           string  `json:"disk_level"`
}

type watchdog struct {
	cfg      config
	client   *http.Client
	notifier *notify.FeishuClient
	state    state
	now      func() time.Time
	disk     func(string) (float64, error)
	restarts func(context.Context, string) (uint64, error)
}

func main() {
	cfg := loadConfig()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	w := newWatchdog(cfg)
	if err := w.loadState(); err != nil {
		log.Printf("health watchdog state load failed: %v", err)
	}
	if cfg.TestNotify {
		if err := w.sendTestNotification(ctx); err != nil {
			log.Fatal(err)
		}
		return
	}
	if cfg.Once {
		if err := w.checkOnce(ctx); err != nil {
			log.Fatal(err)
		}
		return
	}
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()
	for {
		if err := w.checkOnce(ctx); err != nil {
			log.Printf("health watchdog check failed: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func loadConfig() config {
	var cfg config
	flag.BoolVar(&cfg.Once, "once", false, "run one check and exit")
	flag.BoolVar(&cfg.DryRun, "dry-run", false, "log notifications without sending Feishu messages")
	flag.BoolVar(&cfg.TestNotify, "test-notification", false, "send one Feishu test notification and exit")
	flag.Parse()
	cfg.ReadyURL = env("WATCHDOG_READY_URL", "http://127.0.0.1:8088/readyz")
	cfg.DiskPath = env("WATCHDOG_DISK_PATH", "/opt/crypto_ticket")
	cfg.StateFile = env("WATCHDOG_STATE_FILE", "/var/lib/crypto-ticket-watchdog/state.json")
	cfg.Interval = time.Duration(envInt("WATCHDOG_INTERVAL_SECONDS", 30)) * time.Second
	cfg.HTTPTimeout = time.Duration(envInt("WATCHDOG_HTTP_TIMEOUT_SECONDS", 5)) * time.Second
	cfg.WebhookURL = os.Getenv("FEISHU_WEBHOOK_URL")
	cfg.ServiceUnit = env("WATCHDOG_SERVICE_UNIT", "crypto-ticket.service")
	return cfg
}

func newWatchdog(cfg config) *watchdog {
	return &watchdog{
		cfg: cfg, client: &http.Client{Timeout: cfg.HTTPTimeout}, notifier: notify.NewFeishuClient(cfg.WebhookURL, nil),
		now: time.Now, disk: diskUsagePercent, restarts: systemdRestartCount,
	}
}

func (w *watchdog) checkOnce(ctx context.Context) error {
	now := w.now()
	ready, err := w.checkReady(ctx)
	w.evaluateService(ctx, now, ready, err)
	countedRestart := false
	if count, restartErr := w.restarts(ctx, w.cfg.ServiceUnit); restartErr == nil {
		countedRestart = w.evaluateRestartCount(ctx, now, count)
	} else {
		log.Printf("health watchdog systemd restart check failed unit=%s err=%v", w.cfg.ServiceUnit, restartErr)
	}
	w.evaluateRestarts(ctx, now, ready, countedRestart)
	usage, diskErr := w.disk(w.cfg.DiskPath)
	if diskErr != nil {
		log.Printf("health watchdog disk check failed path=%s err=%v", w.cfg.DiskPath, diskErr)
	} else {
		w.evaluateDisk(ctx, now, usage)
	}
	return w.saveState()
}

func (w *watchdog) evaluateRestartCount(ctx context.Context, now time.Time, count uint64) bool {
	countedRestart := false
	if w.state.RestartCountSet && count > w.state.LastRestartCount {
		added := count - w.state.LastRestartCount
		for i := uint64(0); i < added; i++ {
			w.state.RestartTimesMS = append(w.state.RestartTimesMS, now.UnixMilli())
		}
		countedRestart = added > 0
	}
	w.state.LastRestartCount = count
	w.state.RestartCountSet = true
	w.evaluateRestartWindow(ctx, now)
	return countedRestart
}

func (w *watchdog) checkReady(ctx context.Context) (readyResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, w.cfg.ReadyURL, nil)
	if err != nil {
		return readyResponse{}, err
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return readyResponse{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return readyResponse{}, err
	}
	var result readyResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return readyResponse{}, fmt.Errorf("decode readiness: %w", err)
	}
	if resp.StatusCode != http.StatusOK || !result.OK {
		return result, fmt.Errorf("readiness status=%d body=%s", resp.StatusCode, truncate(string(body), 500))
	}
	return result, nil
}

func (w *watchdog) evaluateService(ctx context.Context, now time.Time, ready readyResponse, err error) {
	if err != nil {
		w.state.ConsecutiveFailures++
		w.state.HealthyChecks = 0
		if w.state.ConsecutiveFailures >= 2 && !w.state.ServiceAlert {
			w.send(ctx, "行情服务不可用", "red", fmt.Sprintf("**时间**: `%s`\n**连续失败**: `%d`\n**检查地址**: `%s`\n**错误**: %s",
				now.Format(time.RFC3339), w.state.ConsecutiveFailures, w.cfg.ReadyURL, err))
			w.state.ServiceAlert = true
		}
		return
	}
	w.state.ConsecutiveFailures = 0
	if !w.state.ServiceAlert {
		w.state.HealthyChecks = 0
		return
	}
	w.state.HealthyChecks++
	if w.state.HealthyChecks >= 2 {
		w.send(ctx, "行情服务已恢复", "green", fmt.Sprintf("**时间**: `%s`\n**进程启动时间**: `%s`", now.Format(time.RFC3339), time.UnixMilli(ready.StartedAtMS).Format(time.RFC3339)))
		w.state.ServiceAlert = false
		w.state.HealthyChecks = 0
	}
}

func (w *watchdog) evaluateRestarts(ctx context.Context, now time.Time, ready readyResponse, countedBySystemd bool) {
	if ready.StartedAtMS <= 0 {
		return
	}
	if !countedBySystemd && w.state.LastStartedAtMS > 0 && ready.StartedAtMS != w.state.LastStartedAtMS {
		w.state.RestartTimesMS = append(w.state.RestartTimesMS, now.UnixMilli())
	}
	w.state.LastStartedAtMS = ready.StartedAtMS
	w.evaluateRestartWindow(ctx, now)
}

func (w *watchdog) evaluateRestartWindow(ctx context.Context, now time.Time) {
	cutoff := now.Add(-10 * time.Minute).UnixMilli()
	filtered := w.state.RestartTimesMS[:0]
	for _, item := range w.state.RestartTimesMS {
		if item >= cutoff {
			filtered = append(filtered, item)
		}
	}
	w.state.RestartTimesMS = filtered
	if len(filtered) >= 3 && !w.state.RestartAlert {
		w.send(ctx, "行情服务频繁重启", "red", fmt.Sprintf("**时间**: `%s`\n**10 分钟重启次数**: `%d`", now.Format(time.RFC3339), len(filtered)))
		w.state.RestartAlert = true
	} else if len(filtered) == 0 && w.state.RestartAlert {
		w.send(ctx, "行情服务重启风暴已恢复", "green", fmt.Sprintf("**时间**: `%s`\n最近 10 分钟没有再次重启", now.Format(time.RFC3339)))
		w.state.RestartAlert = false
	}
}

func (w *watchdog) evaluateDisk(ctx context.Context, now time.Time, usage float64) {
	level := "ok"
	if usage >= 90 {
		level = "critical"
	} else if usage >= 80 {
		level = "warning"
	}
	previous := w.state.DiskLevel
	if previous == "" {
		previous = "ok"
	}
	if level == "critical" && previous != "critical" {
		w.send(ctx, "磁盘空间严重不足", "red", fmt.Sprintf("**路径**: `%s`\n**使用率**: `%.2f%%`\n**时间**: `%s`", w.cfg.DiskPath, usage, now.Format(time.RFC3339)))
		w.state.DiskLevel = level
		return
	}
	if level == "warning" && previous == "ok" {
		w.send(ctx, "磁盘空间告警", "orange", fmt.Sprintf("**路径**: `%s`\n**使用率**: `%.2f%%`\n**时间**: `%s`", w.cfg.DiskPath, usage, now.Format(time.RFC3339)))
		w.state.DiskLevel = level
		return
	}
	if previous == "critical" && usage < 85 {
		if usage >= 80 {
			w.send(ctx, "磁盘空间由严重降为警告", "orange", fmt.Sprintf("**路径**: `%s`\n**使用率**: `%.2f%%`", w.cfg.DiskPath, usage))
			w.state.DiskLevel = "warning"
		} else if usage < 75 {
			w.send(ctx, "磁盘空间已恢复", "green", fmt.Sprintf("**路径**: `%s`\n**使用率**: `%.2f%%`", w.cfg.DiskPath, usage))
			w.state.DiskLevel = "ok"
		}
		return
	}
	if previous == "warning" && usage < 75 {
		w.send(ctx, "磁盘空间已恢复", "green", fmt.Sprintf("**路径**: `%s`\n**使用率**: `%.2f%%`", w.cfg.DiskPath, usage))
		w.state.DiskLevel = "ok"
	}
}

func (w *watchdog) send(ctx context.Context, title string, template string, body string) {
	if w.cfg.DryRun {
		log.Printf("health watchdog dry-run title=%q body=%s", title, strings.ReplaceAll(body, "\n", " "))
		return
	}
	if err := w.notifier.SendCard(ctx, notify.Card{Title: title, Template: template, Body: body}); err != nil {
		log.Printf("health watchdog notification failed title=%q err=%v", title, err)
	}
}

func (w *watchdog) sendTestNotification(ctx context.Context) error {
	if w.cfg.DryRun {
		log.Printf("health watchdog dry-run test notification")
		return nil
	}
	return w.notifier.SendCard(ctx, notify.Card{
		Title:    "行情监控测试通知",
		Template: "blue",
		Body:     fmt.Sprintf("**状态**: 部署验证\n**时间**: `%s`\n**检查地址**: `%s`", w.now().Format(time.RFC3339), w.cfg.ReadyURL),
	})
}

func (w *watchdog) loadState() error {
	body, err := os.ReadFile(w.cfg.StateFile)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return json.Unmarshal(body, &w.state)
}

func (w *watchdog) saveState() error {
	if err := os.MkdirAll(filepath.Dir(w.cfg.StateFile), 0o755); err != nil {
		return err
	}
	body, err := json.MarshalIndent(w.state, "", "  ")
	if err != nil {
		return err
	}
	temporary := w.cfg.StateFile + ".tmp"
	if err := os.WriteFile(temporary, body, 0o600); err != nil {
		return err
	}
	return os.Rename(temporary, w.cfg.StateFile)
}

func diskUsagePercent(path string) (float64, error) {
	var stats syscall.Statfs_t
	if err := syscall.Statfs(path, &stats); err != nil {
		return 0, err
	}
	if stats.Blocks == 0 {
		return 0, nil
	}
	used := stats.Blocks - stats.Bavail
	return float64(used) / float64(stats.Blocks) * 100, nil
}

func systemdRestartCount(ctx context.Context, unit string) (uint64, error) {
	command := exec.CommandContext(ctx, "systemctl", "show", unit, "--property=NRestarts", "--value")
	body, err := command.Output()
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(strings.TrimSpace(string(body)), 10, 64)
}

func env(name string, fallback string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
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

func truncate(value string, limit int) string {
	value = strings.TrimSpace(value)
	if len(value) <= limit {
		return value
	}
	return value[:limit] + "..."
}
