package corporateaction

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"crypto-ticket/internal/notify"
)

type FeishuNotifier struct {
	client *notify.FeishuClient
}

func NewFeishuNotifier(webhookURL string, client *http.Client) *FeishuNotifier {
	return &FeishuNotifier{client: notify.NewFeishuClient(webhookURL, client)}
}

func (n *FeishuNotifier) Notify(ctx context.Context, notification Notification) error {
	if n == nil {
		return nil
	}
	return n.client.SendCard(ctx, notify.Card{
		Title: notification.Title, Template: headerTemplate(notification.Stage), Body: formatNotification(notification),
	})
}

func formatNotification(notification Notification) string {
	job := notification.Job
	lines := []string{
		fmt.Sprintf("**交易所**: `%s`  **品种**: `%s`", job.Exchange, job.Symbol),
		fmt.Sprintf("**事件时间**: `%s`", time.UnixMilli(job.EffectiveMS).UTC().Format(time.RFC3339)),
		fmt.Sprintf("**观察比例**: `%.8f`  **因子**: `%.8f`", job.ObservedRatio, job.Factor),
		fmt.Sprintf("**状态**: `%s`  **尝试次数**: `%d`", job.Status, job.Attempts),
	}
	if notification.Message != "" {
		lines = append(lines, "**说明**: "+escapeMarkdown(notification.Message))
	}
	if notification.RetryAtMS > 0 {
		lines = append(lines, fmt.Sprintf("**下次重试**: `%s`", time.UnixMilli(notification.RetryAtMS).UTC().Format(time.RFC3339)))
	}
	if notification.RowsWritten > 0 {
		lines = append(lines, fmt.Sprintf("**写入行数**: `%d`", notification.RowsWritten))
	}
	for _, report := range notification.Timeframes {
		lines = append(lines, fmt.Sprintf("- `%s`: %s, fetched=%d, written=%d, verified=%d, mismatch=%d",
			report.Timeframe, report.Adjustment, report.RowsFetched, report.RowsWritten, report.RowsVerified, report.MismatchCount))
		if report.VerificationErr != "" {
			lines = append(lines, "  - error: "+escapeMarkdown(report.VerificationErr))
		}
	}
	return strings.Join(lines, "\n")
}

func headerTemplate(stage string) string {
	switch stage {
	case "failed", "confirmation_failed":
		return "red"
	case "retry", "suspected", "unsupported":
		return "orange"
	case "completed":
		return "green"
	default:
		return "blue"
	}
}

func escapeMarkdown(value string) string {
	value = strings.ReplaceAll(value, "`", "'")
	value = strings.ReplaceAll(value, "\n", " ")
	if len(value) > 500 {
		value = value[:500] + "..."
	}
	return value
}
