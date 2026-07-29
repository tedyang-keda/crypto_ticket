package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type Card struct {
	Title    string
	Template string
	Body     string
}

type FeishuClient struct {
	webhookURL  string
	client      *http.Client
	maxAttempts int
	baseDelay   time.Duration
}

func NewFeishuClient(webhookURL string, client *http.Client) *FeishuClient {
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	return &FeishuClient{
		webhookURL:  strings.TrimSpace(webhookURL),
		client:      client,
		maxAttempts: 3,
		baseDelay:   500 * time.Millisecond,
	}
}

func (c *FeishuClient) SendCard(ctx context.Context, card Card) error {
	if c == nil || c.webhookURL == "" {
		return nil
	}
	template := strings.TrimSpace(card.Template)
	if template == "" {
		template = "blue"
	}
	payload := map[string]any{
		"msg_type": "interactive",
		"card": map[string]any{
			"config": map[string]any{"wide_screen_mode": true},
			"header": map[string]any{
				"template": template,
				"title":    map[string]string{"tag": "plain_text", "content": card.Title},
			},
			"elements": []map[string]any{{
				"tag":  "div",
				"text": map[string]string{"tag": "lark_md", "content": card.Body},
			}},
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	var lastErr error
	for attempt := 0; attempt < c.maxAttempts; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.webhookURL, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := c.client.Do(req)
		if err == nil {
			responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
			resp.Body.Close()
			if readErr != nil {
				lastErr = readErr
			} else if resp.StatusCode >= 200 && resp.StatusCode < 300 && feishuResponseOK(responseBody) {
				return nil
			} else if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				lastErr = fmt.Errorf("feishu webhook rejected request: %s", strings.TrimSpace(string(responseBody)))
			} else {
				lastErr = fmt.Errorf("feishu webhook status %s", resp.Status)
			}
		} else {
			lastErr = err
		}
		if attempt+1 < c.maxAttempts {
			timer := time.NewTimer(c.baseDelay * time.Duration(1<<attempt))
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
	return lastErr
}

func feishuResponseOK(body []byte) bool {
	if len(bytes.TrimSpace(body)) == 0 {
		return true
	}
	var response struct {
		Code       int `json:"code"`
		StatusCode int `json:"StatusCode"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return false
	}
	return response.Code == 0 && response.StatusCode == 0
}
