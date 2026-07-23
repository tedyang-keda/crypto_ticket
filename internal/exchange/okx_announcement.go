package exchange

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

var (
	reOKXCompactContract = regexp.MustCompile(`\b([A-Z0-9]{1,24})(USDT|USDC|USD)\b`)
	reOKXRename          = regexp.MustCompile(`(?i)\b([A-Z0-9]+(?:USDT|USDC|USD))\b.*?rename(?:\s+it)?\s+to\s+\b([A-Z0-9]+(?:USDT|USDC|USD))\b`)
	reOKXUTCDateTime     = regexp.MustCompile(`(?i)\b([0-9]{2}:[0-9]{2}(?::[0-9]{2})?)\s*\(UTC\)\s*(?:,|on)?\s*([A-Z][a-z]+\s+[0-9]{1,2},\s+20[0-9]{2})\b`)
)

type OKXAnnouncementQuery struct {
	StartMS      int64
	EndMS        int64
	Symbols      []string
	MaxPages     int
	RequestDelay time.Duration
}

type OKXCorporateAction struct {
	Symbol            string          `json:"symbol"`
	PredecessorSymbol string          `json:"predecessor_symbol,omitempty"`
	Ratio             float64         `json:"ratio"`
	WindowStartMS     int64           `json:"window_start_ms"`
	WindowEndMS       int64           `json:"window_end_ms"`
	PublishedMS       int64           `json:"published_ms"`
	AnnouncementCode  string          `json:"announcement_code"`
	AnnouncementURL   string          `json:"announcement_url"`
	Title             string          `json:"title"`
	Raw               json.RawMessage `json:"raw"`
}

// ListCorporateActions scans OKX announcement history and extracts official
// rebase/split actions. Official 1m candles remain authoritative for the exact
// boundary; announcement times only provide a narrow search window.
func (v *OKXAnnouncementVerifier) ListCorporateActions(ctx context.Context, query OKXAnnouncementQuery) ([]OKXCorporateAction, error) {
	if query.MaxPages <= 0 {
		query.MaxPages = 50
	}
	endMS := query.EndMS
	if endMS <= 0 {
		endMS = time.Now().UnixMilli()
	}
	wanted := make(map[string]bool, len(query.Symbols))
	for _, symbol := range query.Symbols {
		if symbol = normalizeOKXInstrumentID(symbol); symbol != "" {
			wanted[symbol] = true
		}
	}

	seen := make(map[string]bool)
	actions := make([]OKXCorporateAction, 0)
	scanBeforeMS := query.StartMS
	if scanBeforeMS > 0 {
		scanBeforeMS -= (7 * 24 * time.Hour).Milliseconds()
	}
	oldestPublishedMS := int64(0)
	lastPage := 0
	totalPages := 0
	for page := 1; page <= query.MaxPages; page++ {
		announcements, pageTotal, err := v.fetchAnnouncementsPage(ctx, page)
		if err != nil {
			return nil, err
		}
		lastPage = page
		if pageTotal > totalPages {
			totalPages = pageTotal
		}
		if len(announcements) == 0 {
			break
		}
		for _, ann := range announcements {
			if publishedMS, err := strconv.ParseInt(ann.PTime, 10, 64); err == nil && publishedMS > 0 && (oldestPublishedMS == 0 || publishedMS < oldestPublishedMS) {
				oldestPublishedMS = publishedMS
			}
			if !isCorporateActionTitle(ann.Title) {
				continue
			}
			if len(wanted) > 0 && !okxAnnouncementMatchesWanted(ann.Title, wanted) {
				continue
			}
			body, err := v.fetchBodyStrict(ctx, ann.URL)
			if err != nil {
				return nil, err
			}
			parsed := parseOKXCorporateActions(ann, body)
			for _, action := range parsed {
				if len(wanted) > 0 && !wanted[action.Symbol] && !wanted[action.PredecessorSymbol] {
					continue
				}
				if !announcementInRange(query.StartMS, endMS, action.PublishedMS, action.WindowStartMS, action.WindowEndMS) {
					continue
				}
				key := action.Symbol + "|" + action.AnnouncementCode
				if !seen[key] {
					seen[key] = true
					actions = append(actions, action)
				}
			}
			if err := waitForRequestDelay(ctx, query.RequestDelay); err != nil {
				return nil, err
			}
		}
		if totalPages > 0 && page >= totalPages {
			break
		}
		if scanBeforeMS > 0 && oldestPublishedMS > 0 && oldestPublishedMS <= scanBeforeMS {
			break
		}
		if err := waitForRequestDelay(ctx, query.RequestDelay); err != nil {
			return nil, err
		}
	}
	if scanBeforeMS > 0 && oldestPublishedMS > scanBeforeMS && totalPages > lastPage {
		return nil, fmt.Errorf("okx announcement scan incomplete: max-pages=%d oldest_publish_ms=%d requested_start_ms=%d", query.MaxPages, oldestPublishedMS, query.StartMS)
	}
	sort.Slice(actions, func(i, j int) bool { return actions[i].WindowStartMS < actions[j].WindowStartMS })
	return actions, nil
}

func (v *OKXAnnouncementVerifier) fetchAnnouncementsPage(ctx context.Context, pageNumber int) ([]okxAnnouncement, int, error) {
	endpoint, err := url.Parse(v.restURL + "/api/v5/support/announcements")
	if err != nil {
		return nil, 0, err
	}
	query := endpoint.Query()
	query.Set("page", strconv.Itoa(pageNumber))
	endpoint.RawQuery = query.Encode()
	var payload struct {
		Code string `json:"code"`
		Msg  string `json:"msg"`
		Data []struct {
			Details   []okxAnnouncement `json:"details"`
			TotalPage string            `json:"totalPage"`
		} `json:"data"`
	}
	if err := v.getJSONWithRetry(ctx, endpoint.String(), &payload); err != nil {
		return nil, 0, err
	}
	if payload.Code != "" && payload.Code != "0" {
		return nil, 0, fmt.Errorf("okx announcements code=%s msg=%s", payload.Code, payload.Msg)
	}
	var announcements []okxAnnouncement
	totalPages := 0
	for _, group := range payload.Data {
		announcements = append(announcements, group.Details...)
		if parsed, err := strconv.Atoi(group.TotalPage); err == nil && parsed > totalPages {
			totalPages = parsed
		}
	}
	return announcements, totalPages, nil
}

func (v *OKXAnnouncementVerifier) getJSONWithRetry(ctx context.Context, endpoint string, target any) error {
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Accept", "application/json")
		resp, err := v.client.Do(req)
		if err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
			defer resp.Body.Close()
			return json.NewDecoder(resp.Body).Decode(target)
		}
		if resp != nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode != http.StatusTooManyRequests && resp.StatusCode < 500 {
				return fmt.Errorf("okx announcements status %s", resp.Status)
			}
		}
		if attempt == 2 {
			if err != nil {
				return err
			}
			return fmt.Errorf("okx announcements request failed after retries")
		}
		if err := waitForRequestDelay(ctx, time.Duration(attempt+1)*500*time.Millisecond); err != nil {
			return err
		}
	}
	return nil
}

func (v *OKXAnnouncementVerifier) fetchBodyStrict(ctx context.Context, pageURL string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimSpace(pageURL), nil)
	if err != nil {
		return "", err
	}
	resp, err := v.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("okx announcement body status %s", resp.Status)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func parseOKXCorporateActions(ann okxAnnouncement, bodyHTML string) []OKXCorporateAction {
	text := strings.Join(strings.Fields(stripHTMLTags(bodyHTML)), " ")
	compactSymbols := okxCompactContracts(ann.Title)
	if len(compactSymbols) == 0 {
		compactSymbols = okxCompactContracts(text)
	}
	predecessor, successor := okxRenameSymbols(ann.Title)
	publishedMS, _ := strconv.ParseInt(ann.PTime, 10, 64)
	code := path.Base(strings.TrimRight(ann.URL, "/"))
	raw, _ := json.Marshal(map[string]any{
		"title": ann.Title, "url": ann.URL, "publish_time": publishedMS, "body": bodyHTML,
	})

	actions := make([]OKXCorporateAction, 0, len(compactSymbols))
	for _, compact := range compactSymbols {
		if predecessor != "" && successor != "" && compact == predecessor {
			continue
		}
		ratio, actionMS, ok := parseOKXSymbolTerms(compact, text)
		if predecessor != "" && successor != "" && compact == successor {
			predecessorRatio, predecessorMS, predecessorOK := parseOKXSymbolTerms(predecessor, text)
			if predecessorOK {
				ratio, ok = predecessorRatio, true
			}
			if predecessorMS > 0 {
				actionMS = predecessorMS
			}
		}
		if !ok {
			ratio, ok = ParseAnnouncedRatio(text)
			times := parseOKXAnnouncementTimes(text)
			if len(times) > 0 {
				actionMS = times[0]
			}
		}
		if !ok || ratio <= 0 {
			continue
		}
		windowStart, windowEnd := announcementWindow(nil, publishedMS)
		if actionMS > 0 {
			windowStart = actionMS - time.Hour.Milliseconds()
			windowEnd = actionMS + (2 * time.Hour).Milliseconds()
		}
		action := OKXCorporateAction{
			Symbol: okxInstrumentID(compact), Ratio: ratio, WindowStartMS: windowStart, WindowEndMS: windowEnd,
			PublishedMS: publishedMS, AnnouncementCode: code, AnnouncementURL: ann.URL, Title: ann.Title, Raw: raw,
		}
		if successor != "" && compact == successor {
			action.PredecessorSymbol = okxInstrumentID(predecessor)
		}
		actions = append(actions, action)
	}
	return actions
}

func parseOKXSymbolTerms(compact string, text string) (float64, int64, bool) {
	pattern := regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(compact) + `\b\s+([0-9]+(?:\.[0-9]+)?)\s+([0-9]{2}:[0-9]{2}(?::[0-9]{2})?\s*\(UTC\)\s*(?:,|on)?\s*[A-Z][a-z]+\s+[0-9]{1,2},\s+20[0-9]{2})`)
	if match := pattern.FindStringSubmatch(text); match != nil {
		ratio, err := strconv.ParseFloat(match[1], 64)
		if err == nil && ratio > 0 {
			if times := parseOKXAnnouncementTimes(match[2]); len(times) > 0 {
				return ratio, times[0], true
			}
			return ratio, 0, true
		}
	}
	near := regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(compact) + `\b.{0,240}?([0-9]{2}:[0-9]{2}(?::[0-9]{2})?\s*\(UTC\)\s*(?:,|on)?\s*[A-Z][a-z]+\s+[0-9]{1,2},\s+20[0-9]{2})`)
	actionMS := int64(0)
	if match := near.FindStringSubmatch(text); match != nil {
		if times := parseOKXAnnouncementTimes(match[1]); len(times) > 0 {
			actionMS = times[0]
		}
	}
	ratio, ok := ParseAnnouncedRatio(text)
	return ratio, actionMS, ok
}

func parseOKXAnnouncementTimes(text string) []int64 {
	matches := reOKXUTCDateTime.FindAllStringSubmatch(text, -1)
	seen := make(map[int64]bool, len(matches))
	var out []int64
	for _, match := range matches {
		clock := match[1]
		format := "15:04"
		if strings.Count(clock, ":") == 2 {
			format = "15:04:05"
		}
		parsed, err := time.ParseInLocation(format+" January 2, 2006", clock+" "+match[2], time.UTC)
		if err == nil && !seen[parsed.UnixMilli()] {
			seen[parsed.UnixMilli()] = true
			out = append(out, parsed.UnixMilli())
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func okxCompactContracts(text string) []string {
	matches := reOKXCompactContract.FindAllString(strings.ToUpper(text), -1)
	seen := make(map[string]bool, len(matches))
	var out []string
	for _, compact := range matches {
		if !seen[compact] {
			seen[compact] = true
			out = append(out, compact)
		}
	}
	return out
}

func okxRenameSymbols(title string) (string, string) {
	match := reOKXRename.FindStringSubmatch(title)
	if match == nil {
		return "", ""
	}
	return strings.ToUpper(match[1]), strings.ToUpper(match[2])
}

func okxInstrumentID(compact string) string {
	upper := strings.ToUpper(strings.TrimSpace(compact))
	for _, quote := range []string{"USDT", "USDC", "USD"} {
		if strings.HasSuffix(upper, quote) && len(upper) > len(quote) {
			return strings.TrimSuffix(upper, quote) + "-" + quote + "-SWAP"
		}
	}
	return ""
}

func normalizeOKXInstrumentID(symbol string) string {
	upper := strings.ToUpper(strings.TrimSpace(symbol))
	if strings.Contains(upper, "-") {
		return upper
	}
	return okxInstrumentID(upper)
}

func NormalizeOKXInstrumentID(symbol string) string {
	return normalizeOKXInstrumentID(symbol)
}

func okxAnnouncementMatchesWanted(title string, wanted map[string]bool) bool {
	for symbol := range wanted {
		if AnnouncementMatchesSymbol(symbol, title) {
			return true
		}
	}
	return false
}
