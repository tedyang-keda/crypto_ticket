package exchange

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"
)

const binanceDerivativesCatalogID = "49"

var (
	reBinanceContractSymbol = regexp.MustCompile(`\b[A-Z0-9]{1,24}(?:USDT|USDC|USD1)\b`)
	reBinanceUTCDateTime    = regexp.MustCompile(`(?i)\b(20[0-9]{2}-[0-9]{2}-[0-9]{2})\s+([0-9]{2}:[0-9]{2})(?::[0-9]{2})?\s*(?:\(UTC\)|UTC)\b`)
)

type BinanceAnnouncementQuery struct {
	StartMS      int64
	EndMS        int64
	Symbols      []string
	MaxPages     int
	RequestDelay time.Duration
}

type BinanceCorporateAction struct {
	Symbol           string          `json:"symbol"`
	Ratio            float64         `json:"ratio"`
	WindowStartMS    int64           `json:"window_start_ms"`
	WindowEndMS      int64           `json:"window_end_ms"`
	PublishedMS      int64           `json:"published_ms"`
	AnnouncementCode string          `json:"announcement_code"`
	Title            string          `json:"title"`
	Raw              json.RawMessage `json:"raw"`
}

type binanceCMSArticle struct {
	Code        string
	Title       string
	Body        string
	ReleaseDate int64
}

// BinanceAnnouncementVerifier reads Binance's public CMS API. For Binance
// TRADIFI_PERPETUAL adjustments, its parsed scale factor is authoritative; the
// adjustment layer deliberately does not replace it with an empirical gap.
type BinanceAnnouncementVerifier struct {
	baseURL string
	client  *http.Client
}

func NewBinanceAnnouncementVerifier(baseURL string, client *http.Client) *BinanceAnnouncementVerifier {
	if client == nil {
		client = http.DefaultClient
	}
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if baseURL == "" {
		baseURL = "https://www.binance.com"
	}
	return &BinanceAnnouncementVerifier{baseURL: baseURL, client: client}
}

func (v *BinanceAnnouncementVerifier) VerifyCorporateAction(ctx context.Context, _ string, _ string, symbol string, boundaryMS int64) (bool, float64, bool) {
	articles, err := v.fetchArticles(ctx)
	if err != nil {
		return false, 0, false
	}
	found := false
	for _, article := range articles {
		if !AnnouncementMatchesSymbol(symbol, article.Title) || !isCorporateActionTitle(article.Title) {
			continue
		}
		detail, err := v.fetchArticle(ctx, article.Code)
		if err != nil {
			continue
		}
		if boundaryMS > 0 && detail.ReleaseDate > 0 && absInt64(detail.ReleaseDate-boundaryMS) > (14*24*time.Hour).Milliseconds() {
			continue
		}
		found = true
		text := detail.Title + " " + binanceCMSBodyText(detail.Body)
		if ratio, ok := ParseAnnouncedRatio(text); ok {
			return true, ratio, true
		}
	}
	return found, 0, false
}

func (v *BinanceAnnouncementVerifier) fetchArticles(ctx context.Context) ([]binanceCMSArticle, error) {
	return v.fetchArticlesPage(ctx, 1)
}

func (v *BinanceAnnouncementVerifier) fetchArticlesPage(ctx context.Context, page int) ([]binanceCMSArticle, error) {
	endpoint, err := url.Parse(v.baseURL + "/bapi/composite/v1/public/cms/article/catalog/list/query")
	if err != nil {
		return nil, err
	}
	query := endpoint.Query()
	query.Set("catalogId", binanceDerivativesCatalogID)
	query.Set("pageNo", fmt.Sprint(page))
	query.Set("pageSize", "20")
	endpoint.RawQuery = query.Encode()
	var payload any
	if err := v.getJSON(ctx, endpoint.String(), &payload); err != nil {
		return nil, err
	}
	if root, ok := payload.(map[string]any); ok {
		if data, exists := root["data"]; exists && data == nil {
			return nil, fmt.Errorf("binance CMS article page %d returned null data", page)
		}
	}
	return collectBinanceCMSArticles(payload), nil
}

// ListCorporateActions scans Binance CMS history and returns ratio-bearing
// contract-size adjustments. Boundary detection is intentionally left to the
// adjustment layer, which verifies each announcement against official klines.
func (v *BinanceAnnouncementVerifier) ListCorporateActions(ctx context.Context, query BinanceAnnouncementQuery) ([]BinanceCorporateAction, error) {
	if query.MaxPages <= 0 {
		query.MaxPages = 50
	}
	endMS := query.EndMS
	if endMS <= 0 {
		endMS = time.Now().UnixMilli()
	}
	wanted := make(map[string]bool, len(query.Symbols))
	for _, symbol := range query.Symbols {
		if symbol = strings.ToUpper(strings.TrimSpace(symbol)); symbol != "" {
			wanted[symbol] = true
		}
	}
	seenCodes := make(map[string]bool)
	seenActions := make(map[string]bool)
	actions := make([]BinanceCorporateAction, 0)
	for page := 1; page <= query.MaxPages; page++ {
		articles, err := v.fetchArticlesPage(ctx, page)
		if err != nil {
			return nil, err
		}
		if len(articles) == 0 {
			break
		}
		newCodes := 0
		for _, summary := range articles {
			if summary.Code == "" || seenCodes[summary.Code] {
				continue
			}
			seenCodes[summary.Code] = true
			newCodes++
			if !isCorporateActionTitle(summary.Title) {
				continue
			}
			symbols := binanceAnnouncementSymbols(summary.Title)
			if len(wanted) > 0 && !containsWantedSymbol(symbols, wanted) {
				continue
			}
			detail, err := v.fetchArticle(ctx, summary.Code)
			if err != nil {
				return nil, err
			}
			text := detail.Title + " " + binanceCMSBodyText(detail.Body)
			ratio, ok := ParseAnnouncedRatio(text)
			if !ok || ratio <= 0 {
				continue
			}
			if len(symbols) == 0 {
				symbols = binanceAnnouncementSymbols(text)
			}
			times := parseBinanceAnnouncementTimes(text)
			windowStart, windowEnd := announcementWindow(times, detail.ReleaseDate)
			publishedMS := detail.ReleaseDate
			if publishedMS == 0 {
				publishedMS = summary.ReleaseDate
			}
			if !announcementInRange(query.StartMS, endMS, publishedMS, windowStart, windowEnd) {
				continue
			}
			raw, _ := json.Marshal(map[string]any{
				"code": detail.Code, "title": detail.Title, "body": detail.Body,
				"publish_date": publishedMS,
			})
			for _, symbol := range symbols {
				if len(wanted) > 0 && !wanted[symbol] {
					continue
				}
				key := symbol + "|" + detail.Code
				if seenActions[key] {
					continue
				}
				seenActions[key] = true
				actions = append(actions, BinanceCorporateAction{
					Symbol: symbol, Ratio: ratio, WindowStartMS: windowStart, WindowEndMS: windowEnd,
					PublishedMS: publishedMS, AnnouncementCode: detail.Code, Title: detail.Title, Raw: raw,
				})
			}
			if err := waitForRequestDelay(ctx, query.RequestDelay); err != nil {
				return nil, err
			}
		}
		if newCodes == 0 {
			break
		}
	}
	sort.Slice(actions, func(i, j int) bool {
		left := actions[i].WindowStartMS
		if left == 0 {
			left = actions[i].PublishedMS
		}
		right := actions[j].WindowStartMS
		if right == 0 {
			right = actions[j].PublishedMS
		}
		return left < right
	})
	return actions, nil
}

func binanceAnnouncementSymbols(text string) []string {
	upper := strings.ToUpper(text)
	matches := reBinanceContractSymbol.FindAllString(upper, -1)
	seen := make(map[string]bool, len(matches))
	out := make([]string, 0, len(matches))
	for _, symbol := range matches {
		if !seen[symbol] {
			seen[symbol] = true
			out = append(out, symbol)
		}
	}
	return out
}

func parseBinanceAnnouncementTimes(text string) []int64 {
	matches := reBinanceUTCDateTime.FindAllStringSubmatch(text, -1)
	seen := make(map[int64]bool, len(matches))
	out := make([]int64, 0, len(matches))
	for _, match := range matches {
		parsed, err := time.ParseInLocation("2006-01-02 15:04", match[1]+" "+match[2], time.UTC)
		if err == nil && !seen[parsed.UnixMilli()] {
			seen[parsed.UnixMilli()] = true
			out = append(out, parsed.UnixMilli())
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func announcementWindow(times []int64, publishedMS int64) (int64, int64) {
	if len(times) > 0 {
		return times[0] - time.Hour.Milliseconds(), times[len(times)-1] + (2 * time.Hour).Milliseconds()
	}
	if publishedMS > 0 {
		return publishedMS - (24 * time.Hour).Milliseconds(), publishedMS + (72 * time.Hour).Milliseconds()
	}
	return 0, 0
}

func announcementInRange(startMS int64, endMS int64, publishedMS int64, windowStartMS int64, windowEndMS int64) bool {
	referenceStart := windowStartMS
	referenceEnd := windowEndMS
	if referenceStart == 0 {
		referenceStart = publishedMS
	}
	if referenceEnd == 0 {
		referenceEnd = publishedMS
	}
	return (startMS <= 0 || referenceEnd >= startMS) && (endMS <= 0 || referenceStart <= endMS)
}

func containsWantedSymbol(symbols []string, wanted map[string]bool) bool {
	for _, symbol := range symbols {
		if wanted[symbol] {
			return true
		}
	}
	return false
}

func waitForRequestDelay(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (v *BinanceAnnouncementVerifier) fetchArticle(ctx context.Context, code string) (binanceCMSArticle, error) {
	code = strings.TrimSpace(code)
	if code == "" {
		return binanceCMSArticle{}, fmt.Errorf("binance CMS article code is empty")
	}
	endpoint, err := url.Parse(v.baseURL + "/bapi/composite/v1/public/cms/article/detail/query")
	if err != nil {
		return binanceCMSArticle{}, err
	}
	query := endpoint.Query()
	query.Set("articleCode", code)
	endpoint.RawQuery = query.Encode()
	var payload any
	if err := v.getJSON(ctx, endpoint.String(), &payload); err != nil {
		return binanceCMSArticle{}, err
	}
	articles := collectBinanceCMSArticles(payload)
	if len(articles) == 0 {
		return binanceCMSArticle{}, fmt.Errorf("binance CMS detail missing article")
	}
	return articles[0], nil
}

func (v *BinanceAnnouncementVerifier) getJSON(ctx context.Context, endpoint string, target any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("clienttype", "web")
	req.Header.Set("lang", "en")
	resp, err := v.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("binance CMS status %s", resp.Status)
	}
	return json.NewDecoder(resp.Body).Decode(target)
}

func binanceCMSBodyText(body string) string {
	body = strings.TrimSpace(body)
	if body == "" {
		return ""
	}
	var root any
	if err := json.Unmarshal([]byte(body), &root); err != nil {
		return stripHTMLTags(body)
	}
	parts := make([]string, 0)
	var walk func(any)
	walk = func(current any) {
		switch node := current.(type) {
		case []any:
			for _, item := range node {
				walk(item)
			}
		case map[string]any:
			if text, ok := node["text"].(string); ok && strings.TrimSpace(text) != "" {
				parts = append(parts, text)
			}
			if child, ok := node["child"]; ok {
				walk(child)
			}
		}
	}
	walk(root)
	return strings.Join(parts, " ")
}

// collectBinanceCMSArticles walks the CMS envelope defensively. Binance has
// changed nesting between articles, catalogs, and data without versioning the
// endpoint, while the article fields themselves have remained stable.
func collectBinanceCMSArticles(value any) []binanceCMSArticle {
	var out []binanceCMSArticle
	var walk func(any)
	walk = func(current any) {
		switch node := current.(type) {
		case []any:
			for _, item := range node {
				walk(item)
			}
		case map[string]any:
			title := stringValue(firstNonEmpty(node["title"], node["articleTitle"]))
			code := stringValue(firstNonEmpty(node["code"], node["articleCode"]))
			body := stringValue(firstNonEmpty(node["body"], node["content"], node["articleBody"]))
			if strings.TrimSpace(title) != "" && (strings.TrimSpace(code) != "" || strings.TrimSpace(body) != "") {
				out = append(out, binanceCMSArticle{
					Code: code, Title: title, Body: body,
					ReleaseDate: intValue(firstNonEmpty(node["releaseDate"], node["publishDate"], node["publishTime"])),
				})
				return
			}
			for _, child := range node {
				walk(child)
			}
		}
	}
	walk(value)
	return out
}

func absInt64(value int64) int64 {
	if value < 0 {
		return -value
	}
	return value
}
