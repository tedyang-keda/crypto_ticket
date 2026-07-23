package exchange

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

// Corporate-action ratio patterns, matched against announcement text. The
// returned ratio follows the position/quantity-multiplier convention
// (= close_before / open_after), matching the deriver's Derivation.Ratio: a
// 20-for-1 forward split yields 20, a 1-for-5 reverse split yields 0.2.
var (
	reAnnMultiply = regexp.MustCompile(`(?i)(?:×|multiplied by|position quantity\s*[×x])\s*([0-9]+(?:\.[0-9]+)?)`)
	reAnnRatioOf  = regexp.MustCompile(`(?i)ratio of\s*([0-9]+(?:\.[0-9]+)?)`)
	reAnnScale    = regexp.MustCompile(`(?i)adjustment\s+scale\s+factor\s*[:：]?\s*([0-9]+(?:\.[0-9]+)?)`)
	reAnnForRatio = regexp.MustCompile(`(?i)\b([0-9]+(?:\.[0-9]+)?)\s*[-\s]?for[-\s]?\s*([0-9]+(?:\.[0-9]+)?)\b`)
	reAnnColon    = regexp.MustCompile(`\b([0-9]+(?:\.[0-9]+)?)\s*:\s*([0-9]+(?:\.[0-9]+)?)\b`)
)

// ParseAnnouncedRatio extracts a corporate-action ratio from free announcement
// text, best-effort. It returns false when no confident ratio is found.
func ParseAnnouncedRatio(text string) (float64, bool) {
	if strings.TrimSpace(text) == "" {
		return 0, false
	}
	// Unambiguous phrasings first.
	if m := reAnnMultiply.FindStringSubmatch(text); m != nil {
		if v, err := strconv.ParseFloat(m[1], 64); err == nil && v > 0 {
			return v, true
		}
	}
	if m := reAnnRatioOf.FindStringSubmatch(text); m != nil {
		if v, err := strconv.ParseFloat(m[1], 64); err == nil && v > 0 {
			return v, true
		}
	}
	if m := reAnnScale.FindStringSubmatch(text); m != nil {
		if v, err := strconv.ParseFloat(m[1], 64); err == nil && v > 0 {
			return v, true
		}
	}
	if m := reAnnForRatio.FindStringSubmatch(text); m != nil {
		a, errA := strconv.ParseFloat(m[1], 64)
		b, errB := strconv.ParseFloat(m[2], 64)
		if errA == nil && errB == nil && a > 0 && b > 0 {
			return a / b, true
		}
	}
	// "N:M" is ambiguous with clock times, so only trust it in an explicit
	// split/consolidation context, and skip zero-padded time-looking pairs.
	lower := strings.ToLower(text)
	if strings.Contains(lower, "split") || strings.Contains(lower, "consolidat") || strings.Contains(lower, "rebase") {
		for _, m := range reAnnColon.FindAllStringSubmatch(text, -1) {
			if looksLikeClock(m[1], m[2]) {
				continue
			}
			a, errA := strconv.ParseFloat(m[1], 64)
			b, errB := strconv.ParseFloat(m[2], 64)
			if errA == nil && errB == nil && a > 0 && b > 0 {
				return a / b, true
			}
		}
	}
	return 0, false
}

func looksLikeClock(a string, b string) bool {
	// e.g. "00:30", "04:00" — two-digit zero-padded minute component.
	return len(b) == 2 && (strings.HasPrefix(a, "0") || len(a) <= 2) && b[0] >= '0' && b[0] <= '5'
}

// AnnouncementMatchesSymbol reports whether an announcement title/text refers to
// the given instrument, by its base token (e.g. "MUU" from "MUU-USDT-SWAP" or
// "MUUUSDT"). Matching is word-ish and case-insensitive.
func AnnouncementMatchesSymbol(symbol string, text string) bool {
	full := strings.ToUpper(strings.TrimSpace(symbol))
	if full != "" && containsAnnouncementToken(strings.ToUpper(text), full) {
		return true
	}
	base := announcementBaseToken(symbol)
	if base == "" {
		return false
	}
	return containsAnnouncementToken(strings.ToUpper(text), base)
}

func containsAnnouncementToken(upper string, token string) bool {
	idx := strings.Index(upper, token)
	for idx >= 0 {
		before := idx == 0 || !isAlnum(rune(upper[idx-1]))
		after := idx+len(token) >= len(upper) || !isAlnum(rune(upper[idx+len(token)]))
		if before && after {
			return true
		}
		next := strings.Index(upper[idx+1:], token)
		if next < 0 {
			break
		}
		idx = idx + 1 + next
	}
	return false
}

func announcementBaseToken(symbol string) string {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	if symbol == "" {
		return ""
	}
	if i := strings.Index(symbol, "-"); i > 0 {
		return symbol[:i] // OKX: MUU-USDT-SWAP -> MUU
	}
	for _, quote := range []string{"USDT", "USDC", "USD1", "USD"} {
		if strings.HasSuffix(symbol, quote) && len(symbol) > len(quote) {
			return strings.TrimSuffix(symbol, quote) // Binance: MUUUSDT -> MUU
		}
	}
	return symbol
}

func isAlnum(r rune) bool {
	return (r >= '0' && r <= '9') || (r >= 'A' && r <= 'Z')
}

// OKXAnnouncementVerifier checks OKX's public announcements endpoint for a
// corporate-action announcement matching a symbol, and best-effort parses the
// ratio from the title/body. It satisfies adjustment.AnnouncementVerifier.
//
// Live announcement formats vary; this is a best-effort verifier: on any fetch
// or parse failure it reports "not found", letting the deriver proceed
// (lenient) unless configured strict.
type OKXAnnouncementVerifier struct {
	restURL string
	client  *http.Client
}

// NewOKXAnnouncementVerifier builds a verifier against the OKX REST base URL.
func NewOKXAnnouncementVerifier(restURL string, client *http.Client) *OKXAnnouncementVerifier {
	if client == nil {
		client = http.DefaultClient
	}
	return &OKXAnnouncementVerifier{restURL: strings.TrimRight(strings.TrimSpace(restURL), "/"), client: client}
}

type okxAnnouncement struct {
	Title string `json:"title"`
	URL   string `json:"url"`
	PTime string `json:"pTime"`
}

// VerifyCorporateAction implements adjustment.AnnouncementVerifier.
func (v *OKXAnnouncementVerifier) VerifyCorporateAction(ctx context.Context, _ string, _ string, symbol string, _ int64) (found bool, ratio float64, hasRatio bool) {
	anns, err := v.fetchAnnouncements(ctx)
	if err != nil || len(anns) == 0 {
		return false, 0, false
	}
	for _, ann := range anns {
		if !AnnouncementMatchesSymbol(symbol, ann.Title) {
			continue
		}
		if !isCorporateActionTitle(ann.Title) {
			continue
		}
		found = true
		if r, ok := ParseAnnouncedRatio(ann.Title); ok {
			return true, r, true
		}
		// Ratio usually lives in the body; fetch it best-effort.
		if body := v.fetchBody(ctx, ann.URL); body != "" {
			if r, ok := ParseAnnouncedRatio(body); ok {
				return true, r, true
			}
		}
		return true, 0, false
	}
	return false, 0, false
}

func isCorporateActionTitle(title string) bool {
	lower := strings.ToLower(title)
	for _, kw := range []string{"corporate action", "rebase", "split", "consolidat", "adjust"} {
		if strings.Contains(lower, kw) {
			return true
		}
	}
	return false
}

func (v *OKXAnnouncementVerifier) fetchAnnouncements(ctx context.Context) ([]okxAnnouncement, error) {
	endpoint, err := url.Parse(v.restURL + "/api/v5/support/announcements")
	if err != nil {
		return nil, err
	}
	query := endpoint.Query()
	query.Set("page", "1")
	endpoint.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := v.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, nil
	}
	// OKX nests announcements under data[].details[]; decode defensively.
	var payload struct {
		Data []struct {
			Details []okxAnnouncement `json:"details"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}
	var anns []okxAnnouncement
	for _, group := range payload.Data {
		anns = append(anns, group.Details...)
	}
	return anns, nil
}

func (v *OKXAnnouncementVerifier) fetchBody(ctx context.Context, pageURL string) string {
	pageURL = strings.TrimSpace(pageURL)
	if pageURL == "" {
		return ""
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, pageURL, nil)
	if err != nil {
		return ""
	}
	resp, err := v.client.Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return ""
	}
	buf := make([]byte, 64*1024)
	n, _ := resp.Body.Read(buf)
	return stripHTMLTags(string(buf[:n]))
}

var reHTMLTag = regexp.MustCompile(`<[^>]*>`)

func stripHTMLTags(s string) string {
	return reHTMLTag.ReplaceAllString(s, " ")
}
