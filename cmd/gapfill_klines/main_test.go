package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"crypto-ticket/internal/gapfill"
	"crypto-ticket/internal/retention"
	"crypto-ticket/internal/timeframe"
)

func TestRetainedTaskClampsTimestampRetentionWithoutChangingManifest(t *testing.T) {
	now := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	cutoff, _ := retention.CutoffMS(retention.RuleFor("1H"), now)
	starts := []int64{cutoff - timeframe.HourMS, cutoff, cutoff + 2*timeframe.HourMS}
	original := gapfill.Task{ID: "frozen-task", Exchange: "binance", Symbol: "BTCUSDT", Timeframe: "1H", SourceTimeframe: "1H", StartMS: starts[0], EndMS: timeframe.EndMS(starts[2], "1H"), TargetStarts: append([]int64(nil), starts...)}
	got, expired := retainedTask(original, now)
	if expired != 1 || !reflect.DeepEqual(got.TargetStarts, starts[1:]) {
		t.Fatalf("retainedTask = %+v, expired=%d", got, expired)
	}
	if got.ID != original.ID || got.StartMS != cutoff || got.EndMS != original.EndMS || got.SourceTimeframe != original.SourceTimeframe {
		t.Fatalf("unexpected retained bounds/identity: %+v", got)
	}
	if !reflect.DeepEqual(original.TargetStarts, starts) || original.StartMS != starts[0] {
		t.Fatal("retainedTask mutated the frozen task")
	}
	got.TargetStarts[0]++
	if original.TargetStarts[1] != cutoff {
		t.Fatal("retained target slice aliases the original manifest")
	}
}

func TestRetainedTaskAllExpiredAndCountBasedRetention(t *testing.T) {
	now := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	cutoff, _ := retention.CutoffMS(retention.RuleFor("12H"), now)
	old := gapfill.Task{ID: "expired", Timeframe: "12H", TargetStarts: []int64{cutoff - 24*timeframe.HourMS, cutoff - 12*timeframe.HourMS}}
	got, expired := retainedTask(old, now)
	if len(got.TargetStarts) != 0 || expired != 2 || got.ID != old.ID {
		t.Fatalf("all-expired result = %+v, expired=%d", got, expired)
	}
	for _, tf := range []string{"1D", "2D", "3D", "5D", "1W", "2W", "1M", "3M"} {
		t.Run(tf, func(t *testing.T) {
			start := timeframe.FloorStartMS(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli(), tf)
			original := gapfill.Task{ID: tf, Timeframe: tf, StartMS: start, EndMS: timeframe.EndMS(start, tf), TargetStarts: []int64{start}}
			got, expired := retainedTask(original, now)
			if expired != 0 || !reflect.DeepEqual(got, original) {
				t.Fatalf("count-based retention was converted to a date cutoff: %+v, expired=%d", got, expired)
			}
		})
	}
}

func TestOpenJournalCreatesPrivateManifestAndResumesLatestResult(t *testing.T) {
	path := filepath.Join(t.TempDir(), "progress.jsonl")
	f, results, err := openJournal(path, "report-hash", true)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 0 {
		t.Fatalf("new journal results = %+v", results)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("journal mode = %v, error=%v", info, err)
	}
	entries := []gapfill.Result{
		{TaskID: "one", Status: "partial", Requested: 2, Inserted: 1, MissingStarts: []int64{2}},
		{TaskID: "two", Status: "already_present", Requested: 1, AlreadyPresent: 1},
		{TaskID: "one", Status: "repaired", Requested: 2, Inserted: 2},
	}
	for index := range entries {
		entry := journalEntry{Kind: "result", At: "2026-10-15T12:00:00Z", Result: &entries[index]}
		if err := json.NewEncoder(f).Encode(entry); err != nil {
			f.Close()
			t.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	f, results, err = openJournal(path, "report-hash", true)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if len(results) != 2 || results["one"].Status != "repaired" || results["one"].Inserted != 2 || results["two"].AlreadyPresent != 1 {
		t.Fatalf("wrong resumed latest results: %+v", results)
	}
	position, err := f.Seek(0, io.SeekCurrent)
	info, statErr := f.Stat()
	if err != nil || statErr != nil || position != info.Size() {
		t.Fatalf("journal not positioned for append: pos=%d info=%v err=%v/%v", position, info, err, statErr)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var manifest journalEntry
	if err := json.Unmarshal([]byte(strings.SplitN(string(body), "\n", 2)[0]), &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.Kind != "manifest" || manifest.ReportSHA256 != "report-hash" || !manifest.Apply || manifest.At == "" {
		t.Fatalf("invalid manifest: %+v", manifest)
	}
}

func TestOpenJournalRejectsHashModeAndConcurrentWriter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "progress.jsonl")
	f, _, err := openJournal(path, "original", true)
	if err != nil {
		t.Fatal(err)
	}
	if second, _, err := openJournal(path, "original", true); err == nil {
		second.Close()
		f.Close()
		t.Fatal("second writer acquired a locked journal")
	}
	f.Close()
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		digest string
		apply  bool
	}{{"other", true}, {"original", false}} {
		if rejected, _, err := openJournal(path, test.digest, test.apply); err == nil {
			rejected.Close()
			t.Fatalf("accepted digest=%q apply=%t", test.digest, test.apply)
		}
	}
	after, _ := os.ReadFile(path)
	if string(before) != string(after) {
		t.Fatal("rejected resume modified journal")
	}
	// Failed opens must close their descriptor and release the exclusive lock.
	f, _, err = openJournal(path, "original", true)
	if err != nil {
		t.Fatalf("failed open retained journal lock: %v", err)
	}
	f.Close()
}

func TestOpenJournalRejectsMalformedOrMissingManifest(t *testing.T) {
	for _, body := range []string{
		"not JSON\n",
		"{\"kind\":\"result\",\"result\":{\"task_id\":\"x\"}}\n",
		"{\"kind\":\"manifest\",\"report_sha256\":\"hash\",\"apply\":true}\n{\"kind\":\"result\"",
	} {
		path := filepath.Join(t.TempDir(), "progress.jsonl")
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		if f, _, err := openJournal(path, "hash", true); err == nil {
			f.Close()
			t.Fatalf("accepted invalid journal %q", body)
		}
		after, _ := os.ReadFile(path)
		if string(after) != body {
			t.Fatal("invalid journal was silently rewritten")
		}
	}
}

func TestOpenJournalResumesCompleteLastLineWithoutNewline(t *testing.T) {
	for _, tailResult := range []bool{false, true} {
		t.Run(fmt.Sprintf("result-tail-%t", tailResult), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "progress.jsonl")
			body := `{"kind":"manifest","report_sha256":"hash","apply":true}`
			if tailResult {
				body += "\n" + `{"kind":"result","result":{"task_id":"before-resume","status":"partial","inserted":2}}`
			}
			if err := os.WriteFile(path, []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
			f, results, err := openJournal(path, "hash", true)
			if err != nil {
				t.Fatal(err)
			}
			if tailResult && results["before-resume"].Inserted != 2 {
				f.Close()
				t.Fatalf("lost complete unterminated result: %+v", results)
			}
			result := gapfill.Result{TaskID: "after-resume", Status: "repaired"}
			if err := json.NewEncoder(f).Encode(journalEntry{Kind: "result", Result: &result}); err != nil {
				f.Close()
				t.Fatal(err)
			}
			f.Close()
			f, results, err = openJournal(path, "hash", true)
			if err != nil {
				t.Fatalf("resume appended JSON directly onto a complete unterminated line: %v", err)
			}
			defer f.Close()
			if results["after-resume"].Status != "repaired" || tailResult && results["before-resume"].Inserted != 2 {
				t.Fatalf("missing appended or earlier result: %+v", results)
			}
		})
	}
}

func TestSafeErrorStripsNestedClientURLs(t *testing.T) {
	if safeError(nil) != "" {
		t.Fatal("nil error must produce empty text")
	}
	underlying := errors.New("connection reset by peer")
	err := fmt.Errorf("fetch failed: %w", &url.Error{Op: "Get", URL: "https://example.invalid/klines?api_key=secret-value", Err: &url.Error{Op: "Get", URL: "https://other.invalid/?password=second-secret", Err: underlying}})
	got := safeError(err)
	if got != underlying.Error() || strings.Contains(got, "secret") || strings.Contains(got, "https://") || strings.Contains(got, "?") {
		t.Fatalf("unsafe client error text: %q", got)
	}
	if got := safeError(safeRequestError{cause: underlying}); strings.Contains(got, "secret") || !strings.Contains(got, "request failed") {
		t.Fatalf("unsafe transport wrapper: %q", got)
	}
}

func TestTransientErrorRecognizesWrappedTransportCauses(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", want: false},
		{name: "connection", err: errors.New("connection reset by peer"), want: true},
		{name: "status429", err: errors.New("HTTP status 429 Too Many Requests"), want: true},
		{name: "status503", err: errors.New("HTTP status 503 Service Unavailable"), want: true},
		{name: "okx rate", err: errors.New("OKX code 50011"), want: true},
		{name: "wrapped deadline", err: safeRequestError{cause: context.DeadlineExceeded}, want: true},
		{name: "wrapped EOF", err: safeRequestError{cause: io.EOF}, want: true},
		{name: "wrapped dial", err: safeRequestError{cause: &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}}, want: true},
		{name: "wrapped client EOF", err: &url.Error{Op: "Get", URL: "https://example.invalid/?token=secret", Err: safeRequestError{cause: io.ErrUnexpectedEOF}}, want: true},
		{name: "canceled", err: context.Canceled, want: false},
		{name: "route", err: errors.New("no verified registry market route"), want: false},
		{name: "adjustment", err: errors.New("adjustment_history_requires_review"), want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := transientError(test.err); got != test.want {
				t.Fatalf("transientError(%T, %q) = %t, want %t", test.err, safeError(test.err), got, test.want)
			}
		})
	}
}

func summaryFixture() ([]gapfill.Task, []gapfill.Skipped, map[string]gapfill.Result) {
	tasks := []gapfill.Task{
		{ID: "a", Exchange: "binance", TargetStarts: []int64{1, 2}},
		{ID: "b", Exchange: "okx", TargetStarts: []int64{3}},
		{ID: "c", Exchange: "binance", TargetStarts: []int64{4, 5, 6}},
		{ID: "pending", Exchange: "okx", TargetStarts: []int64{7, 8}},
	}
	skipped := []gapfill.Skipped{{Exchange: "binance", Symbol: "MIXED", Timeframe: "3D", MissingBars: 9, Reason: "mixed_phase"}}
	results := map[string]gapfill.Result{
		"a":       {TaskID: "a", Exchange: "binance", Status: "repaired", Requested: 2, Inserted: 1, AlreadyPresent: 1},
		"b":       {TaskID: "b", Exchange: "okx", Status: "no_data", Requested: 1, MissingStarts: []int64{3}},
		"c":       {TaskID: "c", Exchange: "binance", Status: "retention_expired", Requested: 3, Expired: 3},
		"foreign": {TaskID: "foreign", Exchange: "okx", Status: "repaired", Requested: 999, Inserted: 999},
	}
	return tasks, skipped, results
}

func readSummaryForTest(t *testing.T, path string) runSummary {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var summary runSummary
	if err := json.Unmarshal(body, &summary); err != nil {
		t.Fatal(err)
	}
	return summary
}

func TestSaveSummaryCountsSelectedTasksAndAtomicallyReplaces(t *testing.T) {
	path := filepath.Join(t.TempDir(), "summary.json")
	if err := os.WriteFile(path, []byte("old value"), 0600); err != nil {
		t.Fatal(err)
	}
	tasks, skipped, results := summaryFixture()
	if err := saveSummary(path, "digest", "audit-at", "started-at", "finished", true, tasks, skipped, results); err != nil {
		t.Fatal(err)
	}
	summary := readSummaryForTest(t, path)
	if summary.ReportSHA256 != "digest" || summary.AuditGeneratedAt != "audit-at" || summary.StartedAt != "started-at" || summary.Status != "finished" || !summary.Apply {
		t.Fatalf("wrong summary metadata: %+v", summary)
	}
	if summary.Tasks != 4 || summary.Requested != 8 || summary.CompletedTasks != 3 || summary.RemainingTasks != 1 || summary.Inserted != 1 || summary.AlreadyPresent != 1 || summary.Unresolved != 1 || summary.Expired != 3 || summary.SkippedBars != 9 {
		t.Fatalf("wrong summary counters: %+v", summary)
	}
	if summary.ByStatus["repaired"] != 1 || summary.ByStatus["no_data"] != 1 || summary.ByStatus["retention_expired"] != 1 || len(summary.ByStatus) != 3 {
		t.Fatalf("wrong status counters: %+v", summary.ByStatus)
	}
	if got := summary.ByExchange["binance"]; got.Tasks != 2 || got.Requested != 5 || got.Inserted != 1 || got.AlreadyPresent != 1 || got.Unresolved != 0 {
		t.Fatalf("wrong Binance counters: %+v", got)
	}
	if got := summary.ByExchange["okx"]; got.Tasks != 1 || got.Requested != 1 || got.Unresolved != 1 {
		t.Fatalf("wrong OKX counters: %+v", got)
	}
	if len(summary.Results) != 3 || summary.Results[0].TaskID != "a" || summary.Results[1].TaskID != "b" || summary.Results[2].TaskID != "c" || len(summary.Skipped) != 1 {
		t.Fatalf("summary included foreign tasks or did not sort results: %+v", summary)
	}
	if _, err := time.Parse(time.RFC3339, summary.UpdatedAt); err != nil {
		t.Fatalf("invalid updated timestamp: %q", summary.UpdatedAt)
	}
	if _, err := os.Stat(path + ".tmp"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temporary summary still exists: %v", err)
	}
	info, _ := os.Stat(path)
	if info.Mode().Perm() != 0600 {
		t.Fatalf("summary permission=%o", info.Mode().Perm())
	}
}

func TestSaveSummaryRunningOmitsLargeDetailsButPreservesCounters(t *testing.T) {
	path := filepath.Join(t.TempDir(), "summary.json")
	tasks, skipped, results := summaryFixture()
	if err := saveSummary(path, "digest", "audit", "started", "running", false, tasks, skipped, results); err != nil {
		t.Fatal(err)
	}
	summary := readSummaryForTest(t, path)
	if len(summary.Results) != 0 || len(summary.Skipped) != 0 || summary.SkippedBars != 9 || summary.CompletedTasks != 3 || summary.Unresolved != 1 {
		t.Fatalf("wrong compact running summary: %+v", summary)
	}
	if err := saveSummary(filepath.Join(t.TempDir(), "missing", "summary.json"), "", "", "", "", false, nil, nil, nil); err == nil {
		t.Fatal("summary write into nonexistent directory must fail")
	}
}

func TestRunPlanOnlyDoesNotOverwriteFrozenReport(t *testing.T) {
	path := filepath.Join(t.TempDir(), "report.json")
	input := []byte(`{"generated_at":"2026-10-15T04:26:02Z","results":[]}`)
	if err := os.WriteFile(path, input, 0600); err != nil {
		t.Fatal(err)
	}
	err := run(options{report: path, summary: path, exchanges: "binance,okx", planOnly: true, binanceRPS: 1, okxRPS: 1, maxRuntime: time.Second})
	if err == nil {
		t.Fatal("run accepted the frozen report as its summary destination")
	}
	after, _ := os.ReadFile(path)
	if !reflect.DeepEqual(after, input) {
		t.Fatal("path validation modified the frozen report")
	}
}

func TestValidatePathsRejectsArtifactAndTemporaryCollisions(t *testing.T) {
	for _, collision := range []string{"report-journal", "report-summary", "report-temporary", "journal-summary", "journal-temporary", "normalized"} {
		t.Run(collision, func(t *testing.T) {
			dir := t.TempDir()
			o := options{report: filepath.Join(dir, "report.json"), journal: filepath.Join(dir, "journal.jsonl"), summary: filepath.Join(dir, "summary.json")}
			switch collision {
			case "report-journal":
				o.journal = o.report
			case "report-summary":
				o.summary = o.report
			case "report-temporary":
				o.report = o.summary + ".tmp"
			case "journal-summary":
				o.journal = o.summary
			case "journal-temporary":
				o.journal = o.summary + ".tmp"
			case "normalized":
				o.journal = dir + "/unused/../report.json"
			}
			if err := validatePaths(o); err == nil {
				t.Fatalf("accepted colliding artifact paths: %+v", o)
			}
		})
	}
	base := t.TempDir()
	if err := validatePaths(options{report: filepath.Join(base, "report.json"), journal: filepath.Join(base, "journal.jsonl"), summary: filepath.Join(base, "summary.json")}); err != nil {
		t.Fatalf("distinct new artifacts rejected: %v", err)
	}
	if err := validatePaths(options{report: filepath.Join(base, "report.json"), summary: filepath.Join(base, "summary.json"), planOnly: true}); err != nil {
		t.Fatalf("plan-only empty journal rejected: %v", err)
	}
}

func TestValidatePathsResolvesSymlinksAndHardlinks(t *testing.T) {
	for _, linkType := range []string{"symlink", "hardlink", "parent-symlink", "dangling-symlink"} {
		t.Run(linkType, func(t *testing.T) {
			dir := t.TempDir()
			o := options{report: filepath.Join(dir, "report.json"), journal: filepath.Join(dir, "journal.jsonl"), summary: filepath.Join(dir, "summary.json")}
			if err := os.WriteFile(o.report, []byte("frozen-report"), 0600); err != nil {
				t.Fatal(err)
			}
			switch linkType {
			case "symlink":
				if err := os.Symlink(o.report, o.journal); err != nil {
					t.Fatal(err)
				}
			case "hardlink":
				if err := os.Link(o.report, o.journal); err != nil {
					t.Fatal(err)
				}
			case "parent-symlink":
				alias := filepath.Join(dir, "alias")
				if err := os.Symlink(dir, alias); err != nil {
					t.Fatal(err)
				}
				o.journal = filepath.Join(alias, "summary.json")
			case "dangling-symlink":
				if err := os.Symlink(o.summary, o.journal); err != nil {
					t.Fatal(err)
				}
			}
			if err := validatePaths(o); err == nil {
				t.Fatalf("accepted aliased artifacts (%s): %+v", linkType, o)
			}
			after, err := os.ReadFile(o.report)
			if err != nil || string(after) != "frozen-report" {
				t.Fatalf("path validation changed frozen report: %q, %v", after, err)
			}
		})
	}
}

func skipSafetyMaintenanceWindow(t *testing.T) {
	t.Helper()
	now := time.Now().In(time.FixedZone("CST", 8*3600))
	if now.Hour() == 5 && now.Minute() >= 10 && now.Minute() < 30 {
		t.Skip("wall-clock maintenance window intentionally takes priority over readiness checks")
	}
}

func TestCheckRunSafetyDiskAndReadiness(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.jsonl")
	if err := checkRunSafety(context.Background(), options{journal: filepath.Join(t.TempDir(), "missing", "journal"), minFreeGB: 1}); err == nil {
		t.Fatal("missing journal filesystem did not stop run")
	}
	if err := checkRunSafety(context.Background(), options{journal: path, minFreeGB: 1 << 30}); err == nil || !strings.Contains(err.Error(), "low disk") {
		t.Fatalf("unattainable disk reserve did not stop run: %v", err)
	}
	skipSafetyMaintenanceWindow(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/ok" {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	if err := checkRunSafety(context.Background(), options{journal: path, healthURL: server.URL + "/ok"}); err != nil {
		t.Fatal(err)
	}
	if err := checkRunSafety(context.Background(), options{journal: path, healthURL: server.URL + "/bad"}); err == nil || !strings.Contains(err.Error(), "503") {
		t.Fatalf("failed readiness accepted: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := checkRunSafety(ctx, options{journal: path, healthURL: server.URL + "/ok?token=secret-value"}); err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatalf("canceled readiness error missing or exposed URL: %v", err)
	}
}
