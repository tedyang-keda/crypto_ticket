package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"crypto-ticket/internal/timeaudit"
)

func TestNewPathsAndCompactProgress(t *testing.T) {
	dir := t.TempDir()
	rpath, ppath := filepath.Join(dir, "report.json"), filepath.Join(dir, "progress.json")
	if err := reservePaths(rpath, ppath); err != nil {
		t.Fatal(err)
	}
	if err := reservePaths(rpath, ppath); err == nil {
		t.Fatal("existing artifacts overwritten")
	}
	r := report{Status: "running", TotalPairs: 28, Results: []timeaudit.Result{{Exchange: "okx", Timeframe: "1H", BarCount: 3, Missing: 2, Series: []timeaudit.Series{{}}}}}
	if err := saveReport(options{report: rpath, progress: ppath}, &r); err != nil {
		t.Fatal(err)
	}
	bytes, err := os.ReadFile(ppath)
	if err != nil {
		t.Fatal(err)
	}
	var p map[string]any
	if err = json.Unmarshal(bytes, &p); err != nil {
		t.Fatal(err)
	}
	if p["complete"] != false || p["bar_count"] != float64(3) || p["internal_missing_bars"] != float64(2) {
		t.Fatalf("progress=%s", bytes)
	}
	result := p["results"].([]any)[0].(map[string]any)
	if _, found := result["series"]; found {
		t.Fatal("progress copied full series arrays")
	}
}

func TestPathsCannotAlias(t *testing.T) {
	path := filepath.Join(t.TempDir(), "same.json")
	if err := reservePaths(path, path); err == nil {
		t.Fatal("alias accepted")
	}
}
