package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

func validatePaths(o options) error {
	seen := map[string]bool{}
	for _, path := range artifactPaths(o) {
		if path == "" {
			continue
		}
		abs, err := filepath.Abs(path)
		if err != nil {
			return err
		}
		if info, statErr := os.Lstat(abs); statErr == nil && info.Mode()&os.ModeSymlink != 0 {
			if _, resolveErr := filepath.EvalSymlinks(abs); resolveErr != nil {
				return errors.New("artifact path is a dangling or unresolvable symbolic link")
			}
		}
		if resolved, err := filepath.EvalSymlinks(abs); err == nil {
			abs = resolved
		} else if parent, err := filepath.EvalSymlinks(filepath.Dir(abs)); err == nil {
			abs = filepath.Join(parent, filepath.Base(abs))
		}
		if seen[abs] {
			return errors.New("report, journal, summary and temporary paths must be distinct")
		}
		seen[abs] = true
	}
	// Also reject hard links between existing targets.
	var infos []os.FileInfo
	for _, path := range artifactPaths(o) {
		if path == "" {
			continue
		}
		info, err := os.Stat(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}
		for _, previous := range infos {
			if os.SameFile(info, previous) {
				return errors.New("artifact paths refer to the same file")
			}
		}
		infos = append(infos, info)
	}
	return nil
}

func artifactPaths(o options) []string {
	paths := []string{o.report, o.journal, o.summary, o.summary + ".tmp"}
	if o.snapshot != "" {
		paths = append(paths, o.snapshot, o.snapshot+".tmp")
	}
	return paths
}

func checkRunSafety(ctx context.Context, o options) error {
	if o.minFreeGB > 0 {
		var stat syscall.Statfs_t
		if err := syscall.Statfs(filepath.Dir(o.journal), &stat); err != nil {
			return err
		}
		free := uint64(stat.Bavail) * uint64(stat.Bsize)
		if free < o.minFreeGB*(1<<30) {
			return fmt.Errorf("low disk space: available=%d bytes", free)
		}
	}
	// Avoid the existing 05:20 CST retention DELETE / partition maintenance.
	cst := time.Now().In(time.FixedZone("CST", 8*3600))
	if cst.Hour() == 5 && cst.Minute() >= 10 && cst.Minute() < 30 {
		return errors.New("stopped for daily 05:20 CST maintenance window; resume after 05:30")
	}
	if o.healthURL != "" {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, o.healthURL, nil)
		if err != nil {
			return err
		}
		c := &http.Client{Timeout: 5 * time.Second}
		resp, err := c.Do(req)
		if err != nil {
			return errors.New("live readiness check failed")
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 65536))
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("live readiness HTTP status %d", resp.StatusCode)
		}
	}
	return nil
}
