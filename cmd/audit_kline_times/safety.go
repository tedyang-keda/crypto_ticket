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

func parentDir(path string) string { return filepath.Dir(path) }

func reservePaths(report, progress string) error {
	a, err := filepath.Abs(report)
	if err != nil {
		return err
	}
	b, err := filepath.Abs(progress)
	if err != nil {
		return err
	}
	pa, err := filepath.EvalSymlinks(filepath.Dir(a))
	if err != nil {
		return err
	}
	pb, err := filepath.EvalSymlinks(filepath.Dir(b))
	if err != nil {
		return err
	}
	a = filepath.Join(pa, filepath.Base(a))
	b = filepath.Join(pb, filepath.Base(b))
	if a == b {
		return errors.New("report and progress must be distinct")
	}
	for _, p := range []string{a, b} {
		if _, err := os.Lstat(p); err == nil {
			return errors.New("report/progress already exists; choose new artifact paths")
		} else if !os.IsNotExist(err) {
			return err
		}
	}
	f, err := os.OpenFile(a, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	f, err = os.OpenFile(b, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		_ = os.Remove(a)
		return err
	}
	return f.Close()
}

func checkSafety(ctx context.Context, o options) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if o.minFreeGB > 0 {
		var st syscall.Statfs_t
		if err := syscall.Statfs(parentDir(o.report), &st); err != nil {
			return err
		}
		free := uint64(st.Bavail) * uint64(st.Bsize)
		if free < o.minFreeGB*(1<<30) {
			return fmt.Errorf("low free disk: %d bytes", free)
		}
	}
	cst := time.Now().In(time.FixedZone("CST", 8*3600))
	if cst.Hour() == 5 && cst.Minute() >= 10 && cst.Minute() < 30 {
		return errors.New("daily retention maintenance window; run after 05:30 CST")
	}
	if o.healthURL != "" {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, o.healthURL, nil)
		if err != nil {
			return errors.New("invalid health-url")
		}
		response, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
		if err != nil {
			return errors.New("live readiness check failed")
		}
		defer response.Body.Close()
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 65536))
		if response.StatusCode != http.StatusOK {
			return fmt.Errorf("live readiness HTTP %d", response.StatusCode)
		}
	}
	return nil
}
