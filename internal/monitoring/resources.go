package monitoring

import (
	"bufio"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type ResourceSnapshot struct {
	SampledAtMS int64   `json:"sampled_at_ms"`
	CPURatio    float64 `json:"cpu_ratio"`
	RSSBytes    uint64  `json:"rss_bytes"`
	MemoryRatio float64 `json:"memory_ratio"`
	OpenFDs     int     `json:"open_fds"`
	FDLimit     uint64  `json:"fd_limit"`
	FDRatio     float64 `json:"fd_ratio"`
	Goroutines  int     `json:"goroutines"`
	DiskRatio   float64 `json:"disk_ratio"`
}

type ResourceSampler struct {
	mu          sync.Mutex
	previousCPU time.Duration
	previousAt  time.Time
}

func (s *ResourceSampler) Sample(now time.Time, diskPath string) ResourceSnapshot {
	result := ResourceSnapshot{SampledAtMS: now.UnixMilli(), Goroutines: runtime.NumGoroutine()}
	result.RSSBytes, result.MemoryRatio = memoryUsage()
	result.OpenFDs, result.FDLimit, result.FDRatio = fileDescriptorUsage()
	result.DiskRatio = processDiskRatio(diskPath)
	cpu := processCPUTime()
	s.mu.Lock()
	if !s.previousAt.IsZero() && now.After(s.previousAt) && cpu >= s.previousCPU {
		elapsed := now.Sub(s.previousAt)
		result.CPURatio = float64(cpu-s.previousCPU) / float64(elapsed) / float64(maxInt(runtime.NumCPU(), 1))
	}
	s.previousCPU = cpu
	s.previousAt = now
	s.mu.Unlock()
	return result
}

func processDiskRatio(path string) float64 {
	if strings.TrimSpace(path) == "" {
		return 0
	}
	var stats syscall.Statfs_t
	if syscall.Statfs(path, &stats) != nil || stats.Blocks == 0 {
		return 0
	}
	return float64(stats.Blocks-stats.Bavail) / float64(stats.Blocks)
}

func processCPUTime() time.Duration {
	var usage syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &usage) != nil {
		return 0
	}
	return timevalDuration(usage.Utime) + timevalDuration(usage.Stime)
}

func timevalDuration(value syscall.Timeval) time.Duration {
	return time.Duration(value.Sec)*time.Second + time.Duration(value.Usec)*time.Microsecond
}

func memoryUsage() (uint64, float64) {
	pageSize := uint64(os.Getpagesize())
	if body, err := os.ReadFile("/proc/self/statm"); err == nil {
		fields := strings.Fields(string(body))
		if len(fields) >= 2 {
			pages, _ := strconv.ParseUint(fields[1], 10, 64)
			rss := pages * pageSize
			total := totalMemoryBytes()
			if total > 0 {
				return rss, float64(rss) / float64(total)
			}
			return rss, 0
		}
	}
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats.Sys, 0
}

func totalMemoryBytes() uint64 {
	file, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 2 && fields[0] == "MemTotal:" {
			kilobytes, _ := strconv.ParseUint(fields[1], 10, 64)
			return kilobytes * 1024
		}
	}
	return 0
}

func fileDescriptorUsage() (int, uint64, float64) {
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return 0, 0, 0
	}
	limit := fileDescriptorLimit()
	if limit == 0 {
		return len(entries), 0, 0
	}
	return len(entries), limit, float64(len(entries)) / float64(limit)
}

func fileDescriptorLimit() uint64 {
	file, err := os.Open("/proc/self/limits")
	if err != nil {
		return 0
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "Max open files") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 4 || fields[3] == "unlimited" {
			return 0
		}
		limit, _ := strconv.ParseUint(fields[3], 10, 64)
		return limit
	}
	return 0
}

func maxInt(left int, right int) int {
	if left > right {
		return left
	}
	return right
}
