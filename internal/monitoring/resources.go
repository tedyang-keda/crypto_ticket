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
	SampledAtMS          int64   `json:"sampled_at_ms"`
	CPURatio             float64 `json:"cpu_ratio"`
	HostCPURatio         float64 `json:"host_cpu_ratio"`
	RSSBytes             uint64  `json:"rss_bytes"`
	MemoryRatio          float64 `json:"memory_ratio"`
	HostMemoryUsedBytes  uint64  `json:"host_memory_used_bytes"`
	HostMemoryTotalBytes uint64  `json:"host_memory_total_bytes"`
	HostMemoryRatio      float64 `json:"host_memory_ratio"`
	OpenFDs              int     `json:"open_fds"`
	FDLimit              uint64  `json:"fd_limit"`
	FDRatio              float64 `json:"fd_ratio"`
	Goroutines           int     `json:"goroutines"`
	DiskUsedBytes        uint64  `json:"disk_used_bytes"`
	DiskTotalBytes       uint64  `json:"disk_total_bytes"`
	DiskRatio            float64 `json:"disk_ratio"`
}

type ResourceSampler struct {
	mu                sync.Mutex
	previousCPU       time.Duration
	previousAt        time.Time
	previousHostIdle  uint64
	previousHostTotal uint64
}

func (s *ResourceSampler) Sample(now time.Time, diskPath string) ResourceSnapshot {
	result := ResourceSnapshot{SampledAtMS: now.UnixMilli(), Goroutines: runtime.NumGoroutine()}
	result.RSSBytes, result.MemoryRatio = memoryUsage()
	result.HostMemoryUsedBytes, result.HostMemoryTotalBytes, result.HostMemoryRatio = hostMemoryUsage()
	result.OpenFDs, result.FDLimit, result.FDRatio = fileDescriptorUsage()
	result.DiskUsedBytes, result.DiskTotalBytes, result.DiskRatio = diskUsage(diskPath)
	cpu := processCPUTime()
	hostIdle, hostTotal := hostCPUTimes()
	s.mu.Lock()
	if !s.previousAt.IsZero() && now.After(s.previousAt) && cpu >= s.previousCPU {
		elapsed := now.Sub(s.previousAt)
		result.CPURatio = float64(cpu-s.previousCPU) / float64(elapsed) / float64(maxInt(runtime.NumCPU(), 1))
	}
	if s.previousHostTotal > 0 && hostTotal > s.previousHostTotal {
		totalDelta := hostTotal - s.previousHostTotal
		idleDelta := hostIdle - s.previousHostIdle
		if totalDelta > 0 && idleDelta <= totalDelta {
			result.HostCPURatio = float64(totalDelta-idleDelta) / float64(totalDelta)
		}
	}
	s.previousCPU = cpu
	s.previousAt = now
	s.previousHostIdle = hostIdle
	s.previousHostTotal = hostTotal
	s.mu.Unlock()
	return result
}

func diskUsage(path string) (uint64, uint64, float64) {
	if strings.TrimSpace(path) == "" {
		return 0, 0, 0
	}
	var stats syscall.Statfs_t
	if syscall.Statfs(path, &stats) != nil || stats.Blocks == 0 {
		return 0, 0, 0
	}
	total := stats.Blocks * uint64(stats.Bsize)
	used := (stats.Blocks - stats.Bavail) * uint64(stats.Bsize)
	return used, total, float64(used) / float64(total)
}

func hostCPUTimes() (uint64, uint64) {
	body, err := os.ReadFile("/proc/stat")
	if err != nil {
		return 0, 0
	}
	line, _, _ := strings.Cut(string(body), "\n")
	fields := strings.Fields(line)
	if len(fields) < 5 || fields[0] != "cpu" {
		return 0, 0
	}
	var values []uint64
	for _, field := range fields[1:] {
		value, err := strconv.ParseUint(field, 10, 64)
		if err != nil {
			return 0, 0
		}
		values = append(values, value)
	}
	var total uint64
	for _, value := range values {
		total += value
	}
	idle := values[3]
	if len(values) > 4 {
		idle += values[4]
	}
	return idle, total
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

func hostMemoryUsage() (uint64, uint64, float64) {
	file, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, 0, 0
	}
	defer file.Close()
	var total uint64
	var available uint64
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 {
			continue
		}
		value, _ := strconv.ParseUint(fields[1], 10, 64)
		switch fields[0] {
		case "MemTotal:":
			total = value * 1024
		case "MemAvailable:":
			available = value * 1024
		}
	}
	if total == 0 || available > total {
		return 0, total, 0
	}
	used := total - available
	return used, total, float64(used) / float64(total)
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
