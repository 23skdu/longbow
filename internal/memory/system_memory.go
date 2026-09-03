package memory

import (
	"bufio"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

var (
	physicalMemOnce sync.Once
	cachedPhysicalMem int64
)

// GetPhysicalMemory returns the total available physical RAM or container memory limit in bytes.
// It checks in order:
// 1. LONGBOW_PHYSICAL_RAM environment variable (supports bytes or suffixes: KB, MB, GB, KiB, MiB, GiB).
// 2. Container cgroup memory limits (cgroup v2 memory.max, cgroup v1 memory.limit_in_bytes).
// 3. /proc/meminfo MemTotal (on Linux) or platform syscalls.
// 4. Default fallback (16 GB).
func GetPhysicalMemory() int64 {
	// If LONGBOW_PHYSICAL_RAM is set, allow dynamic overrides (useful for testing)
	if override := os.Getenv("LONGBOW_PHYSICAL_RAM"); override != "" {
		if bytes := parseMemorySize(override); bytes > 0 {
			return bytes
		}
	}

	physicalMemOnce.Do(func() {
		cachedPhysicalMem = detectSystemMemory()
	})

	return cachedPhysicalMem
}

// ResetPhysicalMemoryCache clears the cached physical memory value (for testing).
func ResetPhysicalMemoryCache() {
	physicalMemOnce = sync.Once{}
	cachedPhysicalMem = 0
}

func detectSystemMemory() int64 {
	// 1. Check cgroups first if running in container
	if cgroupMem := getCgroupMemoryLimit(); cgroupMem > 0 {
		return cgroupMem
	}

	// 2. Linux /proc/meminfo
	if runtime.GOOS == "linux" {
		if procMem := getProcMemInfo(); procMem > 0 {
			return procMem
		}
	}

	// 3. Fallback: 16 GB baseline
	return 16 * 1024 * 1024 * 1024
}

func getProcMemInfo() int64 {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "MemTotal:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				if kb, err := strconv.ParseInt(fields[1], 10, 64); err == nil && kb > 0 {
					return kb * 1024
				}
			}
		}
	}
	return 0
}

func getCgroupMemoryLimit() int64 {
	// cgroup v2: /sys/fs/cgroup/memory.max
	if data, err := os.ReadFile("/sys/fs/cgroup/memory.max"); err == nil {
		str := strings.TrimSpace(string(data))
		if str != "" && str != "max" {
			if val, err := strconv.ParseInt(str, 10, 64); err == nil && val > 0 && val < (1<<60) {
				return val
			}
		}
	}

	// cgroup v1: /sys/fs/cgroup/memory/memory.limit_in_bytes
	if data, err := os.ReadFile("/sys/fs/cgroup/memory/memory.limit_in_bytes"); err == nil {
		str := strings.TrimSpace(string(data))
		if str != "" {
			if val, err := strconv.ParseInt(str, 10, 64); err == nil && val > 0 && val < (1<<60) {
				return val
			}
		}
	}

	return 0
}

func parseMemorySize(s string) int64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}

	multiplier := int64(1)
	upper := strings.ToUpper(s)
	switch {
	case strings.HasSuffix(upper, "GIB"):
		multiplier = 1024 * 1024 * 1024
		s = strings.TrimSpace(s[:len(s)-3])
	case strings.HasSuffix(upper, "GB"):
		multiplier = 1000 * 1000 * 1000
		s = strings.TrimSpace(s[:len(s)-2])
	case strings.HasSuffix(upper, "MIB"):
		multiplier = 1024 * 1024
		s = strings.TrimSpace(s[:len(s)-3])
	case strings.HasSuffix(upper, "MB"):
		multiplier = 1000 * 1000
		s = strings.TrimSpace(s[:len(s)-2])
	case strings.HasSuffix(upper, "KIB"):
		multiplier = 1024
		s = strings.TrimSpace(s[:len(s)-3])
	case strings.HasSuffix(upper, "KB"):
		multiplier = 1000
		s = strings.TrimSpace(s[:len(s)-2])
	}

	if val, err := strconv.ParseInt(s, 10, 64); err == nil && val > 0 {
		return val * multiplier
	}
	return 0
}
