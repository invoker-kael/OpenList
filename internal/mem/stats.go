package mem

import (
	"os"
	"strconv"
	"strings"

	gopsutilmem "github.com/shirou/gopsutil/v4/mem"
)

type memoryFileReader func(string) ([]byte, error)

type cgroupMemoryFiles struct {
	limit   string
	usage   string
	stat    string
	unified bool
}

var cgroupMemoryFileSets = [...]cgroupMemoryFiles{
	{limit: "/sys/fs/cgroup/memory.max", usage: "/sys/fs/cgroup/memory.current", stat: "/sys/fs/cgroup/memory.stat", unified: true},
	{limit: "/sys/fs/cgroup/memory/memory.limit_in_bytes", usage: "/sys/fs/cgroup/memory/memory.usage_in_bytes", stat: "/sys/fs/cgroup/memory/memory.stat"},
	{limit: "/sys/fs/cgroup/memory.limit_in_bytes", usage: "/sys/fs/cgroup/memory.usage_in_bytes", stat: "/sys/fs/cgroup/memory.stat"},
}

// VirtualMemory returns host memory statistics constrained by the current
// cgroup's memory limit when one is available.
func VirtualMemory() (*gopsutilmem.VirtualMemoryStat, error) {
	stat, err := gopsutilmem.VirtualMemory()
	if err != nil {
		return nil, err
	}
	applyCgroupMemoryLimit(stat, os.ReadFile)
	return stat, nil
}

func applyCgroupMemoryLimit(stat *gopsutilmem.VirtualMemoryStat, readFile memoryFileReader) {
	limit, usage, reclaimable, found := readCgroupMemory(readFile)
	if !found || limit >= stat.Total {
		return
	}

	stat.Total = limit
	available := uint64(0)
	if reclaimable > usage {
		reclaimable = usage
	}
	used := usage - reclaimable
	if used < limit {
		available = limit - used
	}
	if stat.Available > available {
		stat.Available = available
	}
}

func readCgroupMemory(readFile memoryFileReader) (limit, usage, reclaimable uint64, found bool) {
	for _, files := range cgroupMemoryFileSets {
		limitData, err := readFile(files.limit)
		if err != nil {
			continue
		}
		limitText := strings.TrimSpace(string(limitData))
		if files.unified && limitText == "max" {
			continue
		}
		limit, err = strconv.ParseUint(limitText, 10, 64)
		if err != nil {
			continue
		}

		usageData, err := readFile(files.usage)
		if err != nil {
			// The limit is known but usage is not. Treat the budget as exhausted
			// so cache allocation cannot exceed the container's limit.
			return limit, limit, 0, true
		}
		usage, err = strconv.ParseUint(strings.TrimSpace(string(usageData)), 10, 64)
		if err != nil {
			return limit, limit, 0, true
		}
		if statData, err := readFile(files.stat); err == nil {
			reclaimable = inactiveFileMemory(statData, files.unified)
		}
		return limit, usage, reclaimable, true
	}
	return 0, 0, 0, false
}

func inactiveFileMemory(statData []byte, unified bool) uint64 {
	// Inactive file-backed pages can be reclaimed under memory pressure, so
	// they do not consume the budget reserved for memory-backed cache growth.
	var inactiveFile, totalInactiveFile uint64
	hasTotalInactiveFile := false
	for _, line := range strings.Split(string(statData), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		value, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			continue
		}
		switch fields[0] {
		case "inactive_file":
			inactiveFile = value
		case "total_inactive_file":
			totalInactiveFile = value
			hasTotalInactiveFile = true
		}
	}
	if !unified && hasTotalInactiveFile {
		return totalInactiveFile
	}
	return inactiveFile
}
