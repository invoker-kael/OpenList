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
	unified bool
}

var cgroupMemoryFileSets = [...]cgroupMemoryFiles{
	{limit: "/sys/fs/cgroup/memory.max", usage: "/sys/fs/cgroup/memory.current", unified: true},
	{limit: "/sys/fs/cgroup/memory/memory.limit_in_bytes", usage: "/sys/fs/cgroup/memory/memory.usage_in_bytes"},
	{limit: "/sys/fs/cgroup/memory.limit_in_bytes", usage: "/sys/fs/cgroup/memory.usage_in_bytes"},
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
	limit, usage, found := readCgroupMemory(readFile)
	if !found || limit >= stat.Total {
		return
	}

	stat.Total = limit
	available := uint64(0)
	if usage < limit {
		available = limit - usage
	}
	if stat.Available > available {
		stat.Available = available
	}
}

func readCgroupMemory(readFile memoryFileReader) (limit, usage uint64, found bool) {
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
			return limit, limit, true
		}
		usage, err = strconv.ParseUint(strings.TrimSpace(string(usageData)), 10, 64)
		if err != nil {
			return limit, limit, true
		}
		return limit, usage, true
	}
	return 0, 0, false
}
