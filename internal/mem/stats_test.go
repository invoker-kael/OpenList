package mem

import (
	"errors"
	"testing"

	gopsutilmem "github.com/shirou/gopsutil/v4/mem"
)

func TestApplyCgroupMemoryLimit(t *testing.T) {
	stat := &gopsutilmem.VirtualMemoryStat{Total: 16 << 30, Available: 10 << 30}
	files := map[string]string{
		"/sys/fs/cgroup/memory.max":     "8589934592\n",
		"/sys/fs/cgroup/memory.current": "6442450944\n",
	}
	readFile := func(path string) ([]byte, error) {
		value, ok := files[path]
		if !ok {
			return nil, errors.New("file not found")
		}
		return []byte(value), nil
	}

	applyCgroupMemoryLimit(stat, readFile)

	if stat.Total != 8<<30 {
		t.Fatalf("Total = %d, want %d", stat.Total, uint64(8<<30))
	}
	if stat.Available != 2<<30 {
		t.Fatalf("Available = %d, want %d", stat.Available, uint64(2<<30))
	}
}

func TestApplyCgroupMemoryLimitExhausted(t *testing.T) {
	stat := &gopsutilmem.VirtualMemoryStat{Total: 16 << 30, Available: 10 << 30}
	files := map[string]string{
		"/sys/fs/cgroup/memory.max":     "8589934592",
		"/sys/fs/cgroup/memory.current": "9000000000",
	}
	applyCgroupMemoryLimit(stat, func(path string) ([]byte, error) {
		value, ok := files[path]
		if !ok {
			return nil, errors.New("file not found")
		}
		return []byte(value), nil
	})

	if stat.Available != 0 {
		t.Fatalf("Available = %d, want 0", stat.Available)
	}
}

func TestApplyCgroupMemoryLimitV1(t *testing.T) {
	stat := &gopsutilmem.VirtualMemoryStat{Total: 16 << 30, Available: 10 << 30}
	files := map[string]string{
		"/sys/fs/cgroup/memory/memory.limit_in_bytes": "8589934592",
		"/sys/fs/cgroup/memory/memory.usage_in_bytes": "7516192768",
	}
	applyCgroupMemoryLimit(stat, func(path string) ([]byte, error) {
		value, ok := files[path]
		if !ok {
			return nil, errors.New("file not found")
		}
		return []byte(value), nil
	})

	if stat.Total != 8<<30 || stat.Available != 1<<30 {
		t.Fatalf("v1 cgroup stats = Total %d, Available %d", stat.Total, stat.Available)
	}
}

func TestApplyCgroupMemoryLimitIgnoresUnlimited(t *testing.T) {
	stat := &gopsutilmem.VirtualMemoryStat{Total: 16 << 30, Available: 10 << 30}
	files := map[string]string{
		"/sys/fs/cgroup/memory.max": "max\n",
	}
	applyCgroupMemoryLimit(stat, func(path string) ([]byte, error) {
		value, ok := files[path]
		if !ok {
			return nil, errors.New("file not found")
		}
		return []byte(value), nil
	})

	if stat.Total != 16<<30 || stat.Available != 10<<30 {
		t.Fatalf("unlimited cgroup changed memory stats: Total=%d Available=%d", stat.Total, stat.Available)
	}
}

func TestApplyCgroupMemoryLimitFailsClosedWhenUsageUnavailable(t *testing.T) {
	stat := &gopsutilmem.VirtualMemoryStat{Total: 16 << 30, Available: 10 << 30}
	applyCgroupMemoryLimit(stat, func(path string) ([]byte, error) {
		if path == "/sys/fs/cgroup/memory.max" {
			return []byte("8589934592"), nil
		}
		return nil, errors.New("file not found")
	})

	if stat.Total != 8<<30 || stat.Available != 0 {
		t.Fatalf("missing usage was not conservative: Total=%d Available=%d", stat.Total, stat.Available)
	}
}
