package conf

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

var saveConfigMu sync.Mutex

// SaveConfig persists a complete OpenList config through the same config.json
// mechanism used by bootstrap. Callers should validate a copy before replacing
// the live configuration.
func SaveConfig(next *Config) error {
	if next == nil {
		return fmt.Errorf("config is nil")
	}
	if ConfigPath == "" {
		return fmt.Errorf("config path is empty")
	}

	body, err := json.MarshalIndent(next, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}

	saveConfigMu.Lock()
	defer saveConfigMu.Unlock()

	dir := filepath.Dir(ConfigPath)
	tmp, err := os.CreateTemp(dir, ".openlist-config-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary config: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)

	mode := os.FileMode(0o600)
	if info, statErr := os.Stat(ConfigPath); statErr == nil {
		mode = info.Mode().Perm()
	}
	if err := tmp.Chmod(mode); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("set temporary config mode: %w", err)
	}
	if _, err := tmp.Write(body); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temporary config: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync temporary config: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temporary config: %w", err)
	}
	if err := os.Rename(tmpPath, ConfigPath); err != nil {
		return fmt.Errorf("replace config: %w", err)
	}
	return nil
}
