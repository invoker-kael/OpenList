package conf

import (
	"fmt"
	"sync"

	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
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

	saveConfigMu.Lock()
	defer saveConfigMu.Unlock()
	if !utils.WriteJsonToFile(ConfigPath, next) {
		return fmt.Errorf("failed to persist config to %s", ConfigPath)
	}
	return nil
}
