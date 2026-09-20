package model

import "time"

// WebDAVWritebackObject stores the durable state exposed to WebDAV clients
// while the payload is asynchronously written to the backing storage.
//
// PathKey and ParentKey are SHA-256 hex strings. Keeping hashes in indexes
// avoids MySQL utf8mb4 index length limits for long WebDAV paths.
type WebDAVWritebackObject struct {
	ID          uint       `json:"id" gorm:"primaryKey"`
	PathKey     string     `json:"path_key" gorm:"size:64;uniqueIndex"`
	ParentKey   string     `json:"parent_key" gorm:"size:64;index"`
	Path        string     `json:"path" gorm:"type:text"`
	Parent      string     `json:"parent" gorm:"type:text"`
	Name        string     `json:"name" gorm:"size:1024"`
	IsDir       bool       `json:"is_dir" gorm:"index"`
	Size        int64      `json:"size"`
	ModTime     time.Time  `json:"mod_time"`
	CreateTime  time.Time  `json:"create_time"`
	ETag        string     `json:"etag" gorm:"size:160"`
	Generation  uint64     `json:"generation"`
	State       string     `json:"state" gorm:"size:24;index"`
	SpoolPath   string     `json:"spool_path" gorm:"type:text"`
	MimeType    string     `json:"mime_type" gorm:"size:255"`
	CleanupPath string     `json:"cleanup_path" gorm:"type:text"`
	LastError   string     `json:"last_error" gorm:"type:text"`
	RetryCount  int        `json:"retry_count"`
	VerifyCount int        `json:"verify_count"`
	RetryAt     *time.Time `json:"retry_at" gorm:"index"`
	CompletedAt *time.Time `json:"completed_at" gorm:"index"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
}
