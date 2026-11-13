package model

import "time"

// WebDAVWritebackReconcileRun tracks one manually started Cloud Sync inventory
// pass. File contents are never stored by this task.
type WebDAVWritebackReconcileRun struct {
	ID                 uint       `json:"id" gorm:"primaryKey"`
	Root               string     `json:"root" gorm:"type:text"`
	StorageID          uint       `json:"storage_id"`
	StorageDriver      string     `json:"storage_driver" gorm:"size:32"`
	StorageMountPath   string     `json:"storage_mount_path" gorm:"type:text"`
	State              string     `json:"state" gorm:"size:32;index"`
	LastError          string     `json:"last_error" gorm:"type:text"`
	SourceFiles        int64      `json:"source_files"`
	SourceBytes        int64      `json:"source_bytes"`
	RemoteFiles        int64      `json:"remote_files"`
	MatchedFiles       int64      `json:"matched_files"`
	MissingFiles       int64      `json:"missing_files"`
	ExtraFiles         int64      `json:"extra_files"`
	UnknownFiles       int64      `json:"unknown_files"`
	ScannedDirectories int64      `json:"scanned_directories"`
	DeletedFiles       int64      `json:"deleted_files"`
	StartedAt          time.Time  `json:"started_at"`
	CollectionEndedAt  *time.Time `json:"collection_ended_at"`
	ComparedAt         *time.Time `json:"compared_at"`
	CompletedAt        *time.Time `json:"completed_at"`
	CreatedAt          time.Time  `json:"created_at"`
	UpdatedAt          time.Time  `json:"updated_at"`
}

// WebDAVWritebackReconcileEntry stores only source/remote metadata for one
// path in a reconciliation run. PathKey is SHA-256 hex to keep MySQL indexes
// within utf8mb4 limits.
type WebDAVWritebackReconcileEntry struct {
	ID             uint      `json:"id" gorm:"primaryKey"`
	RunID          uint      `json:"run_id" gorm:"uniqueIndex:idx_webdav_reconcile_run_path,priority:1;index"`
	PathKey        string    `json:"path_key" gorm:"size:64;uniqueIndex:idx_webdav_reconcile_run_path,priority:2"`
	ParentKey      string    `json:"parent_key" gorm:"size:64;index"`
	Path           string    `json:"path" gorm:"type:text"`
	Parent         string    `json:"parent" gorm:"type:text"`
	Name           string    `json:"name" gorm:"size:1024"`
	IsDir          bool      `json:"is_dir"`
	LocalPresent   bool      `json:"local_present" gorm:"index"`
	Size           int64     `json:"size"`
	SHA1           string    `json:"sha1" gorm:"size:40"`
	ModTime        time.Time `json:"mod_time"`
	CreateTime     time.Time `json:"create_time"`
	MimeType       string    `json:"mime_type" gorm:"size:255"`
	RemoteSize     int64     `json:"remote_size"`
	RemoteSHA1     string    `json:"remote_sha1" gorm:"size:40"`
	RemoteObjectID string    `json:"remote_object_id" gorm:"size:255"`
	Result         string    `json:"result" gorm:"size:32;index"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}
