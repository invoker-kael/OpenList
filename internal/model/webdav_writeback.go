package model

import "time"

// WebDAVWritebackObject stores the durable state exposed to WebDAV clients
// while the payload is asynchronously written to the backing storage.
//
// PathKey and ParentKey are SHA-256 hex strings. Keeping hashes in indexes
// avoids MySQL utf8mb4 index length limits for long WebDAV paths.
type WebDAVWritebackObject struct {
	ID         uint      `json:"id" gorm:"primaryKey"`
	PathKey    string    `json:"path_key" gorm:"size:64;uniqueIndex"`
	ParentKey  string    `json:"parent_key" gorm:"size:64;index;index:idx_webdav_writeback_parent_state,priority:1"`
	Path       string    `json:"path" gorm:"type:text"`
	Parent     string    `json:"parent" gorm:"type:text"`
	Name       string    `json:"name" gorm:"size:1024"`
	IsDir      bool      `json:"is_dir" gorm:"index;index:idx_webdav_writeback_dispatch,priority:2"`
	Size       int64     `json:"size" gorm:"index:idx_webdav_writeback_state_size,priority:2"`
	ModTime    time.Time `json:"mod_time"`
	CreateTime time.Time `json:"create_time"`
	ETag       string    `json:"etag" gorm:"size:160"`
	Generation uint64    `json:"generation"`
	// CanonicalState is the client-visible lifecycle. State below is retained as
	// the provider-replication lifecycle so ACKed WebDAV identity cannot flap as
	// the backing provider moves through queued/uploading/verifying/failed.
	CanonicalState   string     `json:"canonical_state" gorm:"size:24"`
	AckTime          *time.Time `json:"ack_time"`
	DurableAt        *time.Time `json:"durable_at"`
	RemoteSyncState  string     `json:"remote_sync_state" gorm:"size:16;index"`
	State            string     `json:"state" gorm:"size:24;index;index:idx_webdav_writeback_queue,priority:1;index:idx_webdav_writeback_completed,priority:1;index:idx_webdav_writeback_dispatch,priority:1;index:idx_webdav_writeback_parent_state,priority:2;index:idx_webdav_writeback_state_size,priority:1"`
	SpoolPath        string     `json:"spool_path" gorm:"type:text"`
	PayloadSHA1      string     `json:"payload_sha1" gorm:"size:40"`
	RemoteObjectID   string     `json:"remote_object_id" gorm:"size:255"`
	RemoteSHA1       string     `json:"remote_sha1" gorm:"size:40"`
	RemoteGeneration uint64     `json:"remote_generation"`
	RemoteVerifiedAt *time.Time `json:"remote_verified_at"`
	MimeType         string     `json:"mime_type" gorm:"size:255"`
	CleanupPath      string     `json:"cleanup_path" gorm:"type:text"`
	LastError        string     `json:"last_error" gorm:"type:text"`
	RetryCount       int        `json:"retry_count"`
	VerifyCount      int        `json:"verify_count"`
	RetryAt          *time.Time `json:"retry_at" gorm:"index;index:idx_webdav_writeback_queue,priority:2;index:idx_webdav_writeback_dispatch,priority:3"`
	CompletedAt      *time.Time `json:"completed_at" gorm:"index;index:idx_webdav_writeback_completed,priority:2"`
	CreatedAt        time.Time  `json:"created_at"`
	UpdatedAt        time.Time  `json:"updated_at" gorm:"index:idx_webdav_writeback_dispatch,priority:4"`
}

// WebDAVWritebackReceiveFence serializes same-path PUT publication through
// MySQL. NextSequence is allocated when a PUT starts; LastCommittedSequence is
// advanced only when that receive publishes canonical state. Keeping the fence
// independent from the canonical object preserves ordering across restart,
// multiple OpenList instances, and later canonical-row reconciliation.
type WebDAVWritebackReceiveFence struct {
	ID                    uint       `json:"id" gorm:"primaryKey"`
	PathKey               string     `json:"path_key" gorm:"size:64;uniqueIndex"`
	Path                  string     `json:"path" gorm:"type:text"`
	NextSequence          uint64     `json:"next_sequence"`
	LastCommittedSequence uint64     `json:"last_committed_sequence"`
	ActiveReceivers       int        `json:"active_receivers" gorm:"index:idx_webdav_writeback_receive_active_lease,priority:1"`
	LatestExpectedSize    int64      `json:"latest_expected_size"`
	LatestStartedAt       *time.Time `json:"latest_started_at"`
	ReceiveLeaseUntil     *time.Time `json:"receive_lease_until" gorm:"index:idx_webdav_writeback_receive_active_lease,priority:2"`
	ReceiveState          string     `json:"receive_state" gorm:"size:16;index"`
	ReceiveUpdatedAt      *time.Time `json:"receive_updated_at"`
	CreatedAt             time.Time  `json:"created_at"`
	UpdatedAt             time.Time  `json:"updated_at"`
}

// WebDAVWritebackAdmissionFence serializes backlog admission across OpenList
// instances. The singleton row is only a transaction lock.
type WebDAVWritebackAdmissionFence struct {
	ID        uint      `json:"id" gorm:"primaryKey"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// WebDAVWritebackReceiveReservation tracks one in-flight PUT's logical backlog
// reservation. Sequence ownership makes overlapping same-path receives safe.
type WebDAVWritebackReceiveReservation struct {
	ID         uint      `json:"id" gorm:"primaryKey"`
	PathKey    string    `json:"path_key" gorm:"size:64;uniqueIndex:idx_webdav_writeback_receive_reservation,priority:1"`
	Sequence   uint64    `json:"sequence" gorm:"uniqueIndex:idx_webdav_writeback_receive_reservation,priority:2"`
	Bytes      uint64    `json:"bytes" gorm:"index:idx_webdav_writeback_receive_lease_bytes,priority:2"`
	LeaseUntil time.Time `json:"lease_until" gorm:"index;index:idx_webdav_writeback_receive_lease_bytes,priority:1"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// WebDAVProviderOperation is a durable intent around synchronous provider
// COPY/MOVE mutations. It closes the gap between a successful remote mutation
// and the MySQL transaction that reconciles canonical WebDAV metadata.
type WebDAVProviderOperation struct {
	ID                            uint       `json:"id" gorm:"primaryKey"`
	OperationKey                  string     `json:"operation_key" gorm:"size:64;uniqueIndex"`
	Method                        string     `json:"method" gorm:"size:8;index"`
	SourceKey                     string     `json:"source_key" gorm:"size:64;index"`
	DestinationKey                string     `json:"destination_key" gorm:"size:64;index"`
	SourcePath                    string     `json:"source_path" gorm:"type:text"`
	DestinationPath               string     `json:"destination_path" gorm:"type:text"`
	DestinationObjectID           string     `json:"destination_object_id" gorm:"size:255"`
	DestinationGeneration         uint64     `json:"destination_generation"`
	DestinationETag               string     `json:"destination_etag" gorm:"size:160"`
	SourceObjectID                string     `json:"source_object_id" gorm:"size:255"`
	SourceGeneration              uint64     `json:"source_generation"`
	SourceETag                    string     `json:"source_etag" gorm:"size:160"`
	SourceIsDir                   bool       `json:"source_is_dir"`
	SourceSize                    int64      `json:"source_size"`
	SourceSHA1                    string     `json:"source_sha1" gorm:"size:40"`
	SourceTreeSHA256              string     `json:"source_tree_sha256" gorm:"size:64"`
	SourceTreeEntries             int        `json:"source_tree_entries"`
	SourceTreeOverlay             bool       `json:"source_tree_overlay"`
	FailureDestinationObserved    bool       `json:"failure_destination_observed"`
	FailureDestinationObjectID    string     `json:"failure_destination_object_id" gorm:"size:255"`
	FailureDestinationReady       bool       `json:"failure_destination_ready"`
	FailureDestinationIsDir       bool       `json:"failure_destination_is_dir"`
	FailureDestinationSize        int64      `json:"failure_destination_size"`
	FailureDestinationSHA1        string     `json:"failure_destination_sha1" gorm:"size:40"`
	FailureDestinationTreeSHA256  string     `json:"failure_destination_tree_sha256" gorm:"size:64"`
	FailureDestinationTreeEntries int        `json:"failure_destination_tree_entries"`
	SourceModTime                 time.Time  `json:"source_mod_time"`
	SourceCreateTime              time.Time  `json:"source_create_time"`
	Overwrite                     bool       `json:"overwrite"`
	Depth                         int        `json:"depth"`
	DestinationExisted            bool       `json:"destination_existed"`
	State                         string     `json:"state" gorm:"size:16;index;index:idx_webdav_provider_recovery,priority:1"`
	RecoveryCount                 int        `json:"recovery_count"`
	LastRecovery                  string     `json:"last_recovery" gorm:"size:24"`
	LastError                     string     `json:"last_error" gorm:"type:text"`
	LastCheckedAt                 *time.Time `json:"last_checked_at" gorm:"index:idx_webdav_provider_recovery,priority:2"`
	AppliedAt                     *time.Time `json:"applied_at"`
	CreatedAt                     time.Time  `json:"created_at"`
	UpdatedAt                     time.Time  `json:"updated_at"`
}
