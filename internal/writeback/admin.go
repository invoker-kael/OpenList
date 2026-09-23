package writeback

import (
	"fmt"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/shirou/gopsutil/v4/disk"
)

const maxAdminWorkers = 256

type CacheCleanupResult struct {
	Files     int    `json:"files"`
	Bytes     uint64 `json:"bytes"`
	Truncated bool   `json:"truncated,omitempty"`
}

type AdminRuntimeStats struct {
	DiskTotalBytes              uint64 `json:"disk_total_bytes"`
	DiskUsedBytes               uint64 `json:"disk_used_bytes"`
	DiskFreeBytes               uint64 `json:"disk_free_bytes"`
	DiskError                   string `json:"disk_error,omitempty"`
	ReceivingReservationBytes   uint64 `json:"receiving_reservation_bytes"`
	MissingSpool                int64  `json:"missing_spool"`
	RestartRecovery             int64  `json:"restart_recovery"`
	WaitingProviderVerification int64  `json:"waiting_provider_verification"`
	NeedsCloudSyncRehydrate     int64  `json:"needs_cloudsync_rehydrate"`
	AutomaticRecovery           int64  `json:"automatic_recovery"`
	RemoteHashMismatch          int64  `json:"remote_hash_mismatch"`
}

func ValidateAdminConfig(cfg conf.WebDAVWritebackConfig) error {
	if strings.TrimSpace(cfg.SpoolDir) == "" {
		return fmt.Errorf("spool_dir must not be empty")
	}
	if cfg.Workers < 1 || cfg.Workers > maxAdminWorkers {
		return fmt.Errorf("workers must be between 1 and %d", maxAdminWorkers)
	}
	for name, value := range map[string]int{
		"upload_workers":         cfg.UploadWorkers,
		"large_upload_workers":   cfg.LargeUploadWorkers,
		"provider_probe_workers": cfg.ProviderProbeWorkers,
	} {
		if value < 1 || value > cfg.Workers {
			return fmt.Errorf("%s must be between 1 and workers (%d)", name, cfg.Workers)
		}
	}
	if cfg.IncomingReservationChunkMB == 0 {
		return fmt.Errorf("incoming_reservation_chunk_mb must be greater than 0")
	}
	if cfg.AdmissionRetrySeconds <= 0 {
		return fmt.Errorf("admission_retry_seconds must be greater than 0")
	}
	if cfg.CloudSyncSettleMillis < 0 || cfg.CloudSyncPlaceholderMillis < 0 {
		return fmt.Errorf("Cloud Sync settle/placeholder delays must not be negative")
	}
	if cfg.DirectoryGraceSeconds < 0 {
		return fmt.Errorf("directory_grace_seconds must not be negative")
	}
	if cfg.RetryInitialSeconds <= 0 || cfg.RetryMaxSeconds <= 0 {
		return fmt.Errorf("retry intervals must be greater than 0")
	}
	if cfg.RetryMaxSeconds < cfg.RetryInitialSeconds {
		return fmt.Errorf("retry_max_seconds must be greater than or equal to retry_initial_seconds")
	}
	if cfg.VerifyIntervalSeconds <= 0 || cfg.VerifyAttempts <= 0 {
		return fmt.Errorf("verification interval and attempts must be greater than 0")
	}
	if cfg.CompletedCacheTTLMinutes < -1 {
		return fmt.Errorf("completed_cache_ttl_minutes must be -1 (disabled) or non-negative")
	}
	if cfg.CompletedRemoteProbeSeconds < 0 {
		return fmt.Errorf("completed_remote_probe_seconds must not be negative")
	}
	if cfg.ProviderSnapshotTTLSeconds < -1 {
		return fmt.Errorf("provider_snapshot_ttl_seconds must be -1 (disabled) or non-negative")
	}
	const bytesPerMiB = uint64(1024 * 1024)
	maxMB := ^uint64(0) / bytesPerMiB
	if cfg.ReserveFreeSpaceMB > maxMB || cfg.MaxPendingSpoolMB > maxMB || cfg.IncomingReservationChunkMB > maxMB {
		return fmt.Errorf("capacity value exceeds addressable byte range")
	}
	return nil
}

func RestartRequiredFields(before, after conf.WebDAVWritebackConfig) []string {
	fields := make([]string, 0, 5)
	if before.Enabled != after.Enabled {
		fields = append(fields, "enabled")
	}
	if before.Workers != after.Workers {
		fields = append(fields, "workers")
	}
	if before.UploadWorkers != after.UploadWorkers {
		fields = append(fields, "upload_workers")
	}
	if before.LargeUploadWorkers != after.LargeUploadWorkers {
		fields = append(fields, "large_upload_workers")
	}
	if before.ProviderProbeWorkers != after.ProviderProbeWorkers {
		fields = append(fields, "provider_probe_workers")
	}
	return fields
}

const (
	RecoveryTypeNone               = "none"
	RecoveryTypeAutomatic          = "automatic"
	RecoveryTypeRestartRecovery    = "restart_recovery"
	RecoveryTypeMissingSpool       = "missing_spool"
	RecoveryTypeCloudSyncRehydrate = "cloudsync_rehydrate"
	RecoveryTypeRemoteMismatch     = "remote_mismatch"
	RecoveryTypeManual             = "manual"

	TaskHealthHealthy        = "healthy"
	TaskHealthStalled        = "stalled"
	TaskHealthRetryExhausted = "retry_exhausted"
)

func RecoveryType(row *model.WebDAVWritebackObject) string {
	if row == nil {
		return RecoveryTypeNone
	}
	if row.CloudSyncReuploadRequired || row.State == StateWaitingCloudSyncReupload {
		return RecoveryTypeCloudSyncRehydrate
	}
	if row.ResolutionReason == ResolutionRemoteHashMismatch {
		return RecoveryTypeRemoteMismatch
	}
	if row.State == StateWaitingRepair {
		return RecoveryTypeManual
	}
	switch RecoveryLabel(row) {
	case "needs_cloudsync_rehydrate":
		return RecoveryTypeCloudSyncRehydrate
	case "missing_spool":
		return RecoveryTypeMissingSpool
	case "restart_recovery":
		return RecoveryTypeRestartRecovery
	case "waiting_provider_verification":
		return RecoveryTypeAutomatic
	}
	if row.RetryCount > 0 || row.VerifyCount > 0 || row.LastError != "" {
		return RecoveryTypeAutomatic
	}
	return RecoveryTypeNone
}

func TaskHealth(row *model.WebDAVWritebackObject) string {
	if row == nil {
		return TaskHealthHealthy
	}
	if row.State == StateWaitingRepair {
		return TaskHealthRetryExhausted
	}
	if row.State == StateWaitingCloudSyncReupload {
		return TaskHealthStalled
	}
	if row.State == StateVerifying && row.RetryAt == nil {
		return TaskHealthStalled
	}
	return TaskHealthHealthy
}

func RecoveryLabel(row *model.WebDAVWritebackObject) string {
	if row == nil {
		return ""
	}
	if row.CloudSyncReuploadRequired || row.State == StateWaitingCloudSyncReupload || row.ResolutionReason == ResolutionNeedsCloudSyncRehydrate {
		return "needs_cloudsync_rehydrate"
	}
	msg := strings.ToLower(row.LastError)
	// Legacy rows created before resolution_reason existed keep the exact old
	// diagnostic fallback so upgrades preserve operator visibility.
	if row.State == StateDeleted && canonicalDeleted(row) && strings.Contains(msg, "cloud sync can re-upload") {
		return "needs_cloudsync_rehydrate"
	}
	if strings.Contains(msg, "durable spool is missing") {
		return "missing_spool"
	}
	if row.State == StateVerifying &&
		(strings.Contains(msg, "interrupted upload") || strings.Contains(msg, "after restart")) {
		return "restart_recovery"
	}
	if row.State == StateVerifying {
		return "waiting_provider_verification"
	}
	return ""
}

func AdminRuntimeSnapshot() (AdminRuntimeStats, error) {
	now := time.Now()
	stats := AdminRuntimeStats{}

	var reservation struct {
		Bytes uint64 `gorm:"column:bytes"`
	}
	if err := db.GetDb().Model(&model.WebDAVWritebackReceiveReservation{}).
		Select("COALESCE(SUM(bytes), 0) AS bytes").
		Where("lease_until > ?", now).
		Scan(&reservation).Error; err != nil {
		return stats, err
	}
	stats.ReceivingReservationBytes = reservation.Bytes

	count := func(query string, args ...any) (int64, error) {
		var n int64
		err := db.GetDb().Model(&model.WebDAVWritebackObject{}).Where(query, args...).Count(&n).Error
		return n, err
	}
	var err error
	stats.MissingSpool, err = count(
		"is_dir = ? AND canonical_state = ? AND state IN ? AND LOWER(last_error) LIKE ?",
		false, CanonicalStateAcked, []string{StateQueued, StateVerifying}, "%durable spool is missing%",
	)
	if err != nil {
		return stats, err
	}
	stats.RestartRecovery, err = count(
		"state = ? AND (LOWER(last_error) LIKE ? OR LOWER(last_error) LIKE ?)",
		StateVerifying, "%interrupted upload%", "%after restart%",
	)
	if err != nil {
		return stats, err
	}
	stats.WaitingProviderVerification, err = count("state = ?", StateVerifying)
	if err != nil {
		return stats, err
	}
	stats.NeedsCloudSyncRehydrate, err = count(
		"cloud_sync_reupload_required = ? OR state = ? OR resolution_reason = ?",
		true, StateWaitingCloudSyncReupload, ResolutionNeedsCloudSyncRehydrate,
	)
	if err != nil {
		return stats, err
	}
	stats.AutomaticRecovery, err = count(
		"state IN ? AND (retry_count > 0 OR verify_count > 0 OR last_error <> '')",
		[]string{StateQueued, StateUploading, StateVerifying},
	)
	if err != nil {
		return stats, err
	}
	stats.RemoteHashMismatch, err = count(
		"resolution_reason = ? OR provider_evidence_result = ?",
		ResolutionRemoteHashMismatch, ResolutionRemoteHashMismatch,
	)
	if err != nil {
		return stats, err
	}

	if conf.Conf != nil && conf.Conf.WebDAVWriteback.SpoolDir != "" {
		usage, usageErr := disk.Usage(conf.Conf.WebDAVWriteback.SpoolDir)
		if usageErr != nil {
			stats.DiskError = usageErr.Error()
		} else {
			stats.DiskTotalBytes = usage.Total
			stats.DiskUsedBytes = usage.Used
			stats.DiskFreeBytes = usage.Free
		}
	}
	return stats, nil
}

func PreviewCompletedCacheNow() (CacheCleanupResult, error) {
	cutoff := time.Now()
	limit := completedCleanupBatchSize * completedCleanupMaxBatches * 4
	var rows []model.WebDAVWritebackObject
	query := db.GetDb().
		Select("id", "generation", "size", "spool_path", "completed_at", "remote_generation", "remote_verified_at", "state").
		Where("state = ? AND spool_path <> '' AND completed_at IS NOT NULL AND completed_at <= ?", StateCompleted, cutoff).
		Where("remote_verified_at IS NOT NULL AND remote_generation = generation")
	if err := completedSpoolOldestFirst(query).
		Limit(limit).
		Find(&rows).Error; err != nil {
		return CacheCleanupResult{}, err
	}

	result := CacheCleanupResult{Truncated: len(rows) == limit}
	for i := range rows {
		row := &rows[i]
		if !completedSpoolReleaseSafe(row, cutoff) {
			continue
		}
		result.Files++
		if row.Size > 0 {
			result.Bytes += uint64(row.Size)
		}
	}
	return result, nil
}

func VerifyNow(ids []uint) (int64, error) {
	if len(ids) == 0 {
		return 0, fmt.Errorf("ids must not be empty")
	}

	var rows []model.WebDAVWritebackObject
	if err := db.GetDb().
		Select("id", "parent").
		Where("id IN ? AND state = ? AND is_dir = ?", ids, StateVerifying, false).
		Find(&rows).Error; err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}

	rowIDs := make([]uint, 0, len(rows))
	parents := make(map[string]struct{}, len(rows))
	for i := range rows {
		rowIDs = append(rowIDs, rows[i].ID)
		if rows[i].Parent != "" {
			parents[rows[i].Parent] = struct{}{}
		}
	}

	for parent := range parents {
		providerProbeCooldowns.success(parent)
		providerParentSnapshots.invalidate(parent)
	}

	now := time.Now()
	res := db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("id IN ? AND state = ? AND is_dir = ?", rowIDs, StateVerifying, false).
		Update("retry_at", &now)
	if res.Error != nil {
		return 0, res.Error
	}
	wake()
	return res.RowsAffected, nil
}
