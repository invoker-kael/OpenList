package handles

import (
	"errors"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/writeback"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	"github.com/gin-gonic/gin"
)

const (
	webDAVMonitorDefaultLimit     = 100
	webDAVMonitorMaxLimit         = 500
	webDAVWritebackMonitorColumns = "id, path, name, is_dir, size, e_tag, canonical_state, state, generation, remote_generation, payload_sha1, remote_sha1, remote_object_id, retry_count, verify_count, last_error, resolution_reason, retry_at, remote_verified_at, receive_started_at, ack_time, durable_at, provider_upload_started_at, provider_upload_completed_at, provider_uploaded_bytes, recovery_started_at, cloud_sync_reupload_required, provider_evidence_first_at, provider_evidence_last_at, provider_evidence_count, provider_evidence_result, completed_at, created_at, updated_at"
)

type webDAVWritebackStateSummary struct {
	Count int64 `json:"count"`
	Bytes int64 `json:"bytes"`
}

type webDAVWritebackSummary struct {
	Enabled                     bool                                   `json:"enabled"`
	Workers                     int                                    `json:"workers"`
	Receiving                   int64                                  `json:"receiving"`
	ReceivingExpectedBytes      int64                                  `json:"receiving_expected_bytes"`
	ReceivingReservationBytes   uint64                                 `json:"receiving_reservation_bytes"`
	BacklogBytes                int64                                  `json:"backlog_bytes"`
	CompletedCacheBytes         int64                                  `json:"completed_cache_bytes"`
	MaxPendingSpoolBytes        uint64                                 `json:"max_pending_spool_bytes"`
	ReserveFreeSpaceBytes       uint64                                 `json:"reserve_free_space_bytes"`
	CompletedCacheTTLMinutes    int                                    `json:"completed_cache_ttl_minutes"`
	DiskTotalBytes              uint64                                 `json:"disk_total_bytes"`
	DiskUsedBytes               uint64                                 `json:"disk_used_bytes"`
	DiskFreeBytes               uint64                                 `json:"disk_free_bytes"`
	DiskError                   string                                 `json:"disk_error,omitempty"`
	MissingSpool                int64                                  `json:"missing_spool"`
	RestartRecovery             int64                                  `json:"restart_recovery"`
	WaitingProviderVerification int64                                  `json:"waiting_provider_verification"`
	NeedsCloudSyncRehydrate     int64                                  `json:"needs_cloudsync_rehydrate"`
	AutomaticRecovery           int64                                  `json:"automatic_recovery"`
	RemoteHashMismatch          int64                                  `json:"remote_hash_mismatch"`
	Errors                      int64                                  `json:"errors"`
	States                      map[string]webDAVWritebackStateSummary `json:"states"`
	UpdatedAt                   time.Time                              `json:"updated_at"`
}

type webDAVWritebackCleanupResult struct {
	Eligible      int    `json:"eligible"`
	EligibleBytes uint64 `json:"eligible_bytes"`
	Released      int    `json:"released"`
	ReleasedBytes uint64 `json:"released_bytes"`
	Truncated     bool   `json:"truncated,omitempty"`
}

type webDAVWritebackSettingsUpdate struct {
	Enabled                     bool   `json:"enabled"`
	ReserveFreeSpaceMB          uint64 `json:"reserve_free_space_mb"`
	MaxPendingSpoolMB           uint64 `json:"max_pending_spool_mb"`
	IncomingReservationChunkMB  uint64 `json:"incoming_reservation_chunk_mb"`
	Workers                     int    `json:"workers"`
	UploadWorkers               int    `json:"upload_workers"`
	LargeUploadWorkers          int    `json:"large_upload_workers"`
	ProviderProbeWorkers        int    `json:"provider_probe_workers"`
	CompletedCacheTTLMinutes    int    `json:"completed_cache_ttl_minutes"`
	CompletedRemoteProbeSeconds int    `json:"completed_remote_probe_seconds"`
	CloudSyncSettleMillis       int    `json:"cloudsync_settle_millis"`
	CloudSyncPlaceholderMillis  int    `json:"cloudsync_placeholder_millis"`
	RetryInitialSeconds         int    `json:"retry_initial_seconds"`
	RetryMaxSeconds             int    `json:"retry_max_seconds"`
	VerifyIntervalSeconds       int    `json:"verify_interval_seconds"`
	VerifyAttempts              int    `json:"verify_attempts"`
}

type webDAVWritebackSettings struct {
	webDAVWritebackSettingsUpdate
	SpoolDir              string   `json:"spool_dir"`
	RestartRequiredFields []string `json:"restart_required_fields,omitempty"`
}

type webDAVWritebackStateAggregate struct {
	State string
	Count int64
	Bytes int64
}

type webDAVWritebackMonitorRow struct {
	ID                        string     `json:"id"`
	Path                      string     `json:"path"`
	Name                      string     `json:"name"`
	IsDir                     bool       `json:"is_dir"`
	Size                      int64      `json:"size"`
	ReceivedBytes             int64      `json:"received_bytes,omitempty"`
	ClientState               string     `json:"client_state"`
	ProviderState             string     `json:"provider_state"`
	EffectiveStatus           string     `json:"effective_status"`
	OperatorAction            string     `json:"operator_action"`
	RecoveryType              string     `json:"recovery_type"`
	TaskHealth                string     `json:"task_health"`
	Generation                uint64     `json:"generation"`
	RemoteGeneration          uint64     `json:"remote_generation"`
	ETag                      string     `json:"etag"`
	PayloadSHA1               string     `json:"payload_sha1"`
	RemoteSHA1                string     `json:"remote_sha1"`
	RemoteObjectID            string     `json:"remote_object_id"`
	RetryCount                int        `json:"retry_count"`
	VerifyCount               int        `json:"verify_count"`
	ActiveReceivers           int        `json:"active_receivers,omitempty"`
	LastError                 string     `json:"last_error"`
	ResolutionReason          string     `json:"resolution_reason,omitempty"`
	RetryAt                   *time.Time `json:"retry_at"`
	RecoveryState             string     `json:"recovery_state,omitempty"`
	RemoteVerifiedAt          *time.Time `json:"remote_verified_at"`
	ReceiveStartedAt          *time.Time `json:"receive_started_at,omitempty"`
	AckTime                   *time.Time `json:"ack_time"`
	DurableAt                 *time.Time `json:"durable_at"`
	ProviderUploadStartedAt   *time.Time `json:"provider_upload_started_at,omitempty"`
	ProviderUploadCompletedAt *time.Time `json:"provider_upload_completed_at,omitempty"`
	ProviderUploadedBytes     int64      `json:"provider_uploaded_bytes,omitempty"`
	RecoveryStartedAt         *time.Time `json:"recovery_started_at,omitempty"`
	CloudSyncReuploadRequired bool       `json:"cloudsync_reupload_required"`
	ProviderEvidenceFirstAt   *time.Time `json:"provider_evidence_first_at,omitempty"`
	ProviderEvidenceLastAt    *time.Time `json:"provider_evidence_last_at,omitempty"`
	ProviderEvidenceCount     int        `json:"provider_evidence_count"`
	ProviderEvidenceResult    string     `json:"provider_evidence_result,omitempty"`
	CompletedAt               *time.Time `json:"completed_at"`
	StartedAt                 *time.Time `json:"started_at,omitempty"`
	CreatedAt                 time.Time  `json:"created_at"`
	UpdatedAt                 time.Time  `json:"updated_at"`
}

func WebDAVWritebackMonitorSummary(c *gin.Context) {
	now := time.Now()
	states := make(map[string]webDAVWritebackStateSummary)
	var aggregates []webDAVWritebackStateAggregate
	if err := db.GetDb().
		Model(&model.WebDAVWritebackObject{}).
		Select("state, COUNT(*) AS count, COALESCE(SUM(size), 0) AS bytes").
		Group("state").
		Scan(&aggregates).Error; err != nil {
		common.ErrorResp(c, err, http.StatusInternalServerError)
		return
	}
	for _, aggregate := range aggregates {
		states[aggregate.State] = webDAVWritebackStateSummary{
			Count: aggregate.Count,
			Bytes: aggregate.Bytes,
		}
	}

	var receiving int64
	if err := db.GetDb().
		Model(&model.WebDAVWritebackReceiveFence{}).
		Where("active_receivers > 0 AND receive_lease_until > ?", now).
		Count(&receiving).Error; err != nil {
		common.ErrorResp(c, err, http.StatusInternalServerError)
		return
	}

	var receivingExpectedBytes int64
	if err := db.GetDb().
		Model(&model.WebDAVWritebackReceiveFence{}).
		Where("active_receivers > 0 AND receive_lease_until > ?", now).
		Select("COALESCE(SUM(latest_expected_size), 0)").
		Scan(&receivingExpectedBytes).Error; err != nil {
		common.ErrorResp(c, err, http.StatusInternalServerError)
		return
	}

	var backlogBytes int64
	if err := db.GetDb().
		Model(&model.WebDAVWritebackObject{}).
		Where("state IN ? AND spool_path <> ''", []string{writeback.StateQueued, writeback.StateUploading, writeback.StateVerifying}).
		Select("COALESCE(SUM(size), 0)").
		Scan(&backlogBytes).Error; err != nil {
		common.ErrorResp(c, err, http.StatusInternalServerError)
		return
	}

	var completedCacheBytes int64
	if err := db.GetDb().
		Model(&model.WebDAVWritebackObject{}).
		Where("state = ? AND spool_path <> ''", writeback.StateCompleted).
		Where("remote_verified_at IS NOT NULL AND remote_generation = generation").
		Select("COALESCE(SUM(size), 0)").
		Scan(&completedCacheBytes).Error; err != nil {
		common.ErrorResp(c, err, http.StatusInternalServerError)
		return
	}

	var errorsCount int64
	if err := db.GetDb().
		Model(&model.WebDAVWritebackObject{}).
		Where("last_error <> ''").
		Where("state <> ?", writeback.StateWaitingCloudSyncReupload).
		Count(&errorsCount).Error; err != nil {
		common.ErrorResp(c, err, http.StatusInternalServerError)
		return
	}

	runtimeStats, err := writeback.AdminRuntimeSnapshot()
	if err != nil {
		common.ErrorResp(c, err, http.StatusInternalServerError)
		return
	}

	common.SuccessResp(c, webDAVWritebackSummary{
		Enabled:                     writeback.Enabled(),
		Workers:                     conf.Conf.WebDAVWriteback.Workers,
		Receiving:                   receiving,
		ReceivingExpectedBytes:      receivingExpectedBytes,
		ReceivingReservationBytes:   runtimeStats.ReceivingReservationBytes,
		BacklogBytes:                backlogBytes,
		CompletedCacheBytes:         completedCacheBytes,
		MaxPendingSpoolBytes:        conf.Conf.WebDAVWriteback.MaxPendingSpoolMB * uint64(1024*1024),
		ReserveFreeSpaceBytes:       conf.Conf.WebDAVWriteback.ReserveFreeSpaceMB * uint64(1024*1024),
		CompletedCacheTTLMinutes:    conf.Conf.WebDAVWriteback.CompletedCacheTTLMinutes,
		DiskTotalBytes:              runtimeStats.DiskTotalBytes,
		DiskUsedBytes:               runtimeStats.DiskUsedBytes,
		DiskFreeBytes:               runtimeStats.DiskFreeBytes,
		DiskError:                   runtimeStats.DiskError,
		MissingSpool:                runtimeStats.MissingSpool,
		RestartRecovery:             runtimeStats.RestartRecovery,
		WaitingProviderVerification: runtimeStats.WaitingProviderVerification,
		NeedsCloudSyncRehydrate:     runtimeStats.NeedsCloudSyncRehydrate,
		AutomaticRecovery:           runtimeStats.AutomaticRecovery,
		RemoteHashMismatch:          runtimeStats.RemoteHashMismatch,
		Errors:                      errorsCount,
		States:                      states,
		UpdatedAt:                   now,
	})
}

func WebDAVWritebackCleanupPreview(c *gin.Context) {
	preview, err := writeback.PreviewCompletedCacheNow()
	if err != nil {
		common.ErrorResp(c, err, http.StatusInternalServerError)
		return
	}
	common.SuccessResp(c, webDAVWritebackCleanupResult{
		Eligible:      preview.Files,
		EligibleBytes: preview.Bytes,
		Truncated:     preview.Truncated,
	})
}

func WebDAVWritebackCleanupCompletedCache(c *gin.Context) {
	preview, err := writeback.PreviewCompletedCacheNow()
	if err != nil {
		common.ErrorResp(c, err, http.StatusInternalServerError)
		return
	}
	released, err := writeback.CleanupCompletedCacheNowDetailed()
	if err != nil {
		common.ErrorResp(c, err, http.StatusInternalServerError)
		return
	}
	common.SuccessResp(c, webDAVWritebackCleanupResult{
		Eligible:      preview.Files,
		EligibleBytes: preview.Bytes,
		Released:      released.Files,
		ReleasedBytes: released.Bytes,
		Truncated:     preview.Truncated,
	})
}

func webDAVWritebackSettingsFromConfig(cfg conf.WebDAVWritebackConfig, restartFields []string) webDAVWritebackSettings {
	return webDAVWritebackSettings{
		webDAVWritebackSettingsUpdate: webDAVWritebackSettingsUpdate{
			Enabled:                     cfg.Enabled,
			ReserveFreeSpaceMB:          cfg.ReserveFreeSpaceMB,
			MaxPendingSpoolMB:           cfg.MaxPendingSpoolMB,
			IncomingReservationChunkMB:  cfg.IncomingReservationChunkMB,
			Workers:                     cfg.Workers,
			UploadWorkers:               cfg.UploadWorkers,
			LargeUploadWorkers:          cfg.LargeUploadWorkers,
			ProviderProbeWorkers:        cfg.ProviderProbeWorkers,
			CompletedCacheTTLMinutes:    cfg.CompletedCacheTTLMinutes,
			CompletedRemoteProbeSeconds: cfg.CompletedRemoteProbeSeconds,
			CloudSyncSettleMillis:       cfg.CloudSyncSettleMillis,
			CloudSyncPlaceholderMillis:  cfg.CloudSyncPlaceholderMillis,
			RetryInitialSeconds:         cfg.RetryInitialSeconds,
			RetryMaxSeconds:             cfg.RetryMaxSeconds,
			VerifyIntervalSeconds:       cfg.VerifyIntervalSeconds,
			VerifyAttempts:              cfg.VerifyAttempts,
		},
		SpoolDir:              cfg.SpoolDir,
		RestartRequiredFields: restartFields,
	}
}

func WebDAVWritebackSettings(c *gin.Context) {
	common.SuccessResp(c, webDAVWritebackSettingsFromConfig(conf.Conf.WebDAVWriteback, nil))
}

func WebDAVWritebackSaveSettings(c *gin.Context) {
	var req webDAVWritebackSettingsUpdate
	if err := c.ShouldBindJSON(&req); err != nil {
		common.ErrorResp(c, err, http.StatusBadRequest)
		return
	}

	before := conf.Conf.WebDAVWriteback
	next := before
	next.Enabled = req.Enabled
	next.ReserveFreeSpaceMB = req.ReserveFreeSpaceMB
	next.MaxPendingSpoolMB = req.MaxPendingSpoolMB
	next.IncomingReservationChunkMB = req.IncomingReservationChunkMB
	next.Workers = req.Workers
	next.UploadWorkers = req.UploadWorkers
	next.LargeUploadWorkers = req.LargeUploadWorkers
	next.ProviderProbeWorkers = req.ProviderProbeWorkers
	next.CompletedCacheTTLMinutes = req.CompletedCacheTTLMinutes
	next.CompletedRemoteProbeSeconds = req.CompletedRemoteProbeSeconds
	next.CloudSyncSettleMillis = req.CloudSyncSettleMillis
	next.CloudSyncPlaceholderMillis = req.CloudSyncPlaceholderMillis
	next.RetryInitialSeconds = req.RetryInitialSeconds
	next.RetryMaxSeconds = req.RetryMaxSeconds
	next.VerifyIntervalSeconds = req.VerifyIntervalSeconds
	next.VerifyAttempts = req.VerifyAttempts
	if err := writeback.ValidateAdminConfig(next); err != nil {
		common.ErrorResp(c, err, http.StatusBadRequest)
		return
	}

	persisted := *conf.Conf
	persisted.WebDAVWriteback = next
	if err := conf.SaveConfig(&persisted); err != nil {
		common.ErrorResp(c, err, http.StatusInternalServerError)
		return
	}
	conf.Conf.WebDAVWriteback = next
	restartFields := writeback.RestartRequiredFields(before, next)
	common.SuccessResp(c, webDAVWritebackSettingsFromConfig(next, restartFields))
}

func webDAVMonitorLimit(c *gin.Context) int {
	limit, err := strconv.Atoi(c.DefaultQuery("limit", strconv.Itoa(webDAVMonitorDefaultLimit)))
	if err != nil || limit <= 0 {
		return webDAVMonitorDefaultLimit
	}
	if limit > webDAVMonitorMaxLimit {
		return webDAVMonitorMaxLimit
	}
	return limit
}

func webDAVMonitorCanonicalState(row *model.WebDAVWritebackObject) string {
	if row.CanonicalState != "" {
		return row.CanonicalState
	}
	if row.State == writeback.StateDeleted {
		return "deleted"
	}
	return writeback.CanonicalStateAcked
}

const (
	webDAVStatusReceiving              = "receiving"
	webDAVStatusDurableAcked           = "durable_acked"
	webDAVStatusRemoteUploading        = "remote_uploading"
	webDAVStatusRemoteVerifying        = "remote_verifying"
	webDAVStatusCompleted              = "completed"
	webDAVStatusRemoteMissing          = "remote_missing"
	webDAVStatusRemoteHashMismatch     = "remote_hash_mismatch"
	webDAVStatusNeedsCloudSyncReupload = "needs_cloudsync_reupload"
	webDAVStatusWaitingRepair          = "waiting_repair"
	webDAVStatusWaitingCloudSync       = webDAVStatusNeedsCloudSyncReupload
	webDAVStatusReuploadReceiving      = "reupload_receiving"
	webDAVStatusReuploadReceived       = "reupload_received"
	webDAVStatusReuploadUploading      = "reupload_uploading"
	webDAVStatusReuploadVerifying      = "reupload_verifying"
	webDAVStatusRecovered              = "recovered"
	webDAVStatusAutomaticRecovery      = "automatic_recovery"

	webDAVActionNone                  = "none"
	webDAVActionWait                  = "wait"
	webDAVActionRetryingAutomatically = "retrying_automatically"
	webDAVActionRestartCloudSync      = "restart_cloudsync_required"
	webDAVActionManualCheck           = "manual_check_required"
)

func webDAVCurrentEffectiveStatus(row *model.WebDAVWritebackObject) (string, string) {
	if row == nil {
		return "", webDAVActionNone
	}
	if row.CloudSyncReuploadRequired || row.State == writeback.StateWaitingCloudSyncReupload {
		switch row.ResolutionReason {
		case writeback.ResolutionRemoteMissing:
			return webDAVStatusRemoteMissing, webDAVActionRestartCloudSync
		case writeback.ResolutionRemoteHashMismatch:
			return webDAVStatusRemoteHashMismatch, webDAVActionRestartCloudSync
		default:
			return webDAVStatusNeedsCloudSyncReupload, webDAVActionRestartCloudSync
		}
	}
	switch row.ResolutionReason {
	case writeback.ResolutionRemoteHashMismatch:
		return webDAVStatusRemoteHashMismatch, webDAVActionManualCheck
	case writeback.ResolutionRemoteMissing:
		return webDAVStatusRemoteMissing, webDAVActionManualCheck
	}
	if row.State == writeback.StateWaitingRepair {
		return webDAVStatusWaitingRepair, webDAVActionManualCheck
	}
	switch writeback.RecoveryLabel(row) {
	case "needs_cloudsync_rehydrate":
		return webDAVStatusNeedsCloudSyncReupload, webDAVActionRestartCloudSync
	case "missing_spool", "restart_recovery", "waiting_provider_verification":
		return webDAVStatusAutomaticRecovery, webDAVActionRetryingAutomatically
	}
	switch row.State {
	case writeback.StateQueued:
		if row.RetryCount > 0 || row.LastError != "" {
			return webDAVStatusDurableAcked, webDAVActionRetryingAutomatically
		}
		return webDAVStatusDurableAcked, webDAVActionWait
	case writeback.StateUploading:
		return webDAVStatusRemoteUploading, webDAVActionWait
	case writeback.StateVerifying:
		if row.VerifyCount > 0 || row.LastError != "" {
			return webDAVStatusRemoteVerifying, webDAVActionRetryingAutomatically
		}
		return webDAVStatusRemoteVerifying, webDAVActionWait
	case writeback.StateCompleted:
		return webDAVStatusCompleted, webDAVActionNone
	default:
		return row.State, webDAVActionNone
	}
}

func webDAVHistoryCurrentIsNewer(history *model.WebDAVWritebackHistory, current *model.WebDAVWritebackObject) bool {
	if history == nil || current == nil || history.PathKey == "" || current.PathKey != history.PathKey {
		return false
	}
	if current.Generation > history.Generation {
		return true
	}
	var ack *time.Time
	if current.AckTime != nil {
		ack = current.AckTime
	} else {
		ack = current.DurableAt
	}
	return ack != nil && ack.After(history.UpdatedAt)
}

func webDAVHistoryCurrentCorrelatesRehydrate(history *model.WebDAVWritebackHistory, current *model.WebDAVWritebackObject) bool {
	// A newer accepted generation at the same path supersedes the old recovery
	// incident even when the payload bytes changed. Cloud Sync encryption or a
	// legitimate local edit can produce a different size/hash on the fresh PUT;
	// generation/ACK ordering is the authoritative recovery signal.
	return webDAVHistoryCurrentIsNewer(history, current) && webDAVHistoryCurrentIsAcked(current)
}

func webDAVHistoryCurrentIsAcked(current *model.WebDAVWritebackObject) bool {
	if current == nil {
		return false
	}
	switch webDAVMonitorCanonicalState(current) {
	case writeback.CanonicalStateAcked, "acked":
		return true
	default:
		return false
	}
}

func webDAVHistoryReceiveIsNewer(history *model.WebDAVWritebackHistory, fence *model.WebDAVWritebackReceiveFence, now time.Time) bool {
	return history != nil &&
		fence != nil &&
		fence.PathKey == history.PathKey &&
		fence.ActiveReceivers > 0 &&
		fence.ReceiveLeaseUntil != nil &&
		fence.ReceiveLeaseUntil.After(now) &&
		fence.LatestStartedAt != nil &&
		fence.LatestStartedAt.After(history.UpdatedAt)
}

func webDAVHistoryEffectiveStatus(history *model.WebDAVWritebackHistory, current *model.WebDAVWritebackObject, fence *model.WebDAVWritebackReceiveFence, now time.Time) (string, string) {
	if history == nil {
		return "", ""
	}
	if history.RecoveryType == writeback.HistoryRecoveryCloudSyncRehydrateRequired {
		if webDAVHistoryCurrentCorrelatesRehydrate(history, current) {
			if writeback.RecoveryLabel(current) == "needs_cloudsync_rehydrate" {
				return webDAVCurrentEffectiveStatus(current)
			}
			if current.State == writeback.StateWaitingRepair {
				return webDAVCurrentEffectiveStatus(current)
			}
			switch current.State {
			case writeback.StateCompleted:
				if current.ResolutionReason == writeback.ResolutionRemoteHashMismatch {
					return webDAVStatusRemoteHashMismatch, webDAVActionManualCheck
				}
				return webDAVStatusRecovered, ""
			case writeback.StateUploading:
				return webDAVStatusReuploadUploading, ""
			case writeback.StateVerifying:
				return webDAVStatusReuploadVerifying, ""
			default:
				return webDAVStatusReuploadReceived, ""
			}
		}
		if webDAVHistoryReceiveIsNewer(history, fence, now) {
			return webDAVStatusReuploadReceiving, ""
		}
		return webDAVStatusWaitingCloudSync, webDAVActionRestartCloudSync
	}
	if history.ResolutionReason == writeback.ResolutionRemoteHashMismatch ||
		history.FinalState == writeback.ResolutionRemoteHashMismatch {
		return webDAVStatusRemoteHashMismatch, webDAVActionManualCheck
	}
	if history.RecoveryType != "" {
		if history.Result == writeback.HistoryResultCompleted {
			return webDAVStatusRecovered, ""
		}
		return webDAVStatusAutomaticRecovery, ""
	}
	return history.Result, ""
}

func WebDAVWritebackMonitorList(c *gin.Context) {
	now := time.Now()
	limit := webDAVMonitorLimit(c)
	state := strings.ToLower(strings.TrimSpace(c.DefaultQuery("state", "active")))
	search := strings.TrimSpace(c.Query("q"))

	rows := make([]webDAVWritebackMonitorRow, 0, limit)
	includeReceiving := state == "active" || state == "all" || state == "receiving"
	includeObjects := state != "receiving"

	if includeReceiving {
		var fences []model.WebDAVWritebackReceiveFence
		query := db.GetDb().
			Model(&model.WebDAVWritebackReceiveFence{}).
			Where("active_receivers > 0 AND receive_lease_until > ?", now)
		if search != "" {
			query = query.Where("path LIKE ?", "%"+search+"%")
		}
		if err := query.Order("updated_at DESC").Limit(limit).Find(&fences).Error; err != nil {
			common.ErrorResp(c, err, http.StatusInternalServerError)
			return
		}
		for _, fence := range fences {
			rows = append(rows, webDAVWritebackMonitorRow{
				ID:              "receive:" + fence.PathKey,
				Path:            fence.Path,
				Name:            fence.Path,
				Size:            fence.LatestExpectedSize,
				ReceivedBytes:   fence.LatestReceivedSize,
				ClientState:     "receiving",
				ProviderState:   "not_started",
				EffectiveStatus: webDAVStatusReceiving,
				OperatorAction:  webDAVActionWait,
				RecoveryType:    writeback.RecoveryType(nil),
				TaskHealth:      writeback.TaskHealth(nil),
				ActiveReceivers: fence.ActiveReceivers,
				StartedAt:       fence.LatestStartedAt,
				CreatedAt:       fence.CreatedAt,
				UpdatedAt:       fence.UpdatedAt,
			})
		}
	}

	if includeObjects {
		var objects []model.WebDAVWritebackObject
		query := db.GetDb().
			Model(&model.WebDAVWritebackObject{}).
			Select(webDAVWritebackMonitorColumns)

		switch state {
		case "", "active":
			query = query.Where("state IN ?", []string{
				writeback.StateQueued,
				writeback.StateUploading,
				writeback.StateVerifying,
				writeback.StateDeleted,
				writeback.StateWaitingCloudSyncReupload,
				writeback.StateWaitingRepair,
			})
		case "all":
		case "syncing":
			query = query.Where("state IN ?", []string{
				writeback.StateQueued,
				writeback.StateUploading,
				writeback.StateVerifying,
			})
		case "waiting_reupload":
			query = query.Where(
				"state = ? OR cloud_sync_reupload_required = ? OR resolution_reason = ? OR (state = ? AND canonical_state = ? AND LOWER(last_error) LIKE ?)",
				writeback.StateWaitingCloudSyncReupload,
				true,
				writeback.ResolutionNeedsCloudSyncRehydrate,
				writeback.StateDeleted,
				writeback.CanonicalStateDeleted,
				"%cloud sync can re-upload%",
			)
		case "error":
			query = query.Where("last_error <> ''")
		default:
			query = query.Where("state = ?", state)
		}
		if search != "" {
			query = query.Where("path LIKE ?", "%"+search+"%")
		}
		if err := query.Order("updated_at DESC").Limit(limit).Find(&objects).Error; err != nil {
			common.ErrorResp(c, err, http.StatusInternalServerError)
			return
		}
		for i := range objects {
			row := &objects[i]
			effectiveStatus, operatorAction := webDAVCurrentEffectiveStatus(row)
			rows = append(rows, webDAVWritebackMonitorRow{
				ID:                        strconv.FormatUint(uint64(row.ID), 10),
				Path:                      row.Path,
				Name:                      row.Name,
				IsDir:                     row.IsDir,
				Size:                      row.Size,
				ClientState:               webDAVMonitorCanonicalState(row),
				ProviderState:             row.State,
				EffectiveStatus:           effectiveStatus,
				OperatorAction:            operatorAction,
				RecoveryType:              writeback.RecoveryType(row),
				TaskHealth:                writeback.TaskHealth(row),
				Generation:                row.Generation,
				RemoteGeneration:          row.RemoteGeneration,
				ETag:                      row.ETag,
				PayloadSHA1:               row.PayloadSHA1,
				RemoteSHA1:                row.RemoteSHA1,
				RemoteObjectID:            row.RemoteObjectID,
				RetryCount:                row.RetryCount,
				VerifyCount:               row.VerifyCount,
				LastError:                 row.LastError,
				ResolutionReason:          row.ResolutionReason,
				RetryAt:                   row.RetryAt,
				RecoveryState:             writeback.RecoveryLabel(row),
				RemoteVerifiedAt:          row.RemoteVerifiedAt,
				ReceiveStartedAt:          row.ReceiveStartedAt,
				AckTime:                   row.AckTime,
				DurableAt:                 row.DurableAt,
				ProviderUploadStartedAt:   row.ProviderUploadStartedAt,
				ProviderUploadCompletedAt: row.ProviderUploadCompletedAt,
				ProviderUploadedBytes:     row.ProviderUploadedBytes,
				RecoveryStartedAt:         row.RecoveryStartedAt,
				CloudSyncReuploadRequired: row.CloudSyncReuploadRequired,
				ProviderEvidenceFirstAt:   row.ProviderEvidenceFirstAt,
				ProviderEvidenceLastAt:    row.ProviderEvidenceLastAt,
				ProviderEvidenceCount:     row.ProviderEvidenceCount,
				ProviderEvidenceResult:    row.ProviderEvidenceResult,
				CompletedAt:               row.CompletedAt,
				StartedAt:                 row.ReceiveStartedAt,
				CreatedAt:                 row.CreatedAt,
				UpdatedAt:                 row.UpdatedAt,
			})
		}
	}

	sort.SliceStable(rows, func(i, j int) bool {
		return rows[i].UpdatedAt.After(rows[j].UpdatedAt)
	})
	if len(rows) > limit {
		rows = rows[:limit]
	}
	common.SuccessResp(c, rows)
}

type webDAVWritebackHistoryAggregate struct {
	Value string
	Count int64
}

type webDAVWritebackTimelineEvent struct {
	Event string    `json:"event"`
	At    time.Time `json:"at"`
}

type webDAVWritebackHistoryRow struct {
	model.WebDAVWritebackHistory
	EffectiveStatus           string                         `json:"effective_status"`
	OperatorAction            string                         `json:"operator_action,omitempty"`
	CurrentGeneration         uint64                         `json:"current_generation,omitempty"`
	CurrentProviderState      string                         `json:"current_provider_state,omitempty"`
	CurrentRecoveryState      string                         `json:"current_recovery_state,omitempty"`
	CurrentAckTime            *time.Time                     `json:"current_ack_time,omitempty"`
	CurrentCompletedAt        *time.Time                     `json:"current_completed_at,omitempty"`
	LifecycleDurationMS       int64                          `json:"lifecycle_duration_ms"`
	CloudSyncUploadDurationMS int64                          `json:"cloudsync_upload_duration_ms"`
	ProviderSyncDurationMS    int64                          `json:"provider_sync_duration_ms"`
	RecoveryDurationMS        int64                          `json:"recovery_duration_ms"`
	EventTimeline             []webDAVWritebackTimelineEvent `json:"event_timeline,omitempty"`
}

func webDAVDurationMillis(start, end *time.Time) int64 {
	if start == nil || end == nil || end.Before(*start) {
		return 0
	}
	return end.Sub(*start).Milliseconds()
}

func webDAVHistoryTimeline(row *model.WebDAVWritebackHistory) []webDAVWritebackTimelineEvent {
	if row == nil {
		return nil
	}
	events := make([]webDAVWritebackTimelineEvent, 0, 6)
	appendEvent := func(event string, at *time.Time) {
		if at != nil {
			events = append(events, webDAVWritebackTimelineEvent{Event: event, At: *at})
		}
	}
	appendEvent("PUT_START", row.StartedAt)
	appendEvent("DURABLE_ACK", row.DurableAt)
	appendEvent("PROVIDER_UPLOAD_START", row.ProviderUploadStartedAt)
	appendEvent("PROVIDER_UPLOAD_COMPLETE", row.ProviderUploadCompletedAt)
	appendEvent("REMOTE_VERIFY", row.RemoteVerifiedAt)
	if row.CompletedAt != nil {
		event := "COMPLETED"
		if row.Result != writeback.HistoryResultCompleted && row.Result != writeback.HistoryResultDeleted {
			event = "RECOVERY_REQUIRED"
		}
		appendEvent(event, row.CompletedAt)
	}
	return events
}

func webDAVHistoryDurations(item *webDAVWritebackHistoryRow) {
	if item == nil {
		return
	}
	row := &item.WebDAVWritebackHistory
	item.CloudSyncUploadDurationMS = webDAVDurationMillis(row.StartedAt, row.DurableAt)
	if row.Result == writeback.HistoryResultCompleted {
		item.LifecycleDurationMS = webDAVDurationMillis(row.StartedAt, row.CompletedAt)
		item.ProviderSyncDurationMS = webDAVDurationMillis(row.DurableAt, row.CompletedAt)
		if row.RecoveryType != "" {
			item.RecoveryDurationMS = webDAVDurationMillis(row.RecoveryStartedAt, row.CompletedAt)
		}
	}
	item.EventTimeline = webDAVHistoryTimeline(row)
}

type webDAVWritebackHistorySummary struct {
	Total                    int64            `json:"total"`
	RemoteMissingOrRehydrate int64            `json:"remote_missing_or_rehydrate"`
	Results                  map[string]int64 `json:"results"`
	Recoveries               map[string]int64 `json:"recoveries"`
}

type webDAVWritebackHistoryCleanupRequest struct {
	IDs           []uint `json:"ids"`
	Class         string `json:"class"`
	OlderThanDays int    `json:"older_than_days"`
}

type webDAVWritebackHistoryCleanupResult struct {
	Deleted int64 `json:"deleted"`
}

func webDAVHistoryStatusInGroup(status, action, group string) bool {
	switch group {
	case "":
		return true
	case "receiving":
		return status == webDAVStatusReceiving || status == webDAVStatusReuploadReceiving
	case "syncing":
		switch status {
		case webDAVStatusDurableAcked,
			webDAVStatusRemoteUploading,
			webDAVStatusRemoteVerifying,
			webDAVStatusAutomaticRecovery,
			webDAVStatusReuploadReceived,
			webDAVStatusReuploadUploading,
			webDAVStatusReuploadVerifying:
			return true
		default:
			return false
		}
	case "completed":
		return status == webDAVStatusCompleted || status == webDAVStatusRecovered
	case "waiting_reupload":
		return status != webDAVStatusCompleted &&
			status != webDAVStatusRecovered &&
			status != "deleted"
	case "processing":
		return status != webDAVStatusCompleted &&
			status != webDAVStatusRecovered &&
			status != "deleted" &&
			action != webDAVActionRestartCloudSync
	default:
		return false
	}
}

func WebDAVWritebackHistoryList(c *gin.Context) {
	limit := webDAVMonitorLimit(c)
	statusFilter := strings.ToLower(strings.TrimSpace(c.Query("status")))
	statusGroupFilter := strings.ToLower(strings.TrimSpace(c.Query("status_group")))
	switch statusGroupFilter {
	case "", "receiving", "syncing", "completed", "waiting_reupload", "processing":
	default:
		common.ErrorResp(c, errors.New("unsupported status_group"), http.StatusBadRequest)
		return
	}
	currentStateFilter := strings.ToLower(strings.TrimSpace(c.Query("current_state")))
	actionRequiredRaw := strings.TrimSpace(c.Query("action_required"))
	var actionRequiredFilter *bool
	if actionRequiredRaw != "" {
		value, err := strconv.ParseBool(actionRequiredRaw)
		if err != nil {
			common.ErrorResp(c, err, http.StatusBadRequest)
			return
		}
		actionRequiredFilter = &value
	}

	query := db.GetDb().Model(&model.WebDAVWritebackHistory{})
	if search := strings.TrimSpace(c.Query("q")); search != "" {
		query = query.Where("path LIKE ?", "%"+search+"%")
	}
	if result := strings.ToLower(strings.TrimSpace(c.Query("result"))); result != "" {
		query = query.Where("result = ?", result)
	}
	if recovery := strings.ToLower(strings.TrimSpace(c.Query("recovery"))); recovery != "" {
		query = query.Where("recovery_type = ?", recovery)
	}
	if trigger := strings.ToUpper(strings.TrimSpace(c.Query("trigger"))); trigger != "" {
		query = query.Where("trigger_type = ?", trigger)
	}
	if raw := strings.TrimSpace(c.Query("generation")); raw != "" {
		generation, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			common.ErrorResp(c, err, http.StatusBadRequest)
			return
		}
		query = query.Where("generation = ?", generation)
	}
	if raw := strings.TrimSpace(c.Query("has_error")); raw != "" {
		hasError, err := strconv.ParseBool(raw)
		if err != nil {
			common.ErrorResp(c, err, http.StatusBadRequest)
			return
		}
		if hasError {
			query = query.Where("last_error <> ''")
		} else {
			query = query.Where("last_error = '' OR last_error IS NULL")
		}
	}
	if raw := strings.TrimSpace(c.Query("before")); raw != "" {
		before, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			common.ErrorResp(c, err, http.StatusBadRequest)
			return
		}
		query = query.Where("updated_at < ?", before)
	}
	if raw := strings.TrimSpace(c.Query("after")); raw != "" {
		after, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			common.ErrorResp(c, err, http.StatusBadRequest)
			return
		}
		query = query.Where("updated_at >= ?", after)
	}
	candidateLimit := limit
	if statusFilter != "" || statusGroupFilter != "" || currentStateFilter != "" || actionRequiredFilter != nil {
		candidateLimit = webDAVMonitorMaxLimit
	}
	var rows []model.WebDAVWritebackHistory
	if err := query.Order("updated_at DESC").Order("id DESC").Limit(candidateLimit).Find(&rows).Error; err != nil {
		common.ErrorResp(c, err, http.StatusInternalServerError)
		return
	}

	pathKeys := make([]string, 0, len(rows))
	seenPathKeys := make(map[string]struct{}, len(rows))
	for i := range rows {
		if rows[i].PathKey == "" {
			continue
		}
		if _, exists := seenPathKeys[rows[i].PathKey]; exists {
			continue
		}
		seenPathKeys[rows[i].PathKey] = struct{}{}
		pathKeys = append(pathKeys, rows[i].PathKey)
	}

	currentByPath := make(map[string]*model.WebDAVWritebackObject, len(pathKeys))
	fenceByPath := make(map[string]*model.WebDAVWritebackReceiveFence, len(pathKeys))
	if len(pathKeys) > 0 {
		var current []model.WebDAVWritebackObject
		if err := db.GetDb().
			Select("path_key", "generation", "size", "payload_sha1", "canonical_state", "state", "ack_time", "durable_at", "completed_at", "last_error", "resolution_reason", "cloud_sync_reupload_required").
			Where("path_key IN ?", pathKeys).
			Find(&current).Error; err != nil {
			common.ErrorResp(c, err, http.StatusInternalServerError)
			return
		}
		for i := range current {
			currentByPath[current[i].PathKey] = &current[i]
		}

		var fences []model.WebDAVWritebackReceiveFence
		if err := db.GetDb().
			Select("path_key", "active_receivers", "latest_expected_size", "latest_started_at", "receive_lease_until").
			Where("path_key IN ?", pathKeys).
			Find(&fences).Error; err != nil {
			common.ErrorResp(c, err, http.StatusInternalServerError)
			return
		}
		for i := range fences {
			fenceByPath[fences[i].PathKey] = &fences[i]
		}
	}

	now := time.Now()
	view := make([]webDAVWritebackHistoryRow, 0, min(limit, len(rows)))
	for i := range rows {
		row := &rows[i]
		current := currentByPath[row.PathKey]
		fence := fenceByPath[row.PathKey]
		status, action := webDAVHistoryEffectiveStatus(row, current, fence, now)

		if statusFilter != "" {
			if statusFilter == webDAVStatusCompleted {
				if status != webDAVStatusCompleted && status != webDAVStatusRecovered {
					continue
				}
			} else if status != statusFilter {
				continue
			}
		}
		if currentStateFilter != "" && (current == nil || current.State != currentStateFilter) {
			continue
		}
		actionRequired := action == webDAVActionRestartCloudSync || action == webDAVActionManualCheck
		if actionRequiredFilter != nil && actionRequired != *actionRequiredFilter {
			continue
		}
		if !webDAVHistoryStatusInGroup(status, action, statusGroupFilter) {
			continue
		}

		item := webDAVWritebackHistoryRow{
			WebDAVWritebackHistory: *row,
			EffectiveStatus:        status,
			OperatorAction:         action,
		}
		webDAVHistoryDurations(&item)
		if current != nil {
			item.CurrentGeneration = current.Generation
			item.CurrentProviderState = current.State
			item.CurrentRecoveryState = writeback.RecoveryLabel(current)
			item.CurrentAckTime = current.AckTime
			item.CurrentCompletedAt = current.CompletedAt
		}
		view = append(view, item)
		if len(view) >= limit {
			break
		}
	}
	common.SuccessResp(c, view)
}

func WebDAVWritebackHistorySummary(c *gin.Context) {
	summary := webDAVWritebackHistorySummary{
		Results:    map[string]int64{},
		Recoveries: map[string]int64{},
	}
	if err := db.GetDb().Model(&model.WebDAVWritebackHistory{}).Count(&summary.Total).Error; err != nil {
		common.ErrorResp(c, err, http.StatusInternalServerError)
		return
	}
	var results []webDAVWritebackHistoryAggregate
	if err := db.GetDb().Model(&model.WebDAVWritebackHistory{}).
		Select("result AS value, COUNT(*) AS count").
		Group("result").
		Scan(&results).Error; err != nil {
		common.ErrorResp(c, err, http.StatusInternalServerError)
		return
	}
	for _, item := range results {
		summary.Results[item.Value] = item.Count
	}
	var recoveries []webDAVWritebackHistoryAggregate
	if err := db.GetDb().Model(&model.WebDAVWritebackHistory{}).
		Where("recovery_type <> ''").
		Select("recovery_type AS value, COUNT(*) AS count").
		Group("recovery_type").
		Scan(&recoveries).Error; err != nil {
		common.ErrorResp(c, err, http.StatusInternalServerError)
		return
	}
	for _, item := range recoveries {
		summary.Recoveries[item.Value] = item.Count
	}
	if err := db.GetDb().Model(&model.WebDAVWritebackHistory{}).
		Where("result = ? OR recovery_type = ?", writeback.HistoryResultRemoteMissing, writeback.HistoryRecoveryCloudSyncRehydrateRequired).
		Count(&summary.RemoteMissingOrRehydrate).Error; err != nil {
		common.ErrorResp(c, err, http.StatusInternalServerError)
		return
	}
	common.SuccessResp(c, summary)
}

func WebDAVWritebackHistoryCleanup(c *gin.Context) {
	var req webDAVWritebackHistoryCleanupRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		common.ErrorResp(c, err, http.StatusBadRequest)
		return
	}
	class := strings.ToLower(strings.TrimSpace(req.Class))
	if len(req.IDs) == 0 {
		switch class {
		case "successful":
			if req.OlderThanDays <= 0 {
				common.ErrorResp(c, errors.New("successful history cleanup requires older_than_days > 0"), http.StatusBadRequest)
				return
			}
		case "recovery", "error", "remote_missing":
		default:
			common.ErrorResp(c, errors.New("select history rows or choose a supported cleanup class"), http.StatusBadRequest)
			return
		}
	}
	deleted, err := writeback.CleanupHistory(db.GetDb(), writeback.HistoryCleanupSpec{
		IDs:           req.IDs,
		Class:         class,
		OlderThanDays: req.OlderThanDays,
	})
	if err != nil {
		common.ErrorResp(c, err, http.StatusInternalServerError)
		return
	}
	common.SuccessResp(c, webDAVWritebackHistoryCleanupResult{Deleted: deleted})
}
