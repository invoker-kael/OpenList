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
	webDAVMonitorDefaultLimit = 100
	webDAVMonitorMaxLimit     = 500
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
	ID               string     `json:"id"`
	Path             string     `json:"path"`
	Name             string     `json:"name"`
	IsDir            bool       `json:"is_dir"`
	Size             int64      `json:"size"`
	ClientState      string     `json:"client_state"`
	ProviderState    string     `json:"provider_state"`
	Generation       uint64     `json:"generation"`
	RemoteGeneration uint64     `json:"remote_generation"`
	ETag             string     `json:"etag"`
	PayloadSHA1      string     `json:"payload_sha1"`
	RemoteSHA1       string     `json:"remote_sha1"`
	RemoteObjectID   string     `json:"remote_object_id"`
	RetryCount       int        `json:"retry_count"`
	VerifyCount      int        `json:"verify_count"`
	ActiveReceivers  int        `json:"active_receivers,omitempty"`
	LastError        string     `json:"last_error"`
	RetryAt          *time.Time `json:"retry_at"`
	RecoveryState    string     `json:"recovery_state,omitempty"`
	RemoteVerifiedAt *time.Time `json:"remote_verified_at"`
	AckTime          *time.Time `json:"ack_time"`
	DurableAt        *time.Time `json:"durable_at"`
	CompletedAt      *time.Time `json:"completed_at"`
	StartedAt        *time.Time `json:"started_at,omitempty"`
	CreatedAt        time.Time  `json:"created_at"`
	UpdatedAt        time.Time  `json:"updated_at"`
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
	if row.State == "deleted" {
		return "deleted"
	}
	return "acked"
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
				ClientState:     "receiving",
				ProviderState:   "not_started",
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
			Select("id, path, name, is_dir, size, etag, canonical_state, state, generation, remote_generation, payload_sha1, remote_sha1, remote_object_id, retry_count, verify_count, last_error, retry_at, remote_verified_at, ack_time, durable_at, completed_at, created_at, updated_at")

		switch state {
		case "", "active":
			query = query.Where("state IN ?", []string{writeback.StateQueued, writeback.StateUploading, writeback.StateVerifying, writeback.StateDeleted})
		case "all":
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
			rows = append(rows, webDAVWritebackMonitorRow{
				ID:               strconv.FormatUint(uint64(row.ID), 10),
				Path:             row.Path,
				Name:             row.Name,
				IsDir:            row.IsDir,
				Size:             row.Size,
				ClientState:      webDAVMonitorCanonicalState(row),
				ProviderState:    row.State,
				Generation:       row.Generation,
				RemoteGeneration: row.RemoteGeneration,
				ETag:             row.ETag,
				PayloadSHA1:      row.PayloadSHA1,
				RemoteSHA1:       row.RemoteSHA1,
				RemoteObjectID:   row.RemoteObjectID,
				RetryCount:       row.RetryCount,
				VerifyCount:      row.VerifyCount,
				LastError:        row.LastError,
				RetryAt:          row.RetryAt,
				RecoveryState:    writeback.RecoveryLabel(row),
				RemoteVerifiedAt: row.RemoteVerifiedAt,
				AckTime:          row.AckTime,
				DurableAt:        row.DurableAt,
				CompletedAt:      row.CompletedAt,
				StartedAt:        row.AckTime,
				CreatedAt:        row.CreatedAt,
				UpdatedAt:        row.UpdatedAt,
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

func WebDAVWritebackHistoryList(c *gin.Context) {
	limit := webDAVMonitorLimit(c)
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
	var rows []model.WebDAVWritebackHistory
	if err := query.Order("updated_at DESC").Order("id DESC").Limit(limit).Find(&rows).Error; err != nil {
		common.ErrorResp(c, err, http.StatusInternalServerError)
		return
	}
	common.SuccessResp(c, rows)
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
