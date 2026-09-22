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

func WebDAVWritebackMonitorPage(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	c.Data(http.StatusOK, "text/html; charset=utf-8", []byte(webDAVWritebackMonitorHTML))
}

const webDAVWritebackMonitorHTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>WebDAV Writeback</title>
<style>
:root{font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#172033;background:#f6f8fb}
*{box-sizing:border-box}body{margin:0}.wrap{max-width:1550px;margin:0 auto;padding:24px}
.top,.toolbar,.actions{display:flex;gap:10px;align-items:center;flex-wrap:wrap}.top{margin-bottom:14px}h1{font-size:24px;margin:0;margin-right:auto}
button,select,input{border:1px solid #ccd4e0;border-radius:8px;background:#fff;padding:8px 10px;font:inherit}button{cursor:pointer}
.tabs{display:flex;gap:6px;margin:12px 0 16px}.tab{background:#fff}.tab.active{font-weight:700;border-color:#667085}
.panel{display:none}.panel.active{display:block}.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(165px,1fr));gap:10px;margin:12px 0}
.card,.box{background:#fff;border:1px solid #e1e6ef;border-radius:12px;padding:14px}.card .label{font-size:12px;color:#667085}.card .value{font-size:22px;font-weight:700;margin-top:3px}
.note,.small{font-size:12px;color:#667085}.status{font-size:13px}.status.error,.err{color:#b42318}.table-wrap{overflow:auto;background:#fff;border:1px solid #e1e6ef;border-radius:12px;margin-top:12px}
table{border-collapse:collapse;width:100%;min-width:1180px}th,td{padding:9px 10px;border-bottom:1px solid #eef1f5;text-align:left;vertical-align:top;font-size:13px}
th{position:sticky;top:0;background:#f8fafc;z-index:1;color:#475467}.path{max-width:500px;word-break:break-all}.muted{color:#98a2b3}
.badge{display:inline-block;border-radius:999px;padding:2px 8px;background:#eef2f6;font-size:12px;white-space:nowrap}.badge.receiving{background:#e0f2fe;color:#075985}.badge.queued{background:#fef3c7;color:#92400e}.badge.uploading{background:#dbeafe;color:#1d4ed8}.badge.verifying{background:#ede9fe;color:#6d28d9}.badge.completed,.badge.durable_acked{background:#dcfce7;color:#166534}.badge.deleted,.badge.remote_missing,.badge.recovery_required{background:#fee2e2;color:#991b1b}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(230px,1fr));gap:10px}.field{display:flex;flex-direction:column;gap:5px;font-size:12px;color:#475467}.field.check{flex-direction:row;align-items:center}
details{max-width:520px}summary{cursor:pointer}.danger{border-color:#fda29b}.section-title{font-weight:700;margin:4px 0 10px}
@media(max-width:700px){.wrap{padding:12px}h1{font-size:20px}}
</style>
</head>
<body>
<div class="wrap">
  <div class="top"><h1>WebDAV Writeback</h1><a id="back" href="#">Back to admin</a><button id="refresh">Refresh</button><label class="small"><input id="auto" type="checkbox" checked> auto 3s</label><span id="status" class="status"></span></div>
  <div class="tabs">
    <button class="tab active" data-tab="overview">Overview</button>
    <button class="tab" data-tab="active">Active</button>
    <button class="tab" data-tab="history">History</button>
    <button class="tab" data-tab="settings">Settings</button>
  </div>

  <section id="overview" class="panel active">
    <div class="cards">
      <div class="card"><div class="label">Writeback Enabled</div><div class="value" id="ovEnabled">-</div></div>
      <div class="card"><div class="label">Receiving</div><div class="value" id="ovReceiving">-</div></div>
      <div class="card"><div class="label">Pending / Backlog</div><div class="value" id="ovPending">-</div><div class="small" id="ovBacklogBytes"></div></div>
      <div class="card"><div class="label">Uploading</div><div class="value" id="ovUploading">-</div></div>
      <div class="card"><div class="label">Verifying</div><div class="value" id="ovVerifying">-</div></div>
      <div class="card"><div class="label">Errors</div><div class="value" id="ovErrors">-</div></div>
      <div class="card"><div class="label">Restart Recovery</div><div class="value" id="ovRestart">-</div></div>
      <div class="card"><div class="label">Missing Spool</div><div class="value" id="ovMissing">-</div></div>
      <div class="card"><div class="label">Needs CloudSync Rehydrate</div><div class="value" id="ovRehydrate">-</div></div>
      <div class="card"><div class="label">Completed Cache</div><div class="value" id="ovCache">-</div></div>
      <div class="card"><div class="label">Receiving Reservation</div><div class="value" id="ovReservation">-</div></div>
      <div class="card"><div class="label">Spool Disk Free</div><div class="value" id="ovDiskFree">-</div><div class="small" id="ovDiskUsage"></div></div>
      <div class="card"><div class="label">MaxPendingSpool</div><div class="value" id="ovMaxPending">-</div></div>
      <div class="card"><div class="label">ReserveFreeSpace</div><div class="value" id="ovReserve">-</div></div>
    </div>
    <div class="box">
      <div class="section-title">Completed cache maintenance</div>
      <div class="note">Release Completed Cache removes only safe, provider-verified completed .data spool files. It does not delete current canonical rows, History, active queue entries, recovery state, or provider objects.</div>
      <div class="actions"><button id="previewCache">Preview Completed Cache Cleanup</button><button id="releaseCache">Release Completed Cache</button><span id="cacheStatus" class="status"></span></div>
    </div>
  </section>

  <section id="active" class="panel">
    <div class="toolbar">
      <select id="activeState"><option value="active">Active</option><option value="receiving">Receiving</option><option value="queued">Queued</option><option value="uploading">Uploading</option><option value="verifying">Verifying</option><option value="error">Errors</option></select>
      <input id="activeSearch" placeholder="Filter path"><button id="activeRefresh">Refresh Active</button>
    </div>
    <div class="note">Only live work, retry/recovery and errors are shown here. Completed generations belong in History. No synthetic upload percentage is generated.</div>
    <div class="table-wrap"><table><thead><tr><th>Path</th><th>Size</th><th>State</th><th>Recovery</th><th>Retry</th><th>Verify</th><th>Started</th><th>Updated</th><th>Retry At</th><th>Error</th></tr></thead><tbody id="activeRows"></tbody></table></div>
  </section>

  <section id="history" class="panel">
    <div class="toolbar">
      <input id="historySearch" placeholder="Filter path">
      <select id="historyResult"><option value="">All results</option><option value="completed">Completed</option><option value="deleted">Deleted</option><option value="remote_missing">Remote Missing</option><option value="recovery_required">Recovery Required</option></select>
      <select id="historyRecovery"><option value="">All recovery</option><option value="restart_recovery">Restart recovery</option><option value="missing_spool_provider_recovered">Missing spool recovered</option><option value="cloudsync_rehydrate_required">CloudSync rehydrate required</option></select>
      <select id="historyError"><option value="">Any error</option><option value="true">Has Error</option><option value="false">No Error</option></select>
      <input id="historyAfter" type="datetime-local" title="After">
      <input id="historyBefore" type="datetime-local" title="Before">
      <select id="historyLimit"><option>100</option><option>200</option><option>500</option></select>
      <button id="historyRefresh">Refresh History</button>
    </div>
    <div class="cards">
      <div class="card"><div class="label">History rows</div><div class="value" id="histTotal">-</div></div>
      <div class="card"><div class="label">Completed</div><div class="value" id="histCompleted">-</div></div>
      <div class="card"><div class="label">Recovery evidence</div><div class="value" id="histRecoveryCount">-</div></div>
      <div class="card"><div class="label">Remote missing / rehydrate</div><div class="value" id="histRemoteMissing">-</div></div>
    </div>
    <div class="box">
      <div class="section-title">Delete History</div>
      <div class="note">History cleanup deletes only WebDAVWritebackHistory rows. It cannot change current generation/canonical state, delete spool files or provider data, trigger re-upload, or affect PROPFIND.</div>
      <div class="actions">
        <button id="deleteSelected" class="danger">Delete selected rows</button>
        <select id="cleanupClass"><option value="successful">Successful History</option><option value="recovery">Recovery History</option><option value="error">Error History</option><option value="remote_missing">Remote Missing</option></select>
        <input id="cleanupDays" type="number" min="0" value="30" title="Older than days; required for successful History">
        <button id="deleteClass" class="danger">Delete by class</button><span id="historyStatus" class="status"></span>
      </div>
    </div>
    <div class="table-wrap"><table><thead><tr><th></th><th>Path</th><th>Generation</th><th>Size</th><th>Result</th><th>Recovery</th><th>Retry</th><th>Verify</th><th>Duration</th><th>Completed</th><th>Remote Verified</th><th>Error</th></tr></thead><tbody id="historyRows"></tbody></table></div>
  </section>

  <section id="settings" class="panel">
    <div class="box">
      <div class="section-title">Writeback settings</div>
      <div class="note">SpoolDir remains read-only. Enabled/worker topology changes may require restart. CompletedRemoteProbeSeconds controls how long a released completed object may rely on fresh provider evidence before it is checked again; the reconcile algorithm itself is unchanged.</div>
      <div class="grid">
        <label class="field check"><input id="cfgEnabled" type="checkbox"> Enabled</label>
        <label class="field"><span>SpoolDir (read only)</span><input id="cfgSpoolDir" readonly></label>
        <label class="field"><span>ReserveFreeSpaceMB</span><input id="cfgReserve" type="number" min="0"></label>
        <label class="field"><span>MaxPendingSpoolMB (0 = unlimited)</span><input id="cfgMaxPending" type="number" min="0"></label>
        <label class="field"><span>IncomingReservationChunkMB</span><input id="cfgReservationChunk" type="number" min="1"></label>
        <label class="field"><span>Workers</span><input id="cfgWorkers" type="number" min="1" max="256"></label>
        <label class="field"><span>UploadWorkers</span><input id="cfgUploadWorkers" type="number" min="1"></label>
        <label class="field"><span>LargeUploadWorkers</span><input id="cfgLargeUploadWorkers" type="number" min="1"></label>
        <label class="field"><span>ProviderProbeWorkers</span><input id="cfgProviderProbeWorkers" type="number" min="1"></label>
        <label class="field"><span>CompletedCacheTTLMinutes (-1 = disabled)</span><input id="cfgCacheTTL" type="number" min="-1"></label>
        <label class="field"><span>CompletedRemoteProbeSeconds</span><input id="cfgRemoteProbe" type="number" min="0"></label>
        <label class="field"><span>CloudSyncSettleMillis</span><input id="cfgSettle" type="number" min="0"></label>
        <label class="field"><span>CloudSyncPlaceholderMillis</span><input id="cfgPlaceholder" type="number" min="0"></label>
        <label class="field"><span>RetryInitialSeconds</span><input id="cfgRetryInitial" type="number" min="1"></label>
        <label class="field"><span>RetryMaxSeconds</span><input id="cfgRetryMax" type="number" min="1"></label>
        <label class="field"><span>VerifyIntervalSeconds</span><input id="cfgVerifyInterval" type="number" min="1"></label>
        <label class="field"><span>VerifyAttempts</span><input id="cfgVerifyAttempts" type="number" min="1"></label>
      </div>
      <div class="actions"><button id="saveSettings">Save settings</button><span id="settingsStatus" class="status"></span></div>
    </div>
  </section>
</div>
<script>
(function(){
  var marker="/@manage/webdav-writeback",idx=location.pathname.lastIndexOf(marker),base=idx>=0?location.pathname.slice(0,idx):"",api=base+"/api/admin/webdav-writeback";
  document.getElementById("back").href=base+"/@manage";
  var currentTab="overview",status=document.getElementById("status");
  function token(){return localStorage.getItem("token")||""}
  function esc(v){return String(v==null?"":v).replace(/[&<>"']/g,function(c){return {"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[c]})}
  function bytes(n){n=Number(n||0);if(!n)return"0 B";var u=["B","KiB","MiB","GiB","TiB"],i=0;while(n>=1024&&i<u.length-1){n/=1024;i++}return(i?n.toFixed(2):n.toFixed(0))+" "+u[i]}
  function tm(v){return v?new Date(v).toLocaleString():"-"}
  function duration(a,b){if(!a||!b)return"-";var ms=new Date(b)-new Date(a);if(ms<0)return"-";var s=Math.round(ms/1000);if(s<60)return s+"s";if(s<3600)return Math.floor(s/60)+"m "+(s%60)+"s";return Math.floor(s/3600)+"h "+Math.floor((s%3600)/60)+"m"}
  function badge(v){return '<span class="badge '+esc(v||"")+'">'+esc(v||"-")+'</span>'}
  async function json(url,opt){opt=opt||{};var h=Object.assign({Authorization:token()},opt.headers||{}),r=await fetch(url,Object.assign({},opt,{headers:h})),j=await r.json().catch(function(){return{}});if(!r.ok||j.code!==200)throw new Error(j.message||("HTTP "+r.status));return j.data}
  function stateCount(s,n){return s.states&&s.states[n]?s.states[n].count:0}
  async function loadOverview(){
    var s=await json(api+"/summary");
    document.getElementById("ovEnabled").textContent=s.enabled?"Yes":"No";
    document.getElementById("ovReceiving").textContent=s.receiving||0;
    document.getElementById("ovPending").textContent=stateCount(s,"queued")+stateCount(s,"deleted");
    document.getElementById("ovBacklogBytes").textContent="durable backlog "+bytes(s.backlog_bytes);
    document.getElementById("ovUploading").textContent=stateCount(s,"uploading");
    document.getElementById("ovVerifying").textContent=stateCount(s,"verifying");
    document.getElementById("ovErrors").textContent=s.errors||0;
    document.getElementById("ovRestart").textContent=s.restart_recovery||0;
    document.getElementById("ovMissing").textContent=s.missing_spool||0;
    document.getElementById("ovRehydrate").textContent=s.needs_cloudsync_rehydrate||0;
    document.getElementById("ovCache").textContent=bytes(s.completed_cache_bytes);
    document.getElementById("ovReservation").textContent=bytes(s.receiving_reservation_bytes);
    document.getElementById("ovDiskFree").textContent=s.disk_error?"unavailable":bytes(s.disk_free_bytes);
    document.getElementById("ovDiskUsage").textContent=s.disk_error?s.disk_error:("used "+bytes(s.disk_used_bytes)+" / total "+bytes(s.disk_total_bytes));
    document.getElementById("ovMaxPending").textContent=s.max_pending_spool_bytes?bytes(s.max_pending_spool_bytes):"unlimited";
    document.getElementById("ovReserve").textContent=bytes(s.reserve_free_space_bytes);
  }
  async function loadActive(){
    var p=new URLSearchParams({state:document.getElementById("activeState").value,limit:"200"}),q=document.getElementById("activeSearch").value.trim();if(q)p.set("q",q);
    var list=await json(api+"/list?"+p.toString()),body=document.getElementById("activeRows");
    body.innerHTML=list.length?list.map(function(x){var detail='<details><summary>Advanced</summary><div class="small">Generation '+esc(x.generation)+' · Canonical '+esc(x.client_state)+' · ETag '+esc(x.etag||"-")+'<br>RemoteGeneration '+esc(x.remote_generation)+' · RemoteVerified '+esc(tm(x.remote_verified_at))+'<br>PayloadSHA1 '+esc(x.payload_sha1||"-")+'<br>RemoteSHA1 '+esc(x.remote_sha1||"-")+'<br>RemoteObjectID '+esc(x.remote_object_id||"-")+'</div></details>';return '<tr><td class="path"><strong>'+esc(x.path)+'</strong>'+detail+'</td><td>'+bytes(x.size)+'</td><td>'+badge(x.provider_state)+'</td><td>'+(x.recovery_state?badge(x.recovery_state):'<span class="muted">-</span>')+'</td><td>'+esc(x.retry_count)+'</td><td>'+esc(x.verify_count)+'</td><td>'+esc(tm(x.started_at))+'</td><td>'+esc(tm(x.updated_at))+'</td><td>'+esc(tm(x.retry_at))+'</td><td class="err">'+esc(x.last_error||"")+'</td></tr>'}).join(""):'<tr><td colspan="10" class="muted">No active writeback work.</td></tr>';
  }
  async function loadHistory(){
    var p=new URLSearchParams({limit:document.getElementById("historyLimit").value}),q=document.getElementById("historySearch").value.trim(),r=document.getElementById("historyResult").value,rec=document.getElementById("historyRecovery").value,hasErr=document.getElementById("historyError").value,after=document.getElementById("historyAfter").value,before=document.getElementById("historyBefore").value;if(q)p.set("q",q);if(r)p.set("result",r);if(rec)p.set("recovery",rec);if(hasErr)p.set("has_error",hasErr);if(after)p.set("after",new Date(after).toISOString());if(before)p.set("before",new Date(before).toISOString());
    var both=await Promise.all([json(api+"/history?"+p.toString()),json(api+"/history/summary")]),list=both[0],s=both[1],body=document.getElementById("historyRows");
    document.getElementById("histTotal").textContent=s.total||0;document.getElementById("histCompleted").textContent=(s.results&&s.results.completed)||0;
    var rc=0;if(s.recoveries)Object.keys(s.recoveries).forEach(function(k){rc+=s.recoveries[k]||0});document.getElementById("histRecoveryCount").textContent=rc;
    document.getElementById("histRemoteMissing").textContent=s.remote_missing_or_rehydrate||0;
    body.innerHTML=list.length?list.map(function(x){var detail='<details><summary>Advanced</summary><div class="small">Ack '+esc(tm(x.ack_time))+' · Durable '+esc(tm(x.durable_at))+' · Final event '+esc(tm(x.updated_at))+'<br>PayloadSHA1 '+esc(x.payload_sha1||"-")+'<br>RemoteSHA1 '+esc(x.remote_sha1||"-")+'<br>RemoteObjectID '+esc(x.remote_object_id||"-")+'</div></details>';return '<tr><td><input class="histSel" type="checkbox" value="'+esc(x.id)+'"></td><td class="path"><strong>'+esc(x.path)+'</strong>'+detail+'</td><td>'+esc(x.generation)+'</td><td>'+bytes(x.size)+'</td><td>'+badge(x.result)+'</td><td>'+(x.recovery_type?badge(x.recovery_type):'<span class="muted">-</span>')+'</td><td>'+esc(x.retry_count)+'</td><td>'+esc(x.verify_count)+'</td><td>'+esc(duration(x.started_at,x.completed_at))+'</td><td>'+esc(tm(x.completed_at))+'</td><td>'+esc(tm(x.remote_verified_at))+'</td><td class="err">'+esc(x.last_error||"")+'</td></tr>'}).join(""):'<tr><td colspan="12" class="muted">No matching History.</td></tr>';
  }
  function num(id){return Number(document.getElementById(id).value)}
  function applySettings(c){document.getElementById("cfgEnabled").checked=!!c.enabled;document.getElementById("cfgSpoolDir").value=c.spool_dir||"";document.getElementById("cfgReserve").value=c.reserve_free_space_mb;document.getElementById("cfgMaxPending").value=c.max_pending_spool_mb;document.getElementById("cfgReservationChunk").value=c.incoming_reservation_chunk_mb;document.getElementById("cfgWorkers").value=c.workers;document.getElementById("cfgUploadWorkers").value=c.upload_workers;document.getElementById("cfgLargeUploadWorkers").value=c.large_upload_workers;document.getElementById("cfgProviderProbeWorkers").value=c.provider_probe_workers;document.getElementById("cfgCacheTTL").value=c.completed_cache_ttl_minutes;document.getElementById("cfgRemoteProbe").value=c.completed_remote_probe_seconds;document.getElementById("cfgSettle").value=c.cloudsync_settle_millis;document.getElementById("cfgPlaceholder").value=c.cloudsync_placeholder_millis;document.getElementById("cfgRetryInitial").value=c.retry_initial_seconds;document.getElementById("cfgRetryMax").value=c.retry_max_seconds;document.getElementById("cfgVerifyInterval").value=c.verify_interval_seconds;document.getElementById("cfgVerifyAttempts").value=c.verify_attempts}
  async function loadSettings(){applySettings(await json(api+"/settings"))}
  async function refresh(){status.className="status";status.textContent="Refreshing…";try{await loadOverview();if(currentTab==="active")await loadActive();if(currentTab==="history")await loadHistory();if(currentTab==="settings")await loadSettings();status.textContent="Updated "+new Date().toLocaleTimeString()}catch(e){status.className="status error";status.textContent=e.message+(token()?"":" — log in to the OpenList admin UI first")}}
  document.querySelectorAll(".tab").forEach(function(b){b.addEventListener("click",function(){document.querySelectorAll(".tab,.panel").forEach(function(x){x.classList.remove("active")});b.classList.add("active");currentTab=b.dataset.tab;document.getElementById(currentTab).classList.add("active");refresh()})});
  document.getElementById("refresh").onclick=refresh;document.getElementById("activeRefresh").onclick=loadActive;document.getElementById("historyRefresh").onclick=loadHistory;
  document.getElementById("activeSearch").addEventListener("keydown",function(e){if(e.key==="Enter")loadActive()});document.getElementById("historySearch").addEventListener("keydown",function(e){if(e.key==="Enter")loadHistory()});
  document.getElementById("previewCache").onclick=async function(){var el=document.getElementById("cacheStatus");try{var x=await json(api+"/cleanup/preview");el.textContent="Eligible "+(x.eligible||0)+" files / "+bytes(x.eligible_bytes)+(x.truncated?" (preview capped)":"")}catch(e){el.className="status error";el.textContent=e.message}};
  document.getElementById("releaseCache").onclick=async function(){if(!confirm("Release only safe completed spool cache? Canonical state, History, active queue, recovery state and provider files are preserved."))return;var el=document.getElementById("cacheStatus");try{var x=await json(api+"/cleanup",{method:"POST"});el.textContent="Released "+(x.released||0)+" files / "+bytes(x.released_bytes);await loadOverview()}catch(e){el.className="status error";el.textContent=e.message}};
  document.getElementById("saveSettings").onclick=async function(){var el=document.getElementById("settingsStatus"),payload={enabled:document.getElementById("cfgEnabled").checked,reserve_free_space_mb:num("cfgReserve"),max_pending_spool_mb:num("cfgMaxPending"),incoming_reservation_chunk_mb:num("cfgReservationChunk"),workers:num("cfgWorkers"),upload_workers:num("cfgUploadWorkers"),large_upload_workers:num("cfgLargeUploadWorkers"),provider_probe_workers:num("cfgProviderProbeWorkers"),completed_cache_ttl_minutes:num("cfgCacheTTL"),completed_remote_probe_seconds:num("cfgRemoteProbe"),cloudsync_settle_millis:num("cfgSettle"),cloudsync_placeholder_millis:num("cfgPlaceholder"),retry_initial_seconds:num("cfgRetryInitial"),retry_max_seconds:num("cfgRetryMax"),verify_interval_seconds:num("cfgVerifyInterval"),verify_attempts:num("cfgVerifyAttempts")};try{var c=await json(api+"/settings",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(payload)});applySettings(c);var rr=c.restart_required_fields||[];el.textContent=rr.length?"Saved. Restart required for: "+rr.join(", "):"Saved and active."}catch(e){el.className="status error";el.textContent=e.message}};
  document.getElementById("deleteSelected").onclick=async function(){var ids=Array.from(document.querySelectorAll(".histSel:checked")).map(function(x){return Number(x.value)});if(!ids.length)return;if(!confirm("Delete "+ids.length+" selected History rows only?"))return;var el=document.getElementById("historyStatus");try{var x=await json(api+"/history/cleanup",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({ids:ids})});el.textContent="Deleted "+x.deleted+" History rows.";await loadHistory()}catch(e){el.className="status error";el.textContent=e.message}};
  document.getElementById("deleteClass").onclick=async function(){var cls=document.getElementById("cleanupClass").value,days=num("cleanupDays");if(cls==="successful"&&days<=0){alert("Successful History cleanup requires a positive age.");return}if(!confirm("Delete "+cls+" History"+(days>0?" older than "+days+" days":"")+"? Current canonical/spool/provider state is untouched."))return;var el=document.getElementById("historyStatus");try{var x=await json(api+"/history/cleanup",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({class:cls,older_than_days:days})});el.textContent="Deleted "+x.deleted+" History rows.";await loadHistory()}catch(e){el.className="status error";el.textContent=e.message}};
  setInterval(function(){if(document.getElementById("auto").checked&&(currentTab==="overview"||currentTab==="active"))refresh()},3000);
  refresh();
})();
</script>
</body>
</html>`
