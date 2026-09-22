package handles

import (
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
	Enabled                    bool   `json:"enabled"`
	ReserveFreeSpaceMB         uint64 `json:"reserve_free_space_mb"`
	MaxPendingSpoolMB          uint64 `json:"max_pending_spool_mb"`
	IncomingReservationChunkMB uint64 `json:"incoming_reservation_chunk_mb"`
	Workers                    int    `json:"workers"`
	UploadWorkers              int    `json:"upload_workers"`
	LargeUploadWorkers         int    `json:"large_upload_workers"`
	ProviderProbeWorkers       int    `json:"provider_probe_workers"`
	CompletedCacheTTLMinutes   int    `json:"completed_cache_ttl_minutes"`
	CloudSyncSettleMillis      int    `json:"cloudsync_settle_millis"`
	CloudSyncPlaceholderMillis int    `json:"cloudsync_placeholder_millis"`
	RetryInitialSeconds        int    `json:"retry_initial_seconds"`
	RetryMaxSeconds            int    `json:"retry_max_seconds"`
	VerifyIntervalSeconds      int    `json:"verify_interval_seconds"`
	VerifyAttempts             int    `json:"verify_attempts"`
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
	PayloadSHA1      string     `json:"payload_sha1"`
	RemoteSHA1       string     `json:"remote_sha1"`
	RetryCount       int        `json:"retry_count"`
	VerifyCount      int        `json:"verify_count"`
	ActiveReceivers  int        `json:"active_receivers,omitempty"`
	LastError        string     `json:"last_error"`
	RetryAt          *time.Time `json:"retry_at"`
	RecoveryState    string     `json:"recovery_state,omitempty"`
	RemoteVerifiedAt *time.Time `json:"remote_verified_at"`
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
			Enabled:                    cfg.Enabled,
			ReserveFreeSpaceMB:         cfg.ReserveFreeSpaceMB,
			MaxPendingSpoolMB:          cfg.MaxPendingSpoolMB,
			IncomingReservationChunkMB: cfg.IncomingReservationChunkMB,
			Workers:                    cfg.Workers,
			UploadWorkers:              cfg.UploadWorkers,
			LargeUploadWorkers:         cfg.LargeUploadWorkers,
			ProviderProbeWorkers:       cfg.ProviderProbeWorkers,
			CompletedCacheTTLMinutes:   cfg.CompletedCacheTTLMinutes,
			CloudSyncSettleMillis:      cfg.CloudSyncSettleMillis,
			CloudSyncPlaceholderMillis: cfg.CloudSyncPlaceholderMillis,
			RetryInitialSeconds:        cfg.RetryInitialSeconds,
			RetryMaxSeconds:            cfg.RetryMaxSeconds,
			VerifyIntervalSeconds:      cfg.VerifyIntervalSeconds,
			VerifyAttempts:             cfg.VerifyAttempts,
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
			Select("id, path, name, is_dir, size, canonical_state, state, generation, remote_generation, payload_sha1, remote_sha1, retry_count, verify_count, last_error, retry_at, remote_verified_at, completed_at, created_at, updated_at")

		switch state {
		case "", "active":
			query = query.Where("state IN ?", []string{writeback.StateQueued, writeback.StateUploading, writeback.StateVerifying})
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
				PayloadSHA1:      row.PayloadSHA1,
				RemoteSHA1:       row.RemoteSHA1,
				RetryCount:       row.RetryCount,
				VerifyCount:      row.VerifyCount,
				LastError:        row.LastError,
				RetryAt:          row.RetryAt,
				RecoveryState:    writeback.RecoveryLabel(row),
				RemoteVerifiedAt: row.RemoteVerifiedAt,
				CompletedAt:      row.CompletedAt,
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

func WebDAVWritebackMonitorPage(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	c.Data(http.StatusOK, "text/html; charset=utf-8", []byte(webDAVWritebackMonitorHTML))
}

const webDAVWritebackMonitorHTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>WebDAV Writeback Monitor</title>
<style>
:root{font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#172033;background:#f6f8fb}
*{box-sizing:border-box}
body{margin:0}
.wrap{max-width:1500px;margin:0 auto;padding:24px}
.top{display:flex;gap:12px;align-items:center;flex-wrap:wrap;margin-bottom:16px}
h1{font-size:24px;margin:0;margin-right:auto}
button,select,input{border:1px solid #ccd4e0;border-radius:8px;background:#fff;padding:8px 10px;font:inherit}
button{cursor:pointer}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(155px,1fr));gap:10px;margin:14px 0}
.card{background:#fff;border:1px solid #e1e6ef;border-radius:12px;padding:14px}
.card .label{font-size:12px;color:#667085}
.card .value{font-size:24px;font-weight:700;margin-top:3px}
.note{font-size:12px;color:#667085;margin:8px 0 14px}
.status{font-size:13px;margin-left:4px}
.status.error{color:#b42318}
.table-wrap{overflow:auto;background:#fff;border:1px solid #e1e6ef;border-radius:12px}
table{border-collapse:collapse;width:100%;min-width:1250px}
th,td{padding:10px 12px;border-bottom:1px solid #eef1f5;text-align:left;vertical-align:top;font-size:13px}
th{position:sticky;top:0;background:#f8fafc;z-index:1;color:#475467}
.path{max-width:520px;word-break:break-all}
.badge{display:inline-block;border-radius:999px;padding:2px 8px;background:#eef2f6;font-size:12px;white-space:nowrap}
.badge.receiving{background:#e0f2fe;color:#075985}
.badge.queued{background:#fef3c7;color:#92400e}
.badge.uploading{background:#dbeafe;color:#1d4ed8}
.badge.verifying{background:#ede9fe;color:#6d28d9}
.badge.completed,.badge.acked{background:#dcfce7;color:#166534}
.badge.deleted,.badge.error{background:#fee2e2;color:#991b1b}
.err{color:#b42318;max-width:340px;word-break:break-word}
.muted{color:#98a2b3}
.small{font-size:12px}
.settings{background:#fff;border:1px solid #e1e6ef;border-radius:12px;padding:14px;margin:14px 0}
.settings summary{cursor:pointer;font-weight:700}
.settings-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:10px;margin-top:12px}
.field{display:flex;flex-direction:column;gap:5px;font-size:12px;color:#475467}
.field input[type="number"],.field input[type="text"]{width:100%}
.field.check{flex-direction:row;align-items:center}
.actions{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-top:12px}
.badge.missing_spool,.badge.needs_cloudsync_rehydrate{background:#fee2e2;color:#991b1b}
.badge.restart_recovery,.badge.waiting_provider_verification{background:#fff7ed;color:#9a3412}
@media(max-width:700px){.wrap{padding:12px}h1{font-size:20px}}
</style>
</head>
<body>
<div class="wrap">
  <div class="top">
    <h1>WebDAV Writeback Monitor</h1>
    <a id="back" href="#">Back to admin</a>
    <select id="state">
      <option value="active">Active</option>
      <option value="receiving">Receiving</option>
      <option value="queued">Queued</option>
      <option value="uploading">Uploading</option>
      <option value="verifying">Verifying</option>
      <option value="completed">Completed</option>
      <option value="error">Errors</option>
      <option value="all">All</option>
    </select>
    <input id="search" placeholder="Filter path">
    <button id="refresh">Refresh</button>
    <button id="cleanup">Clean verified cache</button>
    <label class="small"><input id="auto" type="checkbox" checked> auto 2s</label>
    <span id="status" class="status"></span>
  </div>
  <div class="cards">
    <div class="card"><div class="label">Receiving</div><div class="value" id="receiving">-</div></div>
    <div class="card"><div class="label">Queued</div><div class="value" id="queued">-</div></div>
    <div class="card"><div class="label">Uploading</div><div class="value" id="uploading">-</div></div>
    <div class="card"><div class="label">Verifying</div><div class="value" id="verifying">-</div></div>
    <div class="card"><div class="label">Completed</div><div class="value" id="completed">-</div></div>
    <div class="card"><div class="label">Errors</div><div class="value" id="errors">-</div></div>
    <div class="card"><div class="label">Durable backlog</div><div class="value" id="backlog">-</div><div class="small muted" id="backlogLimit"></div></div>
    <div class="card"><div class="label">Completed cache</div><div class="value" id="completedCache">-</div><div class="small muted" id="cacheTTL"></div></div>
    <div class="card"><div class="label">Disk reserve</div><div class="value" id="reserve">-</div></div>
    <div class="card"><div class="label">Workers</div><div class="value" id="workers">-</div></div>
    <div class="card"><div class="label">Receiving reservation</div><div class="value" id="receivingReservation">-</div></div>
    <div class="card"><div class="label">Disk free</div><div class="value" id="diskFree">-</div><div class="small muted" id="diskUsage"></div></div>
    <div class="card"><div class="label">Missing spool</div><div class="value" id="missingSpool">-</div></div>
    <div class="card"><div class="label">Restart recovery</div><div class="value" id="restartRecovery">-</div></div>
    <div class="card"><div class="label">Waiting provider verify</div><div class="value" id="waitingProvider">-</div></div>
    <div class="card"><div class="label">Needs CloudSync rehydrate</div><div class="value" id="needsRehydrate">-</div></div>
  </div>
  <details class="settings" open>
    <summary>WebDAV Durable Writeback settings</summary>
    <div class="note">Settings are persisted to the OpenList config file. SpoolDir is read-only here to avoid moving durable ACK data accidentally. Enabled/worker-pool changes are saved but require a restart to fully apply. Completed cache TTL = -1 disables automatic cleanup.</div>
    <div class="settings-grid">
      <label class="field check"><input id="cfgEnabled" type="checkbox"> Enabled</label>
      <label class="field"><span>SpoolDir (read only)</span><input id="cfgSpoolDir" type="text" readonly></label>
      <label class="field"><span>ReserveFreeSpaceMB</span><input id="cfgReserve" type="number" min="0"></label>
      <label class="field"><span>MaxPendingSpoolMB (0 = unlimited)</span><input id="cfgMaxPending" type="number" min="0"></label>
      <label class="field"><span>IncomingReservationChunkMB</span><input id="cfgReservationChunk" type="number" min="1"></label>
      <label class="field"><span>Workers</span><input id="cfgWorkers" type="number" min="1" max="256"></label>
      <label class="field"><span>UploadWorkers</span><input id="cfgUploadWorkers" type="number" min="1"></label>
      <label class="field"><span>LargeUploadWorkers</span><input id="cfgLargeUploadWorkers" type="number" min="1"></label>
      <label class="field"><span>ProviderProbeWorkers</span><input id="cfgProviderProbeWorkers" type="number" min="1"></label>
      <label class="field"><span>CompletedCacheTTLMinutes (-1 = disabled)</span><input id="cfgCacheTTL" type="number" min="-1"></label>
      <label class="field"><span>CloudSyncSettleMillis</span><input id="cfgSettle" type="number" min="0"></label>
      <label class="field"><span>CloudSyncPlaceholderMillis</span><input id="cfgPlaceholder" type="number" min="0"></label>
      <label class="field"><span>RetryInitialSeconds</span><input id="cfgRetryInitial" type="number" min="1"></label>
      <label class="field"><span>RetryMaxSeconds</span><input id="cfgRetryMax" type="number" min="1"></label>
      <label class="field"><span>VerifyIntervalSeconds</span><input id="cfgVerifyInterval" type="number" min="1"></label>
      <label class="field"><span>VerifyAttempts</span><input id="cfgVerifyAttempts" type="number" min="1"></label>
    </div>
    <div class="actions">
      <button id="saveSettings">Save settings</button>
      <button id="previewCleanup">Preview verified cache cleanup</button>
      <span id="settingsStatus" class="status"></span>
    </div>
  </details>
  <div class="note">Authoritative lifecycle only: receiving → durable_acked → queued → uploading → verifying → completed. No upload percentage is shown unless a storage driver eventually exposes real byte progress. Backlog and physical free-space admission apply WebDAV Retry-After backpressure; queued/uploading/verifying/recovery spools are never evicted. Generic providers use fresh listing + size/type evidence and stay inconclusive when evidence is insufficient; 115 Open may additionally use strong SHA1 verification.</div>
  <div class="table-wrap">
    <table>
      <thead><tr>
        <th>Path</th><th>Client</th><th>Provider</th><th>Recovery</th><th>Size</th><th>Generation</th>
        <th>Remote generation</th><th>Retry</th><th>Verify</th><th>Updated</th><th>Error</th>
      </tr></thead>
      <tbody id="rows"><tr><td colspan="11" class="muted">Loading…</td></tr></tbody>
    </table>
  </div>
</div>
<script>
(() => {
  const marker="/@manage/webdav-writeback";
  const idx=location.pathname.lastIndexOf(marker);
  const base=idx>=0?location.pathname.slice(0,idx):"";
  const api=base+"/api/admin/webdav-writeback";
  document.getElementById("back").href=base+"/@manage";
  const token=()=>localStorage.getItem("token")||"";
  const status=document.getElementById("status");
  const state=document.getElementById("state");
  const search=document.getElementById("search");
  const rows=document.getElementById("rows");
  const settingsStatus=document.getElementById("settingsStatus");
  const esc=(v)=>String(v??"").replace(/[&<>"']/g,(c)=>({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[c]));
  const fmtBytes=(n)=>{
    n=Number(n||0); if(!n)return "0 B";
    const u=["B","KiB","MiB","GiB","TiB"]; let i=0;
    while(n>=1024&&i<u.length-1){n/=1024;i++}
    return (i===0?n.toFixed(0):n.toFixed(2))+" "+u[i];
  };
  const fmtTime=(v)=>v?new Date(v).toLocaleString():"-";
  const badge=(v)=>'<span class="badge '+esc(v)+'">'+esc(v||"-")+'</span>';
  async function getJSON(url,options={}){
    const headers=Object.assign({Authorization:token()},options.headers||{});
    const r=await fetch(url,Object.assign({},options,{headers}));
    const j=await r.json().catch(()=>({}));
    if(!r.ok||j.code!==200) throw new Error(j.message||("HTTP "+r.status));
    return j.data;
  }
  function stateCount(summary,name){return summary.states?.[name]?.count||0}
  const numberValue=(id)=>Number(document.getElementById(id).value);
  function applySettings(cfg){
    document.getElementById("cfgEnabled").checked=!!cfg.enabled;
    document.getElementById("cfgSpoolDir").value=cfg.spool_dir||"";
    document.getElementById("cfgReserve").value=cfg.reserve_free_space_mb;
    document.getElementById("cfgMaxPending").value=cfg.max_pending_spool_mb;
    document.getElementById("cfgReservationChunk").value=cfg.incoming_reservation_chunk_mb;
    document.getElementById("cfgWorkers").value=cfg.workers;
    document.getElementById("cfgUploadWorkers").value=cfg.upload_workers;
    document.getElementById("cfgLargeUploadWorkers").value=cfg.large_upload_workers;
    document.getElementById("cfgProviderProbeWorkers").value=cfg.provider_probe_workers;
    document.getElementById("cfgCacheTTL").value=cfg.completed_cache_ttl_minutes;
    document.getElementById("cfgSettle").value=cfg.cloudsync_settle_millis;
    document.getElementById("cfgPlaceholder").value=cfg.cloudsync_placeholder_millis;
    document.getElementById("cfgRetryInitial").value=cfg.retry_initial_seconds;
    document.getElementById("cfgRetryMax").value=cfg.retry_max_seconds;
    document.getElementById("cfgVerifyInterval").value=cfg.verify_interval_seconds;
    document.getElementById("cfgVerifyAttempts").value=cfg.verify_attempts;
  }
  async function loadSettings(){
    try{ applySettings(await getJSON(api+"/settings")); }
    catch(e){ settingsStatus.className="status error"; settingsStatus.textContent=e.message; }
  }
  async function refresh(){
    status.className="status";
    status.textContent="Refreshing…";
    try{
      const params=new URLSearchParams({state:state.value,limit:"200"});
      if(search.value.trim())params.set("q",search.value.trim());
      const [summary,list]=await Promise.all([
        getJSON(api+"/summary"),
        getJSON(api+"/list?"+params.toString())
      ]);
      document.getElementById("receiving").textContent=summary.receiving||0;
      document.getElementById("queued").textContent=stateCount(summary,"queued");
      document.getElementById("uploading").textContent=stateCount(summary,"uploading");
      document.getElementById("verifying").textContent=stateCount(summary,"verifying");
      document.getElementById("completed").textContent=stateCount(summary,"completed");
      document.getElementById("errors").textContent=summary.errors||0;
      document.getElementById("backlog").textContent=fmtBytes(summary.backlog_bytes);
      document.getElementById("backlogLimit").textContent=summary.max_pending_spool_bytes?("limit "+fmtBytes(summary.max_pending_spool_bytes)):"unlimited";
      document.getElementById("completedCache").textContent=fmtBytes(summary.completed_cache_bytes);
      document.getElementById("cacheTTL").textContent=summary.completed_cache_ttl_minutes<0?"automatic cleanup disabled":("TTL "+summary.completed_cache_ttl_minutes+" min");
      document.getElementById("reserve").textContent=fmtBytes(summary.reserve_free_space_bytes);
      document.getElementById("workers").textContent=summary.workers||0;
      document.getElementById("receivingReservation").textContent=fmtBytes(summary.receiving_reservation_bytes);
      document.getElementById("diskFree").textContent=summary.disk_error?"unavailable":fmtBytes(summary.disk_free_bytes);
      document.getElementById("diskUsage").textContent=summary.disk_error?summary.disk_error:("used "+fmtBytes(summary.disk_used_bytes)+" / total "+fmtBytes(summary.disk_total_bytes));
      document.getElementById("missingSpool").textContent=summary.missing_spool||0;
      document.getElementById("restartRecovery").textContent=summary.restart_recovery||0;
      document.getElementById("waitingProvider").textContent=summary.waiting_provider_verification||0;
      document.getElementById("needsRehydrate").textContent=summary.needs_cloudsync_rehydrate||0;
      rows.innerHTML=(list||[]).length?(list||[]).map(x=>
        '<tr>'+
          '<td class="path"><strong>'+esc(x.path)+'</strong><div class="small muted">'+(x.is_dir?"directory":"file")+'</div></td>'+
          '<td>'+badge(x.client_state)+'</td>'+
          '<td>'+badge(x.provider_state)+'</td>'+
          '<td>'+(x.recovery_state?badge(x.recovery_state):'<span class="muted">-</span>')+'</td>'+
          '<td>'+esc(fmtBytes(x.size))+'</td>'+
          '<td>'+esc(x.generation)+'</td>'+
          '<td>'+esc(x.remote_generation)+'</td>'+
          '<td>'+esc(x.retry_count)+'</td>'+
          '<td>'+esc(x.verify_count)+'</td>'+
          '<td class="small">'+esc(fmtTime(x.updated_at))+'</td>'+
          '<td class="err">'+esc(x.last_error||"")+'</td>'+
        '</tr>'
      ).join(""):'<tr><td colspan="11" class="muted">No matching WebDAV writeback entries.</td></tr>';
      status.textContent="Updated "+new Date().toLocaleTimeString();
    }catch(e){
      status.className="status error";
      status.textContent=e.message+(token()?"":" — log in to the OpenList admin UI first");
    }
  }
  document.getElementById("refresh").addEventListener("click",refresh);
  document.getElementById("previewCleanup").addEventListener("click",async()=>{
    settingsStatus.className="status";
    settingsStatus.textContent="Checking cleanup eligibility…";
    try{
      const result=await getJSON(api+"/cleanup/preview");
      settingsStatus.textContent="Eligible "+(result?.eligible||0)+" files / "+fmtBytes(result?.eligible_bytes)+(result?.truncated?" (preview capped)":"");
    }catch(e){
      settingsStatus.className="status error";
      settingsStatus.textContent=e.message;
    }
  });
  document.getElementById("saveSettings").addEventListener("click",async()=>{
    settingsStatus.className="status";
    settingsStatus.textContent="Saving…";
    const payload={
      enabled:document.getElementById("cfgEnabled").checked,
      reserve_free_space_mb:numberValue("cfgReserve"),
      max_pending_spool_mb:numberValue("cfgMaxPending"),
      incoming_reservation_chunk_mb:numberValue("cfgReservationChunk"),
      workers:numberValue("cfgWorkers"),
      upload_workers:numberValue("cfgUploadWorkers"),
      large_upload_workers:numberValue("cfgLargeUploadWorkers"),
      provider_probe_workers:numberValue("cfgProviderProbeWorkers"),
      completed_cache_ttl_minutes:numberValue("cfgCacheTTL"),
      cloudsync_settle_millis:numberValue("cfgSettle"),
      cloudsync_placeholder_millis:numberValue("cfgPlaceholder"),
      retry_initial_seconds:numberValue("cfgRetryInitial"),
      retry_max_seconds:numberValue("cfgRetryMax"),
      verify_interval_seconds:numberValue("cfgVerifyInterval"),
      verify_attempts:numberValue("cfgVerifyAttempts")
    };
    try{
      const saved=await getJSON(api+"/settings",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(payload)});
      applySettings(saved);
      const restart=saved.restart_required_fields||[];
      settingsStatus.textContent=restart.length?("Saved. Restart required for: "+restart.join(", ")):"Saved and active.";
      await refresh();
    }catch(e){
      settingsStatus.className="status error";
      settingsStatus.textContent=e.message;
    }
  });
  document.getElementById("cleanup").addEventListener("click",async()=>{
    if(!confirm("Remove only completed, provider-verified local WebDAV cache files? Pending uploads will be kept."))return;
    status.className="status";
    status.textContent="Cleaning verified cache…";
    try{
      const result=await getJSON(api+"/cleanup",{method:"POST"});
      status.textContent="Released "+(result?.released||0)+" files / "+fmtBytes(result?.released_bytes)+" (eligible before cleanup: "+(result?.eligible||0)+" / "+fmtBytes(result?.eligible_bytes)+")";
      await refresh();
    }catch(e){
      status.className="status error";
      status.textContent=e.message;
    }
  });
  state.addEventListener("change",refresh);
  search.addEventListener("keydown",(e)=>{if(e.key==="Enter")refresh()});
  setInterval(()=>{if(document.getElementById("auto").checked)refresh()},2000);
  loadSettings();
  refresh();
})();
</script>
</body>
</html>`
