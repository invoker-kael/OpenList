package writeback

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	HistoryResultCompleted        = "completed"
	HistoryResultDeleted          = "deleted"
	HistoryResultRemoteMissing    = "remote_missing"
	HistoryResultRecoveryRequired = "recovery_required"

	HistoryRecoveryRestart                    = "restart_recovery"
	HistoryRecoveryMissingSpoolProvider       = "missing_spool_provider_recovered"
	HistoryRecoveryCloudSyncRehydrateRequired = "cloudsync_rehydrate_required"
	HistoryRecoveryRemoteMismatch             = "remote_mismatch"
	HistoryRecoveryManual                     = "manual"
)

type HistoryCleanupSpec struct {
	IDs           []uint
	Class         string
	OlderThanDays int
}

func cloneHistoryTime(v *time.Time) *time.Time {
	if v == nil {
		return nil
	}
	out := *v
	return &out
}

func historyRecoveryForCompletion(row *model.WebDAVWritebackObject) string {
	if row == nil {
		return ""
	}
	msg := strings.ToLower(row.LastError)
	if strings.Contains(msg, "durable spool is missing") {
		return HistoryRecoveryMissingSpoolProvider
	}
	if row.SpoolPath != "" {
		if availability, err := inspectDurableLocalPayload(row); err == nil && availability == durablePayloadMissing {
			return HistoryRecoveryMissingSpoolProvider
		}
	}
	if strings.Contains(msg, "interrupted") || strings.Contains(msg, "after restart") {
		return HistoryRecoveryRestart
	}
	return ""
}

func buildWritebackHistory(row *model.WebDAVWritebackObject, result, finalState, recovery string, finalAt time.Time, lastError string) *model.WebDAVWritebackHistory {
	if row == nil {
		return nil
	}
	key := row.PathKey
	if key == "" && row.Path != "" {
		key = pathKey(row.Path)
	}
	startedAt := cloneHistoryTime(row.AckTime)
	if startedAt == nil {
		startedAt = cloneHistoryTime(row.DurableAt)
	}
	var completedAt *time.Time
	if !finalAt.IsZero() {
		stamp := finalAt
		completedAt = &stamp
	}
	// DELETE generations do not have a new Durable ACK. Avoid carrying an ACK
	// timestamp from the previous visible generation into delete audit data.
	ackTime := cloneHistoryTime(row.AckTime)
	durableAt := cloneHistoryTime(row.DurableAt)
	if result == HistoryResultDeleted {
		startedAt = nil
		ackTime = nil
		durableAt = nil
	}
	return &model.WebDAVWritebackHistory{
		PathKey:                   key,
		Path:                      row.Path,
		Parent:                    row.Parent,
		Name:                      row.Name,
		Generation:                row.Generation,
		Size:                      row.Size,
		IsDir:                     row.IsDir,
		StartedAt:                 startedAt,
		AckTime:                   ackTime,
		DurableAt:                 durableAt,
		ProviderUploadStartedAt:   cloneHistoryTime(row.ProviderUploadStartedAt),
		ProviderUploadCompletedAt: cloneHistoryTime(row.ProviderUploadCompletedAt),
		CompletedAt:               completedAt,
		Result:                    result,
		FinalState:                finalState,
		RecoveryType:              recovery,
		PayloadSHA1:               row.PayloadSHA1,
		RemoteSHA1:                row.RemoteSHA1,
		RemoteObjectID:            row.RemoteObjectID,
		RemoteVerifiedAt:          cloneHistoryTime(row.RemoteVerifiedAt),
		RetryCount:                row.RetryCount,
		VerifyCount:               row.VerifyCount,
		LastError:                 lastError,
		ResolutionReason:          row.ResolutionReason,
		MimeType:                  row.MimeType,
	}
}

func upsertWritebackHistory(database *gorm.DB, value *model.WebDAVWritebackHistory) error {
	if database == nil {
		return errors.New("writeback history database is unavailable")
	}
	if value == nil || value.PathKey == "" || value.Generation == 0 {
		return errors.New("writeback history requires path key and generation")
	}
	res := database.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "path_key"}, {Name: "generation"}},
		DoNothing: true,
	}).Create(value)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected > 0 {
		return nil
	}

	var existing model.WebDAVWritebackHistory
	if err := database.Where("path_key = ? AND generation = ?", value.PathKey, value.Generation).Take(&existing).Error; err != nil {
		return err
	}
	updates := map[string]any{}
	if existing.Path == "" && value.Path != "" {
		updates["path"] = value.Path
	}
	if existing.Parent == "" && value.Parent != "" {
		updates["parent"] = value.Parent
	}
	if existing.Name == "" && value.Name != "" {
		updates["name"] = value.Name
	}
	if existing.Size == 0 && value.Size != 0 {
		updates["size"] = value.Size
	}
	if existing.StartedAt == nil && value.StartedAt != nil {
		updates["started_at"] = value.StartedAt
	}
	if existing.AckTime == nil && value.AckTime != nil {
		updates["ack_time"] = value.AckTime
	}
	if existing.DurableAt == nil && value.DurableAt != nil {
		updates["durable_at"] = value.DurableAt
	}
	if existing.ProviderUploadStartedAt == nil && value.ProviderUploadStartedAt != nil {
		updates["provider_upload_started_at"] = value.ProviderUploadStartedAt
	}
	if existing.ProviderUploadCompletedAt == nil && value.ProviderUploadCompletedAt != nil {
		updates["provider_upload_completed_at"] = value.ProviderUploadCompletedAt
	}
	if existing.CompletedAt == nil && value.CompletedAt != nil {
		updates["completed_at"] = value.CompletedAt
	}
	if existing.Result == "" && value.Result != "" {
		updates["result"] = value.Result
	}
	if existing.FinalState == "" && value.FinalState != "" {
		updates["final_state"] = value.FinalState
	}
	if existing.RecoveryType == "" && value.RecoveryType != "" {
		updates["recovery_type"] = value.RecoveryType
	}
	if existing.PayloadSHA1 == "" && value.PayloadSHA1 != "" {
		updates["payload_sha1"] = value.PayloadSHA1
	}
	if existing.RemoteSHA1 == "" && value.RemoteSHA1 != "" {
		updates["remote_sha1"] = value.RemoteSHA1
	}
	if existing.RemoteObjectID == "" && value.RemoteObjectID != "" {
		updates["remote_object_id"] = value.RemoteObjectID
	}
	if existing.RemoteVerifiedAt == nil && value.RemoteVerifiedAt != nil {
		updates["remote_verified_at"] = value.RemoteVerifiedAt
	}
	if value.RetryCount > existing.RetryCount {
		updates["retry_count"] = value.RetryCount
	}
	if value.VerifyCount > existing.VerifyCount {
		updates["verify_count"] = value.VerifyCount
	}
	if existing.LastError == "" && value.LastError != "" {
		updates["last_error"] = value.LastError
	}
	if existing.ResolutionReason == "" && value.ResolutionReason != "" {
		updates["resolution_reason"] = value.ResolutionReason
		if value.FinalState != "" {
			updates["final_state"] = value.FinalState
		}
	}
	if existing.MimeType == "" && value.MimeType != "" {
		updates["mime_type"] = value.MimeType
	}
	if len(updates) == 0 {
		return nil
	}
	return database.Model(&model.WebDAVWritebackHistory{}).
		Where("id = ?", existing.ID).
		Updates(updates).Error
}

func recordWritebackHistoryBestEffort(value *model.WebDAVWritebackHistory) {
	if value == nil {
		return
	}
	if err := upsertWritebackHistory(db.GetDb(), value); err != nil {
		log.Errorf("write-back history persistence failed for %s generation %d: %v", value.Path, value.Generation, err)
	}
}

func recordHistoryOutcomeBestEffort(row *model.WebDAVWritebackObject, result, finalState, recovery string, finalAt time.Time, lastError string) {
	recordWritebackHistoryBestEffort(buildWritebackHistory(row, result, finalState, recovery, finalAt, lastError))
}

func recordCompletedHistoryBestEffort(row *model.WebDAVWritebackObject, evidence remoteVerificationEvidence, now time.Time) {
	if row == nil {
		return
	}
	final := *row
	final.ResolutionReason = ""
	history := buildWritebackHistory(&final, HistoryResultCompleted, StateCompleted, historyRecoveryForCompletion(row), now, row.LastError)
	if history == nil {
		return
	}
	history.RemoteObjectID = evidence.objectID
	history.RemoteSHA1 = evidence.sha1
	history.RemoteVerifiedAt = &evidence.verifiedAt
	recordWritebackHistoryBestEffort(history)
}

func CleanupHistory(database *gorm.DB, spec HistoryCleanupSpec) (int64, error) {
	if database == nil {
		return 0, errors.New("writeback history database is unavailable")
	}
	query := database.Model(&model.WebDAVWritebackHistory{})
	if len(spec.IDs) > 0 {
		query = query.Where("id IN ?", spec.IDs)
	} else {
		switch strings.ToLower(strings.TrimSpace(spec.Class)) {
		case "successful":
			if spec.OlderThanDays <= 0 {
				return 0, errors.New("successful history cleanup requires older_than_days > 0")
			}
			query = query.
				Where("result IN ?", []string{HistoryResultCompleted, HistoryResultDeleted}).
				Where("(recovery_type = '' OR recovery_type IS NULL)")
		case "recovery":
			query = query.Where("recovery_type <> ''")
		case "error":
			query = query.Where("last_error <> ''")
		case "remote_missing":
			query = query.Where("result = ? OR recovery_type = ?", HistoryResultRemoteMissing, HistoryRecoveryCloudSyncRehydrateRequired)
		default:
			return 0, fmt.Errorf("unsupported history cleanup class %q", spec.Class)
		}
		if spec.OlderThanDays > 0 {
			cutoff := time.Now().Add(-time.Duration(spec.OlderThanDays) * 24 * time.Hour)
			query = query.Where("updated_at < ?", cutoff)
		}
	}
	res := query.Delete(&model.WebDAVWritebackHistory{})
	return res.RowsAffected, res.Error
}
