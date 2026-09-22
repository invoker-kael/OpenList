package handles

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/writeback"
	"gorm.io/gorm/schema"
)

func TestWebDAVWritebackMonitorUsesGormETagColumnName(t *testing.T) {
	parsed, err := schema.Parse(&model.WebDAVWritebackObject{}, &sync.Map{}, schema.NamingStrategy{})
	if err != nil {
		t.Fatalf("parse writeback schema: %v", err)
	}
	field := parsed.LookUpField("ETag")
	if field == nil {
		t.Fatal("ETag field missing from GORM schema")
	}
	if field.DBName != "e_tag" {
		t.Fatalf("unexpected ETag DB column: got %q want %q", field.DBName, "e_tag")
	}
	if !strings.Contains(webDAVWritebackMonitorColumns, field.DBName) {
		t.Fatalf("monitor SELECT %q does not contain GORM ETag column %q", webDAVWritebackMonitorColumns, field.DBName)
	}
	if strings.Contains(webDAVWritebackMonitorColumns, " etag") {
		t.Fatalf("monitor SELECT must not use non-existent raw column etag: %q", webDAVWritebackMonitorColumns)
	}
}


func TestWebDAVMonitorCanonicalStateKeepsWaitingReuploadAcked(t *testing.T) {
	row := &model.WebDAVWritebackObject{
		State: writeback.StateWaitingCloudSyncReupload,
	}
	if got := webDAVMonitorCanonicalState(row); got != writeback.CanonicalStateAcked {
		t.Fatalf("canonical state=%q, want %q", got, writeback.CanonicalStateAcked)
	}

	row.State = writeback.StateDeleted
	if got := webDAVMonitorCanonicalState(row); got != "deleted" {
		t.Fatalf("deleted canonical state=%q, want deleted", got)
	}
}

func TestWebDAVHistoryEffectiveStatusWaitsForCloudSync(t *testing.T) {
	now := time.Unix(200, 0)
	history := &model.WebDAVWritebackHistory{
		PathKey:      "path-key",
		Generation:   4,
		RecoveryType: "cloudsync_rehydrate_required",
		UpdatedAt:    now.Add(-time.Minute),
	}
	status, action := webDAVHistoryEffectiveStatus(history, nil, nil, now)
	if status != webDAVStatusWaitingCloudSync || action != webDAVActionRestartCloudSync {
		t.Fatalf("status=%q action=%q, want waiting/restart", status, action)
	}
}

func TestWebDAVHistoryEffectiveStatusDetectsActiveReuploadReceive(t *testing.T) {
	now := time.Unix(300, 0)
	started := now.Add(-5 * time.Second)
	lease := now.Add(time.Minute)
	history := &model.WebDAVWritebackHistory{
		PathKey:      "path-key",
		Generation:   4,
		RecoveryType: "cloudsync_rehydrate_required",
		UpdatedAt:    now.Add(-time.Minute),
	}
	fence := &model.WebDAVWritebackReceiveFence{
		PathKey:           "path-key",
		ActiveReceivers:   1,
		LatestStartedAt:   &started,
		ReceiveLeaseUntil: &lease,
	}
	status, action := webDAVHistoryEffectiveStatus(history, nil, fence, now)
	if status != webDAVStatusReuploadReceiving || action != "" {
		t.Fatalf("status=%q action=%q, want receiving/no action", status, action)
	}
}

func TestWebDAVHistoryEffectiveStatusDetectsReuploadAfterGenerationReset(t *testing.T) {
	now := time.Unix(400, 0)
	ack := now.Add(-10 * time.Second)
	history := &model.WebDAVWritebackHistory{
		PathKey:      "path-key",
		Generation:   7,
		RecoveryType: "cloudsync_rehydrate_required",
		UpdatedAt:    now.Add(-time.Minute),
	}
	current := &model.WebDAVWritebackObject{
		PathKey:        "path-key",
		Generation:     1,
		CanonicalState: "durable_acked",
		State:          "uploading",
		AckTime:        &ack,
	}
	status, action := webDAVHistoryEffectiveStatus(history, current, nil, now)
	if status != webDAVStatusReuploadUploading || action != "" {
		t.Fatalf("status=%q action=%q, want uploading/no action", status, action)
	}
}

func TestWebDAVHistoryEffectiveStatusRecovered(t *testing.T) {
	now := time.Unix(500, 0)
	ack := now.Add(-10 * time.Second)
	completed := now.Add(-5 * time.Second)
	history := &model.WebDAVWritebackHistory{
		PathKey:      "path-key",
		Generation:   3,
		RecoveryType: "cloudsync_rehydrate_required",
		UpdatedAt:    now.Add(-time.Minute),
	}
	current := &model.WebDAVWritebackObject{
		PathKey:        "path-key",
		Generation:     4,
		CanonicalState: "durable_acked",
		State:          "completed",
		AckTime:        &ack,
		CompletedAt:    &completed,
	}
	status, action := webDAVHistoryEffectiveStatus(history, current, nil, now)
	if status != webDAVStatusRecovered || action != "" {
		t.Fatalf("status=%q action=%q, want recovered/no action", status, action)
	}
}


func TestWebDAVHistoryEffectiveStatusRemoteHashMismatch(t *testing.T) {
	now := time.Unix(600, 0)
	history := &model.WebDAVWritebackHistory{
		PathKey:          "path-key",
		Generation:       9,
		Result:           "completed",
		FinalState:       "remote_hash_mismatch",
		ResolutionReason: "remote_hash_mismatch",
		UpdatedAt:        now.Add(-time.Minute),
	}
	status, action := webDAVHistoryEffectiveStatus(history, nil, nil, now)
	if status != webDAVStatusRemoteHashMismatch || action != webDAVActionManualCheck {
		t.Fatalf("status=%q action=%q, want remote_hash_mismatch/manual_check", status, action)
	}
}

func TestWebDAVHistoryRehydrateCanFinishWithRemoteHashMismatch(t *testing.T) {
	now := time.Unix(700, 0)
	ack := now.Add(-10 * time.Second)
	sha := strings.Repeat("a", 40)
	history := &model.WebDAVWritebackHistory{
		PathKey:      "path-key",
		Generation:   3,
		Size:         1024,
		PayloadSHA1:  sha,
		RecoveryType: "cloudsync_rehydrate_required",
		UpdatedAt:    now.Add(-time.Minute),
	}
	current := &model.WebDAVWritebackObject{
		PathKey:          "path-key",
		Generation:       4,
		Size:             1024,
		PayloadSHA1:      sha,
		CanonicalState:   "durable_acked",
		State:            "completed",
		ResolutionReason: "remote_hash_mismatch",
		AckTime:          &ack,
	}
	status, action := webDAVHistoryEffectiveStatus(history, current, nil, now)
	if status != webDAVStatusRemoteHashMismatch || action != webDAVActionManualCheck {
		t.Fatalf("status=%q action=%q, want remote_hash_mismatch/manual_check", status, action)
	}
}

func TestWebDAVHistoryRehydrateNewerGenerationSupersedesOldIncident(t *testing.T) {
	now := time.Unix(800, 0)
	ack := now.Add(-10 * time.Second)
	history := &model.WebDAVWritebackHistory{
		PathKey:      "path-key",
		Generation:   3,
		Size:         1024,
		PayloadSHA1:  strings.Repeat("a", 40),
		RecoveryType: "cloudsync_rehydrate_required",
		UpdatedAt:    now.Add(-time.Minute),
	}
	current := &model.WebDAVWritebackObject{
		PathKey:        "path-key",
		Generation:     4,
		Size:           2048,
		PayloadSHA1:    strings.Repeat("b", 40),
		CanonicalState: "durable_acked",
		State:          "completed",
		AckTime:        &ack,
	}
	status, action := webDAVHistoryEffectiveStatus(history, current, nil, now)
	if status != webDAVStatusRecovered || action != "" {
		t.Fatalf("newer same-path generation must supersede old recovery incident: status=%q action=%q", status, action)
	}
}

func TestWebDAVHistoryRehydrateNewerReceiveSupersedesOldIncident(t *testing.T) {
	now := time.Unix(900, 0)
	started := now.Add(-5 * time.Second)
	lease := now.Add(time.Minute)
	history := &model.WebDAVWritebackHistory{
		PathKey:      "path-key",
		Generation:   3,
		Size:         1024,
		RecoveryType: "cloudsync_rehydrate_required",
		UpdatedAt:    now.Add(-time.Minute),
	}
	fence := &model.WebDAVWritebackReceiveFence{
		PathKey:            "path-key",
		ActiveReceivers:    1,
		LatestExpectedSize: 2048,
		LatestStartedAt:    &started,
		ReceiveLeaseUntil:  &lease,
	}
	status, action := webDAVHistoryEffectiveStatus(history, nil, fence, now)
	if status != webDAVStatusReuploadReceiving || action != "" {
		t.Fatalf("newer same-path receive must supersede old recovery incident: status=%q action=%q", status, action)
	}
}

func TestWebDAVCurrentEffectiveStatusKeepsUnclassifiedWaitingRepairManual(t *testing.T) {
	row := &model.WebDAVWritebackObject{
		State:      "waiting_repair",
		RetryCount: 1,
	}
	status, action := webDAVCurrentEffectiveStatus(row)
	if status != webDAVStatusWaitingRepair || action != webDAVActionManualCheck {
		t.Fatalf("status=%q action=%q, want waiting_repair/manual_check", status, action)
	}
}

func TestWebDAVHistoryRehydrateHashMismatchRequiresCloudSyncRescan(t *testing.T) {
	now := time.Unix(1000, 0)
	ack := now.Add(-10 * time.Second)
	sha := strings.Repeat("a", 40)
	history := &model.WebDAVWritebackHistory{
		PathKey:      "path-key",
		Generation:   3,
		Size:         1024,
		PayloadSHA1:  sha,
		RecoveryType: "cloudsync_rehydrate_required",
		UpdatedAt:    now.Add(-time.Minute),
	}
	current := &model.WebDAVWritebackObject{
		PathKey:          "path-key",
		Generation:       4,
		Size:             1024,
		PayloadSHA1:      sha,
		CanonicalState:   "durable_acked",
		State:                     "waiting_cloudsync_reupload",
		ResolutionReason:          "remote_hash_mismatch",
		CloudSyncReuploadRequired: true,
		AckTime:                   &ack,
	}
	status, action := webDAVHistoryEffectiveStatus(history, current, nil, now)
	if status != webDAVStatusRemoteHashMismatch || action != webDAVActionRestartCloudSync {
		t.Fatalf("status=%q action=%q, want remote_hash_mismatch/restart_cloudsync_required", status, action)
	}
}

func TestWebDAVCurrentEffectiveStatusCloudSyncReuploadKeepsRootCause(t *testing.T) {
	row := &model.WebDAVWritebackObject{
		State:                     "waiting_cloudsync_reupload",
		CanonicalState:            "durable_acked",
		CloudSyncReuploadRequired: true,
		ResolutionReason:          "remote_missing",
	}
	status, action := webDAVCurrentEffectiveStatus(row)
	if status != webDAVStatusRemoteMissing || action != webDAVActionRestartCloudSync {
		t.Fatalf("status=%q action=%q, want remote_missing/restart_cloudsync_required", status, action)
	}

	row.ResolutionReason = "remote_hash_mismatch"
	status, action = webDAVCurrentEffectiveStatus(row)
	if status != webDAVStatusRemoteHashMismatch || action != webDAVActionRestartCloudSync {
		t.Fatalf("status=%q action=%q, want remote_hash_mismatch/restart_cloudsync_required", status, action)
	}
}

func TestWebDAVHistoryDurationsSeparateCloudSyncAndProviderTime(t *testing.T) {
	start := time.Unix(100, 0)
	durable := time.Unix(103, 0)
	providerStart := time.Unix(110, 0)
	providerDone := time.Unix(120, 0)
	completed := time.Unix(125, 0)
	item := webDAVWritebackHistoryRow{WebDAVWritebackHistory: model.WebDAVWritebackHistory{
		StartedAt:                 &start,
		DurableAt:                 &durable,
		ProviderUploadStartedAt:   &providerStart,
		ProviderUploadCompletedAt: &providerDone,
		CompletedAt:               &completed,
		Result:                    "completed",
	}}
	webDAVHistoryDurations(&item)
	if item.CloudSyncUploadDurationMS != 3000 {
		t.Fatalf("cloudsync duration=%dms, want 3000", item.CloudSyncUploadDurationMS)
	}
	if item.ProviderSyncDurationMS != 22000 {
		t.Fatalf("provider duration=%dms, want 22000", item.ProviderSyncDurationMS)
	}
	if item.LifecycleDurationMS != 25000 {
		t.Fatalf("lifecycle duration=%dms, want 25000", item.LifecycleDurationMS)
	}
	if len(item.EventTimeline) != 5 {
		t.Fatalf("timeline events=%d, want 5", len(item.EventTimeline))
	}
}


func TestWebDAVHistoryStatusInCloudSyncGroups(t *testing.T) {
	tests := []struct {
		name   string
		status string
		action string
		group  string
		want   bool
	}{
		{"receiving", webDAVStatusReceiving, webDAVActionWait, "receiving", true},
		{"reupload receiving", webDAVStatusReuploadReceiving, webDAVActionNone, "receiving", true},
		{"durable ack syncing", webDAVStatusDurableAcked, webDAVActionWait, "syncing", true},
		{"automatic recovery syncing", webDAVStatusAutomaticRecovery, webDAVActionRetryingAutomatically, "syncing", true},
		{"reupload verify syncing", webDAVStatusReuploadVerifying, webDAVActionWait, "syncing", true},
		{"completed", webDAVStatusCompleted, webDAVActionNone, "completed", true},
		{"recovered is completed", webDAVStatusRecovered, webDAVActionNone, "completed", true},
		{"restart means waiting reupload", webDAVStatusRemoteMissing, webDAVActionRestartCloudSync, "waiting_reupload", true},
		{"manual unresolved history is waiting reupload", webDAVStatusRemoteHashMismatch, webDAVActionManualCheck, "waiting_reupload", true},
		{"reupload progress stays waiting in history", webDAVStatusReuploadUploading, webDAVActionNone, "waiting_reupload", true},
		{"completed is not waiting reupload", webDAVStatusCompleted, webDAVActionNone, "waiting_reupload", false},
		{"deleted is not waiting reupload", "deleted", webDAVActionNone, "waiting_reupload", false},
		{"completed is not syncing", webDAVStatusCompleted, webDAVActionNone, "syncing", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := webDAVHistoryStatusInGroup(tc.status, tc.action, tc.group); got != tc.want {
				t.Fatalf("webDAVHistoryStatusInGroup(%q, %q, %q)=%v, want %v", tc.status, tc.action, tc.group, got, tc.want)
			}
		})
	}
}
