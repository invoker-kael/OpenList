package writeback

import (
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func historyTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	database, err := gorm.Open(sqlite.Open("file:"+t.Name()+"?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.AutoMigrate(&model.WebDAVWritebackHistory{}, &model.WebDAVWritebackObject{}, &model.WebDAVProviderOperation{}); err != nil {
		t.Fatal(err)
	}
	return database
}

func TestHistoryUpsertIsIdempotentPerPathGeneration(t *testing.T) {
	database := historyTestDB(t)
	now := time.Now()
	first := &model.WebDAVWritebackHistory{
		PathKey: "path-key", Path: "/backup/a.zip", Generation: 7,
		Result: HistoryResultCompleted, FinalState: StateCompleted, CompletedAt: &now,
	}
	if err := upsertWritebackHistory(database, first); err != nil {
		t.Fatal(err)
	}
	second := &model.WebDAVWritebackHistory{
		PathKey: "path-key", Path: "/backup/a.zip", Generation: 7,
		Result: HistoryResultRemoteMissing, FinalState: StateDeleted,
		RecoveryType: HistoryRecoveryCloudSyncRehydrateRequired,
		RetryCount:   2, VerifyCount: 3, LastError: "confirmed provider loss",
	}
	if err := upsertWritebackHistory(database, second); err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := database.Model(&model.WebDAVWritebackHistory{}).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("history rows=%d, want 1", count)
	}
	var got model.WebDAVWritebackHistory
	if err := database.First(&got).Error; err != nil {
		t.Fatal(err)
	}
	if got.Result != HistoryResultCompleted {
		t.Fatalf("existing final result was overwritten: %q", got.Result)
	}
	if got.RecoveryType != HistoryRecoveryCloudSyncRehydrateRequired || got.RetryCount != 2 || got.VerifyCount != 3 {
		t.Fatalf("missing supplemental recovery data: %+v", got)
	}
}

func TestHistoryNewGenerationCreatesNewRow(t *testing.T) {
	database := historyTestDB(t)
	for generation := uint64(1); generation <= 2; generation++ {
		h := &model.WebDAVWritebackHistory{
			PathKey: "same-path", Path: "/backup/a.zip", Generation: generation,
			Result: HistoryResultCompleted, FinalState: StateCompleted,
		}
		if err := upsertWritebackHistory(database, h); err != nil {
			t.Fatal(err)
		}
	}
	var count int64
	if err := database.Model(&model.WebDAVWritebackHistory{}).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("history rows=%d, want 2", count)
	}
}

func TestHistoryCompletionRecoveryClassification(t *testing.T) {
	restart := &model.WebDAVWritebackObject{LastError: "resuming remote verification after interrupted upload"}
	if got := historyRecoveryForCompletion(restart); got != HistoryRecoveryRestart {
		t.Fatalf("restart recovery=%q", got)
	}
	missing := &model.WebDAVWritebackObject{LastError: "durable spool is missing after restart; checking provider"}
	if got := historyRecoveryForCompletion(missing); got != HistoryRecoveryMissingSpoolProvider {
		t.Fatalf("missing spool recovery=%q", got)
	}
}

func TestHistoryCleanupCannotTouchCanonicalOrSpoolState(t *testing.T) {
	database := historyTestDB(t)
	old := time.Now().Add(-10 * 24 * time.Hour)
	canonical := model.WebDAVWritebackObject{
		PathKey: "canonical-key", Path: "/backup/a.zip", Parent: "/backup", Name: "a.zip",
		Generation: 9, CanonicalState: CanonicalStateAcked, State: StateCompleted,
		SpoolPath: "/tmp/writeback/a.data",
	}
	if err := database.Create(&canonical).Error; err != nil {
		t.Fatal(err)
	}
	history := model.WebDAVWritebackHistory{
		PathKey: "history-key", Path: "/old.zip", Generation: 1,
		Result: HistoryResultCompleted, FinalState: StateCompleted, UpdatedAt: old,
	}
	if err := database.Create(&history).Error; err != nil {
		t.Fatal(err)
	}
	deleted, err := CleanupHistory(database, HistoryCleanupSpec{Class: "successful", OlderThanDays: 7})
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 1 {
		t.Fatalf("deleted=%d, want 1", deleted)
	}
	var got model.WebDAVWritebackObject
	if err := database.First(&got, canonical.ID).Error; err != nil {
		t.Fatalf("canonical row was removed: %v", err)
	}
	if got.Generation != canonical.Generation || got.SpoolPath != canonical.SpoolPath || got.CanonicalState != canonical.CanonicalState {
		t.Fatalf("canonical state changed during history cleanup: %+v", got)
	}
}

func TestSuccessfulHistoryCleanupPreservesRecoveryEvidence(t *testing.T) {
	database := historyTestDB(t)
	old := time.Now().Add(-90 * 24 * time.Hour)
	rows := []model.WebDAVWritebackHistory{
		{PathKey: "plain", Path: "/plain", Generation: 1, Result: HistoryResultCompleted, FinalState: StateCompleted, UpdatedAt: old},
		{PathKey: "recovery", Path: "/recovery", Generation: 1, Result: HistoryResultCompleted, FinalState: StateCompleted, RecoveryType: HistoryRecoveryRestart, UpdatedAt: old},
		{PathKey: "missing", Path: "/missing", Generation: 1, Result: HistoryResultRemoteMissing, FinalState: StateDeleted, RecoveryType: HistoryRecoveryCloudSyncRehydrateRequired, UpdatedAt: old},
	}
	if err := database.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := CleanupHistory(database, HistoryCleanupSpec{Class: "successful", OlderThanDays: 30}); err != nil {
		t.Fatal(err)
	}
	var remaining []model.WebDAVWritebackHistory
	if err := database.Order("path asc").Find(&remaining).Error; err != nil {
		t.Fatal(err)
	}
	if len(remaining) != 2 {
		t.Fatalf("remaining=%d, want recovery and remote-missing rows", len(remaining))
	}
}
