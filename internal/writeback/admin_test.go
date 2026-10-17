package writeback

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func validAdminConfig() conf.WebDAVWritebackConfig {
	return conf.WebDAVWritebackConfig{
		Enabled:                     true,
		SpoolDir:                    "/tmp/writeback",
		ReserveFreeSpaceMB:          1024,
		MaxPendingSpoolMB:           4096,
		IncomingReservationChunkMB:  64,
		AdmissionRetrySeconds:       5,
		Workers:                     4,
		UploadWorkers:               3,
		LargeUploadWorkers:          2,
		ProviderProbeWorkers:        2,
		CloudSyncSettleMillis:       2000,
		CloudSyncPlaceholderMillis:  10000,
		DirectoryGraceSeconds:       60,
		RetryInitialSeconds:         30,
		RetryMaxSeconds:             1800,
		VerifyIntervalSeconds:       5,
		VerifyAttempts:              60,
		CompletedCacheTTLMinutes:    30,
		CompletedRemoteProbeSeconds: 1800,
		ProviderSnapshotTTLSeconds:  600,
	}
}

func TestValidateAdminConfigRejectsUnsafeValues(t *testing.T) {
	tests := []struct {
		name string
		edit func(*conf.WebDAVWritebackConfig)
	}{
		{"workers zero", func(c *conf.WebDAVWritebackConfig) { c.Workers = 0 }},
		{"upload workers exceed pool", func(c *conf.WebDAVWritebackConfig) { c.UploadWorkers = c.Workers + 1 }},
		{"reservation chunk zero", func(c *conf.WebDAVWritebackConfig) { c.IncomingReservationChunkMB = 0 }},
		{"retry max before initial", func(c *conf.WebDAVWritebackConfig) { c.RetryMaxSeconds = c.RetryInitialSeconds - 1 }},
		{"verify interval zero", func(c *conf.WebDAVWritebackConfig) { c.VerifyIntervalSeconds = 0 }},
		{"verify attempts zero", func(c *conf.WebDAVWritebackConfig) { c.VerifyAttempts = 0 }},
		{"invalid negative cache ttl", func(c *conf.WebDAVWritebackConfig) { c.CompletedCacheTTLMinutes = -2 }},
		{"empty spool directory", func(c *conf.WebDAVWritebackConfig) { c.SpoolDir = "" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validAdminConfig()
			tt.edit(&cfg)
			if err := ValidateAdminConfig(cfg); err == nil {
				t.Fatal("unsafe configuration unexpectedly validated")
			}
		})
	}
}

func TestValidateAdminConfigPreservesDocumentedSentinels(t *testing.T) {
	cfg := validAdminConfig()
	cfg.CompletedCacheTTLMinutes = -1
	cfg.ProviderSnapshotTTLSeconds = -1
	cfg.MaxPendingSpoolMB = 0
	cfg.ReserveFreeSpaceMB = 0
	if err := ValidateAdminConfig(cfg); err != nil {
		t.Fatalf("documented disabled/unlimited values should remain valid: %v", err)
	}
}

func TestCollectAndRemoveRebuildSpoolFilesIsScopedToPayloads(t *testing.T) {
	spoolDir := t.TempDir()
	payload := filepath.Join(spoolDir, "payload.data")
	partial := filepath.Join(spoolDir, "recv.part")
	keep := filepath.Join(spoolDir, "keep.txt")
	nested := filepath.Join(spoolDir, "nested")
	if err := os.Mkdir(nested, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{payload, partial, keep, filepath.Join(nested, "nested.data")} {
		if err := os.WriteFile(name, []byte("data"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	files, err := collectRebuildSpoolFiles(spoolDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 || files[0] != payload || files[1] != partial {
		t.Fatalf("rebuild spool files=%v, want only top-level .data and .part payloads", files)
	}
	if err := removeRebuildSpoolFiles(files); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{payload, partial} {
		if _, err := os.Stat(name); !os.IsNotExist(err) {
			t.Fatalf("spool payload %q still exists or returned unexpected error: %v", name, err)
		}
	}
	for _, name := range []string{keep, filepath.Join(nested, "nested.data")} {
		if _, err := os.Stat(name); err != nil {
			t.Fatalf("non-payload file %q was touched: %v", name, err)
		}
	}
}

func TestExisting115MatchCompletesQueuedGenerationBeforeUpload(t *testing.T) {
	oldConf := conf.Conf
	conf.Conf = &conf.Config{Database: conf.Database{Type: "sqlite"}}
	defer func() { conf.Conf = oldConf }()

	database, err := gorm.Open(sqlite.Open("file:"+t.Name()+"?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	db.Init(database)

	sha := strings.Repeat("a", 40)
	row := model.WebDAVWritebackObject{
		PathKey:        pathKey("/backup/file.bin"),
		ParentKey:      pathKey("/backup"),
		Path:           "/backup/file.bin",
		Parent:         "/backup",
		Name:           "file.bin",
		Generation:     7,
		CanonicalState: CanonicalStateAcked,
		State:          StateQueued,
		Size:           10,
		PayloadSHA1:    sha,
	}
	if err := database.Create(&row).Error; err != nil {
		t.Fatal(err)
	}

	remote := &model.Object{
		ID:       "115-object-id",
		Name:     row.Name,
		Size:     row.Size,
		HashInfo: utils.NewHashInfo(utils.SHA1, sha),
	}
	comparison := (&workerManager{}).completeExistingRemote(&row, []model.Obj{remote})
	if comparison != remoteContentMatch {
		t.Fatalf("remote comparison=%v, want exact match", comparison)
	}
	var saved model.WebDAVWritebackObject
	if err := database.First(&saved, row.ID).Error; err != nil {
		t.Fatal(err)
	}
	if saved.State != StateCompleted || saved.RemoteObjectID != remote.GetID() || saved.RemoteSHA1 != sha {
		t.Fatalf("existing remote match did not complete generation: state=%q object_id=%q sha1_match=%t", saved.State, saved.RemoteObjectID, saved.RemoteSHA1 == sha)
	}
	if saved.ProviderUploadStartedAt != nil {
		t.Fatalf("existing remote match started a provider upload at %v", saved.ProviderUploadStartedAt)
	}
}

func TestCloudSyncReuploadRefusesWhileSpoolIsAvailable(t *testing.T) {
	oldConf := conf.Conf
	cfg := validAdminConfig()
	cfg.SpoolDir = t.TempDir()
	conf.Conf = &conf.Config{Database: conf.Database{Type: "sqlite"}, WebDAVWriteback: cfg}
	defer func() { conf.Conf = oldConf }()

	database, err := gorm.Open(sqlite.Open("file:"+t.Name()+"?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	db.Init(database)

	spool := filepath.Join(t.TempDir(), "payload.data")
	if err := os.WriteFile(spool, []byte("durable payload"), 0o600); err != nil {
		t.Fatal(err)
	}
	row := model.WebDAVWritebackObject{
		PathKey:        pathKey("/backup/file.bin"),
		ParentKey:      pathKey("/backup"),
		Path:           "/backup/file.bin",
		Parent:         "/backup",
		Name:           "file.bin",
		Generation:     3,
		CanonicalState: CanonicalStateAcked,
		State:          StateVerifying,
		Size:           int64(len("durable payload")),
		SpoolPath:      spool,
	}
	if err := database.Create(&row).Error; err != nil {
		t.Fatal(err)
	}

	if requireCloudSyncReupload(&row, StateVerifying, ResolutionVerificationExhausted, "115 content was not confirmed", 3, true) {
		t.Fatal("Cloud Sync re-upload must not be requested while the durable spool is available")
	}
	obj, found, deleted, err := Canonical(row.Path)
	if err != nil {
		t.Fatal(err)
	}
	if !found || deleted || obj == nil {
		t.Fatalf("canonical path after inconclusive provider result = (%v, %v, %v), want the ACKed object", obj, found, deleted)
	}
	if _, err := os.Stat(spool); err != nil {
		t.Fatalf("durable spool was removed or returned unexpected error: %v", err)
	}
}

func setupUploadLifecycleTest(t *testing.T) (*gorm.DB, string) {
	t.Helper()
	oldConf := conf.Conf
	spoolDir := t.TempDir()
	conf.Conf = &conf.Config{
		Database: conf.Database{Type: "sqlite"},
		WebDAVWriteback: conf.WebDAVWritebackConfig{
			Enabled:                  true,
			SpoolDir:                 spoolDir,
			Workers:                  1,
			UploadWorkers:            1,
			LargeUploadWorkers:       1,
			ProviderProbeWorkers:     1,
			RetryInitialSeconds:      1,
			RetryMaxSeconds:          2,
			VerifyIntervalSeconds:    1,
			VerifyAttempts:           3,
			CompletedCacheTTLMinutes: 30,
		},
	}
	database, err := gorm.Open(sqlite.Open("file:"+t.Name()+"?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	db.Init(database)
	t.Cleanup(func() { conf.Conf = oldConf })
	return database, spoolDir
}

func createUploadLifecycleRow(t *testing.T, database *gorm.DB, spoolDir, payload string) model.WebDAVWritebackObject {
	t.Helper()
	spool := filepath.Join(spoolDir, "payload.data")
	if err := os.WriteFile(spool, []byte(payload), 0o600); err != nil {
		t.Fatal(err)
	}
	hash := sha1.Sum([]byte(payload))
	row := model.WebDAVWritebackObject{
		PathKey:        pathKey("/backup/file.bin"),
		ParentKey:      pathKey("/backup"),
		Path:           "/backup/file.bin",
		Parent:         "/backup",
		Name:           "file.bin",
		Generation:     1,
		CanonicalState: CanonicalStateAcked,
		State:          StateQueued,
		Size:           int64(len(payload)),
		PayloadSHA1:    hex.EncodeToString(hash[:]),
		SpoolPath:      spool,
	}
	if err := database.Create(&row).Error; err != nil {
		t.Fatal(err)
	}
	return row
}

func reloadUploadLifecycleRow(t *testing.T, database *gorm.DB, id uint) model.WebDAVWritebackObject {
	t.Helper()
	var row model.WebDAVWritebackObject
	if err := database.First(&row, id).Error; err != nil {
		t.Fatal(err)
	}
	return row
}

func TestProcessUploadCompletesOnProviderSuccessWithoutPostUploadVerification(t *testing.T) {
	database, spoolDir := setupUploadLifecycleTest(t)
	row := createUploadLifecycleRow(t, database, spoolDir, "durable payload")
	listCalls, putCalls := 0, 0
	manager := &workerManager{
		ctx:         context.Background(),
		requireHash: func(string) bool { return true },
		listParent: func(string) ([]model.Obj, error) {
			listCalls++
			return nil, nil
		},
		putProvider: func(_ string, file model.FileStreamer, progress model.UpdateProgress) error {
			putCalls++
			if _, err := io.Copy(io.Discard, file); err != nil {
				return err
			}
			progress(100)
			return nil
		},
	}

	manager.processUpload(&row)

	saved := reloadUploadLifecycleRow(t, database, row.ID)
	if saved.State != StateCompleted || saved.ProviderUploadCompletedAt == nil {
		t.Fatalf("provider success state=%q completed_at=%v, want completed", saved.State, saved.ProviderUploadCompletedAt)
	}
	if saved.RemoteVerifiedAt != nil || saved.RemoteSHA1 != "" {
		t.Fatalf("normal upload unexpectedly required remote verification: verified_at=%v remote_sha1=%q", saved.RemoteVerifiedAt, saved.RemoteSHA1)
	}
	if saved.ProviderUploadAttempts != 1 || putCalls != 1 || listCalls != 1 {
		t.Fatalf("attempts=%d PUTs=%d preflight LISTs=%d, want 1/1/1", saved.ProviderUploadAttempts, putCalls, listCalls)
	}
}

func TestProcessUploadSkipsPUTWhenFreshListHasExactContent(t *testing.T) {
	database, spoolDir := setupUploadLifecycleTest(t)
	row := createUploadLifecycleRow(t, database, spoolDir, "durable payload")
	remote := &model.Object{
		Name:     row.Name,
		Size:     row.Size,
		HashInfo: utils.NewHashInfo(utils.SHA1, row.PayloadSHA1),
	}
	putCalls := 0
	manager := &workerManager{
		ctx:         context.Background(),
		requireHash: func(string) bool { return true },
		listParent:  func(string) ([]model.Obj, error) { return []model.Obj{remote}, nil },
		putProvider: func(string, model.FileStreamer, model.UpdateProgress) error {
			putCalls++
			return nil
		},
	}

	manager.processUpload(&row)

	saved := reloadUploadLifecycleRow(t, database, row.ID)
	if saved.State != StateCompleted || saved.RemoteSHA1 != row.PayloadSHA1 || saved.ProviderUploadAttempts != 0 {
		t.Fatalf("existing exact object state=%q remote_sha1=%q attempts=%d", saved.State, saved.RemoteSHA1, saved.ProviderUploadAttempts)
	}
	if putCalls != 0 {
		t.Fatalf("provider PUTs=%d for exact fresh-list match, want 0", putCalls)
	}
}

func TestProcessUploadProviderErrorKeepsSpoolAndExactRetryMatchAvoidsSecondPut(t *testing.T) {
	database, spoolDir := setupUploadLifecycleTest(t)
	row := createUploadLifecycleRow(t, database, spoolDir, "durable payload")
	hash := row.PayloadSHA1
	listCalls, putCalls := 0, 0
	remote := &model.Object{Name: row.Name, Size: row.Size, HashInfo: utils.NewHashInfo(utils.SHA1, hash)}
	manager := &workerManager{
		ctx:         context.Background(),
		requireHash: func(string) bool { return true },
		listParent: func(string) ([]model.Obj, error) {
			listCalls++
			if listCalls == 1 {
				return nil, nil
			}
			return []model.Obj{remote}, nil
		},
		putProvider: func(_ string, file model.FileStreamer, progress model.UpdateProgress) error {
			putCalls++
			buf := make([]byte, 4)
			_, _ = file.Read(buf)
			progress(40)
			return errors.New("provider response timed out after accepting the payload")
		},
	}

	manager.processUpload(&row)
	failedAttempt := reloadUploadLifecycleRow(t, database, row.ID)
	if failedAttempt.State != StateQueued || failedAttempt.CanonicalState != CanonicalStateAcked || failedAttempt.RetryCount != 1 {
		t.Fatalf("provider error state=%q canonical=%q retries=%d, want queued/ACKed/1", failedAttempt.State, failedAttempt.CanonicalState, failedAttempt.RetryCount)
	}
	if _, err := os.Stat(failedAttempt.SpoolPath); err != nil {
		t.Fatalf("provider error discarded durable spool: %v", err)
	}

	manager.processUpload(&failedAttempt)
	saved := reloadUploadLifecycleRow(t, database, row.ID)
	if saved.State != StateCompleted || saved.RemoteSHA1 != hash {
		t.Fatalf("retry matching existing object did not complete: state=%q remote_sha1=%q", saved.State, saved.RemoteSHA1)
	}
	if putCalls != 1 || listCalls != 2 {
		t.Fatalf("PUTs=%d preflight LISTs=%d, want one PUT and one retry LIST", putCalls, listCalls)
	}
}

func TestProcessUploadListErrorDoesNotBecomeMissingOrStartPut(t *testing.T) {
	database, spoolDir := setupUploadLifecycleTest(t)
	row := createUploadLifecycleRow(t, database, spoolDir, "durable payload")
	putCalls := 0
	manager := &workerManager{
		ctx:         context.Background(),
		requireHash: func(string) bool { return true },
		listParent: func(string) ([]model.Obj, error) {
			return nil, errors.New("115 rate limit")
		},
		putProvider: func(string, model.FileStreamer, model.UpdateProgress) error {
			putCalls++
			return nil
		},
	}

	manager.processUpload(&row)

	saved := reloadUploadLifecycleRow(t, database, row.ID)
	if saved.State != StateQueued || saved.CanonicalState != CanonicalStateAcked || saved.CloudSyncReuploadRequired {
		t.Fatalf("list API error changed client-visible existence: state=%q canonical=%q reupload=%v", saved.State, saved.CanonicalState, saved.CloudSyncReuploadRequired)
	}
	if putCalls != 0 {
		t.Fatalf("provider PUTs=%d after inconclusive LIST, want 0", putCalls)
	}
	if _, err := os.Stat(saved.SpoolPath); err != nil {
		t.Fatalf("list API error discarded durable spool: %v", err)
	}
}

func TestProcessRemoteVerificationAPIErrorRemainsRetryable(t *testing.T) {
	database, _ := setupUploadLifecycleTest(t)
	row := model.WebDAVWritebackObject{
		PathKey:        pathKey("/backup/file.bin"),
		ParentKey:      pathKey("/backup"),
		Path:           "/backup/file.bin",
		Parent:         "/backup",
		Name:           "file.bin",
		Generation:     1,
		CanonicalState: CanonicalStateAcked,
		State:          StateVerifying,
		Size:           100,
	}
	if err := database.Create(&row).Error; err != nil {
		t.Fatal(err)
	}
	manager := &workerManager{
		ctx:         context.Background(),
		requireHash: func(string) bool { return true },
		listParent: func(string) ([]model.Obj, error) {
			return nil, errors.New("115 API timeout")
		},
	}

	manager.processRemoteVerification(&row, StateVerifying, true)

	saved := reloadUploadLifecycleRow(t, database, row.ID)
	if saved.State != StateVerifying || saved.CanonicalState != CanonicalStateAcked || saved.CloudSyncReuploadRequired {
		t.Fatalf("API error state=%q canonical=%q reupload=%v, want retryable/ACKed/not-reuploading", saved.State, saved.CanonicalState, saved.CloudSyncReuploadRequired)
	}
	if saved.RetryCount != 1 || saved.VerifyCount != 0 {
		t.Fatalf("API error retry_count=%d verify_count=%d, want retry=1 and no missing confirmation", saved.RetryCount, saved.VerifyCount)
	}
}

func TestRecoverInterruptedUploadQueuesDurableSpoolForPreflight(t *testing.T) {
	database, spoolDir := setupUploadLifecycleTest(t)
	row := createUploadLifecycleRow(t, database, spoolDir, "durable payload")
	if err := database.Model(&model.WebDAVWritebackObject{}).
		Where("id = ? AND generation = ?", row.ID, row.Generation).
		Updates(map[string]any{"state": StateUploading, "provider_upload_attempts": 1}).Error; err != nil {
		t.Fatal(err)
	}

	manager := &workerManager{}
	if err := manager.recoverInterrupted(); err != nil {
		t.Fatal(err)
	}
	saved := reloadUploadLifecycleRow(t, database, row.ID)
	if saved.State != StateQueued || !saved.RestartUploadRecovery || saved.CanonicalState != CanonicalStateAcked {
		t.Fatalf("restart state=%q recovery=%v canonical=%q, want queued preflight/ACKed", saved.State, saved.RestartUploadRecovery, saved.CanonicalState)
	}
	if _, err := os.Stat(saved.SpoolPath); err != nil {
		t.Fatalf("restart recovery discarded durable spool: %v", err)
	}
}

func TestProcessUploadCountsOnlyRepeatedProviderBytes(t *testing.T) {
	database, spoolDir := setupUploadLifecycleTest(t)
	row := createUploadLifecycleRow(t, database, spoolDir, "durable payload")
	putCalls := 0
	manager := &workerManager{
		ctx:         context.Background(),
		requireHash: func(string) bool { return true },
		listParent:  func(string) ([]model.Obj, error) { return nil, nil },
		putProvider: func(_ string, file model.FileStreamer, progress model.UpdateProgress) error {
			putCalls++
			if _, err := io.Copy(io.Discard, file); err != nil {
				return err
			}
			progress(50)
			if putCalls == 1 {
				return errors.New("115 temporary upload error")
			}
			progress(100)
			return nil
		},
	}

	manager.processUpload(&row)
	retry := reloadUploadLifecycleRow(t, database, row.ID)
	manager.processUpload(&retry)

	saved := reloadUploadLifecycleRow(t, database, row.ID)
	if saved.State != StateCompleted || saved.ProviderUploadAttempts != 2 || saved.RetryCount != 1 {
		t.Fatalf("state=%q provider_attempts=%d retries=%d, want completed/2/1", saved.State, saved.ProviderUploadAttempts, saved.RetryCount)
	}
	if saved.ProviderRetryBytes != saved.Size {
		t.Fatalf("retry bytes=%d, want one payload size %d", saved.ProviderRetryBytes, saved.Size)
	}
	var history model.WebDAVWritebackHistory
	if err := database.Where("path_key = ? AND generation = ?", saved.PathKey, saved.Generation).Take(&history).Error; err != nil {
		t.Fatal(err)
	}
	if history.ProviderUploadAttempts != 2 || history.ProviderRetryBytes != saved.Size {
		t.Fatalf("history attempts=%d retry_bytes=%d, want 2/%d", history.ProviderUploadAttempts, history.ProviderRetryBytes, saved.Size)
	}
}

func TestCommitDoesNotAcknowledgeWhenSpoolDirectorySyncFails(t *testing.T) {
	database, spoolDir := setupUploadLifecycleTest(t)
	oldSync := syncSpoolDirectory
	syncSpoolDirectory = func(string) error { return errors.New("directory sync failed") }
	defer func() { syncSpoolDirectory = oldSync }()

	payload := "durable payload"
	if _, _, err := Commit(context.Background(), "/backup/file.bin", strings.NewReader(payload), int64(len(payload)), time.Time{}, time.Time{}, ""); err == nil {
		t.Fatal("Commit succeeded without durable spool directory metadata")
	}
	var count int64
	if err := database.Model(&model.WebDAVWritebackObject{}).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("ACKed writeback rows=%d after spool sync failure, want 0", count)
	}
	entries, err := os.ReadDir(spoolDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("spool entries=%v after failed durable commit, want none", entries)
	}
}

func TestCommitCoalescesCompletedSameContentWithoutCreatingGeneration(t *testing.T) {
	database, spoolDir := setupUploadLifecycleTest(t)
	payload := "durable payload"
	first, _, err := Commit(context.Background(), "/backup/file.bin", strings.NewReader(payload), int64(len(payload)), time.Time{}, time.Time{}, "")
	if err != nil {
		t.Fatal(err)
	}
	completedAt := time.Now()
	if err := database.Model(&model.WebDAVWritebackObject{}).
		Where("id = ? AND generation = ?", first.ID, first.Generation).
		Updates(map[string]any{
			"state":                        StateCompleted,
			"completed_at":                 &completedAt,
			"provider_upload_completed_at": &completedAt,
		}).Error; err != nil {
		t.Fatal(err)
	}

	second, created, err := Commit(context.Background(), "/backup/file.bin", strings.NewReader(payload), int64(len(payload)), time.Time{}, time.Time{}, "")
	if err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := database.Model(&model.WebDAVWritebackObject{}).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if created || count != 1 || second.ID != first.ID || second.Generation != first.Generation || second.State != StateCompleted || second.ClientDuplicatePuts != 1 {
		t.Fatalf("duplicate PUT created=%v rows=%d id=%d generation=%d state=%q duplicates=%d; want one unchanged completed generation", created, count, second.ID, second.Generation, second.State, second.ClientDuplicatePuts)
	}
	entries, err := os.ReadDir(spoolDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("spool entries=%d after coalesced PUT, want only the original payload", len(entries))
	}

	differentPayload := "different payload"
	third, created, err := Commit(context.Background(), "/backup/file.bin", strings.NewReader(differentPayload), int64(len(differentPayload)), time.Time{}, time.Time{}, "")
	if err != nil {
		t.Fatal(err)
	}
	if created || third.ID != first.ID || third.Generation <= first.Generation || third.State != StateQueued || third.ClientDuplicatePuts != 0 {
		t.Fatalf("different content created=%v id=%d generation=%d state=%q duplicates=%d, want new generation on same row", created, third.ID, third.Generation, third.State, third.ClientDuplicatePuts)
	}
	entries, err = os.ReadDir(spoolDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("spool entries=%d after content change, want only the new generation payload", len(entries))
	}
}

func TestRebuildTaskReturnsBeforeBackgroundWorkCompletes(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})
	startResult := make(chan bool, 1)

	go func() {
		startResult <- startRebuildTask(func() (RebuildAllResult, error) {
			close(started)
			<-release
			close(finished)
			return RebuildAllResult{}, nil
		})
	}()

	select {
	case ok := <-startResult:
		if !ok {
			t.Fatal("rebuild did not start")
		}
	case <-time.After(time.Second):
		close(release)
		t.Fatal("rebuild start waited for background work")
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("background rebuild did not start")
	}
	if !Rebuilding() {
		close(release)
		t.Fatal("rebuild state cleared before background work completed")
	}
	close(release)
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("background rebuild did not finish")
	}
	deadline := time.Now().Add(time.Second)
	for Rebuilding() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if Rebuilding() {
		t.Fatal("rebuild state remained set after background work finished")
	}
}

func TestRebuildClearsWritebackRowsAndLocalSpoolOnly(t *testing.T) {
	oldConf := conf.Conf
	conf.Conf = &conf.Config{
		Database:        conf.Database{Type: "sqlite"},
		WebDAVWriteback: conf.WebDAVWritebackConfig{SpoolDir: t.TempDir()},
	}
	defer func() { conf.Conf = oldConf }()

	database, err := gorm.Open(sqlite.Open("file:"+t.Name()+"?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	db.Init(database)
	spoolDir := conf.Conf.WebDAVWriteback.SpoolDir

	rows := []any{
		&model.WebDAVWritebackObject{PathKey: "object", Path: "/backup/file.bin", Name: "file.bin", State: StateQueued},
		&model.WebDAVWritebackHistory{PathKey: "history", Path: "/backup/file.bin", Generation: 1},
		&model.WebDAVWritebackReceiveFence{PathKey: "fence", Path: "/backup/file.bin"},
		&model.WebDAVWritebackReceiveReservation{PathKey: "reservation", Sequence: 1},
		&model.WebDAVProviderOperation{OperationKey: "operation"},
	}
	for _, row := range rows {
		if err := database.Create(row).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"payload.data", "recv.part", "keep.txt"} {
		if err := os.WriteFile(filepath.Join(spoolDir, name), []byte("payload"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	providerFile := filepath.Join(t.TempDir(), "provider-content.data")
	if err := os.WriteFile(providerFile, []byte("115 content"), 0o600); err != nil {
		t.Fatal(err)
	}

	result, err := RebuildAllTaskRecords()
	if err != nil {
		t.Fatal(err)
	}
	if result.Tasks != 1 || result.History != 1 || result.ReceiveFences != 1 || result.Reservations != 1 || result.ProviderOperations != 1 {
		t.Fatalf("rebuild result=%+v, want one row removed from each writeback table", result)
	}
	for _, table := range []struct {
		name  string
		model any
	}{
		{"objects", &model.WebDAVWritebackObject{}},
		{"history", &model.WebDAVWritebackHistory{}},
		{"receive fences", &model.WebDAVWritebackReceiveFence{}},
		{"reservations", &model.WebDAVWritebackReceiveReservation{}},
		{"provider operations", &model.WebDAVProviderOperation{}},
	} {
		var count int64
		if err := database.Model(table.model).Count(&count).Error; err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Errorf("%s rows=%d after rebuild, want 0", table.name, count)
		}
	}
	for _, name := range []string{"payload.data", "recv.part"} {
		if _, err := os.Stat(filepath.Join(spoolDir, name)); !os.IsNotExist(err) {
			t.Errorf("local spool file %q remains after rebuild: %v", name, err)
		}
	}
	if _, err := os.Stat(filepath.Join(spoolDir, "keep.txt")); err != nil {
		t.Errorf("unrelated local file was touched: %v", err)
	}
	if _, err := os.Stat(providerFile); err != nil {
		t.Errorf("provider content was touched by rebuild: %v", err)
	}
}

func TestRestartRequiredFieldsOnlyCoversWorkerTopologyAndEnablement(t *testing.T) {
	before := validAdminConfig()
	after := before
	after.CloudSyncSettleMillis++
	after.MaxPendingSpoolMB++
	if got := RestartRequiredFields(before, after); len(got) != 0 {
		t.Fatalf("dynamic settings unexpectedly require restart: %v", got)
	}
	after.Workers++
	after.UploadWorkers++
	got := RestartRequiredFields(before, after)
	if len(got) != 2 || got[0] != "workers" || got[1] != "upload_workers" {
		t.Fatalf("restart fields=%v", got)
	}
}

func TestCompletedSpoolReleaseSafeProtectsActiveAndUnacceptedPayloads(t *testing.T) {
	now := time.Now()
	completed := now.Add(-time.Minute)
	uploaded := now.Add(-30 * time.Second)
	row := &model.WebDAVWritebackObject{
		Generation:                2,
		State:                     StateCompleted,
		SpoolPath:                 "/tmp/current.data",
		CompletedAt:               &completed,
		ProviderUploadCompletedAt: &uploaded,
		Size:                      1024,
	}
	if !completedSpoolReleaseSafe(row, now) {
		t.Fatal("provider-accepted idle completed cache should be releasable")
	}
	release := markSpoolActive(row.SpoolPath)
	if completedSpoolReleaseSafe(row, now) {
		t.Fatal("active worker spool must never be releasable")
	}
	release()
	row.ProviderUploadCompletedAt = nil
	if completedSpoolReleaseSafe(row, now) {
		t.Fatal("completed cache without provider PUT completion must never be releasable")
	}
}

func TestRecoveryLabelOnlyMarksActualRecovery(t *testing.T) {
	cases := []struct {
		row  model.WebDAVWritebackObject
		want string
	}{
		{
			row:  model.WebDAVWritebackObject{State: StateVerifying, CanonicalState: CanonicalStateAcked, LastError: "durable spool is missing after restart; checking provider"},
			want: "missing_spool",
		},
		{
			row:  model.WebDAVWritebackObject{State: StateVerifying, CanonicalState: CanonicalStateAcked, LastError: "resuming remote verification after interrupted upload"},
			want: "restart_recovery",
		},
		{
			row:  model.WebDAVWritebackObject{State: StateVerifying, CanonicalState: CanonicalStateAcked},
			want: "",
		},
		{
			row:  model.WebDAVWritebackObject{State: StateDeleted, CanonicalState: CanonicalStateDeleted, LastError: "exposing the loss so Cloud Sync can re-upload"},
			want: "needs_cloudsync_rehydrate",
		},
	}
	for _, tc := range cases {
		if got := RecoveryLabel(&tc.row); got != tc.want {
			t.Fatalf("RecoveryLabel(%q)=%q, want %q", tc.row.LastError, got, tc.want)
		}
	}
}

func TestGenericProviderAndStrongSHA1EvidenceDiffer(t *testing.T) {
	sha := strings.Repeat("a", 40)
	row := &model.WebDAVWritebackObject{Size: 4096, PayloadSHA1: sha}
	hashless := &model.Object{Size: row.Size}
	wrongHash := &model.Object{
		Size:     row.Size,
		HashInfo: utils.NewHashInfo(utils.SHA1, strings.Repeat("b", 40)),
	}

	if got := compareRemoteContent(row, hashless, false); got != remoteContentMatch {
		t.Fatalf("generic fresh listing should be allowed to converge by size/type, got %v", got)
	}
	if got := compareRemoteContent(row, hashless, true); got != remoteContentInconclusive {
		t.Fatalf("strong SHA1 provider without hash evidence must stay inconclusive, got %v", got)
	}
	if got := compareRemoteContent(row, wrongHash, true); got != remoteContentMismatch {
		t.Fatalf("strong SHA1 mismatch must be divergent, got %v", got)
	}
}

func TestRecoveryLabelWaitingCloudSyncReupload(t *testing.T) {
	row := &model.WebDAVWritebackObject{
		State:            StateWaitingCloudSyncReupload,
		CanonicalState:   CanonicalStateDeleted,
		ResolutionReason: ResolutionNeedsCloudSyncRehydrate,
	}
	if got := RecoveryLabel(row); got != "needs_cloudsync_rehydrate" {
		t.Fatalf("RecoveryLabel=%q, want needs_cloudsync_rehydrate", got)
	}
}
