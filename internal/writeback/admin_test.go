package writeback

import (
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

func TestCloudSyncReuploadFailureHidesCanonicalAndReleasesSpool(t *testing.T) {
	oldConf := conf.Conf
	conf.Conf = &conf.Config{Database: conf.Database{Type: "sqlite"}}
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

	if !requireCloudSyncReupload(&row, StateVerifying, ResolutionVerificationExhausted, "115 content was not confirmed", 3, true) {
		t.Fatal("confirmed upload failure should request a Cloud Sync re-upload")
	}
	obj, found, deleted, err := Canonical(row.Path)
	if err != nil {
		t.Fatal(err)
	}
	if !found || !deleted || obj != nil {
		t.Fatalf("canonical path after failure = (%v, %v, %v), want a tombstone", obj, found, deleted)
	}
	if _, err := os.Stat(spool); !os.IsNotExist(err) {
		t.Fatalf("released spool still exists or returned unexpected error: %v", err)
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
