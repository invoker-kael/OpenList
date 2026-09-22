package writeback

import (
	"strings"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
)

func validAdminConfig() conf.WebDAVWritebackConfig {
	return conf.WebDAVWritebackConfig{
		Enabled:                    true,
		SpoolDir:                   "/tmp/writeback",
		ReserveFreeSpaceMB:         1024,
		MaxPendingSpoolMB:          4096,
		IncomingReservationChunkMB: 64,
		AdmissionRetrySeconds:      5,
		Workers:                    4,
		UploadWorkers:              3,
		LargeUploadWorkers:         2,
		ProviderProbeWorkers:       2,
		CloudSyncSettleMillis:      2000,
		CloudSyncPlaceholderMillis: 10000,
		DirectoryGraceSeconds:      60,
		RetryInitialSeconds:        30,
		RetryMaxSeconds:            1800,
		VerifyIntervalSeconds:      5,
		VerifyAttempts:             60,
		CompletedCacheTTLMinutes:   30,
		CompletedRemoteProbeSeconds: 1800,
		ProviderSnapshotTTLSeconds: 600,
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

func TestCompletedSpoolReleaseSafeProtectsActiveAndUnverifiedPayloads(t *testing.T) {
	now := time.Now()
	completed := now.Add(-time.Minute)
	verified := now.Add(-30 * time.Second)
	row := &model.WebDAVWritebackObject{
		Generation:       2,
		RemoteGeneration: 2,
		State:            StateCompleted,
		SpoolPath:        "/tmp/current.data",
		CompletedAt:      &completed,
		RemoteVerifiedAt: &verified,
		Size:             1024,
	}
	if !completedSpoolReleaseSafe(row, now) {
		t.Fatal("verified idle completed cache should be releasable")
	}
	release := markSpoolActive(row.SpoolPath)
	if completedSpoolReleaseSafe(row, now) {
		t.Fatal("active worker spool must never be releasable")
	}
	release()
	row.RemoteVerifiedAt = nil
	if completedSpoolReleaseSafe(row, now) {
		t.Fatal("completed cache without provider evidence must never be releasable")
	}
}

func TestRecoveryLabelMakesMissingAndRestartStatesVisible(t *testing.T) {
	cases := []struct {
		row  model.WebDAVWritebackObject
		want string
	}{
		{
			row: model.WebDAVWritebackObject{State: StateVerifying, CanonicalState: CanonicalStateAcked, LastError: "durable spool is missing after restart; checking provider"},
			want: "missing_spool",
		},
		{
			row: model.WebDAVWritebackObject{State: StateVerifying, CanonicalState: CanonicalStateAcked, LastError: "resuming remote verification after interrupted upload"},
			want: "restart_recovery",
		},
		{
			row: model.WebDAVWritebackObject{State: StateVerifying, CanonicalState: CanonicalStateAcked},
			want: "waiting_provider_verification",
		},
		{
			row: model.WebDAVWritebackObject{State: StateDeleted, CanonicalState: CanonicalStateDeleted, LastError: "exposing the loss so Cloud Sync can re-upload"},
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
