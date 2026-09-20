package writeback

import (
	"context"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
)

func TestProviderOperationCopyUsesNative(t *testing.T) {
	if !ProviderOperationCopyUsesNative("/a/album", "/b/album", -1) {
		t.Fatal("recursive same-name cross-directory COPY should use native provider COPY")
	}
	if ProviderOperationCopyUsesNative("/a/album", "/b/renamed", -1) {
		t.Fatal("renamed COPY must use exact overlay-aware traversal")
	}
	if ProviderOperationCopyUsesNative("/a/album", "/b/album", 0) {
		t.Fatal("Depth:0 COPY must not use recursive provider COPY")
	}
	if ProviderOperationCopyUsesNative("/a/album", "/a/album", -1) {
		t.Fatal("same-directory path is not a native cross-directory COPY")
	}
}

func TestProviderOperationDestinationGenerationFence(t *testing.T) {
	op := &model.WebDAVProviderOperation{DestinationGeneration: 7}
	if providerOperationDestinationGenerationAdvanced(op, &model.WebDAVWritebackObject{Generation: 7}) {
		t.Fatal("unchanged destination generation must not count as reconciled")
	}
	if !providerOperationDestinationGenerationAdvanced(op, &model.WebDAVWritebackObject{Generation: 8}) {
		t.Fatal("advanced destination generation should prove metadata mutation")
	}
	if !providerOperationDestinationGenerationAdvanced(&model.WebDAVProviderOperation{}, &model.WebDAVWritebackObject{Generation: 1}) {
		t.Fatal("new destination without prior canonical generation should be accepted")
	}
}

func TestProviderOperationProtectsCanonicalPath(t *testing.T) {
	now := time.Now()
	ops := []model.WebDAVProviderOperation{
		{
			SourcePath:      "/src/album",
			DestinationPath: "/dst/album",
			State:           ProviderOperationStarted,
			CreatedAt:       now,
			UpdatedAt:       now,
		},
	}
	for _, p := range []string{"/src/album", "/src/album/a.jpg", "/dst/album", "/dst/album/sub/b.jpg"} {
		if !providerOperationProtectsCanonicalPath(ops, p, now) {
			t.Fatalf("path %q should be protected by active provider intent", p)
		}
	}
	if providerOperationProtectsCanonicalPath(ops, "/other/file.jpg", now) {
		t.Fatal("unrelated canonical path must not be protected")
	}
}

func TestProviderOperationRecoveryDue(t *testing.T) {
	now := time.Now()
	if !providerOperationRecoveryDue(&model.WebDAVProviderOperation{}, now) {
		t.Fatal("never-checked operation should be due immediately")
	}
	checked := now.Add(-providerOperationConfirmationDelay() / 2)
	op := &model.WebDAVProviderOperation{LastCheckedAt: &checked}
	if providerOperationRecoveryDue(op, now) {
		t.Fatal("recently checked operation should be throttled")
	}
	checked = now.Add(-providerOperationConfirmationDelay())
	if !providerOperationRecoveryDue(op, now) {
		t.Fatal("operation should be due after confirmation delay")
	}
}

func TestNotAppliedConfirmationStates(t *testing.T) {
	for _, state := range []string{ProviderOperationStarted, ProviderOperationFailed} {
		if !providerOperationNeedsNotAppliedConfirmation(state) {
			t.Fatalf("state %q must require separated not-applied confirmation", state)
		}
	}
	for _, state := range []string{ProviderOperationPrepared, ProviderOperationApplied, ""} {
		if providerOperationNeedsNotAppliedConfirmation(state) {
			t.Fatalf("state %q must not require not-applied confirmation", state)
		}
	}
}

func TestFailedProviderCopyCleanupIdentity(t *testing.T) {
	sha := strings.Repeat("a", 40)
	op := &model.WebDAVProviderOperation{
		FailureDestinationObserved: true,
		FailureDestinationObjectID: "copy-partial-123",
		FailureDestinationReady:    true,
		FailureDestinationIsDir:    false,
		FailureDestinationSize:     4096,
		FailureDestinationSHA1:     sha,
	}
	same := &model.Object{
		ID:       "copy-partial-123",
		Size:     4096,
		HashInfo: utils.NewHashInfo(utils.SHA1, sha),
	}
	replaced := &model.Object{
		ID:       "external-replacement-999",
		Size:     4096,
		HashInfo: utils.NewHashInfo(utils.SHA1, sha),
	}
	noID := &model.Object{
		Size:     4096,
		HashInfo: utils.NewHashInfo(utils.SHA1, sha),
	}

	if !failedProviderCopyCleanupAllowed(op, same, true) {
		t.Fatal("115 cleanup should allow the exact failure-time object snapshot")
	}
	if failedProviderCopyCleanupAllowed(op, replaced, true) {
		t.Fatal("115 cleanup must not delete a replacement object with a different ID")
	}
	if failedProviderCopyCleanupAllowed(op, noID, true) {
		t.Fatal("115 cleanup must not delete an object whose identity cannot be proven")
	}
	if !failedProviderCopyCleanupAllowed(op, noID, false) {
		t.Fatal("non-strict providers may fall back when object IDs are unavailable")
	}

	changedContent := &model.Object{
		ID:       "copy-partial-123",
		Size:     8192,
		HashInfo: utils.NewHashInfo(utils.SHA1, strings.Repeat("b", 40)),
	}
	if failedProviderCopyCleanupAllowed(op, changedContent, true) {
		t.Fatal("same object ID with changed content must not be auto-deleted")
	}

	op.FailureDestinationObserved = false
	op.FailureDestinationObjectID = ""
	op.FailureDestinationReady = false
	if failedProviderCopyCleanupAllowed(op, same, true) {
		t.Fatal("115 cleanup must not claim an object that was not observed at failure time")
	}
}

func TestFailedProviderCopyRecoveryDecision(t *testing.T) {
	if got := providerOperationRecoveryDecision(
		ProviderOperationCopy,
		ProviderOperationFailed,
		providerOperationRemoteMismatch,
		providerOperationRemoteInconclusive,
		true,
	); got != ProviderOperationNotApplied {
		t.Fatalf("failed partial directory COPY = %v, want retryable not-applied", got)
	}
	if got := providerOperationRecoveryDecision(
		ProviderOperationCopy,
		ProviderOperationFailed,
		providerOperationRemoteMatch,
		providerOperationRemoteInconclusive,
		true,
	); got != ProviderOperationRecovered {
		t.Fatalf("failed COPY with complete destination = %v, want recovered", got)
	}
}

func TestProviderOperationTreeDepth(t *testing.T) {
	if got := providerOperationTreeDepth(ProviderOperationMove, 0); got != -1 {
		t.Fatalf("MOVE tree depth = %d, want infinity", got)
	}
	if got := providerOperationTreeDepth(ProviderOperationCopy, 0); got != 0 {
		t.Fatalf("Depth:0 COPY tree depth = %d, want 0", got)
	}
	if got := providerOperationTreeDepth(ProviderOperationCopy, -1); got != -1 {
		t.Fatalf("recursive COPY tree depth = %d, want infinity", got)
	}
}

func TestProviderOperationTreeEvidenceRecoversStartedDirectoryCopy(t *testing.T) {
	op := &model.WebDAVProviderOperation{
		Method:           ProviderOperationCopy,
		State:            ProviderOperationStarted,
		SourceIsDir:      true,
		SourceTreeSHA256: strings.Repeat("a", 64),
	}
	if got := providerOperationRecoveryDecisionWithEvidence(
		op,
		providerOperationRemoteMatch,
		providerOperationRemoteInconclusive,
	); got != ProviderOperationRecovered {
		t.Fatalf("tree-verified STARTED directory COPY = %v, want recovered", got)
	}
	op.SourceTreeSHA256 = ""
	if got := providerOperationRecoveryDecisionWithEvidence(
		op,
		providerOperationRemoteMatch,
		providerOperationRemoteInconclusive,
	); got != ProviderOperationInconclusive {
		t.Fatalf("legacy directory COPY without tree evidence = %v, want inconclusive", got)
	}
}

func TestProviderOperationMaintenanceCadence(t *testing.T) {
	if providerOperationMaintenanceEvery != 5*time.Second {
		t.Fatalf("provider operation maintenance cadence = %v, want 5s", providerOperationMaintenanceEvery)
	}
}

func TestProviderOperationRecoveryIndex(t *testing.T) {
	typ := reflect.TypeOf(model.WebDAVProviderOperation{})
	state, _ := typ.FieldByName("State")
	lastChecked, _ := typ.FieldByName("LastCheckedAt")
	if !strings.Contains(state.Tag.Get("gorm"), "idx_webdav_provider_recovery") {
		t.Fatal("provider operation State must lead recovery index")
	}
	if !strings.Contains(lastChecked.Tag.Get("gorm"), "idx_webdav_provider_recovery") {
		t.Fatal("LastCheckedAt must participate in recovery index")
	}
}

func TestStartedDirectoryCopyWithoutFingerprintRemainsConservative(t *testing.T) {
	if got := providerOperationRecoveryDecision(
		ProviderOperationCopy,
		ProviderOperationStarted,
		providerOperationRemoteMatch,
		providerOperationRemoteInconclusive,
		true,
	); got != ProviderOperationInconclusive {
		t.Fatalf("legacy STARTED directory COPY = %v, want inconclusive without tree fingerprint", got)
	}
}

func TestProviderOperationKeyIsStableAndScoped(t *testing.T) {
	copyKey := providerOperationKey(ProviderOperationCopy, "/src/file.bin", "/dst/file.bin", -1)
	if copyKey != providerOperationKey("copy", "/src/file.bin", "/dst/file.bin", -1) {
		t.Fatal("provider operation key must normalize method casing")
	}
	if len(copyKey) != 64 {
		t.Fatalf("provider operation key length = %d, want 64", len(copyKey))
	}
	if copyKey == providerOperationKey(ProviderOperationMove, "/src/file.bin", "/dst/file.bin", -1) {
		t.Fatal("COPY and MOVE intents must not share a key")
	}
	if copyKey == providerOperationKey(ProviderOperationCopy, "/src/file.bin", "/dst/file.bin", 0) {
		t.Fatal("COPY depth must scope the durable intent")
	}
}

func TestProviderOperationConflictRules(t *testing.T) {
	existing := &model.WebDAVProviderOperation{
		OperationKey:    providerOperationKey(ProviderOperationCopy, "/src/a", "/dst/a", -1),
		Method:          ProviderOperationCopy,
		SourcePath:      "/src/a",
		DestinationPath: "/dst/a",
	}
	if !providerOperationsConflict(existing, ProviderOperationCopy, "/other", "/dst/a/child", -1) {
		t.Fatal("destination subtree overlap must be fenced")
	}
	if providerOperationsConflict(existing, ProviderOperationCopy, "/src/a", "/dst/b", -1) {
		t.Fatal("parallel COPY operations may share a stable source")
	}
	if !providerOperationsConflict(existing, ProviderOperationMove, "/src/a", "/dst/b", -1) {
		t.Fatal("MOVE must conflict with an unresolved operation touching its source")
	}

	same := providerOperationKey(ProviderOperationCopy, "/src/a", "/dst/a", -1)
	existing.OperationKey = same
	if providerOperationsConflict(existing, ProviderOperationCopy, "/src/a", "/dst/a", -1) {
		t.Fatal("the exact retry intent must not conflict with itself")
	}
}

func TestProviderOperationTouchesPath(t *testing.T) {
	op := &model.WebDAVProviderOperation{
		SourcePath:      "/source/album",
		DestinationPath: "/archive/album",
	}
	for _, p := range []string{"/source/album", "/source/album/photo.jpg", "/archive", "/archive/album/photo.jpg"} {
		if !providerOperationTouchesPath(op, p) {
			t.Fatalf("expected unresolved intent to fence %q", p)
		}
	}
	if providerOperationTouchesPath(op, "/unrelated/photo.jpg") {
		t.Fatal("unrelated path must not be fenced")
	}
}

func TestProviderOperationSourceMatchesCanonicalGeneration(t *testing.T) {
	op := &model.WebDAVProviderOperation{
		SourceIsDir:      false,
		SourceGeneration: 7,
		SourceETag:       "\"olwb-generation-7\"",
		SourceSize:       1024,
	}
	source := &CanonicalObject{
		Object: model.Object{Size: 1024},
		etag:   "\"olwb-generation-7\"",
	}
	if !ProviderOperationSourceMatches(op, source) {
		t.Fatal("matching canonical generation ETag should retain the intent")
	}
	source.etag = "\"olwb-generation-8\""
	if ProviderOperationSourceMatches(op, source) {
		t.Fatal("a newer canonical generation must supersede the old intent source")
	}
}

func TestProviderOperationSourceMatches(t *testing.T) {
	modTime := time.Date(2026, time.September, 20, 8, 0, 0, 0, time.UTC)
	sha := strings.Repeat("a", 40)
	op := &model.WebDAVProviderOperation{
		SourcePath:    "/src/file.bin",
		SourceSize:    4096,
		SourceSHA1:    sha,
		SourceModTime: modTime,
	}
	source := &model.Object{
		Name:     "file.bin",
		Size:     4096,
		Modified: modTime,
		HashInfo: utils.NewHashInfo(utils.SHA1, strings.ToUpper(sha)),
	}
	if !ProviderOperationSourceMatches(op, source) {
		t.Fatal("same provider source snapshot should match")
	}
	source.HashInfo = utils.NewHashInfo(utils.SHA1, strings.Repeat("b", 40))
	if ProviderOperationSourceMatches(op, source) {
		t.Fatal("changed source content must invalidate a stale provider intent")
	}
}

func TestProviderOperationPreparedExpiry(t *testing.T) {
	now := time.Now()
	old := now.Add(-providerOperationPreparedAbandonAfter - time.Second)
	fresh := now.Add(-providerOperationPreparedAbandonAfter + time.Second)

	if !providerOperationPreparedExpired(&model.WebDAVProviderOperation{
		State:     ProviderOperationPrepared,
		UpdatedAt: old,
	}, now) {
		t.Fatal("old PREPARED intent should be retired")
	}
	if providerOperationPreparedExpired(&model.WebDAVProviderOperation{
		State:     ProviderOperationPrepared,
		UpdatedAt: fresh,
	}, now) {
		t.Fatal("fresh PREPARED intent must not race the request that is starting it")
	}
	if providerOperationPreparedExpired(&model.WebDAVProviderOperation{
		State:     ProviderOperationStarted,
		UpdatedAt: old,
	}, now) {
		t.Fatal("STARTED intent must never be age-deleted without provider evidence")
	}
}

func TestProviderOperationNotAppliedConfirmationNeedsTwoObservations(t *testing.T) {
	oldConf := conf.Conf
	conf.Conf = &conf.Config{WebDAVWriteback: conf.WebDAVWritebackConfig{VerifyIntervalSeconds: 2}}
	defer func() { conf.Conf = oldConf }()

	if got := providerOperationConfirmationDelay(); got != 2*time.Second {
		t.Fatalf("provider operation confirmation delay = %v, want 2s", got)
	}
}

func TestProviderOperationRecoveryDecisionMoveRecreate(t *testing.T) {
	if got := providerOperationRecoveryDecision(
		ProviderOperationMove,
		ProviderOperationStarted,
		providerOperationRemoteMatch,
		providerOperationRemoteMismatch,
		false,
	); got != ProviderOperationRecovered {
		t.Fatalf("MOVE with recreated source and matching destination = %v, want recovered", got)
	}
	if got := providerOperationRecoveryDecision(
		ProviderOperationMove,
		ProviderOperationApplied,
		providerOperationRemoteMatch,
		providerOperationRemoteMatch,
		false,
	); got != ProviderOperationRecovered {
		t.Fatalf("APPLIED MOVE with lagging source visibility = %v, want recovered", got)
	}
}

func TestProviderOperationRecoveryDecision(t *testing.T) {
	if got := providerOperationRecoveryDecision(
		ProviderOperationMove,
		ProviderOperationStarted,
		providerOperationRemoteMatch,
		providerOperationRemoteAbsent,
		false,
	); got != ProviderOperationRecovered {
		t.Fatalf("completed MOVE recovery = %v, want recovered", got)
	}
	if got := providerOperationRecoveryDecision(
		ProviderOperationMove,
		ProviderOperationStarted,
		providerOperationRemoteMatch,
		providerOperationRemoteMatch,
		false,
	); got != ProviderOperationInconclusive {
		t.Fatalf("MOVE with both names visible = %v, want inconclusive", got)
	}
	if got := providerOperationRecoveryDecision(
		ProviderOperationMove,
		ProviderOperationStarted,
		providerOperationRemoteMismatch,
		providerOperationRemoteMismatch,
		false,
	); got != ProviderOperationNotApplied {
		t.Fatalf("MOVE with a different recreated source = %v, want not applied", got)
	}
	if got := providerOperationRecoveryDecision(
		ProviderOperationCopy,
		ProviderOperationStarted,
		providerOperationRemoteMatch,
		providerOperationRemoteInconclusive,
		false,
	); got != ProviderOperationRecovered {
		t.Fatalf("file COPY with matching destination = %v, want recovered", got)
	}
	if got := providerOperationRecoveryDecision(
		ProviderOperationCopy,
		ProviderOperationStarted,
		providerOperationRemoteMatch,
		providerOperationRemoteInconclusive,
		true,
	); got != ProviderOperationInconclusive {
		t.Fatalf("unmarked directory COPY = %v, want inconclusive", got)
	}
	if got := providerOperationRecoveryDecision(
		ProviderOperationCopy,
		ProviderOperationPrepared,
		providerOperationRemoteMatch,
		providerOperationRemoteInconclusive,
		false,
	); got != ProviderOperationNotApplied {
		t.Fatalf("prepared intent = %v, want not applied", got)
	}
}

func TestProviderOperationModelUsesSafeKey(t *testing.T) {
	typ := reflect.TypeOf(model.WebDAVProviderOperation{})
	field, ok := typ.FieldByName("OperationKey")
	if !ok {
		t.Fatal("provider operation model is missing OperationKey")
	}
	tag := field.Tag.Get("gorm")
	if !strings.Contains(tag, "size:64") || !strings.Contains(tag, "uniqueIndex") {
		t.Fatalf("operation key gorm tag = %q", tag)
	}
}

func TestPathKeyIsStableAndMySQLIndexSafe(t *testing.T) {
	key1 := pathKey("/encrypted/very/long/path/file.bin")
	key2 := pathKey("/encrypted/very/long/path/file.bin")
	if key1 != key2 {
		t.Fatal("path key is not stable")
	}
	if len(key1) != 64 {
		t.Fatalf("path key length = %d, want 64", len(key1))
	}
}

func TestCanonicalETagChangesWithGeneration(t *testing.T) {
	key := pathKey("/encrypted/file.bin")
	a := canonicalETag(key, 1, 1024)
	b := canonicalETag(key, 2, 1024)
	if a == b {
		t.Fatal("etag must change when generation changes")
	}
	if !strings.HasPrefix(a, "\"olwb-") {
		t.Fatalf("unexpected etag %q", a)
	}
}

func TestShouldRemoveStaleRemote(t *testing.T) {
	tests := []struct {
		name     string
		uploaded string
		current  *model.WebDAVWritebackObject
		want     bool
	}{
		{
			name:     "newer generation same path is kept",
			uploaded: "/encrypted/file.bin",
			current: &model.WebDAVWritebackObject{
				Path:       "/encrypted/file.bin",
				Generation: 2,
				State:      StateQueued,
			},
			want: false,
		},
		{
			name:     "deleted path is removed",
			uploaded: "/encrypted/file.bin",
			current: &model.WebDAVWritebackObject{
				Path:  "/encrypted/file.bin",
				State: StateDeleted,
			},
			want: true,
		},
		{
			name:     "moved path cleans old remote",
			uploaded: "/encrypted/.tmp-file",
			current: &model.WebDAVWritebackObject{
				Path:  "/encrypted/file.bin",
				State: StateQueued,
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldRemoveStaleRemote(tt.uploaded, tt.current); got != tt.want {
				t.Fatalf("shouldRemoveStaleRemote() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCompletedDivergenceConfirmationRequiresSeparatedObservation(t *testing.T) {
	now := time.Unix(100, 0)
	next := now.Add(5 * time.Second)
	if completedDivergenceConfirmed(0, nil, now) {
		t.Fatal("first divergence observation must not request fresh confirmation")
	}
	if completedDivergenceConfirmed(1, &next, now.Add(4*time.Second)) {
		t.Fatal("repeated scans inside the confirmation window must not request fresh confirmation")
	}
	if !completedDivergenceConfirmed(1, &next, now.Add(5*time.Second)) {
		t.Fatal("a separated observation should require a force-refreshed provider confirmation")
	}
}

func TestCompletedDivergenceConfirmationDelayUsesVerifyInterval(t *testing.T) {
	oldConf := conf.Conf
	conf.Conf = &conf.Config{WebDAVWriteback: conf.WebDAVWritebackConfig{VerifyIntervalSeconds: 7}}
	defer func() { conf.Conf = oldConf }()
	if got := completedDivergenceConfirmationDelay(); got != 7*time.Second {
		t.Fatalf("divergence confirmation delay=%v, want 7s", got)
	}
}

func TestCompletedDivergenceProbeStateFirstObservationOnlyArms(t *testing.T) {
	oldConf := conf.Conf
	conf.Conf = &conf.Config{WebDAVWriteback: conf.WebDAVWritebackConfig{VerifyIntervalSeconds: 7}}
	defer func() { conf.Conf = oldConf }()

	now := time.Unix(100, 0)
	claim, count, retryAt := completedDivergenceProbeState(0, nil, now)
	if claim {
		t.Fatal("first divergence observation must not claim a provider refresh")
	}
	if count != 1 {
		t.Fatalf("verify count=%d, want 1", count)
	}
	if retryAt == nil || !retryAt.Equal(now.Add(7*time.Second)) {
		t.Fatalf("retry_at=%v, want %v", retryAt, now.Add(7*time.Second))
	}
}

func TestCompletedDivergenceProbeStateClaimsAndRearms(t *testing.T) {
	oldConf := conf.Conf
	conf.Conf = &conf.Config{WebDAVWriteback: conf.WebDAVWritebackConfig{VerifyIntervalSeconds: 7}}
	defer func() { conf.Conf = oldConf }()

	now := time.Unix(200, 0)
	due := now.Add(-time.Second)
	claim, count, retryAt := completedDivergenceProbeState(1, &due, now)
	if !claim {
		t.Fatal("due divergence must claim the fresh provider confirmation")
	}
	if count != 2 {
		t.Fatalf("verify count=%d, want 2", count)
	}
	wantRetry := now.Add(7 * time.Second)
	if retryAt == nil || !retryAt.Equal(wantRetry) {
		t.Fatalf("retry_at=%v, want %v", retryAt, wantRetry)
	}

	claim, count, next := completedDivergenceProbeState(count, retryAt, now.Add(time.Second))
	if claim {
		t.Fatal("a second scan inside the claimed window must not refresh again")
	}
	if count != 2 {
		t.Fatalf("verify count changed inside claim window: %d", count)
	}
	if next != nil {
		t.Fatalf("claimed window unexpectedly requested a persistence update: %v", next)
	}
}

func TestShouldDropCanonicalAfterRemoteList(t *testing.T) {
	completedNoSpool := &model.WebDAVWritebackObject{
		State:     StateCompleted,
		SpoolPath: "",
	}
	if !shouldDropCanonicalAfterRemoteList(completedNoSpool, true, nil, time.Now(), false) {
		t.Fatal("missing remote object should be surfaced after a reliable listing")
	}
	if shouldDropCanonicalAfterRemoteList(completedNoSpool, false, nil, time.Now(), false) {
		t.Fatal("provider/listing failure must not drop canonical metadata")
	}
	if shouldDropCanonicalAfterRemoteList(completedNoSpool, true, &model.Object{Size: completedNoSpool.Size}, time.Now(), false) {
		t.Fatal("present remote object must keep canonical metadata")
	}

	completedCached := &model.WebDAVWritebackObject{
		State:     StateCompleted,
		SpoolPath: "/spool/object.data",
	}
	if shouldDropCanonicalAfterRemoteList(completedCached, true, nil, time.Now(), false) {
		t.Fatal("locally cached completed object must remain authoritative")
	}

	queued := &model.WebDAVWritebackObject{
		State:     StateQueued,
		SpoolPath: "/spool/object.data",
	}
	if shouldDropCanonicalAfterRemoteList(queued, true, nil, time.Now(), false) {
		t.Fatal("pending upload must remain visible even before provider listing catches up")
	}
}

func TestLockNullRetryAt(t *testing.T) {
	now := time.Unix(100, 0)
	expires := lockNullRetryAt(now, 30*time.Second)
	if expires == nil || !expires.Equal(now.Add(30*time.Second)) {
		t.Fatalf("finite lock-null expiry = %v", expires)
	}
	if expires := lockNullRetryAt(now, -1); expires != nil {
		t.Fatalf("infinite lock-null expiry = %v, want nil", expires)
	}
}

func TestLockNullCanonicalSurvivesMissingProviderList(t *testing.T) {
	row := &model.WebDAVWritebackObject{
		State:     StateLockNull,
		SpoolPath: "",
	}
	if shouldDropCanonicalAfterRemoteList(row, true, nil, time.Now(), true) {
		t.Fatal("lock-null resource must remain canonical while the lock is active")
	}
}

func TestReceivingPathReferenceCount(t *testing.T) {
	p := "/encrypted/placeholder.bin"
	_, release1 := beginReceiving(p)
	_, release2 := beginReceiving(p)
	if !isReceiving(p) {
		t.Fatal("path should be marked receiving while PUTs are active")
	}
	release1()
	if !isReceiving(p) {
		t.Fatal("path should stay receiving until all overlapping PUTs finish")
	}
	release2()
	if isReceiving(p) {
		t.Fatal("path should stop receiving after the final PUT finishes")
	}
}

func TestReceiveSequenceSuperseded(t *testing.T) {
	if receiveSequenceSuperseded(0, 1) {
		t.Fatal("first receive sequence cannot be superseded")
	}
	if receiveSequenceSuperseded(7, 7) {
		t.Fatal("the sequence that advanced the fence remains current")
	}
	if !receiveSequenceSuperseded(8, 7) {
		t.Fatal("older PUT finishing after a newer committed sequence must be superseded")
	}
}

func TestReceiveFenceSchemaKeepsPathOrderingDurable(t *testing.T) {
	typ := reflect.TypeOf(model.WebDAVWritebackReceiveFence{})
	pathKeyField, ok := typ.FieldByName("PathKey")
	if !ok || !strings.Contains(pathKeyField.Tag.Get("gorm"), "uniqueIndex") {
		t.Fatal("receive fence PathKey must be unique so one MySQL row serializes each WebDAV path")
	}
	next, ok := typ.FieldByName("NextSequence")
	if !ok || next.Type.Kind() != reflect.Uint64 {
		t.Fatal("receive fence NextSequence must be uint64")
	}
	last, ok := typ.FieldByName("LastCommittedSequence")
	if !ok || last.Type.Kind() != reflect.Uint64 {
		t.Fatal("receive fence LastCommittedSequence must be uint64")
	}
}

func TestCanonicalDirectoryObject(t *testing.T) {
	row := &model.WebDAVWritebackObject{
		ID:         7,
		Path:       "/encrypted/folder",
		Name:       "folder",
		IsDir:      true,
		Generation: 1,
		ETag:       canonicalETag(pathKey("/encrypted/folder"), 1, 0),
	}
	obj := toObject(row)
	if !obj.IsDir() {
		t.Fatal("canonical directory must be exposed as a WebDAV collection")
	}
	if obj.GetName() != "folder" {
		t.Fatalf("canonical directory name = %q, want folder", obj.GetName())
	}
}

func TestDirectoryShadowGrace(t *testing.T) {
	oldConf := conf.Conf
	conf.Conf = &conf.Config{
		WebDAVWriteback: conf.WebDAVWritebackConfig{
			DirectoryGraceSeconds: 60,
		},
	}
	defer func() { conf.Conf = oldConf }()

	now := time.Now()
	completed := now.Add(-30 * time.Second)
	row := &model.WebDAVWritebackObject{
		IsDir:       true,
		State:       StateCompleted,
		CompletedAt: &completed,
	}
	if directoryShadowExpired(row, now) {
		t.Fatal("directory shadow must remain visible during provider consistency grace")
	}

	completed = now.Add(-61 * time.Second)
	row.CompletedAt = &completed
	if !directoryShadowExpired(row, now) {
		t.Fatal("directory shadow should expire after provider consistency grace")
	}

	row.State = StateQueued
	if directoryShadowExpired(row, now) {
		t.Fatal("pending directory shadow must never expire while creation is queued")
	}
}

func TestCloudSyncPlaceholderSettleDelay(t *testing.T) {
	oldConf := conf.Conf
	conf.Conf = &conf.Config{
		WebDAVWriteback: conf.WebDAVWritebackConfig{
			CloudSyncSettleMillis:      2000,
			CloudSyncPlaceholderMillis: 10000,
		},
	}
	defer func() { conf.Conf = oldConf }()

	if got := cloudSyncSettleDelay(1024); got != 2*time.Second {
		t.Fatalf("non-empty settle delay = %v, want 2s", got)
	}
	if got := cloudSyncSettleDelay(0); got != 10*time.Second {
		t.Fatalf("zero-byte placeholder settle delay = %v, want 10s", got)
	}
}

func TestIsPathOrDescendant(t *testing.T) {
	tests := []struct {
		candidate string
		root      string
		want      bool
	}{
		{candidate: "/a", root: "/a", want: true},
		{candidate: "/a/b", root: "/a", want: true},
		{candidate: "/a/b/c", root: "/a", want: true},
		{candidate: "/ab", root: "/a", want: false},
		{candidate: "/a_b", root: "/a", want: false},
		{candidate: "/a%2Fb", root: "/a", want: false},
		{candidate: "/a/../b", root: "/a", want: false},
		{candidate: "/", root: "/", want: true},
		{candidate: "/a", root: "/", want: true},
		{candidate: "/a/b", root: "/", want: true},
	}
	for _, tt := range tests {
		t.Run(tt.candidate+" under "+tt.root, func(t *testing.T) {
			if got := isPathOrDescendant(tt.candidate, tt.root); got != tt.want {
				t.Fatalf("isPathOrDescendant(%q, %q) = %v, want %v", tt.candidate, tt.root, got, tt.want)
			}
		})
	}
}

func TestTombstoneMovedSource(t *testing.T) {
	completed := time.Now().Add(-time.Minute)
	row := &model.WebDAVWritebackObject{
		Path:        "/encrypted/source.bin",
		Generation:  3,
		State:       StateCompleted,
		SpoolPath:   "/spool/source.data",
		CleanupPath: "/encrypted/old-source.bin",
		LastError:   "old error",
		RetryCount:  4,
		VerifyCount: 2,
		CompletedAt: &completed,
	}
	now := time.Now()
	tombstoneMovedSource(row, now)

	if row.Path != "/encrypted/source.bin" {
		t.Fatalf("move tombstone changed source path to %q", row.Path)
	}
	if row.Generation != 4 {
		t.Fatalf("generation = %d, want 4", row.Generation)
	}
	if row.State != StateDeleted {
		t.Fatalf("state = %q, want %q", row.State, StateDeleted)
	}
	if row.SpoolPath != "" || row.CleanupPath != "" {
		t.Fatalf("tombstone retained local cleanup state: spool=%q cleanup=%q", row.SpoolPath, row.CleanupPath)
	}
	if row.RetryAt == nil || !row.RetryAt.Equal(now) {
		t.Fatal("move tombstone must be immediately eligible for provider cleanup")
	}
	if row.CompletedAt != nil || row.RetryCount != 0 || row.VerifyCount != 0 || row.LastError != "" {
		t.Fatal("move tombstone did not reset completion/retry state")
	}
}

func TestCanonicalParentBlocksChild(t *testing.T) {
	if canonicalParentBlocksChild(nil) {
		t.Fatal("missing canonical parent should defer to provider lookup")
	}
	if canonicalParentBlocksChild(&model.WebDAVWritebackObject{IsDir: true, State: StateQueued}) {
		t.Fatal("pending canonical directory should wait, not be treated as invalid")
	}
	if canonicalParentBlocksChild(&model.WebDAVWritebackObject{IsDir: true, State: StateCompleted}) {
		t.Fatal("completed canonical directory should allow child dispatch")
	}
	if !canonicalParentBlocksChild(&model.WebDAVWritebackObject{IsDir: true, State: StateDeleted}) {
		t.Fatal("deleted canonical parent must block child dispatch")
	}
	if !canonicalParentBlocksChild(&model.WebDAVWritebackObject{IsDir: false, State: StateCompleted}) {
		t.Fatal("canonical file parent must block child dispatch")
	}
}

func TestPendingDirectoryMoveLocalAuthority(t *testing.T) {
	oldConf := conf.Conf
	conf.Conf = &conf.Config{
		WebDAVWriteback: conf.WebDAVWritebackConfig{DirectoryGraceSeconds: 60},
	}
	defer func() { conf.Conf = oldConf }()

	now := time.Now()
	root := model.WebDAVWritebackObject{
		Path:  "/encrypted/album",
		IsDir: true,
		State: StateQueued,
	}
	rows := []model.WebDAVWritebackObject{root}
	for i := 0; i < 20; i++ {
		rows = append(rows, model.WebDAVWritebackObject{
			Path:      "/encrypted/album/file",
			State:     StateQueued,
			SpoolPath: "/spool/file.data",
		})
	}
	if !pendingDirectoryMoveLocallyAuthoritative(&rows[0], rows, now) {
		t.Fatal("queued directory with 20 locally spooled files should be movable without provider visibility")
	}

	rows[10].SpoolPath = ""
	if pendingDirectoryMoveLocallyAuthoritative(&rows[0], rows, now) {
		t.Fatal("one live file without a spool must force provider fallback")
	}
	rows[10].State = StateDeleted
	if !pendingDirectoryMoveLocallyAuthoritative(&rows[0], rows, now) {
		t.Fatal("deleted descendants do not need a payload for a pending directory move")
	}

	completed := now.Add(-30 * time.Second)
	rows[0].State = StateCompleted
	rows[0].CompletedAt = &completed
	if !pendingDirectoryMoveLocallyAuthoritative(&rows[0], rows, now) {
		t.Fatal("completed directory inside consistency grace should remain locally authoritative")
	}

	completed = now.Add(-61 * time.Second)
	rows[0].CompletedAt = &completed
	if pendingDirectoryMoveLocallyAuthoritative(&rows[0], rows, now) {
		t.Fatal("old completed directory should fall back to provider MOVE")
	}

	rows[0].CompletedAt = nil
	if pendingDirectoryMoveLocallyAuthoritative(&rows[0], rows, now) {
		t.Fatal("completed directory without completion evidence must not use local fast MOVE")
	}
}

func TestSpoolAdmissionRequired(t *testing.T) {
	required, ok := spoolAdmissionRequired(20, 30, 40)
	if !ok || required != 90 {
		t.Fatalf("required=%d ok=%v, want 90 true", required, ok)
	}
	if _, ok := spoolAdmissionRequired(^uint64(0)-5, 10, 0); ok {
		t.Fatal("overflowing admission requirement must be rejected")
	}
}

func TestIncomingReservationConsumesWithoutLeak(t *testing.T) {
	spaceMu.Lock()
	oldReserved := reservedIncoming
	reservedIncoming = 1024
	spaceMu.Unlock()
	defer func() {
		spaceMu.Lock()
		reservedIncoming = oldReserved
		spaceMu.Unlock()
	}()

	r := &incomingReservation{remaining: 1024}
	r.consume(256)
	if r.remaining != 768 {
		t.Fatalf("remaining=%d, want 768", r.remaining)
	}
	spaceMu.Lock()
	got := reservedIncoming
	spaceMu.Unlock()
	if got != 768 {
		t.Fatalf("global reservation=%d, want 768", got)
	}
	r.release()
	spaceMu.Lock()
	got = reservedIncoming
	spaceMu.Unlock()
	if got != 0 {
		t.Fatalf("global reservation leaked %d bytes", got)
	}
	r.release()
	spaceMu.Lock()
	got = reservedIncoming
	spaceMu.Unlock()
	if got != 0 {
		t.Fatalf("double release changed reservation to %d", got)
	}
}

func TestUnknownUploadReservationCanGrowAndRelease(t *testing.T) {
	oldConf := conf.Conf
	tmp := t.TempDir()
	conf.Conf = &conf.Config{
		WebDAVWriteback: conf.WebDAVWritebackConfig{
			SpoolDir:                   tmp,
			ReserveFreeSpaceMB:         0,
			IncomingReservationChunkMB: 1,
		},
	}
	defer func() { conf.Conf = oldConf }()

	spaceMu.Lock()
	oldReserved := reservedIncoming
	reservedIncoming = 0
	spaceMu.Unlock()
	defer func() {
		spaceMu.Lock()
		reservedIncoming = oldReserved
		spaceMu.Unlock()
	}()

	r, err := reserveIncomingBytes(-1)
	if err != nil {
		t.Fatal(err)
	}
	chunk := uint64(utils.MB)
	if r.remaining != chunk {
		t.Fatalf("initial rolling reservation=%d, want %d", r.remaining, chunk)
	}
	r.consume(chunk)
	if r.remaining != 0 {
		t.Fatalf("remaining after consume=%d, want 0", r.remaining)
	}
	if err := r.ensureForWrite(1); err != nil {
		t.Fatal(err)
	}
	if r.remaining != chunk {
		t.Fatalf("replenished reservation=%d, want %d", r.remaining, chunk)
	}
	r.release()
	spaceMu.Lock()
	got := reservedIncoming
	spaceMu.Unlock()
	if got != 0 {
		t.Fatalf("rolling reservation leaked %d bytes", got)
	}
}

func TestZeroByteUploadStillHonorsFreeSpaceFloor(t *testing.T) {
	oldConf := conf.Conf
	conf.Conf = &conf.Config{
		WebDAVWriteback: conf.WebDAVWritebackConfig{
			SpoolDir:           t.TempDir(),
			ReserveFreeSpaceMB: ^uint64(0),
		},
	}
	defer func() { conf.Conf = oldConf }()

	r, err := reserveIncomingBytes(0)
	if r != nil {
		r.release()
	}
	if !errors.Is(err, ErrSpoolCapacity) {
		t.Fatalf("zero-byte admission error=%v, want ErrSpoolCapacity", err)
	}
}

func TestMegabytesToBytesSaturates(t *testing.T) {
	if got := megabytesToBytes(^uint64(0)); got != ^uint64(0) {
		t.Fatalf("overflowing MB conversion=%d, want max uint64", got)
	}
}

func TestUnknownUploadReservationUsesRollingChunk(t *testing.T) {
	oldConf := conf.Conf
	conf.Conf = &conf.Config{
		WebDAVWriteback: conf.WebDAVWritebackConfig{
			IncomingReservationChunkMB: 8,
		},
	}
	defer func() { conf.Conf = oldConf }()
	if got := incomingReservationChunkBytes(); got != 8*uint64(utils.MB) {
		t.Fatalf("rolling reservation chunk=%d, want %d", got, 8*uint64(utils.MB))
	}
}

func TestSpoolCapacityErrorSupportsErrorsIs(t *testing.T) {
	err := &SpoolCapacityError{Free: 1, Required: 2}
	if !errors.Is(err, ErrSpoolCapacity) {
		t.Fatal("SpoolCapacityError must unwrap to ErrSpoolCapacity")
	}
}

func TestCopyToSpoolRejectsDeclaredSizeOverrunBeforeWrite(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "spool-*.data")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	reservation := &incomingReservation{remaining: 3}
	spaceMu.Lock()
	oldReserved := reservedIncoming
	reservedIncoming = 3
	spaceMu.Unlock()
	defer func() {
		reservation.release()
		spaceMu.Lock()
		reservedIncoming = oldReserved
		spaceMu.Unlock()
	}()

	size, _, err := copyToSpool(f, strings.NewReader("four"), 3, reservation)
	if err == nil {
		t.Fatal("declared-size overrun should fail")
	}
	if size != 0 {
		t.Fatalf("overrun wrote %d bytes before rejection, want 0", size)
	}
	info, statErr := f.Stat()
	if statErr != nil {
		t.Fatal(statErr)
	}
	if info.Size() != 0 {
		t.Fatalf("spool contains %d bytes after pre-write overrun rejection", info.Size())
	}
}

func TestDurableCommitContextSurvivesClientCancellation(t *testing.T) {
	type contextKey string
	const key contextKey = "request-value"

	parent, cancel := context.WithCancel(context.WithValue(context.Background(), key, "preserved"))
	cancel()
	if parent.Err() == nil {
		t.Fatal("test parent context should be canceled")
	}

	commitCtx := durableCommitContext(parent)
	if err := commitCtx.Err(); err != nil {
		t.Fatalf("durable commit context inherited request cancellation: %v", err)
	}
	if got := commitCtx.Value(key); got != "preserved" {
		t.Fatalf("request-scoped value was not preserved: %v", got)
	}
	if _, ok := commitCtx.Deadline(); ok {
		t.Fatal("durable commit context must not inherit the HTTP request deadline")
	}
}

type terminalErrorReader struct {
	payload []byte
	err     error
	sent    bool
}

func (r *terminalErrorReader) Read(p []byte) (int, error) {
	if !r.sent {
		r.sent = true
		n := copy(p, r.payload)
		return n, nil
	}
	return 0, r.err
}

func TestCopyToSpoolAcceptsLateCancellationAfterDeclaredLength(t *testing.T) {
	payload := []byte("complete-large-put")
	f, err := os.CreateTemp(t.TempDir(), "spool-*.data")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	reservation := &incomingReservation{remaining: uint64(len(payload))}
	spaceMu.Lock()
	reservedIncoming += uint64(len(payload))
	spaceMu.Unlock()
	defer reservation.release()

	reader := &terminalErrorReader{payload: payload, err: context.Canceled}
	size, sha1sum, err := copyToSpool(f, reader, int64(len(payload)), reservation)
	if err != nil {
		t.Fatalf("late cancellation after the declared length should not discard a complete PUT: %v", err)
	}
	if size != int64(len(payload)) {
		t.Fatalf("spooled size = %d, want %d", size, len(payload))
	}
	if want := utils.HashData(utils.SHA1, payload); sha1sum != want {
		t.Fatalf("payload sha1 = %q, want %q", sha1sum, want)
	}
}

func TestCopyToSpoolRejectsCancellationBeforeDeclaredLength(t *testing.T) {
	payload := []byte("short")
	expected := int64(len(payload) + 1)
	f, err := os.CreateTemp(t.TempDir(), "spool-*.data")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	reservation := &incomingReservation{remaining: uint64(expected)}
	spaceMu.Lock()
	reservedIncoming += uint64(expected)
	spaceMu.Unlock()
	defer reservation.release()

	reader := &terminalErrorReader{payload: payload, err: context.Canceled}
	if _, _, err := copyToSpool(f, reader, expected, reservation); !errors.Is(err, context.Canceled) {
		t.Fatalf("incomplete canceled PUT error = %v, want context.Canceled", err)
	}
}

func TestCopyToSpoolComputesPayloadSHA1(t *testing.T) {
	payload := "cloud-sync-encrypted-payload"
	f, err := os.CreateTemp(t.TempDir(), "spool-*.data")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	reservation := &incomingReservation{remaining: uint64(len(payload))}
	spaceMu.Lock()
	reservedIncoming += uint64(len(payload))
	spaceMu.Unlock()
	defer reservation.release()
	size, sha1sum, err := copyToSpool(f, strings.NewReader(payload), int64(len(payload)), reservation)
	if err != nil {
		t.Fatal(err)
	}
	if size != int64(len(payload)) {
		t.Fatalf("spooled size = %d, want %d", size, len(payload))
	}
	want := utils.HashData(utils.SHA1, []byte(payload))
	if sha1sum != want {
		t.Fatalf("payload sha1 = %q, want %q", sha1sum, want)
	}
}

func TestCanonicalShadowGraceProtectsFreshCompletedFile(t *testing.T) {
	oldConf := conf.Conf
	conf.Conf = &conf.Config{
		WebDAVWriteback: conf.WebDAVWritebackConfig{DirectoryGraceSeconds: 60},
	}
	defer func() { conf.Conf = oldConf }()

	now := time.Now()
	completed := now.Add(-30 * time.Second)
	row := &model.WebDAVWritebackObject{
		State:       StateCompleted,
		CompletedAt: &completed,
	}
	if !canonicalShadowInGrace(row, now) {
		t.Fatal("fresh completed file metadata should stay canonical during provider consistency grace")
	}
	if shouldDropCanonicalAfterRemoteList(row, true, nil, now, false) {
		t.Fatal("fresh completed file must not be dropped on a transient provider miss")
	}

	completed = now.Add(-61 * time.Second)
	row.CompletedAt = &completed
	if canonicalShadowInGrace(row, now) {
		t.Fatal("completed file metadata should leave grace after the configured window")
	}
	if !shouldDropCanonicalAfterRemoteList(row, true, nil, now, false) {
		t.Fatal("expired completed file should expose a confirmed provider loss")
	}
}

func TestProviderOverwriteQuiescent(t *testing.T) {
	if !providerOverwriteQuiescent([]model.WebDAVWritebackObject{
		{State: StateCompleted},
		{State: StateCompleted},
	}) {
		t.Fatal("all-completed destination tree should be safe for provider overwrite")
	}
	for _, state := range []string{StateQueued, StateUploading, StateVerifying, StateFailed, StateDeleted} {
		rows := []model.WebDAVWritebackObject{{State: StateCompleted}, {State: state}}
		if providerOverwriteQuiescent(rows) {
			t.Fatalf("destination state %q must block provider fallback overwrite", state)
		}
	}
}

func TestPendingDirectoryCopyLocalAuthority(t *testing.T) {
	oldConf := conf.Conf
	conf.Conf = &conf.Config{
		WebDAVWriteback: conf.WebDAVWritebackConfig{DirectoryGraceSeconds: 60},
	}
	defer func() { conf.Conf = oldConf }()

	now := time.Now()
	completed := now.Add(-2 * time.Minute)
	root := model.WebDAVWritebackObject{
		Path:        "/encrypted/album",
		IsDir:       true,
		State:       StateCompleted,
		CompletedAt: &completed,
	}
	rows := []model.WebDAVWritebackObject{
		root,
		{
			Path:      "/encrypted/album/file.bin",
			State:     StateCompleted,
			SpoolPath: "",
		},
	}

	if pendingDirectoryCopyLocallyAuthoritative(&rows[0], rows, now, false) {
		t.Fatal("expired completed collection must reconcile provider before even a Depth: 0 COPY")
	}
	if pendingDirectoryCopyLocallyAuthoritative(&rows[0], rows, now, true) {
		t.Fatal("recursive directory COPY must fall back when a live file has no local spool")
	}

	rows[0].State = StateQueued
	rows[0].CompletedAt = nil
	rows[1].State = StateQueued
	rows[1].SpoolPath = "/spool/file.data"
	if !pendingDirectoryCopyLocallyAuthoritative(&rows[0], rows, now, true) {
		t.Fatal("recursive pending directory with all live payloads spooled should use the local fast path")
	}
}

func TestSetProviderCompletedRoot(t *testing.T) {
	now := time.Now()
	source := &model.Object{
		Name:     "source.bin",
		Size:     1234,
		Modified: now.Add(-time.Hour),
		Ctime:    now.Add(-2 * time.Hour),
		HashInfo: utils.NewHashInfo(utils.SHA1, strings.Repeat("a", 40)),
	}
	row := &model.WebDAVWritebackObject{Generation: 7, State: StateQueued, SpoolPath: "/old.data"}
	setProviderCompletedRoot(row, "/dst/renamed.bin", source, now)

	if row.Generation != 8 || row.Path != "/dst/renamed.bin" || row.Name != "renamed.bin" {
		t.Fatalf("unexpected provider root identity: generation=%d path=%q name=%q", row.Generation, row.Path, row.Name)
	}
	if row.State != StateCompleted || row.SpoolPath != "" || row.CompletedAt == nil || !row.CompletedAt.Equal(now) {
		t.Fatal("provider root was not converted to a fresh completed canonical shadow")
	}
	if row.PayloadSHA1 != strings.Repeat("a", 40) {
		t.Fatalf("provider root sha1 = %q", row.PayloadSHA1)
	}
}

func TestCloudSyncRemoteVerificationEvidencePolicy(t *testing.T) {
	sha := strings.Repeat("a", 40)
	row := &model.WebDAVWritebackObject{
		Size:        8192,
		PayloadSHA1: sha,
	}
	matching := &model.Object{
		Size:     row.Size,
		HashInfo: utils.NewHashInfo(utils.SHA1, sha),
	}
	missingHash := &model.Object{Size: row.Size}
	wrongHash := &model.Object{
		Size:     row.Size,
		HashInfo: utils.NewHashInfo(utils.SHA1, strings.Repeat("b", 40)),
	}

	tests := []struct {
		name        string
		remote      model.Obj
		err         error
		requireHash bool
		want        remoteVerificationState
	}{
		{name: "exact 115 object converges", remote: matching, requireHash: true, want: remoteVerificationMatch},
		{name: "115 missing sha1 stays inconclusive", remote: missingHash, requireHash: true, want: remoteVerificationInconclusive},
		{name: "provider error stays inconclusive", err: errors.New("temporary provider outage"), requireHash: true, want: remoteVerificationInconclusive},
		{name: "fresh confirmed absence is divergent", remote: nil, err: nil, requireHash: true, want: remoteVerificationDivergent},
		{name: "wrong sha1 is divergent", remote: wrongHash, requireHash: true, want: remoteVerificationDivergent},
		{name: "hashless provider can converge by size", remote: missingHash, requireHash: false, want: remoteVerificationMatch},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyRemoteVerification(row, tt.remote, tt.err, tt.requireHash); got != tt.want {
				t.Fatalf("classifyRemoteVerification()=%v, want %v", got, tt.want)
			}
		})
	}
}

func TestCompletedRemoteVerificationFresh(t *testing.T) {
	oldConf := conf.Conf
	conf.Conf = &conf.Config{
		WebDAVWriteback: conf.WebDAVWritebackConfig{VerifyIntervalSeconds: 10},
	}
	defer func() { conf.Conf = oldConf }()

	now := time.Now()
	verified := now.Add(-5 * time.Second)
	row := &model.WebDAVWritebackObject{
		State:            StateCompleted,
		Generation:       7,
		RemoteGeneration: 7,
		RemoteVerifiedAt: &verified,
	}
	if !completedRemoteVerificationFresh(row, now) {
		t.Fatal("recent generation-scoped provider verification should suppress an immediate direct provider lookup")
	}

	stale := now.Add(-11 * time.Second)
	row.RemoteVerifiedAt = &stale
	if completedRemoteVerificationFresh(row, now) {
		t.Fatal("verification older than the configured interval must be rechecked")
	}

	row.RemoteVerifiedAt = &verified
	row.RemoteGeneration = 6
	if completedRemoteVerificationFresh(row, now) {
		t.Fatal("verification evidence from an older generation must not suppress reconciliation")
	}

	row.RemoteGeneration = 7
	retryAt := now.Add(time.Second)
	row.RetryAt = &retryAt
	if completedRemoteVerificationFresh(row, now) {
		t.Fatal("pending divergence retry must bypass the verification cooldown")
	}

	row.RetryAt = nil
	row.SpoolPath = "/spool/current.data"
	if completedRemoteVerificationFresh(row, now) {
		t.Fatal("locally cached completed payloads do not use direct provider reconciliation cooldown")
	}
}

func TestCloudSyncInconclusiveVerificationNeverConsumesReuploadBudget(t *testing.T) {
	count := 0
	for i := 0; i < 100; i++ {
		var retry bool
		count, retry = advanceRemoteVerification(remoteVerificationInconclusive, count, 3)
		if retry {
			t.Fatalf("inconclusive observation %d unexpectedly requested provider reupload", i+1)
		}
	}
	if count != 0 {
		t.Fatalf("inconclusive observations consumed divergence budget: %d", count)
	}

	for i := 0; i < 2; i++ {
		var retry bool
		count, retry = advanceRemoteVerification(remoteVerificationDivergent, count, 3)
		if retry {
			t.Fatalf("divergence %d retried before configured threshold", i+1)
		}
	}
	count, retry := advanceRemoteVerification(remoteVerificationDivergent, count, 3)
	if !retry || count != 3 {
		t.Fatalf("three conclusive divergences should permit one repair upload: count=%d retry=%v", count, retry)
	}
}

func TestLarge115VerificationWindowUsesRetryMax(t *testing.T) {
	oldConf := conf.Conf
	conf.Conf = &conf.Config{
		WebDAVWriteback: conf.WebDAVWritebackConfig{
			VerifyIntervalSeconds: 5,
			VerifyAttempts:        60,
			RetryMaxSeconds:       1800,
		},
	}
	defer func() { conf.Conf = oldConf }()

	small := &model.WebDAVWritebackObject{Size: open115MultipartChunkSize}
	if got := verificationAttemptsFor(small, true); got != 60 {
		t.Fatalf("single-part-sized verification attempts = %d, want 60", got)
	}

	large := &model.WebDAVWritebackObject{Size: open115MultipartChunkSize + 1}
	if got := verificationAttemptsFor(large, true); got != 360 {
		t.Fatalf("large 115 verification attempts = %d, want 360", got)
	}
	if got := verificationAttemptsFor(large, false); got != 60 {
		t.Fatalf("non-115 large verification attempts = %d, want 60", got)
	}

	conf.Conf.WebDAVWriteback.VerifyAttempts = 500
	if got := verificationAttemptsFor(large, true); got != 500 {
		t.Fatalf("explicit longer verification budget = %d, want 500", got)
	}
}

func TestLarge115RepairUploadIsBounded(t *testing.T) {
	large := &model.WebDAVWritebackObject{
		Size:       open115MultipartChunkSize + 1,
		RetryCount: 0,
	}
	if suppressRepeatedLargeProviderRepair(large, true) {
		t.Fatal("initial large 115 divergence should still permit one repair upload")
	}
	large.RetryCount = 1
	if !suppressRepeatedLargeProviderRepair(large, true) {
		t.Fatal("large 115 generation must stop automatic provider reuploads after one repair")
	}
	if suppressRepeatedLargeProviderRepair(&model.WebDAVWritebackObject{Size: open115MultipartChunkSize, RetryCount: 2}, true) {
		t.Fatal("single-part-sized files should retain the normal retry policy")
	}
	if suppressRepeatedLargeProviderRepair(large, false) {
		t.Fatal("non-115 providers should retain the normal retry policy")
	}
}

func TestRepeatedLargeProviderVerifyDelayUsesRetryMaxFloor(t *testing.T) {
	oldConf := conf.Conf
	conf.Conf = &conf.Config{
		WebDAVWriteback: conf.WebDAVWritebackConfig{
			RetryInitialSeconds: 30,
			RetryMaxSeconds:     1800,
		},
	}
	defer func() { conf.Conf = oldConf }()

	row := &model.WebDAVWritebackObject{RetryCount: 1}
	if got := repeatedLargeProviderVerifyDelay(row); got != 30*time.Minute {
		t.Fatalf("large provider verification hold = %v, want 30m", got)
	}
}

func TestRemoteVerificationInconclusiveDelayReducesProviderPolling(t *testing.T) {
	oldConf := conf.Conf
	defer func() { conf.Conf = oldConf }()

	conf.Conf = &conf.Config{WebDAVWriteback: conf.WebDAVWritebackConfig{
		VerifyIntervalSeconds: 5,
		RetryInitialSeconds:   30,
	}}
	if got := remoteVerificationInconclusiveDelay(); got != 30*time.Second {
		t.Fatalf("default inconclusive delay=%v, want 30s", got)
	}

	conf.Conf.WebDAVWriteback.VerifyIntervalSeconds = 60
	if got := remoteVerificationInconclusiveDelay(); got != 60*time.Second {
		t.Fatalf("long configured verify interval=%v, want 60s", got)
	}
}

func TestRemoteMatchesCanonical(t *testing.T) {
	wantSHA1 := strings.Repeat("a", 40)
	row := &model.WebDAVWritebackObject{
		Size:        4096,
		PayloadSHA1: wantSHA1,
	}

	match := &model.Object{
		Size:     4096,
		HashInfo: utils.NewHashInfo(utils.SHA1, strings.ToUpper(wantSHA1)),
	}
	if !remoteMatchesCanonical(row, match, true) {
		t.Fatal("same-size same-SHA1 remote object should verify")
	}

	wrongHash := &model.Object{
		Size:     4096,
		HashInfo: utils.NewHashInfo(utils.SHA1, strings.Repeat("b", 40)),
	}
	if remoteMatchesCanonical(row, wrongHash, true) {
		t.Fatal("same-size old generation with a different SHA1 must not verify")
	}

	noHash := &model.Object{Size: 4096}
	if !remoteMatchesCanonical(row, noHash, false) {
		t.Fatal("provider without hash support should retain size-based fallback")
	}
	if remoteMatchesCanonical(row, noHash, true) {
		t.Fatal("115 verification must wait for SHA1 instead of accepting size-only metadata")
	}

	wrongSize := &model.Object{
		Size:     4095,
		HashInfo: utils.NewHashInfo(utils.SHA1, wantSHA1),
	}
	if remoteMatchesCanonical(row, wrongSize, true) {
		t.Fatal("matching hash cannot compensate for a size mismatch")
	}
}

func TestCanCoalesceDuplicatePut(t *testing.T) {
	sha := strings.Repeat("a", 40)
	base := &model.WebDAVWritebackObject{
		Size:        1024,
		State:       StateQueued,
		SpoolPath:   "/spool/current.data",
		PayloadSHA1: sha,
	}
	if !canCoalesceDuplicatePut(base, 1024, strings.ToUpper(sha)) {
		t.Fatal("identical durable payload should coalesce")
	}

	noSpool := *base
	noSpool.SpoolPath = ""
	if canCoalesceDuplicatePut(&noSpool, 1024, sha) {
		t.Fatal("completed metadata without a durable spool must accept a new PUT for recovery")
	}

	deleted := *base
	deleted.State = StateDeleted
	if canCoalesceDuplicatePut(&deleted, 1024, sha) {
		t.Fatal("deleted generation must not absorb a recreate PUT")
	}

	wrongHash := *base
	if canCoalesceDuplicatePut(&wrongHash, 1024, strings.Repeat("b", 40)) {
		t.Fatal("different encrypted payload must create a new generation")
	}

	if canCoalesceDuplicatePut(base, 2048, sha) {
		t.Fatal("different payload size must create a new generation")
	}
}

func TestRemoteListContainsName(t *testing.T) {
	objs := []model.Obj{
		&model.Object{Name: "keep.bin"},
		&model.Object{Name: "folder", IsFolder: true},
	}
	if !remoteListContainsName(objs, "keep.bin") {
		t.Fatal("exact file name should be detected in provider listing")
	}
	if !remoteListContainsName(objs, "folder") {
		t.Fatal("same-name directory should still block delete confirmation")
	}
	if remoteListContainsName(objs, "missing.bin") {
		t.Fatal("absent name must not be reported present")
	}
}

func TestMoveMetadataNeedsProviderRootShadow(t *testing.T) {
	sourceRoot := &model.Object{
		Name:     "src",
		IsFolder: true,
		Modified: time.Now().Add(-time.Hour),
		Ctime:    time.Now().Add(-2 * time.Hour),
	}
	rows := []model.WebDAVWritebackObject{
		{Path: "/src/child.bin", State: StateCompleted},
	}
	hasSourceRoot := false
	for i := range rows {
		if rows[i].Path == "/src" {
			hasSourceRoot = true
			break
		}
	}
	if hasSourceRoot {
		t.Fatal("test fixture unexpectedly contains a canonical source root")
	}

	now := time.Now()
	var root model.WebDAVWritebackObject
	setProviderCompletedRoot(&root, "/dst", sourceRoot, now)
	if !root.IsDir || root.Path != "/dst" || root.State != StateCompleted {
		t.Fatalf("provider directory root shadow = %+v", root)
	}
	if root.CompletedAt == nil || !root.CompletedAt.Equal(now) {
		t.Fatal("provider directory root shadow must receive fresh consistency grace")
	}
}

func TestDeleteParentMissing(t *testing.T) {
	if deleteParentMissing(nil) {
		t.Fatal("nil error cannot prove a missing provider parent")
	}
	if !deleteParentMissing(errs.ObjectNotFound) {
		t.Fatal("provider ObjectNotFound parent should prove descendant absence")
	}
	if deleteParentMissing(errors.New("network failure")) {
		t.Fatal("unrelated provider failure must not be treated as descendant absence")
	}
}

func TestRemoteContentMatchesCanonical(t *testing.T) {
	sha := strings.Repeat("a", 40)
	row := &model.WebDAVWritebackObject{
		Size:        8192,
		PayloadSHA1: sha,
	}
	same := &model.Object{
		Size:     8192,
		HashInfo: utils.NewHashInfo(utils.SHA1, strings.ToUpper(sha)),
	}
	if !remoteContentMatchesCanonical(row, same) {
		t.Fatal("same size and SHA1 should retain canonical metadata")
	}
	if remoteContentMatchesCanonical(row, &model.Object{Size: 4096, HashInfo: same.HashInfo}) {
		t.Fatal("provider size change must invalidate canonical metadata")
	}
	if remoteContentMatchesCanonical(row, &model.Object{Size: 8192, HashInfo: utils.NewHashInfo(utils.SHA1, strings.Repeat("b", 40))}) {
		t.Fatal("provider SHA1 change must invalidate canonical metadata")
	}
	if !remoteContentMatchesCanonical(row, &model.Object{Size: 8192}) {
		t.Fatal("missing provider hash should not create a false content mismatch")
	}

	dir := &model.WebDAVWritebackObject{IsDir: true}
	if !remoteContentMatchesCanonical(dir, &model.Object{IsFolder: true}) {
		t.Fatal("same-type directory should reconcile by provider visibility")
	}
	if remoteContentMatchesCanonical(dir, &model.Object{}) {
		t.Fatal("resource type mismatch must invalidate canonical metadata")
	}
}

func TestCompareRemoteContentRequires115HashForConclusiveMatch(t *testing.T) {
	sha := strings.Repeat("a", 40)
	row := &model.WebDAVWritebackObject{
		Size:        8192,
		PayloadSHA1: sha,
	}

	if got := compareRemoteContent(row, &model.Object{Size: 8192}, true); got != remoteContentInconclusive {
		t.Fatalf("missing required SHA1 comparison = %v, want inconclusive", got)
	}
	if got := compareRemoteContent(row, &model.Object{Size: 8192}, false); got != remoteContentMatch {
		t.Fatalf("non-hash provider comparison = %v, want match", got)
	}
	if got := compareRemoteContent(row, &model.Object{
		Size:     8192,
		HashInfo: utils.NewHashInfo(utils.SHA1, strings.ToUpper(sha)),
	}, true); got != remoteContentMatch {
		t.Fatalf("matching 115 SHA1 comparison = %v, want match", got)
	}
	if got := compareRemoteContent(row, &model.Object{
		Size:     8192,
		HashInfo: utils.NewHashInfo(utils.SHA1, strings.Repeat("b", 40)),
	}, true); got != remoteContentMismatch {
		t.Fatalf("wrong 115 SHA1 comparison = %v, want mismatch", got)
	}
	if got := compareRemoteContent(row, &model.Object{
		Size:     0,
		HashInfo: utils.NewHashInfo(utils.SHA1, sha),
	}, true); got != remoteContentMismatch {
		t.Fatalf("zero-size stale metadata comparison = %v, want mismatch pending parent confirmation", got)
	}
}

func TestExactRemoteByName(t *testing.T) {
	objs := []model.Obj{
		&model.Object{Name: "other.bin"},
		&model.Object{Name: "target.bin", Size: 1024},
	}
	if got := exactRemoteByName(objs, "target.bin"); got == nil || got.GetName() != "target.bin" {
		t.Fatal("exact provider name was not found")
	}
	if got := exactRemoteByName(objs, "missing.bin"); got != nil {
		t.Fatalf("unexpected provider match: %q", got.GetName())
	}
}

func TestRefreshUnknownProviderOverwrite(t *testing.T) {
	now := time.Now()
	row := &model.WebDAVWritebackObject{
		Path:        "/dst/old.bin",
		PathKey:     pathKey("/dst/old.bin"),
		Generation:  4,
		Size:        512,
		State:       StateCompleted,
		SpoolPath:   "/spool/old.data",
		PayloadSHA1: strings.Repeat("a", 40),
	}
	refreshUnknownProviderOverwrite(row, now)
	if row.Generation != 5 || row.State != StateCompleted {
		t.Fatalf("unknown overwrite row generation/state = %d/%s", row.Generation, row.State)
	}
	if row.SpoolPath != "" {
		t.Fatal("old destination spool must stop being authoritative after provider overwrite")
	}
	if row.PayloadSHA1 == "" {
		t.Fatal("old SHA1 should remain temporarily as reconciliation evidence")
	}
	if row.CompletedAt == nil || !row.CompletedAt.Equal(now) {
		t.Fatal("unknown provider overwrite must receive a fresh consistency grace")
	}
}

func TestCollectConfirmedTombstoneSubtree(t *testing.T) {
	root := &model.WebDAVWritebackObject{
		ID:         1,
		Path:       "/old-folder",
		Generation: 7,
		State:      StateDeleted,
	}
	rows := []model.WebDAVWritebackObject{
		{ID: 1, Path: "/old-folder", Generation: 7, State: StateDeleted},
		{ID: 2, Path: "/old-folder/a.bin", Generation: 2, State: StateDeleted, SpoolPath: "/spool/shared.data"},
		{ID: 3, Path: "/old-folder/sub/b.bin", Generation: 1, State: StateDeleted, SpoolPath: "/spool/shared.data"},
		{ID: 4, Path: "/old-folder/recreated.bin", Generation: 3, State: StateQueued, SpoolPath: "/spool/new.data"},
		{ID: 5, Path: "/old-folder-other/c.bin", Generation: 1, State: StateDeleted},
	}
	ids, spools, valid := collectConfirmedTombstoneSubtree(root, rows)
	if !valid {
		t.Fatal("matching root generation should validate subtree cleanup")
	}
	if len(ids) != 3 || ids[0] != 1 || ids[1] != 2 || ids[2] != 3 {
		t.Fatalf("subtree tombstone ids = %v", ids)
	}
	if len(spools) != 1 || spools[0] != "/spool/shared.data" {
		t.Fatalf("deduplicated subtree spool paths = %v", spools)
	}

	staleRoot := *root
	staleRoot.Generation = 6
	if _, _, valid := collectConfirmedTombstoneSubtree(&staleRoot, rows); valid {
		t.Fatal("superseded root generation must not bulk-delete descendants")
	}
}

func TestWritebackCompositeIndexes(t *testing.T) {
	typ := reflect.TypeOf(model.WebDAVWritebackObject{})
	checks := map[string]string{
		"State":       "idx_webdav_writeback_queue",
		"RetryAt":     "idx_webdav_writeback_queue",
		"CompletedAt": "idx_webdav_writeback_completed",
	}
	for fieldName, indexName := range checks {
		field, ok := typ.FieldByName(fieldName)
		if !ok {
			t.Fatalf("missing model field %s", fieldName)
		}
		if !strings.Contains(field.Tag.Get("gorm"), indexName) {
			t.Fatalf("%s gorm tag %q does not include %s", fieldName, field.Tag.Get("gorm"), indexName)
		}
	}
	stateField, _ := typ.FieldByName("State")
	if !strings.Contains(stateField.Tag.Get("gorm"), "idx_webdav_writeback_completed") {
		t.Fatal("State must lead the completed-cache composite index")
	}
}

func TestDescendantLikePattern(t *testing.T) {
	tests := []struct {
		root string
		want string
	}{
		{root: "/a", want: "/a/%"},
		{root: "/a_b", want: "/a~_b/%"},
		{root: "/a%b", want: "/a~%b/%"},
		{root: "/a~b", want: "/a~~b/%"},
		{root: "/", want: "/%"},
	}
	for _, tt := range tests {
		if got := descendantLikePattern(tt.root); got != tt.want {
			t.Fatalf("descendantLikePattern(%q) = %q, want %q", tt.root, got, tt.want)
		}
	}
}

func TestCaptureRemoteVerification(t *testing.T) {
	now := time.Date(2026, time.September, 20, 7, 30, 0, 0, time.UTC)
	sha := strings.Repeat("A", 40)
	row := &model.WebDAVWritebackObject{Generation: 9}
	remote := &model.Object{
		ID:       "115-file-id-123",
		HashInfo: utils.NewHashInfo(utils.SHA1, sha),
	}

	evidence := captureRemoteVerification(row, remote, now)
	if evidence.objectID != "115-file-id-123" {
		t.Fatalf("remote object id = %q", evidence.objectID)
	}
	if evidence.sha1 != strings.ToLower(sha) {
		t.Fatalf("remote sha1 = %q", evidence.sha1)
	}
	if evidence.generation != 9 {
		t.Fatalf("remote generation = %d", evidence.generation)
	}
	if !evidence.verifiedAt.Equal(now) {
		t.Fatalf("verified at = %v, want %v", evidence.verifiedAt, now)
	}
}

func TestCaptureRemoteVerificationWithoutProviderHash(t *testing.T) {
	now := time.Date(2026, time.September, 20, 7, 30, 0, 0, time.UTC)
	row := &model.WebDAVWritebackObject{Generation: 3}
	remote := &model.Object{ID: "provider-object"}

	evidence := captureRemoteVerification(row, remote, now)
	if evidence.objectID != "provider-object" || evidence.sha1 != "" || evidence.generation != 3 {
		t.Fatalf("unexpected verification evidence: %+v", evidence)
	}
}

func TestClearRemoteVerification(t *testing.T) {
	now := time.Now()
	row := &model.WebDAVWritebackObject{
		Generation:       9,
		RemoteObjectID:   "115-object",
		RemoteSHA1:       strings.Repeat("a", 40),
		RemoteGeneration: 8,
		RemoteVerifiedAt: &now,
	}

	clearRemoteVerification(row)
	if row.RemoteObjectID != "" || row.RemoteSHA1 != "" || row.RemoteGeneration != 0 || row.RemoteVerifiedAt != nil {
		t.Fatalf("remote verification evidence was not cleared: %+v", row)
	}
	if row.Generation != 9 {
		t.Fatalf("clearing remote evidence changed canonical generation to %d", row.Generation)
	}
}

func TestCanonicalContentSHA1IsGenerationScoped(t *testing.T) {
	payloadSHA1 := strings.Repeat("a", 40)
	remoteSHA1 := strings.Repeat("b", 40)
	row := &model.WebDAVWritebackObject{
		Generation:       7,
		PayloadSHA1:      payloadSHA1,
		RemoteSHA1:       remoteSHA1,
		RemoteGeneration: 7,
	}
	if got := canonicalContentSHA1(row); got != payloadSHA1 {
		t.Fatalf("payload SHA1 should win, got %q", got)
	}

	row.PayloadSHA1 = ""
	if got := canonicalContentSHA1(row); got != remoteSHA1 {
		t.Fatalf("verified SHA1 for current generation = %q, want %q", got, remoteSHA1)
	}

	row.RemoteGeneration = 6
	if got := canonicalContentSHA1(row); got != "" {
		t.Fatalf("stale remote generation leaked SHA1 %q", got)
	}
}

func TestRemoteMatchesCanonicalUsesGenerationScopedEvidence(t *testing.T) {
	sha := strings.Repeat("c", 40)
	row := &model.WebDAVWritebackObject{
		Size:             4096,
		Generation:       3,
		RemoteSHA1:       sha,
		RemoteGeneration: 3,
	}
	same := &model.Object{Size: 4096, HashInfo: utils.NewHashInfo(utils.SHA1, strings.ToUpper(sha))}
	if !remoteMatchesCanonical(row, same, true) {
		t.Fatal("current-generation verified SHA1 should match remote content")
	}

	different := &model.Object{Size: 4096, HashInfo: utils.NewHashInfo(utils.SHA1, strings.Repeat("d", 40))}
	if remoteMatchesCanonical(row, different, true) {
		t.Fatal("different remote SHA1 must not match current-generation evidence")
	}

	row.RemoteGeneration = 2
	if !remoteMatchesCanonical(row, different, true) {
		t.Fatal("stale remote verification evidence must not constrain a newer generation without payload SHA1")
	}
}

func TestSetMovedDestinationFromSourceCreatesIndependentGeneration(t *testing.T) {
	now := time.Now()
	source := &model.WebDAVWritebackObject{
		Path:           "/src/file.bin",
		Parent:         "/src",
		Name:           "file.bin",
		Size:           8192,
		ModTime:        now.Add(-time.Hour),
		CreateTime:     now.Add(-2 * time.Hour),
		Generation:     4,
		State:          StateCompleted,
		PayloadSHA1:    strings.Repeat("a", 40),
		MimeType:       "application/octet-stream",
		RemoteObjectID: "old-source-id",
	}
	verifiedAt := now.Add(-time.Minute)
	destination := &model.WebDAVWritebackObject{
		Path:             "/dst/old.bin",
		Generation:       5,
		RemoteObjectID:   "old-destination-id",
		RemoteSHA1:       strings.Repeat("b", 40),
		RemoteGeneration: 5,
		RemoteVerifiedAt: &verifiedAt,
	}

	setMovedDestinationFromSource(destination, source, "/dst/file.bin", now)

	if destination.Path != "/dst/file.bin" || destination.Parent != "/dst" || destination.Name != "file.bin" {
		t.Fatalf("destination path metadata = %s / %s / %s", destination.Path, destination.Parent, destination.Name)
	}
	if destination.Generation != 6 || destination.State != StateCompleted {
		t.Fatalf("destination generation/state = %d/%s", destination.Generation, destination.State)
	}
	if destination.PayloadSHA1 != source.PayloadSHA1 || destination.Size != source.Size {
		t.Fatal("destination content identity was not copied from source")
	}
	if destination.CompletedAt == nil || !destination.CompletedAt.Equal(now) {
		t.Fatal("completed provider MOVE destination must receive fresh consistency grace")
	}
	if destination.RemoteObjectID != "" || destination.RemoteSHA1 != "" || destination.RemoteGeneration != 0 || destination.RemoteVerifiedAt != nil {
		t.Fatal("destination must not retain remote verification evidence from the overwritten generation")
	}
	if source.Path != "/src/file.bin" || source.Generation != 4 || source.State != StateCompleted {
		t.Fatal("building the destination must not mutate the source canonical row")
	}
}

func TestProviderMoveSourceTombstone(t *testing.T) {
	now := time.Now()
	modified := now.Add(-time.Hour)
	created := now.Add(-2 * time.Hour)
	source := &model.Object{
		Name:     "file.bin",
		Size:     12345,
		Modified: modified,
		Ctime:    created,
	}

	row := providerMoveSourceTombstone("/src/file.bin", source, now)
	if row.Path != "/src/file.bin" || row.Parent != "/src" || row.Name != "file.bin" {
		t.Fatalf("source tombstone path metadata = %s / %s / %s", row.Path, row.Parent, row.Name)
	}
	if row.State != StateDeleted || row.Generation != 1 || row.Size != 12345 || row.IsDir {
		t.Fatalf("unexpected source tombstone: %+v", row)
	}
	if row.RetryAt == nil || !row.RetryAt.Equal(now) {
		t.Fatal("provider MOVE source tombstone must be eligible for absence verification immediately")
	}
	if !row.ModTime.Equal(modified) || !row.CreateTime.Equal(created) {
		t.Fatal("provider MOVE source tombstone should retain captured provider timestamps")
	}
}

func TestCanReverifyCompletedDuplicatePut(t *testing.T) {
	sha := strings.Repeat("e", 40)
	row := &model.WebDAVWritebackObject{
		Size:        64 * 1024,
		Generation:  8,
		State:       StateCompleted,
		PayloadSHA1: sha,
	}
	if !canReverifyCompletedDuplicatePut(row, row.Size, strings.ToUpper(sha)) {
		t.Fatal("completed same-content PUT without a spool should use remote re-verification")
	}
	if canReverifyCompletedDuplicatePut(row, row.Size, strings.Repeat("f", 40)) {
		t.Fatal("different encrypted content must create a new canonical generation")
	}
	row.State = StateQueued
	if canReverifyCompletedDuplicatePut(row, row.Size, sha) {
		t.Fatal("only a completed generation may be revived for duplicate re-verification")
	}

	row.State = StateCompleted
	row.PayloadSHA1 = ""
	row.RemoteSHA1 = sha
	row.RemoteGeneration = row.Generation
	if !canReverifyCompletedDuplicatePut(row, row.Size, sha) {
		t.Fatal("current-generation verified SHA1 should identify duplicate content after spool cleanup")
	}
	row.RemoteGeneration--
	if canReverifyCompletedDuplicatePut(row, row.Size, sha) {
		t.Fatal("stale remote evidence must not coalesce a newer canonical generation")
	}
}

func TestApplyDuplicatePutMetadataKeepsContentGenerationStable(t *testing.T) {
	oldMod := time.Date(2026, time.September, 20, 1, 0, 0, 0, time.UTC)
	oldCreate := oldMod.Add(-time.Hour)
	newMod := oldMod.Add(2 * time.Hour)
	newCreate := oldCreate.Add(30 * time.Minute)
	row := &model.WebDAVWritebackObject{
		Generation: 7,
		ETag:       canonicalETag(pathKey("/encrypted/file.bin"), 7, 4096),
		ModTime:    oldMod,
		CreateTime: oldCreate,
		MimeType:   "application/octet-stream",
	}

	oldGeneration := row.Generation
	oldETag := row.ETag
	applyDuplicatePutMetadata(row, newMod, newCreate, "application/x-cloudsync", true, true)

	if !row.ModTime.Equal(newMod) || !row.CreateTime.Equal(newCreate) {
		t.Fatalf("duplicate PUT metadata = %v/%v, want %v/%v", row.ModTime, row.CreateTime, newMod, newCreate)
	}
	if row.MimeType != "application/x-cloudsync" {
		t.Fatalf("duplicate PUT mime = %q", row.MimeType)
	}
	if row.Generation != oldGeneration || row.ETag != oldETag {
		t.Fatal("metadata-only duplicate PUT must not manufacture a new content generation or ETag")
	}
}

func TestApplyDuplicatePutMetadataPreservesOmittedTimes(t *testing.T) {
	oldMod := time.Date(2026, time.September, 20, 1, 0, 0, 0, time.UTC)
	oldCreate := oldMod.Add(-time.Hour)
	row := &model.WebDAVWritebackObject{
		Generation: 9,
		ModTime:    oldMod,
		CreateTime: oldCreate,
		MimeType:   "application/octet-stream",
	}

	applyDuplicatePutMetadata(
		row,
		oldMod.Add(4*time.Hour),
		oldCreate.Add(4*time.Hour),
		"application/x-cloudsync",
		false,
		false,
	)

	if !row.ModTime.Equal(oldMod) || !row.CreateTime.Equal(oldCreate) {
		t.Fatalf("omitted duplicate timestamps changed canonical times to %v/%v", row.ModTime, row.CreateTime)
	}
	if row.MimeType != "application/x-cloudsync" {
		t.Fatalf("mime metadata should still converge, got %q", row.MimeType)
	}
}
