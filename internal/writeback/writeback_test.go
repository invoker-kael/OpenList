package writeback

import (
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

func TestShouldDropCanonicalAfterRemoteList(t *testing.T) {
	completedNoSpool := &model.WebDAVWritebackObject{
		State:     StateCompleted,
		SpoolPath: "",
	}
	if !shouldDropCanonicalAfterRemoteList(completedNoSpool, true, nil, time.Now()) {
		t.Fatal("missing remote object should be surfaced after a reliable listing")
	}
	if shouldDropCanonicalAfterRemoteList(completedNoSpool, false, nil, time.Now()) {
		t.Fatal("provider/listing failure must not drop canonical metadata")
	}
	if shouldDropCanonicalAfterRemoteList(completedNoSpool, true, &model.Object{Size: completedNoSpool.Size}, time.Now()) {
		t.Fatal("present remote object must keep canonical metadata")
	}

	completedCached := &model.WebDAVWritebackObject{
		State:     StateCompleted,
		SpoolPath: "/spool/object.data",
	}
	if shouldDropCanonicalAfterRemoteList(completedCached, true, nil, time.Now()) {
		t.Fatal("locally cached completed object must remain authoritative")
	}

	queued := &model.WebDAVWritebackObject{
		State:     StateQueued,
		SpoolPath: "/spool/object.data",
	}
	if shouldDropCanonicalAfterRemoteList(queued, true, nil, time.Now()) {
		t.Fatal("pending upload must remain visible even before provider listing catches up")
	}
}

func TestReceivingPathReferenceCount(t *testing.T) {
	p := "/encrypted/placeholder.bin"
	release1 := beginReceiving(p)
	release2 := beginReceiving(p)
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

func TestCopyToSpoolComputesPayloadSHA1(t *testing.T) {
	payload := "cloud-sync-encrypted-payload"
	f, err := os.CreateTemp(t.TempDir(), "spool-*.data")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	size, sha1sum, err := copyToSpool(f, strings.NewReader(payload), int64(len(payload)))
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
	if shouldDropCanonicalAfterRemoteList(row, true, nil, now) {
		t.Fatal("fresh completed file must not be dropped on a transient provider miss")
	}

	completed = now.Add(-61 * time.Second)
	row.CompletedAt = &completed
	if canonicalShadowInGrace(row, now) {
		t.Fatal("completed file metadata should leave grace after the configured window")
	}
	if !shouldDropCanonicalAfterRemoteList(row, true, nil, now) {
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
	applyDuplicatePutMetadata(row, newMod, newCreate, "application/x-cloudsync")

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
