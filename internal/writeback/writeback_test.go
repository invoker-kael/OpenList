package writeback

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
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
	if !shouldDropCanonicalAfterRemoteList(completedNoSpool, true, false, time.Now()) {
		t.Fatal("missing remote object should be surfaced after a reliable listing")
	}
	if shouldDropCanonicalAfterRemoteList(completedNoSpool, false, false, time.Now()) {
		t.Fatal("provider/listing failure must not drop canonical metadata")
	}
	if shouldDropCanonicalAfterRemoteList(completedNoSpool, true, true, time.Now()) {
		t.Fatal("present remote object must keep canonical metadata")
	}

	completedCached := &model.WebDAVWritebackObject{
		State:     StateCompleted,
		SpoolPath: "/spool/object.data",
	}
	if shouldDropCanonicalAfterRemoteList(completedCached, true, false, time.Now()) {
		t.Fatal("locally cached completed object must remain authoritative")
	}

	queued := &model.WebDAVWritebackObject{
		State:     StateQueued,
		SpoolPath: "/spool/object.data",
	}
	if shouldDropCanonicalAfterRemoteList(queued, true, false, time.Now()) {
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
	if shouldDropCanonicalAfterRemoteList(row, true, false, now) {
		t.Fatal("fresh completed file must not be dropped on a transient provider miss")
	}

	completed = now.Add(-61 * time.Second)
	row.CompletedAt = &completed
	if canonicalShadowInGrace(row, now) {
		t.Fatal("completed file metadata should leave grace after the configured window")
	}
	if !shouldDropCanonicalAfterRemoteList(row, true, false, now) {
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

