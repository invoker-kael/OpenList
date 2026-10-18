package writeback

import (
	"path"
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

func remoteIndexTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	oldConf := conf.Conf
	conf.Conf = &conf.Config{
		Database: conf.Database{Type: "sqlite"},
		WebDAVWriteback: conf.WebDAVWritebackConfig{
			ProviderSnapshotTTLSeconds: 600,
		},
	}
	t.Cleanup(func() { conf.Conf = oldConf })

	database, err := gorm.Open(sqlite.Open("file:"+t.Name()+"?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	db.Init(database)
	return database
}

func TestRemoteIndexStoresOnlyTrackedNamesAndReusesExactMatch(t *testing.T) {
	database := remoteIndexTestDB(t)
	sha := strings.Repeat("a", 40)
	row := model.WebDAVWritebackObject{
		PathKey: pathKey("/backup/file.bin"), ParentKey: pathKey("/backup"),
		Path: "/backup/file.bin", Parent: "/backup", Name: "file.bin",
		Generation: 2, CanonicalState: CanonicalStateAcked, State: StateQueued,
		Size: 10, PayloadSHA1: sha,
	}
	if err := database.Create(&row).Error; err != nil {
		t.Fatal(err)
	}

	observedAt := time.Now().Add(-time.Minute).Truncate(time.Second)
	remote := []model.Obj{
		&model.Object{Name: row.Name, ID: "wrong-size", Size: 11, HashInfo: utils.NewHashInfo(utils.SHA1, sha)},
		&model.Object{Name: row.Name, ID: "exact-match", Size: 10, HashInfo: utils.NewHashInfo(utils.SHA1, strings.ToUpper(sha))},
		&model.Object{Name: "untracked.bin", ID: "ignored", Size: 10, HashInfo: utils.NewHashInfo(utils.SHA1, sha)},
	}
	version := beginRemoteIndexCapture(row.Parent)
	persistRemoteIndexSnapshot(row.Parent, remote, observedAt, version)
	endRemoteIndexCapture(row.Parent)

	var saved model.WebDAVWritebackRemoteSnapshot
	if err := database.Where("parent_key = ?", row.ParentKey).Take(&saved).Error; err != nil {
		t.Fatal(err)
	}
	if saved.ObjectCount != 2 {
		t.Fatalf("indexed entries=%d, want only two tracked-name matches", saved.ObjectCount)
	}

	cached, cachedAt, ok := cachedRemoteMatches(&row)
	if !ok || len(cached) != 2 || !cachedAt.Equal(observedAt) {
		t.Fatalf("cached snapshot matches=%d, observed=%v, ok=%v", len(cached), cachedAt, ok)
	}
	if got := (&workerManager{}).completeExistingRemoteAt(&row, cached, cachedAt); got != remoteContentMatch {
		t.Fatalf("cached exact match comparison=%v, want match", got)
	}

	var savedRow model.WebDAVWritebackObject
	if err := database.First(&savedRow, row.ID).Error; err != nil {
		t.Fatal(err)
	}
	if savedRow.State != StateCompleted || savedRow.RemoteObjectID != "exact-match" {
		t.Fatalf("cached match did not complete expected remote object: state=%q id=%q", savedRow.State, savedRow.RemoteObjectID)
	}
	if savedRow.RemoteVerifiedAt == nil || !savedRow.RemoteVerifiedAt.Equal(observedAt) {
		t.Fatalf("remote verification time=%v, want snapshot observation time %v", savedRow.RemoteVerifiedAt, observedAt)
	}
}

func TestRemoteIndexCacheExpiresAtConfiguredTTL(t *testing.T) {
	database := remoteIndexTestDB(t)
	sha := strings.Repeat("b", 40)
	row := model.WebDAVWritebackObject{
		PathKey: pathKey("/backup/expired.bin"), ParentKey: pathKey("/backup"),
		Path: "/backup/expired.bin", Parent: "/backup", Name: "expired.bin",
		State: StateQueued, Size: 20, PayloadSHA1: sha,
	}
	if err := database.Create(&row).Error; err != nil {
		t.Fatal(err)
	}
	version := beginRemoteIndexCapture(row.Parent)
	persistRemoteIndexSnapshot(row.Parent, []model.Obj{
		&model.Object{Name: row.Name, ID: "stale", Size: row.Size, HashInfo: utils.NewHashInfo(utils.SHA1, sha)},
	}, time.Now().Add(-11*time.Minute), version)
	endRemoteIndexCapture(row.Parent)
	if _, _, ok := cachedRemoteMatches(&row); ok {
		t.Fatal("expired remote index snapshot must not satisfy a new upload")
	}
}

func TestInvalidatedInFlightSnapshotCannotBePersisted(t *testing.T) {
	database := remoteIndexTestDB(t)
	row := model.WebDAVWritebackObject{
		PathKey: pathKey("/backup/racing.bin"), ParentKey: pathKey("/backup"),
		Path: "/backup/racing.bin", Parent: "/backup", Name: "racing.bin",
		State: StateQueued,
	}
	if err := database.Create(&row).Error; err != nil {
		t.Fatal(err)
	}

	version := beginRemoteIndexCapture(row.Parent)
	invalidateRemoteIndexSnapshots(row.Parent)
	persistRemoteIndexSnapshot(row.Parent, []model.Obj{
		&model.Object{Name: row.Name, ID: "stale", Size: 10},
	}, time.Now(), version)
	endRemoteIndexCapture(row.Parent)

	var snapshot model.WebDAVWritebackRemoteSnapshot
	err := database.Where("parent_key = ?", row.ParentKey).Take(&snapshot).Error
	if err != gorm.ErrRecordNotFound {
		t.Fatalf("stale in-flight listing repopulated the invalidated snapshot: err=%v", err)
	}
}

func TestRemoteIndexParentPathsAreDeduplicated(t *testing.T) {
	got := compactParentPaths([]string{path.Clean("/backup"), path.Clean("/backup"), path.Clean("/other")})
	if len(got) != 2 || got[0] != "/backup" || got[1] != "/other" {
		t.Fatalf("compacted parents=%v", got)
	}
}
