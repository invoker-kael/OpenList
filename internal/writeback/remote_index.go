package writeback

import (
	"context"
	"encoding/json"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type remoteIndexObject struct {
	Name     string `json:"name"`
	ObjectID string `json:"object_id"`
	Size     int64  `json:"size"`
	IsDir    bool   `json:"is_dir"`
	SHA1     string `json:"sha1,omitempty"`
}

type remoteIndexEpoch struct {
	version uint64
	active  int
}

type RemoteIndexRefreshStatus struct {
	Running    bool       `json:"running"`
	Total      int        `json:"total"`
	Completed  int        `json:"completed"`
	Failed     int        `json:"failed"`
	StartedAt  *time.Time `json:"started_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
}

type RemoteIndexStats struct {
	Directories int64                    `json:"directories"`
	Objects     int64                    `json:"objects"`
	LastAt      *time.Time               `json:"last_at,omitempty"`
	Refresh     RemoteIndexRefreshStatus `json:"refresh"`
}

var remoteIndexRefresh = struct {
	sync.Mutex
	status RemoteIndexRefreshStatus
	cancel context.CancelFunc
}{}

var remoteIndexEpochs = struct {
	sync.Mutex
	byParent map[string]*remoteIndexEpoch
}{byParent: make(map[string]*remoteIndexEpoch)}

func beginRemoteIndexCapture(parent string) uint64 {
	key := pathKey(utils.FixAndCleanPath(parent))
	remoteIndexEpochs.Lock()
	defer remoteIndexEpochs.Unlock()
	epoch := remoteIndexEpochs.byParent[key]
	if epoch == nil {
		epoch = &remoteIndexEpoch{}
		remoteIndexEpochs.byParent[key] = epoch
	}
	epoch.active++
	return epoch.version
}

func endRemoteIndexCapture(parent string) {
	key := pathKey(utils.FixAndCleanPath(parent))
	remoteIndexEpochs.Lock()
	defer remoteIndexEpochs.Unlock()
	epoch := remoteIndexEpochs.byParent[key]
	if epoch == nil {
		return
	}
	epoch.active--
	if epoch.active <= 0 {
		delete(remoteIndexEpochs.byParent, key)
	}
}

func persistRemoteIndexSnapshot(parent string, objects []model.Obj, refreshedAt time.Time, expectedVersion uint64) {
	parent = utils.FixAndCleanPath(parent)
	parentKey := pathKey(parent)
	remoteIndexEpochs.Lock()
	defer remoteIndexEpochs.Unlock()
	epoch := remoteIndexEpochs.byParent[parentKey]
	if epoch == nil || epoch.version != expectedVersion {
		return
	}
	if Rebuilding() || !rebuildGate.TryRLock() {
		return
	}
	defer rebuildGate.RUnlock()
	if Rebuilding() {
		return
	}

	var names []string
	if err := db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("parent_key = ?", parentKey).
		Distinct("name").Pluck("name", &names).Error; err != nil {
		return
	}
	knownNames := make(map[string]struct{}, len(names))
	for _, name := range names {
		knownNames[name] = struct{}{}
	}

	indexed := make([]remoteIndexObject, 0)
	for _, object := range objects {
		if object == nil {
			continue
		}
		if _, known := knownNames[object.GetName()]; !known {
			continue
		}
		entry := remoteIndexObject{
			Name:     object.GetName(),
			ObjectID: object.GetID(),
			Size:     object.GetSize(),
			IsDir:    object.IsDir(),
		}
		entry.SHA1 = strings.ToLower(object.GetHash().GetHash(utils.SHA1))
		indexed = append(indexed, entry)
	}

	encoded, err := json.Marshal(indexed)
	if err != nil {
		return
	}
	snapshot := model.WebDAVWritebackRemoteSnapshot{
		ParentKey:   parentKey,
		Parent:      parent,
		ObjectsJSON: string(encoded),
		ObjectCount: len(indexed),
		RefreshedAt: refreshedAt,
	}
	_ = db.GetDb().Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "parent_key"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"parent", "objects_json", "object_count", "refreshed_at", "updated_at",
		}),
	}).Create(&snapshot).Error
}

func cachedRemoteMatches(row *model.WebDAVWritebackObject) ([]model.Obj, time.Time, bool) {
	if row == nil || canonicalContentSHA1(row) == "" {
		return nil, time.Time{}, false
	}
	ttl := providerSnapshotTTL()
	if ttl <= 0 {
		return nil, time.Time{}, false
	}

	var snapshot model.WebDAVWritebackRemoteSnapshot
	err := db.GetDb().Where("parent_key = ?", pathKey(row.Parent)).Take(&snapshot).Error
	if err != nil || time.Since(snapshot.RefreshedAt) >= ttl {
		return nil, time.Time{}, false
	}
	var entries []remoteIndexObject
	if json.Unmarshal([]byte(snapshot.ObjectsJSON), &entries) != nil {
		return nil, time.Time{}, false
	}

	remotes := make([]model.Obj, 0)
	for _, entry := range entries {
		if entry.Name != row.Name || entry.ObjectID == "" {
			continue
		}
		remotes = append(remotes, &model.Object{
			ID:       entry.ObjectID,
			Path:     path.Join(row.Parent, entry.Name),
			Name:     entry.Name,
			Size:     entry.Size,
			IsFolder: entry.IsDir,
			HashInfo: utils.NewHashInfo(utils.SHA1, entry.SHA1),
		})
	}
	return remotes, snapshot.RefreshedAt, len(remotes) > 0
}

func invalidateRemoteIndexSnapshots(parents ...string) {
	keys := make([]string, 0, len(parents))
	for _, parent := range parents {
		parent = utils.FixAndCleanPath(parent)
		if parent != "" {
			keys = append(keys, pathKey(parent))
		}
	}
	if len(keys) == 0 {
		return
	}
	remoteIndexEpochs.Lock()
	defer remoteIndexEpochs.Unlock()
	for _, key := range keys {
		epoch := remoteIndexEpochs.byParent[key]
		if epoch == nil {
			epoch = &remoteIndexEpoch{}
			remoteIndexEpochs.byParent[key] = epoch
		}
		epoch.version++
	}
	_ = db.GetDb().Where("parent_key IN ?", keys).
		Delete(&model.WebDAVWritebackRemoteSnapshot{}).Error
	for _, key := range keys {
		if epoch := remoteIndexEpochs.byParent[key]; epoch != nil && epoch.active == 0 {
			delete(remoteIndexEpochs.byParent, key)
		}
	}
}

func StartRemoteIndexRefresh() bool {
	if Rebuilding() || !rebuildGate.TryRLock() {
		return false
	}
	if Rebuilding() {
		rebuildGate.RUnlock()
		return false
	}

	remoteIndexRefresh.Lock()
	if remoteIndexRefresh.status.Running {
		remoteIndexRefresh.Unlock()
		rebuildGate.RUnlock()
		return false
	}
	startedAt := time.Now()
	ctx, cancel := context.WithCancel(context.Background())
	remoteIndexRefresh.status = RemoteIndexRefreshStatus{Running: true, StartedAt: &startedAt}
	remoteIndexRefresh.cancel = cancel
	remoteIndexRefresh.Unlock()

	go func() {
		defer rebuildGate.RUnlock()
		runRemoteIndexRefresh(ctx)
	}()
	return true
}

func StopRemoteIndexRefresh() {
	remoteIndexRefresh.Lock()
	if remoteIndexRefresh.cancel != nil {
		remoteIndexRefresh.cancel()
	}
	remoteIndexRefresh.Unlock()
}

func runRemoteIndexRefresh(ctx context.Context) {
	var parents []string
	if err := db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("is_dir = ? AND state <> ?", false, StateDeleted).
		Distinct("parent").Pluck("parent", &parents).Error; err != nil {
		finishRemoteIndexRefresh(1, 0, 1)
		return
	}
	normalizedParents := parents[:0]
	for _, parent := range parents {
		if parent == "" {
			continue
		}
		normalizedParents = append(normalizedParents, utils.FixAndCleanPath(parent))
	}
	parents = normalizedParents
	sort.Strings(parents)
	parents = compactParentPaths(parents)

	remoteIndexRefresh.Lock()
	remoteIndexRefresh.status.Total = len(parents)
	remoteIndexRefresh.Unlock()
	if len(parents) == 0 {
		finishRemoteIndexRefresh(0, 0, 0)
		return
	}

	configured := 1
	if conf.Conf != nil {
		configured = providerProbeWorkerLimit(conf.Conf.WebDAVWriteback.Workers, conf.Conf.WebDAVWriteback.ProviderProbeWorkers)
	}
	slots := currentProviderProbeSlots()
	if slots == nil {
		slots = make(chan struct{}, configured)
	}
	workerCount := min(max(1, configured), max(1, cap(slots)))
	jobs := make(chan string)
	var workers sync.WaitGroup
	var completed, failed int
	var resultMu sync.Mutex
	for i := 0; i < workerCount; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case parent, ok := <-jobs:
					if !ok {
						return
					}
					_, err := providerFreshList(ctx, ctx.Done(), slots, parent)
					resultMu.Lock()
					if err != nil {
						failed++
					} else {
						completed++
					}
					progress := completed + failed
					failureCount := failed
					resultMu.Unlock()

					remoteIndexRefresh.Lock()
					remoteIndexRefresh.status.Completed = progress
					remoteIndexRefresh.status.Failed = failureCount
					remoteIndexRefresh.Unlock()
				}
			}
		}()
	}

sendJobs:
	for _, parent := range parents {
		select {
		case jobs <- parent:
		case <-ctx.Done():
			break sendJobs
		}
	}
	close(jobs)
	workers.Wait()
	finishRemoteIndexRefresh(len(parents), completed, failed)
}

func compactParentPaths(paths []string) []string {
	if len(paths) < 2 {
		return paths
	}
	result := paths[:1]
	for _, parent := range paths[1:] {
		if parent != result[len(result)-1] {
			result = append(result, parent)
		}
	}
	return result
}

func finishRemoteIndexRefresh(total, completed, failed int) {
	now := time.Now()
	remoteIndexRefresh.Lock()
	remoteIndexRefresh.status.Running = false
	remoteIndexRefresh.status.Total = total
	remoteIndexRefresh.status.Completed = completed + failed
	remoteIndexRefresh.status.Failed = failed
	remoteIndexRefresh.status.FinishedAt = &now
	if remoteIndexRefresh.cancel != nil {
		remoteIndexRefresh.cancel()
		remoteIndexRefresh.cancel = nil
	}
	remoteIndexRefresh.Unlock()
}

func RemoteIndexSnapshot() (RemoteIndexStats, error) {
	var aggregate struct {
		Directories int64      `gorm:"column:directories"`
		Objects     int64      `gorm:"column:objects"`
		LastAt      *time.Time `gorm:"column:last_at"`
	}
	if err := db.GetDb().Model(&model.WebDAVWritebackRemoteSnapshot{}).
		Select("COUNT(*) AS directories, COALESCE(SUM(object_count), 0) AS objects, MAX(refreshed_at) AS last_at").
		Scan(&aggregate).Error; err != nil {
		return RemoteIndexStats{}, err
	}
	return RemoteIndexStats{
		Directories: aggregate.Directories,
		Objects:     aggregate.Objects,
		LastAt:      aggregate.LastAt,
		Refresh:     RemoteIndexRefreshSnapshot(),
	}, nil
}

func RemoteIndexRefreshSnapshot() RemoteIndexRefreshStatus {
	remoteIndexRefresh.Lock()
	defer remoteIndexRefresh.Unlock()
	return remoteIndexRefresh.status
}

func clearRemoteIndexSnapshots(tx *gorm.DB) (int64, error) {
	res := tx.Session(&gorm.Session{AllowGlobalUpdate: true}).Delete(&model.WebDAVWritebackRemoteSnapshot{})
	return res.RowsAffected, res.Error
}
