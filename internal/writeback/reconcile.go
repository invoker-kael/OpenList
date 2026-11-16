package writeback

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/fs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	ReconcileStateCollecting   = "collecting"
	ReconcileStateCollected    = "collected"
	ReconcileStateComparing    = "comparing"
	ReconcileStateCompareError = "compare_failed"
	ReconcileStatePreview      = "preview"
	ReconcileStateDeleting     = "deleting"
	ReconcileStateDeleteError  = "delete_failed"
	ReconcileStateReady        = "ready"
	ReconcileStateCompleted    = "completed"
	ReconcileStateCanceled     = "canceled"

	reconcileResultPending  = "pending"
	reconcileResultMatch    = "match"
	reconcileResultMissing  = "missing"
	reconcileResultMismatch = "mismatch"
	reconcileResultExtra    = "extra"
	reconcileResultUnknown  = "unknown"
)

var (
	ErrReconciliationNotCollecting = errors.New("Cloud Sync inventory collection is closed")
	ErrReconciliationPathConflict  = errors.New("path type conflicts with the Cloud Sync inventory")
)

type ReconciliationCandidate struct {
	Path            string `json:"path"`
	Size            int64  `json:"size"`
	RemoteSize      int64  `json:"remote_size"`
	RemoteSHA1      string `json:"remote_sha1"`
	RemoteObjectID  string `json:"remote_object_id"`
	IsDir           bool   `json:"is_dir"`
	Result          string `json:"result"`
}

var reconciliationJobs = struct {
	sync.Mutex
	runs map[uint]bool
}{runs: make(map[uint]bool)}

func activeReconciliationStates() []string {
	return []string{
		ReconcileStateCollecting,
		ReconcileStateCollected,
		ReconcileStateComparing,
		ReconcileStateCompareError,
		ReconcileStatePreview,
		ReconcileStateDeleting,
		ReconcileStateDeleteError,
		ReconcileStateReady,
	}
}

func currentReconciliation() (*model.WebDAVWritebackReconcileRun, error) {
	var run model.WebDAVWritebackReconcileRun
	err := db.GetDb().Where("state IN ?", activeReconciliationStates()).Order("id DESC").Take(&run).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &run, nil
}

func reconciliationForPath(p string) (*model.WebDAVWritebackReconcileRun, bool, error) {
	run, err := currentReconciliation()
	if err != nil || run == nil {
		return run, false, err
	}
	p = utils.FixAndCleanPath(p)
	root := utils.FixAndCleanPath(run.Root)
	return run, isPathOrDescendant(p, root), nil
}

// LockForWritebackDisable serializes disabling Writeback with task creation.
// The caller must invoke the returned release function after saving settings.
func LockForWritebackDisable() (func(), error) {
	if !rebuildGate.TryLock() {
		return nil, errors.New("writeback is busy; retry after active requests finish")
	}
	run, err := currentReconciliation()
	if err != nil {
		rebuildGate.Unlock()
		return nil, err
	}
	if run != nil {
		rebuildGate.Unlock()
		return nil, errors.New("finish or cancel the Cloud Sync inventory task before disabling Writeback")
	}
	return rebuildGate.Unlock, nil
}

func ReconciliationBlocksMutation(p string) (bool, error) {
	_, applies, err := reconciliationForPath(p)
	return applies, err
}

func ReconciliationAcceptsPut(p string) (bool, error) {
	run, applies, err := reconciliationForPath(p)
	return applies && run != nil && run.State == ReconcileStateCollecting, err
}

// ReconciliationViewObject returns a temporary metadata-only WebDAV object.
// active distinguishes an empty reconciliation view from ordinary storage.
func ReconciliationViewObject(p string) (model.Obj, bool, bool, error) {
	run, applies, err := reconciliationForPath(p)
	if err != nil || !applies || run == nil {
		return nil, false, applies, err
	}
	p = utils.FixAndCleanPath(p)
	root := utils.FixAndCleanPath(run.Root)
	if p == root {
		return reconciliationCanonicalObject(run.ID, root, path.Base(root), 0, true, run.UpdatedAt, ""), true, true, nil
	}
	if run.State == ReconcileStateCollecting {
		return nil, false, true, nil
	}
	var entry model.WebDAVWritebackReconcileEntry
	err = db.GetDb().Where("run_id = ? AND path_key = ? AND local_present = ?", run.ID, pathKey(p), true).Take(&entry).Error
	if err == nil {
		return reconciliationCanonicalObject(run.ID, entry.Path, entry.Name, entry.Size, entry.IsDir, entry.ModTime, entry.SHA1), true, true, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, false, true, err
	}
	var descendants int64
	if err := db.GetDb().Model(&model.WebDAVWritebackReconcileEntry{}).
		Where("run_id = ? AND local_present = ? AND path LIKE ? ESCAPE '~'", run.ID, true, descendantLikePattern(p)).
		Limit(1).Count(&descendants).Error; err != nil {
		return nil, false, true, err
	}
	if descendants > 0 {
		return reconciliationCanonicalObject(run.ID, p, path.Base(p), 0, true, run.UpdatedAt, ""), true, true, nil
	}
	return nil, false, true, nil
}

func reconciliationCanonicalObject(runID uint, p, name string, size int64, isDir bool, modTime time.Time, sha1Value string) model.Obj {
	if modTime.IsZero() {
		modTime = time.Now()
	}
	info := model.Object{Path: p, Name: name, Size: size, Modified: modTime, IsFolder: isDir}
	if sha1Value != "" {
		info.HashInfo = utils.NewHashInfo(utils.SHA1, strings.ToLower(sha1Value))
	}
	etagKey := pathKey(p + "\x00" + strings.ToLower(sha1Value))
	etagGeneration := uint64(runID)
	if !modTime.IsZero() {
		etagGeneration ^= uint64(modTime.UnixNano())
	}
	return &CanonicalObject{Object: info, etag: canonicalETag(etagKey, etagGeneration, size)}
}

func reconciliationStorageMatches(run *model.WebDAVWritebackReconcileRun, p string) error {
	root := utils.FixAndCleanPath(run.Root)
	bestMount, matchingStorages := "", 0
	for _, candidate := range op.GetAllStorages() {
		mount := utils.FixAndCleanPath(utils.GetActualMountPath(candidate.GetStorage().MountPath))
		if mount != root && isPathOrDescendant(mount, root) {
			return errors.New("a nested storage mapping appeared during reconciliation")
		}
		if isPathOrDescendant(root, mount) {
			if len(mount) > len(bestMount) {
				bestMount, matchingStorages = mount, 1
			} else if mount == bestMount {
				matchingStorages++
			}
		}
	}
	if matchingStorages != 1 || bestMount != utils.FixAndCleanPath(utils.GetActualMountPath(run.StorageMountPath)) {
		return errors.New("the storage mappings changed during reconciliation")
	}
	storage, _, err := op.GetStorageAndActualPath(p)
	if err != nil || storage == nil {
		return errors.New("the selected storage is unavailable; no remote changes were made")
	}
	meta := storage.GetStorage()
	if meta.ID != run.StorageID || meta.Driver != run.StorageDriver ||
		utils.FixAndCleanPath(meta.MountPath) != utils.FixAndCleanPath(run.StorageMountPath) {
		return errors.New("the storage mapping changed during reconciliation; no further remote changes were made")
	}
	return nil
}

func reconciliationFreshList(ctx context.Context, stop <-chan struct{}, slots chan struct{}, run *model.WebDAVWritebackReconcileRun, parent string) ([]model.Obj, error) {
	if err := reconciliationStorageMatches(run, parent); err != nil {
		return nil, err
	}
	objects, err := providerFreshList(ctx, stop, slots, parent)
	if err != nil {
		return nil, err
	}
	filtered := objects[:0]
	for _, object := range objects {
		if object == nil {
			return nil, errors.New("115 returned an invalid directory entry")
		}
		if !model.ObjHasMask(object, model.Virtual) {
			filtered = append(filtered, object)
		}
	}
	return filtered, nil
}

func reconciliationHasSHA1(value string) bool {
	if len(strings.TrimSpace(value)) != sha1.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func reconciliationValidName(name string) bool {
	return name != "" && name != "." && name != ".." && path.Base(name) == name && !strings.ContainsRune(name, '\x00')
}

// ReconciliationChildren supplies the task's source manifest to WebDAV
// PROPFIND without consulting or exposing the 115 directory.
func ReconciliationChildren(parent string) ([]model.Obj, bool, error) {
	run, applies, err := reconciliationForPath(parent)
	if err != nil || !applies || run == nil {
		return nil, false, err
	}
	if run.State == ReconcileStateCollecting {
		return []model.Obj{}, true, nil
	}
	var entries []model.WebDAVWritebackReconcileEntry
	if err := db.GetDb().Where("run_id = ? AND parent_key = ? AND local_present = ?", run.ID, pathKey(parent), true).
		Order("name ASC").Find(&entries).Error; err != nil {
		return nil, true, err
	}
	children := make([]model.Obj, 0, len(entries))
	for i := range entries {
		entry := &entries[i]
		modTime := entry.ModTime
		if entry.IsDir {
			modTime = run.UpdatedAt
		}
		children = append(children, reconciliationCanonicalObject(run.ID, entry.Path, entry.Name, entry.Size, entry.IsDir, modTime, entry.SHA1))
	}
	return children, true, nil
}

func saveReconciliationEntry(tx *gorm.DB, entry *model.WebDAVWritebackReconcileEntry) error {
	return tx.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "run_id"}, {Name: "path_key"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"parent_key", "path", "parent", "name", "is_dir", "local_present", "size", "sha1",
			"mod_time", "create_time", "mime_type", "result", "updated_at",
		}),
	}).Create(entry).Error
}

func saveReconciliationDirectory(tx *gorm.DB, runID uint, root, p string, modTime time.Time) error {
	root = utils.FixAndCleanPath(root)
	p = utils.FixAndCleanPath(p)
	if p == root {
		return nil
	}
	entry := model.WebDAVWritebackReconcileEntry{
		RunID:       runID,
		PathKey:     pathKey(p),
		ParentKey:   pathKey(path.Dir(p)),
		Path:        p,
		Parent:      path.Dir(p),
		Name:        path.Base(p),
		IsDir:       true,
		LocalPresent: true,
		ModTime:     modTime,
		CreateTime:  modTime,
		Result:      reconcileResultPending,
	}
	var existing model.WebDAVWritebackReconcileEntry
	err := tx.Where("run_id = ? AND path_key = ?", runID, entry.PathKey).Take(&existing).Error
	if err == nil && !existing.IsDir {
		return fmt.Errorf("path type conflict in reconciliation manifest")
	}
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}
	return saveReconciliationEntry(tx, &entry)
}

func recordReconciliationParents(tx *gorm.DB, run *model.WebDAVWritebackReconcileRun, p string, modTime time.Time) error {
	root := utils.FixAndCleanPath(run.Root)
	for parent := path.Dir(p); isPathOrDescendant(parent, root) && parent != root; parent = path.Dir(parent) {
		if err := saveReconciliationDirectory(tx, run.ID, root, parent, modTime); err != nil {
			return err
		}
		if parent == "/" || path.Dir(parent) == parent {
			break
		}
	}
	return nil
}

// CaptureReconciliationPut streams the request body into SHA-1 and discards
// it. It never creates a spool file or enters the ordinary upload worker queue.
func CaptureReconciliationPut(ctx context.Context, p string, body io.Reader, expectedSize int64, modTime, createTime time.Time, mime string) (created bool, handled bool, err error) {
	p = utils.FixAndCleanPath(p)
	run, applies, err := reconciliationForPath(p)
	if err != nil || !applies || run == nil {
		return false, applies, err
	}
	if !enterWritebackMutation() {
		return false, true, ErrRebuildInProgress
	}
	defer rebuildGate.RUnlock()
	run, applies, err = reconciliationForPath(p)
	if err != nil {
		return false, true, err
	}
	if !applies || run == nil {
		// The task may have been canceled while this PUT waited for the gate.
		// Let the normal WebDAV path handle the request instead of ACKing data
		// that was never recorded in either inventory.
		return false, false, nil
	}
	if run.State != ReconcileStateCollecting {
		return false, true, ErrReconciliationNotCollecting
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return false, true, err
	}
	if p == utils.FixAndCleanPath(run.Root) {
		return false, true, errors.New("reconciliation root cannot be a file")
	}
	if modTime.IsZero() {
		modTime = time.Now()
	}
	if createTime.IsZero() {
		createTime = modTime
	}
	hasher := sha1.New()
	actualSize, err := io.Copy(io.MultiWriter(hasher, io.Discard), body)
	if err != nil {
		return false, true, err
	}
	if expectedSize >= 0 && actualSize != expectedSize {
		return false, true, fmt.Errorf("received size does not match Content-Length")
	}
	digest := hex.EncodeToString(hasher.Sum(nil))
	parent := path.Dir(p)
	entry := model.WebDAVWritebackReconcileEntry{
		RunID:        run.ID,
		PathKey:      pathKey(p),
		ParentKey:    pathKey(parent),
		Path:         p,
		Parent:       parent,
		Name:         path.Base(p),
		LocalPresent: true,
		Size:         actualSize,
		SHA1:         digest,
		ModTime:      modTime,
		CreateTime:   createTime,
		MimeType:     mime,
		Result:       reconcileResultPending,
	}
	created = false
	err = db.GetDb().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var currentRun model.WebDAVWritebackReconcileRun
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Take(&currentRun, run.ID).Error; err != nil {
			return err
		}
		if currentRun.State != ReconcileStateCollecting {
			return ErrReconciliationNotCollecting
		}
		var old model.WebDAVWritebackReconcileEntry
		oldErr := tx.Where("run_id = ? AND path_key = ?", run.ID, entry.PathKey).Take(&old).Error
		if oldErr != nil && !errors.Is(oldErr, gorm.ErrRecordNotFound) {
			return oldErr
		}
		if oldErr == nil && old.IsDir {
			return ErrReconciliationPathConflict
		}
		created = errors.Is(oldErr, gorm.ErrRecordNotFound)
		if err := recordReconciliationParents(tx, &currentRun, p, modTime); err != nil {
			return err
		}
		if err := saveReconciliationEntry(tx, &entry); err != nil {
			return err
		}
		filesDelta := int64(0)
		bytesDelta := actualSize
		if created {
			filesDelta = 1
		} else {
			bytesDelta -= old.Size
		}
		return tx.Model(&model.WebDAVWritebackReconcileRun{}).Where("id = ?", run.ID).Updates(map[string]any{
			"source_files": gorm.Expr("source_files + ?", filesDelta),
			"source_bytes": gorm.Expr("source_bytes + ?", bytesDelta),
		}).Error
	})
	return created, true, err
}

func RecordReconciliationDirectory(ctx context.Context, p string, modTime time.Time) (created bool, handled bool, err error) {
	p = utils.FixAndCleanPath(p)
	run, applies, err := reconciliationForPath(p)
	if err != nil || !applies || run == nil {
		return false, applies, err
	}
	if !enterWritebackMutation() {
		return false, true, ErrRebuildInProgress
	}
	defer rebuildGate.RUnlock()
	run, applies, err = reconciliationForPath(p)
	if err != nil {
		return false, true, err
	}
	if !applies || run == nil {
		return false, false, nil
	}
	if run.State != ReconcileStateCollecting {
		return false, true, ErrReconciliationNotCollecting
	}
	if modTime.IsZero() {
		modTime = time.Now()
	}
	if p == utils.FixAndCleanPath(run.Root) {
		return false, true, nil
	}
	created = false
	err = db.GetDb().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var currentRun model.WebDAVWritebackReconcileRun
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Take(&currentRun, run.ID).Error; err != nil {
			return err
		}
		if currentRun.State != ReconcileStateCollecting {
			return ErrReconciliationNotCollecting
		}
		var existing model.WebDAVWritebackReconcileEntry
		err := tx.Where("run_id = ? AND path_key = ?", run.ID, pathKey(p)).Take(&existing).Error
		if err == nil {
			if !existing.IsDir {
				return ErrReconciliationPathConflict
			}
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		created = true
		if err := recordReconciliationParents(tx, &currentRun, p, modTime); err != nil {
			return err
		}
		if err := saveReconciliationDirectory(tx, run.ID, currentRun.Root, p, modTime); err != nil {
			return err
		}
		return tx.Model(&model.WebDAVWritebackReconcileRun{}).Where("id = ?", run.ID).
			Update("updated_at", time.Now()).Error
	})
	return created, true, err
}

func StartReconciliation(root string) (*model.WebDAVWritebackReconcileRun, error) {
	if !Enabled() {
		return nil, errors.New("WebDAV writeback is disabled")
	}
	root = utils.FixAndCleanPath(root)
	if root == "/" {
		return nil, errors.New("choose a specific WebDAV root; the global root is not allowed")
	}
	targetStorage, _, err := op.GetStorageAndActualPath(root)
	if err != nil || targetStorage == nil {
		return nil, errors.New("the reconciliation root must be inside one mounted storage")
	}
	targetMeta := targetStorage.GetStorage()
	targetStorageID := targetMeta.ID
	targetDriver := targetMeta.Driver
	targetMountPath := targetMeta.MountPath
	if targetDriver != "115" && targetDriver != "115_open" {
		return nil, errors.New("the reconciliation root must use the 115 or 115 Open storage driver")
	}
	bestMount, matchingStorages := "", 0
	for _, storage := range op.GetAllStorages() {
		mount := utils.FixAndCleanPath(utils.GetActualMountPath(storage.GetStorage().MountPath))
		if mount != root && isPathOrDescendant(mount, root) {
			return nil, errors.New("the reconciliation root cannot contain another mounted storage")
		}
		if isPathOrDescendant(root, mount) {
			if len(mount) > len(bestMount) {
				bestMount, matchingStorages = mount, 1
			} else if mount == bestMount {
				matchingStorages++
			}
		}
	}
	if matchingStorages != 1 {
		return nil, errors.New("the reconciliation root must resolve to exactly one storage")
	}
	rootObject, err := fs.Get(context.Background(), root, &fs.GetArgs{NoLog: true})
	if err != nil || rootObject == nil || !rootObject.IsDir() {
		return nil, errors.New("reconciliation root must be an existing directory")
	}
	if !rebuildGate.TryLock() {
		return nil, errors.New("writeback is receiving or rebuilding; retry after it is idle")
	}
	defer rebuildGate.Unlock()
	if Rebuilding() {
		return nil, ErrRebuildInProgress
	}
	if !Enabled() {
		return nil, errors.New("WebDAV writeback was disabled; enable it and retry")
	}
	startingRun := model.WebDAVWritebackReconcileRun{
		Root: root, StorageID: targetStorageID, StorageDriver: targetDriver, StorageMountPath: targetMountPath,
	}
	if err := reconciliationStorageMatches(&startingRun, root); err != nil {
		return nil, errors.New("the storage mapping changed while starting the reconciliation task")
	}

	var run model.WebDAVWritebackReconcileRun
	err = db.GetDb().Transaction(func(tx *gorm.DB) error {
		var existing model.WebDAVWritebackReconcileRun
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("state IN ?", activeReconciliationStates()).Take(&existing).Error; err == nil {
			return errors.New("another reconciliation task is active")
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		var activeRows int64
		if err := tx.Model(&model.WebDAVWritebackObject{}).
			Where("(path = ? OR path LIKE ? ESCAPE '~')", root, descendantLikePattern(root)).
			Where("state IN ?", []string{StateQueued, StateUploading, StateVerifying, StateLockNull}).
			Count(&activeRows).Error; err != nil {
			return err
		}
		if activeRows != 0 {
			return errors.New("finish or resolve existing writeback work under this root first")
		}
		var receiving int64
		if err := tx.Model(&model.WebDAVWritebackReceiveFence{}).
			Where("active_receivers > 0 AND receive_lease_until > ?", time.Now()).
			Where("(path = ? OR path LIKE ? ESCAPE '~')", root, descendantLikePattern(root)).
			Count(&receiving).Error; err != nil {
			return err
		}
		if receiving != 0 {
			return errors.New("wait for active WebDAV PUT requests under this root")
		}
		ops, err := activeProviderOperations()
		if err != nil {
			return err
		}
		for i := range ops {
			if pathsOverlap(ops[i].SourcePath, root) || pathsOverlap(ops[i].DestinationPath, root) {
				return errors.New("finish provider operations under this root first")
			}
		}
		run = model.WebDAVWritebackReconcileRun{
			Root: root, StorageID: targetStorageID, StorageDriver: targetDriver,
			StorageMountPath: targetMountPath, State: ReconcileStateCollecting, StartedAt: time.Now(),
		}
		return tx.Create(&run).Error
	})
	if err != nil {
		return nil, err
	}
	InvalidateProviderSnapshots(root)
	return &run, nil
}

func CurrentReconciliation() (*model.WebDAVWritebackReconcileRun, error) {
	run, err := currentReconciliation()
	if err != nil || run == nil {
		return run, err
	}
	switch run.State {
	case ReconcileStateComparing:
		startReconciliationJob(run.ID, runReconciliationComparison)
	case ReconcileStateDeleting:
		startReconciliationJob(run.ID, runReconciliationDeletion)
	}
	return run, nil
}

func LatestReconciliation() (*model.WebDAVWritebackReconcileRun, error) {
	var run model.WebDAVWritebackReconcileRun
	err := db.GetDb().Order("id DESC").Take(&run).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if run.State == ReconcileStateComparing {
		startReconciliationJob(run.ID, runReconciliationComparison)
	} else if run.State == ReconcileStateDeleting {
		startReconciliationJob(run.ID, runReconciliationDeletion)
	}
	return &run, nil
}

func resumeReconciliationJobs() {
	run, err := currentReconciliation()
	if err != nil || run == nil {
		return
	}
	switch run.State {
	case ReconcileStateComparing:
		startReconciliationJob(run.ID, runReconciliationComparison)
	case ReconcileStateDeleting:
		startReconciliationJob(run.ID, runReconciliationDeletion)
	}
}

func startReconciliationJob(runID uint, job func(uint)) bool {
	reconciliationJobs.Lock()
	if reconciliationJobs.runs[runID] {
		reconciliationJobs.Unlock()
		return false
	}
	reconciliationJobs.runs[runID] = true
	reconciliationJobs.Unlock()
	go func() {
		defer func() {
			reconciliationJobs.Lock()
			delete(reconciliationJobs.runs, runID)
			reconciliationJobs.Unlock()
		}()
		job(runID)
	}()
	return true
}

func FinishReconciliationCollection(runID uint) (bool, error) {
	if !rebuildGate.TryLock() {
		return false, errors.New("wait for active Cloud Sync PUT requests to finish")
	}
	defer rebuildGate.Unlock()
	run, err := currentReconciliation()
	if err != nil || run == nil {
		return false, err
	}
	if run.ID != runID {
		return false, errors.New("the reconciliation run changed; refresh before continuing")
	}
	if run.State == ReconcileStateCompareError || run.State == ReconcileStateDeleteError {
		return startReconciliationComparisonLocked(run)
	}
	if run.State != ReconcileStateCollecting {
		return false, nil
	}
	now := time.Now()
	res := db.GetDb().Model(&model.WebDAVWritebackReconcileRun{}).
		Where("id = ? AND state = ?", run.ID, ReconcileStateCollecting).
		Updates(map[string]any{
			"state": ReconcileStateCollected, "last_error": "", "collection_ended_at": &now,
		})
	if res.Error != nil {
		return false, res.Error
	}
	return res.RowsAffected > 0, nil
}

func StartReconciliationComparison(runID uint) (bool, error) {
	if !rebuildGate.TryLock() {
		return false, errors.New("wait for active Cloud Sync PUT requests to finish")
	}
	defer rebuildGate.Unlock()
	run, err := currentReconciliation()
	if err != nil || run == nil {
		return false, err
	}
	if run.ID != runID {
		return false, errors.New("the reconciliation run changed; refresh before continuing")
	}
	return startReconciliationComparisonLocked(run)
}

func startReconciliationComparisonLocked(run *model.WebDAVWritebackReconcileRun) (bool, error) {
	if run.State != ReconcileStateCollected && run.State != ReconcileStateCompareError && run.State != ReconcileStateDeleteError {
		return false, nil
	}
	res := db.GetDb().Model(&model.WebDAVWritebackReconcileRun{}).
		Where("id = ? AND state IN ?", run.ID, []string{ReconcileStateCollected, ReconcileStateCompareError, ReconcileStateDeleteError}).
		Updates(map[string]any{
			"state": ReconcileStateComparing, "last_error": "",
			"compared_at": nil, "remote_files": 0, "matched_files": 0,
			"missing_files": 0, "extra_files": 0, "unknown_files": 0,
			"scanned_directories": 0,
		})
	if res.Error != nil {
		return false, res.Error
	}
	if res.RowsAffected == 0 {
		return false, nil
	}
	startReconciliationJob(run.ID, runReconciliationComparison)
	return true, nil
}
func CancelReconciliation(runID uint) error {
	if !rebuildGate.TryLock() {
		return errors.New("wait for active Cloud Sync PUT requests to finish")
	}
	defer rebuildGate.Unlock()
	var parents []string
	var spoolPaths []string
	err := db.GetDb().Transaction(func(tx *gorm.DB) error {
		var run model.WebDAVWritebackReconcileRun
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ? AND state IN ?", runID, []string{
			ReconcileStateCollecting, ReconcileStateCollected, ReconcileStateCompareError, ReconcileStatePreview,
		}).Take(&run).Error; err != nil {
			return err
		}
		parents = append(parents, utils.FixAndCleanPath(run.Root))
		var entries []model.WebDAVWritebackReconcileEntry
		if err := tx.Where("run_id = ?", run.ID).Find(&entries).Error; err != nil {
			return err
		}
		for _, entry := range entries {
			parents = append(parents, entry.Parent)
			if entry.IsDir {
				parents = append(parents, entry.Path)
			}
			if !entry.LocalPresent {
				continue
			}
			var row model.WebDAVWritebackObject
			rowErr := tx.Where("path_key = ?", entry.PathKey).Take(&row).Error
			if rowErr != nil && !errors.Is(rowErr, gorm.ErrRecordNotFound) {
				return rowErr
			}
			exists := rowErr == nil
			if exists && row.SpoolPath != "" {
				spoolPaths = append(spoolPaths, row.SpoolPath)
			}
			generation := uint64(1)
			if exists {
				generation = nextReceiveGeneration(row.Generation, 0)
			}
			row.PathKey = entry.PathKey
			row.ParentKey = entry.ParentKey
			row.Path = entry.Path
			row.Parent = entry.Parent
			row.Name = entry.Name
			row.IsDir = entry.IsDir
			row.Size = entry.Size
			row.ModTime = entry.ModTime
			row.CreateTime = entry.CreateTime
			row.MimeType = entry.MimeType
			row.Generation = generation
			row.ETag = canonicalETag(entry.PathKey, generation, entry.Size)
			row.CanonicalState = CanonicalStateDeleted
			row.State = StateWaitingCloudSyncReupload
			row.CloudSyncReuploadRequired = true
			row.ResolutionReason = ResolutionNeedsCloudSyncRehydrate
			row.LastError = "inventory was canceled; waiting for Cloud Sync to resend this path"
			row.ReceiveStartedAt = nil
			row.PayloadSHA1 = ""
			row.SpoolPath = ""
			row.CleanupPath = ""
			row.RemoteObjectID = ""
			row.RemoteSHA1 = ""
			row.RemoteGeneration = 0
			row.RemoteVerifiedAt = nil
			row.RetryAt = nil
			row.RetryCount = 0
			row.VerifyCount = 0
			row.Paused = false
			row.AckTime = nil
			row.DurableAt = nil
			row.ProviderUploadStartedAt = nil
			row.ProviderUploadCompletedAt = nil
			row.ProviderUploadedBytes = 0
			row.ProviderUploadAttempts = 0
			row.ProviderRetryBytes = 0
			row.ProviderEvidenceFirstAt = nil
			row.ProviderEvidenceLastAt = nil
			row.ProviderEvidenceCount = 0
			row.ProviderEvidenceResult = ""
			row.RestartUploadRecovery = false
			row.RecoveryStartedAt = nil
			row.CompletedAt = nil
			if exists {
				if err := tx.Save(&row).Error; err != nil {
					return err
				}
			} else if err := tx.Create(&row).Error; err != nil {
				return err
			}
		}
		if err := tx.Where("run_id = ?", run.ID).Delete(&model.WebDAVWritebackReconcileEntry{}).Error; err != nil {
			return err
		}
		return tx.Model(&run).Updates(map[string]any{
			"state": ReconcileStateCanceled, "last_error": "", "completed_at": time.Now(),
		}).Error
	})
	if err == nil {
		for _, spoolPath := range spoolPaths {
			removeSpoolIfUnreferenced(spoolPath)
		}
		InvalidateProviderSnapshots(parents...)
	}
	return err
}

func ReconciliationCanCancel(runID uint) (bool, error) {
	var run model.WebDAVWritebackReconcileRun
	if err := db.GetDb().Take(&run, runID).Error; err != nil {
		return false, err
	}
	return run.State == ReconcileStateCollecting || run.State == ReconcileStateCollected || run.State == ReconcileStateCompareError || run.State == ReconcileStatePreview, nil
}

func ReconciliationCandidateCount() (int64, error) {
	run, err := currentReconciliation()
	if err != nil || run == nil {
		return 0, err
	}
	var count int64
	err = db.GetDb().Model(&model.WebDAVWritebackReconcileEntry{}).
		Where("run_id = ? AND result IN ?", run.ID, []string{reconcileResultExtra, reconcileResultMismatch}).
		Count(&count).Error
	return count, err
}

func ReconciliationCandidates(page, limit int) ([]ReconciliationCandidate, error) {
	run, err := currentReconciliation()
	if err != nil || run == nil {
		return nil, err
	}
	if limit < 1 || limit > 200 {
		limit = 100
	}
	if page < 1 {
		page = 1
	}
	var entries []model.WebDAVWritebackReconcileEntry
	if err := db.GetDb().Where("run_id = ? AND result IN ?", run.ID, []string{reconcileResultExtra, reconcileResultMismatch}).
		Order("path ASC").Limit(limit).Offset((page - 1) * limit).Find(&entries).Error; err != nil {
		return nil, err
	}
	candidates := make([]ReconciliationCandidate, 0, len(entries))
	for _, entry := range entries {
		candidates = append(candidates, ReconciliationCandidate{
			Path: entry.Path, Size: entry.Size, RemoteSize: entry.RemoteSize,
			RemoteSHA1: entry.RemoteSHA1, RemoteObjectID: entry.RemoteObjectID,
			IsDir: entry.IsDir, Result: entry.Result,
		})
	}
	return candidates, nil
}

func ApplyReconciliationDeletes(runID uint, expectedCandidates int64) (bool, error) {
	run, err := currentReconciliation()
	if err != nil || run == nil {
		return false, err
	}
	if run.ID != runID {
		return false, errors.New("the reconciliation run changed; refresh and review the candidates again")
	}
	if run.State != ReconcileStatePreview && run.State != ReconcileStateDeleteError {
		return false, nil
	}
	var candidateCount int64
	if err := db.GetDb().Model(&model.WebDAVWritebackReconcileEntry{}).
		Where("run_id = ? AND result IN ?", run.ID, []string{reconcileResultExtra, reconcileResultMismatch}).
		Count(&candidateCount).Error; err != nil {
		return false, err
	}
	if candidateCount == 0 || candidateCount != expectedCandidates {
		return false, errors.New("the candidate count changed; refresh and review the candidates again")
	}
	res := db.GetDb().Model(&model.WebDAVWritebackReconcileRun{}).
		Where("id = ? AND state IN ?", run.ID, []string{ReconcileStatePreview, ReconcileStateDeleteError}).
		Updates(map[string]any{"state": ReconcileStateDeleting, "last_error": ""})
	if res.Error != nil {
		return false, res.Error
	}
	if res.RowsAffected == 0 {
		return false, nil
	}
	startReconciliationJob(run.ID, runReconciliationDeletion)
	return true, nil
}

func CloseReconciliation(runID uint) error {
	if !rebuildGate.TryLock() {
		return errors.New("writeback is busy; retry when it is idle")
	}
	defer rebuildGate.Unlock()
	run, err := currentReconciliation()
	if err != nil || run == nil {
		return err
	}
	if run.State != ReconcileStateReady {
		return errors.New("reconciliation is not ready to close")
	}
	if run.ID != runID {
		return errors.New("the reconciliation run changed; refresh before finishing")
	}
	if err := reconciliationStorageMatches(run, run.Root); err != nil {
		return err
	}
	var spoolPaths []string
	snapshotParents := map[string]struct{}{utils.FixAndCleanPath(run.Root): {}}
	now := time.Now()
	verifiedAt := now
	if run.ComparedAt != nil {
		verifiedAt = *run.ComparedAt
	}
	err = db.GetDb().Transaction(func(tx *gorm.DB) error {
		var locked model.WebDAVWritebackReconcileRun
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Take(&locked, run.ID).Error; err != nil {
			return err
		}
		if locked.State != ReconcileStateReady {
			return errors.New("reconciliation is not ready to close")
		}
		var allEntries []model.WebDAVWritebackReconcileEntry
		if err := tx.Where("run_id = ?", run.ID).Find(&allEntries).Error; err != nil {
			return err
		}
		entries := make([]model.WebDAVWritebackReconcileEntry, 0, len(allEntries))
		localByKey := make(map[string]model.WebDAVWritebackReconcileEntry, len(allEntries))
		for _, entry := range allEntries {
			snapshotParents[entry.Parent] = struct{}{}
			if entry.IsDir {
				snapshotParents[entry.Path] = struct{}{}
			}
			if !entry.LocalPresent {
				continue
			}
			entries = append(entries, entry)
			localByKey[entry.PathKey] = entry
		}

		var existingRows []model.WebDAVWritebackObject
		if err := tx.Where("(path = ? OR path LIKE ? ESCAPE '~')", locked.Root, descendantLikePattern(locked.Root)).Find(&existingRows).Error; err != nil {
			return err
		}
		rowsByKey := make(map[string]model.WebDAVWritebackObject, len(existingRows))
		for _, row := range existingRows {
			snapshotParents[row.Parent] = struct{}{}
			if _, exists := localByKey[row.PathKey]; !exists {
				if row.SpoolPath != "" {
					spoolPaths = append(spoolPaths, row.SpoolPath)
				}
				// Preserve a canonical tombstone so the provider cannot expose a
				// path that was absent from the Cloud Sync inventory.
				row.CanonicalState = CanonicalStateDeleted
				row.State = StateWaitingCloudSyncReupload
				row.CloudSyncReuploadRequired = true
				row.ResolutionReason = ResolutionNeedsCloudSyncRehydrate
				row.LastError = "reconciliation did not receive this path from Cloud Sync"
				row.SpoolPath = ""
				row.CleanupPath = ""
				row.PayloadSHA1 = ""
				row.Size = 0
				row.RetryAt = nil
				row.Paused = false
				row.AckTime = nil
				row.DurableAt = nil
				row.RemoteVerifiedAt = nil
				row.RemoteGeneration = 0
				row.RemoteObjectID = ""
				row.RemoteSHA1 = ""
				row.CompletedAt = nil
				row.ProviderUploadCompletedAt = nil
				row.ProviderUploadStartedAt = nil
				row.ProviderUploadedBytes = 0
				row.ProviderUploadAttempts = 0
				row.ProviderRetryBytes = 0
				row.ProviderEvidenceFirstAt = nil
				row.ProviderEvidenceLastAt = nil
				row.ProviderEvidenceCount = 0
				row.ProviderEvidenceResult = ""
				if err := tx.Save(&row).Error; err != nil {
					return err
				}
				continue
			}
			rowsByKey[row.PathKey] = row
		}

		for _, entry := range entries {
			row, exists := rowsByKey[entry.PathKey]
			if exists && row.SpoolPath != "" {
				spoolPaths = append(spoolPaths, row.SpoolPath)
			}
			generation := uint64(1)
			if exists {
				generation = nextReceiveGeneration(row.Generation, 0)
			}
			row.PathKey = entry.PathKey
			row.ParentKey = entry.ParentKey
			row.Path = entry.Path
			row.Parent = entry.Parent
			row.Name = entry.Name
			row.IsDir = entry.IsDir
			row.Size = entry.Size
			row.ModTime = entry.ModTime
			row.CreateTime = entry.CreateTime
			row.Generation = generation
			row.ETag = canonicalETag(entry.PathKey, generation, entry.Size)
			row.PayloadSHA1 = entry.SHA1
			row.ProviderEvidenceFirstAt = nil
			row.ProviderEvidenceLastAt = nil
			row.ProviderEvidenceCount = 0
			row.ProviderEvidenceResult = ""
			row.RestartUploadRecovery = false
			row.RecoveryStartedAt = nil
			row.ProviderUploadStartedAt = nil
			row.ProviderUploadedBytes = 0
			row.ProviderUploadAttempts = 0
			row.ProviderRetryBytes = 0
			row.MimeType = entry.MimeType
			row.SpoolPath = ""
			row.CleanupPath = ""
			row.Paused = false
			row.RetryCount = 0
			row.VerifyCount = 0
			row.RetryAt = nil
			row.RemoteGeneration = 0
			row.RemoteVerifiedAt = nil
			row.LastError = ""
			row.ResolutionReason = ""
			row.CloudSyncReuploadRequired = false
			row.ProviderUploadCompletedAt = nil
			row.CompletedAt = nil
			row.RemoteObjectID = ""
			row.RemoteSHA1 = ""
			if entry.Result == reconcileResultMatch {
				ackAt := entry.UpdatedAt
				if ackAt.IsZero() {
					ackAt = now
				}
				row.CanonicalState = CanonicalStateAcked
				row.State = StateCompleted
				row.RemoteGeneration = generation
				row.RemoteVerifiedAt = &verifiedAt
				row.RemoteObjectID = entry.RemoteObjectID
				row.RemoteSHA1 = entry.RemoteSHA1
				row.CompletedAt = &now
				row.AckTime = &ackAt
				row.DurableAt = &ackAt
			} else {
				row.CanonicalState = CanonicalStateDeleted
				row.State = StateWaitingCloudSyncReupload
				row.CloudSyncReuploadRequired = true
				row.ResolutionReason = ResolutionNeedsCloudSyncRehydrate
				row.LastError = "reconciliation found no matching 115 content; waiting for Cloud Sync upload"
				row.AckTime = nil
				row.DurableAt = nil
			}
			if exists {
				if err := tx.Save(&row).Error; err != nil {
					return err
				}
			} else if err := tx.Create(&row).Error; err != nil {
				return err
			}
		}
		if err := tx.Where("run_id = ?", run.ID).Delete(&model.WebDAVWritebackReconcileEntry{}).Error; err != nil {
			return err
		}
		completedAt := now
		return tx.Model(&model.WebDAVWritebackReconcileRun{}).Where("id = ? AND state = ?", run.ID, ReconcileStateReady).
			Updates(map[string]any{"state": ReconcileStateCompleted, "completed_at": &completedAt, "last_error": ""}).Error
	})
	if err != nil {
		return err
	}
	for _, spoolPath := range spoolPaths {
		removeSpoolIfUnreferenced(spoolPath)
	}
	for parent := range snapshotParents {
		InvalidateProviderSnapshots(parent)
	}
	return nil
}

func runReconciliationComparison(runID uint) {
	var run model.WebDAVWritebackReconcileRun
	if err := db.GetDb().Take(&run, runID).Error; err != nil {
		return
	}
	if run.State != ReconcileStateComparing {
		return
	}
	if err := resetReconciliationRemoteResults(runID); err != nil {
		failReconciliation(runID, ReconcileStateCompareError, "could not reset the comparison snapshot")
		return
	}

	ctx := context.Background()
	slots := make(chan struct{}, 1)
	queue := []string{utils.FixAndCleanPath(run.Root)}
	visited := make(map[string]struct{})
	var remoteFiles int64
	var directories int64
	for len(queue) > 0 {
		parent := queue[0]
		queue = queue[1:]
		parent = utils.FixAndCleanPath(parent)
		key := pathKey(parent)
		if _, seen := visited[key]; seen {
			continue
		}
		visited[key] = struct{}{}
		objects, err := reconciliationFreshList(ctx, ctx.Done(), slots, &run, parent)
		if err != nil {
			failReconciliation(runID, ReconcileStateCompareError, "115 directory listing failed; no remote files were deleted")
			return
		}
		directories++
		if err := reconcileRemoteParent(runID, run.Root, parent, objects, &queue, &remoteFiles); err != nil {
			failReconciliation(runID, ReconcileStateCompareError, "remote directory contents could not be reconciled safely")
			return
		}
		_ = db.GetDb().Model(&model.WebDAVWritebackReconcileRun{}).Where("id = ?", runID).
			Updates(map[string]any{"scanned_directories": directories, "remote_files": remoteFiles}).Error
	}
	var counts struct {
		Matched int64 `gorm:"column:matched"`
		Missing int64 `gorm:"column:missing"`
		Extra   int64 `gorm:"column:extra"`
		Unknown int64 `gorm:"column:unknown"`
	}
	if err := db.GetDb().Model(&model.WebDAVWritebackReconcileEntry{}).
		Select("SUM(CASE WHEN local_present = 1 AND is_dir = 0 AND result = ? THEN 1 ELSE 0 END) AS matched, SUM(CASE WHEN local_present = 1 AND is_dir = 0 AND result IN ? THEN 1 ELSE 0 END) AS missing, SUM(CASE WHEN result IN ? THEN 1 ELSE 0 END) AS extra, SUM(CASE WHEN result = ? THEN 1 ELSE 0 END) AS unknown", reconcileResultMatch, []string{reconcileResultMissing, reconcileResultMismatch}, []string{reconcileResultExtra, reconcileResultMismatch}, reconcileResultUnknown).
		Where("run_id = ?", runID).Scan(&counts).Error; err != nil {
		failReconciliation(runID, ReconcileStateCompareError, "comparison summary failed")
		return
	}
	if counts.Unknown > 0 {
		_ = db.GetDb().Model(&model.WebDAVWritebackReconcileRun{}).Where("id = ?", runID).
			Update("unknown_files", counts.Unknown).Error
		failReconciliation(runID, ReconcileStateCompareError, "115 returned same-name files without a complete SHA-1; comparison paused and no files were deleted")
		return
	}
	now := time.Now()
	state := ReconcileStatePreview
	if counts.Extra == 0 {
		state = ReconcileStateReady
	}
	_ = db.GetDb().Model(&model.WebDAVWritebackReconcileRun{}).Where("id = ? AND state = ?", runID, ReconcileStateComparing).
		Updates(map[string]any{
			"state": state, "last_error": "", "compared_at": &now,
			"matched_files": counts.Matched, "missing_files": counts.Missing,
			"extra_files": counts.Extra, "unknown_files": counts.Unknown,
			"scanned_directories": directories, "remote_files": remoteFiles,
		}).Error
}

func resetReconciliationRemoteResults(runID uint) error {
	if err := db.GetDb().Where("run_id = ? AND local_present = ?", runID, false).
		Delete(&model.WebDAVWritebackReconcileEntry{}).Error; err != nil {
		return err
	}
	return db.GetDb().Model(&model.WebDAVWritebackReconcileEntry{}).
		Where("run_id = ? AND local_present = ?", runID, true).
		Updates(map[string]any{
			"result": reconcileResultMissing, "remote_size": 0, "remote_sha1": "", "remote_object_id": "",
		}).Error
}

func reconcileRemoteParent(runID uint, root, parent string, objects []model.Obj, queue *[]string, remoteFiles *int64) error {
	var locals []model.WebDAVWritebackReconcileEntry
	if err := db.GetDb().Where("run_id = ? AND parent_key = ? AND local_present = ?", runID, pathKey(parent), true).
		Find(&locals).Error; err != nil {
		return err
	}
	localByName := make(map[string]model.WebDAVWritebackReconcileEntry, len(locals))
	for _, local := range locals {
		localByName[local.Name] = local
	}
	remoteByName := make(map[string][]model.Obj)
	remoteNames := make(map[string]struct{}, len(objects))
	for _, object := range objects {
		if object == nil || !reconciliationValidName(object.GetName()) {
			return errors.New("115 returned an invalid path name")
		}
		if _, duplicate := remoteNames[object.GetName()]; duplicate {
			return errors.New("115 returned duplicate names that cannot be safely deleted by WebDAV path")
		}
		remoteNames[object.GetName()] = struct{}{}
		if object.IsDir() {
			child := utils.FixAndCleanPath(path.Join(parent, object.GetName()))
			if child == utils.FixAndCleanPath(root) || !isPathOrDescendant(child, root) {
				return errors.New("115 returned a path outside the selected reconciliation root")
			}
			if local, exists := localByName[object.GetName()]; exists && !local.IsDir {
				return fmt.Errorf("remote directory conflicts with source file")
			}
			if local, exists := localByName[object.GetName()]; exists {
				if err := db.GetDb().Model(&model.WebDAVWritebackReconcileEntry{}).
					Where("run_id = ? AND path_key = ? AND local_present = ?", runID, local.PathKey, true).
					Updates(map[string]any{"result": reconcileResultMatch, "remote_object_id": object.GetID()}).Error; err != nil {
					return err
				}
			} else {
				entry := model.WebDAVWritebackReconcileEntry{
					RunID: runID, PathKey: pathKey(child), ParentKey: pathKey(parent),
					Path: child, Parent: parent, Name: object.GetName(), IsDir: true,
					RemoteSize: object.GetSize(), RemoteObjectID: object.GetID(), Result: reconcileResultExtra,
				}
				if err := saveReconciliationEntry(db.GetDb(), &entry); err != nil {
					return err
				}
			}
			*queue = append(*queue, child)
			continue
		}
		remoteByName[object.GetName()] = append(remoteByName[object.GetName()], object)
	}

	for _, local := range locals {
		remotes := remoteByName[local.Name]
		if local.IsDir {
			if len(remotes) > 0 {
				return fmt.Errorf("remote file conflicts with source directory")
			}
			remoteDirID := ""
			foundRemoteDir := false
			for _, object := range objects {
				if object != nil && object.IsDir() && object.GetName() == local.Name {
					foundRemoteDir = true
					remoteDirID = object.GetID()
					break
				}
			}
			result := reconcileResultMissing
			if foundRemoteDir {
				result = reconcileResultMatch
			}
			if err := db.GetDb().Model(&model.WebDAVWritebackReconcileEntry{}).
				Where("run_id = ? AND path_key = ? AND local_present = ?", runID, local.PathKey, true).
				Updates(map[string]any{"result": result, "remote_object_id": remoteDirID}).Error; err != nil {
				return err
			}
			continue
		}
		if len(remotes) == 0 {
			continue
		}
		matched := model.Obj(nil)
		for _, remote := range remotes {
			remoteSHA1 := strings.ToLower(remote.GetHash().GetHash(utils.SHA1))
			if remote.GetSize() == local.Size && reconciliationHasSHA1(remoteSHA1) && remoteSHA1 == strings.ToLower(local.SHA1) {
				matched = remote
				break
			}
		}
		if matched != nil {
			if err := db.GetDb().Model(&model.WebDAVWritebackReconcileEntry{}).
				Where("run_id = ? AND path_key = ? AND local_present = ?", runID, local.PathKey, true).
				Updates(map[string]any{
					"result": reconcileResultMatch, "remote_size": matched.GetSize(),
					"remote_sha1": strings.ToLower(matched.GetHash().GetHash(utils.SHA1)),
					"remote_object_id": matched.GetID(),
				}).Error; err != nil {
				return err
			}
			continue
		}
		missingSHA1 := false
		for _, remote := range remotes {
			if !reconciliationHasSHA1(remote.GetHash().GetHash(utils.SHA1)) {
				missingSHA1 = true
				break
			}
		}
		if missingSHA1 {
			if err := db.GetDb().Model(&model.WebDAVWritebackReconcileEntry{}).
				Where("run_id = ? AND path_key = ? AND local_present = ?", runID, local.PathKey, true).
				Updates(map[string]any{"result": reconcileResultUnknown, "remote_object_id": remotes[0].GetID()}).Error; err != nil {
				return err
			}
			continue
		}
		remote := remotes[0]
		if err := db.GetDb().Model(&model.WebDAVWritebackReconcileEntry{}).
			Where("run_id = ? AND path_key = ? AND local_present = ?", runID, local.PathKey, true).
			Updates(map[string]any{
				"result": reconcileResultMismatch, "remote_size": remote.GetSize(),
				"remote_sha1": strings.ToLower(remote.GetHash().GetHash(utils.SHA1)),
				"remote_object_id": remote.GetID(),
			}).Error; err != nil {
			return err
		}
	}

	for name, remotes := range remoteByName {
		if _, exists := localByName[name]; exists {
			continue
		}
		for _, remote := range remotes {
			p := utils.FixAndCleanPath(path.Join(parent, name))
			entry := model.WebDAVWritebackReconcileEntry{
				RunID: runID, PathKey: pathKey(p), ParentKey: pathKey(parent), Path: p,
				Parent: parent, Name: name, LocalPresent: false, Size: 0,
				RemoteSize: remote.GetSize(), RemoteSHA1: strings.ToLower(remote.GetHash().GetHash(utils.SHA1)),
				RemoteObjectID: remote.GetID(), Result: reconcileResultExtra,
			}
			if err := saveReconciliationEntry(db.GetDb(), &entry); err != nil {
				return err
			}
		}
	}
	for _, object := range objects {
		if object != nil && !object.IsDir() {
			*remoteFiles++
		}
	}
	return nil
}

func runReconciliationDeletion(runID uint) {
	var run model.WebDAVWritebackReconcileRun
	if err := db.GetDb().Take(&run, runID).Error; err != nil || run.State != ReconcileStateDeleting {
		return
	}
	var candidates []model.WebDAVWritebackReconcileEntry
	if err := db.GetDb().Where("run_id = ? AND result IN ?", runID, []string{reconcileResultExtra, reconcileResultMismatch}).
		Order("parent ASC, path ASC").Find(&candidates).Error; err != nil {
		failReconciliation(runID, ReconcileStateDeleteError, "could not load deletion candidates")
		return
	}
	byParent := make(map[string][]model.WebDAVWritebackReconcileEntry)
	var parents []string
	for _, candidate := range candidates {
		if _, exists := byParent[candidate.Parent]; !exists {
			parents = append(parents, candidate.Parent)
		}
		byParent[candidate.Parent] = append(byParent[candidate.Parent], candidate)
	}
	sort.Slice(parents, func(i, j int) bool {
		depthI, depthJ := strings.Count(parents[i], "/"), strings.Count(parents[j], "/")
		if depthI != depthJ {
			return depthI > depthJ
		}
		return parents[i] > parents[j]
	})
	ctx := context.Background()
	slots := make(chan struct{}, 1)
	var deleted int64
	for _, parent := range parents {
		current, err := reconciliationFreshList(ctx, ctx.Done(), slots, &run, parent)
		if err != nil {
			failReconciliation(runID, ReconcileStateDeleteError, "115 directory refresh failed; deletion can be retried")
			return
		}
		currentByName := make(map[string][]model.Obj)
		for _, object := range current {
			if object != nil {
				currentByName[object.GetName()] = append(currentByName[object.GetName()], object)
			}
		}
		for _, candidate := range byParent[parent] {
			expectedPath := utils.FixAndCleanPath(path.Join(parent, candidate.Name))
			if !reconciliationValidName(candidate.Name) || expectedPath != utils.FixAndCleanPath(candidate.Path) ||
				expectedPath == utils.FixAndCleanPath(run.Root) || !isPathOrDescendant(expectedPath, run.Root) {
				failReconciliation(runID, ReconcileStateDeleteError, "a deletion candidate is outside the selected root; compare again before deleting")
				return
			}
			remotes := currentByName[candidate.Name]
			if len(remotes) == 0 {
				continue
			}
			sourceMatchStillExists := false
			if candidate.LocalPresent {
				for _, remote := range remotes {
					if remote.IsDir() {
						continue
					}
					remoteSHA1 := strings.ToLower(remote.GetHash().GetHash(utils.SHA1))
					if remote.GetSize() == candidate.Size && remoteSHA1 != "" && remoteSHA1 == strings.ToLower(candidate.SHA1) {
						_ = db.GetDb().Model(&model.WebDAVWritebackReconcileEntry{}).
							Where("run_id = ? AND path_key = ?", runID, candidate.PathKey).
							Updates(map[string]any{"result": reconcileResultMatch, "remote_size": remote.GetSize(), "remote_sha1": remoteSHA1, "remote_object_id": remote.GetID()}).Error
						sourceMatchStillExists = true
						break
					}
				}
			}
			if sourceMatchStillExists {
				continue
			}
			reviewedObjectStillExists := false
			for _, remote := range remotes {
				if remoteMatchesReconciliationCandidate(candidate, remote) {
					reviewedObjectStillExists = true
					break
				}
			}
			if !reviewedObjectStillExists {
				failReconciliation(runID, ReconcileStateDeleteError, "a deletion candidate changed after preview; compare again before deleting")
				return
			}
			if err := reconciliationStorageMatches(&run, candidate.Path); err != nil {
				failReconciliation(runID, ReconcileStateDeleteError, "the selected storage mapping changed after preview; no further remote changes were made")
				return
			}
			if candidate.IsDir {
				children, listErr := reconciliationFreshList(ctx, ctx.Done(), slots, &run, candidate.Path)
				if listErr != nil {
					failReconciliation(runID, ReconcileStateDeleteError, "115 directory refresh failed; deletion can be retried")
					return
				}
				if len(children) != 0 {
					failReconciliation(runID, ReconcileStateDeleteError, "a reviewed 115 directory is no longer empty; compare again before deleting")
					return
				}
			}
			if err := fs.Remove(ctx, candidate.Path); err != nil && !errs.IsObjectNotFound(err) {
				failReconciliation(runID, ReconcileStateDeleteError, "115 delete failed; deletion can be retried")
				return
			}
			deleted++
		}
		verified, err := reconciliationFreshList(ctx, ctx.Done(), slots, &run, parent)
		if err != nil {
			failReconciliation(runID, ReconcileStateDeleteError, "115 delete verification failed; deletion can be retried")
			return
		}
		remaining := make(map[string]struct{})
		for _, object := range verified {
			if object != nil {
				remaining[object.GetName()] = struct{}{}
			}
		}
		for _, candidate := range byParent[parent] {
			if candidate.LocalPresent {
				var currentEntry model.WebDAVWritebackReconcileEntry
				if db.GetDb().Where("run_id = ? AND path_key = ?", runID, candidate.PathKey).Take(&currentEntry).Error == nil && currentEntry.Result == reconcileResultMatch {
					continue
				}
			}
			if _, exists := remaining[candidate.Name]; exists {
				failReconciliation(runID, ReconcileStateDeleteError, "a 115 object remains after deletion; retry after checking the provider")
				return
			}
			if candidate.LocalPresent {
				_ = db.GetDb().Model(&model.WebDAVWritebackReconcileEntry{}).
					Where("run_id = ? AND path_key = ?", runID, candidate.PathKey).
					Update("result", reconcileResultMissing).Error
			}
		}
	}
	_ = db.GetDb().Model(&model.WebDAVWritebackReconcileRun{}).Where("id = ? AND state = ?", runID, ReconcileStateDeleting).
		Updates(map[string]any{"state": ReconcileStateReady, "last_error": "", "deleted_files": deleted}).Error
}

func remoteMatchesReconciliationCandidate(candidate model.WebDAVWritebackReconcileEntry, remote model.Obj) bool {
	if remote == nil || remote.IsDir() != candidate.IsDir || remote.GetSize() != candidate.RemoteSize {
		return false
	}
	// Without a stable remote ID or valid hash, size and name cannot prove that
	// the object reviewed for deletion is still the same object.
	if candidate.RemoteObjectID == "" && !reconciliationHasSHA1(candidate.RemoteSHA1) {
		return false
	}
	if candidate.RemoteObjectID != "" && remote.GetID() != candidate.RemoteObjectID {
		return false
	}
	remoteSHA1 := strings.ToLower(remote.GetHash().GetHash(utils.SHA1))
	return remoteSHA1 == strings.ToLower(candidate.RemoteSHA1)
}

func failReconciliation(runID uint, state, message string) {
	_ = db.GetDb().Model(&model.WebDAVWritebackReconcileRun{}).Where("id = ?", runID).
		Updates(map[string]any{"state": state, "last_error": message}).Error
}
