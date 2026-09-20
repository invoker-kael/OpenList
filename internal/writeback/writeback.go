package writeback

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/fs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/stream"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/google/uuid"
	"github.com/shirou/gopsutil/v4/disk"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	StateQueued    = "queued"
	StateUploading = "uploading"
	StateVerifying = "verifying"
	StateCompleted = "completed"
	StateFailed    = "failed"
	StateDeleted   = "deleted"
)

type CanonicalObject struct {
	model.Object
	etag string
}

func (o *CanonicalObject) ETag(context.Context) (string, error) {
	return o.etag, nil
}

func Enabled() bool {
	return conf.Conf != nil && conf.Conf.WebDAVWriteback.Enabled
}

func pathKey(p string) string {
	sum := sha256.Sum256([]byte(utils.FixAndCleanPath(p)))
	return hex.EncodeToString(sum[:])
}

func canonicalETag(key string, generation uint64, size int64) string {
	return fmt.Sprintf("\"olwb-%s-%d-%x\"", key[:16], generation, uint64(size))
}

var ErrDestinationExists = errors.New("write-back destination already exists")

func removeSpoolIfUnreferenced(spoolPath string) {
	if spoolPath == "" {
		return
	}
	var count int64
	if err := db.GetDb().Model(&model.WebDAVWritebackObject{}).Where("spool_path = ?", spoolPath).Count(&count).Error; err != nil {
		return
	}
	if count == 0 {
		_ = os.Remove(spoolPath)
	}
}

func toObject(row *model.WebDAVWritebackObject) model.Obj {
	return &CanonicalObject{
		Object: model.Object{
			ID:       fmt.Sprintf("writeback:%d", row.ID),
			Path:     row.Path,
			Name:     row.Name,
			Size:     row.Size,
			Modified: row.ModTime,
			Ctime:    row.CreateTime,
		},
		etag: row.ETag,
	}
}

func getByPath(p string) (*model.WebDAVWritebackObject, error) {
	if !Enabled() {
		return nil, nil
	}
	var row model.WebDAVWritebackObject
	err := db.GetDb().Where("path_key = ?", pathKey(p)).First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	return &row, err
}

// Canonical returns the stable WebDAV object for a path. found remains true for
// tombstones so callers can hide a remotely-stale object while delete is pending.
func Canonical(p string) (obj model.Obj, found bool, deleted bool, err error) {
	row, err := getByPath(p)
	if err != nil || row == nil {
		return nil, false, false, err
	}
	if row.State == StateDeleted {
		return nil, true, true, nil
	}
	return toObject(row), true, false, nil
}

func shouldDropCanonicalAfterRemoteList(row *model.WebDAVWritebackObject, remoteReliable, remotePresent bool) bool {
	return remoteReliable &&
		!remotePresent &&
		row.State == StateCompleted &&
		row.SpoolPath == ""
}

// OverlayList replaces remote objects with their canonical WebDAV metadata and
// injects locally committed objects that are not visible on the remote yet.
// When the provider list itself succeeded, a completed row whose local spool
// cache has already been released is dropped if the remote object disappeared.
// That lets one-way Cloud Sync observe the loss and upload the source again.
func OverlayList(parent string, remote []model.Obj, remoteReliable bool) ([]model.Obj, bool, error) {
	if !Enabled() {
		return remote, false, nil
	}
	parent = utils.FixAndCleanPath(parent)
	var rows []model.WebDAVWritebackObject
	if err := db.GetDb().Where("parent_key = ?", pathKey(parent)).Find(&rows).Error; err != nil {
		return nil, false, err
	}
	if len(rows) == 0 {
		return remote, false, nil
	}

	byName := make(map[string]model.Obj, len(remote)+len(rows))
	order := make([]string, 0, len(remote)+len(rows))
	for _, obj := range remote {
		name := obj.GetName()
		if _, ok := byName[name]; !ok {
			order = append(order, name)
		}
		byName[name] = obj
	}
	for i := range rows {
		row := &rows[i]
		if row.State == StateDeleted {
			delete(byName, row.Name)
			continue
		}
		_, remotePresent := byName[row.Name]
		if shouldDropCanonicalAfterRemoteList(row, remoteReliable, remotePresent) {
			res := db.GetDb().
				Where("id = ? AND generation = ? AND state = ? AND spool_path = ''", row.ID, row.Generation, StateCompleted).
				Delete(&model.WebDAVWritebackObject{})
			if res.Error != nil {
				return nil, false, res.Error
			}
			continue
		}
		if !remotePresent {
			order = append(order, row.Name)
		}
		byName[row.Name] = toObject(row)
	}

	out := make([]model.Obj, 0, len(byName))
	for _, name := range order {
		if obj, ok := byName[name]; ok {
			out = append(out, obj)
		}
	}
	return out, true, nil
}

func reserveBytes() uint64 {
	mb := conf.Conf.WebDAVWriteback.ReserveFreeSpaceMB
	return mb * uint64(utils.MB)
}

var (
	spaceMu          sync.Mutex
	reservedIncoming uint64
	receivingMu      sync.Mutex
	receivingPaths   = make(map[string]int)
)

func beginReceiving(p string) func() {
	key := pathKey(p)
	receivingMu.Lock()
	receivingPaths[key]++
	receivingMu.Unlock()
	return func() {
		receivingMu.Lock()
		if receivingPaths[key] <= 1 {
			delete(receivingPaths, key)
		} else {
			receivingPaths[key]--
		}
		receivingMu.Unlock()
	}
}

func isReceiving(p string) bool {
	key := pathKey(p)
	receivingMu.Lock()
	defer receivingMu.Unlock()
	return receivingPaths[key] > 0
}

func cloudSyncSettleDelay() time.Duration {
	ms := conf.Conf.WebDAVWriteback.CloudSyncSettleMillis
	if ms < 0 {
		ms = 0
	}
	return time.Duration(ms) * time.Millisecond
}

func checkFreeSpace(extra uint64) error {
	usage, err := disk.Usage(conf.Conf.WebDAVWriteback.SpoolDir)
	if err != nil {
		return err
	}
	required := reserveBytes() + extra
	if usage.Free < required {
		return fmt.Errorf("write-back spool free space %d is below required %d", usage.Free, required)
	}
	return nil
}

func reserveIncomingBytes(expected int64) (func(), error) {
	if expected <= 0 {
		if err := checkFreeSpace(0); err != nil {
			return nil, err
		}
		return func() {}, nil
	}
	spaceMu.Lock()
	defer spaceMu.Unlock()
	usage, err := disk.Usage(conf.Conf.WebDAVWriteback.SpoolDir)
	if err != nil {
		return nil, err
	}
	need := uint64(expected)
	required := reserveBytes() + reservedIncoming + need
	if usage.Free < required {
		return nil, fmt.Errorf("write-back spool free space %d is below reserved requirement %d", usage.Free, required)
	}
	reservedIncoming += need
	return func() {
		spaceMu.Lock()
		reservedIncoming -= need
		spaceMu.Unlock()
	}, nil
}

func copyToSpool(dst *os.File, src io.Reader, expected int64) (int64, error) {

	buf := make([]byte, 4*utils.MB)
	var total int64
	var sinceCheck int64
	for {
		n, readErr := src.Read(buf)
		if n > 0 {
			wn, writeErr := dst.Write(buf[:n])
			total += int64(wn)
			sinceCheck += int64(wn)
			if writeErr != nil {
				return total, writeErr
			}
			if wn != n {
				return total, io.ErrShortWrite
			}
			if sinceCheck >= 64*utils.MB {
				if err := checkFreeSpace(0); err != nil {
					return total, err
				}
				sinceCheck = 0
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return total, readErr
		}
	}
	if expected >= 0 && total != expected {
		return total, fmt.Errorf("incomplete WebDAV PUT: expected %d bytes, received %d", expected, total)
	}
	return total, nil
}

func syncDir(dir string) {
	f, err := os.Open(dir)
	if err == nil {
		_ = f.Sync()
		_ = f.Close()
	}
}

// Commit receives the complete opaque WebDAV object into the local spool and
// only then commits a new canonical generation into MySQL.
func Commit(ctx context.Context, p string, body io.Reader, expected int64, modTime, createTime time.Time, mime string) (*model.WebDAVWritebackObject, bool, error) {
	if !Enabled() {
		return nil, false, errors.New("WebDAV write-back is disabled")
	}
	p = utils.FixAndCleanPath(p)
	releaseReceiving := beginReceiving(p)
	defer releaseReceiving()
	spoolDir := conf.Conf.WebDAVWriteback.SpoolDir
	if err := os.MkdirAll(spoolDir, 0o700); err != nil {
		return nil, false, err
	}

	releaseReservation, err := reserveIncomingBytes(expected)
	if err != nil {
		return nil, false, err
	}
	defer releaseReservation()

	tmp, err := os.CreateTemp(spoolDir, "recv-*.part")
	if err != nil {
		return nil, false, err
	}
	tmpName := tmp.Name()
	committed := false
	defer func() {
		_ = tmp.Close()
		if !committed {
			_ = os.Remove(tmpName)
		}
	}()

	actualSize, err := copyToSpool(tmp, body, expected)
	if err != nil {
		return nil, false, err
	}
	if err := tmp.Sync(); err != nil {
		return nil, false, err
	}
	if err := tmp.Close(); err != nil {
		return nil, false, err
	}
	finalName := filepath.Join(spoolDir, uuid.NewString()+".data")
	if err := os.Rename(tmpName, finalName); err != nil {
		return nil, false, err
	}
	syncDir(spoolDir)
	committed = true

	if modTime.IsZero() {
		modTime = time.Now()
	}
	if createTime.IsZero() {
		createTime = modTime
	}
	parent := path.Dir(p)
	key := pathKey(p)
	var oldSpool string
	var saved model.WebDAVWritebackObject
	created := false
	settleAt := time.Now().Add(cloudSyncSettleDelay())

	err = db.GetDb().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var row model.WebDAVWritebackObject
		findErr := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("path_key = ?", key).First(&row).Error
		if findErr != nil && !errors.Is(findErr, gorm.ErrRecordNotFound) {
			return findErr
		}
		if findErr == nil {
			oldSpool = row.SpoolPath
			row.Generation++
		} else {
			created = true
			row.Generation = 1
		}

		row.PathKey = key
		row.ParentKey = pathKey(parent)
		row.Path = p
		row.Parent = parent
		row.Name = path.Base(p)
		row.Size = actualSize
		row.ModTime = modTime
		row.CreateTime = createTime
		row.ETag = canonicalETag(key, row.Generation, actualSize)
		row.State = StateQueued
		row.SpoolPath = finalName
		row.MimeType = mime
		row.CleanupPath = ""
		row.LastError = ""
		row.RetryCount = 0
		row.VerifyCount = 0
		row.RetryAt = &settleAt
		row.CompletedAt = nil

		if row.ID == 0 {
			if err := tx.Create(&row).Error; err != nil {
				return err
			}
		} else if err := tx.Save(&row).Error; err != nil {
			return err
		}
		saved = row
		return nil
	})
	if err != nil {
		_ = os.Remove(finalName)
		return nil, false, err
	}

	if oldSpool != "" && oldSpool != finalName {
		if _, active := activeSpools.Load(oldSpool); !active {
			removeSpoolIfUnreferenced(oldSpool)
		}
	}
	wake()
	return &saved, created, nil
}

func OpenLocal(p string) (*os.File, *model.WebDAVWritebackObject, error) {
	row, err := getByPath(p)
	if err != nil || row == nil || row.State == StateDeleted || row.SpoolPath == "" {
		return nil, row, err
	}
	f, err := os.Open(row.SpoolPath)
	if os.IsNotExist(err) {
		return nil, row, nil
	}
	return f, row, err
}

// Delete creates a durable tombstone and lets the background worker remove the
// remote object. This makes the deletion immediately visible to Cloud Sync.
func Delete(p string) (bool, error) {
	row, err := getByPath(p)
	if err != nil || row == nil {
		return false, err
	}
	now := time.Now()
	err = db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("id = ?", row.ID).
		Updates(map[string]any{
			"generation":   gorm.Expr("generation + 1"),
			"state":        StateDeleted,
			"retry_at":     &now,
			"last_error":   "",
			"verify_count": 0,
		}).Error
	if err == nil {
		wake()
	}
	return true, err
}

// MovePending handles an exact file move when the source has not reached the
// completed remote state yet. It also handles Cloud Sync's common
// temp-file -> final-file overwrite pattern without violating the MySQL
// path_key unique index.
func MovePending(src, dst string, overwrite bool) (bool, error) {
	src = utils.FixAndCleanPath(src)
	dst = utils.FixAndCleanPath(dst)
	srcKey := pathKey(src)
	dstKey := pathKey(dst)

	var srcRow model.WebDAVWritebackObject
	if err := db.GetDb().Where("path_key = ?", srcKey).First(&srcRow).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, nil
		}
		return false, err
	}
	if srcRow.State == StateDeleted || srcRow.State == StateCompleted {
		return false, nil
	}

	var oldDestinationSpool string
	now := time.Now()
	err := db.GetDb().Transaction(func(tx *gorm.DB) error {
		var lockedSrc model.WebDAVWritebackObject
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", srcRow.ID).First(&lockedSrc).Error; err != nil {
			return err
		}
		if lockedSrc.State == StateDeleted || lockedSrc.State == StateCompleted {
			return gorm.ErrRecordNotFound
		}

		var dstRow model.WebDAVWritebackObject
		dstErr := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("path_key = ?", dstKey).First(&dstRow).Error
		if dstErr != nil && !errors.Is(dstErr, gorm.ErrRecordNotFound) {
			return dstErr
		}
		if dstErr == nil && dstRow.ID != lockedSrc.ID {
			if !overwrite {
				return ErrDestinationExists
			}
			oldDestinationSpool = dstRow.SpoolPath
			dstRow.Generation++
			dstRow.ParentKey = pathKey(path.Dir(dst))
			dstRow.Path = dst
			dstRow.Parent = path.Dir(dst)
			dstRow.Name = path.Base(dst)
			dstRow.Size = lockedSrc.Size
			dstRow.ModTime = lockedSrc.ModTime
			dstRow.CreateTime = lockedSrc.CreateTime
			dstRow.ETag = canonicalETag(dstKey, dstRow.Generation, lockedSrc.Size)
			dstRow.State = StateQueued
			dstRow.SpoolPath = lockedSrc.SpoolPath
			dstRow.MimeType = lockedSrc.MimeType
			dstRow.CleanupPath = ""
			dstRow.LastError = ""
			dstRow.RetryCount = 0
			dstRow.VerifyCount = 0
			dstRow.RetryAt = &now
			dstRow.CompletedAt = nil
		if err := tx.Save(&dstRow).Error; err != nil {
			return err
		}

		lockedSrc.Generation++
		lockedSrc.State = StateDeleted
		lockedSrc.SpoolPath = ""
		lockedSrc.LastError = ""
		lockedSrc.RetryCount = 0
		lockedSrc.VerifyCount = 0
		lockedSrc.RetryAt = &now
		lockedSrc.CompletedAt = nil
		return tx.Save(&lockedSrc).Error
		}

		lockedSrc.PathKey = dstKey
		lockedSrc.ParentKey = pathKey(path.Dir(dst))
		lockedSrc.Path = dst
		lockedSrc.Parent = path.Dir(dst)
		lockedSrc.Name = path.Base(dst)
		lockedSrc.Generation++
		lockedSrc.ETag = canonicalETag(dstKey, lockedSrc.Generation, lockedSrc.Size)
		lockedSrc.State = StateQueued
		lockedSrc.CleanupPath = src
		lockedSrc.RetryAt = &now
		lockedSrc.LastError = ""
		lockedSrc.RetryCount = 0
		lockedSrc.VerifyCount = 0
		lockedSrc.CompletedAt = nil
		return tx.Save(&lockedSrc).Error
	})
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	if err != nil {
		return true, err
	}
	if oldDestinationSpool != "" {
		if _, active := activeSpools.Load(oldDestinationSpool); !active {
			removeSpoolIfUnreferenced(oldDestinationSpool)
		}
	}
	wake()
	return true, nil
}

// MoveTreeMetadata follows a successful remote MOVE and keeps canonical
// metadata aligned with the new path. Pending descendants are re-queued.
func MoveTreeMetadata(src, dst string) error {
	if !Enabled() {
		return nil
	}
	src = utils.FixAndCleanPath(src)
	dst = utils.FixAndCleanPath(dst)
	var rows []model.WebDAVWritebackObject
	// LIKE treats '%' and '_' inside src as wildcards. The database query is
	// only a candidate scan; enforce the real path boundary again in Go before
	// mutating any row so unusual Cloud Sync names cannot move unrelated state.
	if err := db.GetDb().Where("path = ? OR path LIKE ?", src, src+"%").Find(&rows).Error; err != nil {
		return err
	}
	for i := range rows {
		row := &rows[i]
		if row.Path != src && !strings.HasPrefix(row.Path, src+"/") {
			continue
		}
		suffix := strings.TrimPrefix(row.Path, src)
		newPath := utils.FixAndCleanPath(dst + suffix)
		parent := path.Dir(newPath)
		updates := map[string]any{
			"path_key":   pathKey(newPath),
			"parent_key": pathKey(parent),
			"path":       newPath,
			"parent":     parent,
			"name":       path.Base(newPath),
			"generation": gorm.Expr("generation + 1"),
			"etag":       canonicalETag(pathKey(newPath), row.Generation+1, row.Size),
		}
		if row.State != StateCompleted && row.State != StateDeleted {
			updates["state"] = StateQueued
			updates["retry_at"] = time.Now()
		}
		if err := db.GetDb().Model(&model.WebDAVWritebackObject{}).Where("id = ?", row.ID).Updates(updates).Error; err != nil {
			return err
		}
	}
	wake()
	return nil
}

var (
	managerMu    sync.Mutex
	manager      *workerManager
	activeSpools sync.Map
)

type workerManager struct {
	ctx      context.Context
	cancel   context.CancelFunc
	stop     chan struct{}
	wake     chan struct{}
	jobs     chan uint
	inflight sync.Map
	wg       sync.WaitGroup
}

func wake() {
	managerMu.Lock()
	m := manager
	managerMu.Unlock()
	if m == nil {
		return
	}
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

func Start() {
	if !Enabled() {
		return
	}
	managerMu.Lock()
	if manager != nil {
		managerMu.Unlock()
		return
	}
	workerCtx, cancel := context.WithCancel(context.Background())
	m := &workerManager{
		ctx:    workerCtx,
		cancel: cancel,
		stop:   make(chan struct{}),
		wake:   make(chan struct{}, 1),
		jobs:   make(chan uint, max(4, conf.Conf.WebDAVWriteback.Workers*4)),
	}
	manager = m
	managerMu.Unlock()

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		select {
		case <-conf.StoragesLoadSignal():
		case <-m.stop:
			return
		}
		if err := m.recoverInterrupted(); err != nil {
			log.Errorf("write-back recovery failed: %v", err)
		}
		m.cleanupOrphans()
		workers := max(1, conf.Conf.WebDAVWriteback.Workers)
		for i := 0; i < workers; i++ {
			m.wg.Add(1)
			go m.worker()
		}
		m.scheduler()
	}()
}

func Stop() {
	managerMu.Lock()
	m := manager
	manager = nil
	managerMu.Unlock()
	if m == nil {
		return
	}
	m.cancel()
	close(m.stop)
	m.wg.Wait()
}

func (m *workerManager) recoverInterrupted() error {
	// An UPLOADING row is ambiguous after a process crash: the provider may
	// have accepted the payload even though we never durably recorded the
	// transition to VERIFYING. Do not accept a same-sized pre-existing remote
	// object as proof that this generation arrived. Re-queue the durable spool
	// payload instead. Remote writes are therefore at-least-once across crashes,
	// which is safer for one-way Cloud Sync than a false-positive completion.
	now := time.Now()
	return db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("state = ?", StateUploading).
		Updates(map[string]any{
			"state":        StateQueued,
			"retry_at":     &now,
			"verify_count": 0,
			"last_error":   "re-queued after restart because upload completion was not durably confirmed",
		}).Error
}

func (m *workerManager) scheduler() {
	ticker := time.NewTicker(2 * time.Second)
	cleanupTicker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	defer cleanupTicker.Stop()
	for {
		m.dispatch()
		select {
		case <-m.stop:
			return
		case <-m.wake:
		case <-ticker.C:
		case <-cleanupTicker.C:
			m.cleanupCompleted()
		}
	}
}

func (m *workerManager) dispatch() {
	now := time.Now()
	var rows []model.WebDAVWritebackObject
	err := db.GetDb().
		Where("state IN ? AND (retry_at IS NULL OR retry_at <= ?)", []string{StateQueued, StateFailed, StateVerifying, StateDeleted}, now).
		Order("updated_at asc").
		Limit(max(8, conf.Conf.WebDAVWriteback.Workers*4)).
		Find(&rows).Error
	if err != nil {
		log.Errorf("write-back queue scan failed: %v", err)
		return
	}
	for i := range rows {
		id := rows[i].ID
		if _, loaded := m.inflight.LoadOrStore(id, struct{}{}); loaded {
			continue
		}
		select {
		case m.jobs <- id:
		default:
			m.inflight.Delete(id)
			return
		}
	}
}

func (m *workerManager) worker() {
	defer m.wg.Done()
	for {
		select {
		case <-m.stop:
			return
		case id := <-m.jobs:
			m.process(id)
			m.inflight.Delete(id)
		}
	}
}

func (m *workerManager) process(id uint) {
	var row model.WebDAVWritebackObject
	if err := db.GetDb().First(&row, id).Error; err != nil {
		return
	}
	switch row.State {
	case StateDeleted:
		m.processDelete(&row)
	case StateVerifying:
		m.processVerify(&row)
	case StateQueued, StateFailed:
		m.processUpload(&row)
	}
}

func shouldRemoveStaleRemote(uploadedPath string, current *model.WebDAVWritebackObject) bool {
	if current == nil {
		return false
	}
	// A newer generation at the same path must win by being uploaded next.
	// Removing the remote object here would create an avoidable visibility gap
	// and can race with another worker/process writing the new generation.
	return current.State == StateDeleted || current.Path != uploadedPath
}

func (m *workerManager) processUpload(row *model.WebDAVWritebackObject) {
	if isReceiving(row.Path) {
		next := time.Now().Add(time.Second)
		_ = db.GetDb().Model(&model.WebDAVWritebackObject{}).
			Where("id = ? AND generation = ? AND state IN ?", row.ID, row.Generation, []string{StateQueued, StateFailed}).
			Update("retry_at", &next).Error
		return
	}
	if row.SpoolPath == "" {
		m.fail(row, errors.New("spool payload is missing"))
		return
	}
	f, err := os.Open(row.SpoolPath)
	if err != nil {
		m.fail(row, err)
		return
	}
	activeSpools.Store(row.SpoolPath, struct{}{})
	defer func() {
		activeSpools.Delete(row.SpoolPath)
		_ = f.Close()
	}()

	res := db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("id = ? AND generation = ? AND state IN ?", row.ID, row.Generation, []string{StateQueued, StateFailed}).
		Updates(map[string]any{"state": StateUploading, "retry_at": nil, "last_error": ""})
	if res.Error != nil || res.RowsAffected == 0 {
		return
	}

	obj := &model.Object{
		Name:     row.Name,
		Size:     row.Size,
		Modified: row.ModTime,
		Ctime:    row.CreateTime,
	}
	fsStream := &stream.FileStream{
		Obj:      obj,
		Reader:   f,
		Mimetype: row.MimeType,
	}
	err = fs.PutDirectly(m.ctx, row.Parent, fsStream, true)
	if err != nil {
		m.fail(row, err)
		return
	}

	var current model.WebDAVWritebackObject
	if err := db.GetDb().First(&current, row.ID).Error; err != nil {
		return
	}
	if current.Generation != row.Generation || current.State == StateDeleted || current.Path != row.Path {
		if shouldRemoveStaleRemote(row.Path, &current) {
			_ = fs.Remove(m.ctx, row.Path)
		}
		if current.SpoolPath != row.SpoolPath {
			removeSpoolIfUnreferenced(row.SpoolPath)
		}
		return
	}

	now := time.Now()
	if err := db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("id = ? AND generation = ?", row.ID, row.Generation).
		Updates(map[string]any{"state": StateVerifying, "retry_at": &now, "verify_count": 0}).Error; err != nil {
		log.Errorf("write-back failed to enter verifying state for %s: %v", row.Path, err)
		return
	}
	m.processVerify(row)
}

func (m *workerManager) processVerify(row *model.WebDAVWritebackObject) {
	remote, err := fs.Get(m.ctx, row.Path, &fs.GetArgs{NoLog: true})
	if err == nil && !remote.IsDir() && remote.GetSize() == row.Size {
		now := time.Now()
		res := db.GetDb().Model(&model.WebDAVWritebackObject{}).
			Where("id = ? AND generation = ? AND state = ?", row.ID, row.Generation, StateVerifying).
			Updates(map[string]any{
				"state":        StateCompleted,
				"completed_at": &now,
				"retry_at":     nil,
				"last_error":   "",
				"verify_count": 0,
			})
		if res.Error != nil || res.RowsAffected == 0 {
			return
		}
		if row.CleanupPath != "" && row.CleanupPath != row.Path {
			_ = fs.Remove(m.ctx, row.CleanupPath)
			_ = db.GetDb().Model(&model.WebDAVWritebackObject{}).
				Where("id = ? AND generation = ?", row.ID, row.Generation).
				Update("cleanup_path", "").Error
		}
		return
	}

	attempts := max(1, conf.Conf.WebDAVWriteback.VerifyAttempts)
	nextCount := row.VerifyCount + 1
	if nextCount >= attempts {
		next := time.Now().Add(retryDelay(row.RetryCount + 1))
		_ = db.GetDb().Model(&model.WebDAVWritebackObject{}).
			Where("id = ? AND generation = ?", row.ID, row.Generation).
			Updates(map[string]any{
				"state":        StateQueued,
				"retry_at":     &next,
				"retry_count":  row.RetryCount + 1,
				"verify_count": 0,
				"last_error":   "remote object did not become stable before verify timeout",
			}).Error
		return
	}
	interval := time.Duration(max(1, conf.Conf.WebDAVWriteback.VerifyIntervalSeconds)) * time.Second
	next := time.Now().Add(interval)
	msg := "remote object is not stable yet"
	if err != nil {
		msg = err.Error()
	} else if remote != nil {
		msg = fmt.Sprintf("remote size %d does not match canonical size %d", remote.GetSize(), row.Size)
	}
	_ = db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("id = ? AND generation = ?", row.ID, row.Generation).
		Updates(map[string]any{
			"state":        StateVerifying,
			"retry_at":     &next,
			"verify_count": nextCount,
			"last_error":   msg,
		}).Error
}

func (m *workerManager) processDelete(row *model.WebDAVWritebackObject) {
	err := fs.Remove(m.ctx, row.Path)
	if err != nil && !errs.IsObjectNotFound(err) {
		m.failDeleted(row, err)
		return
	}
	if row.CleanupPath != "" && row.CleanupPath != row.Path {
		_ = fs.Remove(m.ctx, row.CleanupPath)
	}
	res := db.GetDb().
		Where("id = ? AND generation = ? AND state = ?", row.ID, row.Generation, StateDeleted).
		Delete(&model.WebDAVWritebackObject{})
	if res.Error != nil || res.RowsAffected == 0 {
		return
	}
	if row.SpoolPath != "" {
		removeSpoolIfUnreferenced(row.SpoolPath)
	}
}

func retryDelay(retry int) time.Duration {
	initial := max(1, conf.Conf.WebDAVWriteback.RetryInitialSeconds)
	maxDelay := max(initial, conf.Conf.WebDAVWriteback.RetryMaxSeconds)
	shift := min(retry, 10)
	seconds := initial * (1 << shift)
	if seconds > maxDelay {
		seconds = maxDelay
	}
	return time.Duration(seconds) * time.Second
}

func (m *workerManager) fail(row *model.WebDAVWritebackObject, err error) {
	next := time.Now().Add(retryDelay(row.RetryCount))
	_ = db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("id = ? AND generation = ?", row.ID, row.Generation).
		Updates(map[string]any{
			"state":       StateFailed,
			"retry_at":    &next,
			"retry_count": row.RetryCount + 1,
			"last_error":  err.Error(),
		}).Error
}

func (m *workerManager) failDeleted(row *model.WebDAVWritebackObject, err error) {
	next := time.Now().Add(retryDelay(row.RetryCount))
	_ = db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("id = ? AND generation = ? AND state = ?", row.ID, row.Generation, StateDeleted).
		Updates(map[string]any{
			"retry_at":    &next,
			"retry_count": row.RetryCount + 1,
			"last_error":  err.Error(),
		}).Error
}

func (m *workerManager) cleanupCompleted() {
	ttl := conf.Conf.WebDAVWriteback.CompletedCacheTTLMinutes
	if ttl < 0 {
		return
	}
	cutoff := time.Now().Add(-time.Duration(ttl) * time.Minute)
	var rows []model.WebDAVWritebackObject
	if err := db.GetDb().
		Where("state = ? AND spool_path <> '' AND completed_at IS NOT NULL AND completed_at <= ?", StateCompleted, cutoff).
		Limit(100).
		Find(&rows).Error; err != nil {
		return
	}
	for i := range rows {
		row := &rows[i]
		if _, active := activeSpools.Load(row.SpoolPath); active {
			continue
		}
		if err := os.Remove(row.SpoolPath); err != nil && !os.IsNotExist(err) {
			continue
		}
		_ = db.GetDb().Model(&model.WebDAVWritebackObject{}).
			Where("id = ? AND generation = ?", row.ID, row.Generation).
			Update("spool_path", "").Error
	}
}

func (m *workerManager) cleanupOrphans() {
	spoolDir := conf.Conf.WebDAVWriteback.SpoolDir
	entries, err := os.ReadDir(spoolDir)
	if err != nil {
		return
	}
	var rows []model.WebDAVWritebackObject
	if err := db.GetDb().Select("spool_path").Where("spool_path <> ''").Find(&rows).Error; err != nil {
		return
	}
	keep := make(map[string]struct{}, len(rows))
	for i := range rows {
		keep[filepath.Clean(rows[i].SpoolPath)] = struct{}{}
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		p := filepath.Join(spoolDir, entry.Name())
		if strings.HasSuffix(entry.Name(), ".part") {
			// recv-*.part files can only survive a hard process/container crash.
			// cleanupOrphans runs during startup before new WebDAV PUTs are served.
			_ = os.Remove(p)
			continue
		}
		if !strings.HasSuffix(entry.Name(), ".data") {
			continue
		}
		if _, ok := keep[filepath.Clean(p)]; !ok {
			_ = os.Remove(p)
		}
	}
}
