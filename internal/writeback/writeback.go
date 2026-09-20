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

func isPathOrDescendant(candidate, root string) bool {
	candidate = utils.FixAndCleanPath(candidate)
	root = utils.FixAndCleanPath(root)
	if root == "/" {
		return strings.HasPrefix(candidate, "/")
	}
	return candidate == root || strings.HasPrefix(candidate, root+"/")
}

func descendantLikePattern(root string) string {
	root = utils.FixAndCleanPath(root)
	escaped := strings.NewReplacer(
		"~", "~~",
		"%", "~%",
		"_", "~_",
	).Replace(root)
	if root == "/" {
		return "/%"
	}
	return escaped + "/%"
}

func canonicalETag(key string, generation uint64, size int64) string {
	return fmt.Sprintf("\"olwb-%s-%d-%x\"", key[:16], generation, uint64(size))
}

func clearRemoteVerification(row *model.WebDAVWritebackObject) {
	if row == nil {
		return
	}
	row.RemoteObjectID = ""
	row.RemoteSHA1 = ""
	row.RemoteGeneration = 0
	row.RemoteVerifiedAt = nil
}

func canonicalContentSHA1(row *model.WebDAVWritebackObject) string {
	if row == nil {
		return ""
	}
	if row.PayloadSHA1 != "" {
		return row.PayloadSHA1
	}
	if row.RemoteGeneration == row.Generation {
		return row.RemoteSHA1
	}
	return ""
}

func canCoalesceDuplicatePut(row *model.WebDAVWritebackObject, size int64, payloadSHA1 string) bool {
	return row != nil &&
		!row.IsDir &&
		row.State != StateDeleted &&
		row.SpoolPath != "" &&
		row.Size == size &&
		row.PayloadSHA1 != "" &&
		payloadSHA1 != "" &&
		strings.EqualFold(row.PayloadSHA1, payloadSHA1)
}

func canReverifyCompletedDuplicatePut(row *model.WebDAVWritebackObject, size int64, payloadSHA1 string) bool {
	if row == nil ||
		row.IsDir ||
		row.State != StateCompleted ||
		row.SpoolPath != "" ||
		row.Size != size ||
		payloadSHA1 == "" {
		return false
	}
	canonicalSHA1 := canonicalContentSHA1(row)
	return canonicalSHA1 != "" && strings.EqualFold(canonicalSHA1, payloadSHA1)
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
			IsFolder: row.IsDir,
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

func remoteContentMatchesCanonical(row *model.WebDAVWritebackObject, remote model.Obj) bool {
	if row == nil || remote == nil || remote.IsDir() != row.IsDir {
		return false
	}
	if row.IsDir {
		return true
	}
	if remote.GetSize() != row.Size {
		return false
	}
	expectedSHA1 := canonicalContentSHA1(row)
	if expectedSHA1 != "" {
		remoteSHA1 := remote.GetHash().GetHash(utils.SHA1)
		if remoteSHA1 != "" && !strings.EqualFold(remoteSHA1, expectedSHA1) {
			return false
		}
	}
	return true
}

func deleteCompletedCanonical(row *model.WebDAVWritebackObject) (bool, error) {
	res := db.GetDb().
		Where("id = ? AND generation = ? AND state = ? AND spool_path = ''", row.ID, row.Generation, StateCompleted).
		Delete(&model.WebDAVWritebackObject{})
	if res.Error != nil {
		return false, res.Error
	}
	return res.RowsAffected > 0, nil
}

// ReconcileDirect validates a completed canonical object after its local spool
// cache is gone. Provider mtime is deliberately ignored; content identity is
// based on resource type, size and SHA-1 when the provider exposes it.
func ReconcileDirect(ctx context.Context, p string) (missing bool, err error) {
	if !Enabled() {
		return false, nil
	}
	row, err := getByPath(p)
	if err != nil || row == nil || row.State != StateCompleted || row.SpoolPath != "" {
		return false, err
	}
	if canonicalShadowInGrace(row, time.Now()) {
		return false, nil
	}

	remote, getErr := fs.Get(ctx, row.Path, &fs.GetArgs{NoLog: true})
	if getErr == nil && remote != nil && remote.IsDir() == row.IsDir {
		if !row.IsDir && remoteContentMatchesCanonical(row, remote) {
			return false, nil
		}
		// Expired directory shadows hand control back to the provider. For files,
		// a known size/SHA1 mismatch exposes the real remote object so one-way
		// Cloud Sync can repair it.
		_, delErr := deleteCompletedCanonical(row)
		return false, delErr
	}
	if getErr != nil && !errs.IsObjectNotFound(getErr) {
		return false, nil
	}

	objs, listErr := fs.List(ctx, row.Parent, &fs.ListArgs{Refresh: true, NoLog: true})
	if listErr != nil {
		return false, nil
	}
	for _, obj := range objs {
		if obj.GetName() != row.Name {
			continue
		}
		if obj.IsDir() == row.IsDir {
			if !row.IsDir && remoteContentMatchesCanonical(row, obj) {
				return false, nil
			}
			_, delErr := deleteCompletedCanonical(row)
			return false, delErr
		}
		// The name exists with the wrong resource type.
		_, delErr := deleteCompletedCanonical(row)
		return false, delErr
	}

	dropped, delErr := deleteCompletedCanonical(row)
	if delErr != nil {
		return false, delErr
	}
	return dropped, nil
}

func shouldDropCanonicalAfterRemoteList(row *model.WebDAVWritebackObject, remoteReliable bool, remote model.Obj, now time.Time) bool {
	if !remoteReliable ||
		row.IsDir ||
		row.State != StateCompleted ||
		row.SpoolPath != "" ||
		canonicalShadowInGrace(row, now) {
		return false
	}
	if remote == nil {
		return true
	}
	return !remoteContentMatchesCanonical(row, remote)
}

func canonicalShadowInGrace(row *model.WebDAVWritebackObject, now time.Time) bool {
	if row.State != StateCompleted || row.CompletedAt == nil {
		return false
	}
	// The directory consistency window also protects freshly remapped completed
	// file metadata after a provider MOVE. Normal completed files retain their
	// local spool far longer than this window, so this only changes the
	// no-spool provider-fallback edge case.
	grace := max(1, conf.Conf.WebDAVWriteback.DirectoryGraceSeconds)
	return now.Before(row.CompletedAt.Add(time.Duration(grace) * time.Second))
}

func directoryShadowExpired(row *model.WebDAVWritebackObject, now time.Time) bool {
	return row.IsDir && row.State == StateCompleted && row.CompletedAt != nil && !canonicalShadowInGrace(row, now)
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
	now := time.Now()
	for i := range rows {
		row := &rows[i]
		if row.State == StateDeleted {
			delete(byName, row.Name)
			continue
		}
		remoteObj, remotePresent := byName[row.Name]
		if row.IsDir {
			if remoteReliable && directoryShadowExpired(row, now) {
				res := db.GetDb().
					Where("id = ? AND generation = ? AND state = ?", row.ID, row.Generation, StateCompleted).
					Delete(&model.WebDAVWritebackObject{})
				if res.Error != nil {
					return nil, false, res.Error
				}
				// After the grace window, hand the name back to the provider. If
				// it is missing, remove the shadowed name; if it exists with either
				// directory or file type, leave the real provider object visible.
				if !remotePresent {
					delete(byName, row.Name)
				}
				continue
			}
		}
		if shouldDropCanonicalAfterRemoteList(row, remoteReliable, remoteObj, now) {
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

func cloudSyncSettleDelay(size int64) time.Duration {
	ms := conf.Conf.WebDAVWriteback.CloudSyncSettleMillis
	if size == 0 {
		ms = max(ms, conf.Conf.WebDAVWriteback.CloudSyncPlaceholderMillis)
	}
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

func copyToSpool(dst *os.File, src io.Reader, expected int64) (int64, string, error) {
	buf := make([]byte, 4*utils.MB)
	payloadHasher := utils.SHA1.NewFunc()
	writer := io.MultiWriter(dst, payloadHasher)
	var total int64
	var sinceCheck int64
	for {
		n, readErr := src.Read(buf)
		if n > 0 {
			wn, writeErr := writer.Write(buf[:n])
			total += int64(wn)
			sinceCheck += int64(wn)
			if writeErr != nil {
				return total, "", writeErr
			}
			if wn != n {
				return total, "", io.ErrShortWrite
			}
			if sinceCheck >= 64*utils.MB {
				if err := checkFreeSpace(0); err != nil {
					return total, "", err
				}
				sinceCheck = 0
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return total, "", readErr
		}
	}
	if expected >= 0 && total != expected {
		return total, "", fmt.Errorf("incomplete WebDAV PUT: expected %d bytes, received %d", expected, total)
	}
	return total, hex.EncodeToString(payloadHasher.Sum(nil)), nil
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

	actualSize, payloadSHA1, err := copyToSpool(tmp, body, expected)
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
	duplicate := false
	wakeDuplicate := false
	settleAt := time.Now().Add(cloudSyncSettleDelay(actualSize))

	err = db.GetDb().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var row model.WebDAVWritebackObject
		findErr := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("path_key = ?", key).First(&row).Error
		if findErr != nil && !errors.Is(findErr, gorm.ErrRecordNotFound) {
			return findErr
		}
		if findErr == nil {
			if canCoalesceDuplicatePut(&row, actualSize, payloadSHA1) {
				if _, statErr := os.Stat(row.SpoolPath); statErr == nil {
					duplicate = true
					if row.State == StateQueued || row.State == StateFailed {
						now := time.Now()
						row.RetryAt = &now
						row.LastError = ""
						if err := tx.Save(&row).Error; err != nil {
							return err
						}
						wakeDuplicate = true
					}
					saved = row
					return nil
				}
			}
			if canReverifyCompletedDuplicatePut(&row, actualSize, payloadSHA1) {
				now := time.Now()
				row.State = StateVerifying
				row.SpoolPath = finalName
				row.PayloadSHA1 = payloadSHA1
				row.MimeType = mime
				row.CleanupPath = ""
				row.LastError = ""
				row.RetryCount = 0
				row.VerifyCount = 0
				row.RetryAt = &now
				row.CompletedAt = nil
				clearRemoteVerification(&row)
				if err := tx.Save(&row).Error; err != nil {
					return err
				}
				saved = row
				return nil
			}
			oldSpool = row.SpoolPath
			if row.State == StateDeleted {
				created = true
			}
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
		row.IsDir = false
		row.Size = actualSize
		row.ModTime = modTime
		row.CreateTime = createTime
		row.ETag = canonicalETag(key, row.Generation, actualSize)
		row.State = StateQueued
		row.SpoolPath = finalName
		row.PayloadSHA1 = payloadSHA1
		row.MimeType = mime
		row.CleanupPath = ""
		row.LastError = ""
		row.RetryCount = 0
		row.VerifyCount = 0
		row.RetryAt = &settleAt
		row.CompletedAt = nil
		clearRemoteVerification(&row)

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
	if duplicate {
		_ = os.Remove(finalName)
		syncDir(spoolDir)
		if wakeDuplicate {
			wake()
		}
		return &saved, false, nil
	}

	if oldSpool != "" && oldSpool != finalName {
		if !spoolIsActive(oldSpool) {
			removeSpoolIfUnreferenced(oldSpool)
		}
	}
	wake()
	return &saved, created, nil
}

// CommitDir makes a WebDAV collection immediately visible to Cloud Sync and
// persists directory creation so provider-side path visibility can catch up in
// the background.
func CommitDir(ctx context.Context, p string, modTime, createTime time.Time) (*model.WebDAVWritebackObject, bool, error) {
	if !Enabled() {
		return nil, false, errors.New("WebDAV write-back is disabled")
	}
	p = utils.FixAndCleanPath(p)
	if modTime.IsZero() {
		modTime = time.Now()
	}
	if createTime.IsZero() {
		createTime = modTime
	}
	parent := path.Dir(p)
	key := pathKey(p)
	now := time.Now()
	var saved model.WebDAVWritebackObject
	created := false

	err := db.GetDb().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var row model.WebDAVWritebackObject
		findErr := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("path_key = ?", key).First(&row).Error
		if findErr != nil && !errors.Is(findErr, gorm.ErrRecordNotFound) {
			return findErr
		}
		if findErr == nil {
			if row.State != StateDeleted && !row.IsDir {
				return ErrDestinationExists
			}
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
		row.IsDir = true
		row.Size = 0
		row.ModTime = modTime
		row.CreateTime = createTime
		row.ETag = canonicalETag(key, row.Generation, 0)
		row.State = StateQueued
		row.SpoolPath = ""
		row.PayloadSHA1 = ""
		row.MimeType = ""
		row.CleanupPath = ""
		row.LastError = ""
		row.RetryCount = 0
		row.VerifyCount = 0
		row.RetryAt = &now
		row.CompletedAt = nil
		clearRemoteVerification(&row)

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
		return nil, false, err
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

// DeleteTree creates durable tombstones for a tracked path and every tracked
// descendant. If only descendants are tracked, add a synthetic directory
// tombstone so the backing provider tree is still removed asynchronously.
func DeleteTree(p string) (bool, error) {
	if !Enabled() {
		return false, nil
	}
	p = utils.FixAndCleanPath(p)
	now := time.Now()
	handled := false

	err := db.GetDb().Transaction(func(tx *gorm.DB) error {
		var candidates []model.WebDAVWritebackObject
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("path = ? OR path LIKE ? ESCAPE '~'", p, descendantLikePattern(p)).
			Find(&candidates).Error; err != nil {
			return err
		}

		hasExact := false
		matchedDescendant := false
		for i := range candidates {
			row := &candidates[i]
			if !isPathOrDescendant(row.Path, p) {
				continue
			}
			handled = true
			if row.Path == p {
				hasExact = true
			} else {
				matchedDescendant = true
			}
			if row.State == StateDeleted {
				continue
			}
			if err := tx.Model(&model.WebDAVWritebackObject{}).
				Where("id = ?", row.ID).
				Updates(map[string]any{
					"generation":   gorm.Expr("generation + 1"),
					"state":        StateDeleted,
					"retry_at":     &now,
					"last_error":   "",
					"retry_count":        0,
					"verify_count":       0,
					"completed_at":       nil,
					"remote_object_id":   "",
					"remote_sha1":        "",
					"remote_generation":  0,
					"remote_verified_at": nil,
				}).Error; err != nil {
				return err
			}
		}

		if !hasExact && matchedDescendant {
			key := pathKey(p)
			row := model.WebDAVWritebackObject{
				PathKey:    key,
				ParentKey:  pathKey(path.Dir(p)),
				Path:       p,
				Parent:     path.Dir(p),
				Name:       path.Base(p),
				IsDir:      true,
				Size:       0,
				ModTime:    now,
				CreateTime: now,
				ETag:       canonicalETag(key, 1, 0),
				Generation: 1,
				State:      StateDeleted,
				RetryAt:    &now,
			}
			if err := tx.Create(&row).Error; err != nil {
				return err
			}
			handled = true
		}
		return nil
	})
	if err != nil {
		return handled, err
	}
	if handled {
		wake()
	}
	return handled, nil
}

func tombstoneMovedSource(row *model.WebDAVWritebackObject, now time.Time) {
	row.Generation++
	row.State = StateDeleted
	row.SpoolPath = ""
	row.PayloadSHA1 = ""
	row.CleanupPath = ""
	row.LastError = ""
	row.RetryCount = 0
	row.VerifyCount = 0
	row.RetryAt = &now
	row.CompletedAt = nil
	clearRemoteVerification(row)
}

func pendingDirectoryMoveLocallyAuthoritative(root *model.WebDAVWritebackObject, rows []model.WebDAVWritebackObject, now time.Time) bool {
	if root == nil || !root.IsDir || root.State == StateDeleted {
		return false
	}
	if root.State == StateCompleted {
		if root.CompletedAt == nil || directoryShadowExpired(root, now) {
			return false
		}
	}
	for i := range rows {
		row := &rows[i]
		if row.State == StateDeleted || row.IsDir {
			continue
		}
		if row.SpoolPath == "" {
			return false
		}
	}
	return true
}

var errPendingDirectoryMoveFallback = errors.New("pending directory move requires provider fallback")

func movePendingDirectory(src, dst string, overwrite bool) (handled bool, overwritten bool, err error) {
	if isPathOrDescendant(dst, src) {
		return false, false, nil
	}

	now := time.Now()
	var oldDestinationSpools []string
	err = db.GetDb().Transaction(func(tx *gorm.DB) error {
		// Lock both subtrees in one deterministic ID order. This avoids the
		// classic A->B / B->A pattern where two MOVE transactions lock their
		// source first and deadlock while trying to lock the other's target.
		var candidates []model.WebDAVWritebackObject
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("path = ? OR path LIKE ? ESCAPE '~' OR path = ? OR path LIKE ? ESCAPE '~'", src, descendantLikePattern(src), dst, descendantLikePattern(dst)).
			Order("id asc").
			Find(&candidates).Error; err != nil {
			return err
		}

		sourceRows := make([]model.WebDAVWritebackObject, 0, len(candidates))
		destinationRows := make([]model.WebDAVWritebackObject, 0, len(candidates))
		rootIndex := -1
		for i := range candidates {
			row := candidates[i]
			if isPathOrDescendant(row.Path, src) {
				if row.Path == src {
					rootIndex = len(sourceRows)
				}
				sourceRows = append(sourceRows, row)
			}
			if isPathOrDescendant(row.Path, dst) {
				destinationRows = append(destinationRows, row)
			}
		}
		if rootIndex < 0 || !pendingDirectoryMoveLocallyAuthoritative(&sourceRows[rootIndex], sourceRows, now) {
			return errPendingDirectoryMoveFallback
		}

		destinationByPath := make(map[string]int, len(destinationRows))
		for i := range destinationRows {
			row := &destinationRows[i]
			if row.State != StateDeleted {
				if !overwrite {
					return ErrDestinationExists
				}
				// Overwriting an already-live directory tree is intentionally
				// delegated to the provider; locally merging two trees would
				// make WebDAV overwrite semantics ambiguous.
				return errPendingDirectoryMoveFallback
			}
			destinationByPath[utils.FixAndCleanPath(row.Path)] = i
		}

		for i := range sourceRows {
			sourceRow := &sourceRows[i]
			if sourceRow.State == StateDeleted {
				continue
			}
			suffix := strings.TrimPrefix(sourceRow.Path, src)
			newPath := utils.FixAndCleanPath(dst + suffix)
			newParent := path.Dir(newPath)

			var destinationRow model.WebDAVWritebackObject
			if idx, ok := destinationByPath[newPath]; ok {
				destinationRow = destinationRows[idx]
				if destinationRow.ID == sourceRow.ID {
					return errPendingDirectoryMoveFallback
				}
				if destinationRow.SpoolPath != "" && destinationRow.SpoolPath != sourceRow.SpoolPath {
					oldDestinationSpools = append(oldDestinationSpools, destinationRow.SpoolPath)
				}
				destinationRow.Generation++
			} else {
				destinationRow.Generation = 1
			}

			destinationRow.PathKey = pathKey(newPath)
			destinationRow.ParentKey = pathKey(newParent)
			destinationRow.Path = newPath
			destinationRow.Parent = newParent
			destinationRow.Name = path.Base(newPath)
			destinationRow.IsDir = sourceRow.IsDir
			destinationRow.Size = sourceRow.Size
			destinationRow.ModTime = sourceRow.ModTime
			destinationRow.CreateTime = sourceRow.CreateTime
			destinationRow.ETag = canonicalETag(destinationRow.PathKey, destinationRow.Generation, sourceRow.Size)
			destinationRow.State = StateQueued
			if sourceRow.IsDir {
				destinationRow.SpoolPath = ""
			} else {
				destinationRow.SpoolPath = sourceRow.SpoolPath
			}
			destinationRow.PayloadSHA1 = sourceRow.PayloadSHA1
			destinationRow.MimeType = sourceRow.MimeType
			destinationRow.CleanupPath = ""
			destinationRow.LastError = ""
			destinationRow.RetryCount = 0
			destinationRow.VerifyCount = 0
			destinationRow.CompletedAt = nil
			clearRemoteVerification(&destinationRow)
			retryAt := now
			if !sourceRow.IsDir {
				retryAt = now.Add(cloudSyncSettleDelay(sourceRow.Size))
			}
			destinationRow.RetryAt = &retryAt

			if destinationRow.ID == 0 {
				if err := tx.Create(&destinationRow).Error; err != nil {
					return err
				}
			} else if err := tx.Save(&destinationRow).Error; err != nil {
				return err
			}
		}

		for i := range sourceRows {
			sourceRow := &sourceRows[i]
			if sourceRow.State == StateDeleted {
				continue
			}
			tombstoneMovedSource(sourceRow, now)
			if err := tx.Save(sourceRow).Error; err != nil {
				return err
			}
		}
		handled = true
		return nil
	})
	if errors.Is(err, errPendingDirectoryMoveFallback) {
		return false, false, nil
	}
	if err != nil {
		return true, overwritten, err
	}
	for _, spoolPath := range oldDestinationSpools {
		if spoolPath != "" && !spoolIsActive(spoolPath) {
			removeSpoolIfUnreferenced(spoolPath)
		}
	}
	if handled {
		wake()
	}
	return handled, overwritten, nil
}

func pendingDirectoryCopyLocallyAuthoritative(root *model.WebDAVWritebackObject, rows []model.WebDAVWritebackObject, now time.Time, recursive bool) bool {
	if root == nil || !root.IsDir || root.State == StateDeleted {
		return false
	}
	if root.State == StateCompleted {
		if root.CompletedAt == nil || directoryShadowExpired(root, now) {
			return false
		}
	}
	if !recursive {
		return true
	}
	return pendingDirectoryMoveLocallyAuthoritative(root, rows, now)
}

var errPendingDirectoryCopyFallback = errors.New("pending directory copy requires provider fallback")

func copyPendingDirectory(src, dst string, recursive bool) (handled bool, overwritten bool, err error) {
	if isPathOrDescendant(dst, src) {
		return false, false, nil
	}

	now := time.Now()
	err = db.GetDb().Transaction(func(tx *gorm.DB) error {
		var candidates []model.WebDAVWritebackObject
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("path = ? OR path LIKE ? ESCAPE '~' OR path = ? OR path LIKE ? ESCAPE '~'", src, descendantLikePattern(src), dst, descendantLikePattern(dst)).
			Order("id asc").
			Find(&candidates).Error; err != nil {
			return err
		}

		sourceRows := make([]model.WebDAVWritebackObject, 0, len(candidates))
		destinationRows := make([]model.WebDAVWritebackObject, 0, len(candidates))
		rootIndex := -1
		for i := range candidates {
			row := candidates[i]
			if isPathOrDescendant(row.Path, src) {
				if row.Path == src {
					rootIndex = len(sourceRows)
				}
				sourceRows = append(sourceRows, row)
			}
			if isPathOrDescendant(row.Path, dst) {
				destinationRows = append(destinationRows, row)
			}
		}
		if rootIndex < 0 || !pendingDirectoryCopyLocallyAuthoritative(&sourceRows[rootIndex], sourceRows, now, recursive) {
			return errPendingDirectoryCopyFallback
		}

		// The asynchronous local-tree fast path is intentionally create-only.
		// Replacing an existing remote directory may leave provider-only children
		// that are unknown to canonical state; delegate that case to provider
		// overwrite semantics instead of accidentally merging two trees.
		if len(destinationRows) != 0 {
			return errPendingDirectoryCopyFallback
		}

		for i := range sourceRows {
			sourceRow := &sourceRows[i]
			if sourceRow.State == StateDeleted {
				continue
			}
			if !recursive && sourceRow.Path != src {
				continue
			}

			suffix := strings.TrimPrefix(sourceRow.Path, src)
			newPath := utils.FixAndCleanPath(dst + suffix)
			newParent := path.Dir(newPath)
			retryAt := now
			spoolPath := ""
			payloadSHA1 := ""
			if !sourceRow.IsDir {
				spoolPath = sourceRow.SpoolPath
				payloadSHA1 = sourceRow.PayloadSHA1
				retryAt = now.Add(cloudSyncSettleDelay(sourceRow.Size))
			}

			row := model.WebDAVWritebackObject{
				PathKey:     pathKey(newPath),
				ParentKey:   pathKey(newParent),
				Path:        newPath,
				Parent:      newParent,
				Name:        path.Base(newPath),
				IsDir:       sourceRow.IsDir,
				Size:        sourceRow.Size,
				ModTime:     sourceRow.ModTime,
				CreateTime:  sourceRow.CreateTime,
				ETag:        canonicalETag(pathKey(newPath), 1, sourceRow.Size),
				Generation:  1,
				State:       StateQueued,
				SpoolPath:   spoolPath,
				PayloadSHA1: payloadSHA1,
				MimeType:    sourceRow.MimeType,
				RetryAt:     &retryAt,
			}
			if err := tx.Create(&row).Error; err != nil {
				return err
			}
		}
		handled = true
		return nil
	})
	if errors.Is(err, errPendingDirectoryCopyFallback) {
		return false, false, nil
	}
	if err != nil {
		return true, false, err
	}
	if handled {
		wake()
	}
	return handled, false, nil
}

// MovePending handles an exact pending file move without waiting for provider
// visibility. The source always becomes an immediate tombstone, even when the
// destination did not previously exist. This prevents a provider-visible old
// path from leaking back into Cloud Sync while the destination upload catches
// up asynchronously. overwritten reports whether the canonical destination
// existed so WebDAV can return 204 instead of 201.
func MovePending(src, dst string, overwrite bool) (handled bool, overwritten bool, err error) {
	src = utils.FixAndCleanPath(src)
	dst = utils.FixAndCleanPath(dst)
	srcKey := pathKey(src)
	dstKey := pathKey(dst)
	if srcKey == dstKey {
		return false, false, nil
	}

	var srcRow model.WebDAVWritebackObject
	if err := db.GetDb().Where("path_key = ?", srcKey).First(&srcRow).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, false, nil
		}
		return false, false, err
	}
	if srcRow.State == StateDeleted {
		return false, false, nil
	}
	if srcRow.IsDir {
		return movePendingDirectory(src, dst, overwrite)
	}
	if srcRow.SpoolPath == "" {
		return false, false, nil
	}

	var oldDestinationSpool string
	now := time.Now()
	settleAt := now.Add(cloudSyncSettleDelay(srcRow.Size))
	err = db.GetDb().Transaction(func(tx *gorm.DB) error {
		var lockedSrc model.WebDAVWritebackObject
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", srcRow.ID).First(&lockedSrc).Error; err != nil {
			return err
		}
		if lockedSrc.IsDir || lockedSrc.State == StateDeleted || lockedSrc.SpoolPath == "" {
			return gorm.ErrRecordNotFound
		}

		var dstRow model.WebDAVWritebackObject
		dstErr := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("path_key = ?", dstKey).First(&dstRow).Error
		if dstErr != nil && !errors.Is(dstErr, gorm.ErrRecordNotFound) {
			return dstErr
		}
		if dstErr == nil {
			if dstRow.ID == lockedSrc.ID {
				return gorm.ErrRecordNotFound
			}
			destinationExists := dstRow.State != StateDeleted
			if destinationExists && !overwrite {
				return ErrDestinationExists
			}
			if destinationExists && dstRow.IsDir {
				return ErrDestinationExists
			}
			overwritten = destinationExists
			oldDestinationSpool = dstRow.SpoolPath
			dstRow.Generation++
		} else {
			dstRow.Generation = 1
		}

		dstRow.PathKey = dstKey
		dstRow.ParentKey = pathKey(path.Dir(dst))
		dstRow.Path = dst
		dstRow.Parent = path.Dir(dst)
		dstRow.Name = path.Base(dst)
		dstRow.IsDir = false
		dstRow.Size = lockedSrc.Size
		dstRow.ModTime = lockedSrc.ModTime
		dstRow.CreateTime = lockedSrc.CreateTime
		dstRow.ETag = canonicalETag(dstKey, dstRow.Generation, lockedSrc.Size)
		dstRow.State = StateQueued
		dstRow.SpoolPath = lockedSrc.SpoolPath
		dstRow.PayloadSHA1 = lockedSrc.PayloadSHA1
		dstRow.MimeType = lockedSrc.MimeType
		dstRow.CleanupPath = ""
		dstRow.LastError = ""
		dstRow.RetryCount = 0
		dstRow.VerifyCount = 0
		clearRemoteVerification(&dstRow)
		dstRow.RetryAt = &settleAt
		dstRow.CompletedAt = nil

		if dstRow.ID == 0 {
			if err := tx.Create(&dstRow).Error; err != nil {
				return err
			}
		} else if err := tx.Save(&dstRow).Error; err != nil {
			return err
		}

		tombstoneMovedSource(&lockedSrc, now)
		return tx.Save(&lockedSrc).Error
	})
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, false, nil
	}
	if err != nil {
		return true, overwritten, err
	}
	if oldDestinationSpool != "" && oldDestinationSpool != srcRow.SpoolPath {
		if !spoolIsActive(oldDestinationSpool) {
			removeSpoolIfUnreferenced(oldDestinationSpool)
		}
	}
	wake()
	return true, overwritten, nil
}

// CopyPending copies a canonical file directly from the durable local spool.
// This lets Cloud Sync COPY a file immediately after PUT, before 115 exposes
// the source object. The source and destination safely reference the same
// immutable generation spool payload until their independent uploads finish.
func CopyPending(src, dst string, overwrite bool, recursive bool) (handled bool, overwritten bool, err error) {
	src = utils.FixAndCleanPath(src)
	dst = utils.FixAndCleanPath(dst)
	if src == dst {
		return false, false, nil
	}
	var srcRow model.WebDAVWritebackObject
	if err := db.GetDb().Where("path_key = ?", pathKey(src)).First(&srcRow).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, false, nil
		}
		return false, false, err
	}
	if srcRow.State == StateDeleted {
		return false, false, nil
	}
	if srcRow.IsDir {
		return copyPendingDirectory(src, dst, recursive)
	}
	if srcRow.SpoolPath == "" {
		return false, false, nil
	}

	var oldDestinationSpool string
	settleAt := time.Now().Add(cloudSyncSettleDelay(srcRow.Size))
	err = db.GetDb().Transaction(func(tx *gorm.DB) error {
		var lockedSrc model.WebDAVWritebackObject
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", srcRow.ID).First(&lockedSrc).Error; err != nil {
			return err
		}
		if lockedSrc.IsDir || lockedSrc.State == StateDeleted || lockedSrc.SpoolPath == "" {
			return gorm.ErrRecordNotFound
		}

		dstKey := pathKey(dst)
		var dstRow model.WebDAVWritebackObject
		dstErr := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("path_key = ?", dstKey).First(&dstRow).Error
		if dstErr != nil && !errors.Is(dstErr, gorm.ErrRecordNotFound) {
			return dstErr
		}

		if dstErr == nil {
			destinationExists := dstRow.State != StateDeleted
			if destinationExists && !overwrite {
				return ErrDestinationExists
			}
			if destinationExists && dstRow.IsDir {
				return ErrDestinationExists
			}
			overwritten = destinationExists
			oldDestinationSpool = dstRow.SpoolPath
			dstRow.Generation++
		} else {
			dstRow.Generation = 1
		}

		dstRow.PathKey = dstKey
		dstRow.ParentKey = pathKey(path.Dir(dst))
		dstRow.Path = dst
		dstRow.Parent = path.Dir(dst)
		dstRow.Name = path.Base(dst)
		dstRow.IsDir = false
		dstRow.Size = lockedSrc.Size
		dstRow.ModTime = lockedSrc.ModTime
		dstRow.CreateTime = lockedSrc.CreateTime
		dstRow.ETag = canonicalETag(dstKey, dstRow.Generation, lockedSrc.Size)
		dstRow.State = StateQueued
		dstRow.SpoolPath = lockedSrc.SpoolPath
		dstRow.PayloadSHA1 = lockedSrc.PayloadSHA1
		dstRow.MimeType = lockedSrc.MimeType
		dstRow.CleanupPath = ""
		dstRow.LastError = ""
		dstRow.RetryCount = 0
		dstRow.VerifyCount = 0
		clearRemoteVerification(&dstRow)
		dstRow.RetryAt = &settleAt
		dstRow.CompletedAt = nil

		if dstRow.ID == 0 {
			return tx.Create(&dstRow).Error
		}
		return tx.Save(&dstRow).Error
	})
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, false, nil
	}
	if err != nil {
		return true, overwritten, err
	}
	if oldDestinationSpool != "" && oldDestinationSpool != srcRow.SpoolPath {
		if !spoolIsActive(oldDestinationSpool) {
			removeSpoolIfUnreferenced(oldDestinationSpool)
		}
	}
	wake()
	return true, overwritten, nil
}

func providerOverwriteQuiescent(rows []model.WebDAVWritebackObject) bool {
	for i := range rows {
		if rows[i].State != StateCompleted {
			return false
		}
	}
	return true
}

func refreshUnknownProviderOverwrite(row *model.WebDAVWritebackObject, now time.Time) {
	row.Generation++
	row.ETag = canonicalETag(pathKey(row.Path), row.Generation, row.Size)
	row.State = StateCompleted
	row.SpoolPath = ""
	row.CleanupPath = ""
	row.LastError = ""
	row.RetryCount = 0
	row.VerifyCount = 0
	clearRemoteVerification(row)
	row.RetryAt = nil
	row.CompletedAt = &now
}

func setProviderCompletedRoot(row *model.WebDAVWritebackObject, dst string, source model.Obj, now time.Time) {
	dst = utils.FixAndCleanPath(dst)
	if row.Generation == 0 {
		row.Generation = 1
	} else {
		row.Generation++
	}
	row.PathKey = pathKey(dst)
	row.ParentKey = pathKey(path.Dir(dst))
	row.Path = dst
	row.Parent = path.Dir(dst)
	row.Name = path.Base(dst)
	row.IsDir = source.IsDir()
	row.Size = source.GetSize()
	row.ModTime = source.ModTime()
	row.CreateTime = source.CreateTime()
	row.ETag = canonicalETag(row.PathKey, row.Generation, row.Size)
	row.State = StateCompleted
	row.SpoolPath = ""
	row.PayloadSHA1 = source.GetHash().GetHash(utils.SHA1)
	if row.IsDir {
		row.MimeType = ""
	} else {
		row.MimeType = utils.GetMimeType(dst)
	}
	row.CleanupPath = ""
	row.LastError = ""
	row.RetryCount = 0
	row.VerifyCount = 0
	clearRemoteVerification(row)
	row.RetryAt = nil
	row.CompletedAt = &now
}

func setMovedDestinationFromSource(row, source *model.WebDAVWritebackObject, dst string, now time.Time) {
	if row == nil || source == nil {
		return
	}
	dst = utils.FixAndCleanPath(dst)
	if row.Generation == 0 {
		row.Generation = 1
	} else {
		row.Generation++
	}
	row.PathKey = pathKey(dst)
	row.ParentKey = pathKey(path.Dir(dst))
	row.Path = dst
	row.Parent = path.Dir(dst)
	row.Name = path.Base(dst)
	row.IsDir = source.IsDir
	row.Size = source.Size
	row.ModTime = source.ModTime
	row.CreateTime = source.CreateTime
	row.ETag = canonicalETag(row.PathKey, row.Generation, row.Size)
	row.SpoolPath = source.SpoolPath
	row.PayloadSHA1 = source.PayloadSHA1
	row.MimeType = source.MimeType
	row.CleanupPath = ""
	row.LastError = ""
	row.RetryCount = 0
	row.VerifyCount = 0
	clearRemoteVerification(row)

	switch {
	case source.State == StateDeleted:
		row.State = StateDeleted
		row.SpoolPath = ""
		row.PayloadSHA1 = ""
		row.RetryAt = &now
		row.CompletedAt = nil
	case source.State == StateCompleted && source.SpoolPath == "":
		row.State = StateCompleted
		row.RetryAt = nil
		row.CompletedAt = &now
	default:
		row.State = StateQueued
		row.RetryAt = &now
		row.CompletedAt = nil
	}
}

func providerMoveSourceTombstone(src string, source model.Obj, now time.Time) model.WebDAVWritebackObject {
	src = utils.FixAndCleanPath(src)
	size := int64(0)
	isDir := false
	modTime := now
	createTime := now
	if source != nil {
		size = source.GetSize()
		isDir = source.IsDir()
		if !source.ModTime().IsZero() {
			modTime = source.ModTime()
		}
		if !source.CreateTime().IsZero() {
			createTime = source.CreateTime()
		} else {
			createTime = modTime
		}
	}
	key := pathKey(src)
	return model.WebDAVWritebackObject{
		PathKey:    key,
		ParentKey:  pathKey(path.Dir(src)),
		Path:       src,
		Parent:     path.Dir(src),
		Name:       path.Base(src),
		IsDir:      isDir,
		Size:       size,
		ModTime:    modTime,
		CreateTime: createTime,
		ETag:       canonicalETag(key, 1, size),
		Generation: 1,
		State:      StateDeleted,
		RetryAt:    &now,
	}
}

// ProviderOverwriteReady checks whether a provider COPY/MOVE may safely replace
// a tracked destination. It intentionally does not mutate canonical state:
// provider failure must leave the old stable WebDAV view intact.
func ProviderOverwriteReady(p string) (tracked bool, busy bool, err error) {
	if !Enabled() {
		return false, false, nil
	}
	p = utils.FixAndCleanPath(p)
	var candidates []model.WebDAVWritebackObject
	if err := db.GetDb().
		Where("path = ? OR path LIKE ? ESCAPE '~'", p, descendantLikePattern(p)).
		Order("id asc").
		Find(&candidates).Error; err != nil {
		return false, false, err
	}
	rows := make([]model.WebDAVWritebackObject, 0, len(candidates))
	for i := range candidates {
		row := candidates[i]
		if !isPathOrDescendant(row.Path, p) {
			continue
		}
		tracked = true
		rows = append(rows, row)
	}
	if len(rows) == 0 {
		return false, false, nil
	}
	return true, !providerOverwriteQuiescent(rows), nil
}

// CopyTreeMetadata follows a successful synchronous provider COPY. It mirrors
// canonical source generations to the destination only after the provider has
// succeeded, so a failed provider operation never destroys the old destination
// shadow. Pending source generations stay queued with their shared immutable
// spool and therefore still converge to the newest Cloud Sync payload.
func CopyTreeMetadata(src, dst string, sourceRoot model.Obj) error {
	if !Enabled() {
		return nil
	}
	src = utils.FixAndCleanPath(src)
	dst = utils.FixAndCleanPath(dst)
	if src == dst {
		return nil
	}
	if isPathOrDescendant(dst, src) || isPathOrDescendant(src, dst) {
		return fmt.Errorf("write-back metadata copy paths overlap: %s -> %s", src, dst)
	}

	now := time.Now()
	var staleSpools []string
	err := db.GetDb().Transaction(func(tx *gorm.DB) error {
		var candidates []model.WebDAVWritebackObject
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("path = ? OR path LIKE ? ESCAPE '~' OR path = ? OR path LIKE ? ESCAPE '~'", src, descendantLikePattern(src), dst, descendantLikePattern(dst)).
			Order("id asc").
			Find(&candidates).Error; err != nil {
			return err
		}

		sourceRows := make([]model.WebDAVWritebackObject, 0, len(candidates))
		destinationRows := make([]model.WebDAVWritebackObject, 0, len(candidates))
		for i := range candidates {
			row := candidates[i]
			if isPathOrDescendant(row.Path, src) {
				sourceRows = append(sourceRows, row)
			}
			if isPathOrDescendant(row.Path, dst) {
				destinationRows = append(destinationRows, row)
			}
		}
		if len(sourceRows) == 0 {
			if sourceRoot == nil {
				return nil
			}
			var root model.WebDAVWritebackObject
			rootFound := false
			for i := range destinationRows {
				if destinationRows[i].Path == dst {
					root = destinationRows[i]
					rootFound = true
					break
				}
			}
			setProviderCompletedRoot(&root, dst, sourceRoot, now)
			if rootFound {
				if err := tx.Save(&root).Error; err != nil {
					return err
				}
			} else if err := tx.Create(&root).Error; err != nil {
				return err
			}
			for i := range destinationRows {
				row := &destinationRows[i]
				if row.Path == dst {
					continue
				}
				if row.SpoolPath != "" {
					staleSpools = append(staleSpools, row.SpoolPath)
				}
				refreshUnknownProviderOverwrite(row, now)
				if err := tx.Save(row).Error; err != nil {
					return err
				}
			}
			return nil
		}

		destinationByPath := make(map[string]int, len(destinationRows))
		for i := range destinationRows {
			destinationByPath[utils.FixAndCleanPath(destinationRows[i].Path)] = i
		}
		usedDestination := make(map[uint]struct{})

		for i := range sourceRows {
			sourceRow := &sourceRows[i]
			suffix := strings.TrimPrefix(sourceRow.Path, src)
			newPath := utils.FixAndCleanPath(dst + suffix)
			newParent := path.Dir(newPath)

			var destinationRow model.WebDAVWritebackObject
			if idx, ok := destinationByPath[newPath]; ok {
				destinationRow = destinationRows[idx]
				usedDestination[destinationRow.ID] = struct{}{}
				if destinationRow.SpoolPath != "" && destinationRow.SpoolPath != sourceRow.SpoolPath {
					staleSpools = append(staleSpools, destinationRow.SpoolPath)
				}
				destinationRow.Generation++
			} else {
				destinationRow.Generation = 1
			}

			destinationRow.PathKey = pathKey(newPath)
			destinationRow.ParentKey = pathKey(newParent)
			destinationRow.Path = newPath
			destinationRow.Parent = newParent
			destinationRow.Name = path.Base(newPath)
			destinationRow.IsDir = sourceRow.IsDir
			destinationRow.Size = sourceRow.Size
			destinationRow.ModTime = sourceRow.ModTime
			destinationRow.CreateTime = sourceRow.CreateTime
			destinationRow.ETag = canonicalETag(destinationRow.PathKey, destinationRow.Generation, sourceRow.Size)
			destinationRow.SpoolPath = sourceRow.SpoolPath
			destinationRow.PayloadSHA1 = sourceRow.PayloadSHA1
			destinationRow.MimeType = sourceRow.MimeType
			destinationRow.CleanupPath = ""
			destinationRow.LastError = ""
			destinationRow.RetryCount = 0
			destinationRow.VerifyCount = 0
			clearRemoteVerification(&destinationRow)

			switch sourceRow.State {
			case StateDeleted:
				destinationRow.State = StateDeleted
				destinationRow.SpoolPath = ""
				destinationRow.PayloadSHA1 = ""
				destinationRow.RetryAt = &now
				destinationRow.CompletedAt = nil
			case StateCompleted:
				destinationRow.State = StateCompleted
				destinationRow.RetryAt = nil
				destinationRow.CompletedAt = &now
			default:
				destinationRow.State = StateQueued
				destinationRow.RetryAt = &now
				destinationRow.CompletedAt = nil
			}

			if destinationRow.ID == 0 {
				if err := tx.Create(&destinationRow).Error; err != nil {
					return err
				}
			} else if err := tx.Save(&destinationRow).Error; err != nil {
				return err
			}
		}

		hasSourceRoot := false
		for i := range sourceRows {
			if sourceRows[i].Path == src {
				hasSourceRoot = true
				break
			}
		}
		if !hasSourceRoot && sourceRoot != nil {
			newPath := dst
			var root model.WebDAVWritebackObject
			if idx, ok := destinationByPath[newPath]; ok {
				root = destinationRows[idx]
				usedDestination[root.ID] = struct{}{}
				if root.SpoolPath != "" {
					staleSpools = append(staleSpools, root.SpoolPath)
				}
			}
			setProviderCompletedRoot(&root, newPath, sourceRoot, now)
			if root.ID == 0 {
				if err := tx.Create(&root).Error; err != nil {
					return err
				}
			} else if err := tx.Save(&root).Error; err != nil {
				return err
			}
		}

		for i := range destinationRows {
			destinationRow := &destinationRows[i]
			if _, ok := usedDestination[destinationRow.ID]; ok {
				continue
			}
			if destinationRow.SpoolPath != "" {
				staleSpools = append(staleSpools, destinationRow.SpoolPath)
			}
			refreshUnknownProviderOverwrite(destinationRow, now)
			if err := tx.Save(destinationRow).Error; err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, spoolPath := range staleSpools {
		if spoolPath != "" && !spoolIsActive(spoolPath) {
			removeSpoolIfUnreferenced(spoolPath)
		}
	}
	wake()
	return nil
}

// MoveTreeMetadata follows a successful remote MOVE and keeps canonical
// metadata aligned with the new path. Pending descendants are re-queued.
func MoveTreeMetadata(src, dst string, sourceRoot model.Obj) error {
	if !Enabled() {
		return nil
	}
	src = utils.FixAndCleanPath(src)
	dst = utils.FixAndCleanPath(dst)
	if src == dst {
		return nil
	}
	if isPathOrDescendant(dst, src) || isPathOrDescendant(src, dst) {
		return fmt.Errorf("write-back metadata move paths overlap: %s -> %s", src, dst)
	}

	now := time.Now()
	var staleSpools []string
	err := db.GetDb().Transaction(func(tx *gorm.DB) error {
		var candidates []model.WebDAVWritebackObject
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("path = ? OR path LIKE ? ESCAPE '~' OR path = ? OR path LIKE ? ESCAPE '~'", src, descendantLikePattern(src), dst, descendantLikePattern(dst)).
			Order("id asc").
			Find(&candidates).Error; err != nil {
			return err
		}

		sourceRows := make([]model.WebDAVWritebackObject, 0, len(candidates))
		destinationRows := make([]model.WebDAVWritebackObject, 0, len(candidates))
		for i := range candidates {
			row := candidates[i]
			if isPathOrDescendant(row.Path, src) {
				sourceRows = append(sourceRows, row)
			}
			if isPathOrDescendant(row.Path, dst) {
				destinationRows = append(destinationRows, row)
			}
		}
		if len(sourceRows) == 0 {
			if sourceRoot == nil {
				return nil
			}
			var root model.WebDAVWritebackObject
			rootFound := false
			for i := range destinationRows {
				if destinationRows[i].Path == dst {
					root = destinationRows[i]
					rootFound = true
					break
				}
			}
			setProviderCompletedRoot(&root, dst, sourceRoot, now)
			if rootFound {
				if err := tx.Save(&root).Error; err != nil {
					return err
				}
			} else if err := tx.Create(&root).Error; err != nil {
				return err
			}
			for i := range destinationRows {
				row := &destinationRows[i]
				if row.Path == dst {
					continue
				}
				if row.SpoolPath != "" {
					staleSpools = append(staleSpools, row.SpoolPath)
				}
				refreshUnknownProviderOverwrite(row, now)
				if err := tx.Save(row).Error; err != nil {
					return err
				}
			}
			tombstone := providerMoveSourceTombstone(src, sourceRoot, now)
			if err := tx.Create(&tombstone).Error; err != nil {
				return err
			}
			return nil
		}

		destinationByPath := make(map[string]int, len(destinationRows))
		for i := range destinationRows {
			destinationByPath[utils.FixAndCleanPath(destinationRows[i].Path)] = i
		}
		collidedDestination := make(map[uint]struct{})

		for i := range sourceRows {
			sourceRow := &sourceRows[i]
			suffix := strings.TrimPrefix(sourceRow.Path, src)
			newPath := utils.FixAndCleanPath(dst + suffix)

			var destinationRow model.WebDAVWritebackObject
			if idx, ok := destinationByPath[newPath]; ok {
				destinationRow = destinationRows[idx]
				collidedDestination[destinationRow.ID] = struct{}{}
				if destinationRow.SpoolPath != "" && destinationRow.SpoolPath != sourceRow.SpoolPath {
					staleSpools = append(staleSpools, destinationRow.SpoolPath)
				}
			}
			setMovedDestinationFromSource(&destinationRow, sourceRow, newPath, now)
			if destinationRow.ID == 0 {
				if err := tx.Create(&destinationRow).Error; err != nil {
					return err
				}
			} else if err := tx.Save(&destinationRow).Error; err != nil {
				return err
			}

			tombstoneMovedSource(sourceRow, now)
			if err := tx.Save(sourceRow).Error; err != nil {
				return err
			}
		}

		hasSourceRoot := false
		for i := range sourceRows {
			if sourceRows[i].Path == src {
				hasSourceRoot = true
				break
			}
		}
		if !hasSourceRoot && sourceRoot != nil {
			var root model.WebDAVWritebackObject
			if idx, ok := destinationByPath[dst]; ok {
				root = destinationRows[idx]
				collidedDestination[root.ID] = struct{}{}
				if root.SpoolPath != "" {
					staleSpools = append(staleSpools, root.SpoolPath)
				}
			}
			setProviderCompletedRoot(&root, dst, sourceRoot, now)
			if root.ID == 0 {
				if err := tx.Create(&root).Error; err != nil {
					return err
				}
			} else if err := tx.Save(&root).Error; err != nil {
				return err
			}
			tombstone := providerMoveSourceTombstone(src, sourceRoot, now)
			if err := tx.Create(&tombstone).Error; err != nil {
				return err
			}
		}

		for i := range destinationRows {
			destinationRow := &destinationRows[i]
			if _, ok := collidedDestination[destinationRow.ID]; ok {
				continue
			}
			if destinationRow.SpoolPath != "" {
				staleSpools = append(staleSpools, destinationRow.SpoolPath)
			}
			refreshUnknownProviderOverwrite(destinationRow, now)
			if err := tx.Save(destinationRow).Error; err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, spoolPath := range staleSpools {
		if spoolPath != "" && !spoolIsActive(spoolPath) {
			removeSpoolIfUnreferenced(spoolPath)
		}
	}
	wake()
	return nil
}

var (
	managerMu      sync.Mutex
	manager        *workerManager
	activeSpoolMu  sync.Mutex
	activeSpoolRef = make(map[string]int)
)

func markSpoolActive(spoolPath string) func() {
	activeSpoolMu.Lock()
	activeSpoolRef[spoolPath]++
	activeSpoolMu.Unlock()
	return func() {
		activeSpoolMu.Lock()
		if activeSpoolRef[spoolPath] <= 1 {
			delete(activeSpoolRef, spoolPath)
		} else {
			activeSpoolRef[spoolPath]--
		}
		activeSpoolMu.Unlock()
	}
}

func spoolIsActive(spoolPath string) bool {
	activeSpoolMu.Lock()
	defer activeSpoolMu.Unlock()
	return activeSpoolRef[spoolPath] > 0
}

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
	// An UPLOADING file is ambiguous after a process crash: 115 may already
	// contain the exact encrypted payload even though MySQL never recorded
	// VERIFYING. Resume files in VERIFYING so the normal size/SHA-1 window runs
	// before retransmission. Directories have no payload/hash verification, so
	// re-queue interrupted MKCOL operations instead.
	now := time.Now()
	return db.GetDb().Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&model.WebDAVWritebackObject{}).
			Where("state = ? AND is_dir = ?", StateUploading, true).
			Updates(map[string]any{
				"state":        StateQueued,
				"retry_at":     &now,
				"verify_count": 0,
				"last_error":   "re-queued interrupted directory creation",
			}).Error; err != nil {
			return err
		}
		return tx.Model(&model.WebDAVWritebackObject{}).
			Where("state = ? AND is_dir = ?", StateUploading, false).
			Updates(map[string]any{
				"state":        StateVerifying,
				"retry_at":     &now,
				"verify_count": 0,
				"last_error":   "resuming remote verification after interrupted upload",
			}).Error
	})
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
		Select("id").
		Where("state IN ? AND (retry_at IS NULL OR retry_at <= ?)", []string{StateQueued, StateFailed, StateVerifying, StateDeleted}, now).
		Order("is_dir desc, updated_at asc").
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
		if row.IsDir {
			m.processMkdir(&row)
		} else {
			m.processUpload(&row)
		}
	}
}

func canonicalParentBlocksChild(parent *model.WebDAVWritebackObject) bool {
	return parent != nil && (!parent.IsDir || parent.State == StateDeleted)
}

func (m *workerManager) waitForCanonicalParent(row *model.WebDAVWritebackObject) (bool, error) {
	parent, err := getByPath(row.Parent)
	if err != nil {
		return false, err
	}
	if parent == nil {
		return false, nil
	}
	if canonicalParentBlocksChild(parent) {
		next := time.Now().Add(2 * time.Second)
		res := db.GetDb().Model(&model.WebDAVWritebackObject{}).
			Where("id = ? AND generation = ? AND state IN ?", row.ID, row.Generation, []string{StateQueued, StateFailed}).
			Updates(map[string]any{
				"retry_at":   &next,
				"last_error": "waiting because canonical parent is deleted or is not a directory",
			})
		return true, res.Error
	}
	if parent.State == StateCompleted {
		return false, nil
	}
	next := time.Now().Add(time.Second)
	res := db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("id = ? AND generation = ? AND state IN ?", row.ID, row.Generation, []string{StateQueued, StateFailed}).
		Update("retry_at", &next)
	return true, res.Error
}

func (m *workerManager) remoteDirectoryExists(row *model.WebDAVWritebackObject) bool {
	if existing, err := fs.Get(m.ctx, row.Path, &fs.GetArgs{NoLog: true}); err == nil && existing != nil && existing.IsDir() {
		return true
	}
	objs, err := fs.List(m.ctx, row.Parent, &fs.ListArgs{Refresh: true, NoLog: true})
	if err != nil {
		return false
	}
	for _, obj := range objs {
		if obj.GetName() == row.Name && obj.IsDir() {
			return true
		}
	}
	return false
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
	if waiting, err := m.waitForCanonicalParent(row); err != nil {
		m.failAfter(row, err, 2*time.Second)
		return
	} else if waiting {
		return
	}
	if isReceiving(row.Path) {
		next := time.Now().Add(time.Second)
		_ = db.GetDb().Model(&model.WebDAVWritebackObject{}).
			Where("id = ? AND generation = ? AND state IN ?", row.ID, row.Generation, []string{StateQueued, StateFailed}).
			Update("retry_at", &next).Error
		return
	}
	if row.RetryCount > 0 {
		requireHash := providerRequiresPayloadHash(row.Path)
		remote, verifyErr := m.remoteForVerify(row)
		if verifyErr == nil && remoteMatchesCanonical(row, remote, requireHash) {
			m.completeRemoteVerification(row, remote, []string{StateQueued, StateFailed})
			return
		}
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
	releaseActiveSpool := markSpoolActive(row.SpoolPath)
	defer func() {
		releaseActiveSpool()
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
		HashInfo: utils.NewHashInfo(utils.SHA1, row.PayloadSHA1),
	}
	fsStream := &stream.FileStream{
		Obj:      obj,
		Reader:   f,
		Mimetype: row.MimeType,
	}
	err = fs.PutDirectly(m.ctx, row.Parent, fsStream, true)
	if err != nil {
		if errs.IsNotFoundError(err) {
			m.failAfter(row, err, 2*time.Second)
		} else {
			m.fail(row, err)
		}
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
	res = db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("id = ? AND generation = ? AND state = ?", row.ID, row.Generation, StateUploading).
		Updates(map[string]any{"state": StateVerifying, "retry_at": &now, "verify_count": 0})
	if res.Error != nil {
		// The provider PUT has already returned success. If MySQL briefly fails
		// here, do not strand the row forever in UPLOADING. A best-effort
		// recovery update resumes VERIFYING, preserving the multi-attempt remote
		// consistency window before any retransmit.
		next := time.Now().Add(2 * time.Second)
		_ = db.GetDb().Model(&model.WebDAVWritebackObject{}).
			Where("id = ? AND generation = ? AND state = ?", row.ID, row.Generation, StateUploading).
			Updates(map[string]any{
				"state":        StateVerifying,
				"retry_at":     &next,
				"verify_count": 0,
				"last_error":   fmt.Sprintf("provider upload succeeded but verification state persistence failed: %v", res.Error),
			}).Error
		log.Errorf("write-back failed to enter verifying state for %s: %v", row.Path, res.Error)
		return
	}
	if res.RowsAffected == 0 {
		return
	}
	m.processVerify(row)
}

func (m *workerManager) processMkdir(row *model.WebDAVWritebackObject) {
	if waiting, err := m.waitForCanonicalParent(row); err != nil {
		m.failAfter(row, err, 2*time.Second)
		return
	} else if waiting {
		return
	}
	res := db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("id = ? AND generation = ? AND is_dir = ? AND state IN ?", row.ID, row.Generation, true, []string{StateQueued, StateFailed}).
		Updates(map[string]any{"state": StateUploading, "retry_at": nil, "last_error": ""})
	if res.Error != nil || res.RowsAffected == 0 {
		return
	}

	err := fs.MakeDir(m.ctx, row.Path)
	if err != nil && m.remoteDirectoryExists(row) {
		err = nil
	}
	if err != nil {
		m.failAfter(row, err, 2*time.Second)
		return
	}

	now := time.Now()
	res = db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("id = ? AND generation = ? AND is_dir = ?", row.ID, row.Generation, true).
		Updates(map[string]any{
			"state":        StateCompleted,
			"completed_at": &now,
			"retry_at":     nil,
			"last_error":   "",
			"retry_count":  0,
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
}

func providerRequiresPayloadHash(p string) bool {
	storage, err := fs.GetStorage(p, &fs.GetStoragesArgs{})
	return err == nil && storage != nil && storage.Config().Name == "115 Open"
}

func remoteMatchesCanonical(row *model.WebDAVWritebackObject, remote model.Obj, requireHash bool) bool {
	if row == nil || remote == nil || remote.IsDir() || remote.GetSize() != row.Size {
		return false
	}
	expectedSHA1 := canonicalContentSHA1(row)
	if expectedSHA1 == "" {
		return true
	}
	remoteSHA1 := remote.GetHash().GetHash(utils.SHA1)
	if remoteSHA1 == "" {
		return !requireHash
	}
	return strings.EqualFold(remoteSHA1, expectedSHA1)
}

func (m *workerManager) remoteForVerify(row *model.WebDAVWritebackObject) (model.Obj, error) {
	requireHash := providerRequiresPayloadHash(row.Path)
	remote, getErr := fs.Get(m.ctx, row.Path, &fs.GetArgs{NoLog: true})
	if getErr == nil && remoteMatchesCanonical(row, remote, requireHash) {
		return remote, nil
	}

	// 115 Open can briefly return NotFound or zero/incomplete metadata from
	// single-object lookup immediately after upload while the parent listing is
	// already correct. Force-refresh the parent and match the exact name/size
	// before deciding that the upload failed.
	objs, listErr := fs.List(m.ctx, row.Parent, &fs.ListArgs{Refresh: true, NoLog: true})
	if listErr == nil {
		for _, obj := range objs {
			if obj.GetName() == row.Name && remoteMatchesCanonical(row, obj, requireHash) {
				return obj, nil
			}
		}
	}

	if getErr != nil {
		return nil, getErr
	}
	if listErr != nil && remote == nil {
		return nil, listErr
	}
	return remote, nil
}

type remoteVerificationEvidence struct {
	objectID   string
	sha1       string
	generation uint64
	verifiedAt time.Time
}

func captureRemoteVerification(row *model.WebDAVWritebackObject, remote model.Obj, now time.Time) remoteVerificationEvidence {
	evidence := remoteVerificationEvidence{verifiedAt: now}
	if row != nil {
		evidence.generation = row.Generation
	}
	if remote != nil {
		evidence.objectID = remote.GetID()
		evidence.sha1 = strings.ToLower(remote.GetHash().GetHash(utils.SHA1))
	}
	return evidence
}

func (m *workerManager) completeRemoteVerification(row *model.WebDAVWritebackObject, remote model.Obj, allowedStates []string) bool {
	now := time.Now()
	evidence := captureRemoteVerification(row, remote, now)
	res := db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("id = ? AND generation = ? AND state IN ?", row.ID, row.Generation, allowedStates).
		Updates(map[string]any{
			"state":              StateCompleted,
			"completed_at":       &now,
			"retry_at":           nil,
			"last_error":         "",
			"retry_count":        0,
			"verify_count":       0,
			"remote_object_id":   evidence.objectID,
			"remote_sha1":        evidence.sha1,
			"remote_generation":  evidence.generation,
			"remote_verified_at": &evidence.verifiedAt,
		})
	if res.Error != nil || res.RowsAffected == 0 {
		return false
	}
	if row.CleanupPath != "" && row.CleanupPath != row.Path {
		_ = fs.Remove(m.ctx, row.CleanupPath)
		_ = db.GetDb().Model(&model.WebDAVWritebackObject{}).
			Where("id = ? AND generation = ?", row.ID, row.Generation).
			Update("cleanup_path", "").Error
	}
	return true
}

func (m *workerManager) processVerify(row *model.WebDAVWritebackObject) {
	if row.IsDir {
		now := time.Now()
		_ = db.GetDb().Model(&model.WebDAVWritebackObject{}).
			Where("id = ? AND generation = ? AND state = ?", row.ID, row.Generation, StateVerifying).
			Updates(map[string]any{
				"state":        StateQueued,
				"retry_at":     &now,
				"verify_count": 0,
				"last_error":   "directory verification state repaired to queued",
			}).Error
		return
	}

	requireHash := providerRequiresPayloadHash(row.Path)
	remote, err := m.remoteForVerify(row)
	if err == nil && remoteMatchesCanonical(row, remote, requireHash) {
		m.completeRemoteVerification(row, remote, []string{StateVerifying})
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
		remoteSHA1 := remote.GetHash().GetHash(utils.SHA1)
		if row.PayloadSHA1 != "" && remoteSHA1 != "" && !strings.EqualFold(remoteSHA1, row.PayloadSHA1) {
			msg = fmt.Sprintf("remote sha1 %s does not match canonical sha1 %s", remoteSHA1, row.PayloadSHA1)
		} else {
			msg = fmt.Sprintf("remote size %d does not match canonical size %d", remote.GetSize(), row.Size)
		}
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

func deleteParentMissing(err error) bool {
	return err != nil && errs.IsObjectNotFound(err)
}

func remoteListContainsName(objs []model.Obj, name string) bool {
	for _, obj := range objs {
		if obj.GetName() == name {
			return true
		}
	}
	return false
}

func (m *workerManager) remoteDeleteAbsent(row *model.WebDAVWritebackObject) (bool, error) {
	remote, getErr := fs.Get(m.ctx, row.Path, &fs.GetArgs{NoLog: true})
	if getErr == nil && remote != nil {
		return false, nil
	}
	if getErr != nil && !errs.IsObjectNotFound(getErr) {
		return false, getErr
	}

	objs, listErr := fs.List(m.ctx, row.Parent, &fs.ListArgs{Refresh: true, NoLog: true})
	if listErr != nil {
		if deleteParentMissing(listErr) {
			// A missing parent proves that this child is absent. Directory-tree
			// DELETE/MOVE commonly reaches this state after the parent tombstone
			// removes the provider subtree before descendant tombstones are checked.
			return true, nil
		}
		return false, listErr
	}
	return !remoteListContainsName(objs, row.Name), nil
}

func collectConfirmedTombstoneSubtree(root *model.WebDAVWritebackObject, rows []model.WebDAVWritebackObject) (ids []uint, spoolPaths []string, valid bool) {
	if root == nil {
		return nil, nil, false
	}
	seenSpool := make(map[string]struct{})
	rootMatched := false
	for i := range rows {
		row := &rows[i]
		if row.State != StateDeleted || !isPathOrDescendant(row.Path, root.Path) {
			continue
		}
		if row.ID == root.ID {
			if row.Generation != root.Generation {
				return nil, nil, false
			}
			rootMatched = true
		}
		ids = append(ids, row.ID)
		if row.SpoolPath != "" {
			if _, ok := seenSpool[row.SpoolPath]; !ok {
				seenSpool[row.SpoolPath] = struct{}{}
				spoolPaths = append(spoolPaths, row.SpoolPath)
			}
		}
	}
	if !rootMatched {
		return nil, nil, false
	}
	return ids, spoolPaths, true
}

func deleteConfirmedTombstoneSubtree(root *model.WebDAVWritebackObject) (bool, error) {
	var spoolPaths []string
	deleted := false
	err := db.GetDb().Transaction(func(tx *gorm.DB) error {
		var candidates []model.WebDAVWritebackObject
		query := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("state = ? AND (path = ? OR path LIKE ? ESCAPE '~')", StateDeleted, root.Path, descendantLikePattern(root.Path)).
			Order("id asc")
		if err := query.Find(&candidates).Error; err != nil {
			return err
		}
		ids, spools, valid := collectConfirmedTombstoneSubtree(root, candidates)
		if !valid || len(ids) == 0 {
			return nil
		}
		spoolPaths = spools
		res := tx.Where("id IN ? AND state = ?", ids, StateDeleted).
			Delete(&model.WebDAVWritebackObject{})
		if res.Error != nil {
			return res.Error
		}
		deleted = res.RowsAffected > 0
		return nil
	})
	if err != nil || !deleted {
		return deleted, err
	}
	for _, spoolPath := range spoolPaths {
		if !spoolIsActive(spoolPath) {
			removeSpoolIfUnreferenced(spoolPath)
		}
	}
	return true, nil
}

func (m *workerManager) processDelete(row *model.WebDAVWritebackObject) {
	// A tombstone can be superseded by a fast Cloud Sync recreate of the same
	// path. Re-check the generation before touching the provider so a queued old
	// delete cannot blindly remove a newer canonical generation.
	var current model.WebDAVWritebackObject
	if err := db.GetDb().First(&current, row.ID).Error; err != nil {
		return
	}
	if current.Generation != row.Generation || current.State != StateDeleted {
		return
	}

	err := fs.Remove(m.ctx, row.Path)
	if err != nil && !errs.IsObjectNotFound(err) {
		m.failDeleted(row, err)
		return
	}

	// The generation may have changed while the provider delete was in flight.
	// If a newer generation already reached COMPLETED, the stale delete may
	// have removed that object after its verification. Re-queue from the local
	// spool so the newest Cloud Sync payload deterministically wins.
	if err := db.GetDb().First(&current, row.ID).Error; err != nil {
		return
	}
	if current.Generation != row.Generation || current.State != StateDeleted {
		if current.State == StateCompleted && current.SpoolPath != "" {
			now := time.Now()
			res := db.GetDb().Model(&model.WebDAVWritebackObject{}).
				Where("id = ? AND generation = ? AND state = ?", current.ID, current.Generation, StateCompleted).
				Updates(map[string]any{
					"state":        StateQueued,
					"retry_at":     &now,
					"completed_at": nil,
					"last_error":   "re-queued because an older delete overlapped this generation",
				})
			if res.Error == nil && res.RowsAffected > 0 {
				wake()
			}
		}
		return
	}

	absent, verifyErr := m.remoteDeleteAbsent(row)
	if verifyErr != nil {
		m.failDeleted(row, verifyErr)
		return
	}
	if !absent {
		m.failDeleted(row, errors.New("remote object is still visible after delete"))
		return
	}

	// A single NotFound/list miss is not enough for an eventually-consistent
	// provider. Keep the tombstone through two force-refreshed absence checks.
	const deleteConfirmations = 2
	nextCount := row.VerifyCount + 1
	if nextCount < deleteConfirmations {
		interval := time.Duration(max(1, conf.Conf.WebDAVWriteback.VerifyIntervalSeconds)) * time.Second
		next := time.Now().Add(interval)
		_ = db.GetDb().Model(&model.WebDAVWritebackObject{}).
			Where("id = ? AND generation = ? AND state = ?", row.ID, row.Generation, StateDeleted).
			Updates(map[string]any{
				"retry_at":     &next,
				"verify_count": nextCount,
				"last_error":   "first remote delete absence confirmation; waiting for second",
			}).Error
		return
	}

	// Re-check one final time after the verification request. A PUT may have
	// recreated the path while we were waiting on the provider listing.
	if err := db.GetDb().First(&current, row.ID).Error; err != nil {
		return
	}
	if current.Generation != row.Generation || current.State != StateDeleted {
		return
	}

	if row.CleanupPath != "" && row.CleanupPath != row.Path {
		_ = fs.Remove(m.ctx, row.CleanupPath)
	}
	deleted, deleteErr := deleteConfirmedTombstoneSubtree(row)
	if deleteErr != nil {
		m.failDeleted(row, deleteErr)
		return
	}
	if !deleted {
		return
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

func (m *workerManager) failAfter(row *model.WebDAVWritebackObject, err error, delay time.Duration) {
	next := time.Now().Add(delay)
	_ = db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("id = ? AND generation = ?", row.ID, row.Generation).
		Updates(map[string]any{
			"state":       StateFailed,
			"retry_at":    &next,
			"retry_count": row.RetryCount + 1,
			"last_error":  err.Error(),
		}).Error
}

func (m *workerManager) fail(row *model.WebDAVWritebackObject, err error) {
	m.failAfter(row, err, retryDelay(row.RetryCount))
}

func (m *workerManager) failDeleted(row *model.WebDAVWritebackObject, err error) {
	next := time.Now().Add(retryDelay(row.RetryCount))
	_ = db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("id = ? AND generation = ? AND state = ?", row.ID, row.Generation, StateDeleted).
		Updates(map[string]any{
			"retry_at":     &next,
			"retry_count":  row.RetryCount + 1,
			"verify_count": 0,
			"last_error":   err.Error(),
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
		Select("id", "generation", "spool_path", "completed_at").
		Where("state = ? AND spool_path <> '' AND completed_at IS NOT NULL AND completed_at <= ?", StateCompleted, cutoff).
		Limit(100).
		Find(&rows).Error; err != nil {
		return
	}
	for i := range rows {
		row := &rows[i]
		if spoolIsActive(row.SpoolPath) {
			continue
		}
		res := db.GetDb().Model(&model.WebDAVWritebackObject{}).
			Where("id = ? AND generation = ? AND state = ? AND spool_path = ? AND completed_at IS NOT NULL AND completed_at <= ?",
				row.ID, row.Generation, StateCompleted, row.SpoolPath, cutoff).
			Update("spool_path", "")
		if res.Error != nil || res.RowsAffected == 0 {
			continue
		}
		// COPY can make several canonical rows reference the same immutable
		// spool payload. Only unlink the physical file after the last database
		// reference has been released.
		removeSpoolIfUnreferenced(row.SpoolPath)
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
