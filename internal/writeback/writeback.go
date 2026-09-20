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
	"sort"
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
	StateLockNull  = "lock_null"
)

var ErrCanonicalChanged = errors.New("canonical WebDAV state changed while staging provider delete")

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

func applyDuplicatePutMetadata(row *model.WebDAVWritebackObject, modTime, createTime time.Time, mime string, modTimeProvided, createTimeProvided bool) {
	if row == nil {
		return
	}
	if modTimeProvided {
		row.ModTime = modTime
	}
	if createTimeProvided {
		row.CreateTime = createTime
	}
	if mime != "" {
		row.MimeType = mime
	}
}

var (
	ErrDestinationExists         = errors.New("write-back destination already exists")
	ErrProviderOperationConflict = errors.New("conflicting provider COPY/MOVE intent is still unresolved")
	ErrProviderOperationStale    = errors.New("provider COPY/MOVE source generation was superseded")
	ErrSpoolCapacity             = errors.New("write-back spool capacity unavailable")
)

type SpoolCapacityError struct {
	Free     uint64
	Required uint64
}

func (e *SpoolCapacityError) Error() string {
	if e == nil {
		return ErrSpoolCapacity.Error()
	}
	return fmt.Sprintf("%s: free=%d required=%d", ErrSpoolCapacity, e.Free, e.Required)
}

func (e *SpoolCapacityError) Unwrap() error {
	return ErrSpoolCapacity
}

func SpoolAdmissionRetrySeconds() int {
	seconds := 5
	if conf.Conf != nil && conf.Conf.WebDAVWriteback.AdmissionRetrySeconds > 0 {
		seconds = conf.Conf.WebDAVWriteback.AdmissionRetrySeconds
	}
	return seconds
}

const (
	ProviderOperationCopy = "COPY"
	ProviderOperationMove = "MOVE"

	ProviderOperationPrepared = "prepared"
	ProviderOperationStarted  = "started"
	ProviderOperationApplied  = "applied"
	ProviderOperationFailed   = "failed"
)

type ProviderOperationRecovery uint8

const (
	ProviderOperationNotApplied ProviderOperationRecovery = iota
	ProviderOperationRecovered
	ProviderOperationInconclusive
)

type providerOperationRemoteState uint8

const (
	providerOperationRemoteAbsent providerOperationRemoteState = iota
	providerOperationRemoteMatch
	providerOperationRemoteMismatch
	providerOperationRemoteInconclusive
)

func providerOperationKey(method, src, dst string, depth int) string {
	method = strings.ToUpper(method)
	src = utils.FixAndCleanPath(src)
	dst = utils.FixAndCleanPath(dst)
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%s\x00%d", method, src, dst, depth)))
	return hex.EncodeToString(sum[:])
}

func ProviderOperationSourceObject(op *model.WebDAVProviderOperation) model.Obj {
	if op == nil {
		return nil
	}
	obj := &model.Object{
		ID:       op.SourceObjectID,
		Path:     op.SourcePath,
		Name:     path.Base(op.SourcePath),
		Size:     op.SourceSize,
		Modified: op.SourceModTime,
		Ctime:    op.SourceCreateTime,
		IsFolder: op.SourceIsDir,
	}
	if op.SourceSHA1 != "" {
		obj.HashInfo = utils.NewHashInfo(utils.SHA1, op.SourceSHA1)
	}
	return obj
}

func ProviderOperationSourceMatches(op *model.WebDAVProviderOperation, source model.Obj) bool {
	if op == nil || source == nil || op.SourceIsDir != source.IsDir() {
		return false
	}
	if op.SourceGeneration > 0 {
		if canonical, ok := source.(*CanonicalObject); ok && op.SourceETag != "" {
			return canonical.etag == op.SourceETag
		}
	}
	if op.SourceIsDir {
		if op.SourceObjectID != "" && source.GetID() != "" {
			return op.SourceObjectID == source.GetID()
		}
		if op.SourceModTime.IsZero() || source.ModTime().IsZero() {
			return false
		}
		return op.SourceModTime.Unix() == source.ModTime().Unix()
	}
	if op.SourceSize != source.GetSize() {
		return false
	}
	sourceSHA1 := source.GetHash().GetHash(utils.SHA1)
	if op.SourceSHA1 != "" || sourceSHA1 != "" {
		return op.SourceSHA1 != "" && sourceSHA1 != "" && strings.EqualFold(op.SourceSHA1, sourceSHA1)
	}
	if op.SourceObjectID != "" && source.GetID() != "" {
		return op.SourceObjectID == source.GetID()
	}
	if op.SourceModTime.IsZero() || source.ModTime().IsZero() {
		return false
	}
	return op.SourceModTime.Unix() == source.ModTime().Unix()
}

func pathsOverlap(a, b string) bool {
	a = utils.FixAndCleanPath(a)
	b = utils.FixAndCleanPath(b)
	return isPathOrDescendant(a, b) || isPathOrDescendant(b, a)
}

func providerOperationsConflict(existing *model.WebDAVProviderOperation, method, src, dst string, depth int) bool {
	if existing == nil {
		return false
	}
	method = strings.ToUpper(method)
	src = utils.FixAndCleanPath(src)
	dst = utils.FixAndCleanPath(dst)
	if existing.OperationKey == providerOperationKey(method, src, dst, depth) {
		return false
	}

	if pathsOverlap(existing.DestinationPath, dst) ||
		pathsOverlap(existing.DestinationPath, src) ||
		pathsOverlap(existing.SourcePath, dst) {
		return true
	}
	if strings.EqualFold(existing.Method, ProviderOperationMove) || method == ProviderOperationMove {
		return pathsOverlap(existing.SourcePath, src)
	}
	return false
}

func providerOperationTouchesPath(op *model.WebDAVProviderOperation, p string) bool {
	if op == nil {
		return false
	}
	return pathsOverlap(op.SourcePath, p) || pathsOverlap(op.DestinationPath, p)
}

func providerOperationProtectsCanonicalPath(ops []model.WebDAVProviderOperation, p string, now time.Time) bool {
	for i := range ops {
		if providerOperationPreparedExpired(&ops[i], now) {
			continue
		}
		if providerOperationTouchesPath(&ops[i], p) {
			return true
		}
	}
	return false
}

func activeProviderOperations() ([]model.WebDAVProviderOperation, error) {
	var ops []model.WebDAVProviderOperation
	// Protection checks only need routing/fence fields. Avoid pulling the
	// operation's large recovery snapshots and TEXT errors on every PROPFIND.
	if err := db.GetDb().
		Select("method", "source_path", "destination_path", "depth", "state", "created_at", "updated_at").
		Order("updated_at asc").
		Find(&ops).Error; err != nil {
		return nil, err
	}
	return ops, nil
}

func ProviderOperationConflict(method, src, dst string, depth int) (*model.WebDAVProviderOperation, error) {
	if !Enabled() {
		return nil, nil
	}
	var ops []model.WebDAVProviderOperation
	if err := db.GetDb().Order("updated_at asc").Find(&ops).Error; err != nil {
		return nil, err
	}
	now := time.Now()
	for i := range ops {
		if providerOperationPreparedExpired(&ops[i], now) {
			continue
		}
		if providerOperationsConflict(&ops[i], method, src, dst, depth) {
			return &ops[i], nil
		}
	}
	return nil, nil
}

func ProviderOperationPathConflict(p string) (*model.WebDAVProviderOperation, error) {
	if !Enabled() {
		return nil, nil
	}
	p = utils.FixAndCleanPath(p)
	var ops []model.WebDAVProviderOperation
	if err := db.GetDb().Order("updated_at asc").Find(&ops).Error; err != nil {
		return nil, err
	}
	now := time.Now()
	for i := range ops {
		if providerOperationPreparedExpired(&ops[i], now) {
			continue
		}
		if providerOperationTouchesPath(&ops[i], p) {
			return &ops[i], nil
		}
	}
	return nil, nil
}

func providerOperationTreeDepth(method string, depth int) int {
	if strings.EqualFold(method, ProviderOperationMove) {
		return -1
	}
	return depth
}

// ProviderOperationCopyUsesNative mirrors the provider COPY branch used by
// WebDAV. Keeping the predicate here prevents recovery evidence from being
// captured from a different source view than the mutation that will run.
func ProviderOperationCopyUsesNative(src, dst string, depth int) bool {
	return depth < 0 &&
		path.Dir(src) != path.Dir(dst) &&
		path.Base(src) == path.Base(dst)
}

func providerOperationTreeObjects(ctx context.Context, current string, overlay bool) ([]model.Obj, error) {
	objs, err := fs.List(ctx, current, &fs.ListArgs{Refresh: true, NoLog: true})
	if !overlay {
		return objs, err
	}

	remoteReliable := err == nil
	overlaid, hasWriteback, overlayErr := OverlayList(ctx, current, objs, remoteReliable)
	if overlayErr != nil {
		return nil, overlayErr
	}
	canonicalParent := false
	if canonical, found, deleted, canonicalErr := Canonical(current); canonicalErr != nil {
		return nil, canonicalErr
	} else if found && !deleted && canonical != nil && canonical.IsDir() {
		canonicalParent = true
	}
	if remoteReliable || hasWriteback || canonicalParent {
		return overlaid, nil
	}
	return nil, err
}

func providerDirectoryTreeFingerprint(ctx context.Context, root string, depth int, overlay bool) (string, int, error) {
	root = utils.FixAndCleanPath(root)
	hasher := sha256.New()
	entries := 0

	var walk func(string, string, int) error
	walk = func(current, relative string, remaining int) error {
		if remaining == 0 {
			return nil
		}
		objs, err := providerOperationTreeObjects(ctx, current, overlay)
		if err != nil {
			return err
		}
		sort.Slice(objs, func(i, j int) bool {
			return objs[i].GetName() < objs[j].GetName()
		})
		for _, obj := range objs {
			if obj == nil {
				continue
			}
			fullPath := path.Join(current, obj.GetName())
			rel := path.Join(relative, obj.GetName())
			if obj.IsDir() {
				_, _ = fmt.Fprintf(hasher, "D\x00%s\n", rel)
				entries++
				next := remaining
				if remaining > 0 {
					next--
				}
				if err := walk(fullPath, rel, next); err != nil {
					return err
				}
				continue
			}
			sha1sum := strings.ToLower(obj.GetHash().GetHash(utils.SHA1))
			if providerRequiresPayloadHash(fullPath) && sha1sum == "" {
				return fmt.Errorf("provider directory fingerprint missing SHA1 for %s", fullPath)
			}
			_, _ = fmt.Fprintf(hasher, "F\x00%s\x00%d\x00%s\n", rel, obj.GetSize(), sha1sum)
			entries++
		}
		return nil
	}

	if err := walk(root, "", depth); err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(hasher.Sum(nil)), entries, nil
}

func providerDirectoryDestinationMatches(ctx context.Context, op *model.WebDAVProviderOperation, remote model.Obj) (providerOperationRemoteState, error) {
	if op == nil || !op.SourceIsDir || remote == nil || !remote.IsDir() {
		return providerOperationRemoteMismatch, nil
	}
	if strings.EqualFold(op.Method, ProviderOperationMove) &&
		providerRequiresPayloadHash(op.DestinationPath) &&
		op.SourceObjectID != "" && remote.GetID() != "" &&
		op.SourceObjectID != remote.GetID() {
		return providerOperationRemoteMismatch, nil
	}
	if op.SourceTreeSHA256 == "" {
		return providerOperationRemoteMatch, nil
	}
	fingerprint, entries, err := providerDirectoryTreeFingerprint(ctx, op.DestinationPath, providerOperationTreeDepth(op.Method, op.Depth), false)
	if err != nil {
		return providerOperationRemoteInconclusive, err
	}
	if entries != op.SourceTreeEntries || !strings.EqualFold(fingerprint, op.SourceTreeSHA256) {
		return providerOperationRemoteMismatch, nil
	}
	return providerOperationRemoteMatch, nil
}

func GetProviderOperation(method, src, dst string, depth int) (*model.WebDAVProviderOperation, error) {
	if !Enabled() {
		return nil, nil
	}
	var op model.WebDAVProviderOperation
	err := db.GetDb().Where("operation_key = ?", providerOperationKey(method, src, dst, depth)).First(&op).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &op, nil
}

func PrepareProviderOperation(ctx context.Context, method, src, dst string, depth int, source model.Obj, overwrite, destinationExisted bool) (*model.WebDAVProviderOperation, error) {
	if !Enabled() || source == nil {
		return nil, nil
	}
	method = strings.ToUpper(method)
	src = utils.FixAndCleanPath(src)
	dst = utils.FixAndCleanPath(dst)
	now := time.Now()
	op := model.WebDAVProviderOperation{
		OperationKey:       providerOperationKey(method, src, dst, depth),
		Method:             method,
		SourceKey:          pathKey(src),
		DestinationKey:     pathKey(dst),
		SourcePath:         src,
		DestinationPath:    dst,
		SourceObjectID:     source.GetID(),
		SourceIsDir:        source.IsDir(),
		SourceSize:         source.GetSize(),
		SourceSHA1:         strings.ToLower(source.GetHash().GetHash(utils.SHA1)),
		SourceModTime:      source.ModTime(),
		SourceCreateTime:   source.CreateTime(),
		Overwrite:          overwrite,
		Depth:              depth,
		DestinationExisted: destinationExisted,
		State:              ProviderOperationPrepared,
		RecoveryCount:      0,
		LastRecovery:       "",
		LastError:          "",
		LastCheckedAt:      nil,
		AppliedAt:          nil,
		UpdatedAt:          now,
	}
	if canonical, err := getByPath(src); err != nil {
		return nil, err
	} else if canonical != nil && canonical.State != StateDeleted {
		op.SourceGeneration = canonical.Generation
		op.SourceETag = canonical.ETag
		if op.SourceSHA1 == "" {
			op.SourceSHA1 = strings.ToLower(canonicalContentSHA1(canonical))
		}
	}

	if destinationExisted {
		if remote, present, observeErr := providerOperationDestinationObject(ctx, dst); observeErr != nil {
			return nil, observeErr
		} else if present {
			op.DestinationObjectID = remote.GetID()
		}
	}
	if canonical, err := getByPath(dst); err != nil {
		return nil, err
	} else if canonical != nil {
		op.DestinationGeneration = canonical.Generation
		op.DestinationETag = canonical.ETag
	}

	if op.SourceIsDir {
		op.SourceTreeOverlay = strings.EqualFold(method, ProviderOperationCopy) &&
			!ProviderOperationCopyUsesNative(src, dst, depth)
		fingerprint, entries, err := providerDirectoryTreeFingerprint(
			ctx,
			src,
			providerOperationTreeDepth(method, depth),
			op.SourceTreeOverlay,
		)
		if err != nil {
			return nil, err
		}
		op.SourceTreeSHA256 = fingerprint
		op.SourceTreeEntries = entries
	}

	err := db.GetDb().Transaction(func(tx *gorm.DB) error {
		var existing []model.WebDAVProviderOperation
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Order("updated_at asc").Find(&existing).Error; err != nil {
			return err
		}
		var same *model.WebDAVProviderOperation
		for i := range existing {
			if providerOperationPreparedExpired(&existing[i], now) {
				if err := tx.Delete(&model.WebDAVProviderOperation{}, existing[i].ID).Error; err != nil {
					return err
				}
				continue
			}
			if existing[i].OperationKey == op.OperationKey {
				same = &existing[i]
				continue
			}
			if providerOperationsConflict(&existing[i], method, src, dst, depth) {
				return ErrProviderOperationConflict
			}
		}
		if same != nil {
			op.ID = same.ID
			op.CreatedAt = same.CreatedAt
			return tx.Save(&op).Error
		}
		return tx.Create(&op).Error
	})
	if err != nil {
		return nil, err
	}
	return GetProviderOperation(method, src, dst, depth)
}

func MarkProviderOperationStarted(id uint) error {
	if id == 0 {
		return errors.New("provider operation intent is missing")
	}
	res := db.GetDb().Model(&model.WebDAVProviderOperation{}).
		Where("id = ? AND state = ?", id, ProviderOperationPrepared).
		Updates(map[string]any{
			"state":                            ProviderOperationStarted,
			"applied_at":                       nil,
			"recovery_count":                   0,
			"last_recovery":                    "",
			"last_error":                       "",
			"last_checked_at":                  nil,
			"failure_destination_observed":     false,
			"failure_destination_object_id":    "",
			"failure_destination_ready":        false,
			"failure_destination_is_dir":       false,
			"failure_destination_size":         0,
			"failure_destination_sha1":         "",
			"failure_destination_tree_sha256":  "",
			"failure_destination_tree_entries": 0,
		})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return errors.New("provider operation intent was not in prepared state")
	}
	return nil
}

func MarkProviderOperationApplied(id uint) error {
	if id == 0 {
		return errors.New("provider operation intent is missing")
	}
	now := time.Now()
	res := db.GetDb().Model(&model.WebDAVProviderOperation{}).
		Where("id = ? AND state IN ?", id, []string{ProviderOperationStarted, ProviderOperationApplied}).
		Updates(map[string]any{
			"state":                            ProviderOperationApplied,
			"applied_at":                       &now,
			"recovery_count":                   0,
			"last_recovery":                    "",
			"last_error":                       "",
			"last_checked_at":                  nil,
			"failure_destination_observed":     false,
			"failure_destination_object_id":    "",
			"failure_destination_ready":        false,
			"failure_destination_is_dir":       false,
			"failure_destination_size":         0,
			"failure_destination_sha1":         "",
			"failure_destination_tree_sha256":  "",
			"failure_destination_tree_entries": 0,
		})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return errors.New("provider operation intent was not started")
	}
	return nil
}

func providerOperationDestinationObject(ctx context.Context, p string) (model.Obj, bool, error) {
	// A refreshed parent listing is the recovery authority. 115 single-object
	// lookups can expose the deleted overwrite target or incomplete metadata
	// after a COPY failure, which is unsafe evidence for automated cleanup.
	objs, listErr := fs.List(ctx, path.Dir(p), &fs.ListArgs{Refresh: true, NoLog: true})
	if listErr != nil {
		if errs.IsObjectNotFound(listErr) {
			return nil, false, nil
		}
		return nil, false, listErr
	}
	remote := exactRemoteByName(objs, path.Base(p))
	return remote, remote != nil, nil
}

func MarkProviderOperationFailed(ctx context.Context, op *model.WebDAVProviderOperation, failure error) error {
	if op == nil || op.ID == 0 {
		return errors.New("provider operation intent is missing")
	}
	now := time.Now()
	lastError := "provider COPY failed after mutation started"
	if failure != nil {
		lastError = failure.Error()
	}

	failureObserved := false
	failureObjectID := ""
	failureReady := false
	failureIsDir := false
	var failureSize int64
	failureSHA1 := ""
	failureTreeSHA256 := ""
	failureTreeEntries := 0

	if remote, present, observeErr := providerOperationDestinationObject(ctx, op.DestinationPath); observeErr != nil {
		lastError += "; destination evidence unavailable: " + observeErr.Error()
	} else if present {
		failureObserved = true
		failureObjectID = remote.GetID()
		failureIsDir = remote.IsDir()
		failureSize = remote.GetSize()
		if remote.IsDir() {
			fingerprint, entries, fingerprintErr := providerDirectoryTreeFingerprint(
				ctx,
				op.DestinationPath,
				providerOperationTreeDepth(op.Method, op.Depth),
				false,
			)
			if fingerprintErr != nil {
				lastError += "; destination tree evidence unavailable: " + fingerprintErr.Error()
			} else {
				failureReady = true
				failureTreeSHA256 = fingerprint
				failureTreeEntries = entries
			}
		} else {
			failureSHA1 = strings.ToLower(remote.GetHash().GetHash(utils.SHA1))
			failureReady = !providerRequiresPayloadHash(op.DestinationPath) || failureSHA1 != ""
		}
	}

	res := db.GetDb().Model(&model.WebDAVProviderOperation{}).
		Where("id = ? AND state IN ?", op.ID, []string{ProviderOperationStarted, ProviderOperationFailed}).
		Updates(map[string]any{
			"state":                            ProviderOperationFailed,
			"last_error":                       lastError,
			"last_checked_at":                  &now,
			"applied_at":                       nil,
			"failure_destination_observed":     failureObserved,
			"failure_destination_object_id":    failureObjectID,
			"failure_destination_ready":        failureReady,
			"failure_destination_is_dir":       failureIsDir,
			"failure_destination_size":         failureSize,
			"failure_destination_sha1":         failureSHA1,
			"failure_destination_tree_sha256":  failureTreeSHA256,
			"failure_destination_tree_entries": failureTreeEntries,
		})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return errors.New("provider operation intent was not started")
	}
	op.State = ProviderOperationFailed
	op.LastError = lastError
	op.LastCheckedAt = &now
	op.AppliedAt = nil
	op.FailureDestinationObserved = failureObserved
	op.FailureDestinationObjectID = failureObjectID
	op.FailureDestinationReady = failureReady
	op.FailureDestinationIsDir = failureIsDir
	op.FailureDestinationSize = failureSize
	op.FailureDestinationSHA1 = failureSHA1
	op.FailureDestinationTreeSHA256 = failureTreeSHA256
	op.FailureDestinationTreeEntries = failureTreeEntries
	return nil
}

func FinishProviderOperation(id uint) error {
	if id == 0 {
		return nil
	}
	return db.GetDb().Delete(&model.WebDAVProviderOperation{}, id).Error
}

func DiscardProviderOperation(method, src, dst string, depth int) error {
	if !Enabled() {
		return nil
	}
	return db.GetDb().Where("operation_key = ?", providerOperationKey(method, src, dst, depth)).
		Delete(&model.WebDAVProviderOperation{}).Error
}

func providerOperationExpectedRow(op *model.WebDAVProviderOperation) *model.WebDAVWritebackObject {
	if op == nil {
		return nil
	}
	return &model.WebDAVWritebackObject{
		IsDir:       op.SourceIsDir,
		Size:        op.SourceSize,
		PayloadSHA1: op.SourceSHA1,
	}
}

func providerOperationPathState(ctx context.Context, p string, op *model.WebDAVProviderOperation) (providerOperationRemoteState, model.Obj, error) {
	expected := providerOperationExpectedRow(op)
	requireHash := providerRequiresPayloadHash(p)
	hashEvidenceMissing := requireHash && op != nil && !op.SourceIsDir && op.SourceSHA1 == ""
	remote, getErr := fs.Get(ctx, p, &fs.GetArgs{NoLog: true})
	if getErr == nil && remote != nil && !hashEvidenceMissing {
		if op != nil && op.SourceIsDir {
			state, err := providerDirectoryDestinationMatches(ctx, op, remote)
			if state == providerOperationRemoteMatch || err != nil {
				return state, remote, err
			}
			// Confirm mismatch/incomplete evidence through the refreshed parent view.
		} else {
			switch compareRemoteContent(expected, remote, requireHash) {
			case remoteContentMatch:
				return providerOperationRemoteMatch, remote, nil
			case remoteContentInconclusive:
				// Confirm through the refreshed parent listing below.
			case remoteContentMismatch:
				// Confirm through the refreshed parent listing below.
			}
		}
	} else if getErr != nil && !errs.IsObjectNotFound(getErr) {
		return providerOperationRemoteInconclusive, nil, getErr
	}

	objs, listErr := fs.List(ctx, path.Dir(p), &fs.ListArgs{Refresh: true, NoLog: true})
	if listErr != nil {
		if errs.IsObjectNotFound(listErr) {
			return providerOperationRemoteAbsent, nil, nil
		}
		return providerOperationRemoteInconclusive, nil, listErr
	}
	remote = exactRemoteByName(objs, path.Base(p))
	if remote == nil {
		return providerOperationRemoteAbsent, nil, nil
	}
	if op != nil && op.SourceIsDir {
		state, err := providerDirectoryDestinationMatches(ctx, op, remote)
		return state, remote, err
	}
	if hashEvidenceMissing {
		return providerOperationRemoteInconclusive, remote, nil
	}
	switch compareRemoteContent(expected, remote, requireHash) {
	case remoteContentMatch:
		return providerOperationRemoteMatch, remote, nil
	case remoteContentMismatch:
		return providerOperationRemoteMismatch, remote, nil
	default:
		return providerOperationRemoteInconclusive, remote, nil
	}
}

func providerOperationSourcePathState(ctx context.Context, op *model.WebDAVProviderOperation) (providerOperationRemoteState, model.Obj, error) {
	if op == nil {
		return providerOperationRemoteInconclusive, nil, nil
	}
	p := op.SourcePath
	remote, getErr := fs.Get(ctx, p, &fs.GetArgs{NoLog: true})
	if getErr == nil && remote != nil {
		if ProviderOperationSourceMatches(op, remote) {
			return providerOperationRemoteMatch, remote, nil
		}
		// Confirm a mismatch with the refreshed parent view.
	} else if getErr != nil && !errs.IsObjectNotFound(getErr) {
		return providerOperationRemoteInconclusive, nil, getErr
	}

	objs, listErr := fs.List(ctx, path.Dir(p), &fs.ListArgs{Refresh: true, NoLog: true})
	if listErr != nil {
		if errs.IsObjectNotFound(listErr) {
			return providerOperationRemoteAbsent, nil, nil
		}
		return providerOperationRemoteInconclusive, nil, listErr
	}
	remote = exactRemoteByName(objs, path.Base(p))
	if remote == nil {
		return providerOperationRemoteAbsent, nil, nil
	}
	if ProviderOperationSourceMatches(op, remote) {
		return providerOperationRemoteMatch, remote, nil
	}
	if op.SourceIsDir && (op.SourceObjectID == "" || remote.GetID() == "") {
		return providerOperationRemoteInconclusive, remote, nil
	}
	if !op.SourceIsDir && op.SourceSHA1 == "" && op.SourceObjectID == "" {
		return providerOperationRemoteInconclusive, remote, nil
	}
	return providerOperationRemoteMismatch, remote, nil
}

func ProviderOperationSourceRecreated(ctx context.Context, op *model.WebDAVProviderOperation) (bool, error) {
	state, _, err := providerOperationSourcePathState(ctx, op)
	return state == providerOperationRemoteMismatch, err
}

func providerOperationRecoveryDecision(method, state string, dstState, srcState providerOperationRemoteState, sourceIsDir bool) ProviderOperationRecovery {
	method = strings.ToUpper(method)
	if state == ProviderOperationPrepared {
		return ProviderOperationNotApplied
	}
	if method == ProviderOperationCopy {
		if state == ProviderOperationApplied {
			if dstState == providerOperationRemoteMatch {
				return ProviderOperationRecovered
			}
			return ProviderOperationInconclusive
		}
		if state == ProviderOperationFailed {
			switch dstState {
			case providerOperationRemoteMatch:
				return ProviderOperationRecovered
			case providerOperationRemoteAbsent, providerOperationRemoteMismatch:
				return ProviderOperationNotApplied
			default:
				return ProviderOperationInconclusive
			}
		}
		if sourceIsDir {
			if dstState == providerOperationRemoteAbsent {
				return ProviderOperationNotApplied
			}
			return ProviderOperationInconclusive
		}
		switch dstState {
		case providerOperationRemoteMatch:
			return ProviderOperationRecovered
		case providerOperationRemoteAbsent, providerOperationRemoteMismatch:
			return ProviderOperationNotApplied
		default:
			return ProviderOperationInconclusive
		}
	}

	if method != ProviderOperationMove {
		return ProviderOperationInconclusive
	}
	if state == ProviderOperationApplied {
		if dstState == providerOperationRemoteMatch {
			return ProviderOperationRecovered
		}
		return ProviderOperationInconclusive
	}
	switch srcState {
	case providerOperationRemoteMismatch:
		// A different object can legitimately reappear at the source after the
		// old MOVE succeeded. A matching destination proves the old payload was
		// moved; otherwise the old intent is safe to retry only after the normal
		// not-applied confirmation fence.
		if dstState == providerOperationRemoteMatch {
			return ProviderOperationRecovered
		}
		if dstState == providerOperationRemoteAbsent || dstState == providerOperationRemoteMismatch {
			return ProviderOperationNotApplied
		}
		return ProviderOperationInconclusive
	case providerOperationRemoteMatch:
		if dstState == providerOperationRemoteAbsent || dstState == providerOperationRemoteMismatch {
			return ProviderOperationNotApplied
		}
		return ProviderOperationInconclusive
	case providerOperationRemoteAbsent:
		if dstState == providerOperationRemoteMatch {
			return ProviderOperationRecovered
		}
		return ProviderOperationInconclusive
	default:
		return ProviderOperationInconclusive
	}
}

func providerOperationRecoveryDecisionWithEvidence(op *model.WebDAVProviderOperation, dstState, srcState providerOperationRemoteState) ProviderOperationRecovery {
	if op == nil {
		return ProviderOperationNotApplied
	}
	if strings.EqualFold(op.Method, ProviderOperationCopy) &&
		op.State == ProviderOperationStarted &&
		op.SourceIsDir &&
		op.SourceTreeSHA256 != "" &&
		dstState == providerOperationRemoteMatch {
		return ProviderOperationRecovered
	}
	return providerOperationRecoveryDecision(op.Method, op.State, dstState, srcState, op.SourceIsDir)
}

func RecoverProviderOperation(ctx context.Context, op *model.WebDAVProviderOperation) (ProviderOperationRecovery, model.Obj, error) {
	if op == nil {
		return ProviderOperationNotApplied, nil, nil
	}
	dstState, dstObj, err := providerOperationPathState(ctx, op.DestinationPath, op)
	if err != nil {
		return ProviderOperationInconclusive, nil, err
	}
	srcState := providerOperationRemoteInconclusive
	if strings.EqualFold(op.Method, ProviderOperationMove) {
		srcState, _, err = providerOperationSourcePathState(ctx, op)
		if err != nil {
			return ProviderOperationInconclusive, dstObj, err
		}
	}
	return providerOperationRecoveryDecisionWithEvidence(op, dstState, srcState), dstObj, nil
}

func providerOperationRecoveryLabel(recovery ProviderOperationRecovery) string {
	switch recovery {
	case ProviderOperationNotApplied:
		return "not_applied"
	case ProviderOperationRecovered:
		return "recovered"
	default:
		return "inconclusive"
	}
}

func providerOperationConfirmationDelay() time.Duration {
	seconds := 1
	if conf.Conf != nil && conf.Conf.WebDAVWriteback.VerifyIntervalSeconds > 0 {
		seconds = conf.Conf.WebDAVWriteback.VerifyIntervalSeconds
	}
	if seconds > 2 {
		seconds = 2
	}
	return time.Duration(seconds) * time.Second
}

func providerOperationRecoveryDue(op *model.WebDAVProviderOperation, now time.Time) bool {
	if op == nil || op.LastCheckedAt == nil {
		return true
	}
	return !now.Before(op.LastCheckedAt.Add(providerOperationConfirmationDelay()))
}

func providerOperationNeedsNotAppliedConfirmation(state string) bool {
	return state == ProviderOperationStarted || state == ProviderOperationFailed
}

// ObserveProviderOperationRecovery persists recovery evidence. STARTED and
// FAILED operations need two separated "not applied" observations before
// WebDAV may retry or clean provider state; one stale 115 view is insufficient.
func ObserveProviderOperationRecovery(op *model.WebDAVProviderOperation, recovery ProviderOperationRecovery, checkErr error) (bool, error) {
	if op == nil || op.ID == 0 {
		return recovery != ProviderOperationInconclusive && checkErr == nil, nil
	}
	now := time.Now()
	label := providerOperationRecoveryLabel(recovery)
	lastError := ""
	if checkErr != nil {
		label = providerOperationRecoveryLabel(ProviderOperationInconclusive)
		lastError = checkErr.Error()
		recovery = ProviderOperationInconclusive
	}

	confirmed := recovery != ProviderOperationInconclusive
	err := db.GetDb().Transaction(func(tx *gorm.DB) error {
		var locked model.WebDAVProviderOperation
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&locked, op.ID).Error; err != nil {
			return err
		}

		count := 1
		sameObservation := locked.LastRecovery == label
		if sameObservation {
			count = locked.RecoveryCount + 1
		}
		if recovery == ProviderOperationNotApplied &&
			providerOperationNeedsNotAppliedConfirmation(locked.State) {
			confirmed = sameObservation &&
				locked.RecoveryCount >= 1 &&
				locked.LastCheckedAt != nil &&
				now.Sub(*locked.LastCheckedAt) >= providerOperationConfirmationDelay()
		}

		if err := tx.Model(&model.WebDAVProviderOperation{}).Where("id = ?", locked.ID).Updates(map[string]any{
			"recovery_count":  count,
			"last_recovery":   label,
			"last_error":      lastError,
			"last_checked_at": &now,
		}).Error; err != nil {
			return err
		}
		op.RecoveryCount = count
		op.LastRecovery = label
		op.LastError = lastError
		op.LastCheckedAt = &now
		return nil
	})
	return confirmed, err
}

func providerOperationSourceSuperseded(op *model.WebDAVProviderOperation) (bool, error) {
	if op == nil {
		return false, nil
	}
	current, err := getByPath(op.SourcePath)
	if err != nil {
		return false, err
	}
	if op.SourceGeneration == 0 {
		return current != nil && current.State != StateDeleted, nil
	}
	if current == nil {
		return true, nil
	}
	return current.Generation != op.SourceGeneration || current.State == StateDeleted, nil
}

func applyProviderOperationDestinationRoot(op *model.WebDAVProviderOperation) error {
	if op == nil {
		return nil
	}
	now := time.Now()
	source := ProviderOperationSourceObject(op)
	return db.GetDb().Transaction(func(tx *gorm.DB) error {
		var dst model.WebDAVWritebackObject
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("path_key = ?", pathKey(op.DestinationPath)).First(&dst).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if err == nil && !dst.UpdatedAt.IsZero() && !op.CreatedAt.IsZero() && dst.UpdatedAt.After(op.CreatedAt) {
			expected := providerOperationExpectedRow(op)
			if compareRemoteContent(expected, toObject(&dst), false) != remoteContentMatch {
				return ErrProviderOperationStale
			}
		}
		setProviderCompletedRoot(&dst, op.DestinationPath, source, now)
		if dst.ID == 0 {
			return tx.Create(&dst).Error
		}
		return tx.Save(&dst).Error
	})
}

// ReconcileProviderOperationMetadata binds metadata recovery to the source
// generation captured before provider mutation. If that source was superseded,
// file operations can safely restore only the destination root from the durable
// snapshot; directory trees remain blocked rather than touching newer children.
func ReconcileProviderOperationMetadata(op *model.WebDAVProviderOperation) error {
	if op == nil {
		return nil
	}
	if applied, err := ProviderOperationMetadataApplied(op); err != nil || applied {
		return err
	}
	superseded, err := providerOperationSourceSuperseded(op)
	if err != nil {
		return err
	}
	if superseded {
		if op.SourceIsDir {
			return ErrProviderOperationStale
		}
		return applyProviderOperationDestinationRoot(op)
	}

	sourceRoot := ProviderOperationSourceObject(op)
	if strings.EqualFold(op.Method, ProviderOperationMove) {
		return MoveTreeMetadata(op.SourcePath, op.DestinationPath, sourceRoot)
	}
	return CopyTreeMetadata(op.SourcePath, op.DestinationPath, sourceRoot)
}

func providerOperationDestinationGenerationAdvanced(op *model.WebDAVProviderOperation, dst *model.WebDAVWritebackObject) bool {
	if op == nil || dst == nil || op.DestinationGeneration == 0 {
		return true
	}
	return dst.Generation > op.DestinationGeneration
}

func ProviderOperationMetadataApplied(op *model.WebDAVProviderOperation) (bool, error) {
	if op == nil {
		return false, nil
	}
	dst, err := getByPath(op.DestinationPath)
	if err != nil || dst == nil || dst.State == StateDeleted || dst.IsDir != op.SourceIsDir {
		return false, err
	}
	if !providerOperationDestinationGenerationAdvanced(op, dst) {
		// A pre-existing canonical destination root is not proof that the
		// provider operation's metadata transaction completed. Reconciliation
		// always advances its generation.
		return false, nil
	}
	if !op.SourceIsDir {
		if dst.Size != op.SourceSize {
			return false, nil
		}
		if op.SourceSHA1 != "" {
			dstSHA1 := canonicalContentSHA1(dst)
			if dstSHA1 == "" || !strings.EqualFold(dstSHA1, op.SourceSHA1) {
				return false, nil
			}
		}
	}
	if strings.EqualFold(op.Method, ProviderOperationMove) {
		src, err := getByPath(op.SourcePath)
		if err != nil {
			return false, err
		}
		if op.SourceGeneration > 0 {
			// A tombstone from the old move or any newer source generation means
			// the old source must no longer be mutated by this intent. With a
			// matching destination above, the old operation is converged.
			if src == nil || src.Generation > op.SourceGeneration {
				return true, nil
			}
			return false, nil
		}
		if src == nil || src.State == StateDeleted {
			return true, nil
		}
		return false, nil
	}
	return true, nil
}

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
			HashInfo: utils.NewHashInfo(utils.SHA1, row.PayloadSHA1),
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

func lockNullRetryAt(now time.Time, duration time.Duration) *time.Time {
	if duration < 0 {
		return nil
	}
	expires := now.Add(duration)
	return &expires
}

// CommitLockNull creates the RFC4918 lock-null resource for a LOCK on an
// unmapped path. The placeholder is canonical-only: it is never dispatched to
// the backing provider and a later PUT upgrades the same row into a normal
// queued generation.
func CommitLockNull(ctx context.Context, p string, now time.Time, duration time.Duration) (*model.WebDAVWritebackObject, bool, error) {
	if !Enabled() {
		return nil, false, nil
	}
	p = utils.FixAndCleanPath(p)
	if now.IsZero() {
		now = time.Now()
	}
	parent := path.Dir(p)
	key := pathKey(p)
	retryAt := lockNullRetryAt(now, duration)
	created := false
	var oldSpool string
	var saved model.WebDAVWritebackObject

	err := db.GetDb().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var row model.WebDAVWritebackObject
		findErr := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("path_key = ?", key).First(&row).Error
		if findErr != nil && !errors.Is(findErr, gorm.ErrRecordNotFound) {
			return findErr
		}
		if findErr == nil {
			if row.State == StateLockNull {
				row.RetryAt = retryAt
				if err := tx.Save(&row).Error; err != nil {
					return err
				}
				saved = row
				return nil
			}
			if row.State != StateDeleted {
				saved = row
				return nil
			}
			created = true
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
		row.IsDir = false
		row.Size = 0
		row.ModTime = now
		row.CreateTime = now
		row.ETag = canonicalETag(key, row.Generation, 0)
		row.State = StateLockNull
		row.SpoolPath = ""
		row.PayloadSHA1 = ""
		row.MimeType = ""
		row.CleanupPath = ""
		row.LastError = ""
		row.RetryCount = 0
		row.VerifyCount = 0
		row.RetryAt = retryAt
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
	if oldSpool != "" && !spoolIsActive(oldSpool) {
		removeSpoolIfUnreferenced(oldSpool)
	}
	return &saved, created, nil
}

func RefreshLockNull(ctx context.Context, p string, now time.Time, duration time.Duration) error {
	if !Enabled() {
		return nil
	}
	if now.IsZero() {
		now = time.Now()
	}
	retryAt := lockNullRetryAt(now, duration)
	value := any(nil)
	if retryAt != nil {
		value = retryAt
	}
	return db.GetDb().WithContext(ctx).
		Model(&model.WebDAVWritebackObject{}).
		Where("path_key = ? AND state = ?", pathKey(p), StateLockNull).
		Update("retry_at", value).Error
}

func ReleaseLockNull(ctx context.Context, p string) error {
	if !Enabled() {
		return nil
	}
	return db.GetDb().WithContext(ctx).
		Where("path_key = ? AND state = ?", pathKey(p), StateLockNull).
		Delete(&model.WebDAVWritebackObject{}).Error
}

type remoteContentComparison uint8

const (
	remoteContentInconclusive remoteContentComparison = iota
	remoteContentMatch
	remoteContentMismatch
)

func compareRemoteContent(row *model.WebDAVWritebackObject, remote model.Obj, requireHash bool) remoteContentComparison {
	if row == nil || remote == nil {
		return remoteContentInconclusive
	}
	if remote.IsDir() != row.IsDir {
		return remoteContentMismatch
	}
	if row.IsDir {
		return remoteContentMatch
	}
	if remote.GetSize() != row.Size {
		return remoteContentMismatch
	}
	expectedSHA1 := canonicalContentSHA1(row)
	if expectedSHA1 == "" {
		return remoteContentMatch
	}
	remoteSHA1 := remote.GetHash().GetHash(utils.SHA1)
	if remoteSHA1 == "" {
		if requireHash {
			return remoteContentInconclusive
		}
		return remoteContentMatch
	}
	if strings.EqualFold(remoteSHA1, expectedSHA1) {
		return remoteContentMatch
	}
	return remoteContentMismatch
}

func remoteContentMatchesCanonical(row *model.WebDAVWritebackObject, remote model.Obj) bool {
	return compareRemoteContent(row, remote, false) == remoteContentMatch
}

func exactRemoteByName(objs []model.Obj, name string) model.Obj {
	for _, obj := range objs {
		if obj != nil && obj.GetName() == name {
			return obj
		}
	}
	return nil
}

func deleteCompletedCanonical(row *model.WebDAVWritebackObject) (bool, error) {
	if row == nil {
		return false, nil
	}
	if ops, err := activeProviderOperations(); err != nil {
		return false, err
	} else if providerOperationProtectsCanonicalPath(ops, row.Path, time.Now()) {
		return false, nil
	}
	res := db.GetDb().
		Where("id = ? AND generation = ? AND state = ? AND spool_path = ''", row.ID, row.Generation, StateCompleted).
		Delete(&model.WebDAVWritebackObject{})
	if res.Error != nil {
		return false, res.Error
	}
	return res.RowsAffected > 0, nil
}

func completedDivergenceConfirmationDelay() time.Duration {
	seconds := 1
	if conf.Conf != nil {
		seconds = max(1, conf.Conf.WebDAVWriteback.VerifyIntervalSeconds)
	}
	return time.Duration(seconds) * time.Second
}

func completedDivergenceConfirmed(verifyCount int, retryAt *time.Time, now time.Time) bool {
	return verifyCount > 0 && retryAt != nil && !now.Before(*retryAt)
}

// completedDivergenceProbeState turns the persisted retry deadline into a
// single-winner lease. A due caller advances the deadline before it performs
// the expensive provider refresh, so concurrent Cloud Sync scans cannot all
// force-refresh the same 115 directory. An inconclusive refresh naturally
// waits until the next deadline instead of hot-looping.
func completedDivergenceProbeState(verifyCount int, retryAt *time.Time, now time.Time) (claimFresh bool, nextCount int, nextRetryAt *time.Time) {
	if verifyCount > 0 && retryAt != nil && now.Before(*retryAt) {
		return false, verifyCount, nil
	}
	next := now.Add(completedDivergenceConfirmationDelay())
	if completedDivergenceConfirmed(verifyCount, retryAt, now) {
		return true, verifyCount + 1, &next
	}
	return false, 1, &next
}

// observeCompletedFileDivergence records a suspicious provider view but never
// deletes canonical metadata by itself. Once the confirmation interval has
// elapsed exactly one caller claims the next fresh provider read by advancing
// retry_at while holding the row lock. This keeps cached/concurrent scans from
// counting as destructive evidence or repeatedly hammering the provider.
func observeCompletedFileDivergenceWithOps(row *model.WebDAVWritebackObject, now time.Time, activeOps []model.WebDAVProviderOperation) (bool, error) {
	if row == nil || row.IsDir || row.State != StateCompleted || row.SpoolPath != "" {
		return false, nil
	}
	if providerOperationProtectsCanonicalPath(activeOps, row.Path, now) {
		return false, nil
	}

	needsFreshConfirmation := false
	err := db.GetDb().Transaction(func(tx *gorm.DB) error {
		var current model.WebDAVWritebackObject
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ? AND generation = ? AND state = ? AND spool_path = ''", row.ID, row.Generation, StateCompleted).
			First(&current).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return err
		}
		if current.IsDir {
			return nil
		}
		claimFresh, nextCount, nextRetryAt := completedDivergenceProbeState(current.VerifyCount, current.RetryAt, now)
		if nextRetryAt == nil {
			return nil
		}
		needsFreshConfirmation = claimFresh
		lastError := "remote divergence observed once; waiting for force-refreshed confirmation"
		if claimFresh {
			lastError = "remote divergence fresh confirmation claimed; throttling additional provider refreshes"
		}
		return tx.Model(&model.WebDAVWritebackObject{}).
			Where("id = ? AND generation = ? AND state = ?", current.ID, current.Generation, StateCompleted).
			Updates(map[string]any{
				"verify_count": nextCount,
				"retry_at":     nextRetryAt,
				"last_error":   lastError,
			}).Error
	})
	return needsFreshConfirmation, err
}

func observeCompletedFileDivergence(row *model.WebDAVWritebackObject, now time.Time) (bool, error) {
	ops, err := activeProviderOperations()
	if err != nil {
		return false, err
	}
	return observeCompletedFileDivergenceWithOps(row, now, ops)
}

func clearCompletedFileDivergence(row *model.WebDAVWritebackObject) error {
	if row == nil || row.IsDir || row.State != StateCompleted || row.SpoolPath != "" {
		return nil
	}
	return db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("id = ? AND generation = ? AND state = ? AND spool_path = ''", row.ID, row.Generation, StateCompleted).
		Where("(verify_count <> 0 OR retry_at IS NOT NULL OR last_error <> '')").
		Updates(map[string]any{
			"verify_count": 0,
			"retry_at":     nil,
			"last_error":   "",
		}).Error
}

// ReconcileDirect validates a completed canonical object after its local spool
// cache is gone. A single-object provider lookup may prove an exact match, but
// it is never allowed to prove a mismatch by itself: 115 can transiently return
// size=0, missing hashes, stale type metadata, or NotFound after upload. Any
// apparent mismatch is confirmed through one force-refreshed exact-name parent
// listing before canonical metadata is dropped.
func completedRemoteVerificationInterval() time.Duration {
	seconds := 1
	if conf.Conf != nil {
		seconds = max(1, conf.Conf.WebDAVWriteback.VerifyIntervalSeconds)
	}
	return time.Duration(seconds) * time.Second
}

func completedRemoteVerificationFresh(row *model.WebDAVWritebackObject, now time.Time) bool {
	if row == nil ||
		row.IsDir ||
		row.State != StateCompleted ||
		row.SpoolPath != "" ||
		row.RemoteVerifiedAt == nil ||
		row.RemoteGeneration != row.Generation ||
		row.VerifyCount != 0 ||
		row.RetryAt != nil ||
		row.LastError != "" {
		return false
	}
	return now.Before(row.RemoteVerifiedAt.Add(completedRemoteVerificationInterval()))
}

func refreshCompletedRemoteVerification(row *model.WebDAVWritebackObject, remote model.Obj, now time.Time) error {
	if row == nil || remote == nil || row.IsDir || row.State != StateCompleted || row.SpoolPath != "" {
		return nil
	}
	evidence := captureRemoteVerification(row, remote, now)
	return db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("id = ? AND generation = ? AND state = ? AND spool_path = ''", row.ID, row.Generation, StateCompleted).
		Updates(map[string]any{
			"verify_count":       0,
			"retry_at":           nil,
			"last_error":         "",
			"remote_object_id":   evidence.objectID,
			"remote_sha1":        evidence.sha1,
			"remote_generation":  evidence.generation,
			"remote_verified_at": &evidence.verifiedAt,
		}).Error
}

func ReconcileDirect(ctx context.Context, p string) (missing bool, err error) {
	if !Enabled() {
		return false, nil
	}
	row, err := getByPath(p)
	if err != nil || row == nil || row.State != StateCompleted || row.SpoolPath != "" {
		return false, err
	}
	now := time.Now()
	if canonicalShadowInGrace(row, now) {
		return false, nil
	}
	if completedRemoteVerificationFresh(row, now) {
		return false, nil
	}

	requireHash := providerRequiresPayloadHash(row.Path)
	remote, getErr := fs.Get(ctx, row.Path, &fs.GetArgs{NoLog: true})
	if getErr == nil && remote != nil {
		if row.IsDir && remote.IsDir() {
			// Once the directory consistency grace expires, a visible provider
			// collection can take ownership of directory metadata immediately.
			_, delErr := deleteCompletedCanonical(row)
			return false, delErr
		}
		if !row.IsDir && compareRemoteContent(row, remote, requireHash) == remoteContentMatch {
			if refreshErr := refreshCompletedRemoteVerification(row, remote, now); refreshErr != nil {
				return false, refreshErr
			}
			return false, nil
		}
	}
	if getErr != nil && !errs.IsObjectNotFound(getErr) {
		// Provider/network failures are not evidence of remote loss.
		return false, nil
	}

	// A single-object miss/mismatch only arms the confirmation deadline. Direct
	// GET/HEAD/PROPFIND traffic inside that window must not force-refresh the
	// parent repeatedly. Once due, the row-lock claim below advances retry_at
	// before the expensive refresh, so only one concurrent request performs it.
	if !row.IsDir {
		ready, observeErr := observeCompletedFileDivergence(row, now)
		if observeErr != nil || !ready {
			return false, observeErr
		}
	}

	objs, listErr := fs.List(ctx, row.Parent, &fs.ListArgs{Refresh: true, NoLog: true})
	if listErr != nil {
		return false, nil
	}
	remote = exactRemoteByName(objs, row.Name)
	if remote == nil {
		if row.IsDir {
			return false, nil
		}
		return deleteCompletedCanonical(row)
	}
	if remote.IsDir() != row.IsDir {
		if row.IsDir {
			_, delErr := deleteCompletedCanonical(row)
			return false, delErr
		}
		_, delErr := deleteCompletedCanonical(row)
		return false, delErr
	}
	if row.IsDir {
		_, delErr := deleteCompletedCanonical(row)
		return false, delErr
	}

	switch compareRemoteContent(row, remote, requireHash) {
	case remoteContentMatch:
		if refreshErr := refreshCompletedRemoteVerification(row, remote, now); refreshErr != nil {
			return false, refreshErr
		}
		return false, nil
	case remoteContentInconclusive:
		// A refreshed 115 listing that still omits SHA-1 cannot prove either
		// identity or loss. The fresh-confirmation claim has already moved
		// retry_at forward, so subsequent scans wait before refreshing again.
		return false, nil
	default:
		_, delErr := deleteCompletedCanonical(row)
		return false, delErr
	}
}

func shouldDropCanonicalAfterRemoteList(row *model.WebDAVWritebackObject, remoteReliable bool, remote model.Obj, now time.Time, requireHash bool) bool {
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
	return compareRemoteContent(row, remote, requireHash) == remoteContentMismatch
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
func OverlayList(ctx context.Context, parent string, remote []model.Obj, remoteReliable bool) ([]model.Obj, bool, error) {
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
	activeOps, err := activeProviderOperations()
	if err != nil {
		return nil, false, err
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

	var freshByName map[string]model.Obj
	freshLoaded := false
	freshReliable := false
	loadFresh := func() {
		if freshLoaded {
			return
		}
		freshLoaded = true
		objs, listErr := fs.List(ctx, parent, &fs.ListArgs{Refresh: true, NoLog: true})
		if listErr != nil {
			return
		}
		freshReliable = true
		freshByName = make(map[string]model.Obj, len(objs))
		for _, obj := range objs {
			if obj != nil {
				freshByName[obj.GetName()] = obj
			}
		}
	}

	now := time.Now()
	for i := range rows {
		row := &rows[i]
		protectedByProviderOperation := providerOperationProtectsCanonicalPath(activeOps, row.Path, now)
		requireHash := false
		if !row.IsDir {
			requireHash = providerRequiresPayloadHash(row.Path)
		}
		if row.State == StateDeleted {
			delete(byName, row.Name)
			continue
		}
		remoteObj, remotePresent := byName[row.Name]
		if row.IsDir {
			if !protectedByProviderOperation && remoteReliable && directoryShadowExpired(row, now) {
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
		if !protectedByProviderOperation && row.State == StateCompleted && !row.IsDir && row.SpoolPath == "" && remoteReliable && remoteObj != nil &&
			compareRemoteContent(row, remoteObj, requireHash) == remoteContentMatch {
			if clearErr := clearCompletedFileDivergence(row); clearErr != nil {
				return nil, false, clearErr
			}
		}
		if !protectedByProviderOperation &&
			shouldDropCanonicalAfterRemoteList(row, remoteReliable, remoteObj, now, requireHash) {
			ready, observeErr := observeCompletedFileDivergenceWithOps(row, now, activeOps)
			if observeErr != nil {
				return nil, false, observeErr
			}
			if ready {
				loadFresh()
				if freshReliable {
					freshObj, freshPresent := freshByName[row.Name]
					freshComparison := remoteContentMismatch
					if freshPresent {
						freshComparison = compareRemoteContent(row, freshObj, requireHash)
					}
					switch freshComparison {
					case remoteContentMatch:
						if clearErr := clearCompletedFileDivergence(row); clearErr != nil {
							return nil, false, clearErr
						}
					case remoteContentInconclusive:
						// A force-refreshed 115 listing still lacks enough identity
						// evidence. Preserve canonical metadata and retry later.
					default:
						deleted, delErr := deleteCompletedCanonical(row)
						if delErr != nil {
							return nil, false, delErr
						}
						if deleted {
							if freshPresent {
								if _, ok := byName[row.Name]; !ok {
									order = append(order, row.Name)
								}
								byName[row.Name] = freshObj
							} else {
								delete(byName, row.Name)
							}
							continue
						}
					}
				}
			}
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

func megabytesToBytes(mb uint64) uint64 {
	unit := uint64(utils.MB)
	maxUint := ^uint64(0)
	if mb > maxUint/unit {
		return maxUint
	}
	return mb * unit
}

func reserveBytes() uint64 {
	if conf.Conf == nil {
		return 0
	}
	return megabytesToBytes(conf.Conf.WebDAVWriteback.ReserveFreeSpaceMB)
}

func incomingReservationChunkBytes() uint64 {
	mb := uint64(64)
	if conf.Conf != nil && conf.Conf.WebDAVWriteback.IncomingReservationChunkMB > 0 {
		mb = conf.Conf.WebDAVWriteback.IncomingReservationChunkMB
	}
	return megabytesToBytes(mb)
}

func spoolAdmissionRequired(floor, reserved, additional uint64) (uint64, bool) {
	maxUint := ^uint64(0)
	if floor > maxUint-reserved {
		return 0, false
	}
	required := floor + reserved
	if required > maxUint-additional {
		return 0, false
	}
	return required + additional, true
}

type receivingPathState struct {
	active int
}

type receivingSession struct {
	key   string
	state *receivingPathState
}

var (
	spaceMu          sync.Mutex
	reservedIncoming uint64
	receivingMu      sync.Mutex
	receivingPaths   = make(map[string]*receivingPathState)
)

type incomingReservation struct {
	remaining uint64
	released  bool
}

func (r *incomingReservation) grow(additional uint64) error {
	if r == nil || additional == 0 {
		return nil
	}
	spaceMu.Lock()
	defer spaceMu.Unlock()

	usage, err := disk.Usage(conf.Conf.WebDAVWriteback.SpoolDir)
	if err != nil {
		return err
	}
	required, ok := spoolAdmissionRequired(reserveBytes(), reservedIncoming, additional)
	if !ok || usage.Free < required {
		if !ok {
			required = ^uint64(0)
		}
		return &SpoolCapacityError{Free: usage.Free, Required: required}
	}
	reservedIncoming += additional
	r.remaining += additional
	return nil
}

func (r *incomingReservation) ensureForWrite(need uint64) error {
	if r == nil || need == 0 || r.remaining >= need {
		return nil
	}
	additional := incomingReservationChunkBytes()
	missing := need - r.remaining
	if additional < missing {
		additional = missing
	}
	return r.grow(additional)
}

func (r *incomingReservation) consume(written uint64) {
	if r == nil || written == 0 {
		return
	}
	spaceMu.Lock()
	defer spaceMu.Unlock()
	if written > r.remaining {
		written = r.remaining
	}
	r.remaining -= written
	if written > reservedIncoming {
		reservedIncoming = 0
	} else {
		reservedIncoming -= written
	}
}

func (r *incomingReservation) verifyCapacity() error {
	if r == nil {
		return nil
	}
	spaceMu.Lock()
	defer spaceMu.Unlock()
	usage, err := disk.Usage(conf.Conf.WebDAVWriteback.SpoolDir)
	if err != nil {
		return err
	}
	required, ok := spoolAdmissionRequired(reserveBytes(), reservedIncoming, 0)
	if !ok || usage.Free < required {
		if !ok {
			required = ^uint64(0)
		}
		return &SpoolCapacityError{Free: usage.Free, Required: required}
	}
	return nil
}

func (r *incomingReservation) release() {
	if r == nil {
		return
	}
	spaceMu.Lock()
	defer spaceMu.Unlock()
	if r.released {
		return
	}
	if r.remaining > reservedIncoming {
		reservedIncoming = 0
	} else {
		reservedIncoming -= r.remaining
	}
	r.remaining = 0
	r.released = true
}

func reserveIncomingBytes(expected int64) (*incomingReservation, error) {
	reservation := &incomingReservation{}
	var initial uint64
	switch {
	case expected > 0:
		initial = uint64(expected)
	case expected == 0:
		if err := reservation.verifyCapacity(); err != nil {
			return nil, err
		}
		return reservation, nil
	default:
		initial = incomingReservationChunkBytes()
	}
	if err := reservation.grow(initial); err != nil {
		return nil, err
	}
	return reservation, nil
}

func beginReceiving(p string) (*receivingSession, func()) {
	key := pathKey(p)
	receivingMu.Lock()
	state := receivingPaths[key]
	if state == nil {
		state = &receivingPathState{}
		receivingPaths[key] = state
	}
	state.active++
	session := &receivingSession{key: key, state: state}
	receivingMu.Unlock()

	return session, func() {
		receivingMu.Lock()
		if current := receivingPaths[key]; current == state {
			state.active--
			if state.active <= 0 {
				delete(receivingPaths, key)
			}
		}
		receivingMu.Unlock()
	}
}

func receiveSequenceSuperseded(lastCommitted, current uint64) bool {
	return lastCommitted > current
}

func beginReceiveSequence(ctx context.Context, p string) (uint64, error) {
	p = utils.FixAndCleanPath(p)
	key := pathKey(p)
	var sequence uint64
	err := db.GetDb().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		seed := model.WebDAVWritebackReceiveFence{PathKey: key, Path: p}
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "path_key"}},
			DoNothing: true,
		}).Create(&seed).Error; err != nil {
			return err
		}

		var fence model.WebDAVWritebackReceiveFence
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("path_key = ?", key).
			First(&fence).Error; err != nil {
			return err
		}
		fence.NextSequence++
		fence.Path = p
		sequence = fence.NextSequence
		return tx.Model(&model.WebDAVWritebackReceiveFence{}).
			Where("id = ?", fence.ID).
			Updates(map[string]any{
				"path":          p,
				"next_sequence": sequence,
			}).Error
	})
	return sequence, err
}

func lockReceiveFence(tx *gorm.DB, p string) (*model.WebDAVWritebackReceiveFence, error) {
	key := pathKey(p)
	var fence model.WebDAVWritebackReceiveFence
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("path_key = ?", key).
		First(&fence).Error; err != nil {
		return nil, err
	}
	return &fence, nil
}

func advanceReceiveFence(tx *gorm.DB, fence *model.WebDAVWritebackReceiveFence, sequence uint64) error {
	if fence == nil || sequence == 0 {
		return errors.New("write-back receive fence is missing")
	}
	if receiveSequenceSuperseded(fence.LastCommittedSequence, sequence) {
		return errors.New("cannot move receive fence backwards")
	}
	if fence.LastCommittedSequence == sequence {
		return nil
	}
	fence.LastCommittedSequence = sequence
	return tx.Model(&model.WebDAVWritebackReceiveFence{}).
		Where("id = ? AND last_committed_sequence <= ?", fence.ID, sequence).
		Update("last_committed_sequence", sequence).Error
}

func isReceiving(p string) bool {
	key := pathKey(p)
	receivingMu.Lock()
	defer receivingMu.Unlock()
	state := receivingPaths[key]
	return state != nil && state.active > 0
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

func copyToSpool(dst *os.File, src io.Reader, expected int64, reservation *incomingReservation) (int64, string, error) {
	buf := make([]byte, 4*utils.MB)
	payloadHasher := utils.SHA1.NewFunc()
	writer := io.MultiWriter(dst, payloadHasher)
	var total int64
	var sinceCheck int64

	for {
		n, readErr := src.Read(buf)
		if n > 0 {
			if expected >= 0 && (int64(n) > expected || total > expected-int64(n)) {
				return total, "", fmt.Errorf("WebDAV PUT exceeds declared size: expected %d bytes", expected)
			}
			if err := reservation.ensureForWrite(uint64(n)); err != nil {
				return total, "", err
			}
			wn, writeErr := writer.Write(buf[:n])
			total += int64(wn)
			sinceCheck += int64(wn)
			reservation.consume(uint64(wn))
			if writeErr != nil {
				return total, "", writeErr
			}
			if wn != n {
				return total, "", io.ErrShortWrite
			}
			if sinceCheck >= 64*utils.MB {
				if err := reservation.verifyCapacity(); err != nil {
					return total, "", err
				}
				sinceCheck = 0
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) || (expected >= 0 && total == expected) {
				// net/http can surface a late request cancellation instead of EOF
				// after all declared bytes have already been delivered. The body is
				// complete in that case; rejecting it would discard a fully received
				// large Cloud Sync PUT and force a full retransmission.
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

func durableCommitContext(ctx context.Context) context.Context {
	// Once the complete request body has been fsynced and atomically moved into
	// the spool, a client disconnect must not discard that durable payload.
	// Preserve request-scoped values for database hooks while detaching
	// cancellation/deadlines from the HTTP request.
	return context.WithoutCancel(ctx)
}

// Commit receives the complete opaque WebDAV object into the local spool and
// only then commits a new canonical generation into MySQL.
func Commit(ctx context.Context, p string, body io.Reader, expected int64, modTime, createTime time.Time, mime string) (*model.WebDAVWritebackObject, bool, error) {
	if !Enabled() {
		return nil, false, errors.New("WebDAV write-back is disabled")
	}
	p = utils.FixAndCleanPath(p)
	modTimeProvided := !modTime.IsZero()
	createTimeProvided := !createTime.IsZero()
	_, releaseReceiving := beginReceiving(p)
	defer releaseReceiving()
	receiveSequence, err := beginReceiveSequence(ctx, p)
	if err != nil {
		return nil, false, err
	}
	spoolDir := conf.Conf.WebDAVWriteback.SpoolDir
	if err := os.MkdirAll(spoolDir, 0o700); err != nil {
		return nil, false, err
	}

	reservation, err := reserveIncomingBytes(expected)
	if err != nil {
		return nil, false, err
	}
	defer reservation.release()

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

	actualSize, payloadSHA1, err := copyToSpool(tmp, body, expected, reservation)
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

	// MySQL is the authoritative ordering point for same-path Cloud Sync PUTs.
	// The path fence survives process restarts and is shared by every instance.
	commitCtx := durableCommitContext(ctx)
	superseded := false
	err = db.GetDb().WithContext(commitCtx).Transaction(func(tx *gorm.DB) error {
		fence, err := lockReceiveFence(tx, p)
		if err != nil {
			return err
		}
		var row model.WebDAVWritebackObject
		findErr := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("path_key = ?", key).First(&row).Error
		if findErr != nil && !errors.Is(findErr, gorm.ErrRecordNotFound) {
			return findErr
		}
		if receiveSequenceSuperseded(fence.LastCommittedSequence, receiveSequence) {
			if findErr != nil {
				return errors.New("superseded WebDAV PUT has no canonical successor")
			}
			superseded = true
			saved = row
			return nil
		}
		if findErr == nil {
			if canCoalesceDuplicatePut(&row, actualSize, payloadSHA1) {
				if _, statErr := os.Stat(row.SpoolPath); statErr == nil {
					duplicate = true
					applyDuplicatePutMetadata(&row, modTime, createTime, mime, modTimeProvided, createTimeProvided)
					updates := map[string]any{
						"mod_time":    row.ModTime,
						"create_time": row.CreateTime,
						"mime_type":   row.MimeType,
					}
					if row.State == StateQueued || row.State == StateFailed {
						now := time.Now()
						row.RetryAt = &now
						row.LastError = ""
						updates["retry_at"] = &now
						updates["last_error"] = ""
						wakeDuplicate = true
					}
					if err := tx.Model(&model.WebDAVWritebackObject{}).
						Where("id = ? AND generation = ?", row.ID, row.Generation).
						Updates(updates).Error; err != nil {
						return err
					}
					saved = row
					return advanceReceiveFence(tx, fence, receiveSequence)
				}
			}
			if canReverifyCompletedDuplicatePut(&row, actualSize, payloadSHA1) {
				now := time.Now()
				applyDuplicatePutMetadata(&row, modTime, createTime, mime, modTimeProvided, createTimeProvided)
				row.State = StateVerifying
				row.SpoolPath = finalName
				row.PayloadSHA1 = payloadSHA1
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
				return advanceReceiveFence(tx, fence, receiveSequence)
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
		return advanceReceiveFence(tx, fence, receiveSequence)
	})
	if err != nil {
		_ = os.Remove(finalName)
		return nil, false, err
	}
	if superseded {
		_ = os.Remove(finalName)
		syncDir(spoolDir)
		return &saved, false, nil
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
					"generation":         gorm.Expr("generation + 1"),
					"state":              StateDeleted,
					"retry_at":           &now,
					"last_error":         "",
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

// StageProviderDelete atomically creates a canonical tombstone for a
// provider-only object. It is used only after DeleteTree found no tracked
// canonical state and the provider lookup proved the object exists.
//
// The transaction re-checks the whole subtree before inserting the tombstone.
// If another request committed canonical state in the meantime, the delete is
// rejected as retryable instead of consuming that newer generation.
func StageProviderDelete(ctx context.Context, p string, source model.Obj) (bool, error) {
	if !Enabled() {
		return false, nil
	}
	p = utils.FixAndCleanPath(p)
	now := time.Now()
	staged := false
	alreadyDeleted := false

	err := db.GetDb().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var candidates []model.WebDAVWritebackObject
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("path = ? OR path LIKE ? ESCAPE '~'", p, descendantLikePattern(p)).
			Order("id asc").
			Find(&candidates).Error; err != nil {
			return err
		}

		matched := 0
		deleted := 0
		for i := range candidates {
			row := &candidates[i]
			if !isPathOrDescendant(row.Path, p) {
				continue
			}
			matched++
			if row.State == StateDeleted {
				deleted++
				continue
			}
			return ErrCanonicalChanged
		}
		if matched > 0 {
			if matched == deleted {
				alreadyDeleted = true
				return nil
			}
			return ErrCanonicalChanged
		}

		row := providerMoveSourceTombstone(p, source, now)
		if err := tx.Create(&row).Error; err != nil {
			return err
		}
		staged = true
		return nil
	})
	if err != nil {
		// A concurrent creator may win the unique path_key race after the
		// subtree scan. Translate that database conflict back into canonical
		// state so callers return a bounded retry instead of deleting blindly.
		current, lookupErr := getByPath(p)
		if lookupErr == nil && current != nil {
			if current.State == StateDeleted {
				return true, nil
			}
			return false, ErrCanonicalChanged
		}
		return false, err
	}
	if alreadyDeleted {
		return true, nil
	}
	if staged {
		wake()
	}
	return staged, nil
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

func providerOperationPathAbsent(ctx context.Context, p string) (bool, error) {
	remote, getErr := fs.Get(ctx, p, &fs.GetArgs{NoLog: true})
	if getErr == nil && remote != nil {
		return false, nil
	}
	if getErr != nil && !errs.IsObjectNotFound(getErr) {
		return false, getErr
	}
	objs, listErr := fs.List(ctx, path.Dir(p), &fs.ListArgs{Refresh: true, NoLog: true})
	if listErr != nil {
		if errs.IsObjectNotFound(listErr) {
			return true, nil
		}
		return false, listErr
	}
	return exactRemoteByName(objs, path.Base(p)) == nil, nil
}

// CleanupFailedProviderCopy removes a partial COPY destination before the same
// durable intent is retired. COPY keeps the source intact, so a clean retry is
// safer than leaving a mismatched directory permanently fenced.
func failedProviderCopyCleanupAllowed(op *model.WebDAVProviderOperation, current model.Obj, strictIdentity bool) bool {
	if op == nil || current == nil {
		return true
	}
	currentID := current.GetID()
	if op.FailureDestinationObjectID != "" && currentID != "" &&
		op.FailureDestinationObjectID != currentID {
		return false
	}
	if strictIdentity {
		if !op.FailureDestinationObserved ||
			!op.FailureDestinationReady ||
			op.FailureDestinationObjectID == "" ||
			currentID == "" ||
			op.FailureDestinationObjectID != currentID {
			return false
		}
	}
	if op.FailureDestinationObserved && current.IsDir() != op.FailureDestinationIsDir {
		return false
	}
	if !current.IsDir() && op.FailureDestinationReady {
		if current.GetSize() != op.FailureDestinationSize {
			return false
		}
		currentSHA1 := strings.ToLower(current.GetHash().GetHash(utils.SHA1))
		if op.FailureDestinationSHA1 != "" {
			return currentSHA1 != "" && strings.EqualFold(currentSHA1, op.FailureDestinationSHA1)
		}
		if strictIdentity {
			return false
		}
	}
	return true
}

func CleanupFailedProviderCopy(ctx context.Context, op *model.WebDAVProviderOperation) (bool, error) {
	if op == nil ||
		!strings.EqualFold(op.Method, ProviderOperationCopy) ||
		op.State != ProviderOperationFailed {
		return false, nil
	}

	current, present, err := providerOperationDestinationObject(ctx, op.DestinationPath)
	if err != nil {
		return false, err
	}
	if !present {
		if !providerRequiresPayloadHash(op.DestinationPath) {
			return true, nil
		}
		timer := time.NewTimer(providerOperationConfirmationDelay())
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-timer.C:
		}
		return providerOperationPathAbsent(ctx, op.DestinationPath)
	}

	strictIdentity := providerRequiresPayloadHash(op.DestinationPath)
	if !failedProviderCopyCleanupAllowed(op, current, strictIdentity) {
		return false, ErrProviderOperationStale
	}
	if current.IsDir() && op.FailureDestinationReady {
		fingerprint, entries, fingerprintErr := providerDirectoryTreeFingerprint(
			ctx,
			op.DestinationPath,
			providerOperationTreeDepth(op.Method, op.Depth),
			false,
		)
		if fingerprintErr != nil {
			return false, fingerprintErr
		}
		if entries != op.FailureDestinationTreeEntries ||
			!strings.EqualFold(fingerprint, op.FailureDestinationTreeSHA256) {
			return false, ErrProviderOperationStale
		}
	}
	if err := fs.Remove(ctx, op.DestinationPath); err != nil && !errs.IsObjectNotFound(err) {
		return false, err
	}
	absent, err := providerOperationPathAbsent(ctx, op.DestinationPath)
	if err != nil || !absent {
		return absent, err
	}
	if !strictIdentity {
		return true, nil
	}
	timer := time.NewTimer(providerOperationConfirmationDelay())
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-timer.C:
	}
	return providerOperationPathAbsent(ctx, op.DestinationPath)
}

const (
	providerOperationPreparedAbandonAfter = 30 * time.Second
	providerOperationMaintenanceEvery     = 5 * time.Second
)

func providerOperationPreparedExpired(op *model.WebDAVProviderOperation, now time.Time) bool {
	if op == nil || op.State != ProviderOperationPrepared {
		return false
	}
	stamp := op.UpdatedAt
	if stamp.IsZero() {
		stamp = op.CreatedAt
	}
	return !stamp.IsZero() && !now.Before(stamp.Add(providerOperationPreparedAbandonAfter))
}

func recordProviderOperationError(id uint, err error) {
	if id == 0 || err == nil {
		return
	}
	now := time.Now()
	_ = db.GetDb().Model(&model.WebDAVProviderOperation{}).Where("id = ?", id).Updates(map[string]any{
		"last_error":      err.Error(),
		"last_checked_at": &now,
	}).Error
}

func (m *workerManager) maintainProviderOperations() {
	var ops []model.WebDAVProviderOperation
	if err := db.GetDb().
		Order("CASE state WHEN 'applied' THEN 0 WHEN 'prepared' THEN 1 ELSE 2 END").
		Order("COALESCE(last_checked_at, created_at) asc").
		Limit(64).
		Find(&ops).Error; err != nil {
		log.Errorf("write-back provider operation scan failed: %v", err)
		return
	}
	if len(ops) == 0 {
		return
	}

	now := time.Now()
	recovered := 0
	retired := 0
	for i := range ops {
		op := &ops[i]
		if applied, err := ProviderOperationMetadataApplied(op); err != nil {
			recordProviderOperationError(op.ID, err)
			continue
		} else if applied {
			if err := FinishProviderOperation(op.ID); err != nil {
				recordProviderOperationError(op.ID, err)
			} else {
				retired++
			}
			continue
		}

		if providerOperationPreparedExpired(op, now) {
			if err := FinishProviderOperation(op.ID); err != nil {
				recordProviderOperationError(op.ID, err)
			} else {
				retired++
			}
			continue
		}
		if !providerOperationRecoveryDue(op, now) {
			continue
		}

		recovery, _, recoveryErr := RecoverProviderOperation(m.ctx, op)
		confirmed, observeErr := ObserveProviderOperationRecovery(op, recovery, recoveryErr)
		if observeErr != nil {
			recordProviderOperationError(op.ID, observeErr)
			continue
		}
		if recoveryErr != nil || recovery == ProviderOperationInconclusive {
			continue
		}

		switch recovery {
		case ProviderOperationNotApplied:
			if !confirmed {
				continue
			}
			if op.State == ProviderOperationFailed && strings.EqualFold(op.Method, ProviderOperationCopy) {
				cleaned, cleanupErr := CleanupFailedProviderCopy(m.ctx, op)
				if cleanupErr != nil {
					recordProviderOperationError(op.ID, cleanupErr)
					continue
				}
				if !cleaned {
					continue
				}
			}
			// Two separated provider observations agree that the started
			// mutation did not take effect, or a known-failed COPY destination
			// was explicitly cleaned. Retire the fence for a clean retry.
			if err := FinishProviderOperation(op.ID); err != nil {
				recordProviderOperationError(op.ID, err)
			} else {
				retired++
			}
		case ProviderOperationRecovered:
			if err := ReconcileProviderOperationMetadata(op); err != nil {
				recordProviderOperationError(op.ID, err)
				continue
			}
			if err := FinishProviderOperation(op.ID); err != nil {
				recordProviderOperationError(op.ID, err)
				continue
			}
			recovered++
		}
	}
	if recovered > 0 || retired > 0 {
		log.Infof("write-back provider operation maintenance: recovered=%d retired=%d pending=%d", recovered, retired, len(ops)-recovered-retired)
	}
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
		m.maintainProviderOperations()
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
	// LockSystem is process-local, so lock-null shadows cannot survive a
	// restart without their corresponding lock token.
	if err := db.GetDb().Where("state = ?", StateLockNull).Delete(&model.WebDAVWritebackObject{}).Error; err != nil {
		return err
	}

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
	providerTicker := time.NewTicker(providerOperationMaintenanceEvery)
	cleanupTicker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	defer providerTicker.Stop()
	defer cleanupTicker.Stop()
	for {
		m.dispatch()
		select {
		case <-m.stop:
			return
		case <-m.wake:
		case <-ticker.C:
		case <-providerTicker.C:
			m.maintainProviderOperations()
		case <-cleanupTicker.C:
			m.cleanupCompleted()
		}
	}
}

func (m *workerManager) dispatch() {
	now := time.Now()
	// Finite lock-null resources expire with the in-memory lock. Infinite locks
	// have retry_at=NULL and are removed by UNLOCK or startup recovery.
	if err := db.GetDb().
		Where("state = ? AND retry_at IS NOT NULL AND retry_at <= ?", StateLockNull, now).
		Delete(&model.WebDAVWritebackObject{}).Error; err != nil {
		log.Errorf("write-back lock-null cleanup failed: %v", err)
	}
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
	if row == nil || row.IsDir || remote == nil || remote.IsDir() {
		return false
	}
	return compareRemoteContent(row, remote, requireHash) == remoteContentMatch
}

type remoteVerificationState uint8

const (
	remoteVerificationInconclusive remoteVerificationState = iota
	remoteVerificationMatch
	remoteVerificationDivergent
)

func classifyRemoteVerification(row *model.WebDAVWritebackObject, remote model.Obj, verifyErr error, requireHash bool) remoteVerificationState {
	if row == nil || verifyErr != nil {
		return remoteVerificationInconclusive
	}
	// remoteForVerify returns nil,nil only after a force-refreshed parent
	// listing succeeded and the exact name was absent. A direct NotFound or a
	// failed refresh is returned as an error and therefore stays inconclusive.
	if remote == nil {
		return remoteVerificationDivergent
	}
	switch compareRemoteContent(row, remote, requireHash) {
	case remoteContentMatch:
		return remoteVerificationMatch
	case remoteContentMismatch:
		return remoteVerificationDivergent
	default:
		return remoteVerificationInconclusive
	}
}

func advanceRemoteVerification(state remoteVerificationState, current, attempts int) (next int, retryUpload bool) {
	if state != remoteVerificationDivergent {
		return current, false
	}
	next = current + 1
	return next, next >= max(1, attempts)
}

const open115MultipartChunkSize int64 = 20 * utils.MB

func verificationAttemptsFor(row *model.WebDAVWritebackObject, requireHash bool) int {
	attempts := max(1, conf.Conf.WebDAVWriteback.VerifyAttempts)
	if row == nil || !requireHash || row.Size <= open115MultipartChunkSize {
		return attempts
	}
	intervalSeconds := max(1, conf.Conf.WebDAVWriteback.VerifyIntervalSeconds)
	minWindowSeconds := max(intervalSeconds, conf.Conf.WebDAVWriteback.RetryMaxSeconds)
	minAttempts := (minWindowSeconds + intervalSeconds - 1) / intervalSeconds
	return max(attempts, minAttempts)
}

func suppressRepeatedLargeProviderRepair(row *model.WebDAVWritebackObject, requireHash bool) bool {
	return row != nil &&
		requireHash &&
		row.Size > open115MultipartChunkSize &&
		row.RetryCount >= 1
}

func repeatedLargeProviderVerifyDelay(row *model.WebDAVWritebackObject) time.Duration {
	delay := retryDelay(1)
	if row != nil {
		delay = retryDelay(row.RetryCount + 1)
	}
	if conf.Conf != nil {
		floor := time.Duration(max(1, conf.Conf.WebDAVWriteback.RetryMaxSeconds)) * time.Second
		if delay < floor {
			delay = floor
		}
	}
	return delay
}

func remoteVerificationInconclusiveDelay() time.Duration {
	verifySeconds := 1
	retrySeconds := 1
	if conf.Conf != nil {
		verifySeconds = max(1, conf.Conf.WebDAVWriteback.VerifyIntervalSeconds)
		retrySeconds = max(1, conf.Conf.WebDAVWriteback.RetryInitialSeconds)
	}
	// Inconclusive evidence cannot justify another upload. Poll it less
	// aggressively than the normal post-upload visibility window while keeping
	// a bounded cadence for providers that eventually populate hashes.
	seconds := max(verifySeconds, min(retrySeconds, 30))
	return time.Duration(seconds) * time.Second
}

func (m *workerManager) remoteForVerify(row *model.WebDAVWritebackObject) (model.Obj, error) {
	requireHash := providerRequiresPayloadHash(row.Path)
	remote, getErr := fs.Get(m.ctx, row.Path, &fs.GetArgs{NoLog: true})
	if getErr == nil && remoteMatchesCanonical(row, remote, requireHash) {
		return remote, nil
	}

	// A direct 115 miss/mismatch is never destructive evidence by itself.
	// Force-refresh the parent. If that refresh fails, return its error instead
	// of leaking an earlier direct NotFound/mismatch into the retry budget.
	objs, listErr := fs.List(m.ctx, row.Parent, &fs.ListArgs{Refresh: true, NoLog: true})
	if listErr != nil {
		return nil, listErr
	}
	if obj := exactRemoteByName(objs, row.Name); obj != nil {
		return obj, nil
	}
	// A successful refreshed parent listing with no exact name is the only
	// absence representation consumed by classifyRemoteVerification.
	return nil, nil
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
	verification := classifyRemoteVerification(row, remote, err, requireHash)
	if verification == remoteVerificationMatch {
		m.completeRemoteVerification(row, remote, []string{StateVerifying})
		return
	}

	if verification == remoteVerificationInconclusive {
		next := time.Now().Add(remoteVerificationInconclusiveDelay())
		msg := "remote verification is inconclusive; keeping canonical generation without reupload"
		if err != nil {
			msg = fmt.Sprintf("remote verification is inconclusive: %v", err)
		} else if remote != nil && requireHash && remote.GetSize() == row.Size &&
			remote.GetHash().GetHash(utils.SHA1) == "" {
			msg = "remote size matches but required 115 SHA-1 is unavailable; keeping canonical generation without reupload"
		}
		_ = db.GetDb().Model(&model.WebDAVWritebackObject{}).
			Where("id = ? AND generation = ? AND state = ?", row.ID, row.Generation, StateVerifying).
			Updates(map[string]any{
				"state":        StateVerifying,
				"retry_at":     &next,
				"verify_count": row.VerifyCount,
				"last_error":   msg,
			}).Error
		return
	}

	attempts := verificationAttemptsFor(row, requireHash)
	nextCount, retryUpload := advanceRemoteVerification(verification, row.VerifyCount, attempts)
	if retryUpload {
		if suppressRepeatedLargeProviderRepair(row, requireHash) {
			next := time.Now().Add(repeatedLargeProviderVerifyDelay(row))
			_ = db.GetDb().Model(&model.WebDAVWritebackObject{}).
				Where("id = ? AND generation = ? AND state = ?", row.ID, row.Generation, StateVerifying).
				Updates(map[string]any{
					"state":        StateVerifying,
					"retry_at":     &next,
					"verify_count": max(0, attempts-1),
					"last_error":   "115 multipart upload remains divergent after one repair upload; preserving durable spool and continuing low-frequency verification without another automatic reupload",
				}).Error
			return
		}
		next := time.Now().Add(retryDelay(row.RetryCount + 1))
		_ = db.GetDb().Model(&model.WebDAVWritebackObject{}).
			Where("id = ? AND generation = ? AND state = ?", row.ID, row.Generation, StateVerifying).
			Updates(map[string]any{
				"state":        StateQueued,
				"retry_at":     &next,
				"retry_count":  row.RetryCount + 1,
				"verify_count": 0,
				"last_error":   "fresh provider evidence stayed divergent through the verification window",
			}).Error
		return
	}

	interval := time.Duration(max(1, conf.Conf.WebDAVWriteback.VerifyIntervalSeconds)) * time.Second
	next := time.Now().Add(interval)
	msg := "remote object is absent from the refreshed provider listing"
	if remote != nil {
		remoteSHA1 := remote.GetHash().GetHash(utils.SHA1)
		expectedSHA1 := canonicalContentSHA1(row)
		if remote.GetSize() != row.Size {
			msg = fmt.Sprintf("remote size %d does not match canonical size %d", remote.GetSize(), row.Size)
		} else if expectedSHA1 != "" && remoteSHA1 != "" && !strings.EqualFold(remoteSHA1, expectedSHA1) {
			msg = fmt.Sprintf("remote sha1 %s does not match canonical sha1 %s", remoteSHA1, expectedSHA1)
		} else {
			msg = "refreshed provider metadata is conclusively divergent from canonical content"
		}
	}
	_ = db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("id = ? AND generation = ? AND state = ?", row.ID, row.Generation, StateVerifying).
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
