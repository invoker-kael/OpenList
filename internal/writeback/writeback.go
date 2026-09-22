package writeback

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime/debug"
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
	StateQueued                   = "queued"
	StateUploading                = "uploading"
	StateVerifying                = "verifying"
	StateCompleted                = "completed"
	StateDeleted                  = "deleted"
	StateWaitingCloudSyncReupload = "waiting_cloudsync_reupload"
	StateWaitingRepair            = "waiting_repair"
	StateLockNull                 = "lock_null"

	ResolutionRemoteHashMismatch      = "remote_hash_mismatch"
	ResolutionRemoteMissing           = "remote_missing"
	ResolutionVerificationExhausted   = "verification_exhausted"
	ResolutionNeedsCloudSyncRehydrate = "needs_cloudsync_rehydrate"

	CanonicalStateAcked     = "durable_acked"
	canonicalStateLegacyAck = "acked"
	CanonicalStateDeleted   = "deleted"
	CanonicalStateLockNull  = "lock_null"

	// Read only for upgrade compatibility. Runtime failures remain queued and
	// are represented by retry_count, retry_at and last_error.
	legacyStateFailed = "failed"

	receiveLeaseDuration          = time.Minute
	receiveHeartbeatEvery         = 10 * time.Second
	receiveProgressHeartbeatEvery = time.Second
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

func canonicalState(row *model.WebDAVWritebackObject) string {
	if row == nil {
		return ""
	}
	if row.CanonicalState != "" {
		if row.CanonicalState == canonicalStateLegacyAck {
			return CanonicalStateAcked
		}
		return row.CanonicalState
	}
	// Rows created before the ACK-state split used State for both concerns.
	// Treat every non-delete/non-lock row as an already accepted canonical
	// object so upgrades do not make existing Cloud Sync files disappear.
	switch row.State {
	case StateDeleted:
		return CanonicalStateDeleted
	case StateLockNull:
		return CanonicalStateLockNull
	default:
		return CanonicalStateAcked
	}
}

func canonicalDeleted(row *model.WebDAVWritebackObject) bool {
	return canonicalState(row) == CanonicalStateDeleted
}

func canonicalAcked(row *model.WebDAVWritebackObject) bool {
	return canonicalState(row) == CanonicalStateAcked
}

func waitingCloudSyncReupload(row *model.WebDAVWritebackObject) bool {
	return row != nil &&
		(row.CloudSyncReuploadRequired ||
			row.State == StateWaitingCloudSyncReupload ||
			row.ResolutionReason == ResolutionNeedsCloudSyncRehydrate)
}

func legacyExhaustedRetryNeedsFreshVerification(row *model.WebDAVWritebackObject) bool {
	if row == nil ||
		row.IsDir ||
		row.CleanupPath != "" ||
		row.RetryCount <= 1 {
		return false
	}
	switch row.State {
	case StateQueued, legacyStateFailed, StateUploading, StateVerifying:
	default:
		return false
	}

	switch row.ResolutionReason {
	case ResolutionRemoteHashMismatch, ResolutionRemoteMissing, ResolutionVerificationExhausted:
		return true
	}
	if row.ProviderUploadCompletedAt != nil ||
		row.VerifyCount > 0 ||
		row.RemoteObjectID != "" ||
		row.RemoteSHA1 != "" ||
		row.ProviderEvidenceCount > 0 {
		return true
	}

	msg := strings.ToLower(row.LastError)
	for _, marker := range []string{
		"fresh provider evidence stayed divergent",
		"verification window",
		"provider hash mismatch",
		"remote sha1",
		"remote object is still absent",
		"automatic repair budget",
		"multipart upload remains divergent",
	} {
		if strings.Contains(msg, marker) {
			return true
		}
	}
	return false
}

func markCanonicalAcked(row *model.WebDAVWritebackObject, durableAt time.Time) {
	if row == nil {
		return
	}
	row.CanonicalState = CanonicalStateAcked
	if !durableAt.IsZero() {
		stamp := durableAt
		row.AckTime = &stamp
		row.DurableAt = &stamp
	}
}

func markCanonicalDeleted(row *model.WebDAVWritebackObject) {
	if row != nil {
		row.CanonicalState = CanonicalStateDeleted
	}
}

func markCanonicalLockNull(row *model.WebDAVWritebackObject) {
	if row != nil {
		row.CanonicalState = CanonicalStateLockNull
		row.DurableAt = nil
	}
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

func nextReceiveGeneration(current, receiveSequence uint64) uint64 {
	next := receiveSequence
	if next == 0 {
		next = 1
	}
	if current >= next {
		return current + 1
	}
	return next
}

func emptyPayloadSHA1() string {
	hasher := utils.SHA1.NewFunc()
	return hex.EncodeToString(hasher.Sum(nil))
}

func clearRemoteVerification(row *model.WebDAVWritebackObject) {
	if row == nil {
		return
	}
	row.RemoteObjectID = ""
	row.RemoteSHA1 = ""
	row.RemoteGeneration = 0
	row.RemoteVerifiedAt = nil
	row.ProviderUploadStartedAt = nil
	row.ProviderUploadCompletedAt = nil
	row.ProviderEvidenceFirstAt = nil
	row.ProviderEvidenceLastAt = nil
	row.ProviderEvidenceCount = 0
	row.ProviderEvidenceResult = ""
}

func canonicalContentSHA1(row *model.WebDAVWritebackObject) string {
	if row == nil {
		return ""
	}
	if row.PayloadSHA1 != "" {
		return row.PayloadSHA1
	}
	if row.RemoteGeneration == row.Generation && row.RemoteVerifiedAt != nil {
		return row.RemoteSHA1
	}
	return ""
}

type durablePayloadAvailability uint8

const (
	durablePayloadUnavailable durablePayloadAvailability = iota
	durablePayloadAvailable
	durablePayloadMissing
)

func inspectDurableLocalPayload(row *model.WebDAVWritebackObject) (durablePayloadAvailability, error) {
	if row == nil || row.IsDir || canonicalDeleted(row) {
		return durablePayloadUnavailable, nil
	}
	if row.SpoolPath == "" {
		if row.Size == 0 && canonicalAcked(row) {
			return durablePayloadAvailable, nil
		}
		return durablePayloadMissing, nil
	}
	_, err := os.Stat(row.SpoolPath)
	if err == nil {
		return durablePayloadAvailable, nil
	}
	if os.IsNotExist(err) {
		return durablePayloadMissing, nil
	}
	return durablePayloadUnavailable, err
}

func durableLocalPayloadAvailable(row *model.WebDAVWritebackObject) bool {
	availability, err := inspectDurableLocalPayload(row)
	return err == nil && availability == durablePayloadAvailable
}

func canCoalesceDuplicatePut(row *model.WebDAVWritebackObject, size int64, payloadSHA1 string) bool {
	return row != nil &&
		!row.IsDir &&
		!canonicalDeleted(row) &&
		!waitingCloudSyncReupload(row) &&
		(row.SpoolPath != "" || row.Size == 0) &&
		row.Size == size &&
		row.PayloadSHA1 != "" &&
		payloadSHA1 != "" &&
		strings.EqualFold(row.PayloadSHA1, payloadSHA1)
}

func canCoalesceCompletedDuplicatePut(row *model.WebDAVWritebackObject, size int64, payloadSHA1 string) bool {
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
	ErrReceiveInProgress         = errors.New("write-back receive already in progress")
)

func ReceiveRetrySeconds() int {
	return 2
}

type SpoolCapacityError struct {
	Free         uint64
	Required     uint64
	Backlog      uint64
	BacklogLimit uint64
}

func (e *SpoolCapacityError) Error() string {
	if e == nil {
		return ErrSpoolCapacity.Error()
	}
	if e.BacklogLimit > 0 {
		return fmt.Sprintf("%s: pending_backlog=%d backlog_limit=%d", ErrSpoolCapacity, e.Backlog, e.BacklogLimit)
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

func activeProviderOperationQuery(now time.Time) *gorm.DB {
	preparedCutoff := now.Add(-providerOperationPreparedAbandonAfter)
	return db.GetDb().
		Where("state IN ?", []string{
			ProviderOperationPrepared,
			ProviderOperationStarted,
			ProviderOperationFailed,
			ProviderOperationApplied,
		}).
		Where("state <> ? OR updated_at > ?", ProviderOperationPrepared, preparedCutoff)
}

const providerOperationRoutingColumns = "operation_key, method, source_path, destination_path, depth, state, created_at, updated_at"

func activeProviderOperations() ([]model.WebDAVProviderOperation, error) {
	var ops []model.WebDAVProviderOperation
	// Protection checks only need routing/fence fields. Filter inactive and
	// abandoned intents in SQL and avoid loading recovery snapshots/TEXT errors.
	if err := activeProviderOperationQuery(time.Now()).
		Select(providerOperationRoutingColumns).
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
	if err := activeProviderOperationQuery(time.Now()).
		Select(providerOperationRoutingColumns).
		Order("updated_at asc").
		Find(&ops).Error; err != nil {
		return nil, err
	}
	for i := range ops {
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
	if err := activeProviderOperationQuery(time.Now()).
		Select(providerOperationRoutingColumns).
		Order("updated_at asc").
		Find(&ops).Error; err != nil {
		return nil, err
	}
	for i := range ops {
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
	overlaid, hasWriteback, canonicalParent, overlayErr := OverlayListState(ctx, current, objs, remoteReliable)
	if overlayErr != nil {
		return nil, overlayErr
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
	err := db.GetDb().Where("operation_key = ?", providerOperationKey(method, src, dst, depth)).Take(&op).Error
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
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Take(&locked, op.ID).Error; err != nil {
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
		return current != nil && !canonicalDeleted(current), nil
	}
	if current == nil {
		return true, nil
	}
	return current.Generation != op.SourceGeneration || canonicalDeleted(current), nil
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
			Where("path_key = ?", pathKey(op.DestinationPath)).Take(&dst).Error
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

func removeSpoolsIfUnreferenced(spoolPaths []string) {
	if len(spoolPaths) == 0 {
		return
	}
	unique := make([]string, 0, len(spoolPaths))
	seen := make(map[string]struct{}, len(spoolPaths))
	for _, spoolPath := range spoolPaths {
		if spoolPath == "" {
			continue
		}
		spoolPath = filepath.Clean(spoolPath)
		if _, ok := seen[spoolPath]; ok {
			continue
		}
		seen[spoolPath] = struct{}{}
		unique = append(unique, spoolPath)
	}
	if len(unique) == 0 {
		return
	}
	var referenced []string
	if err := db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Distinct("spool_path").
		Where("spool_path IN ?", unique).
		Pluck("spool_path", &referenced).Error; err != nil {
		return
	}
	for _, spoolPath := range unreferencedSpoolPaths(unique, referenced) {
		if !spoolIsActive(spoolPath) {
			_ = os.Remove(spoolPath)
		}
	}
}

func removeSpoolIfUnreferenced(spoolPath string) {
	removeSpoolsIfUnreferenced([]string{spoolPath})
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

const canonicalReadColumns = "id, path_key, parent_key, path, name, is_dir, size, mod_time, create_time, e_tag, payload_sha1, canonical_state, state"

func getByPath(p string) (*model.WebDAVWritebackObject, error) {
	if !Enabled() {
		return nil, nil
	}
	var row model.WebDAVWritebackObject
	err := db.GetDb().Where("path_key = ?", pathKey(p)).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	return &row, err
}

func getCanonicalByPath(p string) (*model.WebDAVWritebackObject, error) {
	if !Enabled() {
		return nil, nil
	}
	var row model.WebDAVWritebackObject
	err := db.GetDb().
		Select(canonicalReadColumns).
		Where("path_key = ?", pathKey(p)).
		Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	return &row, err
}

// Canonical returns the stable WebDAV object for a path. found remains true for
// tombstones so callers can hide a remotely-stale object while delete is pending.
func Canonical(p string) (obj model.Obj, found bool, deleted bool, err error) {
	row, err := getCanonicalByPath(p)
	if err != nil || row == nil {
		return nil, false, false, err
	}
	if canonicalDeleted(row) {
		return nil, true, true, nil
	}
	return toObject(row), true, false, nil
}

type CanonicalSnapshot struct {
	Object  model.Obj
	Found   bool
	Deleted bool
}

func CanonicalBatch(paths ...string) (map[string]CanonicalSnapshot, error) {
	snapshots := make(map[string]CanonicalSnapshot, len(paths))
	if !Enabled() || len(paths) == 0 {
		return snapshots, nil
	}

	pathByKey := make(map[string]string, len(paths))
	keys := make([]string, 0, len(paths))
	for _, p := range paths {
		clean := utils.FixAndCleanPath(p)
		key := pathKey(clean)
		if _, exists := pathByKey[key]; exists {
			continue
		}
		pathByKey[key] = clean
		keys = append(keys, key)
		snapshots[clean] = CanonicalSnapshot{}
	}

	var rows []model.WebDAVWritebackObject
	if err := db.GetDb().
		Select(canonicalReadColumns).
		Where("path_key IN ?", keys).
		Find(&rows).Error; err != nil {
		return nil, err
	}
	for i := range rows {
		row := &rows[i]
		clean, ok := pathByKey[row.PathKey]
		if !ok {
			continue
		}
		snapshot := CanonicalSnapshot{Found: true}
		if canonicalDeleted(row) {
			snapshot.Deleted = true
		} else {
			snapshot.Object = toObject(row)
		}
		snapshots[clean] = snapshot
	}
	return snapshots, nil
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
		findErr := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("path_key = ?", key).Take(&row).Error
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
			if !canonicalDeleted(&row) {
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
		markCanonicalLockNull(&row)
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
		if requireHash {
			return remoteContentInconclusive
		}
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
	now := time.Now()
	if ops, err := activeProviderOperations(); err != nil {
		return false, err
	} else if providerOperationProtectsCanonicalPath(ops, row.Path, now) {
		return false, nil
	}
	if row.IsDir {
		res := db.GetDb().
			Where("id = ? AND generation = ? AND state = ? AND spool_path = ''", row.ID, row.Generation, StateCompleted).
			Delete(&model.WebDAVWritebackObject{})
		return res.RowsAffected > 0, res.Error
	}

	resolution := row.ResolutionReason
	result := HistoryResultRecoveryRequired
	evidenceResult := "divergent"
	reason := row.LastError
	if resolution == ResolutionRemoteMissing {
		result = HistoryResultRemoteMissing
		evidenceResult = ResolutionRemoteMissing
		if reason == "" {
			reason = "confirmed remote provider object is missing; Cloud Sync restart/re-upload is required"
		}
	} else {
		resolution = ResolutionNeedsCloudSyncRehydrate
		if reason == "" {
			reason = "confirmed remote provider divergence cannot be repaired without local payload; Cloud Sync restart/re-upload is required"
		}
	}
	recoveryStartedAt := row.RecoveryStartedAt
	if recoveryStartedAt == nil {
		recoveryStartedAt = &now
	}
	updates := map[string]any{
		"canonical_state":              CanonicalStateAcked,
		"state":                        StateWaitingCloudSyncReupload,
		"cloud_sync_reupload_required": true,
		"recovery_started_at":          recoveryStartedAt,
		"retry_at":                     nil,
		"last_error":                   reason,
		"resolution_reason":            resolution,
		"completed_at":                 nil,
	}
	updates = mergeUpdateMaps(updates, applyProviderEvidence(row, now, evidenceResult))
	res := db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("id = ? AND generation = ? AND state = ? AND spool_path = ''", row.ID, row.Generation, StateCompleted).
		Updates(updates)
	if res.Error != nil || res.RowsAffected == 0 {
		return false, res.Error
	}

	historyRow := *row
	historyRow.CanonicalState = CanonicalStateAcked
	historyRow.State = StateWaitingCloudSyncReupload
	historyRow.CloudSyncReuploadRequired = true
	historyRow.RecoveryStartedAt = recoveryStartedAt
	historyRow.CompletedAt = nil
	historyRow.ResolutionReason = resolution
	historyRow.LastError = reason
	recordHistoryOutcomeBestEffort(
		&historyRow,
		result,
		StateWaitingCloudSyncReupload,
		HistoryRecoveryCloudSyncRehydrateRequired,
		now,
		reason,
	)
	return true, nil
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
		return tx.Model(&model.WebDAVWritebackObject{}).
			Where("id = ? AND generation = ? AND state = ?", current.ID, current.Generation, StateCompleted).
			Updates(map[string]any{
				"verify_count": nextCount,
				"retry_at":     nextRetryAt,
				"last_error":   "",
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
	// Upload verification is intentionally frequent so a new generation can
	// converge quickly. Once that generation is COMPLETED and its local spool
	// has been released, direct Cloud Sync scans should not turn the same short
	// cadence into continuous provider health checks. Keep healthy completed
	// probes on a separate, longer cooldown while never probing more frequently
	// than the upload verification interval.
	seconds := 30 * 60
	if conf.Conf != nil {
		if configured := conf.Conf.WebDAVWriteback.CompletedRemoteProbeSeconds; configured > 0 {
			seconds = configured
		}
		seconds = max(seconds, max(1, conf.Conf.WebDAVWriteback.VerifyIntervalSeconds))
	}
	return time.Duration(seconds) * time.Second
}

func remoteVerificationEvidenceConsistent(row *model.WebDAVWritebackObject) bool {
	if row == nil || row.RemoteVerifiedAt == nil || row.RemoteGeneration != row.Generation {
		return false
	}
	if row.PayloadSHA1 != "" && row.RemoteSHA1 != "" && !strings.EqualFold(row.PayloadSHA1, row.RemoteSHA1) {
		return row.ResolutionReason == ResolutionRemoteHashMismatch
	}
	return true
}

func completedRemoteVerificationFresh(row *model.WebDAVWritebackObject, now time.Time) bool {
	if row == nil ||
		row.IsDir ||
		row.State != StateCompleted ||
		row.SpoolPath != "" ||
		!remoteVerificationEvidenceConsistent(row) ||
		row.VerifyCount != 0 ||
		row.RetryAt != nil ||
		row.LastError != "" {
		return false
	}
	return now.Before(row.RemoteVerifiedAt.Add(completedRemoteVerificationInterval()))
}

func refreshCompletedRemoteVerification(row *model.WebDAVWritebackObject, remote model.Obj, now time.Time, requireHash bool) error {
	if row == nil || remote == nil || row.IsDir || row.State != StateCompleted || row.SpoolPath != "" {
		return nil
	}
	evidence, matched := matchedRemoteVerificationEvidence(row, remote, now, requireHash)
	if !matched {
		return nil
	}
	return db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("id = ? AND generation = ? AND state = ? AND spool_path = ''", row.ID, row.Generation, StateCompleted).
		Updates(map[string]any{
			"verify_count":       0,
			"retry_at":           nil,
			"last_error":         "",
			"resolution_reason":  "",
			"remote_object_id":   evidence.objectID,
			"remote_sha1":        evidence.sha1,
			"remote_generation":  evidence.generation,
			"remote_verified_at": &evidence.verifiedAt,
		}).Error
}

const completedSiblingProbeBatchLimit = 128

func matchingCompletedSiblingRows(rows []model.WebDAVWritebackObject, remotes []model.Obj, skipID uint, requireHash bool) []verificationBatchMatch {
	if len(rows) == 0 || len(remotes) == 0 {
		return nil
	}
	byName := make(map[string]model.Obj, len(remotes))
	for _, remote := range remotes {
		if remote != nil {
			byName[remote.GetName()] = remote
		}
	}
	matches := make([]verificationBatchMatch, 0, min(len(rows), len(remotes)))
	for i := range rows {
		row := &rows[i]
		if row.ID == skipID || row.IsDir || row.State != StateCompleted || row.SpoolPath != "" {
			continue
		}
		remote := byName[row.Name]
		if remoteMatchesCanonical(row, remote, requireHash) {
			matches = append(matches, verificationBatchMatch{row: *row, remote: remote})
		}
	}
	return matches
}

func refreshMatchingCompletedSiblings(trigger *model.WebDAVWritebackObject, remotes []model.Obj, requireHash bool, now time.Time) {
	if trigger == nil || !requireHash || len(remotes) == 0 {
		return
	}
	cutoff := now.Add(-completedRemoteVerificationInterval())
	var rows []model.WebDAVWritebackObject
	if err := db.GetDb().
		Select("id", "name", "is_dir", "size", "payload_sha1", "remote_sha1", "remote_generation", "remote_verified_at", "generation", "spool_path", "state", "verify_count", "retry_at", "last_error").
		Where("parent_key = ? AND state = ? AND is_dir = ? AND spool_path = '' AND id <> ?",
			pathKey(trigger.Parent), StateCompleted, false, trigger.ID).
		Where("(remote_verified_at IS NULL OR remote_verified_at <= ? OR remote_generation <> generation OR verify_count <> 0 OR retry_at IS NOT NULL OR last_error <> '')", cutoff).
		Order("remote_verified_at asc").
		Order("id asc").
		Limit(completedSiblingProbeBatchLimit).
		Find(&rows).Error; err != nil {
		log.Errorf("write-back completed sibling probe scan failed for %s: %v", trigger.Parent, err)
		return
	}
	matches := matchingCompletedSiblingRows(rows, remotes, trigger.ID, requireHash)
	if len(matches) == 0 {
		return
	}
	if err := refreshCompletedVerificationEvidenceBatch(matches, now, false); err != nil {
		log.Errorf("write-back completed sibling probe batch refresh failed for %s: %v", trigger.Parent, err)
	}
}

const freshParentCompletedEvidenceBatchLimit = 64

func freshCompletedEvidenceEligible(row *model.WebDAVWritebackObject) bool {
	return row != nil &&
		!row.IsDir &&
		row.State == StateCompleted &&
		row.SpoolPath == "" &&
		row.PayloadSHA1 != "" &&
		row.VerifyCount == 0 &&
		row.RetryAt == nil &&
		row.LastError == ""
}

func matchingFreshCompletedEvidenceRows(rows []model.WebDAVWritebackObject, remotes []model.Obj, now time.Time) []verificationBatchMatch {
	if len(rows) == 0 || len(remotes) == 0 {
		return nil
	}
	byName := make(map[string]model.Obj, len(remotes))
	for _, remote := range remotes {
		if remote != nil {
			byName[remote.GetName()] = remote
		}
	}
	matches := make([]verificationBatchMatch, 0, min(len(rows), len(remotes)))
	for i := range rows {
		row := &rows[i]
		if !freshCompletedEvidenceEligible(row) {
			continue
		}
		remote := byName[row.Name]
		if _, matched := matchedRemoteVerificationEvidence(row, remote, now, true); matched {
			matches = append(matches, verificationBatchMatch{row: *row, remote: remote})
		}
	}
	return matches
}

func refreshCompletedVerificationEvidenceBatch(matches []verificationBatchMatch, now time.Time, healthyOnly bool) error {
	if len(matches) == 0 {
		return nil
	}

	ids := make([]uint, 0, len(matches))
	generationArgs := make([]any, 0, len(matches)*2)
	objectIDArgs := make([]any, 0, len(matches)*2)
	sha1Args := make([]any, 0, len(matches)*2)
	var generationCase strings.Builder
	var objectIDCase strings.Builder
	var sha1Case strings.Builder
	generationCase.WriteString("CASE id")
	objectIDCase.WriteString("CASE id")
	sha1Case.WriteString("CASE id")

	for i := range matches {
		match := &matches[i]
		evidence, ok := matchedRemoteVerificationEvidence(&match.row, match.remote, now, true)
		if !ok {
			continue
		}
		ids = append(ids, match.row.ID)
		generationCase.WriteString(" WHEN ? THEN ?")
		generationArgs = append(generationArgs, match.row.ID, match.row.Generation)
		objectIDCase.WriteString(" WHEN ? THEN ?")
		objectIDArgs = append(objectIDArgs, match.row.ID, evidence.objectID)
		sha1Case.WriteString(" WHEN ? THEN ?")
		sha1Args = append(sha1Args, match.row.ID, evidence.sha1)
	}
	if len(ids) == 0 {
		return nil
	}

	generationCase.WriteString(" ELSE 0 END")
	objectIDCase.WriteString(" ELSE remote_object_id END")
	sha1Case.WriteString(" ELSE remote_sha1 END")

	query := db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("id IN ?", ids).
		Where("generation = "+generationCase.String(), generationArgs...).
		Where("state = ? AND spool_path = ''", StateCompleted)
	if healthyOnly {
		query = query.
			Where("payload_sha1 <> ''").
			Where("verify_count = 0 AND retry_at IS NULL AND last_error = ''").
			Where("(canonical_state = ? OR canonical_state = '' OR canonical_state IS NULL)", CanonicalStateAcked)
	}

	return query.Updates(map[string]any{
		"verify_count":       0,
		"retry_at":           nil,
		"last_error":         "",
		"remote_object_id":   gorm.Expr(objectIDCase.String(), objectIDArgs...),
		"remote_sha1":        gorm.Expr(sha1Case.String(), sha1Args...),
		"remote_generation":  gorm.Expr("generation"),
		"remote_verified_at": &now,
	}).Error
}

func refreshFreshParentCompletedEvidence(parent string, remotes []model.Obj, now time.Time) {
	if len(remotes) == 0 {
		return
	}
	parent = utils.FixAndCleanPath(parent)
	if !providerRequiresPayloadHash(path.Join(parent, ".writeback-evidence-probe")) {
		return
	}

	cutoff := now.Add(-completedRemoteVerificationInterval())
	var rows []model.WebDAVWritebackObject
	if err := db.GetDb().
		Select("id", "name", "size", "payload_sha1", "generation", "state", "spool_path", "verify_count", "retry_at", "last_error").
		Where("parent_key = ? AND state = ? AND is_dir = ? AND spool_path = ''", pathKey(parent), StateCompleted, false).
		Where("verify_count = 0 AND retry_at IS NULL AND last_error = '' AND payload_sha1 <> ''").
		Where("(canonical_state = ? OR canonical_state = '' OR canonical_state IS NULL)", CanonicalStateAcked).
		Where("(remote_verified_at IS NULL OR remote_verified_at <= ? OR remote_generation <> generation OR remote_sha1 = '' OR LOWER(remote_sha1) <> LOWER(payload_sha1))", cutoff).
		Order("remote_verified_at asc").
		Order("id asc").
		Limit(freshParentCompletedEvidenceBatchLimit).
		Find(&rows).Error; err != nil {
		log.Errorf("write-back fresh parent completed evidence scan failed for %s: %v", parent, err)
		return
	}
	matches := matchingFreshCompletedEvidenceRows(rows, remotes, now)
	if len(matches) == 0 {
		return
	}
	if err := refreshCompletedVerificationEvidenceBatch(matches, now, true); err != nil {
		log.Errorf("write-back fresh parent completed evidence refresh failed for %s: %v", parent, err)
	}
}

func settleCompletedRemoteHashMismatch(row *model.WebDAVWritebackObject, remote model.Obj, now time.Time) error {
	if row == nil || remote == nil || row.IsDir || row.State != StateCompleted || row.SpoolPath != "" ||
		!remoteHashMismatch(row, remote, true) {
		return nil
	}
	evidence := captureRemoteVerification(row, remote, now)
	if evidence.objectID == "" {
		return nil
	}
	recoveryStartedAt := row.RecoveryStartedAt
	if recoveryStartedAt == nil {
		recoveryStartedAt = &now
	}
	updates := map[string]any{
		"canonical_state":              CanonicalStateAcked,
		"state":                        StateWaitingCloudSyncReupload,
		"cloud_sync_reupload_required": true,
		"recovery_started_at":          recoveryStartedAt,
		"retry_at":                     nil,
		"last_error":                   remoteHashMismatchMessage(row, remote),
		"resolution_reason":            ResolutionRemoteHashMismatch,
		"completed_at":                 nil,
		"remote_object_id":             evidence.objectID,
		"remote_sha1":                  evidence.sha1,
		"remote_generation":            0,
		"remote_verified_at":           nil,
		"restart_upload_recovery":      false,
	}
	updates = mergeUpdateMaps(updates, applyProviderEvidence(row, now, ResolutionRemoteHashMismatch))
	if err := db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("id = ? AND generation = ? AND state = ? AND spool_path = ''", row.ID, row.Generation, StateCompleted).
		Updates(updates).Error; err != nil {
		return err
	}
	historyRow := *row
	historyRow.CanonicalState = CanonicalStateAcked
	historyRow.State = StateWaitingCloudSyncReupload
	historyRow.CloudSyncReuploadRequired = true
	historyRow.RecoveryStartedAt = recoveryStartedAt
	historyRow.ResolutionReason = ResolutionRemoteHashMismatch
	historyRow.RemoteObjectID = evidence.objectID
	historyRow.RemoteSHA1 = evidence.sha1
	historyRow.RemoteGeneration = 0
	historyRow.RemoteVerifiedAt = nil
	historyRow.LastError = remoteHashMismatchMessage(row, remote)
	recordHistoryOutcomeBestEffort(
		&historyRow,
		HistoryResultRecoveryRequired,
		StateWaitingCloudSyncReupload,
		HistoryRecoveryCloudSyncRehydrateRequired,
		now,
		historyRow.LastError,
	)
	return nil
}
func confirmCompletedRemoteMissing(ctx context.Context, row *model.WebDAVWritebackObject) (bool, model.Obj, error) {
	if row == nil || row.IsDir {
		return false, nil, nil
	}
	remote, err := fs.Get(ctx, row.Path, &fs.GetArgs{NoLog: true})
	if err == nil && remote != nil {
		return false, remote, nil
	}
	if err != nil && !errs.IsObjectNotFound(err) {
		return false, nil, err
	}
	return true, nil, nil
}

const completedRemoteDivergenceConfirmations = 3

func completedRemoteEvidenceKind(row *model.WebDAVWritebackObject, remote model.Obj) string {
	if remote == nil {
		return ResolutionRemoteMissing
	}
	if remoteHashMismatch(row, remote, true) {
		return ResolutionRemoteHashMismatch
	}
	return "divergent"
}

func completedRemoteEvidenceConsistent(row *model.WebDAVWritebackObject, remote model.Obj, kind string) bool {
	if row == nil ||
		row.VerifyCount <= 0 ||
		row.ProviderEvidenceResult != kind ||
		row.RemoteGeneration != 0 ||
		row.RemoteVerifiedAt != nil {
		return false
	}
	if kind == ResolutionRemoteMissing {
		return remote == nil && row.RemoteObjectID == "" && row.RemoteSHA1 == ""
	}
	if remote == nil {
		return false
	}

	remoteID := remote.GetID()
	if row.RemoteObjectID == "" || remoteID == "" || row.RemoteObjectID != remoteID {
		return false
	}
	remoteSHA1 := strings.ToLower(remote.GetHash().GetHash(utils.SHA1))
	if kind == ResolutionRemoteHashMismatch {
		return row.RemoteSHA1 != "" &&
			remoteSHA1 != "" &&
			strings.EqualFold(row.RemoteSHA1, remoteSHA1)
	}
	if row.RemoteSHA1 == "" && remoteSHA1 == "" {
		return true
	}
	return row.RemoteSHA1 != "" &&
		remoteSHA1 != "" &&
		strings.EqualFold(row.RemoteSHA1, remoteSHA1)
}

func nextCompletedRemoteEvidenceCount(row *model.WebDAVWritebackObject, remote model.Obj, kind string) (int, bool) {
	if !completedRemoteEvidenceConsistent(row, remote, kind) {
		return 1, false
	}
	return min(max(0, row.VerifyCount)+1, completedRemoteDivergenceConfirmations), true
}

func completedRemoteEvidenceMessage(row *model.WebDAVWritebackObject, remote model.Obj, kind string) string {
	switch kind {
	case ResolutionRemoteMissing:
		return "remote object is absent from the refreshed provider listing; awaiting consistent confirmation"
	case ResolutionRemoteHashMismatch:
		return remoteHashMismatchMessage(row, remote)
	default:
		if row != nil && remote != nil && remote.GetSize() != row.Size {
			return fmt.Sprintf("remote size %d does not match canonical size %d; awaiting consistent confirmation", remote.GetSize(), row.Size)
		}
		return "refreshed provider metadata is divergent from canonical content; awaiting consistent confirmation"
	}
}

func recordCompletedRemoteEvidence(row *model.WebDAVWritebackObject, remote model.Obj, kind string, now time.Time) (int, error) {
	if row == nil || row.IsDir || row.State != StateCompleted || row.SpoolPath != "" {
		return 0, nil
	}
	nextCount, _ := nextCompletedRemoteEvidenceCount(row, remote, kind)
	evidence := captureRemoteVerification(row, remote, now)
	nextRetry := now.Add(completedDivergenceConfirmationDelay())
	updates := map[string]any{
		"verify_count":       nextCount,
		"retry_at":           &nextRetry,
		"last_error":         completedRemoteEvidenceMessage(row, remote, kind),
		"resolution_reason":  "",
		"remote_object_id":   evidence.objectID,
		"remote_sha1":        evidence.sha1,
		"remote_generation":  0,
		"remote_verified_at": nil,
	}
	updates = mergeUpdateMaps(updates, applyProviderEvidence(row, now, kind))
	res := db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("id = ? AND generation = ? AND state = ? AND spool_path = ''", row.ID, row.Generation, StateCompleted).
		Updates(updates)
	if res.Error != nil {
		return 0, res.Error
	}
	if res.RowsAffected == 0 {
		return 0, nil
	}
	return nextCount, nil
}

func claimCompletedRemoteEvidenceProbe(row *model.WebDAVWritebackObject, now time.Time) (bool, error) {
	if row == nil || row.IsDir || row.State != StateCompleted || row.SpoolPath != "" {
		return false, nil
	}
	ops, err := activeProviderOperations()
	if err != nil {
		return false, err
	}
	if providerOperationProtectsCanonicalPath(ops, row.Path, now) {
		return false, nil
	}

	next := now.Add(completedDivergenceConfirmationDelay())
	res := db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("id = ? AND generation = ? AND state = ? AND spool_path = ''", row.ID, row.Generation, StateCompleted).
		Where("(retry_at IS NULL OR retry_at <= ?)", now).
		Update("retry_at", &next)
	return res.RowsAffected > 0, res.Error
}

func reconcileCompletedHashProvider(ctx context.Context, row *model.WebDAVWritebackObject, now time.Time) (bool, error) {
	if row == nil || row.IsDir {
		return false, nil
	}

	claimed, claimErr := claimCompletedRemoteEvidenceProbe(row, now)
	if claimErr != nil || !claimed {
		return false, claimErr
	}

	// One fresh parent listing is one evidence observation. The retry_at claim
	// above is only a cross-instance throttle; it deliberately does not alter
	// verify_count or provider evidence.
	objs, listErr := providerParentSnapshots.do(ctx.Done(), row.Parent, func() ([]model.Obj, error) {
		fresh, listErr := fs.List(ctx, row.Parent, &fs.ListArgs{Refresh: true, NoLog: true})
		if listErr == nil {
			refreshFreshParentCompletedEvidence(row.Parent, fresh, time.Now())
		}
		return fresh, listErr
	})
	if listErr != nil {
		// Provider/network failures are not evidence of remote loss. retry_at
		// already throttles the next probe without creating a fake observation.
		return false, nil
	}
	remote := exactRemoteByName(objs, row.Name)
	switch compareRemoteContent(row, remote, true) {
	case remoteContentMatch:
		if err := refreshCompletedRemoteVerification(row, remote, now, true); err != nil {
			return false, err
		}
		refreshMatchingCompletedSiblings(row, objs, true, now)
		return false, nil
	case remoteContentInconclusive:
		// Missing provider SHA-1 is not evidence. Keep the existing streak and
		// wait for the next throttled fresh observation.
		return false, nil
	}

	kind := completedRemoteEvidenceKind(row, remote)
	nextCount, consistent := nextCompletedRemoteEvidenceCount(row, remote, kind)
	if !consistent || nextCount < completedRemoteDivergenceConfirmations {
		_, recordErr := recordCompletedRemoteEvidence(row, remote, kind, now)
		return false, recordErr
	}

	if kind == ResolutionRemoteHashMismatch {
		return false, settleCompletedRemoteHashMismatch(row, remote, now)
	}

	if kind == ResolutionRemoteMissing {
		// Only pay for a per-file provider lookup after three consistent fresh
		// parent-listing misses. This keeps the common path at one LIST and
		// reserves GET for the final missing-object confirmation.
		missing, direct, directErr := confirmCompletedRemoteMissing(ctx, row)
		if directErr != nil {
			_, recordErr := recordCompletedRemoteEvidence(row, remote, kind, now)
			return false, recordErr
		}
		if !missing {
			switch compareRemoteContent(row, direct, true) {
			case remoteContentMatch:
				return false, refreshCompletedRemoteVerification(row, direct, now, true)
			case remoteContentMismatch:
				directKind := completedRemoteEvidenceKind(row, direct)
				_, recordErr := recordCompletedRemoteEvidence(row, direct, directKind, now)
				return false, recordErr
			default:
				return false, nil
			}
		}
		missingRow := *row
		missingRow.ResolutionReason = ResolutionRemoteMissing
		return deleteCompletedCanonical(&missingRow)
	}

	// Persistent non-hash metadata divergence also requires one consistent
	// evidence streak; mismatch/missing observations cannot be mixed together.
	missingRow := *row
	missingRow.ResolutionReason = ResolutionNeedsCloudSyncRehydrate
	return deleteCompletedCanonical(&missingRow)
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
	if requireHash && !row.IsDir {
		return reconcileCompletedHashProvider(ctx, row, now)
	}

	remote, getErr := fs.Get(ctx, row.Path, &fs.GetArgs{NoLog: true})
	if getErr == nil && remote != nil {
		if row.IsDir && remote.IsDir() {
			// Once the directory consistency grace expires, a visible provider
			// collection can take ownership of directory metadata immediately.
			_, delErr := deleteCompletedCanonical(row)
			return false, delErr
		}
		if !row.IsDir && compareRemoteContent(row, remote, requireHash) == remoteContentMatch {
			if refreshErr := refreshCompletedRemoteVerification(row, remote, now, requireHash); refreshErr != nil {
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
		missing, direct, directErr := confirmCompletedRemoteMissing(ctx, row)
		if directErr != nil {
			return false, nil
		}
		if !missing {
			if compareRemoteContent(row, direct, requireHash) == remoteContentMatch {
				if refreshErr := refreshCompletedRemoteVerification(row, direct, now, requireHash); refreshErr != nil {
					return false, refreshErr
				}
			}
			return false, nil
		}
		missingRow := *row
		missingRow.ResolutionReason = ResolutionRemoteMissing
		return deleteCompletedCanonical(&missingRow)
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
		if refreshErr := refreshCompletedRemoteVerification(row, remote, now, requireHash); refreshErr != nil {
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
	if remoteHashMismatch(row, remote, requireHash) {
		return false
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

func splitOverlayRows(parent, parentKey string, candidates []model.WebDAVWritebackObject) ([]model.WebDAVWritebackObject, bool) {
	rows := make([]model.WebDAVWritebackObject, 0, len(candidates))
	canonicalParent := false
	for i := range candidates {
		row := candidates[i]
		if row.PathKey == parentKey && utils.FixAndCleanPath(row.Path) == parent {
			canonicalParent = !canonicalDeleted(&row) && row.IsDir
			continue
		}
		if row.ParentKey == parentKey {
			rows = append(rows, row)
		}
	}
	return rows, canonicalParent
}

func loadOverlayRows(parent string) ([]model.WebDAVWritebackObject, bool, error) {
	parent = utils.FixAndCleanPath(parent)
	key := pathKey(parent)
	var candidates []model.WebDAVWritebackObject
	if err := db.GetDb().
		Select(canonicalReadColumns).
		Where("parent_key = ? OR path_key = ?", key, key).
		Find(&candidates).Error; err != nil {
		return nil, false, err
	}
	rows, canonicalParent := splitOverlayRows(parent, key, candidates)
	return rows, canonicalParent, nil
}

func loadOverlayChildRows(parent string) ([]model.WebDAVWritebackObject, error) {
	parent = utils.FixAndCleanPath(parent)
	var rows []model.WebDAVWritebackObject
	if err := db.GetDb().
		Select(canonicalReadColumns).
		Where("parent_key = ?", pathKey(parent)).
		Find(&rows).Error; err != nil {
		return nil, err
	}
	return rows, nil
}

func overlayRowsNeedProviderOperationProtection(rows []model.WebDAVWritebackObject, remoteReliable bool, now time.Time) bool {
	if !remoteReliable {
		return false
	}
	for i := range rows {
		row := &rows[i]
		if canonicalDeleted(row) {
			continue
		}
		if row.IsDir {
			if directoryShadowExpired(row, now) {
				return true
			}
			continue
		}
		if row.State == StateCompleted && row.SpoolPath == "" {
			return true
		}
	}
	return false
}

// OverlayListChildren overlays only the known directory's children. WebDAV
// traversal already owns the parent object, so re-reading the canonical parent
// row would add an unnecessary MySQL round trip and an OR predicate to the
// dominant Cloud Sync PROPFIND path.
func OverlayListChildren(ctx context.Context, parent string, remote []model.Obj, remoteReliable bool) ([]model.Obj, bool, error) {
	if !Enabled() {
		return remote, false, nil
	}
	parent = utils.FixAndCleanPath(parent)
	rows, err := loadOverlayChildRows(parent)
	if err != nil {
		return nil, false, err
	}
	return overlayListRows(ctx, parent, remote, remoteReliable, rows)
}

// OverlayList replaces remote objects with their canonical WebDAV metadata and
// injects locally committed objects that are not visible on the remote yet.
// When the provider list itself succeeded, a completed row whose local spool
// cache has already been released is dropped if the remote object disappeared.
// That lets one-way Cloud Sync observe the loss and upload the source again.
func OverlayList(ctx context.Context, parent string, remote []model.Obj, remoteReliable bool) ([]model.Obj, bool, error) {
	overlaid, hasWriteback, _, err := OverlayListState(ctx, parent, remote, remoteReliable)
	return overlaid, hasWriteback, err
}

// OverlayListState also reports whether the listed collection itself is a
// canonical directory. Parent and child canonical rows are loaded together so
// recursive PROPFIND does not issue a second MySQL lookup for every directory.
func OverlayListState(ctx context.Context, parent string, remote []model.Obj, remoteReliable bool) ([]model.Obj, bool, bool, error) {
	if !Enabled() {
		return remote, false, false, nil
	}
	parent = utils.FixAndCleanPath(parent)
	rows, canonicalParent, err := loadOverlayRows(parent)
	if err != nil {
		return nil, false, false, err
	}
	overlaid, hasWriteback, err := overlayListRows(ctx, parent, remote, remoteReliable, rows)
	return overlaid, hasWriteback, canonicalParent, err
}

func mergeCanonicalOverlay(remote []model.Obj, rows []model.WebDAVWritebackObject) []model.Obj {
	byName := make(map[string]model.Obj, len(remote)+len(rows))
	order := make([]string, 0, len(remote)+len(rows))
	for _, obj := range remote {
		if obj == nil {
			continue
		}
		name := obj.GetName()
		if _, ok := byName[name]; !ok {
			order = append(order, name)
		}
		byName[name] = obj
	}

	for i := range rows {
		row := &rows[i]
		if canonicalDeleted(row) {
			delete(byName, row.Name)
			continue
		}
		if _, ok := byName[row.Name]; !ok {
			order = append(order, row.Name)
		}
		// Durable canonical metadata is Cloud Sync's verification authority.
		// Lagging provider size/mtime/hash/type metadata cannot replace it.
		byName[row.Name] = toObject(row)
	}

	out := make([]model.Obj, 0, len(byName))
	for _, name := range order {
		if obj, ok := byName[name]; ok {
			out = append(out, obj)
		}
	}
	return out
}

func overlayListRows(ctx context.Context, parent string, remote []model.Obj, remoteReliable bool, rows []model.WebDAVWritebackObject) ([]model.Obj, bool, error) {
	if len(rows) == 0 {
		return remote, false, nil
	}
	return mergeCanonicalOverlay(remote, rows), true, nil
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

func maxPendingSpoolBytes() uint64 {
	if conf.Conf == nil {
		return 0
	}
	return megabytesToBytes(conf.Conf.WebDAVWriteback.MaxPendingSpoolMB)
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

func spoolBacklogAdmissionCurrent(pending, reserved uint64) (uint64, bool) {
	if pending > ^uint64(0)-reserved {
		return ^uint64(0), false
	}
	return pending + reserved, true
}

func projectSpoolBacklogAdmission(current, incoming, limit uint64) (uint64, bool) {
	projected, ok := spoolBacklogAdmissionCurrent(current, incoming)
	if !ok {
		return ^uint64(0), false
	}
	if limit > 0 && projected > limit {
		// max_pending_spool_mb is a soft queue high-water mark, not a maximum
		// object size. If the queue is otherwise empty, admit one known oversize
		// object and let its full reservation block subsequent PUTs until it
		// drains. Physical free-space admission still protects the spool.
		if current == 0 && incoming > limit {
			return projected, true
		}
		return projected, false
	}
	return projected, true
}

func spoolBacklogAdmissionWeight(expected int64) uint64 {
	if expected > 0 {
		return uint64(expected)
	}
	if expected < 0 {
		return incomingReservationChunkBytes()
	}
	return 0
}

func receiveReservationProgressBytes(expected, received int64) uint64 {
	if expected >= 0 {
		return spoolBacklogAdmissionWeight(expected)
	}
	chunk := incomingReservationChunkBytes()
	if received <= 0 {
		return chunk
	}
	value := uint64(received)
	if value > ^uint64(0)-chunk {
		return ^uint64(0)
	}
	return value + chunk
}

func pendingBacklogState(state string) bool {
	switch state {
	case StateQueued, legacyStateFailed, StateUploading, StateVerifying:
		return true
	default:
		return false
	}
}

func pendingSpoolBacklogBytes(tx *gorm.DB, excludePath string) (uint64, error) {
	var result struct {
		Bytes int64 `gorm:"column:bytes"`
	}
	query := tx.Model(&model.WebDAVWritebackObject{}).
		Where("state IN ?", []string{StateQueued, legacyStateFailed, StateUploading, StateVerifying})
	if excludePath == "" {
		query = query.Select("COALESCE(SUM(CASE WHEN spool_path = '' THEN 0 ELSE size END), 0) AS bytes")
	} else {
		// Backlog is durable payload pressure, not metadata-only control work.
		query = query.Select(
			"COALESCE(SUM(CASE WHEN spool_path = '' OR path_key = ? THEN 0 ELSE size END), 0) AS bytes",
			pathKey(excludePath),
		)
	}
	if err := query.Scan(&result).Error; err != nil {
		return 0, err
	}
	if result.Bytes < 0 {
		return 0, errors.New("write-back pending spool backlog is negative")
	}
	return uint64(result.Bytes), nil
}

func activeReceiveBacklogBytes(tx *gorm.DB, now time.Time) (uint64, error) {
	var result struct {
		Bytes uint64 `gorm:"column:bytes"`
	}
	err := tx.Model(&model.WebDAVWritebackReceiveReservation{}).
		Select("COALESCE(SUM(bytes), 0) AS bytes").
		Where("lease_until > ?", now).
		Scan(&result).Error
	return result.Bytes, err
}

func durableBacklogAdmissionCurrent(tx *gorm.DB, excludePath string, now time.Time) (uint64, error) {
	pending, err := pendingSpoolBacklogBytes(tx, excludePath)
	if err != nil {
		return 0, err
	}
	receiving, err := activeReceiveBacklogBytes(tx, now)
	if err != nil {
		return 0, err
	}
	current, ok := spoolBacklogAdmissionCurrent(pending, receiving)
	if !ok {
		return ^uint64(0), nil
	}
	return current, nil
}

func lockAdmissionFence(tx *gorm.DB) error {
	seed := model.WebDAVWritebackAdmissionFence{ID: 1}
	if err := tx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "id"}},
		DoNothing: true,
	}).Create(&seed).Error; err != nil {
		return err
	}
	var fence model.WebDAVWritebackAdmissionFence
	return tx.Clauses(clause.Locking{Strength: "UPDATE"}).Take(&fence, 1).Error
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
	remaining       uint64
	consumedPending uint64
	released        bool
}

func (r *incomingReservation) flushConsumedLocked() {
	if r == nil || r.consumedPending == 0 {
		return
	}
	if r.consumedPending > reservedIncoming {
		reservedIncoming = 0
	} else {
		reservedIncoming -= r.consumedPending
	}
	r.consumedPending = 0
}

func completedSpoolPressureReclaimEnabled() bool {
	return conf.Conf != nil && conf.Conf.WebDAVWriteback.CompletedCacheTTLMinutes >= 0
}

func reclaimCompletedSpoolCapacity(targetFree uint64) bool {
	if !completedSpoolPressureReclaimEnabled() || targetFree == 0 || db.GetDb() == nil {
		return false
	}

	reclaimed := false
	for batch := 0; batch < 10; batch++ {
		usage, err := disk.Usage(conf.Conf.WebDAVWriteback.SpoolDir)
		if err != nil || usage.Free >= targetFree {
			return reclaimed
		}

		var rows []model.WebDAVWritebackObject
		if err := db.GetDb().
			Select("id", "generation", "spool_path", "completed_at", "remote_generation", "remote_verified_at").
			Where("state = ? AND spool_path <> '' AND completed_at IS NOT NULL", StateCompleted).
			Where("remote_verified_at IS NOT NULL AND remote_generation = generation").
			Order("completed_at asc, id asc").
			Limit(100).
			Find(&rows).Error; err != nil || len(rows) == 0 {
			return reclaimed
		}

		changed := false
		for i := range rows {
			row := &rows[i]
			if spoolIsActive(row.SpoolPath) {
				continue
			}
			res := db.GetDb().Model(&model.WebDAVWritebackObject{}).
				Where("id = ? AND generation = ? AND state = ? AND spool_path = ? AND completed_at IS NOT NULL",
					row.ID, row.Generation, StateCompleted, row.SpoolPath).
				Where("remote_verified_at IS NOT NULL AND remote_generation = generation").
				Update("spool_path", "")
			if res.Error != nil || res.RowsAffected == 0 {
				continue
			}
			changed = true
			reclaimed = true
			removeSpoolIfUnreferenced(row.SpoolPath)
		}
		if !changed {
			return reclaimed
		}
	}
	return reclaimed
}

func (r *incomingReservation) grow(additional uint64) error {
	if r == nil || additional == 0 {
		return nil
	}

	for attempt := 0; attempt < 2; attempt++ {
		spaceMu.Lock()
		r.flushConsumedLocked()
		usage, err := disk.Usage(conf.Conf.WebDAVWriteback.SpoolDir)
		if err != nil {
			spaceMu.Unlock()
			return err
		}
		required, ok := spoolAdmissionRequired(reserveBytes(), reservedIncoming, additional)
		if ok && usage.Free >= required {
			reservedIncoming += additional
			r.remaining += additional
			spaceMu.Unlock()
			return nil
		}
		spaceMu.Unlock()

		if !ok {
			return &SpoolCapacityError{Free: usage.Free, Required: ^uint64(0)}
		}
		if attempt == 0 && reclaimCompletedSpoolCapacity(required) {
			continue
		}
		return &SpoolCapacityError{Free: usage.Free, Required: required}
	}
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
	if written > r.remaining {
		written = r.remaining
	}
	r.remaining -= written
	r.consumedPending += written
}

func (r *incomingReservation) verifyCapacity() error {
	if r == nil {
		return nil
	}

	for attempt := 0; attempt < 2; attempt++ {
		spaceMu.Lock()
		r.flushConsumedLocked()
		usage, err := disk.Usage(conf.Conf.WebDAVWriteback.SpoolDir)
		if err != nil {
			spaceMu.Unlock()
			return err
		}
		required, ok := spoolAdmissionRequired(reserveBytes(), reservedIncoming, 0)
		if ok && usage.Free >= required {
			spaceMu.Unlock()
			return nil
		}
		spaceMu.Unlock()

		if !ok {
			return &SpoolCapacityError{Free: usage.Free, Required: ^uint64(0)}
		}
		if attempt == 0 && reclaimCompletedSpoolCapacity(required) {
			continue
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
	r.flushConsumedLocked()
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

func claimReceiving(p string, exclusive bool) (*receivingSession, func(), bool) {
	key := pathKey(p)
	receivingMu.Lock()
	state := receivingPaths[key]
	if exclusive && state != nil && state.active > 0 {
		receivingMu.Unlock()
		return nil, nil, false
	}
	if state == nil {
		state = &receivingPathState{}
		receivingPaths[key] = state
	}
	state.active++
	session := &receivingSession{key: key, state: state}
	receivingMu.Unlock()

	release := func() {
		receivingMu.Lock()
		if current := receivingPaths[key]; current == state {
			state.active--
			if state.active <= 0 {
				delete(receivingPaths, key)
			}
		}
		receivingMu.Unlock()
	}
	return session, release, true
}

func beginReceiving(p string) (*receivingSession, func()) {
	session, release, _ := claimReceiving(p, false)
	return session, release
}

func tryBeginReceiving(p string) (*receivingSession, func(), bool) {
	return claimReceiving(p, true)
}

func receiveSequenceSuperseded(lastCommitted, current uint64) bool {
	return lastCommitted > current
}

func lockOrCreateReceiveFence(tx *gorm.DB, p string) (*model.WebDAVWritebackReceiveFence, error) {
	p = utils.FixAndCleanPath(p)
	key := pathKey(p)
	seed := model.WebDAVWritebackReceiveFence{PathKey: key, Path: p}
	if err := tx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "path_key"}},
		DoNothing: true,
	}).Create(&seed).Error; err != nil {
		return nil, err
	}

	var fence model.WebDAVWritebackReceiveFence
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Select("id", "path_key", "path", "next_sequence", "last_committed_sequence", "active_receivers", "latest_started_at", "receive_lease_until").
		Where("path_key = ?", key).
		Take(&fence).Error; err != nil {
		return nil, err
	}
	return &fence, nil
}

func receiveFenceActive(fence *model.WebDAVWritebackReceiveFence, now time.Time) bool {
	return fence != nil &&
		fence.ActiveReceivers > 0 &&
		fence.ReceiveLeaseUntil != nil &&
		now.Before(*fence.ReceiveLeaseUntil)
}

func receiveAdmissionNeedsGlobalFence(limit uint64) bool {
	return limit > 0
}

func beginDurableReceiveSequence(ctx context.Context, p string, expected int64) (uint64, bool, error) {
	p = utils.FixAndCleanPath(p)
	// The caller has already claimed the in-process receive slot atomically.
	// This durable fence remains authoritative across OpenList instances and
	// process restarts.
	var sequence uint64
	now := time.Now()
	leaseUntil := now.Add(receiveLeaseDuration)
	limit := maxPendingSpoolBytes()
	backlogWeight := spoolBacklogAdmissionWeight(expected)
	backlogLimited := receiveAdmissionNeedsGlobalFence(limit) && backlogWeight > 0
	err := db.GetDb().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if backlogLimited {
			if err := lockAdmissionFence(tx); err != nil {
				return err
			}
		}

		fence, err := lockOrCreateReceiveFence(tx, p)
		if err != nil {
			return err
		}
		if receiveFenceActive(fence, now) {
			// A duplicate Cloud Sync PUT for the same path should not consume a
			// second request body while the first receive is still live. Return a
			// transient response before reading the body; once the first PUT is
			// durably ACKed, the next PROPFIND sees canonical state and the retry
			// can converge without a parallel full-file receive.
			return ErrReceiveInProgress
		}
		if fence.ActiveReceivers > 0 {
			// A crashed process can leave an active count behind. An expired
			// receive lease is stale and may be reclaimed by this new receiver.
			fence.ActiveReceivers = 0
		}

		if backlogLimited {
			current, err := durableBacklogAdmissionCurrent(tx, p, now)
			if err != nil {
				return err
			}
			projected, allowed := projectSpoolBacklogAdmission(current, backlogWeight, limit)
			if !allowed {
				return &SpoolCapacityError{Backlog: projected, BacklogLimit: limit}
			}
		}

		fence.NextSequence++
		fence.ActiveReceivers++
		fence.Path = p
		fence.LatestExpectedSize = expected
		fence.LatestReceivedSize = 0
		fence.LatestStartedAt = &now
		fence.ReceiveLeaseUntil = &leaseUntil
		sequence = fence.NextSequence
		if err := tx.Model(&model.WebDAVWritebackReceiveFence{}).
			Where("id = ?", fence.ID).
			Updates(map[string]any{
				"path":                 p,
				"next_sequence":        sequence,
				"active_receivers":     fence.ActiveReceivers,
				"latest_expected_size": expected,
				"latest_received_size": int64(0),
				"latest_started_at":    &now,
				"receive_lease_until":  &leaseUntil,
			}).Error; err != nil {
			return err
		}

		if !backlogLimited {
			return nil
		}
		reservation := model.WebDAVWritebackReceiveReservation{
			PathKey:    pathKey(p),
			Sequence:   sequence,
			Bytes:      backlogWeight,
			LeaseUntil: leaseUntil,
		}
		return tx.Create(&reservation).Error
	})
	return sequence, backlogLimited, err
}

func receiveHeartbeatNeedsAdmission(expected, received int64, backlogLimited bool) bool {
	return backlogLimited && expected < 0 && received > 0
}

func receiveProgressHeartbeatNeeded(expected int64, backlogReserved bool) bool {
	return expected > 0 || (expected < 0 && backlogReserved)
}

func refreshReceiveLease(tx *gorm.DB, key string, sequence uint64, leaseUntil time.Time, received int64, backlogReserved bool) error {
	fenceUpdates := map[string]any{"receive_lease_until": &leaseUntil}
	if received > 0 {
		fenceUpdates["latest_received_size"] = received
	}
	if err := tx.Model(&model.WebDAVWritebackReceiveFence{}).
		Where("path_key = ? AND active_receivers > 0", key).
		Updates(fenceUpdates).Error; err != nil {
		return err
	}
	if !backlogReserved {
		return nil
	}
	return tx.Model(&model.WebDAVWritebackReceiveReservation{}).
		Where("path_key = ? AND sequence = ?", key, sequence).
		Update("lease_until", leaseUntil).Error
}

func heartbeatReceiveSequence(ctx context.Context, p string, sequence uint64, expected, received int64, backlogReserved bool) {
	leaseUntil := time.Now().Add(receiveLeaseDuration)
	key := pathKey(p)

	if !receiveHeartbeatNeedsAdmission(expected, received, backlogReserved) {
		// Known-length PUTs already reserved their complete declared size.
		// Periodic heartbeats (including received=0 for unknown-length bodies)
		// only extend per-path leases, so they must not serialize unrelated PUTs
		// through the singleton admission fence.
		_ = db.GetDb().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			return refreshReceiveLease(tx, key, sequence, leaseUntil, received, backlogReserved)
		})
		return
	}

	// Unknown-length progress increases the durable reservation and therefore
	// participates in global backlog accounting. Keep only this path behind the
	// admission fence so concurrent admissions see a consistent total.
	_ = db.GetDb().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := lockAdmissionFence(tx); err != nil {
			return err
		}
		fenceUpdates := map[string]any{"receive_lease_until": &leaseUntil}
		if received > 0 {
			fenceUpdates["latest_received_size"] = received
		}
		if err := tx.Model(&model.WebDAVWritebackReceiveFence{}).
			Where("path_key = ? AND active_receivers > 0", key).
			Updates(fenceUpdates).Error; err != nil {
			return err
		}
		progressBytes := receiveReservationProgressBytes(expected, received)
		return tx.Model(&model.WebDAVWritebackReceiveReservation{}).
			Where("path_key = ? AND sequence = ? AND bytes < ?", key, sequence, progressBytes).
			Updates(map[string]any{
				"lease_until": leaseUntil,
				"bytes":       progressBytes,
			}).Error
	})
}

func startReceiveLeaseHeartbeat(ctx context.Context, p string, sequence uint64, expected int64, backlogReserved bool) func() {
	if expected == 0 {
		return func() {}
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(receiveHeartbeatEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				// received=0 intentionally refreshes only the lease for
				// unknown-length bodies. Progress checkpoints in copyToSpool
				// monotonically grow the durable byte reservation separately.
				heartbeatReceiveSequence(ctx, p, sequence, expected, 0, backlogReserved)
			case <-stop:
				return
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			close(stop)
			<-done
		})
	}
}

func endReceiveSequence(ctx context.Context, p string, sequence uint64, backlogReserved bool) {
	_ = db.GetDb().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Failure cleanup keeps the same global->path lock order as admission.
		if backlogReserved {
			if err := lockAdmissionFence(tx); err != nil {
				return err
			}
		}

		var fence model.WebDAVWritebackReceiveFence
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Select("id", "last_committed_sequence", "active_receivers", "receive_lease_until").
			Where("path_key = ?", pathKey(p)).
			Take(&fence).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return err
		}
		return finalizeReceiveSequenceTx(tx, p, &fence, sequence, backlogReserved, false)
	})
}

func durableReceiving(p string, now time.Time) (bool, error) {
	if isReceiving(p) {
		return true, nil
	}
	var fence model.WebDAVWritebackReceiveFence
	err := db.GetDb().
		Select("id", "active_receivers", "receive_lease_until").
		Where("path_key = ?", pathKey(p)).
		Take(&fence).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if fence.ActiveReceivers <= 0 || fence.ReceiveLeaseUntil == nil {
		return false, nil
	}
	if now.Before(*fence.ReceiveLeaseUntil) {
		return true, nil
	}
	// Expired lease is stale crash residue. Clear it with a guarded update so
	// another instance that already refreshed the lease is never clobbered.
	_ = db.GetDb().Model(&model.WebDAVWritebackReceiveFence{}).
		Where("id = ? AND active_receivers > 0 AND receive_lease_until <= ?", fence.ID, now).
		Updates(map[string]any{
			"active_receivers":    0,
			"receive_lease_until": nil,
		}).Error
	return false, nil
}

func Receiving(p string) (bool, error) {
	return durableReceiving(utils.FixAndCleanPath(p), time.Now())
}

func receivingTreeActive(fences []model.WebDAVWritebackReceiveFence, root string, now time.Time) bool {
	root = utils.FixAndCleanPath(root)
	for i := range fences {
		fence := &fences[i]
		if !receiveFenceActive(fence, now) {
			continue
		}
		if isPathOrDescendant(fence.Path, root) {
			return true
		}
	}
	return false
}

func ReceivingTree(root string) (bool, error) {
	root = utils.FixAndCleanPath(root)
	now := time.Now()
	var fences []model.WebDAVWritebackReceiveFence
	// Active receive rows are normally tiny in number. Use the composite
	// active+lease index to fetch only live receivers, then do strict path
	// boundary matching in Go. This avoids a recursive TEXT LIKE + COUNT scan
	// on every DELETE/MOVE/COPY overlap check.
	err := db.GetDb().Model(&model.WebDAVWritebackReceiveFence{}).
		Select("path", "active_receivers", "receive_lease_until").
		Where("active_receivers > 0 AND receive_lease_until > ?", now).
		Find(&fences).Error
	if err != nil {
		return false, err
	}
	return receivingTreeActive(fences, root, now), nil
}

func lockReceiveFence(tx *gorm.DB, p string) (*model.WebDAVWritebackReceiveFence, error) {
	return lockOrCreateReceiveFence(tx, p)
}

func advanceMutationFence(tx *gorm.DB, p string) error {
	fence, err := lockOrCreateReceiveFence(tx, p)
	if err != nil {
		return err
	}
	fence.NextSequence++
	fence.LastCommittedSequence = fence.NextSequence
	return tx.Model(&model.WebDAVWritebackReceiveFence{}).
		Where("id = ?", fence.ID).
		Updates(map[string]any{
			"path":                    utils.FixAndCleanPath(p),
			"next_sequence":           fence.NextSequence,
			"last_committed_sequence": fence.LastCommittedSequence,
		}).Error
}

func advanceMutationFenceTree(tx *gorm.DB, root string) error {
	root = utils.FixAndCleanPath(root)
	if _, err := lockOrCreateReceiveFence(tx, root); err != nil {
		return err
	}
	var fences []model.WebDAVWritebackReceiveFence
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("path = ? OR path LIKE ? ESCAPE '~'", root, descendantLikePattern(root)).
		Order("id asc").
		Find(&fences).Error; err != nil {
		return err
	}
	for i := range fences {
		fence := &fences[i]
		if !isPathOrDescendant(fence.Path, root) {
			continue
		}
		fence.NextSequence++
		fence.LastCommittedSequence = fence.NextSequence
		if err := tx.Model(&model.WebDAVWritebackReceiveFence{}).
			Where("id = ?", fence.ID).
			Updates(map[string]any{
				"next_sequence":           fence.NextSequence,
				"last_committed_sequence": fence.LastCommittedSequence,
			}).Error; err != nil {
			return err
		}
	}
	return nil
}

func orderedMutationFenceRoots(roots ...string) []string {
	seen := make(map[string]struct{}, len(roots))
	ordered := make([]string, 0, len(roots))
	for _, root := range roots {
		root = utils.FixAndCleanPath(root)
		if _, ok := seen[root]; ok {
			continue
		}
		seen[root] = struct{}{}
		ordered = append(ordered, root)
	}
	sort.Strings(ordered)
	return ordered
}

func advanceMutationFenceTrees(tx *gorm.DB, roots ...string) error {
	for _, root := range orderedMutationFenceRoots(roots...) {
		if err := advanceMutationFenceTree(tx, root); err != nil {
			return err
		}
	}
	return nil
}

func receiveFenceFinalizeUpdates(fence *model.WebDAVWritebackReceiveFence, sequence uint64, committed bool) (map[string]any, error) {
	if fence == nil || sequence == 0 {
		return nil, errors.New("write-back receive fence is missing")
	}
	updates := make(map[string]any, 3)
	if committed && !receiveSequenceSuperseded(fence.LastCommittedSequence, sequence) && fence.LastCommittedSequence != sequence {
		fence.LastCommittedSequence = sequence
		updates["last_committed_sequence"] = sequence
	}
	if fence.ActiveReceivers <= 1 {
		fence.ActiveReceivers = 0
		fence.ReceiveLeaseUntil = nil
		updates["active_receivers"] = 0
		updates["receive_lease_until"] = nil
	} else {
		fence.ActiveReceivers--
		updates["active_receivers"] = fence.ActiveReceivers
	}
	return updates, nil
}

func finalizeReceiveSequenceTx(tx *gorm.DB, p string, fence *model.WebDAVWritebackReceiveFence, sequence uint64, backlogReserved bool, committed bool) error {
	if backlogReserved {
		if err := tx.Where("path_key = ? AND sequence = ?", pathKey(p), sequence).
			Delete(&model.WebDAVWritebackReceiveReservation{}).Error; err != nil {
			return err
		}
	}
	updates, err := receiveFenceFinalizeUpdates(fence, sequence, committed)
	if err != nil {
		return err
	}
	return tx.Model(&model.WebDAVWritebackReceiveFence{}).
		Where("id = ?", fence.ID).
		Updates(updates).Error
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

const spoolCopyBufferSize = 4 * utils.MB

var spoolCopyBufferPool = sync.Pool{
	New: func() any {
		return make([]byte, spoolCopyBufferSize)
	},
}

func copyToSpool(dst *os.File, src io.Reader, expected int64, reservation *incomingReservation, heartbeat func(int64)) (int64, string, error) {
	buf := spoolCopyBufferPool.Get().([]byte)
	defer spoolCopyBufferPool.Put(buf)
	payloadHasher := utils.SHA1.NewFunc()
	writer := io.MultiWriter(dst, payloadHasher)
	var total int64
	var sinceCheck int64
	lastHeartbeat := time.Now()

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
			if heartbeat != nil && time.Since(lastHeartbeat) >= receiveProgressHeartbeatEvery {
				heartbeat(total)
				lastHeartbeat = time.Now()
			}
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
				if expected < 0 && heartbeat != nil {
					heartbeat(total)
					lastHeartbeat = time.Now()
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
	if heartbeat != nil && total > 0 {
		heartbeat(total)
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

	// Claim the local path before any MySQL admission work. Same-instance
	// Cloud Sync duplicate PUTs are rejected before they can contend on the
	// durable receive/admission fences or consume the request body.
	_, releaseReceiving, claimed := tryBeginReceiving(p)
	if !claimed {
		return nil, false, ErrReceiveInProgress
	}
	defer releaseReceiving()

	receiveSequence, backlogReserved, err := beginDurableReceiveSequence(ctx, p, expected)
	if err != nil {
		return nil, false, err
	}
	receiveCtx := durableCommitContext(ctx)
	stopLeaseHeartbeat := startReceiveLeaseHeartbeat(receiveCtx, p, receiveSequence, expected, backlogReserved)
	receiveFinalized := false
	defer func() {
		stopLeaseHeartbeat()
		if !receiveFinalized {
			endReceiveSequence(receiveCtx, p, receiveSequence, backlogReserved)
		}
	}()
	spoolDir := conf.Conf.WebDAVWriteback.SpoolDir
	actualSize := int64(0)
	payloadSHA1 := ""
	finalName := ""

	if expected == 0 {
		// An exact zero-byte body has no payload bytes that require filesystem
		// durability. Confirm the body is actually empty, then persist only its
		// canonical metadata and content hash in MySQL.
		n, readErr := io.CopyN(io.Discard, body, 1)
		if n > 0 {
			return nil, false, fmt.Errorf("WebDAV PUT exceeds declared size: expected 0 bytes")
		}
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return nil, false, fmt.Errorf("WebDAV PUT receive failed after 0/0 bytes: %w", readErr)
		}
		payloadSHA1 = emptyPayloadSHA1()
	} else {
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

		var progressHeartbeat func(int64)
		if receiveProgressHeartbeatNeeded(expected, backlogReserved) {
			progressHeartbeat = func(received int64) {
				heartbeatReceiveSequence(receiveCtx, p, receiveSequence, expected, received, backlogReserved)
			}
		}
		actualSize, payloadSHA1, err = copyToSpool(tmp, body, expected, reservation, progressHeartbeat)
		if err != nil {
			if expected >= 0 {
				return nil, false, fmt.Errorf("WebDAV PUT receive failed after %d/%d bytes: %w", actualSize, expected, err)
			}
			return nil, false, fmt.Errorf("WebDAV PUT receive failed after %d bytes with unknown declared length: %w", actualSize, err)
		}
		if err := tmp.Sync(); err != nil {
			return nil, false, err
		}
		if err := tmp.Close(); err != nil {
			return nil, false, err
		}
		finalName = filepath.Join(spoolDir, uuid.NewString()+".data")
		if err := os.Rename(tmpName, finalName); err != nil {
			return nil, false, err
		}
		syncDir(spoolDir)
		committed = true
	}

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
	settleAt := time.Now().Add(cloudSyncSettleDelay(actualSize))

	// MySQL is the authoritative ordering point for same-path Cloud Sync PUTs.
	// The path fence survives process restarts and is shared by every instance.
	commitCtx := receiveCtx
	superseded := false
	err = db.GetDb().WithContext(commitCtx).Transaction(func(tx *gorm.DB) error {
		if backlogReserved {
			if err := lockAdmissionFence(tx); err != nil {
				return err
			}
		}
		fence, err := lockReceiveFence(tx, p)
		if err != nil {
			return err
		}
		var row model.WebDAVWritebackObject
		findErr := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("path_key = ?", key).Take(&row).Error
		if findErr != nil && !errors.Is(findErr, gorm.ErrRecordNotFound) {
			return findErr
		}
		if receiveSequenceSuperseded(fence.LastCommittedSequence, receiveSequence) {
			if findErr != nil {
				return errors.New("superseded WebDAV PUT has no canonical successor")
			}
			superseded = true
			saved = row
			return finalizeReceiveSequenceTx(tx, p, fence, receiveSequence, backlogReserved, true)
		}
		if findErr == nil {
			if canCoalesceDuplicatePut(&row, actualSize, payloadSHA1) {
				if durableLocalPayloadAvailable(&row) {
					duplicate = true
					applyDuplicatePutMetadata(&row, modTime, createTime, mime, modTimeProvided, createTimeProvided)
					markCanonicalAcked(&row, time.Now())
					updates := map[string]any{
						"mod_time":        row.ModTime,
						"create_time":     row.CreateTime,
						"mime_type":       row.MimeType,
						"canonical_state": row.CanonicalState,
						"ack_time":        row.AckTime,
						"durable_at":      row.DurableAt,
					}
					if err := tx.Model(&model.WebDAVWritebackObject{}).
						Where("id = ? AND generation = ?", row.ID, row.Generation).
						Updates(updates).Error; err != nil {
						return err
					}
					saved = row
					return finalizeReceiveSequenceTx(tx, p, fence, receiveSequence, backlogReserved, true)
				}
			}
			if canCoalesceCompletedDuplicatePut(&row, actualSize, payloadSHA1) {
				duplicate = true
				applyDuplicatePutMetadata(&row, modTime, createTime, mime, modTimeProvided, createTimeProvided)
				markCanonicalAcked(&row, time.Now())
				if err := tx.Model(&model.WebDAVWritebackObject{}).
					Where("id = ? AND generation = ? AND state = ?", row.ID, row.Generation, StateCompleted).
					Updates(map[string]any{
						"mod_time":        row.ModTime,
						"create_time":     row.CreateTime,
						"mime_type":       row.MimeType,
						"canonical_state": row.CanonicalState,
						"ack_time":        row.AckTime,
						"durable_at":      row.DurableAt,
					}).Error; err != nil {
					return err
				}
				saved = row
				return finalizeReceiveSequenceTx(tx, p, fence, receiveSequence, backlogReserved, true)
			}
			oldSpool = row.SpoolPath
			if canonicalDeleted(&row) {
				created = true
			}
			row.Generation = nextReceiveGeneration(row.Generation, receiveSequence)
		} else {
			created = true
			row.Generation = nextReceiveGeneration(0, receiveSequence)
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
		row.ReceiveStartedAt = cloneHistoryTime(fence.LatestStartedAt)
		markCanonicalAcked(&row, time.Now())
		row.CloudSyncReuploadRequired = false
		row.RecoveryStartedAt = nil
		row.ResolutionReason = ""
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
		return finalizeReceiveSequenceTx(tx, p, fence, receiveSequence, backlogReserved, true)
	})
	if err != nil {
		if finalName != "" {
			_ = os.Remove(finalName)
		}
		return nil, false, err
	}
	receiveFinalized = true
	if superseded {
		if finalName != "" {
			_ = os.Remove(finalName)
			syncDir(spoolDir)
		}
		return &saved, false, nil
	}
	if duplicate {
		if finalName != "" {
			_ = os.Remove(finalName)
			syncDir(spoolDir)
		}
		return &saved, false, nil
	}

	if oldSpool != "" && oldSpool != finalName {
		if !spoolIsActive(oldSpool) {
			removeSpoolIfUnreferenced(oldSpool)
		}
	}
	InvalidateProviderSnapshots(parent)
	// retry_at already carries the Cloud Sync settle window. Waking the
	// scheduler before that deadline only forces an immediate queue query that
	// cannot dispatch this row; the existing 2s scheduler tick will pick it up.
	if !settleAt.After(time.Now()) {
		wake()
	}
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
		if err := advanceMutationFence(tx, p); err != nil {
			return err
		}
		var row model.WebDAVWritebackObject
		findErr := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("path_key = ?", key).Take(&row).Error
		if findErr != nil && !errors.Is(findErr, gorm.ErrRecordNotFound) {
			return findErr
		}
		if findErr == nil {
			if !canonicalDeleted(&row) && !row.IsDir {
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
		row.ReceiveStartedAt = &now
		markCanonicalAcked(&row, now)
		row.CloudSyncReuploadRequired = false
		row.RecoveryStartedAt = nil
		row.ResolutionReason = ""
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
	InvalidateProviderSnapshots(parent, p)
	wake()
	return &saved, created, nil
}

type LocalPayload interface {
	io.ReadSeeker
	io.Closer
}

type memoryPayload struct {
	*bytes.Reader
}

func (m *memoryPayload) Close() error {
	return nil
}

func openLocalPayload(row *model.WebDAVWritebackObject) (LocalPayload, bool, error) {
	if row == nil || canonicalDeleted(row) || row.IsDir {
		return nil, false, nil
	}
	if row.SpoolPath != "" {
		f, err := os.Open(row.SpoolPath)
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, err
		}
		return f, true, nil
	}
	if row.Size == 0 && canonicalAcked(row) {
		return &memoryPayload{Reader: bytes.NewReader(nil)}, true, nil
	}
	return nil, false, nil
}

func OpenLocal(p string) (LocalPayload, *model.WebDAVWritebackObject, error) {
	row, err := getByPath(p)
	if err != nil || row == nil || canonicalDeleted(row) {
		return nil, row, err
	}
	payload, available, err := openLocalPayload(row)
	if err != nil || !available {
		return nil, row, err
	}
	return payload, row, nil
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
		if err := advanceMutationFenceTree(tx, p); err != nil {
			return err
		}
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
			if canonicalDeleted(row) {
				continue
			}
			if err := tx.Model(&model.WebDAVWritebackObject{}).
				Where("id = ?", row.ID).
				Updates(map[string]any{
					"generation":                   gorm.Expr("generation + 1"),
					"canonical_state":              CanonicalStateDeleted,
					"state":                        StateDeleted,
					"cloud_sync_reupload_required": false,
					"recovery_started_at":          nil,
					"retry_at":                     &now,
					"last_error":                   "",
					"resolution_reason":            "",
					"retry_count":                  0,
					"verify_count":                 0,
					"completed_at":                 nil,
					"remote_object_id":             "",
					"remote_sha1":                  "",
					"remote_generation":            0,
					"remote_verified_at":           nil,
				}).Error; err != nil {
				return err
			}
		}

		if !hasExact && matchedDescendant {
			key := pathKey(p)
			row := model.WebDAVWritebackObject{
				PathKey:        key,
				ParentKey:      pathKey(path.Dir(p)),
				Path:           p,
				Parent:         path.Dir(p),
				Name:           path.Base(p),
				IsDir:          true,
				Size:           0,
				ModTime:        now,
				CreateTime:     now,
				ETag:           canonicalETag(key, 1, 0),
				Generation:     1,
				CanonicalState: CanonicalStateDeleted,
				State:          StateDeleted,
				RetryAt:        &now,
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
		InvalidateProviderSnapshots(path.Dir(p), p)
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
		if err := advanceMutationFenceTree(tx, p); err != nil {
			return err
		}
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
			if canonicalDeleted(row) {
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
			if canonicalDeleted(current) {
				return true, nil
			}
			return false, ErrCanonicalChanged
		}
		return false, err
	}
	if alreadyDeleted {
		InvalidateProviderSnapshots(path.Dir(p), p)
		return true, nil
	}
	if staged {
		InvalidateProviderSnapshots(path.Dir(p), p)
		wake()
	}
	return staged, nil
}

func tombstoneMovedSource(row *model.WebDAVWritebackObject, now time.Time) {
	row.Generation++
	markCanonicalDeleted(row)
	row.State = StateDeleted
	row.SpoolPath = ""
	row.PayloadSHA1 = ""
	row.CleanupPath = ""
	row.LastError = ""
	row.ResolutionReason = ""
	row.RetryCount = 0
	row.VerifyCount = 0
	row.RetryAt = &now
	row.CompletedAt = nil
	clearRemoteVerification(row)
}

func pendingDirectoryMoveLocallyAuthoritative(root *model.WebDAVWritebackObject, rows []model.WebDAVWritebackObject, now time.Time) bool {
	if root == nil || !root.IsDir || canonicalDeleted(root) {
		return false
	}
	if root.State == StateCompleted {
		if root.CompletedAt == nil || directoryShadowExpired(root, now) {
			return false
		}
	}
	for i := range rows {
		row := &rows[i]
		if canonicalDeleted(row) || row.IsDir {
			continue
		}
		if row.SpoolPath == "" {
			return false
		}
	}
	return true
}

func completedDirectoryReplicaMoveEligible(root *model.WebDAVWritebackObject, rows []model.WebDAVWritebackObject) bool {
	if root == nil || !root.IsDir || canonicalDeleted(root) || !canonicalAcked(root) || root.State != StateCompleted {
		return false
	}
	for i := range rows {
		row := &rows[i]
		if canonicalDeleted(row) {
			continue
		}
		if !canonicalAcked(row) {
			return false
		}
		if row.IsDir {
			if row.State != StateCompleted {
				return false
			}
			continue
		}
		if row.SpoolPath != "" {
			continue
		}
		if row.State != StateCompleted || canonicalContentSHA1(row) == "" {
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
		if err := advanceMutationFenceTrees(tx, src, dst); err != nil {
			return err
		}
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
		if rootIndex < 0 {
			return errPendingDirectoryMoveFallback
		}
		localRebuild := pendingDirectoryMoveLocallyAuthoritative(&sourceRows[rootIndex], sourceRows, now)
		replicaTreeMove := !localRebuild && completedDirectoryReplicaMoveEligible(&sourceRows[rootIndex], sourceRows)
		if !localRebuild && !replicaTreeMove {
			return errPendingDirectoryMoveFallback
		}
		treeHoldUntil := now.Add(replicaMoveSourceHoldDelay())

		destinationByPath := make(map[string]int, len(destinationRows))
		for i := range destinationRows {
			row := &destinationRows[i]
			if !canonicalDeleted(row) {
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
			if canonicalDeleted(sourceRow) {
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
			markCanonicalAcked(&destinationRow, now)
			destinationRow.State = StateQueued
			if sourceRow.IsDir {
				destinationRow.SpoolPath = ""
			} else {
				destinationRow.SpoolPath = sourceRow.SpoolPath
			}
			destinationRow.PayloadSHA1 = sourceRow.PayloadSHA1
			if !sourceRow.IsDir && destinationRow.PayloadSHA1 == "" && sourceRow.SpoolPath == "" {
				destinationRow.PayloadSHA1 = canonicalContentSHA1(sourceRow)
			}
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

			if replicaTreeMove {
				// Mark the exact generation published by this MOVE. The marker
				// is not remote evidence because RemoteVerifiedAt stays nil; it
				// only lets fallback avoid overwriting later Cloud Sync edits.
				destinationRow.RemoteGeneration = destinationRow.Generation
				switch {
				case sourceRow.Path == src:
					destinationRow.State = StateQueued
					destinationRow.CleanupPath = src
					retryAt = now
				case sourceRow.IsDir:
					// Keep descendants blocked behind the root MOVE. On provider
					// success they become COMPLETED without redundant MKCOLs.
					destinationRow.State = StateQueued
					retryAt = treeHoldUntil
				case sourceRow.SpoolPath != "":
					destinationRow.State = StateQueued
				default:
					// No local payload exists, so preserve the canonical file
					// while root MOVE is pending and suppress background probes.
					destinationRow.State = StateCompleted
					destinationRow.CompletedAt = &now
					retryAt = treeHoldUntil
				}
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
			if canonicalDeleted(sourceRow) {
				continue
			}
			tombstoneMovedSource(sourceRow, now)
			if replicaTreeMove {
				sourceRow.RetryAt = &treeHoldUntil
				sourceRow.LastError = "waiting for canonical destination directory MOVE"
			}
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
		if err := advanceMutationFenceTree(tx, dst); err != nil {
			return err
		}
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
			if canonicalDeleted(sourceRow) {
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
				PathKey:        pathKey(newPath),
				ParentKey:      pathKey(newParent),
				Path:           newPath,
				Parent:         newParent,
				Name:           path.Base(newPath),
				IsDir:          sourceRow.IsDir,
				Size:           sourceRow.Size,
				ModTime:        sourceRow.ModTime,
				CreateTime:     sourceRow.CreateTime,
				ETag:           canonicalETag(pathKey(newPath), 1, sourceRow.Size),
				Generation:     1,
				CanonicalState: CanonicalStateAcked,
				DurableAt:      &now,
				State:          StateQueued,
				SpoolPath:      spoolPath,
				PayloadSHA1:    payloadSHA1,
				MimeType:       sourceRow.MimeType,
				RetryAt:        &retryAt,
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
	if err := db.GetDb().Where("path_key = ?", srcKey).Take(&srcRow).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, false, nil
		}
		return false, false, err
	}
	if canonicalDeleted(&srcRow) {
		return false, false, nil
	}
	if srcRow.IsDir {
		handled, overwritten, err = movePendingDirectory(src, dst, overwrite)
		if err == nil && handled {
			InvalidateProviderSnapshots(path.Dir(src), path.Dir(dst), src, dst)
		}
		return handled, overwritten, err
	}
	if srcRow.SpoolPath == "" && (srcRow.State != StateCompleted || !canonicalAcked(&srcRow)) {
		return false, false, nil
	}

	var oldDestinationSpool string
	now := time.Now()
	settleAt := now.Add(cloudSyncSettleDelay(srcRow.Size))
	err = db.GetDb().Transaction(func(tx *gorm.DB) error {
		if err := advanceMutationFenceTrees(tx, src, dst); err != nil {
			return err
		}
		var lockedSrc model.WebDAVWritebackObject
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", srcRow.ID).Take(&lockedSrc).Error; err != nil {
			return err
		}
		if lockedSrc.IsDir || canonicalDeleted(&lockedSrc) ||
			(lockedSrc.SpoolPath == "" && (lockedSrc.State != StateCompleted || !canonicalAcked(&lockedSrc))) {
			return gorm.ErrRecordNotFound
		}

		var dstRow model.WebDAVWritebackObject
		dstErr := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("path_key = ?", dstKey).Take(&dstRow).Error
		if dstErr != nil && !errors.Is(dstErr, gorm.ErrRecordNotFound) {
			return dstErr
		}
		if dstErr == nil {
			if dstRow.ID == lockedSrc.ID {
				return gorm.ErrRecordNotFound
			}
			destinationExists := !canonicalDeleted(&dstRow)
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
		markCanonicalAcked(&dstRow, now)
		dstRow.State = StateQueued
		dstRow.SpoolPath = lockedSrc.SpoolPath
		dstRow.PayloadSHA1 = lockedSrc.PayloadSHA1
		if dstRow.PayloadSHA1 == "" && lockedSrc.SpoolPath == "" {
			dstRow.PayloadSHA1 = canonicalContentSHA1(&lockedSrc)
		}
		dstRow.MimeType = lockedSrc.MimeType
		dstRow.CleanupPath = ""
		dstRow.LastError = ""
		dstRow.RetryCount = 0
		dstRow.VerifyCount = 0
		clearRemoteVerification(&dstRow)
		dstRow.RetryAt = &settleAt
		dstRow.CompletedAt = nil
		replicaMove := lockedSrc.SpoolPath == ""
		if replicaMove {
			// No payload is needed: Cloud Sync sees the destination now while
			// the old provider path is moved asynchronously by the worker.
			dstRow.CleanupPath = src
			dstRow.RetryAt = &now
		}

		if dstRow.ID == 0 {
			if err := tx.Create(&dstRow).Error; err != nil {
				return err
			}
		} else if err := tx.Save(&dstRow).Error; err != nil {
			return err
		}

		tombstoneMovedSource(&lockedSrc, now)
		if replicaMove {
			hold := now.Add(replicaMoveSourceHoldDelay())
			lockedSrc.RetryAt = &hold
			lockedSrc.LastError = "waiting for canonical destination replica move"
		}
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
	InvalidateProviderSnapshots(path.Dir(src), path.Dir(dst), src, dst)
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
	if err := db.GetDb().Where("path_key = ?", pathKey(src)).Take(&srcRow).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, false, nil
		}
		return false, false, err
	}
	if canonicalDeleted(&srcRow) {
		return false, false, nil
	}
	if srcRow.IsDir {
		handled, overwritten, err = copyPendingDirectory(src, dst, recursive)
		if err == nil && handled {
			InvalidateProviderSnapshots(path.Dir(dst), dst)
		}
		return handled, overwritten, err
	}
	if srcRow.SpoolPath == "" {
		return false, false, nil
	}

	var oldDestinationSpool string
	settleAt := time.Now().Add(cloudSyncSettleDelay(srcRow.Size))
	err = db.GetDb().Transaction(func(tx *gorm.DB) error {
		if err := advanceMutationFence(tx, dst); err != nil {
			return err
		}
		var lockedSrc model.WebDAVWritebackObject
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", srcRow.ID).Take(&lockedSrc).Error; err != nil {
			return err
		}
		if lockedSrc.IsDir || canonicalDeleted(&lockedSrc) || lockedSrc.SpoolPath == "" {
			return gorm.ErrRecordNotFound
		}

		dstKey := pathKey(dst)
		var dstRow model.WebDAVWritebackObject
		dstErr := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("path_key = ?", dstKey).Take(&dstRow).Error
		if dstErr != nil && !errors.Is(dstErr, gorm.ErrRecordNotFound) {
			return dstErr
		}

		if dstErr == nil {
			destinationExists := !canonicalDeleted(&dstRow)
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
		markCanonicalAcked(&dstRow, time.Now())
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
	InvalidateProviderSnapshots(path.Dir(dst), dst)
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
	markCanonicalAcked(row, now)
	row.State = StateCompleted
	row.SpoolPath = ""
	row.CleanupPath = ""
	row.LastError = ""
	row.ResolutionReason = ""
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
	markCanonicalAcked(row, now)
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
	row.ResolutionReason = ""
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
	if canonicalDeleted(source) {
		markCanonicalDeleted(row)
	} else {
		markCanonicalAcked(row, now)
	}
	row.SpoolPath = source.SpoolPath
	row.PayloadSHA1 = source.PayloadSHA1
	row.MimeType = source.MimeType
	row.CleanupPath = ""
	row.LastError = ""
	row.ResolutionReason = ""
	row.RetryCount = 0
	row.VerifyCount = 0
	clearRemoteVerification(row)

	switch {
	case canonicalDeleted(source):
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
		PathKey:        key,
		ParentKey:      pathKey(path.Dir(src)),
		Path:           src,
		Parent:         path.Dir(src),
		Name:           path.Base(src),
		IsDir:          isDir,
		Size:           size,
		ModTime:        modTime,
		CreateTime:     createTime,
		ETag:           canonicalETag(key, 1, size),
		Generation:     1,
		CanonicalState: CanonicalStateDeleted,
		State:          StateDeleted,
		RetryAt:        &now,
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
		if err := advanceMutationFenceTree(tx, dst); err != nil {
			return err
		}
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
			if canonicalDeleted(sourceRow) {
				markCanonicalDeleted(&destinationRow)
			} else {
				markCanonicalAcked(&destinationRow, now)
			}
			destinationRow.SpoolPath = sourceRow.SpoolPath
			destinationRow.PayloadSHA1 = sourceRow.PayloadSHA1
			destinationRow.MimeType = sourceRow.MimeType
			destinationRow.CleanupPath = ""
			destinationRow.LastError = ""
			destinationRow.RetryCount = 0
			destinationRow.VerifyCount = 0
			clearRemoteVerification(&destinationRow)

			switch {
			case canonicalDeleted(sourceRow):
				destinationRow.State = StateDeleted
				destinationRow.SpoolPath = ""
				destinationRow.PayloadSHA1 = ""
				destinationRow.RetryAt = &now
				destinationRow.CompletedAt = nil
			case sourceRow.State == StateCompleted:
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
		if err := advanceMutationFenceTrees(tx, src, dst); err != nil {
			return err
		}
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

func providerOperationMaintenanceDue(op *model.WebDAVProviderOperation, now time.Time) bool {
	if op == nil {
		return false
	}
	switch op.State {
	case ProviderOperationApplied:
		return true
	case ProviderOperationPrepared:
		return providerOperationPreparedExpired(op, now)
	case ProviderOperationStarted, ProviderOperationFailed:
		return providerOperationRecoveryDue(op, now)
	default:
		return false
	}
}

func loadProviderOperationMaintenanceCandidates(now time.Time, limit int) ([]model.WebDAVProviderOperation, error) {
	if limit <= 0 {
		return nil, nil
	}
	recoveryCutoff := now.Add(-providerOperationConfirmationDelay())
	preparedCutoff := now.Add(-providerOperationPreparedAbandonAfter)
	var ops []model.WebDAVProviderOperation
	err := db.GetDb().
		Where(
			"state = ? OR (state = ? AND updated_at <= ?) OR (state IN ? AND (last_checked_at IS NULL OR last_checked_at <= ?))",
			ProviderOperationApplied,
			ProviderOperationPrepared,
			preparedCutoff,
			[]string{ProviderOperationStarted, ProviderOperationFailed},
			recoveryCutoff,
		).
		Order("CASE state WHEN 'applied' THEN 0 WHEN 'prepared' THEN 1 ELSE 2 END").
		Order("last_checked_at asc").
		Order("id asc").
		Limit(limit).
		Find(&ops).Error
	return ops, err
}

func (m *workerManager) maintainProviderOperations() {
	now := time.Now()
	ops, err := loadProviderOperationMaintenanceCandidates(now, 64)
	if err != nil {
		log.Errorf("write-back provider operation scan failed: %v", err)
		return
	}
	if len(ops) == 0 {
		return
	}

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

		if op.State == ProviderOperationPrepared {
			if providerOperationPreparedExpired(op, now) {
				if err := FinishProviderOperation(op.ID); err != nil {
					recordProviderOperationError(op.ID, err)
				} else {
					retired++
				}
			}
			// PREPARED belongs to the request that is about to mutate the
			// provider. Never recover it before abandonment expiry, even if a
			// future query regression accidentally returns the fresh row.
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

type workerJob struct {
	id          uint
	upload      bool
	largeUpload bool
}

type providerRefreshCall struct {
	done chan struct{}
	objs []model.Obj
	err  error
}

type providerSnapshotEntry struct {
	objs        []model.Obj
	refreshedAt time.Time
}

const providerSnapshotMaxEntries = 128

type providerRefreshGroup struct {
	mu        sync.Mutex
	calls     map[string]*providerRefreshCall
	snapshots map[string]providerSnapshotEntry
	revisions map[string]uint64
}

func cloneProviderObjects(objs []model.Obj) []model.Obj {
	if len(objs) == 0 {
		return nil
	}
	return append([]model.Obj(nil), objs...)
}

func providerSnapshotTTL() time.Duration {
	seconds := 10 * 60
	if conf.Conf != nil {
		configured := conf.Conf.WebDAVWriteback.ProviderSnapshotTTLSeconds
		if configured < 0 {
			return 0
		}
		if configured > 0 {
			seconds = configured
		}
	}
	return time.Duration(seconds) * time.Second
}

func (g *providerRefreshGroup) cached(parent string, now time.Time) ([]model.Obj, bool) {
	ttl := providerSnapshotTTL()
	if ttl <= 0 {
		return nil, false
	}
	parent = utils.FixAndCleanPath(parent)
	g.mu.Lock()
	defer g.mu.Unlock()
	entry, ok := g.snapshots[parent]
	if !ok {
		return nil, false
	}
	if !now.Before(entry.refreshedAt.Add(ttl)) {
		delete(g.snapshots, parent)
		return nil, false
	}
	return cloneProviderObjects(entry.objs), true
}

func (g *providerRefreshGroup) storeSnapshotLocked(parent string, objs []model.Obj, now time.Time) {
	if providerSnapshotTTL() <= 0 {
		return
	}
	if g.snapshots == nil {
		g.snapshots = make(map[string]providerSnapshotEntry)
	}
	if _, exists := g.snapshots[parent]; !exists && len(g.snapshots) >= providerSnapshotMaxEntries {
		oldestKey := ""
		var oldest time.Time
		for key, entry := range g.snapshots {
			if oldestKey == "" || entry.refreshedAt.Before(oldest) {
				oldestKey = key
				oldest = entry.refreshedAt
			}
		}
		if oldestKey != "" {
			delete(g.snapshots, oldestKey)
		}
	}
	g.snapshots[parent] = providerSnapshotEntry{objs: cloneProviderObjects(objs), refreshedAt: now}
}

func (g *providerRefreshGroup) invalidate(parents ...string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.revisions == nil {
		g.revisions = make(map[string]uint64)
	}
	for _, parent := range parents {
		if parent == "" {
			continue
		}
		parent = utils.FixAndCleanPath(parent)
		delete(g.snapshots, parent)
		g.revisions[parent]++
	}
}

func (g *providerRefreshGroup) clear() {
	g.mu.Lock()
	g.snapshots = nil
	g.revisions = nil
	g.mu.Unlock()
}

func (g *providerRefreshGroup) do(stop <-chan struct{}, parent string, refresh func() ([]model.Obj, error)) ([]model.Obj, error) {
	parent = utils.FixAndCleanPath(parent)
	g.mu.Lock()
	if g.calls == nil {
		g.calls = make(map[string]*providerRefreshCall)
	}
	if call := g.calls[parent]; call != nil {
		g.mu.Unlock()
		select {
		case <-call.done:
			return call.objs, call.err
		case <-stop:
			return nil, context.Canceled
		}
	}
	revision := g.revisions[parent]
	call := &providerRefreshCall{done: make(chan struct{})}
	g.calls[parent] = call
	g.mu.Unlock()

	call.objs, call.err = refresh()
	if call.err == nil {
		g.mu.Lock()
		// A canonical mutation may invalidate this parent while Refresh:true is
		// in flight. The result can still serve its current caller and canonical
		// overlay, but a pre-mutation provider view must not repopulate the cache.
		if g.revisions[parent] == revision {
			g.storeSnapshotLocked(parent, call.objs, time.Now())
		}
		g.mu.Unlock()
	}
	close(call.done)

	g.mu.Lock()
	delete(g.calls, parent)
	g.mu.Unlock()
	return call.objs, call.err
}

var providerParentSnapshots providerRefreshGroup

// ProviderListForWebDAV serves ordinary directory revalidation from a recent
// successful fresh provider snapshot. Expired/missing snapshots perform one
// coalesced Refresh:true listing. Verification and divergence decisions do not
// use this cache; worker refreshParent always requests fresh provider evidence.
func ProviderListForWebDAV(ctx context.Context, parent string) ([]model.Obj, bool, error) {
	parent = utils.FixAndCleanPath(parent)
	if objs, ok := providerParentSnapshots.cached(parent, time.Now()); ok {
		return objs, true, nil
	}
	objs, err := providerParentSnapshots.do(ctx.Done(), parent, func() ([]model.Obj, error) {
		slots := currentProviderProbeSlots()
		reserved, err := acquireWorkerSlot(slots, ctx.Done())
		if err != nil {
			return nil, err
		}
		fresh, listErr := fs.List(ctx, parent, &fs.ListArgs{Refresh: true, NoLog: true})
		releaseWorkerSlot(slots, reserved)
		if listErr == nil {
			refreshFreshParentCompletedEvidence(parent, fresh, time.Now())
		}
		return fresh, listErr
	})
	return objs, false, err
}

func InvalidateProviderSnapshots(parents ...string) {
	providerParentSnapshots.invalidate(parents...)
}

type workerManager struct {
	ctx            context.Context
	cancel         context.CancelFunc
	stop           chan struct{}
	wake           chan struct{}
	jobs           chan workerJob
	uploads        chan struct{}
	largeUploads   chan struct{}
	providerProbes chan struct{}
	inflight       sync.Map
	batchCompleted sync.Map
	wg             sync.WaitGroup
}

func currentProviderProbeSlots() chan struct{} {
	managerMu.Lock()
	m := manager
	managerMu.Unlock()
	if m == nil {
		return nil
	}
	return m.providerProbes
}

func boundedWorkerLimit(workers, configured int) int {
	workers = max(1, workers)
	if configured <= 0 || configured >= workers {
		return workers
	}
	return max(1, configured)
}

func uploadWorkerLimit(workers, configured int) int {
	return boundedWorkerLimit(workers, configured)
}

func largeUploadWorkerLimit(workers, configured int) int {
	return boundedWorkerLimit(workers, configured)
}

func providerProbeWorkerLimit(workers, configured int) int {
	return boundedWorkerLimit(workers, configured)
}

func queuedReplicaMove(row *model.WebDAVWritebackObject) bool {
	return row != nil &&
		!row.IsDir &&
		row.State == StateQueued &&
		row.SpoolPath == "" &&
		row.CleanupPath != ""
}

func queuedReplicaTreeMove(row *model.WebDAVWritebackObject) bool {
	return row != nil &&
		row.IsDir &&
		row.State == StateQueued &&
		row.CleanupPath != ""
}

func stagedReplicaTreeRow(row *model.WebDAVWritebackObject) bool {
	return row != nil &&
		row.Generation > 0 &&
		row.RemoteVerifiedAt == nil &&
		row.RemoteGeneration == row.Generation
}

func providerUploadCandidate(row *model.WebDAVWritebackObject) bool {
	return row != nil &&
		!row.IsDir &&
		row.State == StateQueued &&
		!queuedReplicaMove(row)
}

func multipartProviderUploadCandidate(row *model.WebDAVWritebackObject) bool {
	return row != nil &&
		!row.IsDir &&
		row.State == StateQueued &&
		row.SpoolPath != "" &&
		row.Size > open115MultipartChunkSize
}

func largeProviderUploadCandidate(row *model.WebDAVWritebackObject, requireHash func(string) bool) bool {
	if !multipartProviderUploadCandidate(row) {
		return false
	}
	return requireHash != nil && requireHash(row.Path)
}

func tryReserveWorkerSlot(slots chan struct{}) (reserved bool, allowed bool) {
	if slots == nil {
		return false, true
	}
	select {
	case slots <- struct{}{}:
		return true, true
	default:
		return false, false
	}
}

func releaseWorkerSlot(slots chan struct{}, reserved bool) {
	if slots == nil || !reserved {
		return
	}
	<-slots
}

func workerQueueNeedsRefill(queued, capacity, workers int) bool {
	if capacity <= 0 {
		return true
	}
	workers = max(1, workers)
	threshold := min(capacity, workers*2)
	return queued < threshold
}

func acquireWorkerSlot(slots chan struct{}, stop <-chan struct{}) (bool, error) {
	if slots == nil {
		return false, nil
	}
	select {
	case slots <- struct{}{}:
		return true, nil
	case <-stop:
		return false, context.Canceled
	}
}

func (m *workerManager) refreshParent(parent string) ([]model.Obj, error) {
	return providerParentSnapshots.do(m.stop, parent, func() ([]model.Obj, error) {
		reserved, err := acquireWorkerSlot(m.providerProbes, m.stop)
		if err != nil {
			return nil, err
		}
		fresh, listErr := fs.List(m.ctx, parent, &fs.ListArgs{Refresh: true, NoLog: true})
		releaseWorkerSlot(m.providerProbes, reserved)
		if listErr == nil {
			refreshFreshParentCompletedEvidence(parent, fresh, time.Now())
		}
		return fresh, listErr
	})
}

func (m *workerManager) markBatchCompleted(id uint) {
	if _, active := m.inflight.Load(id); active {
		m.batchCompleted.Store(id, struct{}{})
	}
}

func (m *workerManager) consumeBatchCompleted(id uint) bool {
	_, ok := m.batchCompleted.LoadAndDelete(id)
	return ok
}

func (m *workerManager) inflightIDSet() map[uint]struct{} {
	ids := make(map[uint]struct{})
	m.inflight.Range(func(key, _ any) bool {
		if id, ok := key.(uint); ok {
			ids[id] = struct{}{}
		}
		return true
	})
	return ids
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
	providerParentSnapshots.clear()
	workerCtx, cancel := context.WithCancel(context.Background())
	workers := max(1, conf.Conf.WebDAVWriteback.Workers)
	uploadWorkers := uploadWorkerLimit(workers, conf.Conf.WebDAVWriteback.UploadWorkers)
	largeUploadWorkers := largeUploadWorkerLimit(workers, conf.Conf.WebDAVWriteback.LargeUploadWorkers)
	providerProbeWorkers := providerProbeWorkerLimit(workers, conf.Conf.WebDAVWriteback.ProviderProbeWorkers)
	var uploads chan struct{}
	if uploadWorkers < workers {
		uploads = make(chan struct{}, uploadWorkers)
	}
	var largeUploads chan struct{}
	if largeUploadWorkers < workers {
		largeUploads = make(chan struct{}, largeUploadWorkers)
	}
	var providerProbes chan struct{}
	if providerProbeWorkers < workers {
		providerProbes = make(chan struct{}, providerProbeWorkers)
	}
	m := &workerManager{
		ctx:            workerCtx,
		cancel:         cancel,
		stop:           make(chan struct{}),
		wake:           make(chan struct{}, 1),
		jobs:           make(chan workerJob, max(4, workers*4)),
		uploads:        uploads,
		largeUploads:   largeUploads,
		providerProbes: providerProbes,
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
		for i := 0; i < workers; i++ {
			m.wg.Add(1)
			go m.worker()
		}
		m.wg.Add(1)
		go m.completedCanonicalLoop()
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
	providerParentSnapshots.clear()
}

const missingDurablePayloadRecoveryMessage = "durable spool is missing after restart; verifying provider before Cloud Sync repair"

func markMissingDurablePayloadForVerification(row *model.WebDAVWritebackObject, now time.Time, message string) error {
	if row == nil {
		return nil
	}
	if message == "" {
		message = missingDurablePayloadRecoveryMessage
	}
	return db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("id = ? AND generation = ? AND state IN ? AND cleanup_path = ''",
			row.ID, row.Generation, []string{StateQueued, StateVerifying}).
		Updates(map[string]any{
			"state":               StateVerifying,
			"retry_at":            &now,
			"verify_count":        0,
			"last_error":          message,
			"recovery_started_at": gorm.Expr("COALESCE(recovery_started_at, ?)", now),
		}).Error
}

func (m *workerManager) recoverMissingDurablePayloads(now time.Time) error {
	var rows []model.WebDAVWritebackObject
	if err := db.GetDb().
		Select("id", "generation", "path", "is_dir", "size", "spool_path", "cleanup_path", "canonical_state", "state").
		Where("is_dir = ? AND cleanup_path = '' AND state IN ?", false, []string{StateQueued, StateVerifying}).
		Where("canonical_state = ?", CanonicalStateAcked).
		Find(&rows).Error; err != nil {
		return err
	}

	for i := range rows {
		row := &rows[i]
		availability, err := inspectDurableLocalPayload(row)
		if err != nil {
			log.Warnf("write-back restart spool audit could not inspect %s: %v", row.Path, err)
			continue
		}
		if availability != durablePayloadMissing {
			continue
		}
		if err := markMissingDurablePayloadForVerification(row, now, "durable spool is missing after restart; checking provider before exposing loss to Cloud Sync"); err != nil {
			return err
		}
	}
	return nil
}

func forceCloudSyncRepairForMissingPayload(row *model.WebDAVWritebackObject, currentState, reason string) bool {
	return requireCloudSyncReupload(
		row,
		currentState,
		ResolutionNeedsCloudSyncRehydrate,
		reason,
		0,
		true,
	)
}
func (m *workerManager) recoverInterrupted() error {
	// Older builds could persist provider divergence as a completed/manual-check
	// result. Under the current model these generations are terminal for provider
	// workers and must wait for a fresh Cloud Sync PUT instead.
	if err := db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("state = ? AND resolution_reason IN ?", StateCompleted, []string{
			ResolutionRemoteHashMismatch,
			ResolutionRemoteMissing,
			ResolutionVerificationExhausted,
		}).
		Updates(map[string]any{
			"canonical_state":              CanonicalStateAcked,
			"state":                        StateWaitingCloudSyncReupload,
			"cloud_sync_reupload_required": true,
			"retry_at":                     nil,
			"completed_at":                 nil,
			"remote_generation":            0,
			"remote_verified_at":           nil,
			"recovery_started_at":          gorm.Expr("COALESCE(recovery_started_at, updated_at, created_at)"),
		}).Error; err != nil {
		return err
	}

	if err := db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("state = ? AND resolution_reason IN ?", StateWaitingRepair, []string{
			ResolutionRemoteHashMismatch,
			ResolutionRemoteMissing,
			ResolutionVerificationExhausted,
		}).
		Updates(map[string]any{
			"canonical_state":              CanonicalStateAcked,
			"state":                        StateWaitingCloudSyncReupload,
			"cloud_sync_reupload_required": true,
			"retry_at":                     nil,
			"recovery_started_at":          gorm.Expr("COALESCE(recovery_started_at, updated_at, created_at)"),
		}).Error; err != nil {
		return err
	}

	if err := db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("state = ? AND canonical_state = ? AND (resolution_reason = ? OR LOWER(last_error) LIKE ?)",
			StateDeleted, CanonicalStateDeleted, ResolutionNeedsCloudSyncRehydrate, "%cloud sync can re-upload%").
		Updates(map[string]any{
			"state":             StateWaitingCloudSyncReupload,
			"retry_at":          nil,
			"resolution_reason": ResolutionNeedsCloudSyncRehydrate,
		}).Error; err != nil {
		return err
	}

	if err := db.GetDb().Where("lease_until <= ?", time.Now()).
		Delete(&model.WebDAVWritebackReceiveReservation{}).Error; err != nil {
		return err
	}

	// Older builds exposed completed-provider recheck evidence through LastError,
	// which made a healthy transient confirmation look like a red failure in the
	// admin monitor. Keep verify_count/retry_at as the durable recheck evidence
	// and clear only those exact legacy diagnostic strings.
	if err := db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("state = ? AND last_error IN ?", StateCompleted, []string{
			"remote divergence observed once; waiting for force-refreshed confirmation",
			"remote divergence fresh confirmation claimed; throttling additional provider refreshes",
		}).
		Update("last_error", "").Error; err != nil {
		return err
	}

	// Backfill the client-visible lifecycle for rows created before the ACK-state
	// split. This does not change the existing provider replication State.
	if err := db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("canonical_state = ?", canonicalStateLegacyAck).
		Updates(map[string]any{
			"canonical_state": CanonicalStateAcked,
			"ack_time":        gorm.Expr("COALESCE(ack_time, durable_at, created_at)"),
			"durable_at":      gorm.Expr("COALESCE(durable_at, ack_time, created_at)"),
		}).Error; err != nil {
		return err
	}
	var legacyHighRetryRows []model.WebDAVWritebackObject
	if err := db.GetDb().
		Select(
			"id", "generation", "state", "is_dir", "cleanup_path",
			"retry_count", "verify_count", "last_error", "resolution_reason",
			"provider_upload_completed_at", "remote_object_id", "remote_sha1",
			"provider_evidence_count",
		).
		Where("state IN ? AND retry_count > 1", []string{
			StateQueued,
			legacyStateFailed,
			StateUploading,
			StateVerifying,
		}).
		Find(&legacyHighRetryRows).Error; err != nil {
		return err
	}
	legacyVerifyNow := time.Now()
	for i := range legacyHighRetryRows {
		row := &legacyHighRetryRows[i]
		if !legacyExhaustedRetryNeedsFreshVerification(row) {
			continue
		}
		if err := db.GetDb().Model(&model.WebDAVWritebackObject{}).
			Where("id = ? AND generation = ? AND state = ?", row.ID, row.Generation, row.State).
			Updates(map[string]any{
				"canonical_state":              CanonicalStateAcked,
				"state":                        StateVerifying,
				"cloud_sync_reupload_required": false,
				"retry_at":                     &legacyVerifyNow,
				"verify_count":                 0,
				"last_error":                   "legacy exhausted provider retry budget; revalidating fresh provider evidence before any further mutation",
				"resolution_reason":            "",
				"completed_at":                 nil,
				"remote_object_id":             "",
				"remote_sha1":                  "",
				"remote_generation":            0,
				"remote_verified_at":           nil,
				"provider_evidence_first_at":   nil,
				"provider_evidence_last_at":    nil,
				"provider_evidence_count":      0,
				"provider_evidence_result":     "",
				"recovery_started_at":          gorm.Expr("COALESCE(recovery_started_at, updated_at, created_at)"),
			}).Error; err != nil {
			return err
		}
	}

	if err := db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("state = ?", legacyStateFailed).
		Updates(map[string]any{
			"state":    StateQueued,
			"retry_at": gorm.Expr("COALESCE(retry_at, CURRENT_TIMESTAMP)"),
		}).Error; err != nil {
		return err
	}
	if err := db.GetDb().Model(&model.WebDAVWritebackReceiveFence{}).
		Where("active_receivers > 0 AND receive_lease_until IS NOT NULL AND receive_lease_until <= ?", time.Now()).
		Updates(map[string]any{
			"active_receivers":    0,
			"receive_lease_until": nil,
		}).Error; err != nil {
		return err
	}
	if err := db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("canonical_state = '' OR canonical_state IS NULL").
		Where("state = ?", StateDeleted).
		Update("canonical_state", CanonicalStateDeleted).Error; err != nil {
		return err
	}
	if err := db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("canonical_state = '' OR canonical_state IS NULL").
		Where("state = ?", StateLockNull).
		Update("canonical_state", CanonicalStateLockNull).Error; err != nil {
		return err
	}
	if err := db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("canonical_state = '' OR canonical_state IS NULL").
		Where("state NOT IN ?", []string{StateDeleted, StateLockNull}).
		Updates(map[string]any{
			"canonical_state": CanonicalStateAcked,
			"ack_time":        gorm.Expr("COALESCE(ack_time, durable_at, created_at)"),
			"durable_at":      gorm.Expr("COALESCE(durable_at, ack_time, created_at)"),
		}).Error; err != nil {
		return err
	}
	if err := db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("state = ?", StateWaitingCloudSyncReupload).
		Updates(map[string]any{
			"canonical_state":              CanonicalStateAcked,
			"cloud_sync_reupload_required": true,
			"retry_at":                     nil,
			"recovery_started_at":          gorm.Expr("COALESCE(recovery_started_at, updated_at, created_at)"),
		}).Error; err != nil {
		return err
	}

	// LockSystem is process-local, so lock-null shadows cannot survive a
	// restart without their corresponding lock token.
	if err := db.GetDb().Where("state = ?", StateLockNull).Delete(&model.WebDAVWritebackObject{}).Error; err != nil {
		return err
	}

	// Replica MOVE control jobs have no payload spool. After a crash they must
	// return to their MOVE state machine, which first checks destination/source
	// evidence and can recover an already-applied provider mutation safely.
	// Treating them as ordinary uploads would add a wrong verification phase.
	now := time.Now()
	if err := db.GetDb().Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&model.WebDAVWritebackObject{}).
			Where("state IN ? AND spool_path = '' AND cleanup_path <> ''", []string{StateUploading, StateVerifying}).
			Updates(map[string]any{
				"state":        StateQueued,
				"retry_at":     &now,
				"verify_count": 0,
				"last_error":   "resuming interrupted provider MOVE control job",
			}).Error; err != nil {
			return err
		}

		// Re-establish the old-source tombstone hold before workers start. This
		// prevents DELETE priority from removing the physical provider source in
		// the small restart window before the MOVE control job is dispatched.
		var moves []model.WebDAVWritebackObject
		if err := tx.Select("is_dir", "cleanup_path").
			Where("state = ? AND spool_path = '' AND cleanup_path <> ''", StateQueued).
			Find(&moves).Error; err != nil {
			return err
		}
		holdUntil := now.Add(replicaMoveSourceHoldDelay())
		for i := range moves {
			move := &moves[i]
			src := utils.FixAndCleanPath(move.CleanupPath)
			if src == "" {
				continue
			}
			query := tx.Model(&model.WebDAVWritebackObject{}).
				Where("state = ?", StateDeleted)
			if move.IsDir {
				query = query.Where("path = ? OR path LIKE ? ESCAPE '~'", src, descendantLikePattern(src))
			} else {
				query = query.Where("path_key = ?", pathKey(src))
			}
			if err := query.Updates(map[string]any{
				"retry_at":   &holdUntil,
				"last_error": "waiting for recovered canonical MOVE",
			}).Error; err != nil {
				return err
			}
		}

		// An ordinary UPLOADING file remains ambiguous after a crash: the
		// provider may already contain the exact payload. Resume it in VERIFYING.
		// Ordinary directories re-enter MKCOL directly.
		if err := tx.Model(&model.WebDAVWritebackObject{}).
			Where("state = ? AND is_dir = ? AND cleanup_path = ''", StateUploading, true).
			Updates(map[string]any{
				"state":        StateQueued,
				"retry_at":     &now,
				"verify_count": 0,
				"last_error":   "re-queued interrupted directory creation",
			}).Error; err != nil {
			return err
		}
		return tx.Model(&model.WebDAVWritebackObject{}).
			Where("state = ? AND is_dir = ? AND cleanup_path = ''", StateUploading, false).
			Updates(map[string]any{
				"state":                   StateVerifying,
				"retry_at":                &now,
				"verify_count":            0,
				"restart_upload_recovery": true,
				"last_error":              "resuming remote verification after interrupted upload",
			}).Error
	}); err != nil {
		return err
	}

	// A normal restart should continue from the persistent spool. If the spool
	// disappeared unexpectedly, never keep advertising the ACK forever: verify
	// the provider first, then either converge remotely or expose a tombstone so
	// Cloud Sync can re-PUT the source.
	return m.recoverMissingDurablePayloads(now)
}

const (
	completedCanonicalReconcileEvery      = 30 * time.Second
	completedCanonicalReconcileBatchLimit = 32
)

func completedCanonicalReconcileDue(row *model.WebDAVWritebackObject, now time.Time) bool {
	if row == nil ||
		row.State != StateCompleted ||
		row.SpoolPath != "" ||
		!canonicalAcked(row) {
		return false
	}
	if row.RetryAt != nil && now.Before(*row.RetryAt) {
		return false
	}
	if row.IsDir {
		return directoryShadowExpired(row, now)
	}
	return !completedRemoteVerificationFresh(row, now)
}

func loadCompletedCanonicalReconcileCandidates(now time.Time, limit int) ([]model.WebDAVWritebackObject, error) {
	if limit <= 0 {
		return nil, nil
	}
	probeCutoff := now.Add(-completedRemoteVerificationInterval())
	graceSeconds := 1
	if conf.Conf != nil {
		graceSeconds = max(1, conf.Conf.WebDAVWriteback.DirectoryGraceSeconds)
	}
	directoryCutoff := now.Add(-time.Duration(graceSeconds) * time.Second)

	var rows []model.WebDAVWritebackObject
	err := db.GetDb().
		Where("state = ? AND spool_path = ''", StateCompleted).
		Where("(canonical_state = ? OR canonical_state = '' OR canonical_state IS NULL)", CanonicalStateAcked).
		Where("(retry_at IS NULL OR retry_at <= ?)", now).
		Where(
			"(is_dir = ? AND completed_at IS NOT NULL AND completed_at <= ?) OR "+
				"(is_dir = ? AND (remote_verified_at IS NULL OR remote_verified_at <= ? OR remote_generation <> generation OR verify_count <> 0 OR last_error <> ''))",
			true, directoryCutoff,
			false, probeCutoff,
		).
		Order("retry_at asc").
		Order("remote_verified_at asc").
		Order("id asc").
		Limit(limit).
		Find(&rows).Error
	return rows, err
}

func (m *workerManager) maintainCompletedCanonical() {
	now := time.Now()
	rows, err := loadCompletedCanonicalReconcileCandidates(now, completedCanonicalReconcileBatchLimit)
	if err != nil {
		log.Errorf("write-back completed canonical reconcile scan failed: %v", err)
		return
	}
	for i := range rows {
		row := &rows[i]
		if !completedCanonicalReconcileDue(row, now) {
			continue
		}
		reserved, slotErr := acquireWorkerSlot(m.providerProbes, m.stop)
		if slotErr != nil {
			return
		}
		_, reconcileErr := ReconcileDirect(m.ctx, row.Path)
		releaseWorkerSlot(m.providerProbes, reserved)
		if reconcileErr != nil && !errors.Is(reconcileErr, context.Canceled) {
			log.Errorf("write-back background canonical reconcile failed for %s: %v", row.Path, reconcileErr)
		}
	}
}

func (m *workerManager) completedCanonicalLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(completedCanonicalReconcileEvery)
	defer ticker.Stop()
	for {
		select {
		case <-m.stop:
			return
		case <-ticker.C:
			m.maintainCompletedCanonical()
		}
	}
}

func cleanupExpiredLockNull(now time.Time) {
	if err := db.GetDb().
		Where("state = ? AND retry_at IS NOT NULL AND retry_at <= ?", StateLockNull, now).
		Delete(&model.WebDAVWritebackObject{}).Error; err != nil {
		log.Errorf("write-back lock-null cleanup failed: %v", err)
	}
}

func cleanupExpiredReceiveReservations(now time.Time) {
	if err := db.GetDb().
		Where("lease_until <= ?", now).
		Delete(&model.WebDAVWritebackReceiveReservation{}).Error; err != nil {
		log.Errorf("write-back expired receive reservation cleanup failed: %v", err)
	}
}

func dispatchWorkDue(now time.Time) (bool, error) {
	var row model.WebDAVWritebackObject
	err := db.GetDb().
		Select("id").
		Where("state IN ? AND (retry_at IS NULL OR retry_at <= ?)", []string{StateDeleted, StateVerifying, StateQueued}, now).
		Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (m *workerManager) scheduler() {
	ticker := time.NewTicker(2 * time.Second)
	providerTicker := time.NewTicker(providerOperationMaintenanceEvery)
	cleanupTicker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	defer providerTicker.Stop()
	defer cleanupTicker.Stop()

	idle := false
	for {
		if !idle {
			idle = !m.dispatch()
		}
		select {
		case <-m.stop:
			return
		case <-m.wake:
			idle = false
		case now := <-ticker.C:
			cleanupExpiredLockNull(now)
			if idle {
				due, err := dispatchWorkDue(now)
				if err != nil {
					log.Errorf("write-back idle queue probe failed: %v", err)
					idle = false
				} else if due {
					idle = false
				}
			}
		case <-providerTicker.C:
			m.maintainProviderOperations()
			// Provider recovery can make canonical work runnable.
			idle = false
		case now := <-cleanupTicker.C:
			cleanupExpiredReceiveReservations(now)
			m.cleanupCompleted()
		}
	}
}

const dispatchSmallBurst = 3

func dispatchFilePriority(row *model.WebDAVWritebackObject) int {
	if row != nil && row.Size > open115MultipartChunkSize {
		return 1
	}
	return 0
}

func fairDispatchFiles(rows []model.WebDAVWritebackObject) []model.WebDAVWritebackObject {
	if len(rows) < 2 {
		return rows
	}
	small := make([]model.WebDAVWritebackObject, 0, len(rows))
	large := make([]model.WebDAVWritebackObject, 0, len(rows))
	for i := range rows {
		if dispatchFilePriority(&rows[i]) == 0 {
			small = append(small, rows[i])
		} else {
			large = append(large, rows[i])
		}
	}
	if len(small) == 0 || len(large) == 0 {
		return rows
	}
	out := make([]model.WebDAVWritebackObject, 0, len(rows))
	si, li := 0, 0
	for si < len(small) || li < len(large) {
		for n := 0; n < dispatchSmallBurst && si < len(small); n++ {
			out = append(out, small[si])
			si++
		}
		if li < len(large) {
			out = append(out, large[li])
			li++
		}
		if si >= len(small) {
			out = append(out, large[li:]...)
			break
		}
	}
	return out
}

func filterDispatchExcludedIDs(rows []model.WebDAVWritebackObject, excludedIDs map[uint]struct{}, limit int) []model.WebDAVWritebackObject {
	if len(rows) == 0 || limit <= 0 {
		return nil
	}
	if len(excludedIDs) == 0 {
		if len(rows) > limit {
			return rows[:limit]
		}
		return rows
	}
	filtered := rows[:0]
	for i := range rows {
		if _, skip := excludedIDs[rows[i].ID]; skip {
			continue
		}
		filtered = append(filtered, rows[i])
		if len(filtered) >= limit {
			break
		}
	}
	return filtered
}

func loadDispatchClass(now time.Time, states []string, isDir *bool, excludedIDs map[uint]struct{}, limit int) ([]model.WebDAVWritebackObject, error) {
	var rows []model.WebDAVWritebackObject
	query := db.GetDb().
		Select("id", "path", "parent_key", "size", "is_dir", "state").
		Where("state IN ? AND (retry_at IS NULL OR retry_at <= ?)", states, now)
	if isDir != nil {
		query = query.Where("is_dir = ?", *isDir)
	}
	// Keep the indexed dispatch predicate stable. A dynamic NOT IN list changes
	// SQL shape every wake and can degrade the queue index plan. Overfetch by the
	// tiny inflight set, then remove those IDs in memory; LoadOrStore remains the
	// final race-safe claim.
	scanLimit := max(1, limit+len(excludedIDs))
	err := query.
		Order("retry_at asc").
		Order("updated_at asc").
		Order("id asc").
		Limit(scanLimit).
		Find(&rows).Error
	if err != nil {
		return nil, err
	}
	return filterDispatchExcludedIDs(rows, excludedIDs, limit), nil
}

func appendDispatchRows(rows, incoming []model.WebDAVWritebackObject, remaining int) ([]model.WebDAVWritebackObject, int) {
	if remaining <= 0 || len(incoming) == 0 {
		return rows, remaining
	}
	if len(incoming) > remaining {
		incoming = incoming[:remaining]
	}
	rows = append(rows, incoming...)
	return rows, remaining - len(incoming)
}

func filterDispatchReadyParents(rows, parents []model.WebDAVWritebackObject) []model.WebDAVWritebackObject {
	if len(rows) == 0 || len(parents) == 0 {
		return rows
	}
	byKey := make(map[string]*model.WebDAVWritebackObject, len(parents))
	for i := range parents {
		parent := &parents[i]
		byKey[parent.PathKey] = parent
	}
	ready := rows[:0]
	for i := range rows {
		row := rows[i]
		parent := byKey[row.ParentKey]
		if parent == nil {
			// No canonical parent means the backing provider remains the
			// authority for the parent path, matching the worker fallback.
			ready = append(ready, row)
			continue
		}
		if parent.IsDir && !canonicalDeleted(parent) && parent.State == StateCompleted {
			ready = append(ready, row)
		}
	}
	return ready
}

func dispatchReadyParents(rows []model.WebDAVWritebackObject) ([]model.WebDAVWritebackObject, error) {
	if len(rows) == 0 {
		return rows, nil
	}
	keys := make([]string, 0, len(rows))
	seen := make(map[string]struct{}, len(rows))
	for i := range rows {
		key := rows[i].ParentKey
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		return rows, nil
	}

	var parents []model.WebDAVWritebackObject
	if err := db.GetDb().
		Select("path_key", "is_dir", "state", "canonical_state").
		Where("path_key IN ?", keys).
		Find(&parents).Error; err != nil {
		return nil, err
	}
	return filterDispatchReadyParents(rows, parents), nil
}

func filterRootDeletedRows(rows []model.WebDAVWritebackObject, deletedAncestorKeys map[string]struct{}) (ready []model.WebDAVWritebackObject, blockedIDs []uint) {
	if len(rows) == 0 || len(deletedAncestorKeys) == 0 {
		return rows, nil
	}
	ready = make([]model.WebDAVWritebackObject, 0, len(rows))
	blockedIDs = make([]uint, 0)
	for i := range rows {
		row := rows[i]
		blocked := false
		for _, key := range ancestorPathKeys(row.Path) {
			if _, ok := deletedAncestorKeys[key]; ok {
				blocked = true
				break
			}
		}
		if blocked {
			blockedIDs = append(blockedIDs, row.ID)
			continue
		}
		ready = append(ready, row)
	}
	return ready, blockedIDs
}

func dispatchRootDeletedRows(rows []model.WebDAVWritebackObject, now time.Time) ([]model.WebDAVWritebackObject, error) {
	if len(rows) == 0 {
		return rows, nil
	}
	ancestorKeys := make([]string, 0, len(rows)*2)
	seen := make(map[string]struct{}, len(rows)*2)
	for i := range rows {
		for _, key := range ancestorPathKeys(rows[i].Path) {
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			ancestorKeys = append(ancestorKeys, key)
		}
	}
	if len(ancestorKeys) == 0 {
		return rows, nil
	}

	var ancestors []model.WebDAVWritebackObject
	if err := db.GetDb().
		Select("path_key").
		Where("state = ? AND path_key IN ?", StateDeleted, ancestorKeys).
		Find(&ancestors).Error; err != nil {
		return nil, err
	}
	deletedAncestorKeys := make(map[string]struct{}, len(ancestors))
	for i := range ancestors {
		deletedAncestorKeys[ancestors[i].PathKey] = struct{}{}
	}
	ready, blockedIDs := filterRootDeletedRows(rows, deletedAncestorKeys)
	if len(blockedIDs) > 0 {
		next := now.Add(deleteAncestorWaitDelay())
		if err := db.GetDb().Model(&model.WebDAVWritebackObject{}).
			Where("id IN ? AND state = ?", blockedIDs, StateDeleted).
			Updates(map[string]any{
				"retry_at":   &next,
				"last_error": "waiting for deleted ancestor to remove provider subtree",
			}).Error; err != nil {
			return nil, err
		}
	}
	return ready, nil
}

func loadDispatchRows(now time.Time, workers, budget int, excludedIDs map[uint]struct{}) ([]model.WebDAVWritebackObject, error) {
	if budget <= 0 {
		return nil, nil
	}
	classLimit := max(4, workers*2)
	isDir := true
	isFile := false
	rows := make([]model.WebDAVWritebackObject, 0, budget)
	remaining := budget

	loadClass := func(states []string, dir *bool) ([]model.WebDAVWritebackObject, error) {
		return loadDispatchClass(now, states, dir, excludedIDs, min(remaining, classLimit))
	}

	deleteScanLimit := max(min(remaining, classLimit), min(classLimit*4, max(remaining, workers*4)))
	deleted, err := loadDispatchClass(now, []string{StateDeleted}, nil, excludedIDs, deleteScanLimit)
	if err != nil {
		return nil, err
	}
	deleted, err = dispatchRootDeletedRows(deleted, now)
	if err != nil {
		return nil, err
	}
	rows, remaining = appendDispatchRows(rows, deleted, remaining)
	if remaining == 0 {
		return rows, nil
	}

	verifying, err := loadClass([]string{StateVerifying}, nil)
	if err != nil {
		return nil, err
	}
	rows, remaining = appendDispatchRows(rows, verifying, remaining)
	if remaining == 0 {
		return rows, nil
	}

	directoryScanLimit := max(min(remaining, classLimit), min(classLimit*4, max(remaining, workers*4)))
	directories, err := loadDispatchClass(now, []string{StateQueued}, &isDir, excludedIDs, directoryScanLimit)
	if err != nil {
		return nil, err
	}
	directories, err = dispatchReadyParents(directories)
	if err != nil {
		return nil, err
	}
	rows, remaining = appendDispatchRows(rows, directories, remaining)
	if remaining == 0 {
		return rows, nil
	}

	// Fetch a wider file window than the remaining queue capacity so the
	// 3-small:1-large fairness pass can still see multipart candidates, but
	// never enqueue more work than the job channel can accept.
	fileScanLimit := max(remaining, remaining*(dispatchSmallBurst+1))
	files, err := loadDispatchClass(now, []string{StateQueued}, &isFile, excludedIDs, fileScanLimit)
	if err != nil {
		return nil, err
	}
	files, err = dispatchReadyParents(files)
	if err != nil {
		return nil, err
	}
	files = fairDispatchFiles(files)
	rows, _ = appendDispatchRows(rows, files, remaining)
	return rows, nil
}

func (m *workerManager) dispatch() bool {
	freeJobs := cap(m.jobs) - len(m.jobs)
	if freeJobs <= 0 {
		return true
	}
	now := time.Now()
	rows, err := loadDispatchRows(now, max(1, conf.Conf.WebDAVWriteback.Workers), freeJobs, m.inflightIDSet())
	if err != nil {
		log.Errorf("write-back queue scan failed: %v", err)
		return true
	}
	if len(rows) == 0 {
		return false
	}
	for i := range rows {
		row := &rows[i]
		id := row.ID
		if _, loaded := m.inflight.LoadOrStore(id, struct{}{}); loaded {
			continue
		}
		job := workerJob{id: id}
		if providerUploadCandidate(row) {
			reserved, allowed := tryReserveWorkerSlot(m.uploads)
			if !allowed {
				m.inflight.Delete(id)
				continue
			}
			job.upload = reserved
			if largeProviderUploadCandidate(row, providerRequiresPayloadHash) {
				largeReserved, largeAllowed := tryReserveWorkerSlot(m.largeUploads)
				if !largeAllowed {
					releaseWorkerSlot(m.uploads, job.upload)
					m.inflight.Delete(id)
					continue
				}
				job.largeUpload = largeReserved
			}
		}
		select {
		case m.jobs <- job:
		default:
			releaseWorkerSlot(m.largeUploads, job.largeUpload)
			releaseWorkerSlot(m.uploads, job.upload)
			m.inflight.Delete(id)
			return true
		}
	}
	return true
}

func (m *workerManager) worker() {
	defer m.wg.Done()
	for {
		select {
		case <-m.stop:
			return
		case job := <-m.jobs:
			if !m.consumeBatchCompleted(job.id) {
				m.processSafely(job.id)
			}
			releaseWorkerSlot(m.largeUploads, job.largeUpload)
			releaseWorkerSlot(m.uploads, job.upload)
			m.inflight.Delete(job.id)
			m.batchCompleted.Delete(job.id)
			if workerQueueNeedsRefill(len(m.jobs), cap(m.jobs), conf.Conf.WebDAVWriteback.Workers) {
				select {
				case m.wake <- struct{}{}:
				default:
				}
			}
		}
	}
}

func (m *workerManager) processSafely(id uint) {
	defer func() {
		if recovered := recover(); recovered != nil {
			stack := debug.Stack()
			log.Errorf("write-back worker panic: id=%d panic=%v\n%s", id, recovered, stack)

			var row model.WebDAVWritebackObject
			if err := db.GetDb().Take(&row, id).Error; err != nil {
				log.Errorf("write-back worker panic recovery could not load id=%d: %v", id, err)
				return
			}
			panicErr := fmt.Errorf("write-back worker panic: %v", recovered)
			switch row.State {
			case StateDeleted:
				m.failDeleted(&row, panicErr)
			case StateQueued, StateUploading, StateVerifying:
				m.fail(&row, panicErr)
			}
		}
	}()
	m.process(id)
}

func (m *workerManager) process(id uint) {
	var row model.WebDAVWritebackObject
	if err := db.GetDb().Take(&row, id).Error; err != nil {
		return
	}
	switch row.State {
	case StateDeleted:
		m.processDelete(&row)
	case StateVerifying:
		m.processVerify(&row)
	case StateQueued:
		if row.IsDir {
			m.processMkdir(&row)
		} else {
			m.processUpload(&row)
		}
	}
}

func canonicalParentBlocksChild(parent *model.WebDAVWritebackObject) bool {
	return parent != nil && (!parent.IsDir || canonicalDeleted(parent))
}

func (m *workerManager) waitForCanonicalParent(row *model.WebDAVWritebackObject) (bool, error) {
	parent, err := getCanonicalByPath(row.Parent)
	if err != nil {
		return false, err
	}
	if parent == nil {
		return false, nil
	}
	if canonicalParentBlocksChild(parent) {
		next := time.Now().Add(2 * time.Second)
		res := db.GetDb().Model(&model.WebDAVWritebackObject{}).
			Where("id = ? AND generation = ? AND state IN ?", row.ID, row.Generation, []string{StateQueued}).
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
		Where("id = ? AND generation = ? AND state IN ?", row.ID, row.Generation, []string{StateQueued}).
		Update("retry_at", &next)
	return true, res.Error
}

func (m *workerManager) remoteDirectoryExists(row *model.WebDAVWritebackObject) bool {
	if existing, err := fs.Get(m.ctx, row.Path, &fs.GetArgs{NoLog: true}); err == nil && existing != nil && existing.IsDir() {
		return true
	}
	objs, err := m.refreshParent(row.Parent)
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
	return canonicalDeleted(current) || current.Path != uploadedPath
}

func holdReplicaMoveSourceTree(src string, until time.Time) {
	src = utils.FixAndCleanPath(src)
	_ = db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("state = ? AND (path = ? OR path LIKE ? ESCAPE '~')", StateDeleted, src, descendantLikePattern(src)).
		Where("retry_at IS NULL OR retry_at < ?", until).
		Updates(map[string]any{
			"retry_at":   &until,
			"last_error": "waiting for canonical destination directory MOVE",
		}).Error
}

func holdReplicaMoveDestinationTree(dst string, until time.Time) {
	dst = utils.FixAndCleanPath(dst)
	_ = db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("state = ? AND spool_path = '' AND remote_verified_at IS NULL AND remote_generation = generation", StateCompleted).
		Where("path = ? OR path LIKE ? ESCAPE '~'", dst, descendantLikePattern(dst)).
		Update("retry_at", &until).Error
}

func releaseReplicaMoveSourceTree(src string, now time.Time) {
	src = utils.FixAndCleanPath(src)
	_ = db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("state = ? AND (path = ? OR path LIKE ? ESCAPE '~')", StateDeleted, src, descendantLikePattern(src)).
		Updates(map[string]any{
			"retry_at":   &now,
			"last_error": "",
		}).Error
}

func loadReplicaTreeRows(root string) ([]model.WebDAVWritebackObject, error) {
	root = utils.FixAndCleanPath(root)
	var rows []model.WebDAVWritebackObject
	err := db.GetDb().
		Where("path = ? OR path LIKE ? ESCAPE '~'", root, descendantLikePattern(root)).
		Order("path asc").
		Find(&rows).Error
	return rows, err
}

func providerListingHasUnexpectedName(objs []model.Obj, allowed map[string]struct{}) bool {
	for _, obj := range objs {
		if obj == nil {
			continue
		}
		if _, ok := allowed[obj.GetName()]; !ok {
			return true
		}
	}
	return false
}

func (m *workerManager) replicaTreeProviderState(canonicalRoot, providerRoot string) (providerOperationRemoteState, error) {
	canonicalRoot = utils.FixAndCleanPath(canonicalRoot)
	providerRoot = utils.FixAndCleanPath(providerRoot)

	rootListing, err := m.refreshParent(path.Dir(providerRoot))
	if err != nil {
		if errs.IsObjectNotFound(err) {
			return providerOperationRemoteAbsent, nil
		}
		return providerOperationRemoteInconclusive, err
	}
	remoteRoot := exactRemoteByName(rootListing, path.Base(providerRoot))
	if remoteRoot == nil {
		return providerOperationRemoteAbsent, nil
	}
	if !remoteRoot.IsDir() {
		return providerOperationRemoteMismatch, nil
	}

	rows, err := loadReplicaTreeRows(canonicalRoot)
	if err != nil {
		return providerOperationRemoteInconclusive, err
	}
	type expectedObject struct {
		row          model.WebDAVWritebackObject
		providerPath string
	}
	byParent := make(map[string][]expectedObject)
	allowedByParent := map[string]map[string]struct{}{
		providerRoot: {},
	}
	ensureParent := func(parent string) map[string]struct{} {
		parent = utils.FixAndCleanPath(parent)
		allowed := allowedByParent[parent]
		if allowed == nil {
			allowed = make(map[string]struct{})
			allowedByParent[parent] = allowed
		}
		return allowed
	}

	for i := range rows {
		row := &rows[i]
		if row.Path == canonicalRoot {
			continue
		}
		rel := strings.TrimPrefix(row.Path, canonicalRoot)
		if rel == row.Path || !strings.HasPrefix(rel, "/") {
			return providerOperationRemoteInconclusive, nil
		}
		providerPath := utils.FixAndCleanPath(providerRoot + rel)
		parent := path.Dir(providerPath)
		ensureParent(parent)[path.Base(providerPath)] = struct{}{}

		// Tombstones authorize temporary source presence because the delete will
		// run after the root MOVE. Later locally-spooled file generations are
		// also safe: their PUT overwrites the moved baseline at destination.
		if canonicalDeleted(row) {
			continue
		}
		if !stagedReplicaTreeRow(row) {
			if row.IsDir || row.SpoolPath == "" {
				// A later directory or no-spool file generation cannot be
				// reconstructed by PUT after the root MOVE, so do not mutate the
				// provider tree from evidence captured by the older generation.
				return providerOperationRemoteInconclusive, nil
			}
			continue
		}
		if row.IsDir {
			ensureParent(providerPath)
			byParent[parent] = append(byParent[parent], expectedObject{row: *row, providerPath: providerPath})
			continue
		}
		// Pending payloads may be absent or stale at source; their durable PUT
		// deterministically overwrites the moved baseline afterward.
		if row.SpoolPath != "" {
			continue
		}
		byParent[parent] = append(byParent[parent], expectedObject{row: *row, providerPath: providerPath})
	}

	for parent, allowed := range allowedByParent {
		objs, listErr := m.refreshParent(parent)
		if listErr != nil {
			return providerOperationRemoteInconclusive, listErr
		}
		if providerListingHasUnexpectedName(objs, allowed) {
			// The provider tree contains a name that canonical state does not
			// know about. Moving the root would leak that object into Cloud
			// Sync's destination namespace, so classify the tree as divergent.
			return providerOperationRemoteMismatch, nil
		}
		for _, item := range byParent[parent] {
			remote := exactRemoteByName(objs, path.Base(item.providerPath))
			if remote == nil {
				// A force-refreshed miss can still be propagation lag around a
				// just-finished provider mutation. Never mutate on this evidence.
				return providerOperationRemoteInconclusive, nil
			}
			if item.row.IsDir {
				if !remote.IsDir() {
					return providerOperationRemoteMismatch, nil
				}
				continue
			}
			requireHash := providerRequiresPayloadHash(item.providerPath)
			switch compareRemoteContent(&item.row, remote, requireHash) {
			case remoteContentMismatch:
				return providerOperationRemoteMismatch, nil
			case remoteContentInconclusive:
				return providerOperationRemoteInconclusive, nil
			}
		}
	}
	return providerOperationRemoteMatch, nil
}

func replicaTreeRetryDelay() time.Duration {
	seconds := 2
	if conf.Conf != nil {
		seconds = max(seconds, max(1, conf.Conf.WebDAVWriteback.VerifyIntervalSeconds))
	}
	return time.Duration(seconds) * time.Second
}

func (m *workerManager) requeueReplicaTreeMove(row *model.WebDAVWritebackObject, message string) {
	next := time.Now().Add(replicaTreeRetryDelay())
	_ = db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("id = ? AND generation = ? AND state = ?", row.ID, row.Generation, StateUploading).
		Updates(map[string]any{
			"state":       StateQueued,
			"retry_at":    &next,
			"retry_count": row.RetryCount + 1,
			"last_error":  message,
		}).Error
}

func (m *workerManager) finishReplicaTreeMove(row *model.WebDAVWritebackObject, src string) error {
	now := time.Now()
	dst := utils.FixAndCleanPath(row.Path)
	src = utils.FixAndCleanPath(src)
	err := db.GetDb().Transaction(func(tx *gorm.DB) error {
		var rows []model.WebDAVWritebackObject
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("path = ? OR path LIKE ? ESCAPE '~'", dst, descendantLikePattern(dst)).
			Order("id asc").
			Find(&rows).Error; err != nil {
			return err
		}
		for i := range rows {
			current := &rows[i]
			if !stagedReplicaTreeRow(current) || canonicalDeleted(current) {
				continue
			}
			updates := map[string]any{
				"remote_generation":  0,
				"remote_verified_at": nil,
				"last_error":         "",
			}
			if current.ID == row.ID && current.Generation == row.Generation {
				updates["state"] = StateCompleted
				updates["cleanup_path"] = ""
				updates["retry_at"] = nil
				updates["retry_count"] = 0
				updates["verify_count"] = 0
				updates["completed_at"] = &now
			} else if current.IsDir && current.State == StateQueued {
				updates["state"] = StateCompleted
				updates["retry_at"] = nil
				updates["completed_at"] = &now
			} else if current.State == StateCompleted && current.SpoolPath == "" {
				updates["retry_at"] = nil
			}
			if err := tx.Model(&model.WebDAVWritebackObject{}).
				Where("id = ? AND generation = ?", current.ID, current.Generation).
				Updates(updates).Error; err != nil {
				return err
			}
		}
		return tx.Model(&model.WebDAVWritebackObject{}).
			Where("state = ? AND (path = ? OR path LIKE ? ESCAPE '~')", StateDeleted, src, descendantLikePattern(src)).
			Updates(map[string]any{
				"retry_at":   &now,
				"last_error": "",
			}).Error
	})
	if err == nil {
		recordHistoryOutcomeBestEffort(row, HistoryResultCompleted, StateCompleted, historyRecoveryForCompletion(row), now, row.LastError)
	}
	return err
}

func (m *workerManager) fallbackReplicaTreeMove(row *model.WebDAVWritebackObject, src, reason string) error {
	now := time.Now()
	dst := utils.FixAndCleanPath(row.Path)
	src = utils.FixAndCleanPath(src)
	err := db.GetDb().Transaction(func(tx *gorm.DB) error {
		var rows []model.WebDAVWritebackObject
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("path = ? OR path LIKE ? ESCAPE '~'", dst, descendantLikePattern(dst)).
			Order("id asc").
			Find(&rows).Error; err != nil {
			return err
		}
		for i := range rows {
			current := &rows[i]
			if canonicalDeleted(current) || !stagedReplicaTreeRow(current) {
				continue
			}
			clearRemoteVerification(current)
			current.CleanupPath = ""
			current.LastError = reason
			current.RetryCount = 0
			current.VerifyCount = 0
			switch {
			case current.IsDir:
				// Rebuild directories through normal MKCOL. Newer rows whose
				// generation changed after the original MOVE are left untouched.
				current.State = StateQueued
				current.RetryAt = &now
				current.CompletedAt = nil
			case current.SpoolPath != "":
				current.State = StateQueued
				current.RetryAt = &now
				current.CompletedAt = nil
			default:
				// No local payload can reconstruct this unchanged generation.
				// Hide only that generation so Cloud Sync repairs it with PUT.
				markCanonicalDeleted(current)
				current.State = StateDeleted
				current.RetryAt = &now
				current.CompletedAt = nil
			}
			if err := tx.Save(current).Error; err != nil {
				return err
			}
		}
		return tx.Model(&model.WebDAVWritebackObject{}).
			Where("state = ? AND (path = ? OR path LIKE ? ESCAPE '~')", StateDeleted, src, descendantLikePattern(src)).
			Updates(map[string]any{
				"retry_at":   &now,
				"last_error": "",
			}).Error
	})
	if err == nil {
		wake()
	}
	return err
}

func (m *workerManager) processReplicaTreeMove(row *model.WebDAVWritebackObject) {
	if row == nil || !queuedReplicaTreeMove(row) {
		return
	}
	src := utils.FixAndCleanPath(row.CleanupPath)
	if src == "" || src == row.Path {
		_ = m.fallbackReplicaTreeMove(row, src, "invalid provider directory MOVE source; rebuilding destination")
		return
	}
	recreated, err := replicaMoveSourceRecreated(src)
	if err != nil {
		m.failAfter(row, err, 2*time.Second)
		return
	}
	if recreated {
		_ = m.fallbackReplicaTreeMove(row, src, "directory MOVE source was recreated; rebuilding destination without moving the newer source")
		return
	}

	holdUntil := time.Now().Add(replicaMoveSourceHoldDelay())
	holdReplicaMoveSourceTree(src, holdUntil)
	holdReplicaMoveDestinationTree(row.Path, holdUntil)

	res := db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("id = ? AND generation = ? AND state = ? AND cleanup_path = ?", row.ID, row.Generation, StateQueued, src).
		Updates(map[string]any{"state": StateUploading, "retry_at": nil, "last_error": ""})
	if res.Error != nil || res.RowsAffected == 0 {
		return
	}

	dstState, dstErr := m.replicaTreeProviderState(row.Path, row.Path)
	if dstErr != nil || dstState == providerOperationRemoteInconclusive {
		if row.RetryCount >= 3 {
			_ = m.fallbackReplicaTreeMove(row, src, "provider destination directory stayed inconclusive; rebuilding destination")
			return
		}
		m.requeueReplicaTreeMove(row, "provider destination directory is inconclusive; waiting without mutation")
		return
	}
	if dstState == providerOperationRemoteMatch {
		if err := m.finishReplicaTreeMove(row, src); err != nil {
			m.failAfter(row, err, 2*time.Second)
			return
		}
		wake()
		return
	}

	srcState, srcErr := m.replicaTreeProviderState(row.Path, src)
	if srcErr != nil || srcState == providerOperationRemoteInconclusive || srcState == providerOperationRemoteAbsent {
		if row.RetryCount >= 3 {
			_ = m.fallbackReplicaTreeMove(row, src, "provider source directory could not be proven; rebuilding destination")
			return
		}
		m.requeueReplicaTreeMove(row, "provider source directory is not yet provable; waiting without mutation")
		return
	}
	if srcState == providerOperationRemoteMismatch {
		_ = m.fallbackReplicaTreeMove(row, src, "provider source directory diverged from canonical evidence; rebuilding destination")
		return
	}

	if dstState == providerOperationRemoteMismatch {
		if err := fs.Remove(m.ctx, row.Path); err != nil && !errs.IsObjectNotFound(err) {
			m.failAfter(row, err, 2*time.Second)
			return
		}
		absent, absentErr := providerOperationPathAbsent(m.ctx, row.Path)
		if absentErr != nil || !absent {
			m.requeueReplicaTreeMove(row, "provider overwrite target is still visible; waiting before directory MOVE")
			return
		}
	}

	if err := moveReplicaExact(m.ctx, src, row.Path); err != nil {
		if !errs.IsObjectNotFound(err) {
			m.failAfter(row, err, 2*time.Second)
			return
		}
	}
	// Verify from a later fresh snapshot. A provider NotFound at this point can
	// simply mean the MOVE succeeded and destination visibility is lagging.
	next := time.Now().Add(replicaTreeRetryDelay())
	_ = db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("id = ? AND generation = ? AND state = ?", row.ID, row.Generation, StateUploading).
		Updates(map[string]any{
			"state":      StateQueued,
			"retry_at":   &next,
			"last_error": "provider directory MOVE issued; waiting for destination verification",
		}).Error
}

func replicaMoveSourceHoldDelay() time.Duration {
	seconds := 30
	if conf.Conf != nil {
		seconds = max(seconds, max(1, conf.Conf.WebDAVWriteback.RetryMaxSeconds)*2)
	}
	return time.Duration(seconds) * time.Second
}

func holdReplicaMoveSource(src string, until time.Time) {
	if src == "" {
		return
	}
	_ = db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("path_key = ? AND state = ?", pathKey(src), StateDeleted).
		Where("retry_at IS NULL OR retry_at < ?", until).
		Update("retry_at", &until).Error
}

func replicaMoveSourceRecreated(src string) (bool, error) {
	current, err := getByPath(src)
	if err != nil || current == nil {
		return false, err
	}
	return !canonicalDeleted(current), nil
}

func rollbackReplicaMove(ctx context.Context, stagedPath, srcDir, srcName, tempName string) {
	if path.Dir(stagedPath) != srcDir {
		if _, err := fs.Move(context.WithValue(ctx, conf.NoTaskKey, struct{}{}), stagedPath, srcDir); err != nil {
			return
		}
		stagedPath = path.Join(srcDir, tempName)
	}
	_ = fs.Rename(ctx, stagedPath, srcName)
}

func moveReplicaExact(ctx context.Context, src, dst string) error {
	src = utils.FixAndCleanPath(src)
	dst = utils.FixAndCleanPath(dst)
	srcDir := path.Dir(src)
	dstDir := path.Dir(dst)
	srcName := path.Base(src)
	dstName := path.Base(dst)

	switch {
	case srcDir == dstDir:
		return fs.Rename(ctx, src, dstName)
	case srcName == dstName:
		_, err := fs.Move(context.WithValue(ctx, conf.NoTaskKey, struct{}{}), src, dstDir)
		return err
	default:
		tempName := ".openlist-webdav-move-" + uuid.NewString()
		if err := fs.Rename(ctx, src, tempName); err != nil {
			return err
		}
		tempSrc := path.Join(srcDir, tempName)
		if _, err := fs.Move(context.WithValue(ctx, conf.NoTaskKey, struct{}{}), tempSrc, dstDir); err != nil {
			rollbackReplicaMove(ctx, tempSrc, srcDir, srcName, tempName)
			return err
		}
		movedTemp := path.Join(dstDir, tempName)
		if err := fs.Rename(ctx, movedTemp, dstName); err != nil {
			rollbackReplicaMove(ctx, movedTemp, srcDir, srcName, tempName)
			return err
		}
		return nil
	}
}

func (m *workerManager) replicaMoveSourceObject(src string, requireHash bool) (model.Obj, error) {
	if requireHash {
		objs, err := m.refreshParent(path.Dir(src))
		if err != nil {
			return nil, err
		}
		return exactRemoteByName(objs, path.Base(src)), nil
	}
	remote, err := fs.Get(m.ctx, src, &fs.GetArgs{NoLog: true})
	if err == nil {
		return remote, nil
	}
	if !errs.IsObjectNotFound(err) {
		return nil, err
	}
	objs, listErr := m.refreshParent(path.Dir(src))
	if listErr != nil {
		return nil, listErr
	}
	return exactRemoteByName(objs, path.Base(src)), nil
}

func (m *workerManager) abandonReplicaMove(row *model.WebDAVWritebackObject, reason string) {
	if row == nil {
		return
	}
	now := time.Now()
	res := db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("id = ? AND generation = ? AND state IN ? AND spool_path = '' AND cleanup_path <> ''",
			row.ID, row.Generation, []string{StateQueued, StateUploading, StateVerifying}).
		Updates(map[string]any{
			"canonical_state":    CanonicalStateDeleted,
			"state":              StateDeleted,
			"cleanup_path":       "",
			"retry_at":           &now,
			"last_error":         reason,
			"retry_count":        0,
			"verify_count":       0,
			"completed_at":       nil,
			"remote_object_id":   "",
			"remote_sha1":        "",
			"remote_generation":  0,
			"remote_verified_at": nil,
		})
	if res.Error == nil && res.RowsAffected > 0 {
		wake()
	}
}

func (m *workerManager) enterReplicaMoveVerification(row *model.WebDAVWritebackObject, message string) {
	now := time.Now()
	res := db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("id = ? AND generation = ? AND state = ?", row.ID, row.Generation, StateUploading).
		Updates(map[string]any{
			"state":        StateVerifying,
			"retry_at":     &now,
			"verify_count": 0,
			"last_error":   message,
		})
	if res.Error == nil && res.RowsAffected > 0 {
		row.State = StateVerifying
		row.RetryAt = &now
		m.processVerify(row)
	}
}

func (m *workerManager) processReplicaMove(row *model.WebDAVWritebackObject) {
	if row == nil || !queuedReplicaMove(row) {
		return
	}
	src := utils.FixAndCleanPath(row.CleanupPath)
	if src == "" || src == row.Path {
		m.abandonReplicaMove(row, "invalid provider MOVE source; forcing Cloud Sync repair")
		return
	}

	recreated, err := replicaMoveSourceRecreated(src)
	if err != nil {
		m.failAfter(row, err, 2*time.Second)
		return
	}
	if recreated {
		m.abandonReplicaMove(row, "MOVE source was recreated before provider convergence; forcing destination re-upload")
		return
	}

	holdUntil := time.Now().Add(replicaMoveSourceHoldDelay())
	holdReplicaMoveSource(src, holdUntil)

	res := db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("id = ? AND generation = ? AND state = ? AND spool_path = '' AND cleanup_path = ?",
			row.ID, row.Generation, StateQueued, src).
		Updates(map[string]any{"state": StateUploading, "retry_at": nil, "last_error": ""})
	if res.Error != nil || res.RowsAffected == 0 {
		return
	}

	requireHash := providerRequiresPayloadHash(row.Path)
	remote, verifyErr := m.remoteForVerify(row, requireHash, false)
	verification := classifyRemoteVerification(row, remote, verifyErr, requireHash)
	switch verification {
	case remoteVerificationMatch:
		m.completeRemoteVerification(row, remote, []string{StateUploading}, requireHash)
		return
	case remoteVerificationInconclusive:
		next := time.Now().Add(remoteVerificationInconclusiveDelay())
		_ = db.GetDb().Model(&model.WebDAVWritebackObject{}).
			Where("id = ? AND generation = ? AND state = ?", row.ID, row.Generation, StateUploading).
			Updates(map[string]any{
				"state":      StateQueued,
				"retry_at":   &next,
				"last_error": "provider MOVE destination identity is inconclusive; waiting without mutation",
			}).Error
		return
	}

	if remote != nil {
		if err := fs.Remove(m.ctx, row.Path); err != nil && !errs.IsObjectNotFound(err) {
			m.failAfter(row, err, 2*time.Second)
			return
		}
		absent, absentErr := m.remoteDeleteAbsent(row)
		if absentErr != nil {
			m.failAfter(row, absentErr, 2*time.Second)
			return
		}
		if !absent {
			m.failAfter(row, errors.New("provider MOVE destination is still visible after overwrite cleanup"), 2*time.Second)
			return
		}
	}

	sourceRemote, sourceErr := m.replicaMoveSourceObject(src, requireHash)
	if sourceErr != nil {
		m.failAfter(row, sourceErr, 2*time.Second)
		return
	}
	if sourceRemote == nil {
		// Source disappearance can mean a previous MOVE succeeded while the
		// destination is still propagating. Verify first; only after repeated
		// full verification cycles do we force Cloud Sync to re-PUT.
		if row.RetryCount >= 2 {
			m.abandonReplicaMove(row, "provider MOVE source and destination stayed absent; forcing destination re-upload")
			return
		}
		m.enterReplicaMoveVerification(row, "provider MOVE source is absent; checking destination propagation")
		return
	}

	switch compareRemoteContent(row, sourceRemote, requireHash) {
	case remoteContentMismatch:
		m.abandonReplicaMove(row, "provider MOVE source no longer matches the canonical generation; forcing destination re-upload")
		return
	case remoteContentInconclusive:
		next := time.Now().Add(remoteVerificationInconclusiveDelay())
		_ = db.GetDb().Model(&model.WebDAVWritebackObject{}).
			Where("id = ? AND generation = ? AND state = ?", row.ID, row.Generation, StateUploading).
			Updates(map[string]any{
				"state":      StateQueued,
				"retry_at":   &next,
				"last_error": "provider MOVE source identity is inconclusive; waiting without mutation",
			}).Error
		return
	}

	if err := moveReplicaExact(m.ctx, src, row.Path); err != nil {
		if errs.IsObjectNotFound(err) {
			m.enterReplicaMoveVerification(row, "provider MOVE source disappeared during mutation; checking destination propagation")
			return
		}
		m.failAfter(row, err, 2*time.Second)
		return
	}
	m.enterReplicaMoveVerification(row, "")
}

func providerUploadProgressBytes(total int64, progress float64) int64 {
	if total <= 0 || progress <= 0 {
		return 0
	}
	if progress >= 100 {
		return total
	}
	return min(total, max(int64(0), int64(float64(total)*progress/100.0)))
}

func (m *workerManager) processUpload(row *model.WebDAVWritebackObject) {
	if waiting, err := m.waitForCanonicalParent(row); err != nil {
		m.failAfter(row, err, 2*time.Second)
		return
	} else if waiting {
		return
	}
	receiving, receiveErr := durableReceiving(row.Path, time.Now())
	if receiveErr != nil {
		next := time.Now().Add(2 * time.Second)
		_ = db.GetDb().Model(&model.WebDAVWritebackObject{}).
			Where("id = ? AND generation = ? AND state IN ?", row.ID, row.Generation, []string{StateQueued}).
			Updates(map[string]any{"retry_at": &next, "last_error": "waiting for durable receive lease"}).Error
		return
	}
	if receiving {
		next := time.Now().Add(time.Duration(ReceiveRetrySeconds()) * time.Second)
		_ = db.GetDb().Model(&model.WebDAVWritebackObject{}).
			Where("id = ? AND generation = ? AND state IN ?", row.ID, row.Generation, []string{StateQueued}).
			Update("retry_at", &next).Error
		return
	}
	if queuedReplicaMove(row) {
		m.processReplicaMove(row)
		return
	}
	requireHash := providerRequiresPayloadHash(row.Path)
	if row.RetryCount > 0 {
		remote, verifyErr := m.remoteForVerify(row, requireHash, false)
		verification := classifyRemoteVerification(row, remote, verifyErr, requireHash)
		if verification == remoteVerificationMatch {
			m.completeRemoteVerification(row, remote, []string{StateQueued}, requireHash)
			return
		}
		if providerRepairNeedsVerification(verification) {
			next := time.Now().Add(remoteVerificationInconclusiveDelay())
			msg := "provider retry probe is inconclusive; entering verification without reupload"
			if verifyErr != nil {
				msg = fmt.Sprintf("provider retry probe is inconclusive: %v", verifyErr)
			} else if remote != nil && requireHash && remote.GetSize() == row.Size &&
				remote.GetHash().GetHash(utils.SHA1) == "" {
				msg = "provider retry probe sees matching size but required 115 SHA-1 is unavailable; entering verification without reupload"
			}
			_ = db.GetDb().Model(&model.WebDAVWritebackObject{}).
				Where("id = ? AND generation = ? AND state IN ?", row.ID, row.Generation, []string{StateQueued}).
				Updates(map[string]any{
					"state":        StateVerifying,
					"retry_at":     &next,
					"verify_count": 0,
					"last_error":   msg,
				}).Error
			return
		}
	}
	payload, available, err := openLocalPayload(row)
	if err != nil {
		m.fail(row, err)
		return
	}
	if !available {
		now := time.Now()
		if err := markMissingDurablePayloadForVerification(row, now, "durable local payload is missing; verifying provider before Cloud Sync repair"); err != nil {
			m.fail(row, err)
		}
		return
	}
	releaseActiveSpool := func() {}
	if row.SpoolPath != "" {
		releaseActiveSpool = markSpoolActive(row.SpoolPath)
	}
	defer func() {
		releaseActiveSpool()
		_ = payload.Close()
	}()

	uploadStartedAt := time.Now()
	res := db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("id = ? AND generation = ? AND state IN ?", row.ID, row.Generation, []string{StateQueued}).
		Updates(map[string]any{
			"state":                        StateUploading,
			"retry_at":                     nil,
			"last_error":                   "",
			"provider_upload_started_at":   &uploadStartedAt,
			"provider_upload_completed_at": nil,
			"provider_uploaded_bytes":      0,
			"restart_upload_recovery":      false,
		})
	if res.Error != nil || res.RowsAffected == 0 {
		return
	}
	row.ProviderUploadStartedAt = &uploadStartedAt
	row.ProviderUploadCompletedAt = nil

	obj := &model.Object{
		Name:     row.Name,
		Size:     row.Size,
		Modified: row.ModTime,
		Ctime:    row.CreateTime,
		HashInfo: utils.NewHashInfo(utils.SHA1, row.PayloadSHA1),
	}
	fsStream := &stream.FileStream{
		Obj:      obj,
		Reader:   payload,
		Mimetype: row.MimeType,
	}
	var progressMu sync.Mutex
	var lastProgressAt time.Time
	lastProgressBytes := int64(-1)
	updateProviderProgress := func(progress float64) {
		uploaded := providerUploadProgressBytes(row.Size, progress)
		now := time.Now()
		progressMu.Lock()
		defer progressMu.Unlock()
		if uploaded <= lastProgressBytes && progress < 100 {
			return
		}
		if progress < 100 && !lastProgressAt.IsZero() && now.Sub(lastProgressAt) < time.Second {
			return
		}
		lastProgressAt = now
		lastProgressBytes = uploaded
		_ = db.GetDb().Model(&model.WebDAVWritebackObject{}).
			Where("id = ? AND generation = ? AND state = ?", row.ID, row.Generation, StateUploading).
			Update("provider_uploaded_bytes", uploaded).Error
	}
	err = fs.PutDirectlyWithProgress(m.ctx, row.Parent, fsStream, updateProviderProgress, true)
	if err != nil {
		if errs.IsNotFoundError(err) {
			m.failAfter(row, err, 2*time.Second)
		} else {
			m.fail(row, err)
		}
		return
	}
	uploadCompletedAt := time.Now()
	_ = db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("id = ? AND generation = ? AND state = ?", row.ID, row.Generation, StateUploading).
		Updates(map[string]any{
			"provider_upload_completed_at": &uploadCompletedAt,
			"provider_uploaded_bytes":      row.Size,
		}).Error
	row.ProviderUploadCompletedAt = &uploadCompletedAt

	var current model.WebDAVWritebackObject
	if err := db.GetDb().
		Select("generation", "canonical_state", "state", "path", "spool_path").
		Take(&current, row.ID).Error; err != nil {
		return
	}
	if current.Generation != row.Generation || canonicalDeleted(&current) || current.Path != row.Path {
		if shouldRemoveStaleRemote(row.Path, &current) {
			_ = fs.Remove(m.ctx, row.Path)
		}
		if current.SpoolPath != row.SpoolPath {
			removeSpoolIfUnreferenced(row.SpoolPath)
		}
		return
	}

	// Keep the provider claim in UPLOADING while the first authoritative
	// verification runs. A normal success can now commit UPLOADING -> COMPLETED
	// in one database write; only uncertain/divergent evidence persists the
	// VERIFYING state. Crash recovery already promotes interrupted UPLOADING
	// files to VERIFYING, so this fast path does not weaken durability.
	m.processRemoteVerification(row, StateUploading, requireHash)
}

func (m *workerManager) processMkdir(row *model.WebDAVWritebackObject) {
	if queuedReplicaTreeMove(row) {
		m.processReplicaTreeMove(row)
		return
	}
	if waiting, err := m.waitForCanonicalParent(row); err != nil {
		m.failAfter(row, err, 2*time.Second)
		return
	} else if waiting {
		return
	}
	res := db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("id = ? AND generation = ? AND is_dir = ? AND state IN ?", row.ID, row.Generation, true, []string{StateQueued}).
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
			"state":             StateCompleted,
			"completed_at":      &now,
			"retry_at":          nil,
			"last_error":        "",
			"resolution_reason": "",
			"retry_count":       0,
		})
	if res.Error != nil || res.RowsAffected == 0 {
		return
	}
	recordHistoryOutcomeBestEffort(row, HistoryResultCompleted, StateCompleted, historyRecoveryForCompletion(row), now, row.LastError)
	if row.CleanupPath != "" && row.CleanupPath != row.Path {
		_ = fs.Remove(m.ctx, row.CleanupPath)
		_ = db.GetDb().Model(&model.WebDAVWritebackObject{}).
			Where("id = ? AND generation = ?", row.ID, row.Generation).
			Update("cleanup_path", "").Error
	}
}

type providerVerificationCapability uint8

const (
	providerVerificationFreshListing providerVerificationCapability = iota
	providerVerificationStrongSHA1
)

func providerVerificationCapabilityForPath(p string) providerVerificationCapability {
	storage, err := fs.GetStorage(p, &fs.GetStoragesArgs{})
	if err != nil || storage == nil {
		return providerVerificationFreshListing
	}
	// 115 Open currently exposes reliable payload SHA1 evidence. Generic
	// drivers intentionally remain on fresh listing + size/type evidence; when
	// that evidence is incomplete, verification must stay inconclusive rather
	// than pretending the replica is complete.
	if storage.Config().Name == "115 Open" {
		return providerVerificationStrongSHA1
	}
	return providerVerificationFreshListing
}

func providerRequiresPayloadHash(p string) bool {
	return providerVerificationCapabilityForPath(p) == providerVerificationStrongSHA1
}

func remoteMatchesCanonical(row *model.WebDAVWritebackObject, remote model.Obj, requireHash bool) bool {
	if row == nil || row.IsDir || remote == nil || remote.IsDir() {
		return false
	}
	return compareRemoteContent(row, remote, requireHash) == remoteContentMatch
}

func remoteHashMismatch(row *model.WebDAVWritebackObject, remote model.Obj, requireHash bool) bool {
	if !requireHash || row == nil || row.IsDir || remote == nil || remote.IsDir() || remote.GetSize() != row.Size {
		return false
	}
	expectedSHA1 := canonicalContentSHA1(row)
	remoteSHA1 := remote.GetHash().GetHash(utils.SHA1)
	return expectedSHA1 != "" && remoteSHA1 != "" && !strings.EqualFold(expectedSHA1, remoteSHA1)
}

const remoteHashMismatchMaxAttempts = 3

func remoteHashMismatchVerificationAttempts() int {
	attempts := remoteHashMismatchMaxAttempts
	if conf.Conf != nil && conf.Conf.WebDAVWriteback.VerifyAttempts > 0 {
		attempts = min(attempts, max(1, conf.Conf.WebDAVWriteback.VerifyAttempts))
	}
	return attempts
}

func advanceRemoteHashMismatch(row *model.WebDAVWritebackObject, remote model.Obj) (next int, terminal bool) {
	attempts := remoteHashMismatchVerificationAttempts()
	current := 0
	if row != nil {
		current = max(0, row.VerifyCount)
		remoteID := ""
		remoteSHA1 := ""
		if remote != nil {
			remoteID = remote.GetID()
			remoteSHA1 = remote.GetHash().GetHash(utils.SHA1)
		}
		if current > 0 {
			if row.RemoteObjectID != "" && remoteID != "" && row.RemoteObjectID != remoteID {
				current = 0
			}
			if row.RemoteSHA1 != "" && remoteSHA1 != "" && !strings.EqualFold(row.RemoteSHA1, remoteSHA1) {
				current = 0
			}
		}
	}
	next = min(current+1, attempts)
	return next, next >= attempts
}

func remoteHashMismatchMessage(row *model.WebDAVWritebackObject, remote model.Obj) string {
	if row == nil || remote == nil {
		return "provider hash differs from canonical hash"
	}
	return fmt.Sprintf(
		"remote sha1 %s does not match canonical sha1 %s",
		remote.GetHash().GetHash(utils.SHA1),
		canonicalContentSHA1(row),
	)
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

const (
	open115MultipartChunkSize       int64 = 20 * utils.MB
	restartUploadVerificationWindow       = time.Minute
)

func verificationAttemptsFor(row *model.WebDAVWritebackObject, requireHash bool) int {
	attempts := max(1, conf.Conf.WebDAVWriteback.VerifyAttempts)
	intervalSeconds := max(1, conf.Conf.WebDAVWriteback.VerifyIntervalSeconds)
	if row != nil && requireHash && row.Size > open115MultipartChunkSize {
		minWindowSeconds := max(intervalSeconds, conf.Conf.WebDAVWriteback.RetryMaxSeconds)
		minAttempts := (minWindowSeconds + intervalSeconds - 1) / intervalSeconds
		attempts = max(attempts, minAttempts)
	}
	if row != nil && row.RestartUploadRecovery {
		windowSeconds := max(intervalSeconds, int(restartUploadVerificationWindow/time.Second))
		restartAttempts := max(2, (windowSeconds+intervalSeconds-1)/intervalSeconds)
		attempts = min(attempts, restartAttempts)
	}
	return attempts
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

func providerRepairNeedsVerification(state remoteVerificationState) bool {
	return state == remoteVerificationInconclusive
}

func shouldBatchVerificationSiblings(currentState string, requireHash bool) bool {
	return requireHash && currentState == StateVerifying
}

const verificationSiblingBatchLimit = 64

type verificationBatchMatch struct {
	row    model.WebDAVWritebackObject
	remote model.Obj
}

func matchingVerificationRows(rows []model.WebDAVWritebackObject, remotes []model.Obj, skipID uint, requireHash bool) []verificationBatchMatch {
	if len(rows) == 0 || len(remotes) == 0 {
		return nil
	}
	byName := make(map[string]model.Obj, len(remotes))
	for _, remote := range remotes {
		if remote != nil {
			byName[remote.GetName()] = remote
		}
	}
	matches := make([]verificationBatchMatch, 0, min(len(rows), len(remotes)))
	for i := range rows {
		row := &rows[i]
		if row.ID == skipID || row.IsDir || row.State != StateVerifying {
			continue
		}
		remote := byName[row.Name]
		if remoteMatchesCanonical(row, remote, requireHash) {
			matches = append(matches, verificationBatchMatch{row: *row, remote: remote})
		}
	}
	return matches
}

func (m *workerManager) completeMatchingVerifySiblings(trigger *model.WebDAVWritebackObject, remotes []model.Obj, requireHash bool) {
	if trigger == nil || !requireHash || len(remotes) == 0 {
		return
	}
	var rows []model.WebDAVWritebackObject
	if err := db.GetDb().
		Where("parent_key = ? AND state = ? AND is_dir = ? AND id <> ?", pathKey(trigger.Parent), StateVerifying, false, trigger.ID).
		Order("retry_at asc").
		Limit(verificationSiblingBatchLimit).
		Find(&rows).Error; err != nil {
		log.Errorf("write-back sibling verification scan failed for %s: %v", trigger.Parent, err)
		return
	}
	for _, match := range matchingVerificationRows(rows, remotes, trigger.ID, requireHash) {
		if m.completeRemoteVerification(&match.row, match.remote, []string{StateVerifying}, requireHash) {
			m.markBatchCompleted(match.row.ID)
		}
	}
}

func (m *workerManager) remoteForVerify(row *model.WebDAVWritebackObject, requireHash, batchSiblings bool) (model.Obj, error) {
	if requireHash {
		// 115 directory listings already carry size and SHA-1. Use one fresh
		// parent snapshot as the verification authority instead of issuing a
		// single-object GET for every completed upload. The same snapshot also
		// completes matching VERIFYING siblings in bounded batches.
		objs, listErr := m.refreshParent(row.Parent)
		if listErr != nil {
			return nil, listErr
		}
		if batchSiblings {
			m.completeMatchingVerifySiblings(row, objs, true)
		}
		return exactRemoteByName(objs, row.Name), nil
	}

	remote, getErr := fs.Get(m.ctx, row.Path, &fs.GetArgs{NoLog: true})
	if getErr == nil && remoteMatchesCanonical(row, remote, false) {
		return remote, nil
	}
	if getErr != nil && !errs.IsObjectNotFound(getErr) {
		return nil, getErr
	}

	objs, listErr := m.refreshParent(row.Parent)
	if listErr != nil {
		return nil, listErr
	}
	if obj := exactRemoteByName(objs, row.Name); obj != nil {
		return obj, nil
	}
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

func matchedRemoteVerificationEvidence(row *model.WebDAVWritebackObject, remote model.Obj, now time.Time, requireHash bool) (remoteVerificationEvidence, bool) {
	if compareRemoteContent(row, remote, requireHash) != remoteContentMatch {
		return remoteVerificationEvidence{}, false
	}
	return captureRemoteVerification(row, remote, now), true
}

func providerEvidenceSnapshot(row *model.WebDAVWritebackObject, now time.Time, result string) (*time.Time, *time.Time, int, string) {
	first := &now
	count := 1
	if row != nil {
		if row.ProviderEvidenceFirstAt != nil {
			first = cloneHistoryTime(row.ProviderEvidenceFirstAt)
		}
		count = max(0, row.ProviderEvidenceCount) + 1
	}
	last := now
	return first, &last, count, result
}

func applyProviderEvidence(row *model.WebDAVWritebackObject, now time.Time, result string) map[string]any {
	first, last, count, outcome := providerEvidenceSnapshot(row, now, result)
	if row != nil {
		row.ProviderEvidenceFirstAt = first
		row.ProviderEvidenceLastAt = last
		row.ProviderEvidenceCount = count
		row.ProviderEvidenceResult = outcome
	}
	return map[string]any{
		"provider_evidence_first_at": first,
		"provider_evidence_last_at":  last,
		"provider_evidence_count":    count,
		"provider_evidence_result":   outcome,
	}
}

func mergeUpdateMaps(dst, src map[string]any) map[string]any {
	if dst == nil {
		dst = map[string]any{}
	}
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func (m *workerManager) completeRemoteVerification(row *model.WebDAVWritebackObject, remote model.Obj, allowedStates []string, requireHash bool) bool {
	now := time.Now()
	evidence, matched := matchedRemoteVerificationEvidence(row, remote, now, requireHash)
	if !matched {
		return false
	}
	updates := map[string]any{
		"state":                   StateCompleted,
		"completed_at":            &now,
		"retry_at":                nil,
		"last_error":              "",
		"resolution_reason":       "",
		"retry_count":             0,
		"verify_count":            0,
		"remote_object_id":        evidence.objectID,
		"remote_sha1":             evidence.sha1,
		"remote_generation":       evidence.generation,
		"remote_verified_at":      &evidence.verifiedAt,
		"restart_upload_recovery": false,
	}
	updates = mergeUpdateMaps(updates, applyProviderEvidence(row, now, "matched"))
	res := db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("id = ? AND generation = ? AND state IN ?", row.ID, row.Generation, allowedStates).
		Updates(updates)
	if res.Error != nil || res.RowsAffected == 0 {
		return false
	}
	// History is deliberately best-effort and runs only after correctness state
	// has committed. A History failure must never turn a provider success into a
	// Cloud Sync retry.
	recordCompletedHistoryBestEffort(row, evidence, now)
	if row.CleanupPath != "" && row.CleanupPath != row.Path {
		_ = fs.Remove(m.ctx, row.CleanupPath)
		_ = db.GetDb().Model(&model.WebDAVWritebackObject{}).
			Where("id = ? AND generation = ?", row.ID, row.Generation).
			Update("cleanup_path", "").Error
	}
	return true
}

func shouldAutomaticallyRepairHashMismatch(row *model.WebDAVWritebackObject, availability durablePayloadAvailability) bool {
	return row != nil && availability == durablePayloadAvailable && row.RetryCount < 1
}

func requireCloudSyncReupload(row *model.WebDAVWritebackObject, currentState, resolution, reason string, verifyCount int, clearSpool bool) bool {
	if row == nil || row.IsDir || row.CleanupPath != "" {
		return false
	}
	now := time.Now()
	recoveryStartedAt := row.RecoveryStartedAt
	if recoveryStartedAt == nil {
		recoveryStartedAt = &now
	}
	oldSpool := row.SpoolPath
	updates := map[string]any{
		"canonical_state":              CanonicalStateAcked,
		"state":                        StateWaitingCloudSyncReupload,
		"cloud_sync_reupload_required": true,
		"recovery_started_at":          recoveryStartedAt,
		"retry_at":                     nil,
		"last_error":                   reason,
		"resolution_reason":            resolution,
		"verify_count":                 verifyCount,
		"completed_at":                 nil,
		"remote_generation":            0,
		"remote_verified_at":           nil,
	}
	if clearSpool {
		updates["spool_path"] = ""
	}
	res := db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("id = ? AND generation = ? AND state = ? AND cleanup_path = ''", row.ID, row.Generation, currentState).
		Updates(updates)
	if res.Error != nil || res.RowsAffected == 0 {
		return false
	}

	final := *row
	final.CanonicalState = CanonicalStateAcked
	final.State = StateWaitingCloudSyncReupload
	final.CloudSyncReuploadRequired = true
	final.RecoveryStartedAt = recoveryStartedAt
	final.RetryAt = nil
	final.LastError = reason
	final.ResolutionReason = resolution
	final.VerifyCount = verifyCount
	final.CompletedAt = nil
	final.RemoteGeneration = 0
	final.RemoteVerifiedAt = nil
	if clearSpool {
		final.SpoolPath = ""
	}
	result := HistoryResultRecoveryRequired
	if resolution == ResolutionRemoteMissing {
		result = HistoryResultRemoteMissing
	}
	recordHistoryOutcomeBestEffort(
		&final,
		result,
		StateWaitingCloudSyncReupload,
		HistoryRecoveryCloudSyncRehydrateRequired,
		now,
		reason,
	)
	if clearSpool && oldSpool != "" && !spoolIsActive(oldSpool) {
		removeSpoolIfUnreferenced(oldSpool)
	}
	return true
}
func verifyingUpdates(currentState string, updates map[string]any) map[string]any {
	if currentState != StateVerifying {
		updates["state"] = StateVerifying
	}
	return updates
}

func (m *workerManager) processRemoteVerification(row *model.WebDAVWritebackObject, currentState string, requireHash bool) {
	remote, err := m.remoteForVerify(row, requireHash, shouldBatchVerificationSiblings(currentState, requireHash))
	verification := classifyRemoteVerification(row, remote, err, requireHash)
	if verification == remoteVerificationMatch {
		if m.completeRemoteVerification(row, remote, []string{currentState}, requireHash) {
			return
		}
		if currentState == StateUploading {
			// A matched provider object must not leave a row stranded in UPLOADING
			// when only the completion metadata write failed. Best-effort promotion
			// to VERIFYING keeps the retry/restart recovery path intact.
			next := time.Now().Add(2 * time.Second)
			_ = db.GetDb().Model(&model.WebDAVWritebackObject{}).
				Where("id = ? AND generation = ? AND state = ?", row.ID, row.Generation, currentState).
				Updates(map[string]any{
					"state":      StateVerifying,
					"retry_at":   &next,
					"last_error": "remote verification matched but completion persistence failed; retrying verification",
				}).Error
		}
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
			Where("id = ? AND generation = ? AND state = ?", row.ID, row.Generation, currentState).
			Updates(verifyingUpdates(currentState, map[string]any{
				"retry_at":   &next,
				"last_error": msg,
			})).Error
		return
	}

	if remoteHashMismatch(row, remote, requireHash) {
		evidenceAt := time.Now()
		evidenceUpdates := applyProviderEvidence(row, evidenceAt, ResolutionRemoteHashMismatch)
		nextCount, terminal := advanceRemoteHashMismatch(row, remote)
		diagnostic := remoteHashMismatchMessage(row, remote)
		remoteID := remote.GetID()
		remoteSHA1 := strings.ToLower(remote.GetHash().GetHash(utils.SHA1))
		if terminal {
			availability, payloadErr := inspectDurableLocalPayload(row)
			if payloadErr != nil {
				next := time.Now().Add(remoteVerificationInconclusiveDelay())
				_ = db.GetDb().Model(&model.WebDAVWritebackObject{}).
					Where("id = ? AND generation = ? AND state = ?", row.ID, row.Generation, currentState).
					Updates(verifyingUpdates(currentState, mergeUpdateMaps(map[string]any{
						"retry_at":         &next,
						"verify_count":     nextCount,
						"remote_object_id": remoteID,
						"remote_sha1":      remoteSHA1,
						"last_error":       fmt.Sprintf("cannot inspect durable spool while resolving provider hash difference: %v", payloadErr),
					}, evidenceUpdates))).Error
				return
			}
			if availability == durablePayloadMissing && row.CleanupPath == "" {
				forceCloudSyncRepairForMissingPayload(
					row,
					currentState,
					"durable spool is missing while provider hash differs from canonical content; exposing the loss so Cloud Sync can re-upload",
				)
				return
			}
			if shouldAutomaticallyRepairHashMismatch(row, availability) {
				// The payload is still durable locally, so manual intervention
				// is unnecessary. Re-queue exactly once. processUpload performs
				// one fresh provider probe before sending bytes, which can skip
				// the repair upload if the provider converged in the meantime.
				next := time.Now()
				updates := map[string]any{
					"state":                   StateQueued,
					"retry_at":                &next,
					"retry_count":             row.RetryCount + 1,
					"verify_count":            0,
					"last_error":              "confirmed provider hash mismatch; scheduling one automatic repair upload",
					"resolution_reason":       "",
					"recovery_started_at":     gorm.Expr("COALESCE(recovery_started_at, ?)", evidenceAt),
					"remote_object_id":        remoteID,
					"remote_sha1":             remoteSHA1,
					"remote_generation":       0,
					"remote_verified_at":      nil,
					"restart_upload_recovery": false,
				}
				_ = db.GetDb().Model(&model.WebDAVWritebackObject{}).
					Where("id = ? AND generation = ? AND state = ?", row.ID, row.Generation, currentState).
					Updates(mergeUpdateMaps(updates, evidenceUpdates)).Error
				return
			}
			if availability == durablePayloadAvailable && row.RetryCount >= 1 {
				requireCloudSyncReupload(
					row,
					currentState,
					ResolutionRemoteHashMismatch,
					diagnostic+"; one automatic provider repair upload did not converge; restart/rescan Cloud Sync to publish a fresh generation",
					nextCount,
					true,
				)
				return
			}
			next := time.Now().Add(remoteVerificationInconclusiveDelay())
			_ = db.GetDb().Model(&model.WebDAVWritebackObject{}).
				Where("id = ? AND generation = ? AND state = ?", row.ID, row.Generation, currentState).
				Updates(verifyingUpdates(currentState, mergeUpdateMaps(map[string]any{
					"retry_at":         &next,
					"verify_count":     nextCount,
					"remote_object_id": remoteID,
					"remote_sha1":      remoteSHA1,
					"last_error":       diagnostic + "; durable local payload is not available for a safe terminal hash-difference state",
				}, evidenceUpdates))).Error
			return
		}

		interval := time.Duration(max(1, conf.Conf.WebDAVWriteback.VerifyIntervalSeconds)) * time.Second
		next := time.Now().Add(interval)
		_ = db.GetDb().Model(&model.WebDAVWritebackObject{}).
			Where("id = ? AND generation = ? AND state = ?", row.ID, row.Generation, currentState).
			Updates(verifyingUpdates(currentState, mergeUpdateMaps(map[string]any{
				"retry_at":         &next,
				"verify_count":     nextCount,
				"remote_object_id": remoteID,
				"remote_sha1":      remoteSHA1,
				"last_error":       diagnostic,
			}, evidenceUpdates))).Error
		return
	}

	attempts := verificationAttemptsFor(row, requireHash)
	nextCount, retryUpload := advanceRemoteVerification(verification, row.VerifyCount, attempts)
	if retryUpload {
		availability, payloadErr := inspectDurableLocalPayload(row)
		if payloadErr != nil {
			next := time.Now().Add(remoteVerificationInconclusiveDelay())
			_ = db.GetDb().Model(&model.WebDAVWritebackObject{}).
				Where("id = ? AND generation = ? AND state = ?", row.ID, row.Generation, currentState).
				Updates(verifyingUpdates(currentState, map[string]any{
					"retry_at":   &next,
					"last_error": fmt.Sprintf("cannot inspect durable spool during recovery: %v", payloadErr),
				})).Error
			return
		}
		if availability == durablePayloadMissing && row.CleanupPath == "" {
			forceCloudSyncRepairForMissingPayload(
				row,
				currentState,
				"durable spool is missing and fresh provider evidence stayed divergent; exposing the loss so Cloud Sync can re-upload",
			)
			return
		}
		if row.RetryCount >= 1 {
			reason := ResolutionVerificationExhausted
			message := "fresh provider evidence stayed divergent after one automatic repair upload; restart/rescan Cloud Sync to publish a fresh generation"
			if remote == nil {
				reason = ResolutionRemoteMissing
				message = "remote object is still absent after one automatic repair upload; restart/rescan Cloud Sync to publish a fresh generation"
			}
			requireCloudSyncReupload(row, currentState, reason, message, nextCount, true)
			return
		}

		next := time.Now().Add(retryDelay(row.RetryCount + 1))
		_ = db.GetDb().Model(&model.WebDAVWritebackObject{}).
			Where("id = ? AND generation = ? AND state = ?", row.ID, row.Generation, currentState).
			Updates(map[string]any{
				"state":                   StateQueued,
				"retry_at":                &next,
				"retry_count":             row.RetryCount + 1,
				"verify_count":            0,
				"restart_upload_recovery": false,
				"last_error":              "fresh provider evidence stayed divergent through the verification window; scheduling one automatic repair upload",
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
		Where("id = ? AND generation = ? AND state = ?", row.ID, row.Generation, currentState).
		Updates(verifyingUpdates(currentState, map[string]any{
			"retry_at":     &next,
			"verify_count": nextCount,
			"last_error":   msg,
		})).Error
}

func (m *workerManager) processVerify(row *model.WebDAVWritebackObject) {
	if row.CleanupPath != "" && row.SpoolPath == "" {
		holdReplicaMoveSource(row.CleanupPath, time.Now().Add(replicaMoveSourceHoldDelay()))
	}
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
	m.processRemoteVerification(row, StateVerifying, requireHash)
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
	// 115 directory listings are already the verification authority used by the
	// upload path and are single-flighted per parent. After a successful DELETE,
	// skip the redundant per-object GET so sibling tombstones can share one
	// refreshed parent snapshot instead of producing GET+LIST pairs.
	if !providerRequiresPayloadHash(row.Path) {
		remote, getErr := fs.Get(m.ctx, row.Path, &fs.GetArgs{NoLog: true})
		if getErr == nil && remote != nil {
			return false, nil
		}
		if getErr != nil && !errs.IsObjectNotFound(getErr) {
			return false, getErr
		}
	}

	objs, listErr := m.refreshParent(row.Parent)
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

func ancestorPathKeys(p string) []string {
	p = utils.FixAndCleanPath(p)
	parent := path.Dir(p)
	if parent == p || parent == "." {
		return nil
	}
	keys := make([]string, 0, 4)
	for {
		keys = append(keys, pathKey(parent))
		if parent == "/" {
			break
		}
		next := path.Dir(parent)
		if next == parent || next == "." {
			break
		}
		parent = next
	}
	return keys
}

func deletedAncestorExists(p string) (bool, error) {
	keys := ancestorPathKeys(p)
	if len(keys) == 0 {
		return false, nil
	}
	var count int64
	err := db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("path_key IN ? AND state = ?", keys, StateDeleted).
		Limit(1).
		Count(&count).Error
	return count > 0, err
}

func deleteAncestorWaitDelay() time.Duration {
	seconds := 2
	if conf.Conf != nil {
		seconds = max(seconds, max(1, conf.Conf.WebDAVWriteback.VerifyIntervalSeconds)*2)
	}
	return time.Duration(seconds) * time.Second
}

func collectConfirmedTombstoneSubtree(root *model.WebDAVWritebackObject, rows []model.WebDAVWritebackObject) (ids []uint, spoolPaths []string, valid bool) {
	if root == nil {
		return nil, nil, false
	}
	seenSpool := make(map[string]struct{})
	rootMatched := false
	for i := range rows {
		row := &rows[i]
		if !canonicalDeleted(row) || !isPathOrDescendant(row.Path, root.Path) {
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
	var deletedRows []model.WebDAVWritebackObject
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
		selected := make(map[uint]struct{}, len(ids))
		for _, id := range ids {
			selected[id] = struct{}{}
		}
		for i := range candidates {
			if _, ok := selected[candidates[i].ID]; ok {
				deletedRows = append(deletedRows, candidates[i])
			}
		}
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
	finalAt := time.Now()
	for i := range deletedRows {
		recordHistoryOutcomeBestEffort(&deletedRows[i], HistoryResultDeleted, StateDeleted, "", finalAt, deletedRows[i].LastError)
	}
	removeSpoolsIfUnreferenced(spoolPaths)
	return true, nil
}

func (m *workerManager) processDelete(row *model.WebDAVWritebackObject) {
	if waitingCloudSyncReupload(row) {
		_ = db.GetDb().Model(&model.WebDAVWritebackObject{}).
			Where("id = ? AND generation = ? AND state = ?", row.ID, row.Generation, StateDeleted).
			Updates(map[string]any{
				"state":             StateWaitingCloudSyncReupload,
				"retry_at":          nil,
				"resolution_reason": ResolutionNeedsCloudSyncRehydrate,
			}).Error
		return
	}
	// A tombstone can be superseded by a fast Cloud Sync recreate of the same
	// path. Re-check the generation before touching the provider so a queued old
	// delete cannot blindly remove a newer canonical generation.
	var current model.WebDAVWritebackObject
	if err := db.GetDb().Take(&current, row.ID).Error; err != nil {
		return
	}
	if current.Generation != row.Generation || !canonicalDeleted(&current) {
		return
	}

	ancestorDeleted, ancestorErr := deletedAncestorExists(row.Path)
	if ancestorErr != nil {
		m.failDeleted(row, ancestorErr)
		return
	}
	if ancestorDeleted {
		next := time.Now().Add(deleteAncestorWaitDelay())
		_ = db.GetDb().Model(&model.WebDAVWritebackObject{}).
			Where("id = ? AND generation = ? AND state = ?", row.ID, row.Generation, StateDeleted).
			Updates(map[string]any{
				"retry_at":   &next,
				"last_error": "waiting for deleted ancestor to remove provider subtree",
			}).Error
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
	if err := db.GetDb().Take(&current, row.ID).Error; err != nil {
		return
	}
	if current.Generation != row.Generation || !canonicalDeleted(&current) {
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
	if err := db.GetDb().Take(&current, row.ID).Error; err != nil {
		return
	}
	if current.Generation != row.Generation || !canonicalDeleted(&current) {
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
	if row == nil || err == nil {
		return
	}
	log.Errorf(
		"write-back provider retry: id=%d path=%q generation=%d state=%s size=%d retry=%d error=%v",
		row.ID, row.Path, row.Generation, row.State, row.Size, row.RetryCount+1, err,
	)
	next := time.Now().Add(delay)
	_ = db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Where("id = ? AND generation = ?", row.ID, row.Generation).
		Updates(map[string]any{
			"state":       StateQueued,
			"retry_at":    &next,
			"retry_count": row.RetryCount + 1,
			"last_error":  err.Error(),
		}).Error
}

func (m *workerManager) fail(row *model.WebDAVWritebackObject, err error) {
	m.failAfter(row, err, retryDelay(row.RetryCount))
}

func (m *workerManager) failDeleted(row *model.WebDAVWritebackObject, err error) {
	if row == nil || err == nil {
		return
	}
	log.Errorf(
		"write-back delete retry: id=%d path=%q generation=%d size=%d retry=%d error=%v",
		row.ID, row.Path, row.Generation, row.Size, row.RetryCount+1, err,
	)
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

const (
	completedCleanupBatchSize  = 256
	completedCleanupMaxBatches = 8
)

func unreferencedSpoolPaths(candidates, referenced []string) []string {
	if len(candidates) == 0 {
		return nil
	}
	live := make(map[string]struct{}, len(referenced))
	for _, p := range referenced {
		if p != "" {
			live[p] = struct{}{}
		}
	}
	seen := make(map[string]struct{}, len(candidates))
	out := make([]string, 0, len(candidates))
	for _, p := range candidates {
		if p == "" {
			continue
		}
		if _, duplicate := seen[p]; duplicate {
			continue
		}
		seen[p] = struct{}{}
		if _, stillReferenced := live[p]; !stillReferenced {
			out = append(out, p)
		}
	}
	return out
}

func completedSpoolReleaseEligible(row *model.WebDAVWritebackObject, cutoff time.Time) bool {
	return row != nil &&
		row.State == StateCompleted &&
		row.SpoolPath != "" &&
		row.CompletedAt != nil &&
		!row.CompletedAt.After(cutoff) &&
		row.RemoteVerifiedAt != nil &&
		row.RemoteGeneration == row.Generation
}

func completedSpoolReleaseSafe(row *model.WebDAVWritebackObject, cutoff time.Time) bool {
	return completedSpoolReleaseEligible(row, cutoff) && !spoolIsActive(row.SpoolPath)
}

func releaseCompletedSpoolBatch(cutoff time.Time, limit int) (selected, released int, releasedBytes uint64, err error) {
	if limit <= 0 {
		return 0, 0, 0, nil
	}
	var rows []model.WebDAVWritebackObject
	if err := db.GetDb().
		Select("id", "generation", "size", "spool_path", "completed_at", "remote_generation", "remote_verified_at", "state").
		Where("state = ? AND spool_path <> '' AND completed_at IS NOT NULL AND completed_at <= ?", StateCompleted, cutoff).
		Where("remote_verified_at IS NOT NULL AND remote_generation = generation").
		Order("completed_at asc").
		Order("id asc").
		Limit(limit * 2).
		Find(&rows).Error; err != nil {
		return 0, 0, 0, err
	}
	selected = len(rows)
	cleared := make([]string, 0, min(len(rows), limit))
	for i := range rows {
		if len(cleared) >= limit {
			break
		}
		row := &rows[i]
		if !completedSpoolReleaseSafe(row, cutoff) {
			continue
		}
		res := db.GetDb().Model(&model.WebDAVWritebackObject{}).
			Where("id = ? AND generation = ? AND state = ? AND spool_path = ? AND completed_at IS NOT NULL AND completed_at <= ?",
				row.ID, row.Generation, StateCompleted, row.SpoolPath, cutoff).
			Where("remote_verified_at IS NOT NULL AND remote_generation = generation").
			Update("spool_path", "")
		if res.Error != nil {
			return selected, released, releasedBytes, res.Error
		}
		if res.RowsAffected == 0 {
			continue
		}
		released++
		if row.Size > 0 {
			releasedBytes += uint64(row.Size)
		}
		cleared = append(cleared, row.SpoolPath)
	}
	if len(cleared) == 0 {
		return selected, released, releasedBytes, nil
	}

	removeSpoolsIfUnreferenced(cleared)
	return selected, released, releasedBytes, nil
}

func CleanupCompletedCacheNowDetailed() (CacheCleanupResult, error) {
	cutoff := time.Now()
	result := CacheCleanupResult{}
	for batch := 0; batch < completedCleanupMaxBatches*4; batch++ {
		selected, released, releasedBytes, err := releaseCompletedSpoolBatch(cutoff, completedCleanupBatchSize)
		if err != nil {
			return result, err
		}
		result.Files += released
		result.Bytes += releasedBytes
		if selected == 0 || released == 0 || selected < completedCleanupBatchSize {
			break
		}
	}
	return result, nil
}

func CleanupCompletedCacheNow() (int, error) {
	result, err := CleanupCompletedCacheNowDetailed()
	return result.Files, err
}

func (m *workerManager) cleanupCompleted() {
	ttl := conf.Conf.WebDAVWriteback.CompletedCacheTTLMinutes
	if ttl < 0 {
		return
	}
	cutoff := time.Now().Add(-time.Duration(ttl) * time.Minute)
	for batch := 0; batch < completedCleanupMaxBatches; batch++ {
		selected, released, _, err := releaseCompletedSpoolBatch(cutoff, completedCleanupBatchSize)
		if err != nil {
			log.Errorf("write-back completed spool cleanup failed: %v", err)
			return
		}
		if selected == 0 || released == 0 || selected < completedCleanupBatchSize {
			return
		}
	}
}

func pruneReferencedSpoolCandidate(candidates map[string]struct{}, spoolPath string) {
	if len(candidates) == 0 || spoolPath == "" {
		return
	}
	delete(candidates, filepath.Clean(spoolPath))
}

func (m *workerManager) cleanupOrphans() {
	spoolDir := conf.Conf.WebDAVWriteback.SpoolDir
	entries, err := os.ReadDir(spoolDir)
	if err != nil {
		return
	}

	candidates := make(map[string]struct{})
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
		if strings.HasSuffix(entry.Name(), ".data") {
			candidates[filepath.Clean(p)] = struct{}{}
		}
	}
	if len(candidates) == 0 {
		return
	}

	rows, err := db.GetDb().Model(&model.WebDAVWritebackObject{}).
		Select("spool_path").
		Where("spool_path <> ''").
		Rows()
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() && len(candidates) > 0 {
		var spoolPath string
		if err := rows.Scan(&spoolPath); err != nil {
			return
		}
		pruneReferencedSpoolCandidate(candidates, spoolPath)
	}
	for orphan := range candidates {
		_ = os.Remove(orphan)
	}
}
