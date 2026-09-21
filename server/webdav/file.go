// Copyright 2014 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package webdav

import (
	"context"
	"net/http"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/fs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/internal/stream"
	"github.com/OpenListTeam/OpenList/v4/internal/writeback"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	"github.com/google/uuid"
	"github.com/pkg/errors"
)

// slashClean is equivalent to but slightly more efficient than
// path.Clean("/" + name).
func slashClean(name string) string {
	if name == "" || name[0] != '/' {
		name = "/" + name
	}
	return path.Clean(name)
}

func resourceObject(ctx context.Context, name string) (model.Obj, error) {
	if writeback.Enabled() {
		obj, found, deleted, err := writeback.Canonical(name)
		if err != nil {
			return nil, err
		}
		if found {
			// Cloud Sync verifies a successful PUT with an immediate PROPFIND.
			// Once a generation is durably ACKed, provider propagation must not
			// alter this client-visible snapshot.
			if deleted || obj == nil {
				return nil, errs.ObjectNotFound
			}
			return obj, nil
		}
	}
	return fs.Get(ctx, name, &fs.GetArgs{NoLog: true})
}

func resourceExists(ctx context.Context, name string) (bool, error) {
	_, err := resourceObject(ctx, name)
	if err == nil {
		return true, nil
	}
	if errs.IsObjectNotFound(err) {
		return false, nil
	}
	return false, err
}

func copyCanUseNative(src, dst string, depth int) bool {
	return writeback.ProviderOperationCopyUsesNative(src, dst, depth)
}

func copyExactFile(ctx context.Context, src, dst string) error {
	if local, row, err := writeback.OpenLocal(src); err != nil {
		return err
	} else if local != nil && row != nil {
		defer local.Close()
		obj := &model.Object{
			Name:     path.Base(dst),
			Size:     row.Size,
			Modified: row.ModTime,
			Ctime:    row.CreateTime,
			HashInfo: utils.NewHashInfo(utils.SHA1, row.PayloadSHA1),
		}
		return fs.PutDirectly(ctx, path.Dir(dst), &stream.FileStream{
			Obj:      obj,
			Reader:   local,
			Mimetype: row.MimeType,
		})
	}

	link, obj, err := fs.Link(ctx, src, model.LinkArgs{})
	if err != nil {
		return err
	}
	targetObj := &model.ObjWrapName{Name: path.Base(dst), Obj: obj}
	ss, err := stream.NewSeekableStream(&stream.FileStream{
		Obj: targetObj,
		Ctx: ctx,
	}, link)
	if err != nil {
		return err
	}
	return fs.PutDirectly(ctx, path.Dir(dst), ss)
}

func copyExactTree(ctx context.Context, src, dst string, srcObj model.Obj, depth int) error {
	return walkFS(ctx, depth, src, srcObj, func(reqPath string, info model.Obj, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		suffix := strings.TrimPrefix(reqPath, src)
		target := utils.FixAndCleanPath(dst + suffix)
		if info.IsDir() {
			return fs.MakeDir(ctx, target)
		}
		return copyExactFile(ctx, reqPath, target)
	})
}

func providerResourceAbsent(ctx context.Context, name string) (bool, error) {
	remote, getErr := fs.Get(ctx, name, &fs.GetArgs{NoLog: true})
	if getErr == nil && remote != nil {
		return false, nil
	}
	if getErr != nil && !errs.IsObjectNotFound(getErr) {
		return false, getErr
	}
	parent := path.Dir(name)
	objs, listErr := fs.List(ctx, parent, &fs.ListArgs{Refresh: true, NoLog: true})
	if listErr != nil {
		if errs.IsObjectNotFound(listErr) {
			return true, nil
		}
		return false, listErr
	}
	targetName := path.Base(name)
	for _, obj := range objs {
		if obj.GetName() == targetName {
			return false, nil
		}
	}
	return true, nil
}

func providerConsistencyConfirmationDelay(storageName string, verifyIntervalSeconds int) time.Duration {
	if storageName != "115 Open" {
		return 0
	}
	if verifyIntervalSeconds <= 0 {
		verifyIntervalSeconds = 1
	}
	if verifyIntervalSeconds > 2 {
		verifyIntervalSeconds = 2
	}
	return time.Duration(verifyIntervalSeconds) * time.Second
}

func providerOverwriteConfirmationDelay(name string) time.Duration {
	storage, err := fs.GetStorage(name, &fs.GetStoragesArgs{})
	if err != nil || storage == nil {
		return 0
	}
	verifyInterval := 1
	if conf.Conf != nil {
		verifyInterval = conf.Conf.WebDAVWriteback.VerifyIntervalSeconds
	}
	return providerConsistencyConfirmationDelay(storage.Config().Name, verifyInterval)
}

func providerResourceAbsentStable(ctx context.Context, name string) (bool, error) {
	absent, err := providerResourceAbsent(ctx, name)
	if err != nil || !absent {
		return absent, err
	}

	delay := providerOverwriteConfirmationDelay(name)
	if delay <= 0 {
		return true, nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-timer.C:
	}
	return providerResourceAbsent(ctx, name)
}

func removeProviderOverwriteDestination(ctx context.Context, name string) (bool, error) {
	if err := fs.Remove(ctx, name); err != nil && !errs.IsObjectNotFound(err) {
		return false, err
	}
	return providerResourceAbsentStable(ctx, name)
}

func moveNeedsStaging(src, dst string) bool {
	return path.Dir(src) != path.Dir(dst) && path.Base(src) != path.Base(dst)
}

func rollbackStagedMove(ctx context.Context, stagedPath, srcDir, srcName, tempName string) {
	if path.Dir(stagedPath) != srcDir {
		if _, err := fs.Move(context.WithValue(ctx, conf.NoTaskKey, struct{}{}), stagedPath, srcDir); err != nil {
			return
		}
		stagedPath = path.Join(srcDir, tempName)
	}
	_ = fs.Rename(ctx, stagedPath, srcName)
}

func moveAcrossDirsExact(ctx context.Context, src, dst string) error {
	srcDir := path.Dir(src)
	dstDir := path.Dir(dst)
	srcName := path.Base(src)
	dstName := path.Base(dst)
	tempName := ".openlist-webdav-move-" + uuid.NewString()

	if err := fs.Rename(ctx, src, tempName); err != nil {
		return err
	}
	tempSrc := path.Join(srcDir, tempName)
	if _, err := fs.Move(context.WithValue(ctx, conf.NoTaskKey, struct{}{}), tempSrc, dstDir); err != nil {
		rollbackStagedMove(ctx, tempSrc, srcDir, srcName, tempName)
		return err
	}

	movedTemp := path.Join(dstDir, tempName)
	if err := fs.Rename(ctx, movedTemp, dstName); err != nil {
		rollbackStagedMove(ctx, movedTemp, srcDir, srcName, tempName)
		return err
	}
	return nil
}

// moveFiles moves files and/or directories from src to dst.
// Individual item permission checks are skipped for performance reasons.
//
// See section 9.9.4 for when various HTTP status codes apply.
func moveFiles(ctx context.Context, src, dst string, overwrite bool) (status int, mutationStarted bool, err error) {
	srcDir := path.Dir(src)
	dstDir := path.Dir(dst)
	srcName := path.Base(src)
	dstName := path.Base(dst)
	user := ctx.Value(conf.UserKey).(*model.User)
	if srcDir != dstDir && !user.CanMove() {
		return http.StatusForbidden, false, nil
	}
	if srcName != dstName && !user.CanRename() {
		return http.StatusForbidden, false, nil
	}
	srcMeta, err := op.GetNearestMeta(srcDir)
	if err != nil && !errors.Is(errors.Cause(err), errs.MetaNotFound) {
		return http.StatusInternalServerError, false, err
	}
	dstMeta, err := op.GetNearestMeta(dstDir)
	if err != nil && !errors.Is(errors.Cause(err), errs.MetaNotFound) {
		return http.StatusInternalServerError, false, err
	}
	if !common.CanWrite(user, srcMeta, srcDir) || !common.CanWrite(user, dstMeta, dstDir) {
		return http.StatusForbidden, false, nil
	}

	dstExists, err := resourceExists(ctx, dst)
	if err != nil {
		return http.StatusInternalServerError, false, err
	}
	if dstExists && !overwrite {
		return http.StatusPreconditionFailed, false, nil
	}
	if dstExists {
		mutationStarted = true
		absent, verifyErr := removeProviderOverwriteDestination(ctx, dst)
		if verifyErr != nil {
			return http.StatusInternalServerError, true, verifyErr
		}
		if !absent {
			// Do not move the source until the provider has stopped exposing
			// the overwritten destination. 115 requires two absence observations
			// across a short consistency fence before the final name is reused.
			return http.StatusServiceUnavailable, true, nil
		}
	}

	mutationStarted = true
	switch {
	case srcDir == dstDir:
		err = fs.Rename(ctx, src, dstName)
	case moveNeedsStaging(src, dst):
		err = moveAcrossDirsExact(ctx, src, dst)
	default:
		_, err = fs.Move(context.WithValue(ctx, conf.NoTaskKey, struct{}{}), src, dstDir)
	}
	if err != nil {
		return http.StatusInternalServerError, true, err
	}
	if dstExists {
		return http.StatusNoContent, true, nil
	}
	return http.StatusCreated, true, nil
}

// copyFiles copies files and/or directories from src to dst.
// Individual item permission checks are skipped for performance reasons.
//
// See section 9.8.5 for when various HTTP status codes apply.
func copyFiles(ctx context.Context, src, dst string, overwrite bool, depth int) (status int, mutationStarted bool, err error) {
	srcDir := path.Dir(src)
	dstDir := path.Dir(dst)
	user := ctx.Value(conf.UserKey).(*model.User)
	if !user.CanCopy() {
		return http.StatusForbidden, false, nil
	}
	srcMeta, err := op.GetNearestMeta(srcDir)
	if err != nil && !errors.Is(errors.Cause(err), errs.MetaNotFound) {
		return http.StatusInternalServerError, false, err
	}
	if !common.CanRead(user, srcMeta, srcDir) {
		return http.StatusForbidden, false, nil
	}
	dstMeta, err := op.GetNearestMeta(dstDir)
	if err != nil && !errors.Is(errors.Cause(err), errs.MetaNotFound) {
		return http.StatusInternalServerError, false, err
	}
	if !common.CanWrite(user, dstMeta, dstDir) {
		return http.StatusForbidden, false, nil
	}

	srcObj, err := resourceObject(ctx, src)
	if err != nil {
		if errs.IsObjectNotFound(err) {
			return http.StatusNotFound, false, err
		}
		return http.StatusInternalServerError, false, err
	}
	if srcObj.IsDir() && strings.HasPrefix(utils.FixAndCleanPath(dst), utils.FixAndCleanPath(src)+"/") {
		return http.StatusForbidden, false, errInvalidDestination
	}
	dstParent, err := resourceObject(ctx, dstDir)
	if err != nil {
		if errs.IsObjectNotFound(err) {
			return http.StatusConflict, false, err
		}
		return http.StatusInternalServerError, false, err
	}
	if !dstParent.IsDir() {
		return http.StatusConflict, false, errNotADirectory
	}

	dstExists, err := resourceExists(ctx, dst)
	if err != nil {
		return http.StatusInternalServerError, false, err
	}
	if dstExists && !overwrite {
		return http.StatusPreconditionFailed, false, nil
	}
	if dstExists {
		mutationStarted = true
		absent, verifyErr := removeProviderOverwriteDestination(ctx, dst)
		if verifyErr != nil {
			return http.StatusInternalServerError, true, verifyErr
		}
		if !absent {
			return http.StatusServiceUnavailable, true, nil
		}
	}

	mutationStarted = true

	// Native provider COPY is safe and efficient only when the final name is
	// unchanged. A different WebDAV destination name must never use
	// dstDir/srcName as a transient object because that path may belong to an
	// unrelated file.
	if copyCanUseNative(src, dst, depth) {
		_, err = fs.Copy(context.WithValue(ctx, conf.NoTaskKey, struct{}{}), src, dstDir)
	} else if srcObj.IsDir() {
		err = copyExactTree(ctx, src, dst, srcObj, depth)
	} else {
		err = copyExactFile(ctx, src, dst)
	}
	if err != nil {
		return http.StatusInternalServerError, true, err
	}
	if dstExists {
		return http.StatusNoContent, true, nil
	}
	return http.StatusCreated, true, nil
}

// walkFS traverses filesystem fs starting at name up to depth levels.
//
// Allowed values for depth are 0, 1 or infiniteDepth. For each visited node,
// walkFS calls walkFn. If a visited file system node is a directory and
// walkFn returns path.SkipDir, walkFS will skip traversal of this node.
func walkFS(ctx context.Context, depth int, name string, info model.Obj, walkFn func(reqPath string, info model.Obj, err error) error) error {
	// This implementation is based on Walk's code in the standard path/path package.
	err := walkFn(name, info, nil)
	if err != nil {
		if info.IsDir() && err == filepath.SkipDir {
			return nil
		}
		return err
	}
	if !info.IsDir() || depth == 0 {
		return nil
	}
	if depth == 1 {
		depth = 0
	}
	meta, _ := op.GetNearestMeta(name)
	// Read directory names. In write-back mode, a recent successful provider
	// snapshot is sufficient for ordinary Cloud Sync revalidation; expired
	// snapshots are refreshed once and coalesced by parent.
	listCtx := context.WithValue(ctx, conf.MetaKey, meta)
	var objs []model.Obj
	if writeback.Enabled() {
		objs, _, err = writeback.ProviderListForWebDAV(listCtx, name)
	} else {
		objs, err = fs.List(listCtx, name, &fs.ListArgs{})
	}
	if writeback.Enabled() {
		remoteReliable := err == nil
		_, canonicalParent := info.(*writeback.CanonicalObject)
		overlaid, hasWriteback, overlayErr := writeback.OverlayListChildren(listCtx, name, objs, remoteReliable)
		if overlayErr != nil {
			return walkFn(name, info, overlayErr)
		}
		// A just-created canonical directory is a valid empty collection even
		// before an eventually-consistent provider can list it. Reuse the parent
		// object already resolved by PROPFIND instead of re-reading its DB row.
		if remoteReliable || hasWriteback || canonicalParent {
			objs = overlaid
			err = nil
		}
	}
	//f, err := fs.OpenFile(ctx, name, os.O_RDONLY, 0)
	//if err != nil {
	//	return walkFn(name, info, err)
	//}
	//fileInfos, err := f.Readdir(0)
	//f.Close()
	if err != nil {
		return walkFn(name, info, err)
	}

	for _, fileInfo := range objs {
		filename := path.Join(name, fileInfo.GetName())
		if err != nil {
			if err := walkFn(filename, fileInfo, err); err != nil && err != filepath.SkipDir {
				return err
			}
		} else {
			err = walkFS(ctx, depth, filename, fileInfo, walkFn)
			if err != nil {
				if !fileInfo.IsDir() || err != filepath.SkipDir {
					return err
				}
			}
		}
	}
	return nil
}
