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

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/fs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/internal/stream"
	"github.com/OpenListTeam/OpenList/v4/internal/writeback"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/OpenListTeam/OpenList/v4/server/common"
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
		missing, err := writeback.ReconcileDirect(ctx, name)
		if err != nil {
			return nil, err
		}
		if missing {
			return nil, errs.ObjectNotFound
		}
		obj, found, deleted, err := writeback.Canonical(name)
		if err != nil {
			return nil, err
		}
		if found {
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
	return depth == infiniteDepth &&
		path.Dir(src) != path.Dir(dst) &&
		path.Base(src) == path.Base(dst)
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

// moveFiles moves files and/or directories from src to dst.
// Individual item permission checks are skipped for performance reasons.
//
// See section 9.9.4 for when various HTTP status codes apply.
func moveFiles(ctx context.Context, src, dst string, overwrite bool) (status int, err error) {
	srcDir := path.Dir(src)
	dstDir := path.Dir(dst)
	srcName := path.Base(src)
	dstName := path.Base(dst)
	user := ctx.Value(conf.UserKey).(*model.User)
	if srcDir != dstDir && !user.CanMove() {
		return http.StatusForbidden, nil
	}
	if srcName != dstName && !user.CanRename() {
		return http.StatusForbidden, nil
	}
	srcMeta, err := op.GetNearestMeta(srcDir)
	if err != nil && !errors.Is(errors.Cause(err), errs.MetaNotFound) {
		return http.StatusInternalServerError, err
	}
	dstMeta, err := op.GetNearestMeta(dstDir)
	if err != nil && !errors.Is(errors.Cause(err), errs.MetaNotFound) {
		return http.StatusInternalServerError, err
	}
	if !common.CanWrite(user, srcMeta, srcDir) || !common.CanWrite(user, dstMeta, dstDir) {
		return http.StatusForbidden, nil
	}
	dstExists, err := resourceExists(ctx, dst)
	if err != nil {
		return http.StatusInternalServerError, err
	}
	if dstExists && !overwrite {
		return http.StatusPreconditionFailed, nil
	}
	if srcDir == dstDir {
		err = fs.Rename(ctx, src, dstName)
	} else {
		_, err = fs.Move(context.WithValue(ctx, conf.NoTaskKey, struct{}{}), src, dstDir)
		if err != nil {
			return http.StatusInternalServerError, err
		}
		if srcName != dstName {
			err = fs.Rename(ctx, path.Join(dstDir, srcName), dstName)
		}
	}
	if err != nil {
		return http.StatusInternalServerError, err
	}
	if dstExists {
		return http.StatusNoContent, nil
	}
	return http.StatusCreated, nil
}

// copyFiles copies files and/or directories from src to dst.
// Individual item permission checks are skipped for performance reasons.
//
// See section 9.8.5 for when various HTTP status codes apply.
func copyFiles(ctx context.Context, src, dst string, overwrite bool, depth int) (status int, err error) {
	srcDir := path.Dir(src)
	dstDir := path.Dir(dst)
	user := ctx.Value(conf.UserKey).(*model.User)
	if !user.CanCopy() {
		return http.StatusForbidden, nil
	}
	srcMeta, err := op.GetNearestMeta(srcDir)
	if err != nil && !errors.Is(errors.Cause(err), errs.MetaNotFound) {
		return http.StatusInternalServerError, err
	}
	if !common.CanRead(user, srcMeta, srcDir) {
		return http.StatusForbidden, nil
	}
	dstMeta, err := op.GetNearestMeta(dstDir)
	if err != nil && !errors.Is(errors.Cause(err), errs.MetaNotFound) {
		return http.StatusInternalServerError, err
	}
	if !common.CanWrite(user, dstMeta, dstDir) {
		return http.StatusForbidden, nil
	}

	srcObj, err := resourceObject(ctx, src)
	if err != nil {
		if errs.IsObjectNotFound(err) {
			return http.StatusNotFound, err
		}
		return http.StatusInternalServerError, err
	}
	if srcObj.IsDir() && strings.HasPrefix(utils.FixAndCleanPath(dst), utils.FixAndCleanPath(src)+"/") {
		return http.StatusForbidden, errInvalidDestination
	}
	dstParent, err := resourceObject(ctx, dstDir)
	if err != nil {
		if errs.IsObjectNotFound(err) {
			return http.StatusConflict, err
		}
		return http.StatusInternalServerError, err
	}
	if !dstParent.IsDir() {
		return http.StatusConflict, errNotADirectory
	}

	dstExists, err := resourceExists(ctx, dst)
	if err != nil {
		return http.StatusInternalServerError, err
	}
	if dstExists && !overwrite {
		return http.StatusPreconditionFailed, nil
	}
	if dstExists {
		if err := fs.Remove(ctx, dst); err != nil {
			return http.StatusInternalServerError, err
		}
	}

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
		return http.StatusInternalServerError, err
	}
	if dstExists {
		return http.StatusNoContent, nil
	}
	return http.StatusCreated, nil
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
	// Read directory names.
	objs, err := fs.List(context.WithValue(ctx, conf.MetaKey, meta), name, &fs.ListArgs{})
	if writeback.Enabled() {
		remoteReliable := err == nil
		overlaid, hasWriteback, overlayErr := writeback.OverlayList(name, objs, remoteReliable)
		if overlayErr != nil {
			return walkFn(name, info, overlayErr)
		}
		canonicalParent := false
		if canonical, found, deleted, canonicalErr := writeback.Canonical(name); canonicalErr != nil {
			return walkFn(name, info, canonicalErr)
		} else if found && !deleted && canonical != nil && canonical.IsDir() {
			canonicalParent = true
		}
		// A just-created canonical directory is a valid empty collection even
		// before an eventually-consistent provider can list it. This is needed
		// for Cloud Sync's immediate MKCOL -> PROPFIND directory scan.
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
