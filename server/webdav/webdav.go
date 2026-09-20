// Copyright 2014 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package webdav provides a WebDAV server implementation.
package webdav // import "golang.org/x/net/webdav"

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/net"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/internal/setting"
	"github.com/OpenListTeam/OpenList/v4/internal/stream"
	"github.com/OpenListTeam/OpenList/v4/internal/writeback"
	"github.com/pkg/errors"

	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/fs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	log "github.com/sirupsen/logrus"
)

type Handler struct {
	// Prefix is the URL path prefix to strip from WebDAV resource paths.
	Prefix string
	// LockSystem is the lock management system.
	LockSystem LockSystem
	// Logger is an optional error logger. If non-nil, it will be called
	// for all HTTP requests.
	Logger func(*http.Request, error)
}

func (h *Handler) stripPrefix(p string) (string, int, error) {
	if h.Prefix == "" {
		return p, http.StatusOK, nil
	}
	if r := strings.TrimPrefix(p, h.Prefix); len(r) < len(p) {
		return r, http.StatusOK, nil
	}
	return p, http.StatusNotFound, errPrefixMismatch
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	status, err := http.StatusBadRequest, errUnsupportedMethod
	brw := newBufferedResponseWriter()
	useBufferedWriter := true
	if h.LockSystem == nil {
		status, err = http.StatusInternalServerError, errNoLockSystem
	} else {
		switch r.Method {
		case "OPTIONS":
			status, err = h.handleOptions(brw, r)
		case "GET", "HEAD", "POST":
			useBufferedWriter = false
			Writer := &common.WrittenResponseWriter{ResponseWriter: w}
			status, err = h.handleGetHeadPost(Writer, r)
			if status != 0 && Writer.IsWritten() {
				status = 0
			}
		case "DELETE":
			status, err = h.handleDelete(brw, r)
		case "PUT":
			status, err = h.handlePut(brw, r)
		case "MKCOL":
			status, err = h.handleMkcol(brw, r)
		case "COPY", "MOVE":
			status, err = h.handleCopyMove(brw, r)
		case "LOCK":
			status, err = h.handleLock(brw, r)
		case "UNLOCK":
			status, err = h.handleUnlock(brw, r)
		case "PROPFIND":
			status, err = h.handlePropfind(brw, r)
			// if there is a error for PROPFIND, we should be as an empty folder to the client
			if err != nil {
				status = http.StatusNotFound
			}
		case "PROPPATCH":
			status, err = h.handleProppatch(brw, r)
		}
	}

	if status != 0 {
		w.WriteHeader(status)
		if status != http.StatusNoContent {
			w.Write([]byte(StatusText(status)))
		}
	} else if useBufferedWriter {
		brw.WriteToResponse(w)
	}
	if h.Logger != nil && err != nil {
		h.Logger(r, err)
	}
}

func (h *Handler) lock(now time.Time, root string) (token string, status int, err error) {
	token, err = h.LockSystem.Create(now, LockDetails{
		Root:      root,
		Duration:  infiniteTimeout,
		ZeroDepth: true,
	})
	if err != nil {
		if err == ErrLocked {
			return "", StatusLocked, err
		}
		return "", http.StatusInternalServerError, err
	}
	return token, 0, nil
}

func (h *Handler) confirmLocks(r *http.Request, src, dst string) (release func(), status int, err error) {
	hdr := r.Header.Get("If")
	if hdr == "" {
		// An empty If header means that the client hasn't previously created locks.
		// Even if this client doesn't care about locks, we still need to check that
		// the resources aren't locked by another client, so we create temporary
		// locks that would conflict with another client's locks. These temporary
		// locks are unlocked at the end of the HTTP request.
		now, srcToken, dstToken := time.Now(), "", ""
		if src != "" {
			srcToken, status, err = h.lock(now, src)
			if err != nil {
				return nil, status, err
			}
		}
		if dst != "" {
			dstToken, status, err = h.lock(now, dst)
			if err != nil {
				if srcToken != "" {
					h.LockSystem.Unlock(now, srcToken)
				}
				return nil, status, err
			}
		}

		return func() {
			if dstToken != "" {
				h.LockSystem.Unlock(now, dstToken)
			}
			if srcToken != "" {
				h.LockSystem.Unlock(now, srcToken)
			}
		}, 0, nil
	}

	ih, ok := parseIfHeader(hdr)
	if !ok {
		return nil, http.StatusBadRequest, errInvalidIfHeader
	}
	// ih is a disjunction (OR) of ifLists, so any ifList will do.
	for _, l := range ih.lists {
		lsrc := l.resourceTag
		if lsrc == "" {
			lsrc = src
		} else {
			u, err := url.Parse(lsrc)
			if err != nil {
				continue
			}
			if u.Host != r.Host {
				continue
			}
			lsrc, status, err = h.stripPrefix(u.Path)
			if err != nil {
				return nil, status, err
			}
		}
		release, err = h.LockSystem.Confirm(time.Now(), lsrc, dst, l.conditions...)
		if err == ErrConfirmationFailed {
			continue
		}
		if err != nil {
			return nil, http.StatusInternalServerError, err
		}
		return release, 0, nil
	}
	// Section 10.4.1 says that "If this header is evaluated and all state lists
	// fail, then the request must fail with a 412 (Precondition Failed) status."
	// We follow the spec even though the cond_put_corrupt_token test case from
	// the litmus test warns on seeing a 412 instead of a 423 (Locked).
	return nil, http.StatusPreconditionFailed, ErrLocked
}

func (h *Handler) handleOptions(w http.ResponseWriter, r *http.Request) (status int, err error) {
	reqPath, status, err := h.stripPrefix(r.URL.Path)
	if err != nil {
		return status, err
	}
	ctx := r.Context()
	user := ctx.Value(conf.UserKey).(*model.User)
	reqPath, err = user.JoinPath(reqPath)
	if err != nil {
		return http.StatusForbidden, err
	}
	allow := "OPTIONS, LOCK, PUT, MKCOL"
	fi, found, deleted, wbErr := writeback.Canonical(reqPath)
	if wbErr != nil {
		return http.StatusInternalServerError, wbErr
	}
	if !found {
		fi, err = fs.Get(ctx, reqPath, &fs.GetArgs{})
	} else if deleted {
		fi = nil
		err = errs.ObjectNotFound
	}
	if err == nil && fi != nil {
		if fi.IsDir() {
			allow = "OPTIONS, LOCK, DELETE, PROPPATCH, COPY, MOVE, UNLOCK, PROPFIND"
		} else {
			allow = "OPTIONS, LOCK, GET, HEAD, POST, DELETE, PROPPATCH, COPY, MOVE, UNLOCK, PROPFIND, PUT"
		}
	}
	w.Header().Set("Allow", allow)
	// http://www.webdav.org/specs/rfc4918.html#dav.compliance.classes
	w.Header().Set("DAV", "1, 2")
	// http://msdn.microsoft.com/en-au/library/cc250217.aspx
	w.Header().Set("MS-Author-Via", "DAV")
	return 0, nil
}

func (h *Handler) handleGetHeadPost(w http.ResponseWriter, r *http.Request) (status int, err error) {
	reqPath, status, err := h.stripPrefix(r.URL.Path)
	if err != nil {
		return status, err
	}
	// TODO: check locks for read-only access??
	ctx := r.Context()
	user := ctx.Value(conf.UserKey).(*model.User)
	password, _ := ctx.Value(conf.MetaPassKey).(string)
	reqPath, err = user.JoinPath(reqPath)
	if err != nil {
		return http.StatusForbidden, err
	}
	meta, err := op.GetNearestMeta(reqPath)
	if err != nil && !errors.Is(errors.Cause(err), errs.MetaNotFound) {
		return http.StatusInternalServerError, err
	}
	if !common.CanAccess(user, meta, reqPath, password) {
		return http.StatusForbidden, errs.PermissionDenied
	}
	if writeback.Enabled() {
		missing, wbErr := writeback.ReconcileDirect(ctx, reqPath)
		if wbErr != nil {
			return http.StatusInternalServerError, wbErr
		}
		if missing {
			return http.StatusNotFound, errs.ObjectNotFound
		}
	}
	fi, found, deleted, wbErr := writeback.Canonical(reqPath)
	if wbErr != nil {
		return http.StatusInternalServerError, wbErr
	}
	if deleted {
		return http.StatusNotFound, errs.ObjectNotFound
	}
	if !found {
		fi, err = fs.Get(ctx, reqPath, &fs.GetArgs{})
		if err != nil {
			return http.StatusNotFound, err
		}
	}
	if fi.IsDir() {
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Type", "httpd/unix-directory")
			w.Header().Set("Content-Length", "0")
			return http.StatusOK, nil
		}
		return http.StatusMethodNotAllowed, nil
	}
	var canonicalRow *model.WebDAVWritebackObject
	if writeback.Enabled() && found {
		localFile, row, localErr := writeback.OpenLocal(reqPath)
		if localErr != nil {
			return http.StatusInternalServerError, localErr
		}
		canonicalRow = row
		if row != nil {
			w.Header().Set("ETag", row.ETag)
			w.Header().Set("Last-Modified", row.ModTime.UTC().Format(http.TimeFormat))
			if row.MimeType != "" {
				w.Header().Set("Content-Type", row.MimeType)
			}
			if r.Method == http.MethodGet || r.Method == http.MethodHead {
				if status := canonicalReadPreconditionStatus(r, row.ETag, row.ModTime); status != 0 {
					return status, nil
				}
				r = canonicalReadRequest(r, row.ETag, row.ModTime)
				if row.Size >= 0 && r.Header.Get("Range") == "" {
					w.Header().Set("Content-Length", strconv.FormatInt(row.Size, 10))
				}
			}
		}
		if localFile != nil {
			defer localFile.Close()
			http.ServeContent(w, r, row.Name, row.ModTime, localFile)
			return 0, nil
		}
	}
	// Let ServeContent determine the Content-Type header.
	storage, _ := fs.GetStorage(reqPath, &fs.GetStoragesArgs{})
	if !found && storage.GetStorage().Webdav302() {
		link, _, err := fs.Link(ctx, reqPath, model.LinkArgs{IP: utils.ClientIP(r), Header: r.Header, Redirect: true})
		if err != nil {
			return http.StatusInternalServerError, err
		}
		defer link.Close()
		http.Redirect(w, r, link.URL, http.StatusFound)
		return 0, nil
	}

	if !found && storage.GetStorage().WebdavProxyURL() {
		if url := common.GenerateDownProxyURL(storage.GetStorage(), reqPath); url != "" {
			w.Header().Set("Cache-Control", "max-age=0, no-cache, no-store, must-revalidate")
			http.Redirect(w, r, url, http.StatusFound)
			return 0, nil
		}
	}

	link, _, err := fs.Link(ctx, reqPath, model.LinkArgs{Header: r.Header})
	if err != nil {
		return http.StatusInternalServerError, err
	}
	defer link.Close()

	if storage.GetStorage().ProxyRange || canonicalRow != nil {
		link = common.ProxyRange(ctx, link, fi.GetSize())
	}
	err = common.Proxy(w, r, link, fi)
	if err != nil {
		if statusCode, ok := errs.UnwrapOrSelf(err).(net.HttpStatusCodeError); ok {
			return int(statusCode), err
		}
		return http.StatusInternalServerError, fmt.Errorf("webdav proxy error: %+v", err)
	}
	return 0, nil
}

func (h *Handler) handleDelete(w http.ResponseWriter, r *http.Request) (status int, err error) {
	reqPath, status, err := h.stripPrefix(r.URL.Path)
	if err != nil {
		return status, err
	}
	release, status, err := h.confirmLocks(r, reqPath, "")
	if err != nil {
		return status, err
	}
	defer release()

	ctx := r.Context()
	user := ctx.Value(conf.UserKey).(*model.User)
	if !user.CanRemove() {
		return http.StatusForbidden, nil
	}
	reqPath, err = user.JoinPath(reqPath)
	if err != nil {
		return http.StatusForbidden, err
	}
	// TODO: return MultiStatus where appropriate.
	if writeback.Enabled() {
		handled, wbErr := writeback.DeleteTree(reqPath)
		if wbErr != nil {
			return http.StatusInternalServerError, wbErr
		}
		if handled {
			return http.StatusNoContent, nil
		}
	}

	// "godoc os RemoveAll" says that "If the path does not exist, RemoveAll
	// returns nil (no error)." WebDAV semantics are that it should return a
	// "404 Not Found". We therefore have to Stat before we RemoveAll.
	if _, err := fs.Get(ctx, reqPath, &fs.GetArgs{}); err != nil {
		if errs.IsObjectNotFound(err) {
			return http.StatusNotFound, err
		}
		return http.StatusMethodNotAllowed, err
	}
	parentPath := path.Dir(reqPath)
	parentMeta, err := op.GetNearestMeta(parentPath)
	if err != nil && !errors.Is(errors.Cause(err), errs.MetaNotFound) {
		return http.StatusInternalServerError, err
	}
	if !common.CanWrite(user, parentMeta, parentPath) {
		return http.StatusForbidden, errs.PermissionDenied
	}
	if err := fs.Remove(ctx, reqPath); err != nil {
		return http.StatusMethodNotAllowed, err
	}
	//fs.ClearCache(path.Dir(reqPath))
	return http.StatusNoContent, nil
}

func (h *Handler) handlePut(w http.ResponseWriter, r *http.Request) (status int, err error) {
	defer func() {
		if n, _ := io.ReadFull(r.Body, []byte{0}); n == 1 {
			_, _ = utils.CopyWithBuffer(io.Discard, r.Body)
		}
		_ = r.Body.Close()
	}()
	reqPath, status, err := h.stripPrefix(r.URL.Path)
	if err != nil {
		return status, err
	}
	if reqPath == "" {
		return http.StatusMethodNotAllowed, nil
	}
	release, status, err := h.confirmLocks(r, reqPath, "")
	if err != nil {
		return status, err
	}
	defer release()
	// Canonical write-back metadata owns conditional PUT evaluation so provider
	// ETags/mtimes cannot make a Cloud Sync retry target the wrong generation.
	ctx := r.Context()
	user := ctx.Value(conf.UserKey).(*model.User)
	reqPath, err = user.JoinPath(reqPath)
	if err != nil {
		return http.StatusForbidden, err
	}
	size := r.ContentLength
	if size < 0 {
		sizeStr := r.Header.Get("X-File-Size")
		if sizeStr != "" {
			size, err = strconv.ParseInt(sizeStr, 10, 64)
			if err != nil {
				return http.StatusBadRequest, err
			}
		}
	}
	obj := model.Object{
		Name:     path.Base(reqPath),
		Size:     size,
		Modified: h.getModTime(r),
		Ctime:    h.getCreateTime(r),
	}
	// Check if system file should be ignored
	if setting.GetBool(conf.IgnoreSystemFiles) && utils.IsSystemFile(obj.Name) {
		return http.StatusForbidden, errs.IgnoredSystemFile
	}
	parentPath := path.Dir(reqPath)
	parentMeta, err := op.GetNearestMeta(parentPath)
	if err != nil && !errors.Is(errors.Cause(err), errs.MetaNotFound) {
		return http.StatusInternalServerError, err
	}
	if !user.CanWriteContent() && !common.CanWriteContentBypassUserPerms(parentMeta, parentPath) {
		return http.StatusForbidden, errs.PermissionDenied
	}
	if !common.CanWrite(user, parentMeta, parentPath) {
		return http.StatusForbidden, errs.PermissionDenied
	}
	if writeback.Enabled() {
		parentObj, parentFound, parentDeleted, wbErr := writeback.Canonical(parentPath)
		if wbErr != nil {
			return http.StatusInternalServerError, wbErr
		}
		if parentFound && (parentDeleted || parentObj == nil || !parentObj.IsDir()) {
			return http.StatusConflict, errs.ObjectNotFound
		}
	}

	// Cloud Sync normally relies on the immediate PROPFIND result, but WebDAV
	// clients may also use entity-tag preconditions while retrying an upload.
	// Only resolve the current provider object when a condition header exists;
	// ordinary Cloud Sync PUTs stay independent of slow/eventually-consistent
	// provider lookups.
	ifMatch := r.Header.Get("If-Match")
	ifNoneMatch := r.Header.Get("If-None-Match")
	ifUnmodifiedSince := r.Header.Get("If-Unmodified-Since")
	if ifMatch != "" || ifNoneMatch != "" || ifUnmodifiedSince != "" {
		var current model.Obj
		exists := false
		current, found, deleted, wbErr := writeback.Canonical(reqPath)
		if wbErr != nil {
			return http.StatusInternalServerError, wbErr
		}
		if found {
			exists = !deleted && current != nil
		} else {
			current, err = fs.Get(ctx, reqPath, &fs.GetArgs{})
			if err == nil {
				exists = true
			} else if errs.IsObjectNotFound(err) {
				err = nil
				current = nil
			} else {
				return http.StatusInternalServerError, err
			}
		}
		etag := ""
		modTime := time.Time{}
		if exists {
			modTime = current.ModTime()
			if ifMatch != "" || ifNoneMatch != "" {
				etag, err = findETag(ctx, h.LockSystem, reqPath, current)
				if err != nil {
					return http.StatusInternalServerError, err
				}
			}
		}
		if putPreconditionFailed(ifMatch, ifNoneMatch, ifUnmodifiedSince, exists, etag, modTime) {
			return http.StatusPreconditionFailed, nil
		}
	}

	if writeback.Enabled() {
		if current, found, deleted, wbErr := writeback.Canonical(reqPath); wbErr != nil {
			return http.StatusInternalServerError, wbErr
		} else if found && !deleted && current != nil && current.IsDir() {
			return http.StatusMethodNotAllowed, nil
		}
	}

	mimeType := r.Header.Get("Content-Type")
	if mimeType == "" {
		mimeType = utils.GetMimeType(reqPath)
	}
	if writeback.Enabled() {
		// Only explicitly supplied client timestamps may mutate an existing
		// canonical generation. The legacy WebDAV helpers intentionally fall
		// back to time.Now()/mtime for synchronous uploads, which would make a
		// header-less Cloud Sync retry look like a metadata change.
		writebackModTime := h.getWritebackHeaderTime(r, "X-OC-Mtime")
		writebackCreateTime := h.getWritebackHeaderTime(r, "X-OC-Ctime")
		row, created, wbErr := writeback.Commit(ctx, reqPath, r.Body, size, writebackModTime, writebackCreateTime, mimeType)
		if wbErr != nil {
			if strings.Contains(wbErr.Error(), "free space") {
				return StatusInsufficientStorage, wbErr
			}
			return http.StatusInternalServerError, wbErr
		}
		w.Header().Set("Etag", row.ETag)
		w.Header().Set("Last-Modified", row.ModTime.UTC().Format(http.TimeFormat))
		if !writebackModTime.IsZero() {
			w.Header().Set("X-OC-MTime", "accepted")
		}
		if created {
			return http.StatusCreated, nil
		}
		return http.StatusNoContent, nil
	}
	fsStream := &stream.FileStream{
		Obj:      &obj,
		Reader:   r.Body,
		Mimetype: mimeType,
	}
	err = fs.PutDirectly(ctx, path.Dir(reqPath), fsStream)
	if errs.IsNotFoundError(err) {
		return http.StatusNotFound, err
	}

	// TODO(rost): Returning 405 Method Not Allowed might not be appropriate.
	if err != nil {
		return http.StatusMethodNotAllowed, err
	}
	fi, err := fs.Get(ctx, reqPath, &fs.GetArgs{})
	if err != nil {
		fi = &obj
	}
	etag, err := findETag(ctx, h.LockSystem, reqPath, fi)
	if err != nil {
		return http.StatusInternalServerError, err
	}
	w.Header().Set("Etag", etag)
	return http.StatusCreated, nil
}

func (h *Handler) handleMkcol(w http.ResponseWriter, r *http.Request) (status int, err error) {
	reqPath, status, err := h.stripPrefix(r.URL.Path)
	if err != nil {
		return status, err
	}
	release, status, err := h.confirmLocks(r, reqPath, "")
	if err != nil {
		return status, err
	}
	defer release()

	ctx := r.Context()
	user := ctx.Value(conf.UserKey).(*model.User)
	reqPath, err = user.JoinPath(reqPath)
	if err != nil {
		return http.StatusForbidden, err
	}

	if r.ContentLength > 0 {
		return http.StatusUnsupportedMediaType, nil
	}

	// RFC 4918 9.3.1: MKCOL can only create an unmapped URL. In write-back
	// mode the canonical shadow is authoritative while the provider catches up.
	if writeback.Enabled() {
		fi, found, deleted, wbErr := writeback.Canonical(reqPath)
		if wbErr != nil {
			return http.StatusInternalServerError, wbErr
		}
		if found && !deleted && fi != nil {
			return http.StatusMethodNotAllowed, nil
		}
		if !found {
			if _, getErr := fs.Get(ctx, reqPath, &fs.GetArgs{}); getErr == nil {
				return http.StatusMethodNotAllowed, nil
			} else if !errs.IsObjectNotFound(getErr) {
				return http.StatusMethodNotAllowed, getErr
			}
		}

		parentPath := path.Dir(reqPath)
		parentOK := false
		parentObj, parentFound, parentDeleted, wbErr := writeback.Canonical(parentPath)
		if wbErr != nil {
			return http.StatusInternalServerError, wbErr
		}
		if parentFound && !parentDeleted && parentObj != nil {
			parentOK = parentObj.IsDir()
		} else if !parentFound {
			parentObj, getErr := fs.Get(ctx, parentPath, &fs.GetArgs{})
			if getErr == nil {
				parentOK = parentObj.IsDir()
			} else if !errs.IsObjectNotFound(getErr) {
				return http.StatusMethodNotAllowed, getErr
			}
		}
		if !parentOK {
			return http.StatusConflict, errs.ObjectNotFound
		}

		parentMeta, metaErr := op.GetNearestMeta(parentPath)
		if metaErr != nil && !errors.Is(errors.Cause(metaErr), errs.MetaNotFound) {
			return http.StatusInternalServerError, metaErr
		}
		if !user.CanWriteContent() && !common.CanWriteContentBypassUserPerms(parentMeta, parentPath) {
			return http.StatusForbidden, errs.PermissionDenied
		}
		if !common.CanWrite(user, parentMeta, parentPath) {
			return http.StatusForbidden, errs.PermissionDenied
		}
		_, _, wbErr = writeback.CommitDir(ctx, reqPath, time.Now(), time.Now())
		if errors.Is(wbErr, writeback.ErrDestinationExists) {
			return http.StatusMethodNotAllowed, wbErr
		}
		if wbErr != nil {
			return http.StatusInternalServerError, wbErr
		}
		return http.StatusCreated, nil
	}

	// Standard synchronous OpenList WebDAV behavior when write-back is disabled.
	if _, err := fs.Get(ctx, reqPath, &fs.GetArgs{}); err == nil {
		return http.StatusMethodNotAllowed, err
	}
	parentPath := path.Dir(reqPath)
	if _, err := fs.Get(ctx, parentPath, &fs.GetArgs{}); err != nil {
		if errs.IsObjectNotFound(err) {
			return http.StatusConflict, err
		}
		return http.StatusMethodNotAllowed, err
	}
	parentMeta, err := op.GetNearestMeta(parentPath)
	if err != nil && !errors.Is(errors.Cause(err), errs.MetaNotFound) {
		return http.StatusInternalServerError, err
	}
	if !user.CanWriteContent() && !common.CanWriteContentBypassUserPerms(parentMeta, parentPath) {
		return http.StatusForbidden, errs.PermissionDenied
	}
	if !common.CanWrite(user, parentMeta, parentPath) {
		return http.StatusForbidden, errs.PermissionDenied
	}
	if err := fs.MakeDir(ctx, reqPath); err != nil {
		if os.IsNotExist(err) {
			return http.StatusConflict, err
		}
		return http.StatusMethodNotAllowed, err
	}
	return http.StatusCreated, nil
}

func copyMoveProviderSucceeded(status int) bool {
	return status == http.StatusCreated || status == http.StatusNoContent
}

func retryMetadataReconciliation(ctx context.Context, fn func() error) error {
	var lastErr error
	for attempt, delay := range []time.Duration{0, 50 * time.Millisecond, 200 * time.Millisecond} {
		if attempt > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
		if err := fn(); err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	return lastErr
}

func providerOperationResponseStatus(op *model.WebDAVProviderOperation) int {
	if op != nil && op.DestinationExisted {
		return http.StatusNoContent
	}
	return http.StatusCreated
}

func recoverProviderCopyMove(ctx context.Context, method, src, dst string, depth int, currentSource model.Obj) (status int, handled bool, err error) {
	op, err := writeback.GetProviderOperation(method, src, dst, depth)
	if err != nil || op == nil {
		return 0, false, err
	}
	if currentSource != nil && !writeback.ProviderOperationSourceMatches(op, currentSource) {
		// The source changed after an older intent. That intent must not suppress
		// a new client operation against the new source generation.
		if finishErr := writeback.FinishProviderOperation(op.ID); finishErr != nil {
			return http.StatusServiceUnavailable, true, finishErr
		}
		return 0, false, nil
	}

	if strings.EqualFold(method, writeback.ProviderOperationCopy) {
		if applied, appliedErr := writeback.ProviderOperationMetadataApplied(op); appliedErr != nil {
			return http.StatusServiceUnavailable, true, appliedErr
		} else if applied {
			_ = writeback.FinishProviderOperation(op.ID)
			return providerOperationResponseStatus(op), true, nil
		}
	}

	recovery, _, recoveryErr := writeback.RecoverProviderOperation(ctx, op)
	if recoveryErr != nil {
		return http.StatusServiceUnavailable, true, recoveryErr
	}
	switch recovery {
	case writeback.ProviderOperationNotApplied:
		if op.State == writeback.ProviderOperationApplied {
			// An already-finished intent now points at a different/recreated
			// source. Retire it before starting a fresh client operation.
			if finishErr := writeback.FinishProviderOperation(op.ID); finishErr != nil {
				return http.StatusServiceUnavailable, true, finishErr
			}
		}
		return 0, false, nil
	case writeback.ProviderOperationInconclusive:
		return http.StatusServiceUnavailable, true, nil
	}

	if strings.EqualFold(method, writeback.ProviderOperationMove) {
		if applied, appliedErr := writeback.ProviderOperationMetadataApplied(op); appliedErr != nil {
			return http.StatusServiceUnavailable, true, appliedErr
		} else if applied {
			_ = writeback.FinishProviderOperation(op.ID)
			return providerOperationResponseStatus(op), true, nil
		}
	}

	sourceRoot := currentSource
	if sourceRoot == nil {
		sourceRoot = writeback.ProviderOperationSourceObject(op)
	}
	reconcile := func() error {
		if strings.EqualFold(method, writeback.ProviderOperationMove) {
			return writeback.MoveTreeMetadata(src, dst, sourceRoot)
		}
		return writeback.CopyTreeMetadata(src, dst, sourceRoot)
	}
	if reconcileErr := retryMetadataReconciliation(ctx, reconcile); reconcileErr != nil {
		return http.StatusServiceUnavailable, true, reconcileErr
	}
	if finishErr := writeback.FinishProviderOperation(op.ID); finishErr != nil {
		log.Warnf("provider %s metadata reconciled but intent cleanup failed for %s -> %s: %v", method, src, dst, finishErr)
	}
	return providerOperationResponseStatus(op), true, nil
}

func prepareProviderOperation(method, src, dst string, depth int, source model.Obj, overwrite, destinationExisted bool) (*model.WebDAVProviderOperation, error) {
	op, err := writeback.PrepareProviderOperation(method, src, dst, depth, source, overwrite, destinationExisted)
	if err != nil {
		return nil, err
	}
	if op == nil {
		return nil, nil
	}
	if err := writeback.MarkProviderOperationStarted(op.ID); err != nil {
		return nil, err
	}
	op.State = writeback.ProviderOperationStarted
	return op, nil
}

func (h *Handler) handleCopyMove(w http.ResponseWriter, r *http.Request) (status int, err error) {
	hdr := r.Header.Get("Destination")
	if hdr == "" {
		return http.StatusBadRequest, errInvalidDestination
	}
	u, err := url.Parse(hdr)
	if err != nil {
		return http.StatusBadRequest, errInvalidDestination
	}
	if u.Host != "" && u.Host != r.Host {
		return http.StatusBadGateway, errInvalidDestination
	}

	src, status, err := h.stripPrefix(r.URL.Path)
	if err != nil {
		return status, err
	}
	dst, status, err := h.stripPrefix(u.Path)
	if err != nil {
		return status, err
	}
	if dst == "" {
		return http.StatusBadGateway, errInvalidDestination
	}
	if dst == src {
		return http.StatusForbidden, errDestinationEqualsSource
	}

	ctx := r.Context()
	user := ctx.Value(conf.UserKey).(*model.User)
	src, err = user.JoinPath(src)
	if err != nil {
		return http.StatusForbidden, err
	}
	dst, err = user.JoinPath(dst)
	if err != nil {
		return http.StatusForbidden, err
	}

	if r.Method == "COPY" {
		release, status, err := h.confirmLocks(r, "", dst)
		if err != nil {
			return status, err
		}
		defer release()

		depth := infiniteDepth
		if hdr := r.Header.Get("Depth"); hdr != "" {
			depth = parseDepth(hdr)
			if depth != 0 && depth != infiniteDepth {
				return http.StatusBadRequest, errInvalidDepth
			}
		}
		overwrite := r.Header.Get("Overwrite") != "F"
		dstExisted := false
		dstTracked := false
		var sourceRoot model.Obj
		if writeback.Enabled() {
			sourceRoot, err = resourceObject(ctx, src)
			if err != nil {
				if errs.IsObjectNotFound(err) {
					return http.StatusNotFound, err
				}
				return http.StatusInternalServerError, err
			}
			recoveredStatus, recovered, recoveryErr := recoverProviderCopyMove(ctx, writeback.ProviderOperationCopy, src, dst, depth, sourceRoot)
			if recovered {
				if recoveredStatus == http.StatusServiceUnavailable {
					w.Header().Set("Retry-After", "2")
				}
				return recoveredStatus, recoveryErr
			}
			dstExisted, err = resourceExists(ctx, dst)
			if err != nil {
				return http.StatusInternalServerError, err
			}
			if dstExisted && !overwrite {
				return http.StatusPreconditionFailed, nil
			}
			_, dstTracked, _, err = writeback.Canonical(dst)
			if err != nil {
				return http.StatusInternalServerError, err
			}
			if !dstExisted || dstTracked {
				handled, overwritten, wbErr := writeback.CopyPending(src, dst, overwrite, depth != 0)
				if errors.Is(wbErr, writeback.ErrDestinationExists) {
					return http.StatusPreconditionFailed, wbErr
				}
				if wbErr != nil {
					return http.StatusInternalServerError, wbErr
				}
				if handled {
					_ = writeback.DiscardProviderOperation(writeback.ProviderOperationCopy, src, dst, depth)
					if dstExisted || overwritten {
						return http.StatusNoContent, nil
					}
					return http.StatusCreated, nil
				}
			}
			_, busy, prepErr := writeback.ProviderOverwriteReady(dst)
			if prepErr != nil {
				return http.StatusInternalServerError, prepErr
			}
			if busy {
				w.Header().Set("Retry-After", "2")
				return http.StatusServiceUnavailable, nil
			}
		}

		var providerOp *model.WebDAVProviderOperation
		if writeback.Enabled() {
			providerOp, err = prepareProviderOperation(writeback.ProviderOperationCopy, src, dst, depth, sourceRoot, overwrite, dstExisted)
			if err != nil {
				w.Header().Set("Retry-After", "2")
				return http.StatusServiceUnavailable, err
			}
		}
		copyStatus, copyErr := copyFiles(ctx, src, dst, overwrite, depth)
		if copyErr == nil && writeback.Enabled() && copyMoveProviderSucceeded(copyStatus) {
			if providerOp != nil {
				if markErr := writeback.MarkProviderOperationApplied(providerOp.ID); markErr != nil {
					log.Warnf("provider COPY succeeded but intent apply marker failed for %s -> %s: %v", src, dst, markErr)
				} else {
					providerOp.State = writeback.ProviderOperationApplied
				}
			}
			if wbErr := retryMetadataReconciliation(ctx, func() error {
				return writeback.CopyTreeMetadata(src, dst, sourceRoot)
			}); wbErr != nil {
				w.Header().Set("Retry-After", "2")
				return http.StatusServiceUnavailable, wbErr
			}
			if providerOp != nil {
				if finishErr := writeback.FinishProviderOperation(providerOp.ID); finishErr != nil {
					log.Warnf("provider COPY metadata reconciled but intent cleanup failed for %s -> %s: %v", src, dst, finishErr)
				}
			}
		}
		if copyStatus == http.StatusServiceUnavailable {
			w.Header().Set("Retry-After", "2")
		}
		if copyErr == nil && dstExisted && copyStatus == http.StatusCreated {
			copyStatus = http.StatusNoContent
		}
		return copyStatus, copyErr
	}

	release, status, err := h.confirmLocks(r, src, dst)
	if err != nil {
		return status, err
	}
	defer release()

	if hdr := r.Header.Get("Depth"); hdr != "" && parseDepth(hdr) != infiniteDepth {
		return http.StatusBadRequest, errInvalidDepth
	}

	overwrite := r.Header.Get("Overwrite") != "F"
	dstExisted := false
	dstTracked := false
	if writeback.Enabled() {
		recoveredStatus, recovered, recoveryErr := recoverProviderCopyMove(ctx, writeback.ProviderOperationMove, src, dst, -1, nil)
		if recovered {
			if recoveredStatus == http.StatusServiceUnavailable {
				w.Header().Set("Retry-After", "2")
			}
			return recoveredStatus, recoveryErr
		}
		dstExisted, err = resourceExists(ctx, dst)
		if err != nil {
			return http.StatusInternalServerError, err
		}
		if dstExisted && !overwrite {
			return http.StatusPreconditionFailed, nil
		}
		_, dstTracked, _, err = writeback.Canonical(dst)
		if err != nil {
			return http.StatusInternalServerError, err
		}
		if !dstExisted || dstTracked {
			handled, overwritten, wbErr := writeback.MovePending(src, dst, overwrite)
			if errors.Is(wbErr, writeback.ErrDestinationExists) {
				return http.StatusPreconditionFailed, wbErr
			}
			if wbErr != nil {
				return http.StatusInternalServerError, wbErr
			}
			if handled {
				_ = writeback.DiscardProviderOperation(writeback.ProviderOperationMove, src, dst, -1)
				if dstExisted || overwritten {
					return http.StatusNoContent, nil
				}
				return http.StatusCreated, nil
			}
		}
		_, busy, prepErr := writeback.ProviderOverwriteReady(dst)
		if prepErr != nil {
			return http.StatusInternalServerError, prepErr
		}
		if busy {
			w.Header().Set("Retry-After", "2")
			return http.StatusServiceUnavailable, nil
		}
	}

	var sourceRoot model.Obj
	var providerOp *model.WebDAVProviderOperation
	if writeback.Enabled() {
		sourceRoot, err = resourceObject(ctx, src)
		if err != nil {
			if errs.IsObjectNotFound(err) {
				return http.StatusNotFound, err
			}
			return http.StatusInternalServerError, err
		}
		providerOp, err = prepareProviderOperation(writeback.ProviderOperationMove, src, dst, -1, sourceRoot, overwrite, dstExisted)
		if err != nil {
			w.Header().Set("Retry-After", "2")
			return http.StatusServiceUnavailable, err
		}
	}
	moveStatus, moveErr := moveFiles(ctx, src, dst, overwrite)
	if moveErr == nil && writeback.Enabled() && copyMoveProviderSucceeded(moveStatus) {
		if providerOp != nil {
			if markErr := writeback.MarkProviderOperationApplied(providerOp.ID); markErr != nil {
				log.Warnf("provider MOVE succeeded but intent apply marker failed for %s -> %s: %v", src, dst, markErr)
			} else {
				providerOp.State = writeback.ProviderOperationApplied
			}
		}
		if wbErr := retryMetadataReconciliation(ctx, func() error {
			return writeback.MoveTreeMetadata(src, dst, sourceRoot)
		}); wbErr != nil {
			w.Header().Set("Retry-After", "2")
			return http.StatusServiceUnavailable, wbErr
		}
		if providerOp != nil {
			if finishErr := writeback.FinishProviderOperation(providerOp.ID); finishErr != nil {
				log.Warnf("provider MOVE metadata reconciled but intent cleanup failed for %s -> %s: %v", src, dst, finishErr)
			}
		}
	}
	if moveStatus == http.StatusServiceUnavailable {
		w.Header().Set("Retry-After", "2")
	}
	if moveErr == nil && dstExisted && moveStatus == http.StatusCreated {
		moveStatus = http.StatusNoContent
	}
	return moveStatus, moveErr
}

func (h *Handler) handleLock(w http.ResponseWriter, r *http.Request) (retStatus int, retErr error) {
	duration, err := parseTimeout(r.Header.Get("Timeout"))
	if err != nil {
		return http.StatusBadRequest, err
	}
	li, status, err := readLockInfo(r.Body)
	if err != nil {
		return status, err
	}

	ctx := r.Context()
	user := ctx.Value(conf.UserKey).(*model.User)
	token, ld, now, created := "", LockDetails{}, time.Now(), false
	if li == (lockInfo{}) {
		// An empty lockInfo means to refresh the lock.
		ih, ok := parseIfHeader(r.Header.Get("If"))
		if !ok {
			return http.StatusBadRequest, errInvalidIfHeader
		}
		if len(ih.lists) == 1 && len(ih.lists[0].conditions) == 1 {
			token = ih.lists[0].conditions[0].Token
		}
		if token == "" {
			return http.StatusBadRequest, errInvalidLockToken
		}
		ld, err = h.LockSystem.Refresh(now, token, duration)
		if err != nil {
			if err == ErrNoSuchLock {
				return http.StatusPreconditionFailed, err
			}
			return http.StatusInternalServerError, err
		}

	} else {
		// Section 9.10.3 says that "If no Depth header is submitted on a LOCK request,
		// then the request MUST act as if a "Depth:infinity" had been submitted."
		depth := infiniteDepth
		if hdr := r.Header.Get("Depth"); hdr != "" {
			depth = parseDepth(hdr)
			if depth != 0 && depth != infiniteDepth {
				// Section 9.10.3 says that "Values other than 0 or infinity must not be
				// used with the Depth header on a LOCK method".
				return http.StatusBadRequest, errInvalidDepth
			}
		}
		reqPath, status, err := h.stripPrefix(r.URL.Path)
		if err != nil {
			return status, err
		}
		reqPath, err = user.JoinPath(reqPath)
		if err != nil {
			return http.StatusForbidden, err
		}
		meta, err := op.GetNearestMeta(reqPath)
		if err != nil && !errors.Is(errors.Cause(err), errs.MetaNotFound) {
			return http.StatusInternalServerError, err
		}
		if !common.CanWrite(user, meta, reqPath) {
			return http.StatusForbidden, errs.PermissionDenied
		}
		ld = LockDetails{
			Root:      reqPath,
			Duration:  duration,
			OwnerXML:  li.Owner.InnerXML,
			ZeroDepth: depth == 0,
		}
		token, err = h.LockSystem.Create(now, ld)
		if err != nil {
			if err == ErrLocked {
				return StatusLocked, err
			}
			return http.StatusInternalServerError, err
		}
		defer func() {
			if retErr != nil {
				h.LockSystem.Unlock(now, token)
			}
		}()

		// ??? Why create resource here?
		//// Create the resource if it didn't previously exist.
		//if _, err := h.FileSystem.Stat(ctx, reqPath); err != nil {
		//	f, err := h.FileSystem.OpenFile(ctx, reqPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
		//	if err != nil {
		//		// TODO: detect missing intermediate dirs and return http.StatusConflict?
		//		return http.StatusInternalServerError, err
		//	}
		//	f.Close()
		//	created = true
		//}

		// http://www.webdav.org/specs/rfc4918.html#HEADER_Lock-Token says that the
		// Lock-Token value is a Coded-URL. We add angle brackets.
		w.Header().Set("Lock-Token", "<"+token+">")
	}

	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	if created {
		// This is "w.WriteHeader(http.StatusCreated)" and not "return
		// http.StatusCreated, nil" because we write our own (XML) response to w
		// and Handler.ServeHTTP would otherwise write "Created".
		w.WriteHeader(http.StatusCreated)
	}
	writeLockInfo(w, token, ld)
	return 0, nil
}

func (h *Handler) handleUnlock(w http.ResponseWriter, r *http.Request) (status int, err error) {
	// http://www.webdav.org/specs/rfc4918.html#HEADER_Lock-Token says that the
	// Lock-Token value is a Coded-URL. We strip its angle brackets.
	t := r.Header.Get("Lock-Token")
	if len(t) < 2 || t[0] != '<' || t[len(t)-1] != '>' {
		return http.StatusBadRequest, errInvalidLockToken
	}
	t = t[1 : len(t)-1]

	reqPath, status, err := h.stripPrefix(r.URL.Path)
	if err != nil {
		return status, err
	}
	ctx := r.Context()
	user := ctx.Value(conf.UserKey).(*model.User)
	reqPath, err = user.JoinPath(reqPath)
	if err != nil {
		return http.StatusForbidden, err
	}
	meta, err := op.GetNearestMeta(reqPath)
	if err != nil && !errors.Is(errors.Cause(err), errs.MetaNotFound) {
		return http.StatusInternalServerError, err
	}
	if !common.CanWrite(user, meta, reqPath) {
		return http.StatusForbidden, errs.PermissionDenied
	}

	switch err = h.LockSystem.Unlock(time.Now(), t); err {
	case nil:
		return http.StatusNoContent, err
	case ErrForbidden:
		return http.StatusForbidden, err
	case ErrLocked:
		return StatusLocked, err
	case ErrNoSuchLock:
		return http.StatusConflict, err
	default:
		return http.StatusInternalServerError, err
	}
}

func (h *Handler) handlePropfind(w http.ResponseWriter, r *http.Request) (status int, err error) {
	reqPath, status, err := h.stripPrefix(r.URL.Path)
	if err != nil {
		return status, err
	}
	ctx := r.Context()
	userAgent := r.Header.Get("User-Agent")
	ctx = context.WithValue(ctx, conf.UserAgentKey, userAgent)
	user := ctx.Value(conf.UserKey).(*model.User)
	password, _ := ctx.Value(conf.MetaPassKey).(string)
	reqPath, err = user.JoinPath(reqPath)
	if err != nil {
		return http.StatusForbidden, err
	}
	meta, err := op.GetNearestMeta(reqPath)
	if err != nil && !errors.Is(errors.Cause(err), errs.MetaNotFound) {
		return http.StatusInternalServerError, err
	}
	if !common.CanAccess(user, meta, reqPath, password) {
		return http.StatusForbidden, errs.PermissionDenied
	}
	if writeback.Enabled() {
		missing, wbErr := writeback.ReconcileDirect(ctx, reqPath)
		if wbErr != nil {
			return http.StatusInternalServerError, wbErr
		}
		if missing {
			return http.StatusNotFound, errs.ObjectNotFound
		}
	}
	fi, found, deleted, wbErr := writeback.Canonical(reqPath)
	if wbErr != nil {
		return http.StatusInternalServerError, wbErr
	}
	if deleted {
		return http.StatusNotFound, errs.ObjectNotFound
	}
	if !found {
		fi, err = fs.Get(ctx, reqPath, &fs.GetArgs{})
		if err != nil {
			if errs.IsNotFoundError(err) {
				return http.StatusNotFound, err
			}
			return http.StatusMethodNotAllowed, err
		}
	}
	depth := infiniteDepth
	if hdr := r.Header.Get("Depth"); hdr != "" {
		depth = parseDepth(hdr)
		if depth == invalidDepth {
			return http.StatusBadRequest, errInvalidDepth
		}
	}
	pf, status, err := readPropfind(r.Body)
	if err != nil {
		return status, err
	}

	mw := multistatusWriter{w: w}

	walkFn := func(reqPath string, info model.Obj, err error) error {
		if err != nil {
			return err
		}
		var pstats []Propstat
		if pf.Propname != nil {
			pnames, err := propnames(ctx, h.LockSystem, info)
			if err != nil {
				return err
			}
			pstat := Propstat{Status: http.StatusOK}
			for _, xmlname := range pnames {
				pstat.Props = append(pstat.Props, Property{XMLName: xmlname})
			}
			pstats = append(pstats, pstat)
		} else if pf.Allprop != nil {
			pstats, err = allprop(ctx, h.LockSystem, info, pf.Prop)
		} else {
			pstats, err = props(ctx, h.LockSystem, info, pf.Prop)
		}
		if err != nil {
			return err
		}
		href := path.Join(h.Prefix, strings.TrimPrefix(reqPath, user.BasePath))
		if href != "/" && info.IsDir() {
			href += "/"
		}
		return mw.write(makePropstatResponse(href, pstats))
	}

	walkErr := walkFS(ctx, depth, reqPath, fi, walkFn)
	closeErr := mw.close()
	if walkErr != nil {
		return http.StatusInternalServerError, walkErr
	}
	if closeErr != nil {
		return http.StatusInternalServerError, closeErr
	}
	return 0, nil
}

func (h *Handler) handleProppatch(w http.ResponseWriter, r *http.Request) (status int, err error) {
	reqPath, status, err := h.stripPrefix(r.URL.Path)
	if err != nil {
		return status, err
	}
	release, status, err := h.confirmLocks(r, reqPath, "")
	if err != nil {
		return status, err
	}
	defer release()

	ctx := r.Context()
	user := ctx.Value(conf.UserKey).(*model.User)
	reqPath, err = user.JoinPath(reqPath)
	if err != nil {
		return http.StatusForbidden, err
	}
	meta, err := op.GetNearestMeta(reqPath)
	if err != nil && !errors.Is(errors.Cause(err), errs.MetaNotFound) {
		return http.StatusInternalServerError, err
	}
	if !common.CanWrite(user, meta, reqPath) {
		return http.StatusForbidden, errs.PermissionDenied
	}
	if writeback.Enabled() {
		_, found, deleted, wbErr := writeback.Canonical(reqPath)
		if wbErr != nil {
			return http.StatusInternalServerError, wbErr
		}
		if deleted {
			return http.StatusNotFound, errs.ObjectNotFound
		}
		if !found {
			if _, getErr := fs.Get(ctx, reqPath, &fs.GetArgs{}); getErr != nil {
				if errs.IsObjectNotFound(getErr) {
					return http.StatusNotFound, getErr
				}
				return http.StatusMethodNotAllowed, getErr
			}
		}
	} else if _, getErr := fs.Get(ctx, reqPath, &fs.GetArgs{}); getErr != nil {
		if errs.IsObjectNotFound(getErr) {
			return http.StatusNotFound, getErr
		}
		return http.StatusMethodNotAllowed, getErr
	}
	patches, status, err := readProppatch(r.Body)
	if err != nil {
		return status, err
	}
	pstats, err := patch(ctx, h.LockSystem, reqPath, patches)
	if err != nil {
		return http.StatusInternalServerError, err
	}
	mw := multistatusWriter{w: w}
	writeErr := mw.write(makePropstatResponse(r.URL.Path, pstats))
	closeErr := mw.close()
	if writeErr != nil {
		return http.StatusInternalServerError, writeErr
	}
	if closeErr != nil {
		return http.StatusInternalServerError, closeErr
	}
	return 0, nil
}

func makePropstatResponse(href string, pstats []Propstat) *response {
	resp := response{
		Href:     []string{(&url.URL{Path: href}).EscapedPath()},
		Propstat: make([]propstat, 0, len(pstats)),
	}
	for _, p := range pstats {
		var xmlErr *xmlError
		if p.XMLError != "" {
			xmlErr = &xmlError{InnerXML: []byte(p.XMLError)}
		}
		resp.Propstat = append(resp.Propstat, propstat{
			Status:              fmt.Sprintf("HTTP/1.1 %d %s", p.Status, StatusText(p.Status)),
			Prop:                p.Props,
			ResponseDescription: p.ResponseDescription,
			Error:               xmlErr,
		})
	}
	return &resp
}

const (
	infiniteDepth = -1
	invalidDepth  = -2
)

// parseDepth maps the strings "0", "1" and "infinity" to 0, 1 and
// infiniteDepth. Parsing any other string returns invalidDepth.
//
// Different WebDAV methods have further constraints on valid depths:
//   - PROPFIND has no further restrictions, as per section 9.1.
//   - COPY accepts only "0" or "infinity", as per section 9.8.3.
//   - MOVE accepts only "infinity", as per section 9.9.2.
//   - LOCK accepts only "0" or "infinity", as per section 9.10.3.
//
// These constraints are enforced by the handleXxx methods.
func parseDepth(s string) int {
	switch s {
	case "0":
		return 0
	case "1":
		return 1
	case "infinity":
		return infiniteDepth
	}
	return invalidDepth
}

// http://www.webdav.org/specs/rfc4918.html#status.code.extensions.to.http11
const (
	StatusMulti               = 207
	StatusUnprocessableEntity = 422
	StatusLocked              = 423
	StatusFailedDependency    = 424
	StatusInsufficientStorage = 507
)

func StatusText(code int) string {
	switch code {
	case StatusMulti:
		return "Multi-Status"
	case StatusUnprocessableEntity:
		return "Unprocessable Entity"
	case StatusLocked:
		return "Locked"
	case StatusFailedDependency:
		return "Failed Dependency"
	case StatusInsufficientStorage:
		return "Insufficient Storage"
	}
	return http.StatusText(code)
}

var (
	errDestinationEqualsSource = errors.New("webdav: destination equals source")
	errDirectoryNotEmpty       = errors.New("webdav: directory not empty")
	errInvalidDepth            = errors.New("webdav: invalid depth")
	errInvalidDestination      = errors.New("webdav: invalid destination")
	errInvalidIfHeader         = errors.New("webdav: invalid If header")
	errInvalidLockInfo         = errors.New("webdav: invalid lock info")
	errInvalidLockToken        = errors.New("webdav: invalid lock token")
	errInvalidPropfind         = errors.New("webdav: invalid propfind")
	errInvalidProppatch        = errors.New("webdav: invalid proppatch")
	errInvalidResponse         = errors.New("webdav: invalid response")
	errInvalidTimeout          = errors.New("webdav: invalid timeout")
	errNoFileSystem            = errors.New("webdav: no file system")
	errNoLockSystem            = errors.New("webdav: no lock system")
	errNotADirectory           = errors.New("webdav: not a directory")
	errPrefixMismatch          = errors.New("webdav: prefix mismatch")
	errRecursionTooDeep        = errors.New("webdav: recursion too deep")
	errUnsupportedLockInfo     = errors.New("webdav: unsupported lock info")
	errUnsupportedMethod       = errors.New("webdav: unsupported method")
)
