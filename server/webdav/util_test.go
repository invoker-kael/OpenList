package webdav

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestWebDAVRebuildAllowsOnlyReadMethods(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodHead, "PROPFIND", http.MethodOptions} {
		if !webDAVMethodAllowedDuringWritebackRebuild(method) {
			t.Errorf("%s should remain available during rebuild", method)
		}
	}
	for _, method := range []string{
		http.MethodPut, http.MethodPost, http.MethodDelete, "MKCOL", "COPY", "MOVE", "LOCK", "UNLOCK", "PROPPATCH", "PATCH",
	} {
		if webDAVMethodAllowedDuringWritebackRebuild(method) {
			t.Errorf("%s should be rejected during rebuild", method)
		}
	}
}

func TestWebDAVMutationGetsRetryableResponseDuringRebuild(t *testing.T) {
	req := httptest.NewRequest(http.MethodPut, "http://example.test/file.bin", nil)
	res := httptest.NewRecorder()
	if !rejectWebDAVMutationDuringWritebackRebuild(res, req) {
		t.Fatal("PUT should be rejected during rebuild")
	}
	if res.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, want 503", res.Code)
	}
	if res.Header().Get("Retry-After") == "" {
		t.Fatal("503 response is missing Retry-After")
	}
}

func TestGetWritebackHeaderTimeRequiresExplicitHeader(t *testing.T) {
	h := &Handler{}
	req := httptest.NewRequest("PUT", "http://example.test/file.bin", nil)

	if got := h.getWritebackHeaderTime(req, "X-OC-Mtime"); !got.IsZero() {
		t.Fatalf("missing X-OC-Mtime = %v, want zero", got)
	}

	const unixTime = int64(1789891200)
	req.Header.Set("X-OC-Mtime", "1789891200")
	if got := h.getWritebackHeaderTime(req, "X-OC-Mtime"); !got.Equal(time.Unix(unixTime, 0)) {
		t.Fatalf("X-OC-Mtime = %v, want %v", got, time.Unix(unixTime, 0))
	}
}

func TestGetWritebackHeaderTimeDoesNotFallbackOrInventTime(t *testing.T) {
	h := &Handler{}
	req := httptest.NewRequest("PUT", "http://example.test/file.bin", nil)
	req.Header.Set("X-OC-Mtime", "1789891200")

	if got := h.getWritebackHeaderTime(req, "X-OC-Ctime"); !got.IsZero() {
		t.Fatalf("missing X-OC-Ctime = %v, want zero even when X-OC-Mtime is present", got)
	}

	req.Header.Set("X-OC-Ctime", "not-a-unix-time")
	if got := h.getWritebackHeaderTime(req, "X-OC-Ctime"); !got.IsZero() {
		t.Fatalf("invalid X-OC-Ctime = %v, want zero", got)
	}
}
