package webdav

import (
	"net/http/httptest"
	"testing"
	"time"
)

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
