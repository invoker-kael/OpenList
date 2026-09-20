package webdav

import (
	"net/http"
	"testing"
	"time"
)

func TestPutPreconditions(t *testing.T) {
	tests := []struct {
		name              string
		ifMatch           string
		ifNoneMatch       string
		ifUnmodifiedSince string
		exists            bool
		etag              string
		modTime           time.Time
		failed            bool
	}{
		{name: "no conditions", exists: false, failed: false},
		{name: "if match star existing", ifMatch: "*", exists: true, etag: "\"v1\"", failed: false},
		{name: "if match star missing", ifMatch: "*", exists: false, failed: true},
		{name: "if match exact", ifMatch: "\"v1\"", exists: true, etag: "\"v1\"", failed: false},
		{name: "if match mismatch", ifMatch: "\"v0\"", exists: true, etag: "\"v1\"", failed: true},
		{name: "if match weak does not strong match", ifMatch: "W/\"v1\"", exists: true, etag: "\"v1\"", failed: true},
		{name: "if none match star existing", ifNoneMatch: "*", exists: true, etag: "\"v1\"", failed: true},
		{name: "if none match star missing", ifNoneMatch: "*", exists: false, failed: false},
		{name: "if none match exact", ifNoneMatch: "\"v1\"", exists: true, etag: "\"v1\"", failed: true},
		{name: "if none match weak", ifNoneMatch: "W/\"v1\"", exists: true, etag: "\"v1\"", failed: true},
		{name: "if none match other", ifNoneMatch: "\"v0\"", exists: true, etag: "\"v1\"", failed: false},
		{name: "etag list", ifMatch: "\"v0\", \"v1\"", exists: true, etag: "\"v1\"", failed: false},
		{
			name:              "if unmodified since stale",
			ifUnmodifiedSince: time.Date(2026, time.September, 20, 4, 59, 0, 0, time.UTC).Format(http.TimeFormat),
			exists:            true,
			modTime:           time.Date(2026, time.September, 20, 5, 0, 0, 0, time.UTC),
			failed:            true,
		},
		{
			name:              "if unmodified since current",
			ifUnmodifiedSince: time.Date(2026, time.September, 20, 5, 0, 0, 0, time.UTC).Format(http.TimeFormat),
			exists:            true,
			modTime:           time.Date(2026, time.September, 20, 5, 0, 0, 0, time.UTC),
			failed:            false,
		},
		{
			name:              "if match takes precedence over stale date",
			ifMatch:           "\"v1\"",
			ifUnmodifiedSince: time.Date(2026, time.September, 20, 4, 59, 0, 0, time.UTC).Format(http.TimeFormat),
			exists:            true,
			etag:              "\"v1\"",
			modTime:           time.Date(2026, time.September, 20, 5, 0, 0, 0, time.UTC),
			failed:            false,
		},
		{
			name:              "malformed date ignored",
			ifUnmodifiedSince: "not-a-date",
			exists:            true,
			modTime:           time.Date(2026, time.September, 20, 5, 0, 0, 0, time.UTC),
			failed:            false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := putPreconditionFailed(tt.ifMatch, tt.ifNoneMatch, tt.ifUnmodifiedSince, tt.exists, tt.etag, tt.modTime); got != tt.failed {
				t.Fatalf("putPreconditionFailed() = %v, want %v", got, tt.failed)
			}
		})
	}
}

func TestCopyCanUseNative(t *testing.T) {
	tests := []struct {
		name  string
		src   string
		dst   string
		depth int
		want  bool
	}{
		{name: "same name cross directory", src: "/a/foo.bin", dst: "/b/foo.bin", depth: infiniteDepth, want: true},
		{name: "renamed cross directory must be exact", src: "/a/foo.bin", dst: "/b/bar.bin", depth: infiniteDepth, want: false},
		{name: "same directory renamed copy must be exact", src: "/a/foo.bin", dst: "/a/bar.bin", depth: infiniteDepth, want: false},
		{name: "depth zero collection copy must be exact", src: "/a/folder", dst: "/b/folder", depth: 0, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := copyCanUseNative(tt.src, tt.dst, tt.depth); got != tt.want {
				t.Fatalf("copyCanUseNative(%q, %q, %d) = %v, want %v", tt.src, tt.dst, tt.depth, got, tt.want)
			}
		})
	}
}

func TestMoveNeedsStaging(t *testing.T) {
	tests := []struct {
		name string
		src  string
		dst  string
		want bool
	}{
		{name: "cross directory rename", src: "/a/foo.bin", dst: "/b/bar.bin", want: true},
		{name: "cross directory same name", src: "/a/foo.bin", dst: "/b/foo.bin", want: false},
		{name: "same directory rename", src: "/a/foo.bin", dst: "/a/bar.bin", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := moveNeedsStaging(tt.src, tt.dst); got != tt.want {
				t.Fatalf("moveNeedsStaging(%q, %q) = %v, want %v", tt.src, tt.dst, got, tt.want)
			}
		})
	}
}

func TestCopyMoveProviderSucceeded(t *testing.T) {
	for _, status := range []int{http.StatusCreated, http.StatusNoContent} {
		if !copyMoveProviderSucceeded(status) {
			t.Fatalf("status %d should permit canonical reconciliation", status)
		}
	}
	for _, status := range []int{
		http.StatusForbidden,
		http.StatusPreconditionFailed,
		http.StatusConflict,
		http.StatusServiceUnavailable,
		http.StatusInternalServerError,
	} {
		if copyMoveProviderSucceeded(status) {
			t.Fatalf("status %d must not mutate canonical COPY/MOVE metadata", status)
		}
	}
}

func TestCanonicalReadPreconditions(t *testing.T) {
	modTime := time.Date(2026, time.September, 20, 5, 0, 0, 0, time.UTC)
	tests := []struct {
		name    string
		method  string
		headers map[string]string
		want    int
	}{
		{name: "no conditions", method: http.MethodHead, want: 0},
		{name: "if match exact", method: http.MethodHead, headers: map[string]string{"If-Match": "\"v2\""}, want: 0},
		{name: "if match mismatch", method: http.MethodHead, headers: map[string]string{"If-Match": "\"v1\""}, want: http.StatusPreconditionFailed},
		{name: "if none match exact", method: http.MethodHead, headers: map[string]string{"If-None-Match": "\"v2\""}, want: http.StatusNotModified},
		{name: "if none match weak", method: http.MethodGet, headers: map[string]string{"If-None-Match": "W/\"v2\""}, want: http.StatusNotModified},
		{name: "if modified since unchanged", method: http.MethodHead, headers: map[string]string{"If-Modified-Since": modTime.Format(http.TimeFormat)}, want: http.StatusNotModified},
		{name: "if modified since older", method: http.MethodGet, headers: map[string]string{"If-Modified-Since": modTime.Add(-time.Minute).Format(http.TimeFormat)}, want: 0},
		{name: "if unmodified since stale", method: http.MethodHead, headers: map[string]string{"If-Unmodified-Since": modTime.Add(-time.Minute).Format(http.TimeFormat)}, want: http.StatusPreconditionFailed},
		{name: "if none match takes precedence over modified since", method: http.MethodHead, headers: map[string]string{"If-None-Match": "\"other\"", "If-Modified-Since": modTime.Add(time.Hour).Format(http.TimeFormat)}, want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest(tt.method, "http://example.test/file.bin", nil)
			if err != nil {
				t.Fatal(err)
			}
			for key, value := range tt.headers {
				req.Header.Set(key, value)
			}
			if got := canonicalReadPreconditionStatus(req, "\"v2\"", modTime); got != tt.want {
				t.Fatalf("canonicalReadPreconditionStatus() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestCanonicalReadRequestSanitizesProviderConditions(t *testing.T) {
	modTime := time.Date(2026, time.September, 20, 5, 0, 0, 0, time.UTC)
	req, err := http.NewRequest(http.MethodGet, "http://example.test/file.bin", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("If-Match", "\"canonical\"")
	req.Header.Set("If-None-Match", "\"other\"")
	req.Header.Set("If-Modified-Since", modTime.Add(-time.Hour).Format(http.TimeFormat))
	req.Header.Set("If-Unmodified-Since", modTime.Add(time.Hour).Format(http.TimeFormat))
	req.Header.Set("Range", "bytes=0-99")
	req.Header.Set("If-Range", "\"canonical\"")

	got := canonicalReadRequest(req, "\"canonical\"", modTime)
	for _, key := range []string{"If-Match", "If-None-Match", "If-Modified-Since", "If-Unmodified-Since", "If-Range"} {
		if value := got.Header.Get(key); value != "" {
			t.Fatalf("%s leaked to provider: %q", key, value)
		}
	}
	if got.Header.Get("Range") != "bytes=0-99" {
		t.Fatalf("matching canonical If-Range should preserve Range, got %q", got.Header.Get("Range"))
	}
	if req.Header.Get("If-Range") == "" {
		t.Fatal("original request must remain unchanged")
	}
}

func TestCanonicalReadRequestDropsRangeOnIfRangeMismatch(t *testing.T) {
	modTime := time.Date(2026, time.September, 20, 5, 0, 0, 0, time.UTC)
	req, err := http.NewRequest(http.MethodGet, "http://example.test/file.bin", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Range", "bytes=0-99")
	req.Header.Set("If-Range", "\"stale\"")

	got := canonicalReadRequest(req, "\"canonical\"", modTime)
	if got.Header.Get("Range") != "" {
		t.Fatalf("mismatched canonical If-Range must downgrade to full response, got %q", got.Header.Get("Range"))
	}
	if got.Header.Get("If-Range") != "" {
		t.Fatalf("If-Range must not be re-evaluated by provider, got %q", got.Header.Get("If-Range"))
	}
}
