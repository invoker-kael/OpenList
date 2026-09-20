package common

import (
	"net/http"
	"testing"
)

func TestCanonicalProxyHeadersRestoreProviderOverrides(t *testing.T) {
	canonical := make(http.Header)
	canonical.Set("Etag", "\"canonical\"")
	canonical.Set("Last-Modified", "Sun, 20 Sep 2026 05:00:00 GMT")
	canonical.Set("Content-Type", "application/octet-stream")
	canonical.Set("Content-Length", "123456")

	saved := snapshotCanonicalProxyHeaders(canonical)
	provider := make(http.Header)
	provider.Set("Etag", "\"provider\"")
	provider.Set("Last-Modified", "Sun, 20 Sep 2026 06:00:00 GMT")
	provider.Set("Content-Type", "text/plain")
	provider.Set("Content-Length", "42")
	provider.Set("Accept-Ranges", "bytes")

	saved.restore(provider)

	checks := map[string]string{
		"Etag":           "\"canonical\"",
		"Last-Modified":  "Sun, 20 Sep 2026 05:00:00 GMT",
		"Content-Type":   "application/octet-stream",
		"Content-Length": "123456",
	}
	for key, want := range checks {
		if got := provider.Get(key); got != want {
			t.Fatalf("%s = %q, want %q", key, got, want)
		}
	}
	if got := provider.Get("Accept-Ranges"); got != "bytes" {
		t.Fatalf("provider-only header should be retained, got %q", got)
	}
}

func TestCanonicalProxyHeadersDoNotInventMissingValues(t *testing.T) {
	saved := snapshotCanonicalProxyHeaders(make(http.Header))
	provider := make(http.Header)
	provider.Set("Etag", "\"provider\"")
	provider.Set("Content-Length", "42")

	saved.restore(provider)

	if got := provider.Get("Etag"); got != "\"provider\"" {
		t.Fatalf("provider ETag unexpectedly replaced: %q", got)
	}
	if got := provider.Get("Content-Length"); got != "42" {
		t.Fatalf("provider Content-Length unexpectedly replaced: %q", got)
	}
}
