package webdav

import "testing"

func TestPutPreconditions(t *testing.T) {
	tests := []struct {
		name        string
		ifMatch     string
		ifNoneMatch string
		exists      bool
		etag        string
		failed      bool
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := putPreconditionFailed(tt.ifMatch, tt.ifNoneMatch, tt.exists, tt.etag); got != tt.failed {
				t.Fatalf("putPreconditionFailed() = %v, want %v", got, tt.failed)
			}
		})
	}
}
