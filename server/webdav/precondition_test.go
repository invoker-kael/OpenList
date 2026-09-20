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

