package writeback

import (
	"strings"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestPathKeyIsStableAndMySQLIndexSafe(t *testing.T) {
	key1 := pathKey("/encrypted/very/long/path/file.bin")
	key2 := pathKey("/encrypted/very/long/path/file.bin")
	if key1 != key2 {
		t.Fatal("path key is not stable")
	}
	if len(key1) != 64 {
		t.Fatalf("path key length = %d, want 64", len(key1))
	}
}

func TestCanonicalETagChangesWithGeneration(t *testing.T) {
	key := pathKey("/encrypted/file.bin")
	a := canonicalETag(key, 1, 1024)
	b := canonicalETag(key, 2, 1024)
	if a == b {
		t.Fatal("etag must change when generation changes")
	}
	if !strings.HasPrefix(a, "\"olwb-") {
		t.Fatalf("unexpected etag %q", a)
	}
}


func TestShouldRemoveStaleRemote(t *testing.T) {
	tests := []struct {
		name     string
		uploaded string
		current  *model.WebDAVWritebackObject
		want     bool
	}{
		{
			name:     "newer generation same path is kept",
			uploaded: "/encrypted/file.bin",
			current: &model.WebDAVWritebackObject{
				Path:       "/encrypted/file.bin",
				Generation: 2,
				State:      StateQueued,
			},
			want: false,
		},
		{
			name:     "deleted path is removed",
			uploaded: "/encrypted/file.bin",
			current: &model.WebDAVWritebackObject{
				Path:  "/encrypted/file.bin",
				State: StateDeleted,
			},
			want: true,
		},
		{
			name:     "moved path cleans old remote",
			uploaded: "/encrypted/.tmp-file",
			current: &model.WebDAVWritebackObject{
				Path:  "/encrypted/file.bin",
				State: StateQueued,
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldRemoveStaleRemote(tt.uploaded, tt.current); got != tt.want {
				t.Fatalf("shouldRemoveStaleRemote() = %v, want %v", got, tt.want)
			}
		})
	}
}


func TestShouldDropCanonicalAfterRemoteList(t *testing.T) {
	completedNoSpool := &model.WebDAVWritebackObject{
		State:     StateCompleted,
		SpoolPath: "",
	}
	if !shouldDropCanonicalAfterRemoteList(completedNoSpool, true, false) {
		t.Fatal("missing remote object should be surfaced after a reliable listing")
	}
	if shouldDropCanonicalAfterRemoteList(completedNoSpool, false, false) {
		t.Fatal("provider/listing failure must not drop canonical metadata")
	}
	if shouldDropCanonicalAfterRemoteList(completedNoSpool, true, true) {
		t.Fatal("present remote object must keep canonical metadata")
	}

	completedCached := &model.WebDAVWritebackObject{
		State:     StateCompleted,
		SpoolPath: "/spool/object.data",
	}
	if shouldDropCanonicalAfterRemoteList(completedCached, true, false) {
		t.Fatal("locally cached completed object must remain authoritative")
	}

	queued := &model.WebDAVWritebackObject{
		State:     StateQueued,
		SpoolPath: "/spool/object.data",
	}
	if shouldDropCanonicalAfterRemoteList(queued, true, false) {
		t.Fatal("pending upload must remain visible even before provider listing catches up")
	}
}


func TestReceivingPathReferenceCount(t *testing.T) {
	p := "/encrypted/placeholder.bin"
	release1 := beginReceiving(p)
	release2 := beginReceiving(p)
	if !isReceiving(p) {
		t.Fatal("path should be marked receiving while PUTs are active")
	}
	release1()
	if !isReceiving(p) {
		t.Fatal("path should stay receiving until all overlapping PUTs finish")
	}
	release2()
	if isReceiving(p) {
		t.Fatal("path should stop receiving after the final PUT finishes")
	}
}
