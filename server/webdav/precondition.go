package webdav

import (
	"net/http"
	"strings"
	"time"
)

// putPreconditionFailed implements the entity-tag conditions relevant to PUT.
// If-Match uses strong comparison; If-None-Match uses weak comparison.
func putPreconditionFailed(ifMatch, ifNoneMatch string, exists bool, currentETag string) bool {
	if ifMatch != "" {
		if !exists {
			return true
		}
		if strings.TrimSpace(ifMatch) != "*" && !etagListMatches(ifMatch, currentETag, false) {
			return true
		}
	}

	if ifNoneMatch != "" && exists {
		if strings.TrimSpace(ifNoneMatch) == "*" || etagListMatches(ifNoneMatch, currentETag, true) {
			return true
		}
	}

	return false
}

func etagListMatches(header, current string, weak bool) bool {
	current = strings.TrimSpace(current)
	if current == "" {
		return false
	}
	for _, candidate := range strings.Split(header, ",") {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" || candidate == "*" {
			continue
		}
		if weak {
			if weakETag(candidate) == weakETag(current) {
				return true
			}
			continue
		}
		// If-Match requires strong comparison. A weak validator never matches.
		if strings.HasPrefix(candidate, "W/") || strings.HasPrefix(current, "W/") {
			continue
		}
		if candidate == current {
			return true
		}
	}
	return false
}

func weakETag(tag string) string {
	tag = strings.TrimSpace(tag)
	if strings.HasPrefix(tag, "W/") {
		tag = strings.TrimSpace(strings.TrimPrefix(tag, "W/"))
	}
	return tag
}

func canonicalReadPreconditionStatus(r *http.Request, currentETag string, modTime time.Time) int {
	if r == nil || (r.Method != http.MethodGet && r.Method != http.MethodHead) {
		return 0
	}

	ifMatch := r.Header.Get("If-Match")
	if ifMatch != "" {
		if strings.TrimSpace(ifMatch) != "*" && !etagListMatches(ifMatch, currentETag, false) {
			return http.StatusPreconditionFailed
		}
	} else if ifUnmodifiedSince := r.Header.Get("If-Unmodified-Since"); ifUnmodifiedSince != "" && !modTime.IsZero() {
		if t, err := http.ParseTime(ifUnmodifiedSince); err == nil && modTime.UTC().Truncate(time.Second).After(t.UTC()) {
			return http.StatusPreconditionFailed
		}
	}

	ifNoneMatch := r.Header.Get("If-None-Match")
	if ifNoneMatch != "" {
		if strings.TrimSpace(ifNoneMatch) == "*" || etagListMatches(ifNoneMatch, currentETag, true) {
			return http.StatusNotModified
		}
	} else if ifModifiedSince := r.Header.Get("If-Modified-Since"); ifModifiedSince != "" && !modTime.IsZero() {
		if t, err := http.ParseTime(ifModifiedSince); err == nil && !modTime.UTC().Truncate(time.Second).After(t.UTC()) {
			return http.StatusNotModified
		}
	}

	return 0
}

func canonicalReadRequest(r *http.Request, currentETag string, modTime time.Time) *http.Request {
	if r == nil {
		return nil
	}
	clone := r.Clone(r.Context())
	clone.Header = r.Header.Clone()

	clone.Header.Del("If-Match")
	clone.Header.Del("If-None-Match")
	clone.Header.Del("If-Modified-Since")
	clone.Header.Del("If-Unmodified-Since")

	ifRange := strings.TrimSpace(clone.Header.Get("If-Range"))
	if ifRange == "" || clone.Header.Get("Range") == "" {
		return clone
	}

	matched := false
	if t, err := http.ParseTime(ifRange); err == nil {
		matched = !modTime.IsZero() && !modTime.UTC().Truncate(time.Second).After(t.UTC())
	} else {
		matched = etagListMatches(ifRange, currentETag, false)
	}
	if !matched {
		clone.Header.Del("Range")
	}
	clone.Header.Del("If-Range")
	return clone
}
