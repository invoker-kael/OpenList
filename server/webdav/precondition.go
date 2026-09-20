package webdav

import "strings"

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
