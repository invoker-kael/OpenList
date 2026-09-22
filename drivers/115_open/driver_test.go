package _115_open

import (
	"strings"
	"testing"
)

func TestParseSignCheckRange(t *testing.T) {
	tests := []struct {
		name       string
		signCheck  string
		fileSize   int64
		wantStart  int64
		wantLength int64
		wantErr    string
	}{
		{name: "valid", signCheck: "10-19", fileSize: 20, wantStart: 10, wantLength: 10},
		{name: "single byte", signCheck: "0-0", fileSize: 1, wantStart: 0, wantLength: 1},
		{name: "missing separator", signCheck: "10", fileSize: 20, wantErr: "invalid sign_check"},
		{name: "too many separators", signCheck: "1-2-3", fileSize: 20, wantErr: "invalid sign_check"},
		{name: "negative start", signCheck: "-1-2", fileSize: 20, wantErr: "invalid sign_check"},
		{name: "reversed", signCheck: "10-9", fileSize: 20, wantErr: "outside file size"},
		{name: "start at eof", signCheck: "20-20", fileSize: 20, wantErr: "outside file size"},
		{name: "end past eof", signCheck: "10-20", fileSize: 20, wantErr: "outside file size"},
		{name: "zero sized file", signCheck: "0-0", fileSize: 0, wantErr: "outside file size"},
		{name: "unknown file size", signCheck: "0-0", fileSize: -1, wantErr: "invalid file size"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, length, err := parseSignCheckRange(tt.signCheck, tt.fileSize)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("parseSignCheckRange(%q, %d) error=%v, want substring %q", tt.signCheck, tt.fileSize, err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseSignCheckRange(%q, %d) unexpected error: %v", tt.signCheck, tt.fileSize, err)
			}
			if start != tt.wantStart || length != tt.wantLength {
				t.Fatalf("parseSignCheckRange(%q, %d)=(%d,%d), want (%d,%d)", tt.signCheck, tt.fileSize, start, length, tt.wantStart, tt.wantLength)
			}
		})
	}
}
