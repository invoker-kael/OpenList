package handles

import (
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestIsValidNewUserRole(t *testing.T) {
	tests := []struct {
		name string
		role int
		want bool
	}{
		{name: "general", role: model.GENERAL, want: true},
		{name: "guest", role: model.GUEST, want: false},
		{name: "admin", role: model.ADMIN, want: true},
		{name: "unknown", role: model.ADMIN + 1, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isValidNewUserRole(tt.role); got != tt.want {
				t.Errorf("isValidNewUserRole(%d) = %t, want %t", tt.role, got, tt.want)
			}
		})
	}
}
