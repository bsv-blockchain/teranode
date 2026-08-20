package settings

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestNewSettingsTrimsAdminAPIKey guards the trim-at-load contract: the servers
// treat GRPCAdminAPIKey == "" as "generate a random key", so a whitespace-only
// or padded value read from the environment (gocore does not trim env values)
// must be trimmed once at load rather than becoming a live secret in some
// readers and empty in others.
func TestNewSettingsTrimsAdminAPIKey(t *testing.T) {
	tests := []struct {
		name string
		env  string
		want string
	}{
		{name: "padded value is trimmed", env: "  padded-admin-secret  ", want: "padded-admin-secret"},
		{name: "whitespace-only resolves to empty", env: "   ", want: ""},
		{name: "tabs and spaces resolve to empty", env: "\t \n", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// gocore reads this exact key from the environment ahead of the config files.
			t.Setenv("grpc_admin_api_key", tt.env)

			s := NewSettings()
			require.Equal(t, tt.want, s.GRPCAdminAPIKey)
		})
	}
}
