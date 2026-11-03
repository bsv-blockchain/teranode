package settings

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestNewSettingsTrimsHTTPAuthTokens guards the trim-at-load contract for the blob API's
// shared secret: a token read from a secret file typically ends in a newline, and gocore
// does not trim environment values.
func TestNewSettingsTrimsHTTPAuthTokens(t *testing.T) {
	tests := []struct {
		name string
		env  string
		want string
	}{
		{name: "trailing newline from a secret file", env: "blob-secret\n", want: "blob-secret"},
		{name: "padded value", env: "  blob-secret  ", want: "blob-secret"},
		{name: "whitespace-only resolves to empty", env: " \t\n", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// gocore reads these exact keys from the environment ahead of the config files.
			t.Setenv("blob_httpAuthToken", tt.env)
			t.Setenv("blockpersister_httpAuthToken", tt.env)

			s := NewSettings()
			require.Equal(t, tt.want, s.BlobHTTPAuthToken)
			require.Equal(t, tt.want, s.BlockPersister.HTTPAuthToken)
		})
	}
}
