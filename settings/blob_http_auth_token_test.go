package settings

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestNewSettingsReadsBlobHTTPAuthToken pins that the HTTP blob client token is a Settings
// field, so stores the daemon builds get it resolved through the settings context rather than
// from a bare process-wide lookup.
func TestNewSettingsReadsBlobHTTPAuthToken(t *testing.T) {
	// gocore reads this exact key from the environment ahead of the config files.
	t.Setenv("blob_httpAuthToken", "x")

	s := NewSettings()
	require.Equal(t, "x", s.BlobHTTPAuthToken)
}
