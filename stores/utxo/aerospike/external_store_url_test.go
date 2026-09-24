package aerospike

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestParseExternalStoreURLNeverEchoesPassword pins the error returned for an
// externalStore value that url.Parse refuses. The error travels back through
// the UTXO store factory to the service start path, where it is logged. A
// space in the host is enough to make url.Parse refuse the value.
func TestParseExternalStoreURLNeverEchoesPassword(t *testing.T) {
	const password = "canary-external-store-password"

	_, err := parseExternalStoreURL("http://teranode:" + password + "@blob server:8080/external")
	require.Error(t, err)
	require.NotContains(t, err.Error(), password, "the externalStore parse error echoes the blob store password")
	require.Contains(t, err.Error(), "invalid externalStore URL in utxostore")

	// A raw "/" in the password ends the authority there, and url.Parse's own
	// reason quotes everything before it as an invalid port. The full password
	// is never in that reason, so the assertion is on the part that used to be.
	_, err = parseExternalStoreURL("http://teranode:canary-external/store-password@blobserver:8080/external")
	require.Error(t, err)
	require.NotContains(t, err.Error(), "canary-external", "the externalStore parse error quotes the start of the password")
	require.Contains(t, err.Error(), "invalid externalStore URL in utxostore")

	u, err := parseExternalStoreURL("http://teranode:secret@blobserver:8080/external")
	require.NoError(t, err)
	require.Equal(t, "blobserver:8080", u.Host)

	pw, ok := u.User.Password()
	require.True(t, ok)
	require.Equal(t, "secret", pw, "a valid URL must keep its credential for the blob store to use")
}
