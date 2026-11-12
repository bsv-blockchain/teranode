package prereq

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCheckChainNeverStoresStorePassword pins the redaction at the point
// CheckChain builds its result. cmd/teranodedev prints result.StoreURL to
// stdout, and the blockchain store URL is a postgres URL with the password in
// it on our deployments. The redaction lands in a struct field, which the
// logging-call guard in pkg/urlutil cannot see, so only this test holds it.
//
// The aerospike scheme reaches CheckChain's default branch, so no database
// connection is attempted.
func TestCheckChainNeverStoresStorePassword(t *testing.T) {
	const password = "canary-checkchain-password"

	storeURL, err := url.Parse("aerospike://teranode:" + password + "@chain.example:3000/blockchain")
	require.NoError(t, err)

	result := CheckChain("regtest", storeURL, t.TempDir())
	require.NotEmpty(t, result.StoreURL, "CheckChain no longer records the store URL, so this test proves nothing")

	require.NotContains(t, result.StoreURL, password, "the chain check result carries the store password")
	require.Contains(t, result.StoreURL, "chain.example:3000", "the redacted URL lost its host")
	require.Contains(t, result.StoreURL, "teranode:xxxxx@", "the username should survive redaction")

	require.Contains(t, storeURL.String(), password, "CheckChain must not mutate the caller's URL")
}
