package aerospike_test

import (
	"testing"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestCloseClient_ClosesTheStoresClient checks that CloseClient, which the test
// daemon's crash uses in place of Close, closes the store's Aerospike client.
func TestCloseClient_ClosesTheStoresClient(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Aerospike integration test in short mode")
	}

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	_, store, _, cleanup := initAerospike(t, tSettings, logger)
	defer cleanup()

	client := store.GetClient()
	require.True(t, client.IsConnected())

	store.CloseClient()

	require.False(t, client.IsConnected(), "CloseClient must close the client")
}
