package aerospike_test

import (
	"testing"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestCloseClient_ClosesAndEvictsTheStoresClient checks that CloseClient, which
// the test daemon's crash uses in place of Close, closes the store's Aerospike
// client and removes it from the per-host cache, so a store built afterwards
// for the same host, as a restarted daemon's is, gets a new connected client.
func TestCloseClient_ClosesAndEvictsTheStoresClient(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Aerospike integration test in short mode")
	}

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	_, store, _, cleanup, storeURL := initAerospikeWithURL(t, tSettings, logger, nil)
	defer cleanup()

	client := store.GetClient()
	require.True(t, client.IsConnected())

	store.CloseClient()

	require.False(t, client.IsConnected(), "CloseClient must close the client")

	fresh, err := util.GetAerospikeClient(logger, storeURL, tSettings)
	require.NoError(t, err)

	defer util.CloseAerospikeClient(storeURL.Host)

	require.NotSame(t, client, fresh, "CloseClient must evict the closed client from the cache")
	require.True(t, fresh.IsConnected())
}
