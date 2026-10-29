package daemon

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

type wrappedBlockchainStore struct{ blockchainstore.Store }

// TestStoreWrappers checks that WithStoreWrappers wraps the store a getter hands
// out, once, while the daemon keeps the unwrapped store for crash. The UTXO
// getter needs a running blockchain service; the e2e test
// TestTestDaemonStoreWrappers covers it.
func TestStoreWrappers(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}

	appSettings := settings.NewSettings()

	var err error

	appSettings.BlockChain.StoreURL, err = url.Parse("sqlitememory:///store_wrappers_blockchain")
	require.NoError(t, err)

	wraps := 0

	d := New(WithStoreWrappers(StoreWrappers{
		BlockchainStore: func(s blockchainstore.Store) blockchainstore.Store {
			wraps++
			return &wrappedBlockchainStore{s}
		},
	}))
	defer d.daemonStores.Cleanup()

	store, err := d.daemonStores.GetBlockchainStore(ctx, logger, appSettings)
	require.NoError(t, err)
	defer func() { _ = store.Close(ctx) }()

	wrapped, ok := store.(*wrappedBlockchainStore)
	require.True(t, ok, "the getter must return the wrapped store")
	require.Same(t, wrapped.Store, d.daemonStores.rawBlockchainStore)

	// The getter returns the cached wrapped store without wrapping it again.
	again, err := d.daemonStores.GetBlockchainStore(ctx, logger, appSettings)
	require.NoError(t, err)
	require.Same(t, store, again)
	require.Equal(t, 1, wraps)
}
