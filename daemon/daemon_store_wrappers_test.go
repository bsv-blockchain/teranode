package daemon

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/test"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

type wrappedBlockchainStore struct{ blockchainstore.Store }

type wrappedBlobStore struct{ blob.Store }

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

// TestStoreWrappers_WrapsBlobStoresOnce checks the tx, subtree and block store
// wrappers the same way TestStoreWrappers checks the blockchain one. Their
// getters need a running blockchain service, so this starts a test daemon.
func TestStoreWrappers_WrapsBlobStoresOnce(t *testing.T) {
	wraps := map[string]int{}

	wrap := func(name string) func(blob.Store) blob.Store {
		return func(s blob.Store) blob.Store {
			wraps[name]++
			return &wrappedBlobStore{s}
		}
	}

	td := NewTestDaemon(t, TestOptions{
		SettingsOverrideFunc: test.ComposeSettings(test.SystemTestSettings()),
		StoreWrappers: StoreWrappers{
			TxStore:      wrap("tx"),
			SubtreeStore: wrap("subtree"),
			BlockStore:   wrap("block"),
		},
	})
	defer td.Stop(t)

	stores := td.d.daemonStores
	getters := map[string]func() (blob.Store, error){
		"tx": func() (blob.Store, error) { return stores.GetTxStore(td.Ctx, td.Logger, td.Settings) },
		"subtree": func() (blob.Store, error) {
			return stores.GetSubtreeStore(td.Ctx, td.Logger, td.Settings)
		},
		"block": func() (blob.Store, error) { return stores.GetBlockStore(td.Ctx, td.Logger, td.Settings) },
	}

	for name, get := range getters {
		store, err := get()
		require.NoError(t, err, name)

		_, ok := store.(*wrappedBlobStore)
		require.True(t, ok, "the %s getter must return the wrapped store", name)

		// The getter returns the cached wrapped store without wrapping it again.
		again, err := get()
		require.NoError(t, err, name)
		require.Same(t, store, again, name)
		require.Equal(t, 1, wraps[name], "the %s store must be wrapped once", name)
	}

	require.Same(t, td.SubtreeStore, stores.mainSubtreeStore, "the test daemon must hold the wrapped subtree store")
}
