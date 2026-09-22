package subtreevalidation

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// newCachedStoreServer builds a Server whose utxoStore is a real TxMetaCache over
// a sqlitememory store (per AGENTS.md: use sqlitememory, don't mock the store),
// pre-loaded with one unmined, non-conflicting tx — the shape the cache is
// willing to populate.
func newCachedStoreServer(t *testing.T) (*Server, *txmetacache.TxMetaCache, chainhash.Hash, context.Context) {
	t.Helper()

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	underlying, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	c, err := txmetacache.NewTxMetaCache(ctx, settings.NewSettings(), logger, underlying, txmetacache.Unallocated)
	require.NoError(t, err)

	cache, ok := c.(*txmetacache.TxMetaCache)
	require.True(t, ok)

	_, _, err = underlying.SpendAndCreate(ctx, tests.Tx, 100, utxo.WithCreateOnly())
	require.NoError(t, err)

	s := settings.NewSettings()
	s.BlockValidation.ProcessTxMetaUsingStoreBatchSize = 1024
	s.BlockValidation.ProcessTxMetaUsingStoreConcurrency = 1
	s.BlockValidation.ProcessTxMetaUsingStoreMissingTxThreshold = 0
	s.BlockValidation.ProcessTxMetaUsingStoreRetries = 0

	server := &Server{logger: logger, utxoStore: cache, settings: s}

	return server, cache, *tests.Tx.TxIDChainHash(), ctx
}

// Baseline: the peer-announced subtree path keeps populating the cache. If this
// stops holding, the skip test below proves nothing.
func TestProcessTxMetaUsingStore_PopulatesCacheByDefault(t *testing.T) {
	server, cache, hash, ctx := newCachedStoreServer(t)

	_, found := cache.GetMetaCached(ctx, hash)
	require.False(t, found, "precondition: not cached yet")

	txHashes := []chainhash.Hash{hash}
	txMetaSlice := make([]metaSliceItem, 1)

	missed, err := server.processTxMetaUsingStore(ctx, txHashes, txMetaSlice, map[uint32]bool{}, true, false, false)
	require.NoError(t, err)
	require.Equal(t, 0, missed)
	require.True(t, txMetaSlice[0].isSet)

	_, found = cache.GetMetaCached(ctx, hash)
	require.True(t, found, "the default path is expected to populate the cache")
}

// Block validation reads a whole block's transactions exactly once and setMined
// evicts them right after, so populating the cache there is pure cost: measured
// on the scaling cluster as 0 cache hits against ~755k insertions/s while
// revalidating a single block, on top of displacing the ingest path's hot set.
// Reading through the underlying store skips the population entirely.
func TestProcessTxMetaUsingStore_SkipsCachePopulationWhenAsked(t *testing.T) {
	server, cache, hash, ctx := newCachedStoreServer(t)

	txHashes := []chainhash.Hash{hash}
	txMetaSlice := make([]metaSliceItem, 1)

	missed, err := server.processTxMetaUsingStore(ctx, txHashes, txMetaSlice, map[uint32]bool{}, true, false, true)
	require.NoError(t, err)

	// The caller must still get its metadata — this is an optimisation, not a
	// change to what the caller sees.
	require.Equal(t, 0, missed)
	require.True(t, txMetaSlice[0].isSet, "metadata must still be resolved from the underlying store")
	require.Equal(t, tests.Tx.Size(), int(txMetaSlice[0].sizeInBytes))

	_, found := cache.GetMetaCached(ctx, hash)
	require.False(t, found, "block validation must not populate the txmeta cache")
}

// The non-batched branch reads through GetMeta, which also populates the cache
// on the way past, so it has to honour the same flag.
func TestProcessTxMetaUsingStore_SkipsCachePopulationUnbatched(t *testing.T) {
	server, cache, hash, ctx := newCachedStoreServer(t)

	txHashes := []chainhash.Hash{hash}
	txMetaSlice := make([]metaSliceItem, 1)

	_, err := server.processTxMetaUsingStore(ctx, txHashes, txMetaSlice, map[uint32]bool{}, false, false, true)
	require.NoError(t, err)
	require.True(t, txMetaSlice[0].isSet)

	_, found := cache.GetMetaCached(ctx, hash)
	require.False(t, found, "the unbatched branch must not populate the cache either")
}

// A store with no cache in front of it must behave identically whichever way the
// flag is set — the unwrap is a no-op there, not a nil dereference.
func TestProcessTxMetaUsingStore_SkipIsNoOpWithoutCache(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	underlying, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	_, _, err = underlying.SpendAndCreate(ctx, tests.Tx, 100, utxo.WithCreateOnly())
	require.NoError(t, err)

	s := settings.NewSettings()
	s.BlockValidation.ProcessTxMetaUsingStoreBatchSize = 1024
	s.BlockValidation.ProcessTxMetaUsingStoreConcurrency = 1
	s.BlockValidation.ProcessTxMetaUsingStoreMissingTxThreshold = 0
	s.BlockValidation.ProcessTxMetaUsingStoreRetries = 0

	server := &Server{logger: logger, utxoStore: underlying, settings: s}

	txHashes := []chainhash.Hash{*tests.Tx.TxIDChainHash()}
	txMetaSlice := make([]metaSliceItem, 1)

	missed, err := server.processTxMetaUsingStore(ctx, txHashes, txMetaSlice, map[uint32]bool{}, true, false, true)
	require.NoError(t, err)
	require.Equal(t, 0, missed)
	require.True(t, txMetaSlice[0].isSet)
}
