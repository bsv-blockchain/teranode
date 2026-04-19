// Package tests provides tests for the CleanupUnmined function.
package tests

import (
	"context"
	"fmt"
	"net/url"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// setupSQLiteFileStore creates a file-based SQLite store in t.TempDir() with
// the shared parent Tx already created. newTestTx-produced transactions spend
// Tx's outputs, so SetConflicting needs Tx in the store to resolve the
// conflicting_children back-reference.
func setupSQLiteFileStore(ctx context.Context, t *testing.T) utxo.Store {
	t.Helper()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.DBTimeout = 30 * time.Second

	dbPath := filepath.Join(t.TempDir(), "purge-test.db")
	storeURL, err := url.Parse(fmt.Sprintf("sqlite:///%s", dbPath))
	require.NoError(t, err)

	store, err := sql.New(ctx, logger, tSettings, storeURL)
	require.NoError(t, err)

	_, err = store.Create(ctx, Tx, 100, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{
		BlockID:        1,
		BlockHeight:    99,
		OnLongestChain: true,
	}))
	require.NoError(t, err)

	return store
}

// testBlockchainQuerier is a local implementation of utxo.BlockchainQuerier for testing.
type testBlockchainQuerier struct {
	bestBlockHash   *chainhash.Hash
	bestBlockHeight uint32
	blockHeaderIDs  []uint32
}

func (tbq *testBlockchainQuerier) GetBestBlockHeaderInfo(ctx context.Context) (utxo.BlockHeaderInfo, error) {
	return utxo.BlockHeaderInfo{Hash: tbq.bestBlockHash, Height: tbq.bestBlockHeight}, nil
}

func (tbq *testBlockchainQuerier) GetBlockHeaderIDs(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]uint32, error) {
	return tbq.blockHeaderIDs, nil
}

// newQuerier creates a simple querier with no blocks on the best chain.
func newQuerier() *testBlockchainQuerier {
	h := &chainhash.Hash{}
	return &testBlockchainQuerier{
		bestBlockHash:   h,
		bestBlockHeight: 0,
		blockHeaderIDs:  []uint32{},
	}
}

// createConflictingUnmined makes a uniquely-identifiable tx, stores it as
// unmined at blockHeight, then marks Conflicting=true. Returns the hash.
func createConflictingUnmined(t *testing.T, ctx context.Context, store utxo.Store, satoshis uint64, blockHeight uint32) chainhash.Hash {
	t.Helper()
	tx := newTestTx(t, satoshis)
	_, err := store.Create(ctx, tx, blockHeight)
	require.NoError(t, err)
	hash := *tx.TxIDChainHash()
	_, _, err = store.SetConflicting(ctx, []chainhash.Hash{hash}, true)
	require.NoError(t, err)
	return hash
}

// createNonConflictingUnmined stores a tx as plain unmined and returns the hash.
func createNonConflictingUnmined(t *testing.T, ctx context.Context, store utxo.Store, satoshis uint64, blockHeight uint32) chainhash.Hash {
	t.Helper()
	tx := newTestTx(t, satoshis)
	_, err := store.Create(ctx, tx, blockHeight)
	require.NoError(t, err)
	return *tx.TxIDChainHash()
}

// createMinedConflicting stores a tx, mines it on the longest chain (clears
// UnminedSince), then marks Conflicting=true — the state where the purge must
// leave the record alone.
func createMinedConflicting(t *testing.T, ctx context.Context, store utxo.Store, satoshis uint64, blockHeight uint32, blockID uint32) chainhash.Hash {
	t.Helper()
	tx := newTestTx(t, satoshis)
	_, err := store.Create(ctx, tx, blockHeight)
	require.NoError(t, err)
	hash := *tx.TxIDChainHash()
	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{&hash}, utxo.MinedBlockInfo{
		BlockID:        blockID,
		BlockHeight:    blockHeight,
		SubtreeIdx:     0,
		OnLongestChain: true,
	})
	require.NoError(t, err)
	_, _, err = store.SetConflicting(ctx, []chainhash.Hash{hash}, true)
	require.NoError(t, err)
	return hash
}

// TestCleanupUnmined_CleanState verifies that running the purge on an
// empty store produces a zeroed report.
func TestCleanupUnmined_CleanState(t *testing.T) {
	ctx := context.Background()
	store := setupSQLiteFileStore(ctx, t)
	querier := newQuerier()

	report, err := utxo.CleanupUnmined(ctx, store, querier, false, nil)
	require.NoError(t, err)
	require.Equal(t, 0, report.UnminedSinceFixed)
	require.Equal(t, 0, report.ConflictingUnminedPurged)
}

// TestCleanupUnmined_DeletesConflictingUnmined verifies that a record
// with Conflicting=true and UnminedSince>0 is removed from the store.
func TestCleanupUnmined_DeletesConflictingUnmined(t *testing.T) {
	ctx := context.Background()
	store := setupSQLiteFileStore(ctx, t)
	querier := newQuerier()

	hash := createConflictingUnmined(t, ctx, store, 100_000, 100)

	report, err := utxo.CleanupUnmined(ctx, store, querier, false, nil)
	require.NoError(t, err)
	require.Equal(t, 1, report.ConflictingUnminedPurged)

	_, err = store.Get(ctx, &hash, fields.Conflicting)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxNotFound), "purged record must not be found; got %v", err)
}

// TestCleanupUnmined_LeavesNonConflictingUnminedAlone verifies that a
// non-conflicting unmined tx — even one whose parent we just purged — remains
// in the store. BA's validateParentChain tolerates the dangling parent ref.
func TestCleanupUnmined_LeavesNonConflictingUnminedAlone(t *testing.T) {
	ctx := context.Background()
	store := setupSQLiteFileStore(ctx, t)
	querier := newQuerier()

	conflictingHash := createConflictingUnmined(t, ctx, store, 200_000, 100)
	nonConflictingHash := createNonConflictingUnmined(t, ctx, store, 300_000, 100)

	report, err := utxo.CleanupUnmined(ctx, store, querier, false, nil)
	require.NoError(t, err)
	require.Equal(t, 1, report.ConflictingUnminedPurged)

	_, err = store.Get(ctx, &conflictingHash, fields.Conflicting)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxNotFound))

	// Non-conflicting tx is untouched.
	meta, err := store.Get(ctx, &nonConflictingHash, fields.Conflicting)
	require.NoError(t, err)
	require.NotNil(t, meta)
	require.False(t, meta.Conflicting)
}

// TestCleanupUnmined_LeavesMinedTxAlone verifies that a mined record
// carrying Conflicting=true but with UnminedSince=0 is NOT deleted (the
// UnminedSince>0 filter guards us from removing historical conflict records).
func TestCleanupUnmined_LeavesMinedTxAlone(t *testing.T) {
	ctx := context.Background()
	store := setupSQLiteFileStore(ctx, t)
	querier := newQuerier()

	minedHash := createMinedConflicting(t, ctx, store, 400_000, 100, 7)

	report, err := utxo.CleanupUnmined(ctx, store, querier, false, nil)
	require.NoError(t, err)
	require.Equal(t, 0, report.ConflictingUnminedPurged)

	meta, err := store.Get(ctx, &minedHash, fields.Conflicting)
	require.NoError(t, err)
	require.NotNil(t, meta)
	require.True(t, meta.Conflicting)
}

// TestCleanupUnmined_DryRun verifies that dryRun=true counts purge
// candidates without actually deleting them.
func TestCleanupUnmined_DryRun(t *testing.T) {
	ctx := context.Background()
	store := setupSQLiteFileStore(ctx, t)
	querier := newQuerier()

	hash := createConflictingUnmined(t, ctx, store, 500_000, 100)

	report, err := utxo.CleanupUnmined(ctx, store, querier, true, nil)
	require.NoError(t, err)
	require.Equal(t, 1, report.ConflictingUnminedPurged, "dry run still counts candidates")

	// Record must still exist.
	meta, err := store.Get(ctx, &hash, fields.Conflicting)
	require.NoError(t, err)
	require.NotNil(t, meta)
	require.True(t, meta.Conflicting)
}

// TestCleanupUnmined_SkipUnminedSinceScan verifies that step 0 is
// skipped when the option is set but step 1 (the purge) still runs.
func TestCleanupUnmined_SkipUnminedSinceScan(t *testing.T) {
	ctx := context.Background()
	store := setupSQLiteFileStore(ctx, t)
	querier := newQuerier()

	hash := createConflictingUnmined(t, ctx, store, 600_000, 100)

	report, err := utxo.CleanupUnmined(ctx, store, querier, false, nil, utxo.CleanupOptions{SkipUnminedSinceScan: true})
	require.NoError(t, err)
	require.Equal(t, 0, report.UnminedSinceFixed)
	require.Equal(t, 1, report.ConflictingUnminedPurged)

	_, err = store.Get(ctx, &hash, fields.Conflicting)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxNotFound))
}

// TestCleanupUnmined_UnminedSinceFix verifies step 0 clears the
// stray-UnminedSince-on-mined-tx case. The tx is marked unmined, then stored
// as mined on the best chain without clearing UnminedSince (simulated by
// MarkTransactionsOnLongestChain(false) after SetMinedMulti). The purge's
// step 0 should re-mark it as on the longest chain.
func TestCleanupUnmined_UnminedSinceFix(t *testing.T) {
	ctx := context.Background()
	store := setupSQLiteFileStore(ctx, t)

	const blockID = uint32(42)
	querier := &testBlockchainQuerier{
		bestBlockHash:   &chainhash.Hash{},
		bestBlockHeight: 100,
		blockHeaderIDs:  []uint32{blockID},
	}

	require.NoError(t, store.SetBlockHeight(100))

	tx := newTestTx(t, 700_000)
	_, err := store.Create(ctx, tx, 100)
	require.NoError(t, err)
	hash := *tx.TxIDChainHash()

	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{&hash}, utxo.MinedBlockInfo{
		BlockID:        blockID,
		BlockHeight:    100,
		SubtreeIdx:     0,
		OnLongestChain: true,
	})
	require.NoError(t, err)

	// Force the inconsistent state: the tx is mined on the best chain yet still
	// carries UnminedSince (the bug cleanup-unmined step 0 repairs).
	err = store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{hash}, false)
	require.NoError(t, err)

	report, err := utxo.CleanupUnmined(ctx, store, querier, false, nil)
	require.NoError(t, err)
	require.Equal(t, 1, report.UnminedSinceFixed)

	meta, err := store.Get(ctx, &hash, fields.UnminedSince)
	require.NoError(t, err)
	require.Zero(t, meta.UnminedSince, "step 0 should have cleared UnminedSince on the mined tx")
}

// TestCleanupUnmined_Idempotent verifies that running the purge a
// second time on the same store is a no-op — all candidates were already
// deleted by the first pass.
func TestCleanupUnmined_Idempotent(t *testing.T) {
	ctx := context.Background()
	store := setupSQLiteFileStore(ctx, t)
	querier := newQuerier()

	_ = createConflictingUnmined(t, ctx, store, 800_000, 100)

	firstReport, err := utxo.CleanupUnmined(ctx, store, querier, false, nil)
	require.NoError(t, err)
	require.Equal(t, 1, firstReport.ConflictingUnminedPurged)

	secondReport, err := utxo.CleanupUnmined(ctx, store, querier, false, nil)
	require.NoError(t, err)
	require.Equal(t, 0, secondReport.ConflictingUnminedPurged)
}

// deleteTrackingStore wraps a utxo.Store and counts Delete calls so tests can
// confirm the purge forwards Delete to the underlying store (which in a live
// node includes the cache-eviction layer when present).
type deleteTrackingStore struct {
	utxo.Store
	deleted atomic.Int64
	hashes  []chainhash.Hash
}

func (d *deleteTrackingStore) Delete(ctx context.Context, hash *chainhash.Hash) error {
	d.deleted.Add(1)
	d.hashes = append(d.hashes, *hash)
	return d.Store.Delete(ctx, hash)
}

// TestCleanupUnmined_DeleteForwardsThroughStore verifies the purge
// calls Store.Delete for every purged hash. In a node with a TxMetaCache layer
// wrapping the UTXO store, this is also the hook that evicts the cached entry.
func TestCleanupUnmined_DeleteForwardsThroughStore(t *testing.T) {
	ctx := context.Background()
	inner := setupSQLiteFileStore(ctx, t)
	tracker := &deleteTrackingStore{Store: inner}
	querier := newQuerier()

	hash := createConflictingUnmined(t, ctx, tracker, 900_000, 100)

	report, err := utxo.CleanupUnmined(ctx, tracker, querier, false, nil)
	require.NoError(t, err)
	require.Equal(t, 1, report.ConflictingUnminedPurged)
	require.Equal(t, int64(1), tracker.deleted.Load(), "purge must call Store.Delete for the purged hash")
	require.Equal(t, []chainhash.Hash{hash}, tracker.hashes)
}
