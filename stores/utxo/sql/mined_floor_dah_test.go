package sql

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/stretchr/testify/require"
)

// A mined record must outlive its highest block by the retention window, whatever
// height a delete-at-height writer is given. Writers that stamp from the store's
// cached tip lag the block being processed during catchup, and the pruner deletes
// purely on the stamp. These tests mine at a height far above the cached tip and
// check the stamp each writer leaves behind.

const (
	floorCachedTip   uint32 = 500
	floorMinedHeight uint32 = 1500
)

func floorRetention(t *testing.T, store *Store) int64 {
	t.Helper()

	retention := store.settings.GetUtxoStoreBlockHeightRetention()
	require.NotZero(t, retention)

	return int64(retention)
}

func readDAH(ctx context.Context, t *testing.T, store *Store, hash chainhash.Hash) *int64 {
	t.Helper()

	var dah *int64

	require.NoError(t, store.db.QueryRowContext(ctx,
		"SELECT delete_at_height FROM transactions WHERE hash = $1", hash[:]).Scan(&dah))

	return dah
}

// createMinedAboveTip stores the parent and the child unmined at the lagging tip,
// then marks the child mined at floorMinedHeight. Its outputs stay unspent, so
// mining leaves no stamp.
func createMinedAboveTip(ctx context.Context, t *testing.T, store *Store) chainhash.Hash {
	t.Helper()

	require.NoError(t, store.SetBlockHeight(floorCachedTip))

	_, err := store.Create(ctx, tests.ParentTx, floorCachedTip)
	require.NoError(t, err)

	_, err = store.Create(ctx, tests.Tx, floorCachedTip)
	require.NoError(t, err)

	hash := *tests.Tx.TxIDChainHash()

	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{&hash}, utxo.MinedBlockInfo{
		BlockID: 100, BlockHeight: floorMinedHeight, SubtreeIdx: 0, OnLongestChain: true,
	})
	require.NoError(t, err)

	require.Nil(t, readDAH(ctx, t, store, hash), "setup: a mined tx with unspent outputs must carry no stamp yet")

	return hash
}

func spendAllOutputs(ctx context.Context, t *testing.T, store *Store, hash chainhash.Hash, tx *bt.Tx, height uint32) {
	t.Helper()

	for i, out := range tx.Outputs {
		spendTx := bt.NewTx()
		require.NoError(t, spendTx.From(hash.String(), uint32(i), out.LockingScript.String(), out.Satoshis))
		require.NoError(t, spendTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))

		_, err := store.Spend(ctx, spendTx, height)
		require.NoError(t, err)
	}
}

// Spending the last output of a mined tx with the lagging tip as the spending height
// must still stamp from the mined height. Runs on the SQLite and Postgres paths.
func TestSpendStampsDAHAtMinedFloor(t *testing.T) {
	t.Run("sqlite", func(t *testing.T) {
		ctx := context.Background()
		store, _ := setup(ctx, t)
		retention := floorRetention(t, store)

		hash := createMinedAboveTip(ctx, t, store)
		spendAllOutputs(ctx, t, store, hash, tests.Tx, floorCachedTip+1)

		dah := readDAH(ctx, t, store, hash)
		require.NotNil(t, dah)
		require.GreaterOrEqual(t, *dah, int64(floorMinedHeight)+retention)
	})

	t.Run("postgres", func(t *testing.T) {
		if testing.Short() {
			t.Skip("skipping Postgres integration test in short mode")
		}

		store, ctx := setupPostgresStore(t)
		retention := floorRetention(t, store)

		hash := createMinedAboveTip(ctx, t, store)
		spendAllOutputs(ctx, t, store, hash, tests.Tx, floorCachedTip+1)

		dah := readDAH(ctx, t, store, hash)
		require.NotNil(t, dah)
		require.GreaterOrEqual(t, *dah, int64(floorMinedHeight)+retention)
	})
}

// Mined far above the cached tip, then flagged conflicting: the stamp comes from the
// mined height, not the lagging tip.
func TestSetConflictingStampsDAHAtMinedFloor_Postgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	store, ctx := setupPostgresStore(t)
	retention := floorRetention(t, store)

	hash := createMinedAboveTip(ctx, t, store)

	_, _, err := store.SetConflicting(ctx, []chainhash.Hash{hash}, true)
	require.NoError(t, err)

	dah := readDAH(ctx, t, store, hash)
	require.NotNil(t, dah)
	require.GreaterOrEqual(t, *dah, int64(floorMinedHeight)+retention,
		"the stamp must be at least mined height + retention, not the lagging tip + retention")
}

// SetConflicting reads each output's spend while its transaction is open. On a
// single-connection store a read on the pool waits for the connection the
// transaction holds, so this call used to hang. It must return, and stamp from the
// mined floor.
func TestSetConflictingStampsDAHAtMinedFloor_SQLite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	store, _ := setup(ctx, t)
	retention := floorRetention(t, store)

	hash := createMinedAboveTip(ctx, t, store)

	_, _, err := store.SetConflicting(ctx, []chainhash.Hash{hash}, true)
	require.NoError(t, err)

	dah := readDAH(ctx, t, store, hash)
	require.NotNil(t, dah)
	require.GreaterOrEqual(t, *dah, int64(floorMinedHeight)+retention,
		"the stamp must be at least mined height + retention, not the lagging tip + retention")
}

// A conflicting record already stamped from the lagging tip, then flagged conflicting
// again after mining, has its stale stamp raised to the mined floor.
func TestSetConflictingRaisesStaleStamp_Postgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	store, ctx := setupPostgresStore(t)
	retention := floorRetention(t, store)

	hash := createMinedAboveTip(ctx, t, store)

	_, err := store.db.ExecContext(ctx,
		"UPDATE transactions SET conflicting = true, delete_at_height = $2 WHERE hash = $1",
		hash[:], int64(floorCachedTip)+1+retention)
	require.NoError(t, err)

	_, _, err = store.SetConflicting(ctx, []chainhash.Hash{hash}, true)
	require.NoError(t, err)

	dah := readDAH(ctx, t, store, hash)
	require.NotNil(t, dah)
	require.GreaterOrEqual(t, *dah, int64(floorMinedHeight)+retention,
		"re-flagging a mined conflicting record must raise a stamp written from a lagging tip")
}

// Flagged conflicting while unmined (stamped from the lagging tip), then mined far
// above it: mining raises the stamp to the mined floor.
func TestSetMinedRaisesConflictingStamp_Postgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	store, ctx := setupPostgresStore(t)
	retention := floorRetention(t, store)

	require.NoError(t, store.SetBlockHeight(floorCachedTip))

	_, err := store.Create(ctx, tests.ParentTx, floorCachedTip)
	require.NoError(t, err)

	_, err = store.Create(ctx, tests.Tx, floorCachedTip)
	require.NoError(t, err)

	hash := *tests.Tx.TxIDChainHash()

	_, _, err = store.SetConflicting(ctx, []chainhash.Hash{hash}, true)
	require.NoError(t, err)

	before := readDAH(ctx, t, store, hash)
	require.NotNil(t, before)
	require.Less(t, *before, int64(floorMinedHeight), "setup: the stamp must come from the lagging tip")

	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{&hash}, utxo.MinedBlockInfo{
		BlockID: 100, BlockHeight: floorMinedHeight, SubtreeIdx: 0, OnLongestChain: true,
	})
	require.NoError(t, err)

	after := readDAH(ctx, t, store, hash)
	require.NotNil(t, after)
	require.GreaterOrEqual(t, *after, int64(floorMinedHeight)+retention)
}

// The bulk unlock recomputes the stamp for a conflicting record from the cached tip.
// It must floor it at the mined height too.
func TestUnlockStampsConflictingDAHAtMinedFloor_Postgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	store, ctx := setupPostgresStore(t)
	retention := floorRetention(t, store)

	hash := createMinedAboveTip(ctx, t, store)

	_, err := store.db.ExecContext(ctx, "UPDATE transactions SET conflicting = true WHERE hash = $1", hash[:])
	require.NoError(t, err)

	require.NoError(t, store.SetLocked(ctx, []chainhash.Hash{hash}, true))
	require.Nil(t, readDAH(ctx, t, store, hash), "setup: locking clears the stamp")

	require.NoError(t, store.SetLocked(ctx, []chainhash.Hash{hash}, false))

	dah := readDAH(ctx, t, store, hash)
	require.NotNil(t, dah)
	require.GreaterOrEqual(t, *dah, int64(floorMinedHeight)+retention)
}

// An expired preservation stamps from the pruner's current height. For a conflicting
// record mined above it, the stamp must still be at least mined height + retention.
func TestExpiredPreservationStampsDAHAtMinedFloor_Postgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	store, ctx := setupPostgresStore(t)
	retention := floorRetention(t, store)

	hash := createMinedAboveTip(ctx, t, store)

	// preservation only applies to a record that already carries a stamp
	_, _, err := store.SetConflicting(ctx, []chainhash.Hash{hash}, true)
	require.NoError(t, err)

	require.NoError(t, store.PreserveTransactions(ctx, []chainhash.Hash{hash}, 20))
	require.Nil(t, readDAH(ctx, t, store, hash), "setup: preserving clears the stamp")

	require.NoError(t, store.ProcessExpiredPreservations(ctx, 20))

	dah := readDAH(ctx, t, store, hash)
	require.NotNil(t, dah)
	require.GreaterOrEqual(t, *dah, int64(floorMinedHeight)+retention)
}
