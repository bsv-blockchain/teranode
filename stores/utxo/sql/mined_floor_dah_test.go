package sql

import (
	"context"
	"database/sql"
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

func spendAllOutputsForMinedFloor(ctx context.Context, t *testing.T, store *Store, hash chainhash.Hash, tx *bt.Tx, height uint32) {
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
		spendAllOutputsForMinedFloor(ctx, t, store, hash, tests.Tx, floorCachedTip+1)

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
		spendAllOutputsForMinedFloor(ctx, t, store, hash, tests.Tx, floorCachedTip+1)

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

// SetConflicting does its spend reads before it opens its write transaction. Done
// inside the transaction, they hang on a single-connection SQLite store. This test
// guards that ordering and checks the stamp comes from the mined floor.
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

// SetConflicting returns the spenders of the flagged transaction's outputs. They
// must be read before the flag is written: a read after the UPDATE sees
// conflicting = true and reports CONFLICTING, which would drop the spender.
func TestSetConflictingReturnsSpendersOfOutputs(t *testing.T) {
	ctx := context.Background()
	store, _ := setup(ctx, t)

	require.NoError(t, store.SetBlockHeight(floorCachedTip))

	_, err := store.Create(ctx, tests.ParentTx, floorCachedTip)
	require.NoError(t, err)

	_, err = store.Create(ctx, tests.Tx, floorCachedTip)
	require.NoError(t, err)

	hash := *tests.Tx.TxIDChainHash()

	spendTx := bt.NewTx()
	require.NoError(t, spendTx.From(hash.String(), 0, tests.Tx.Outputs[0].LockingScript.String(), tests.Tx.Outputs[0].Satoshis))
	require.NoError(t, spendTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))

	_, _, err = store.SpendAndCreate(ctx, spendTx, floorCachedTip, utxo.WithSpendOnly())
	require.NoError(t, err)

	_, spenders, err := store.SetConflicting(ctx, []chainhash.Hash{hash}, true)
	require.NoError(t, err)
	require.Equal(t, []chainhash.Hash{*spendTx.TxIDChainHash()}, spenders)
}

func forEachFloorBackend(t *testing.T, fn func(t *testing.T, ctx context.Context, store *Store, postgres bool)) {
	t.Helper()

	t.Run("sqlite", func(t *testing.T) {
		ctx := context.Background()
		store, _ := setup(ctx, t)

		fn(t, ctx, store, false)
	})

	t.Run("postgres", func(t *testing.T) {
		if testing.Short() {
			t.Skip("skipping Postgres integration test in short mode")
		}

		store, ctx := setupPostgresStore(t)

		fn(t, ctx, store, true)
	})
}

func floorRowID(ctx context.Context, t *testing.T, store *Store, hash chainhash.Hash) int {
	t.Helper()

	var id int

	require.NoError(t, store.db.QueryRowContext(ctx, "SELECT id FROM transactions WHERE hash = $1", hash[:]).Scan(&id))

	return id
}

// setDAH decides from an unlocked read. A concurrent mining commit that adds a higher
// block and raises the stamp after that read must survive the write: the decision is a
// stale value and the UPDATE has to reconcile it with the row.
func TestWriteDAHKeepsConcurrentlyRaisedStamp(t *testing.T) {
	forEachFloorBackend(t, func(t *testing.T, ctx context.Context, store *Store, postgres bool) {
		retention := floorRetention(t, store)

		require.NoError(t, store.SetBlockHeight(floorCachedTip))

		_, err := store.Create(ctx, tests.ParentTx, floorCachedTip)
		require.NoError(t, err)

		_, err = store.Create(ctx, tests.Tx, floorCachedTip)
		require.NoError(t, err)

		hash := *tests.Tx.TxIDChainHash()

		_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{&hash}, utxo.MinedBlockInfo{
			BlockID: 100, BlockHeight: 600, SubtreeIdx: 0, OnLongestChain: true,
		})
		require.NoError(t, err)

		spendAllOutputsForMinedFloor(ctx, t, store, hash, tests.Tx, floorCachedTip+1)

		id := floorRowID(ctx, t, store, hash)
		stale := readDAH(ctx, t, store, hash)
		require.NotNil(t, stale)
		require.Equal(t, int64(600)+retention, *stale)

		raised := int64(floorMinedHeight) + retention + 50

		raise := func() {
			_, execErr := store.db.ExecContext(ctx,
				"INSERT INTO block_ids (transaction_id, block_id, block_height, subtree_idx) VALUES ($1, 101, $2, 0)",
				id, int64(floorMinedHeight))
			require.NoError(t, execErr)

			_, execErr = store.db.ExecContext(ctx,
				"UPDATE transactions SET delete_at_height = $2 WHERE id = $1", id, raised)
			require.NoError(t, execErr)
		}

		var txn *sql.Tx

		if postgres {
			txn, err = store.db.BeginTx(ctx, nil)
			require.NoError(t, err)

			raise()
		} else {
			raise()

			txn, err = store.db.BeginTx(ctx, nil)
			require.NoError(t, err)
		}

		require.NoError(t, store.writeDAH(ctx, txn, id, sql.NullInt64{Int64: *stale, Valid: true}, uint32(retention)))
		require.NoError(t, txn.Commit())

		after := readDAH(ctx, t, store, hash)
		require.NotNil(t, after)
		require.Equal(t, raised, *after, "a stale value must not overwrite the higher stamp")
	})
}

// A value below the row's mined floor is raised by the write, and a NULL decision still clears.
func TestWriteDAHFloorsValueAndClearsOnNull(t *testing.T) {
	forEachFloorBackend(t, func(t *testing.T, ctx context.Context, store *Store, _ bool) {
		retention := floorRetention(t, store)

		hash := createMinedAboveTip(ctx, t, store)
		id := floorRowID(ctx, t, store, hash)

		write := func(value sql.NullInt64) {
			txn, err := store.db.BeginTx(ctx, nil)
			require.NoError(t, err)
			require.NoError(t, store.writeDAH(ctx, txn, id, value, uint32(retention)))
			require.NoError(t, txn.Commit())
		}

		write(sql.NullInt64{Int64: int64(floorCachedTip) + 1 + retention, Valid: true})

		dah := readDAH(ctx, t, store, hash)
		require.NotNil(t, dah)
		require.Equal(t, int64(floorMinedHeight)+retention, *dah)

		write(sql.NullInt64{})
		require.Nil(t, readDAH(ctx, t, store, hash))
	})
}

// Flagged conflicting while unmined at the lagging tip, then mined into a fork far
// above it: the stamp must follow the mined height, not stay at tip + retention.
func TestSetMinedOffLongestChainRaisesConflictingStamp(t *testing.T) {
	forEachFloorBackend(t, func(t *testing.T, ctx context.Context, store *Store, _ bool) {
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
			BlockID: 100, BlockHeight: floorMinedHeight, SubtreeIdx: 0, OnLongestChain: false,
		})
		require.NoError(t, err)

		after := readDAH(ctx, t, store, hash)
		require.NotNil(t, after)
		require.GreaterOrEqual(t, *after, int64(floorMinedHeight)+retention)
	})
}

// A conflicting record with a stale low stamp and a high mined block is raised when
// it is put back on the longest chain.
func TestMarkOnLongestChainRaisesConflictingStamp(t *testing.T) {
	forEachFloorBackend(t, func(t *testing.T, ctx context.Context, store *Store, _ bool) {
		retention := floorRetention(t, store)

		hash := createMinedAboveTip(ctx, t, store)

		_, err := store.db.ExecContext(ctx,
			"UPDATE transactions SET conflicting = true, delete_at_height = $2 WHERE hash = $1",
			hash[:], int64(floorCachedTip)+1+retention)
		require.NoError(t, err)

		require.NoError(t, store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{hash}, true))

		dah := readDAH(ctx, t, store, hash)
		require.NotNil(t, dah)
		require.GreaterOrEqual(t, *dah, int64(floorMinedHeight)+retention)
	})
}

func spendAllAtLaggingTip(ctx context.Context, t *testing.T, store *Store, hash chainhash.Hash) {
	t.Helper()

	spendAllOutputsForMinedFloor(ctx, t, store, hash, tests.Tx, floorCachedTip+1)
}

func createUnminedAtTip(ctx context.Context, t *testing.T, store *Store) chainhash.Hash {
	t.Helper()

	require.NoError(t, store.SetBlockHeight(floorCachedTip))

	_, err := store.Create(ctx, tests.ParentTx, floorCachedTip)
	require.NoError(t, err)

	_, err = store.Create(ctx, tests.Tx, floorCachedTip)
	require.NoError(t, err)

	return *tests.Tx.TxIDChainHash()
}

// A fully spent tx mined on a fork at a high block, then mined on the longest chain at
// a lower one, keeps the highest block as its floor.
func TestSetMinedOnLongestChainFloorsAtHighestForkBlock(t *testing.T) {
	forEachFloorBackend(t, func(t *testing.T, ctx context.Context, store *Store, _ bool) {
		retention := floorRetention(t, store)

		hash := createUnminedAtTip(ctx, t, store)

		_, err := store.SetMinedMulti(ctx, []*chainhash.Hash{&hash}, utxo.MinedBlockInfo{
			BlockID: 100, BlockHeight: floorMinedHeight, SubtreeIdx: 0, OnLongestChain: false,
		})
		require.NoError(t, err)

		spendAllAtLaggingTip(ctx, t, store, hash)
		require.Nil(t, readDAH(ctx, t, store, hash), "setup: a tx still off the longest chain carries no stamp")

		_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{&hash}, utxo.MinedBlockInfo{
			BlockID: 101, BlockHeight: floorCachedTip, SubtreeIdx: 0, OnLongestChain: true,
		})
		require.NoError(t, err)

		dah := readDAH(ctx, t, store, hash)
		require.NotNil(t, dah)
		require.Equal(t, int64(floorMinedHeight)+retention, *dah)
	})
}

// A fully spent tx mined on the longest chain at a high block, with no stamp, then
// placed on a fork at a lower block: the stamp follows the highest block.
func TestSetMinedOffLongestChainFloorsFullySpentStamp(t *testing.T) {
	forEachFloorBackend(t, func(t *testing.T, ctx context.Context, store *Store, _ bool) {
		retention := floorRetention(t, store)

		hash := createMinedAboveTip(ctx, t, store)
		spendAllAtLaggingTip(ctx, t, store, hash)

		_, err := store.db.ExecContext(ctx, "UPDATE transactions SET delete_at_height = NULL WHERE hash = $1", hash[:])
		require.NoError(t, err)

		_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{&hash}, utxo.MinedBlockInfo{
			BlockID: 101, BlockHeight: floorCachedTip, SubtreeIdx: 0, OnLongestChain: false,
		})
		require.NoError(t, err)

		dah := readDAH(ctx, t, store, hash)
		require.NotNil(t, dah)
		require.Equal(t, int64(floorMinedHeight)+retention, *dah)
	})
}

// A conflicting record with a stamp below the floor is raised to the floor itself when
// put back on the longest chain, as the Lua setDeleteAtHeight does, not to the current
// tip + retention. A record with no stamp still gets the tip-based one.
func TestMarkOnLongestChainRaisesLowConflictingStampToFloorOnly(t *testing.T) {
	forEachFloorBackend(t, func(t *testing.T, ctx context.Context, store *Store, _ bool) {
		retention := floorRetention(t, store)

		hash := createMinedAboveTip(ctx, t, store)

		_, err := store.db.ExecContext(ctx,
			"UPDATE transactions SET conflicting = true, delete_at_height = 5 WHERE hash = $1", hash[:])
		require.NoError(t, err)

		require.NoError(t, store.SetBlockHeight(floorMinedHeight+400))
		require.NoError(t, store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{hash}, true))

		dah := readDAH(ctx, t, store, hash)
		require.NotNil(t, dah)
		require.Equal(t, int64(floorMinedHeight)+retention, *dah)

		_, err = store.db.ExecContext(ctx,
			"UPDATE transactions SET delete_at_height = NULL WHERE hash = $1", hash[:])
		require.NoError(t, err)

		require.NoError(t, store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{hash}, true))

		dah = readDAH(ctx, t, store, hash)
		require.NotNil(t, dah)
		require.Equal(t, int64(floorMinedHeight+400+1)+retention, *dah)
	})
}

// Locking clears the stamp and unlocking recomputes it from the cached tip. A mined,
// fully spent tx must come back at its mined floor.
func TestUnlockStampsSpentDAHAtMinedFloor_Postgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	store, ctx := setupPostgresStore(t)
	retention := floorRetention(t, store)

	hash := createMinedAboveTip(ctx, t, store)
	spendAllAtLaggingTip(ctx, t, store, hash)

	before := readDAH(ctx, t, store, hash)
	require.NotNil(t, before)
	require.Equal(t, int64(floorMinedHeight)+retention, *before)

	require.NoError(t, store.SetLocked(ctx, []chainhash.Hash{hash}, true))
	require.NoError(t, store.SetLocked(ctx, []chainhash.Hash{hash}, false))

	dah := readDAH(ctx, t, store, hash)
	require.NotNil(t, dah)
	require.Equal(t, int64(floorMinedHeight)+retention, *dah)
}

// A conflicting row whose stamp was written below the floor between lock and unlock is
// raised to the floor by the unlock.
func TestUnlockRaisesLowConflictingStampToFloor_Postgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	store, ctx := setupPostgresStore(t)
	retention := floorRetention(t, store)

	hash := createMinedAboveTip(ctx, t, store)

	_, err := store.db.ExecContext(ctx, "UPDATE transactions SET conflicting = true WHERE hash = $1", hash[:])
	require.NoError(t, err)

	require.NoError(t, store.SetLocked(ctx, []chainhash.Hash{hash}, true))

	_, err = store.db.ExecContext(ctx,
		"UPDATE transactions SET delete_at_height = $2 WHERE hash = $1", hash[:], int64(floorCachedTip)+1+retention)
	require.NoError(t, err)

	require.NoError(t, store.SetBlockHeight(floorMinedHeight+400))
	require.NoError(t, store.SetLocked(ctx, []chainhash.Hash{hash}, false))

	dah := readDAH(ctx, t, store, hash)
	require.NotNil(t, dah)
	require.Equal(t, int64(floorMinedHeight)+retention, *dah)
}

// An expired preservation of a mined, fully spent, non-conflicting tx stamps from the
// pruner's current height. It must still land at the mined floor.
func TestExpiredPreservationStampsSpentDAHAtMinedFloor(t *testing.T) {
	forEachFloorBackend(t, func(t *testing.T, ctx context.Context, store *Store, _ bool) {
		retention := floorRetention(t, store)

		hash := createMinedAboveTip(ctx, t, store)
		spendAllAtLaggingTip(ctx, t, store, hash)

		require.NoError(t, store.PreserveTransactions(ctx, []chainhash.Hash{hash}, 20))
		require.Nil(t, readDAH(ctx, t, store, hash), "setup: preserving clears the stamp")

		require.NoError(t, store.ProcessExpiredPreservations(ctx, 20))

		dah := readDAH(ctx, t, store, hash)
		require.NotNil(t, dah)
		require.Equal(t, int64(floorMinedHeight)+retention, *dah)
	})
}

func floorCtid(ctx context.Context, t *testing.T, store *Store, hash chainhash.Hash) string {
	t.Helper()

	var ctid string

	require.NoError(t, store.db.QueryRowContext(ctx,
		"SELECT ctid::text FROM transactions WHERE hash = $1", hash[:]).Scan(&ctid))

	return ctid
}

func spendFirstOutput(ctx context.Context, t *testing.T, store *Store, hash chainhash.Hash) []*utxo.Spend {
	t.Helper()

	spendTx := bt.NewTx()
	require.NoError(t, spendTx.From(hash.String(), 0, tests.Tx.Outputs[0].LockingScript.String(), tests.Tx.Outputs[0].Satoshis))
	require.NoError(t, spendTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))

	spends, err := store.Spend(ctx, spendTx, floorCachedTip)
	require.NoError(t, err)
	require.NotEmpty(t, spends)

	return spends
}

// An Unspend of a conflicting parent with a stamp keeps that stamp. The keep decision
// issues no UPDATE, so a concurrent SetConflicting(false) or PreserveTransactions that
// commits after setDAH reads cannot be overwritten with a stale stamp.
func TestUnspendKeepingConflictingStampWritesNothing_Postgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	store, ctx := setupPostgresStore(t)

	hash := createUnminedAtTip(ctx, t, store)
	spends := spendFirstOutput(ctx, t, store, hash)

	stamp := int64(5000)

	_, err := store.db.ExecContext(ctx,
		"UPDATE transactions SET conflicting = true, delete_at_height = $2 WHERE hash = $1", hash[:], stamp)
	require.NoError(t, err)

	ctidBefore := floorCtid(ctx, t, store, hash)

	require.NoError(t, store.Unspend(ctx, spends))

	require.Equal(t, ctidBefore, floorCtid(ctx, t, store, hash), "a keep-existing decision must not rewrite the row")

	dah := readDAH(ctx, t, store, hash)
	require.NotNil(t, dah)
	require.Equal(t, stamp, *dah)
}

// A parent cleared by SetConflicting(false) stays unstamped through an Unspend.
func TestUnspendAfterConflictingClearedKeepsNoStamp_Postgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	store, ctx := setupPostgresStore(t)

	hash := createUnminedAtTip(ctx, t, store)
	spends := spendFirstOutput(ctx, t, store, hash)

	_, err := store.db.ExecContext(ctx,
		"UPDATE transactions SET conflicting = true, delete_at_height = 5000 WHERE hash = $1", hash[:])
	require.NoError(t, err)

	_, _, err = store.SetConflicting(ctx, []chainhash.Hash{hash}, false)
	require.NoError(t, err)
	require.Nil(t, readDAH(ctx, t, store, hash), "setup: clearing the conflicting flag clears the stamp")

	ctidBefore := floorCtid(ctx, t, store, hash)

	require.NoError(t, store.Unspend(ctx, spends))

	require.Nil(t, readDAH(ctx, t, store, hash))
	require.Equal(t, ctidBefore, floorCtid(ctx, t, store, hash))
}
