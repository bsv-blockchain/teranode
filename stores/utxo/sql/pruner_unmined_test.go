package sql

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
	"github.com/stretchr/testify/require"
)

// TestPrunerSkipsUnminedRecords verifies the pruner never deletes a record that is
// currently unmined (unmined_since set) unless it is conflicting, in both the
// default and the defensive delete paths.
func TestPrunerSkipsUnminedRecords(t *testing.T) {
	for _, defensive := range []bool{false, true} {
		t.Run(fmt.Sprintf("defensive=%v", defensive), func(t *testing.T) {
			ResetPrunerServiceForTests()
			t.Cleanup(ResetPrunerServiceForTests)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			store, _ := setup(ctx, t)
			store.settings.Pruner.UTXODefensiveEnabled = defensive

			provider, ok := any(store).(pruner.PrunerServiceProvider)
			require.True(t, ok)

			prunerSvc, err := provider.GetPrunerService()
			require.NoError(t, err)

			const pruneHeight = 100

			insert := func(hash byte, conflicting bool, unminedSince any) {
				_, err := store.db.ExecContext(ctx, `
					INSERT INTO transactions (hash, version, lock_time, fee, size_in_bytes, conflicting, unmined_since, delete_at_height)
					VALUES ($1, 1, 0, 0, 100, $2, $3, $4)`,
					[]byte{hash}, conflicting, unminedSince, pruneHeight-1)
				require.NoError(t, err)
			}

			insert(1, false, 50)  // unmined, not conflicting: must survive, DAH cleared
			insert(2, true, 50)   // unmined, conflicting: deleted
			insert(3, false, nil) // mined: deleted

			// unmined and preserved: survives and keeps its DAH, as the Aerospike Lua does
			insert(4, false, 50)
			_, err = store.db.ExecContext(ctx, `UPDATE transactions SET preserve_until = 500 WHERE hash = $1`, []byte{4})
			require.NoError(t, err)

			deleted, err := prunerSvc.Prune(ctx, pruneHeight, "test")
			require.NoError(t, err)
			require.Equal(t, int64(2), deleted)

			var remaining []byte

			require.NoError(t, store.db.QueryRowContext(ctx, `SELECT hash FROM transactions WHERE hash IN ($1,$2,$3)`,
				[]byte{1}, []byte{2}, []byte{3}).Scan(&remaining))
			require.Equal(t, []byte{1}, remaining)

			dah := func(hash byte) sql.NullInt64 {
				var v sql.NullInt64

				require.NoError(t, store.db.QueryRowContext(ctx, `SELECT delete_at_height FROM transactions WHERE hash = $1`, []byte{hash}).Scan(&v))

				return v
			}

			require.False(t, dah(1).Valid, "skipped unmined record must have its DAH cleared")
			require.True(t, dah(4).Valid, "preserved unmined record must keep its DAH")

			// a second cycle no longer matches the cleared record
			var matching int

			require.NoError(t, store.db.QueryRowContext(ctx, `SELECT count(*) FROM transactions WHERE delete_at_height <= $1 AND hash = $2`, pruneHeight, []byte{1}).Scan(&matching))
			require.Zero(t, matching)
		})
	}
}

// TestPrunerClearedDAHIsRestampedOnLongestChain covers the repair path: a mined tx is marked unmined
// (DAH kept), the pruner clears the DAH, and marking it on the longest chain again must stamp a new
// DAH so the tx is still pruned eventually.
func TestPrunerClearedDAHIsRestampedOnLongestChain(t *testing.T) {
	ResetPrunerServiceForTests()
	t.Cleanup(ResetPrunerServiceForTests)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, _ := setup(ctx, t)
	store.settings.Pruner.UTXODefensiveEnabled = false

	provider, ok := any(store).(pruner.PrunerServiceProvider)
	require.True(t, ok)

	prunerSvc, err := provider.GetPrunerService()
	require.NoError(t, err)

	retention := store.settings.GetUtxoStoreBlockHeightRetention()
	require.NoError(t, store.SetBlockHeight(1000))

	// a mined tx, no unspent outputs, DAH due at 1010
	hash := chainhash.HashH([]byte("repair"))
	_, err = store.db.ExecContext(ctx, `
		INSERT INTO transactions (hash, version, lock_time, fee, size_in_bytes, conflicting, delete_at_height)
		VALUES ($1, 1, 0, 0, 100, false, 1010)`, hash[:])
	require.NoError(t, err)

	_, err = store.db.ExecContext(ctx, `
		INSERT INTO block_ids (transaction_id, block_id, block_height, subtree_idx)
		SELECT id, 1, 900, 0 FROM transactions WHERE hash = $1`, hash[:])
	require.NoError(t, err)

	dah := func() sql.NullInt64 {
		var v sql.NullInt64

		require.NoError(t, store.db.QueryRowContext(ctx, `SELECT delete_at_height FROM transactions WHERE hash = $1`, hash[:]).Scan(&v))

		return v
	}

	require.NoError(t, store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{hash}, false))
	require.True(t, dah().Valid, "marking unmined keeps the DAH")

	_, err = prunerSvc.Prune(ctx, 1010, "test")
	require.NoError(t, err)
	require.False(t, dah().Valid, "the pruner clears the DAH of the unmined tx")

	require.NoError(t, store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{hash}, true))

	got := dah()
	require.True(t, got.Valid, "marking on the longest chain must re-stamp the DAH")
	require.Equal(t, int64(1000)+int64(retention), got.Int64)

	deleted, err := prunerSvc.Prune(ctx, uint32(got.Int64), "test")
	require.NoError(t, err)
	require.Equal(t, int64(1), deleted)
}
