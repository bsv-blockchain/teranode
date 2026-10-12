package sql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSQLRemoveFromConflictingChildren verifies that we can trim a single child
// hash from a parent's conflicting_children list without disturbing unrelated
// entries, and that missing rows are tolerated.
func TestSQLRemoveFromConflictingChildren(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("nil hashes error", func(t *testing.T) {
		store, _ := setup(ctx, t)
		h := chainhash.Hash{}
		require.Error(t, store.RemoveFromConflictingChildren(ctx, []utxo.ConflictingChildRemoval{{ParentHash: nil, ChildHash: &h}}))
		require.Error(t, store.RemoveFromConflictingChildren(ctx, []utxo.ConflictingChildRemoval{{ParentHash: &h, ChildHash: nil}}))
	})

	t.Run("empty slice is a no-op", func(t *testing.T) {
		store, _ := setup(ctx, t)
		require.NoError(t, store.RemoveFromConflictingChildren(ctx, nil))
		require.NoError(t, store.RemoveFromConflictingChildren(ctx, []utxo.ConflictingChildRemoval{}))
	})

	t.Run("unknown parent or child is a no-op", func(t *testing.T) {
		store, _ := setup(ctx, t)
		missing := chainhash.Hash{0x01, 0x02, 0x03}
		other := chainhash.Hash{0x04, 0x05, 0x06}
		require.NoError(t, store.RemoveFromConflictingChildren(ctx, []utxo.ConflictingChildRemoval{{ParentHash: &missing, ChildHash: &other}}))
	})

	t.Run("removes only the specified child", func(t *testing.T) {
		store, parentTx := setup(ctx, t)

		_, err := store.Create(ctx, parentTx, 0)
		require.NoError(t, err)

		parentID := mustLookupTxID(t, ctx, store, parentTx.TxIDChainHash())

		childA := chainhash.Hash{0xAA}
		childB := chainhash.Hash{0xBB}

		// Pre-insert two transactions to satisfy the FK on child_transaction_id.
		childAID := insertFakeTx(t, ctx, store, &childA)
		childBID := insertFakeTx(t, ctx, store, &childB)

		// Insert two conflicting children.
		_, err = store.db.ExecContext(ctx,
			`INSERT INTO conflicting_children (transaction_id, child_transaction_id) VALUES ($1, $2)`,
			parentID, childAID)
		require.NoError(t, err)
		_, err = store.db.ExecContext(ctx,
			`INSERT INTO conflicting_children (transaction_id, child_transaction_id) VALUES ($1, $2)`,
			parentID, childBID)
		require.NoError(t, err)

		// Remove only childA.
		require.NoError(t, store.RemoveFromConflictingChildren(ctx, []utxo.ConflictingChildRemoval{
			{ParentHash: parentTx.TxIDChainHash(), ChildHash: &childA},
		}))

		// Child B should still be there.
		var count int
		err = store.db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM conflicting_children WHERE transaction_id = $1`,
			parentID).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count)

		// Calling again is idempotent.
		require.NoError(t, store.RemoveFromConflictingChildren(ctx, []utxo.ConflictingChildRemoval{
			{ParentHash: parentTx.TxIDChainHash(), ChildHash: &childA},
		}))
	})

	t.Run("batched removal across many pairs", func(t *testing.T) {
		store, parentTx := setup(ctx, t)

		_, err := store.Create(ctx, parentTx, 0)
		require.NoError(t, err)

		parentID := mustLookupTxID(t, ctx, store, parentTx.TxIDChainHash())

		childA := chainhash.Hash{0xAA}
		childB := chainhash.Hash{0xBB}
		childC := chainhash.Hash{0xCC}

		childAID := insertFakeTx(t, ctx, store, &childA)
		childBID := insertFakeTx(t, ctx, store, &childB)
		childCID := insertFakeTx(t, ctx, store, &childC)

		for _, cid := range []int{childAID, childBID, childCID} {
			_, err = store.db.ExecContext(ctx,
				`INSERT INTO conflicting_children (transaction_id, child_transaction_id) VALUES ($1, $2)`,
				parentID, cid)
			require.NoError(t, err)
		}

		// Remove A and C in one batched call.
		require.NoError(t, store.RemoveFromConflictingChildren(ctx, []utxo.ConflictingChildRemoval{
			{ParentHash: parentTx.TxIDChainHash(), ChildHash: &childA},
			{ParentHash: parentTx.TxIDChainHash(), ChildHash: &childC},
		}))

		var count int
		err = store.db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM conflicting_children WHERE transaction_id = $1`,
			parentID).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count)
	})
}

// TestSQLRemoveBlockIDs verifies the block_ids trim helper.
func TestSQLRemoveBlockIDs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("nil hash errors", func(t *testing.T) {
		store, _ := setup(ctx, t)
		require.Error(t, store.RemoveBlockIDs(ctx, []utxo.BlockIDsRemoval{{TxHash: nil, BlockIDs: []uint32{1}}}))
	})

	t.Run("empty slice no-op", func(t *testing.T) {
		store, _ := setup(ctx, t)
		h := chainhash.Hash{0x01}
		require.NoError(t, store.RemoveBlockIDs(ctx, nil))
		require.NoError(t, store.RemoveBlockIDs(ctx, []utxo.BlockIDsRemoval{}))
		require.NoError(t, store.RemoveBlockIDs(ctx, []utxo.BlockIDsRemoval{{TxHash: &h, BlockIDs: nil}}))
	})

	t.Run("missing tx is tolerated", func(t *testing.T) {
		store, _ := setup(ctx, t)
		missing := chainhash.Hash{0xDE, 0xAD}
		require.NoError(t, store.RemoveBlockIDs(ctx, []utxo.BlockIDsRemoval{{TxHash: &missing, BlockIDs: []uint32{1, 2, 3}}}))
	})

	t.Run("removes only specified IDs", func(t *testing.T) {
		store, tx := setup(ctx, t)

		_, err := store.Create(ctx, tx, 0,
			utxo.WithMinedBlockInfo(
				utxo.MinedBlockInfo{BlockID: 10, BlockHeight: 1, SubtreeIdx: 0},
				utxo.MinedBlockInfo{BlockID: 20, BlockHeight: 2, SubtreeIdx: 0},
				utxo.MinedBlockInfo{BlockID: 30, BlockHeight: 3, SubtreeIdx: 0},
			))
		require.NoError(t, err)

		txID := mustLookupTxID(t, ctx, store, tx.TxIDChainHash())

		// Remove block IDs 10 and 30 for this tx.
		require.NoError(t, store.RemoveBlockIDs(ctx, []utxo.BlockIDsRemoval{{TxHash: tx.TxIDChainHash(), BlockIDs: []uint32{10, 30}}}))

		rows, err := store.db.QueryContext(ctx,
			`SELECT block_id FROM block_ids WHERE transaction_id = $1 ORDER BY block_id`, txID)
		require.NoError(t, err)
		defer rows.Close()

		var remaining []uint32
		for rows.Next() {
			var id uint32
			require.NoError(t, rows.Scan(&id))
			remaining = append(remaining, id)
		}
		assert.Equal(t, []uint32{20}, remaining)
	})
}

// insertFakeTx is a helper that inserts a minimal row into the transactions
// table so conflicting_children FKs can be satisfied in tests.
func insertFakeTx(t *testing.T, ctx context.Context, store *Store, hash *chainhash.Hash) int {
	t.Helper()
	var id int
	err := store.db.QueryRowContext(ctx, `
		INSERT INTO transactions (hash, fee, size_in_bytes, version, lock_time, coinbase)
		VALUES ($1, 0, 0, 1, 0, false)
		RETURNING id`, hash[:]).Scan(&id)
	require.NoError(t, err)
	return id
}

func mustLookupTxID(t *testing.T, ctx context.Context, store *Store, hash *chainhash.Hash) int {
	t.Helper()
	var id int
	err := store.db.QueryRowContext(ctx, `SELECT id FROM transactions WHERE hash = $1`, hash[:]).Scan(&id)
	require.NoError(t, err)
	return id
}
