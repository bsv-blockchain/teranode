package sql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/stretchr/testify/require"
)

// Sibling coverage for the SetConflicting cascade lives in:
//   - stores/utxo/setconflicting_cascade_bug_test.go (mock-based, proves the cascade)
//   - stores/utxo/aerospike/setconflicting_cascade_test.go (Aerospike TestContainer)
//
// SQLite used to be untestable here. SetConflicting opened its write transaction
// with s.db.Begin() and then called s.Get / s.GetSpend on the store's connection
// POOL rather than on the open transaction, and in SQLite's shared-cache mode the
// writer's lock blocks every other connection's reads — so the call deadlocked
// against itself and the test could only be described, not run.
//
// That shape was removed in the same commit as this test: every read is now taken
// before the transaction opens, and the transaction body does only writes (see the
// comment on SetConflicting). The test below is the check that it stays removed —
// on the pre-fix code it does not fail, it hangs.
func TestSetConflictingCascade_SQLite(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, _ := setup(ctx, t)

	spendFrom := func(t *testing.T, parent *bt.Tx, vout uint32) *bt.Tx {
		t.Helper()

		child := bt.NewTx()
		require.NoError(t, child.From(
			parent.TxIDChainHash().String(), vout,
			parent.Outputs[vout].LockingScript.String(),
			parent.Outputs[vout].Satoshis,
		))
		require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", parent.Outputs[vout].Satoshis/3))

		// The store's inputs table requires an unlocking script; nothing here is
		// script-verified, so any non-null script will do.
		child.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{bscript.OpTRUE})

		return child
	}

	// A three-generation chain, because updateParentConflictingChildren resolves
	// the parent of every transaction it marks: grandparent -> parent -> child.
	// Only parent and child are marked, so the grandparent needs no ancestor of
	// its own.
	grandparent := tests.ParentTx

	_, _, err := store.SpendAndCreate(ctx, grandparent, 999, utxostore.WithCreateOnly())
	require.NoError(t, err)

	parent := spendFrom(t, grandparent, 0)

	_, _, err = store.SpendAndCreate(ctx, parent, 1000, utxostore.WithCreateOnly())
	require.NoError(t, err)

	child := spendFrom(t, parent, 0)

	_, _, err = store.SpendAndCreate(ctx, child, 1001, utxostore.WithCreateOnly())
	require.NoError(t, err)

	// Spend parent's output 0 with child: this is the spender metadata
	// MarkConflictingRecursively walks to discover the cascade.
	_, _, err = store.SpendAndCreate(ctx, child, store.GetBlockHeight()+1, utxostore.WithSpendOnly())
	require.NoError(t, err)

	parentHash := *parent.TxIDChainHash()

	_, markedHashes, err := utxostore.MarkConflictingRecursively(ctx, store, []chainhash.Hash{parentHash})
	require.NoError(t, err)
	require.Equal(t, []chainhash.Hash{parentHash, *child.TxIDChainHash()}, markedHashes,
		"marked set must be returned in BFS order: parent first, then cascaded child")

	parentMeta, err := store.Get(ctx, parent.TxIDChainHash(), fields.Conflicting)
	require.NoError(t, err)
	require.True(t, parentMeta.Conflicting, "parent must be conflicting")

	childMeta, err := store.Get(ctx, child.TxIDChainHash(), fields.Conflicting)
	require.NoError(t, err)
	require.True(t, childMeta.Conflicting,
		"child must be cascaded to conflicting when parent is marked conflicting")
}
