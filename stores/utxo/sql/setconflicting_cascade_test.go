package sql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
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

	_, _, err := store.SpendAndCreate(ctx, grandparent, 999, utxo.WithCreateOnly())
	require.NoError(t, err)

	parent := spendFrom(t, grandparent, 0)

	_, _, err = store.SpendAndCreate(ctx, parent, 1000, utxo.WithCreateOnly())
	require.NoError(t, err)

	child := spendFrom(t, parent, 0)

	_, _, err = store.SpendAndCreate(ctx, child, 1001, utxo.WithCreateOnly())
	require.NoError(t, err)

	// Spend parent's output 0 with child: this is the spender metadata
	// MarkConflictingRecursively walks to discover the cascade.
	_, _, err = store.SpendAndCreate(ctx, child, store.GetBlockHeight()+1, utxo.WithSpendOnly())
	require.NoError(t, err)

	parentHash := *parent.TxIDChainHash()

	_, markedHashes, err := utxo.MarkConflictingRecursively(ctx, store, []chainhash.Hash{parentHash})
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

func TestSetConflicting_ChildrenSurviveParentFlags(t *testing.T) {
	for _, sparse := range []bool{false, true} {
		name := "spent_and_frozen_outputs"
		if sparse {
			name = "sparse_outputs"
		}
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			store, tx := setup(ctx, t)
			defer func() { require.NoError(t, store.Close(ctx)) }()
			for _, input := range tx.Inputs {
				insertFakeTx(t, ctx, store, input.PreviousTxIDChainHash())
			}
			_, err := store.Create(ctx, tx, 0)
			require.NoError(t, err)
			child := chainhash.HashH([]byte("existing spending child"))
			spend := spendpkg.NewSpendingData(&child, 0)
			_, err = store.db.ExecContext(ctx, "UPDATE outputs SET spending_data=$1 WHERE transaction_id=(SELECT id FROM transactions WHERE hash=$2) AND idx=1", spend.Bytes(), tx.TxIDChainHash().CloneBytes())
			require.NoError(t, err)
			if sparse {
				_, err = store.db.ExecContext(ctx, "DELETE FROM outputs WHERE transaction_id=(SELECT id FROM transactions WHERE hash=$1) AND idx=0", tx.TxIDChainHash().CloneBytes())
			} else {
				_, err = store.db.ExecContext(ctx, "UPDATE outputs SET frozen=true WHERE transaction_id=(SELECT id FROM transactions WHERE hash=$1) AND idx=0", tx.TxIDChainHash().CloneBytes())
			}
			require.NoError(t, err)
			for _, flag := range []bool{true, false} {
				_, children, err := store.SetConflicting(ctx, []chainhash.Hash{*tx.TxIDChainHash()}, flag)
				require.NoError(t, err)
				require.Equal(t, []chainhash.Hash{child}, children, "stored child remains discoverable while parent is conflicting; frozen sentinel is not a child")
				data, err := store.Get(ctx, tx.TxIDChainHash(), fields.Conflicting)
				require.NoError(t, err)
				require.Equal(t, flag, data.Conflicting)
			}
			if !sparse {
				got, err := store.GetSpend(ctx, &utxo.Spend{TxID: tx.TxIDChainHash(), Vout: 0})
				require.NoError(t, err)
				require.Equal(t, int(utxo.Status_FROZEN), got.Status)
			}
		})
	}
}
