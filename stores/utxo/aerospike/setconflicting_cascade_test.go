package aerospike_test

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestSetConflictingCascade_Aerospike verifies that MarkConflictingRecursively
// cascades the conflicting flag to all spending descendants against real Aerospike.
func TestSetConflictingCascade_Aerospike(t *testing.T) {
	t.Parallel()

	fx := newTxFixtures(t)

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	_, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)

	buildChildTx := func(t *testing.T, parentTx *bt.Tx, vout uint32) *bt.Tx {
		t.Helper()
		child := bt.NewTx()
		require.NoError(t, child.From(
			parentTx.TxIDChainHash().String(), vout,
			parentTx.Outputs[vout].LockingScript.String(),
			parentTx.Outputs[vout].Satoshis,
		))
		require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa",
			parentTx.Outputs[vout].Satoshis/3))
		return child
	}

	t.Run("MarkConflictingRecursively cascades to child", func(t *testing.T) {
		*fx = *newTxFixtures(t)

		// tests.ParentTx is the grandparent — needed because tx's input references it
		// and SetConflicting calls updateParentConflictingChildren internally.
		// A copy of tests.ParentTx with a txid of its own, as tx's parent: the server is shared with
		// other tests.
		grandparent := uniqueTx(tests.ParentTx.Clone())
		fx.tx = repointInputs(t, fx.tx, tests.ParentTx, grandparent)

		_, err := store.Create(ctx, grandparent, 999)
		require.NoError(t, err)

		_, err = store.Create(ctx, fx.tx, 1000)
		require.NoError(t, err)

		childTx := buildChildTx(t, fx.tx, 0)
		_, err = store.Create(ctx, childTx, 1001)
		require.NoError(t, err)

		// Spend tx's output 0 with childTx — sets spender metadata
		// that SetConflicting uses to discover children.
		_, err = store.Spend(ctx, childTx, store.GetBlockHeight()+1)
		require.NoError(t, err)

		parentHash := *fx.tx.TxIDChainHash()
		_, markedHashes, err := utxo.MarkConflictingRecursively(ctx, store, []chainhash.Hash{parentHash})
		require.NoError(t, err)
		require.Equal(t, []chainhash.Hash{parentHash, *childTx.TxIDChainHash()}, markedHashes,
			"marked set must be returned in BFS order: parent first, then cascaded child")

		parentMeta, err := store.Get(ctx, fx.tx.TxIDChainHash(), fields.Conflicting)
		require.NoError(t, err)
		require.True(t, parentMeta.Conflicting, "parent must be conflicting")

		childMeta, err := store.Get(ctx, childTx.TxIDChainHash(), fields.Conflicting)
		require.NoError(t, err)
		require.True(t, childMeta.Conflicting,
			"child must be cascaded to conflicting when parent is marked conflicting")
	})

	t.Run("three-level chain cascades to all descendants", func(t *testing.T) {
		*fx = *newTxFixtures(t)

		// A copy of tests.ParentTx with a txid of its own, as tx's parent: the server is shared with
		// other tests.
		grandparent := uniqueTx(tests.ParentTx.Clone())
		fx.tx = repointInputs(t, fx.tx, tests.ParentTx, grandparent)

		_, err := store.Create(ctx, grandparent, 999)
		require.NoError(t, err)

		_, err = store.Create(ctx, fx.tx, 1000)
		require.NoError(t, err)

		childTx := buildChildTx(t, fx.tx, 0)
		_, err = store.Create(ctx, childTx, 1001)
		require.NoError(t, err)

		_, err = store.Spend(ctx, childTx, store.GetBlockHeight()+1)
		require.NoError(t, err)

		grandchildTx := buildChildTx(t, childTx, 0)
		_, err = store.Create(ctx, grandchildTx, 1002)
		require.NoError(t, err)

		_, err = store.Spend(ctx, grandchildTx, store.GetBlockHeight()+1)
		require.NoError(t, err)

		parentHash := *fx.tx.TxIDChainHash()
		_, markedHashes, err := utxo.MarkConflictingRecursively(ctx, store, []chainhash.Hash{parentHash})
		require.NoError(t, err)
		require.Equal(t,
			[]chainhash.Hash{parentHash, *childTx.TxIDChainHash(), *grandchildTx.TxIDChainHash()},
			markedHashes,
			"marked set must be returned in BFS order: parent, child, grandchild")

		parentMeta, err := store.Get(ctx, fx.tx.TxIDChainHash(), fields.Conflicting)
		require.NoError(t, err)
		require.True(t, parentMeta.Conflicting)

		childMeta, err := store.Get(ctx, childTx.TxIDChainHash(), fields.Conflicting)
		require.NoError(t, err)
		require.True(t, childMeta.Conflicting, "child must be cascaded to conflicting")

		gcMeta, err := store.Get(ctx, grandchildTx.TxIDChainHash(), fields.Conflicting)
		require.NoError(t, err)
		require.True(t, gcMeta.Conflicting, "grandchild must be cascaded to conflicting via BFS")
	})
}

// repointInputs returns a copy of tx whose inputs that spend from's outputs spend to's instead.
func repointInputs(t *testing.T, tx, from, to *bt.Tx) *bt.Tx {
	t.Helper()

	repointed := tx.Clone()
	for _, in := range repointed.Inputs {
		if in.PreviousTxIDChainHash().IsEqual(from.TxIDChainHash()) {
			require.NoError(t, in.PreviousTxIDAdd(to.TxIDChainHash()))
		}
	}

	return repointed
}
