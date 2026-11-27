package aerospike_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	astore "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	apruner "github.com/bsv-blockchain/teranode/stores/utxo/aerospike/pruner"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/stretchr/testify/require"
)

// parentKeyForOutput returns the Aerospike key of the record that holds a given
// output of a transaction: the master record for the first page, a pagination
// record beyond it.
func parentKeyForOutput(t *testing.T, store *astore.Store, txID *chainhash.Hash, vout uint32, batchSize int) *aerospike.Key {
	t.Helper()

	key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), uaerospike.CalculateKeySource(txID, vout, batchSize))
	require.NoError(t, err)

	return key
}

// TestSpendDoesNotBlessWithRequireParentOutputs: "parent not found, but the
// spending transaction exists" is treated as a transaction validated before its
// parent was pruned, and the error is cleared. Not with RequireParentOutputs,
// the second wave of a below-checkpoint list, whose first wave may have written
// the record itself.
func TestSpendDoesNotBlessWithRequireParentOutputs(t *testing.T) {
	t.Parallel()

	logger := ulogger.New("spend-bless-test")
	s := test.CreateBaseTestSettings(t)
	s.Aerospike.EnableSpendFilterExpressions = true

	_, store, ctx, cleanup := initAerospike(t, s, logger)
	t.Cleanup(cleanup)
	require.NoError(t, store.SetBlockHeight(1000))

	parent := uniqueTx(bt.NewTx())
	require.NoError(t, parent.From("1111111111111111111111111111111111111111111111111111111111111111", 0, "51", 30000))
	require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
	_, err := store.Create(ctx, parent, 1000)
	require.NoError(t, err)

	child := bt.NewTx()
	require.NoError(t, child.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
	require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3000))
	_, _, err = store.SpendAndCreate(ctx, child, 1000)
	require.NoError(t, err)
	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{parent.TxIDChainHash(), child.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: 1000, BlockHeight: 1000, OnLongestChain: true})
	require.NoError(t, err)

	require.NoError(t, store.DeleteComplete(ctx, parent.TxIDChainHash()))

	_, err = store.Spend(ctx, child, 1200)
	require.NoError(t, err, "control: a pre-existing child with a pruned parent is blessed")

	_, err = store.Spend(ctx, child, 1200, utxo.IgnoreFlags{RequireParentOutputs: true})
	require.ErrorIs(t, err, errors.ErrTxNotFound, "with RequireParentOutputs the record is not proof of prior validation")
}

// TestPrunerDeletesConflictingLoserWithReleasedUncontestedInput is the
// Aerospike twin of the SQL test of the same name. X spends P:0 and Q:0, and
// the block's winner W spends P:0 only. ProcessConflicting marks X conflicting
// and releases every input it spent, so Q:0 is unspent although nobody
// contested it. An unspent output a child names normally holds the child back,
// since that is what a rolled-back spend leaves; for a conflicting loser it
// would hold it back forever. A replay of X meets W's spend, so X is pruned.
func TestPrunerDeletesConflictingLoserWithReleasedUncontestedInput(t *testing.T) {
	t.Parallel()

	logger := ulogger.New("pruner-conflicting-loser-test")
	s := test.CreateBaseTestSettings(t)
	s.UtxoStore.DisableDAHCleaner = false
	s.Pruner.UTXODefensiveEnabled = false
	s.Aerospike.EnableSpendFilterExpressions = true

	client, store, ctx, cleanup := initAerospikeOwnServer(t, s, logger)
	t.Cleanup(cleanup)
	require.NoError(t, store.SetBlockHeight(1000))

	newParent := func(seed string) *bt.Tx {
		parent := bt.NewTx()
		require.NoError(t, parent.From(seed, 0, "51", 30000))
		require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
		require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
		_, err := store.Create(ctx, parent, 1000)
		require.NoError(t, err)

		return parent
	}

	parentP := newParent("3333333333333333333333333333333333333333333333333333333333333333")
	parentQ := newParent("4444444444444444444444444444444444444444444444444444444444444444")
	_, err := store.SetMinedMulti(ctx, []*chainhash.Hash{parentP.TxIDChainHash(), parentQ.TxIDChainHash()},
		utxo.MinedBlockInfo{BlockID: 1000, BlockHeight: 1000, OnLongestChain: true})
	require.NoError(t, err)

	loser := bt.NewTx()
	for _, parent := range []*bt.Tx{parentP, parentQ} {
		require.NoError(t, loser.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
	}

	require.NoError(t, loser.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 7000))
	_, _, err = store.SpendAndCreate(ctx, loser, 1000)
	require.NoError(t, err)

	winner := bt.NewTx()
	require.NoError(t, winner.From(parentP.TxID(), 0, parentP.Outputs[0].LockingScript.String(), parentP.Outputs[0].Satoshis))
	require.NoError(t, winner.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3000))
	_, _, err = store.SpendAndCreate(ctx, winner, 1000, utxo.WithConflicting(true), utxo.WithCreateOnly())
	require.NoError(t, err)

	_, _, err = utxo.ProcessConflicting(ctx, store, 1001, chainhash.Hash{}, []chainhash.Hash{*winner.TxIDChainHash()},
		map[chainhash.Hash]struct{}{}, utxo.NoAncestryGuard)
	require.NoError(t, err)

	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{winner.TxIDChainHash()},
		utxo.MinedBlockInfo{BlockID: 1001, BlockHeight: 1001, OnLongestChain: true})
	require.NoError(t, err)

	loserKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), loser.TxIDChainHash().CloneBytes())
	require.NoError(t, err)

	loserRecord, err := client.Get(nil, loserKey, fields.Conflicting.String(), fields.DeleteAtHeight.String())
	require.NoError(t, err)
	require.Equal(t, true, loserRecord.Bins[fields.Conflicting.String()], "fixture: X is a conflicting loser")
	require.NotNil(t, loserRecord.Bins[fields.DeleteAtHeight.String()], "fixture: SetConflicting gave X a delete-at height")

	qHash, err := util.UTXOHashFromOutput(parentQ.TxIDChainHash(), parentQ.Outputs[0], 0)
	require.NoError(t, err)
	qSpend, err := store.GetSpend(ctx, &utxo.Spend{TxID: parentQ.TxIDChainHash(), Vout: 0, UTXOHash: qHash})
	require.NoError(t, err)
	require.Nil(t, qSpend.SpendingData, "fixture: ProcessConflicting released the uncontested Q:0")

	astore.UsePrunerServiceForTest(t)
	require.NoError(t, store.CreateIndexIfNotExists(ctx, apruner.IndexName, fields.DeleteAtHeight.String(), aerospike.NUMERIC))
	require.NoError(t, store.WaitForIndexReady(ctx, apruner.IndexName))

	svc, err := store.GetPrunerService()
	require.NoError(t, err)

	for _, height := range []uint32{5000, 6000} {
		_, err = svc.(*apruner.Service).PruneWithPartitions(ctx, height, "conflicting-loser-released", 1)
		require.NoError(t, err)
	}

	exists, err := client.Exists(nil, loserKey)
	require.NoError(t, err)
	require.False(t, exists, "a conflicting loser past its delete-at height is pruned although its uncontested input was released")
}

// rollbackFixtureParent creates a parent with the given number of outputs; tag
// keeps its txid apart from other parents in the same test.
func rollbackFixtureParent(t *testing.T, ctx context.Context, store *astore.Store, tag byte, outputs int) *bt.Tx {
	t.Helper()

	p := bt.NewTx()
	require.NoError(t, p.From(fmt.Sprintf("%064x", []byte{tag}), 0, "51", 30000))

	for i := 0; i < outputs; i++ {
		require.NoError(t, p.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
	}

	_, err := store.Create(ctx, p, 1000)
	require.NoError(t, err)

	return p
}

// spendOf is the spend of parent's output vout by child's input vin.
func spendOf(t *testing.T, parent *bt.Tx, vout uint32, child *bt.Tx, vin int) *utxo.Spend {
	t.Helper()

	hash, err := util.UTXOHashFromOutput(parent.TxIDChainHash(), parent.Outputs[vout], vout)
	require.NoError(t, err)

	return &utxo.Spend{TxID: parent.TxIDChainHash(), Vout: vout, UTXOHash: hash, SpendingData: spendpkg.NewSpendingData(child.TxIDChainHash(), vin)}
}

// TestRollbackOfARequireParentOutputsCall: the second wave of
// DefaultSpendAndCreateMulti spends with RequireParentOutputs, so the
// spender-existence probe would always find the first wave's record. That call
// decides by error class instead: a validation failure reverses what it
// wrote, and an infrastructure failure leaves it for the retry to match.
func TestRollbackOfARequireParentOutputsCall(t *testing.T) {
	t.Parallel()

	s := test.CreateBaseTestSettings(t)

	_, store, ctx, cleanup := initAerospike(t, s, ulogger.New("require-parent-outputs-rollback-test"))
	t.Cleanup(cleanup)
	require.NoError(t, store.SetBlockHeight(1000))

	for i, tc := range []struct {
		name           string
		rollbackNeeded bool
		wantReversed   bool
	}{
		{name: "validation failure reverses the write", rollbackNeeded: true, wantReversed: true},
		{name: "infrastructure failure keeps it for the retry"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := rollbackFixtureParent(t, ctx, store, byte(0x61+i), 1)

			c := bt.NewTx()
			require.NoError(t, c.From(p.TxID(), 0, p.Outputs[0].LockingScript.String(), p.Outputs[0].Satoshis))
			require.NoError(t, c.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3000))
			_, _, err := store.SpendAndCreate(ctx, c, 1000)
			require.NoError(t, err)

			written := spendOf(t, p, 0, c, 0)

			store.RollbackRequiredParentSpendsForTest(c, []*utxo.Spend{written}, tc.rollbackNeeded)

			after, err := store.GetSpend(ctx, written)
			require.NoError(t, err)

			if tc.wantReversed {
				require.Nil(t, after.SpendingData, "the call's own write is reversed")
				return
			}

			require.NotNil(t, after.SpendingData, "the write stays for the retry to match")
			require.Equal(t, *c.TxIDChainHash(), *after.SpendingData.TxID)
		})
	}
}

// TestBlessedInputIsNotAWrittenSpend: an input whose parent is gone, of a
// transaction the store already holds, is blessed: it counts as succeeded but
// wrote nothing, so a later rollback must not try to reverse it.
func TestBlessedInputIsNotAWrittenSpend(t *testing.T) {
	t.Parallel()

	s := test.CreateBaseTestSettings(t)

	_, store, ctx, cleanup := initAerospike(t, s, ulogger.New("blessed-input-test"))
	t.Cleanup(cleanup)
	require.NoError(t, store.SetBlockHeight(1000))

	p := rollbackFixtureParent(t, ctx, store, 0x71, 1)
	goneParent := chainhash.HashH([]byte("pruned parent"))

	c := bt.NewTx()
	require.NoError(t, c.From(goneParent.String(), 0, "51", 5000))
	require.NoError(t, c.From(p.TxID(), 0, p.Outputs[0].LockingScript.String(), p.Outputs[0].Satoshis))
	require.NoError(t, c.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3000))

	_, err := store.Create(ctx, c, 1000)
	require.NoError(t, err, "fixture: the store already holds C")

	blessed := &utxo.Spend{TxID: &goneParent, Vout: 0, SpendingData: spendpkg.NewSpendingData(c.TxIDChainHash(), 0), Err: errors.NewTxNotFoundError("parent pruned")}
	written := spendOf(t, p, 0, c, 1)

	spent, succeeded := store.ResolveSpendCompletionsForTest(ctx, c, []*utxo.Spend{blessed, written})
	require.Equal(t, 2, succeeded, "the blessed input counts as succeeded")
	require.NoError(t, blessed.Err, "its error is cleared")
	require.Equal(t, []*utxo.Spend{written}, spent, "but it wrote nothing, so it is not a spend to reverse")
}
