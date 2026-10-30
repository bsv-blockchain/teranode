package sql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// TestPrunerHoldsBackChildWhoseParentOutputIsUnspent: C is mined, buried and
// fully spent, so it is a pruning candidate. Its spend of P:0 was then rolled
// back, which leaves P:0 unspent. The marker INSERT joins to the output's
// spender, so it writes no marker for C there; deleting C anyway left a replay
// of C free to be recreated and spend P:0 cleanly. C must be held back, in both
// pruning modes. The Aerospike pruner applies the same rule.
//
// C also spends Q:0, which still records C's spend. The marker INSERT would
// mark (Q, C), and a marker on a child that stays refuses that live child's
// own re-spend, so a held-back child must get no marker on any parent.
func TestPrunerHoldsBackChildWhoseParentOutputIsUnspent(t *testing.T) {
	for _, defensive := range []bool{false, true} {
		forEachBackend(t, func(t *testing.T, ctx context.Context, store *Store) {
			store.settings.Pruner.UTXODefensiveEnabled = defensive

			newParent := func(seed string) *bt.Tx {
				parent := bt.NewTx()
				require.NoError(t, parent.From(seed, 0, "51", 30000))
				parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
				// Output 1 stays unspent so the parent survives the prune.
				require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
				require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
				_, err := store.Create(ctx, parent, 1000)
				require.NoError(t, err)

				return parent
			}

			parent := newParent("1111111111111111111111111111111111111111111111111111111111111111")
			sibling := newParent("2222222222222222222222222222222222222222222222222222222222222222")

			child := bt.NewTx()
			for _, p := range []*bt.Tx{parent, sibling} {
				require.NoError(t, child.From(p.TxID(), 0, p.Outputs[0].LockingScript.String(), p.Outputs[0].Satoshis))
			}

			for i := range child.Inputs {
				child.Inputs[i].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
			}

			require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 7000))
			_, _, err := store.SpendAndCreate(ctx, child, 1000)
			require.NoError(t, err)
			_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{parent.TxIDChainHash(), sibling.TxIDChainHash(), child.TxIDChainHash()},
				utxo.MinedBlockInfo{BlockID: 1000, BlockHeight: 1000, OnLongestChain: true})
			require.NoError(t, err)

			grandchild := bt.NewTx()
			require.NoError(t, grandchild.From(child.TxID(), 0, child.Outputs[0].LockingScript.String(), child.Outputs[0].Satoshis))
			grandchild.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
			require.NoError(t, grandchild.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 2000))
			_, _, err = store.SpendAndCreate(ctx, grandchild, 1001)
			require.NoError(t, err)
			_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{grandchild.TxIDChainHash()},
				utxo.MinedBlockInfo{BlockID: 1001, BlockHeight: 1001, OnLongestChain: true})
			require.NoError(t, err)

			// Roll C's spend of P:0 back.
			utxoHash, err := util.UTXOHashFromOutput(parent.TxIDChainHash(), parent.Outputs[0], 0)
			require.NoError(t, err)
			require.NoError(t, store.Unspend(ctx, []*utxo.Spend{{
				TxID: parent.TxIDChainHash(), Vout: 0, UTXOHash: utxoHash,
				SpendingData: spendpkg.NewSpendingData(child.TxIDChainHash(), 0),
			}}))

			svc, err := store.GetPrunerService()
			require.NoError(t, err)

			heldBefore := heldBackCounter(t)
			_, err = svc.Prune(ctx, 1300, "unverified-claim")
			require.NoError(t, err)
			require.Equal(t, float64(1), heldBackCounter(t)-heldBefore,
				"the held-back child must be counted, or it cannot be told apart from nothing to prune (defensive=%v)", defensive)

			var childExists, marked bool
			require.NoError(t, store.db.QueryRowContext(ctx,
				"SELECT EXISTS(SELECT 1 FROM transactions WHERE hash = $1)", child.TxIDChainHash()[:]).Scan(&childExists))
			require.True(t, childExists, "C must be held back while P:0 does not record its spend (defensive=%v)", defensive)

			require.NoError(t, store.db.QueryRowContext(ctx,
				"SELECT EXISTS(SELECT 1 FROM deleted_children WHERE child_hash = $1)", child.TxIDChainHash()[:]).Scan(&marked))
			require.False(t, marked, "a held-back child carries no marker on any parent, including the sibling that still records its spend")
		})
	}
}

// heldBackCounter reads utxo_sql_pruner_children_held_back_total from the
// default registry, where the SQL pruner registers it.
func heldBackCounter(t *testing.T) float64 {
	t.Helper()

	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	for _, family := range families {
		if family.GetName() == "utxo_sql_pruner_children_held_back_total" {
			return family.GetMetric()[0].GetCounter().GetValue()
		}
	}

	t.Fatal("utxo_sql_pruner_children_held_back_total is not registered")

	return 0
}

// TestPrunerDeletesConflictingLoserWithReleasedUncontestedInput: X is an
// unmined transaction spending P:0 and Q:0. Block winner W spends P:0 only.
// ProcessConflicting marks X conflicting (which gives it a delete_at_height)
// and unspends every input of X, so Q:0 goes back to NULL although nobody
// contested it. X is a conflicting loser and nothing else: its released spends
// were released by design, and a replay of X meets W's spend of P:0. Once X's
// delete_at_height is reached the pruner must delete it, as upstream does.
func TestPrunerDeletesConflictingLoserWithReleasedUncontestedInput(t *testing.T) {
	for _, defensive := range []bool{false, true} {
		forEachBackend(t, func(t *testing.T, ctx context.Context, store *Store) {
			store.settings.Pruner.UTXODefensiveEnabled = defensive

			newParent := func(seed string) *bt.Tx {
				parent := bt.NewTx()
				require.NoError(t, parent.From(seed, 0, "51", 30000))
				parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
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

			// X: unmined, spends P:0 and Q:0.
			loser := bt.NewTx()
			for _, p := range []*bt.Tx{parentP, parentQ} {
				require.NoError(t, loser.From(p.TxID(), 0, p.Outputs[0].LockingScript.String(), p.Outputs[0].Satoshis))
			}

			for i := range loser.Inputs {
				loser.Inputs[i].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
			}

			require.NoError(t, loser.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 7000))
			_, _, err = store.SpendAndCreate(ctx, loser, 1000)
			require.NoError(t, err)

			// W: the block's winner, spends P:0 only, arrives as conflicting.
			winner := bt.NewTx()
			require.NoError(t, winner.From(parentP.TxID(), 0, parentP.Outputs[0].LockingScript.String(), parentP.Outputs[0].Satoshis))
			winner.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
			require.NoError(t, winner.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3000))
			_, _, err = store.SpendAndCreate(ctx, winner, 1000, utxo.WithConflicting(true), utxo.WithCreateOnly())
			require.NoError(t, err)

			_, _, err = utxo.ProcessConflicting(ctx, store, 1001, chainhash.Hash{}, []chainhash.Hash{*winner.TxIDChainHash()},
				map[chainhash.Hash]struct{}{}, utxo.NoAncestryGuard)
			require.NoError(t, err)

			_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{winner.TxIDChainHash()},
				utxo.MinedBlockInfo{BlockID: 1001, BlockHeight: 1001, OnLongestChain: true})
			require.NoError(t, err)

			// Preconditions: X conflicting with a DAH, P:0 -> W, Q:0 released.
			var conflicting bool

			var dah *int64

			require.NoError(t, store.db.QueryRowContext(ctx,
				"SELECT conflicting, delete_at_height FROM transactions WHERE hash = $1", loser.TxIDChainHash()[:]).Scan(&conflicting, &dah))
			require.True(t, conflicting, "X must be a conflicting loser")
			require.NotNil(t, dah, "SetConflicting must give X a delete_at_height")
			require.Equal(t, winner.TxIDChainHash()[:], outputSpendingData(t, ctx, store, parentP, 0)[:32], "P:0 must record W")
			require.Nil(t, outputSpendingData(t, ctx, store, parentQ, 0), "Q:0 must be released by step 2")

			svc, err := store.GetPrunerService()
			require.NoError(t, err)

			heldBefore := heldBackCounter(t)

			for _, h := range []uint32{5000, 6000, 7000} {
				_, err = svc.Prune(ctx, h, "conflicting-loser")
				require.NoError(t, err)
			}

			var loserExists bool
			require.NoError(t, store.db.QueryRowContext(ctx,
				"SELECT EXISTS(SELECT 1 FROM transactions WHERE hash = $1)", loser.TxIDChainHash()[:]).Scan(&loserExists))
			require.False(t, loserExists,
				"a conflicting loser past its delete_at_height (%d) must be pruned even though its uncontested input Q:0 was released (defensive=%v, held-back counter rose by %v)",
				*dah, defensive, heldBackCounter(t)-heldBefore)
		})
	}
}
