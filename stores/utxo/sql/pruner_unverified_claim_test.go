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
