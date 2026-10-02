package aerospike_test

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestMissingParentRollsBackFreshSiblingSpend: a ghost C spends S:0, whose
// record survives, and G:0, whose record is gone. The spend answers G with
// ErrTxNotFound, which the block paths read as a pruned replay and compensate
// by deleting C's record. DeleteCreated never reverses a ghost's spends, so the
// store must roll back its fresh spend of S:0 in the same call; before the
// missing-parent answer was a rollback error, S:0 was left spent by a
// transaction the store no longer held.
//
// The other half: when S:0 already recorded C's spend (C spent both, then G was
// pruned), that is the confirmed, historical spend and the rollback must leave
// it in place.
func TestMissingParentRollsBackFreshSiblingSpend(t *testing.T) {
	for _, expressions := range []bool{false, true} {
		name := "udf"
		if expressions {
			name = "expressions"
		}

		t.Run(name, func(t *testing.T) {
			s := test.CreateBaseTestSettings(t)
			s.Aerospike.EnableSpendFilterExpressions = expressions

			if expressions {
				// useExpressionSpend requires one utxo per record.
				s.UtxoStore.UtxoBatchSize = 1
			}

			_, store, ctx, cleanup := initAerospike(t, s, ulogger.New("missing-parent-rollback"))
			t.Cleanup(cleanup)
			require.NoError(t, store.SetBlockHeight(1000))

			newParent := func(seed string) *bt.Tx {
				parent := bt.NewTx()
				require.NoError(t, parent.From(seed, 0, "51", 30000))
				require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))

				return parent
			}

			spendOf := func(parents ...*bt.Tx) *bt.Tx {
				tx := bt.NewTx()
				for _, parent := range parents {
					require.NoError(t, tx.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
				}

				require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3000))

				return tx
			}

			spendOfS0 := func(parent *bt.Tx) *utxo.Spend {
				hash, err := util.UTXOHashFromOutput(parent.TxIDChainHash(), parent.Outputs[0], 0)
				require.NoError(t, err)

				return &utxo.Spend{TxID: parent.TxIDChainHash(), Vout: 0, UTXOHash: hash}
			}

			// Fresh: the survivor's output is unspent before the replay.
			survivor := newParent("1111111111111111111111111111111111111111111111111111111111111111")
			gone := newParent("2222222222222222222222222222222222222222222222222222222222222222")
			_, err := store.Create(ctx, survivor, 1000)
			require.NoError(t, err)

			ghost := spendOf(survivor, gone)
			_, err = store.Spend(ctx, ghost, 1200, utxo.IgnoreFlags{SpenderCreatedByCaller: true})
			require.ErrorIs(t, err, errors.ErrTxNotFound, "control: the missing parent answers not found")

			after, err := store.GetSpend(ctx, spendOfS0(survivor))
			require.NoError(t, err)
			require.Nil(t, after.SpendingData, "the fresh spend of the surviving parent must be rolled back")

			// Historical: C spent both outputs, then G's record went.
			survivor2 := newParent("3333333333333333333333333333333333333333333333333333333333333333")
			gone2 := newParent("4444444444444444444444444444444444444444444444444444444444444444")

			for _, parent := range []*bt.Tx{survivor2, gone2} {
				_, err = store.Create(ctx, parent, 1000)
				require.NoError(t, err)
			}

			ghost2 := spendOf(survivor2, gone2)
			_, err = store.Spend(ctx, ghost2, 1000)
			require.NoError(t, err)
			require.NoError(t, store.DeleteComplete(ctx, gone2.TxIDChainHash()))

			_, err = store.Spend(ctx, ghost2, 1200, utxo.IgnoreFlags{SpenderCreatedByCaller: true})
			require.ErrorIs(t, err, errors.ErrTxNotFound)

			kept, err := store.GetSpend(ctx, spendOfS0(survivor2))
			require.NoError(t, err)
			require.NotNil(t, kept.SpendingData, "an idempotent match next to a missing parent is the historical spend and must survive")
			require.Equal(t, *ghost2.TxIDChainHash(), *kept.SpendingData.TxID)
		})
	}
}
