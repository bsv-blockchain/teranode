package sql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/stretchr/testify/require"
)

// TestMissingParentRollsBackFreshSiblingSpend: a ghost C spends S:0, whose
// record survives, and G:0, whose record the pruner removed. The spend answers
// G with ErrTxNotFound, which the block paths read as a pruned replay and
// compensate by deleting C's record. DeleteCreated never reverses a ghost's
// spends, so the store must roll back the fresh spend of S:0 in the same call.
// Before the missing-parent answer was a rollback error, S:0 was left spent by
// a transaction the store no longer held.
//
// The other half: when S:0 already recorded C's spend, that is the confirmed,
// historical one, and the rollback must leave it alone.
func TestMissingParentRollsBackFreshSiblingSpend(t *testing.T) {
	forEachBackend(t, func(t *testing.T, ctx context.Context, store *Store) {
		for i, perRow := range []bool{false, true} {
			store.settings.UtxoStore.BatchSQLOperations = !perRow

			newParent := func(seed string) *bt.Tx {
				parent := bt.NewTx()
				require.NoError(t, parent.From("11111111111111111111111111111111111111111111111111111111111111"+seed, 0, "51", 30000))
				parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
				require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))

				return parent
			}

			suffix := string(rune('0' + i))
			survivor, gone := newParent("a"+suffix), newParent("b"+suffix)
			_, err := store.Create(ctx, survivor, 1000)
			require.NoError(t, err)

			ghost := bt.NewTx()
			for _, parent := range []*bt.Tx{survivor, gone} {
				require.NoError(t, ghost.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
			}

			for j := range ghost.Inputs {
				ghost.Inputs[j].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
			}

			require.NoError(t, ghost.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3000))

			_, err = store.Spend(ctx, ghost, 1200, utxo.IgnoreFlags{SpenderCreatedByCaller: true})
			require.ErrorIs(t, err, errors.ErrTxNotFound, "control: the pruned parent answers not found")
			require.Nil(t, outputSpendingData(t, ctx, store, survivor, 0),
				"the fresh spend of the surviving parent must be rolled back (perRow=%v)", perRow)

			// Historical: the surviving output already records the ghost's spend.
			historical := spendpkg.NewSpendingData(ghost.TxIDChainHash(), 0).Bytes()
			_, err = store.db.ExecContext(ctx,
				"UPDATE outputs SET spending_data = $2 WHERE idx = 0 AND transaction_id IN (SELECT id FROM transactions WHERE hash = $1)",
				survivor.TxIDChainHash()[:], historical)
			require.NoError(t, err)

			_, err = store.Spend(ctx, ghost, 1200, utxo.IgnoreFlags{SpenderCreatedByCaller: true})
			require.ErrorIs(t, err, errors.ErrTxNotFound)
			require.Equal(t, historical, outputSpendingData(t, ctx, store, survivor, 0),
				"an idempotent match next to a missing parent is the historical spend and must survive (perRow=%v)", perRow)
		}
	})
}
