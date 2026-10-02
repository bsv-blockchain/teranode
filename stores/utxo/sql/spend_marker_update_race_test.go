package sql

import (
	"context"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/errors"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/stretchr/testify/require"
)

// TestSpendRefusesMarkerCommittedAfterSelect: the spend path reads the replay
// marker in its SELECT, at READ COMMITTED, and the pruner commits the marker and
// the child's delete together. A marker committed after that SELECT and before
// the UPDATE used to be invisible to the spend, so the write went through and a
// pruned transaction was re-spent. The UPDATE now re-checks the marker, and the
// row it refuses must be answered as a pruned replay with the output untouched.
//
// Postgres only. SQLite serialises writers, so the pruner cannot commit inside
// an open spend transaction there, and the hook's insert would block on it.
// Both Postgres spend paths are covered: the bulk path and the per-row one.
func TestSpendRefusesMarkerCommittedAfterSelect(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	for _, tc := range []struct {
		name string
		bulk bool
		// alsoSpent: the replay's own spend of the output is committed in the
		// same window, so the UPDATE finds the row already recording this exact
		// spend and would otherwise report an idempotent success.
		alsoSpent bool
	}{
		{name: "bulk", bulk: true},
		{name: "per_row"},
		{name: "bulk_idempotent", bulk: true, alsoSpent: true},
		{name: "per_row_idempotent", alsoSpent: true},
	} {
		bulk := tc.bulk

		t.Run(tc.name, func(t *testing.T) {
			store, ctx := setupPostgresStore(t)
			store.settings.UtxoStore.BatchSQLOperations = bulk
			require.NoError(t, store.SetBlockHeight(1000))

			parent := bt.NewTx()
			require.NoError(t, parent.From("1111111111111111111111111111111111111111111111111111111111111111", 0, "51", 30000))
			parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
			require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
			_, err := store.Create(ctx, parent, 1000)
			require.NoError(t, err)

			child := bt.NewTx()
			require.NoError(t, child.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
			child.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
			require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3000))

			// The pruner's commit, landing inside the spend's SELECT-to-UPDATE
			// window on its own connection.
			var once sync.Once

			store.afterSpendSelect = func() {
				once.Do(func() {
					_, insertErr := store.db.ExecContext(context.Background(),
						`INSERT INTO deleted_children (parent_id, child_hash) SELECT id, $2 FROM transactions WHERE hash = $1`,
						parent.TxIDChainHash()[:], child.TxIDChainHash()[:])
					require.NoError(t, insertErr)

					if tc.alsoSpent {
						_, spendErr := store.db.ExecContext(context.Background(),
							"UPDATE outputs SET spending_data = $2 WHERE idx = 0 AND transaction_id IN (SELECT id FROM transactions WHERE hash = $1)",
							parent.TxIDChainHash()[:], spendpkg.NewSpendingData(child.TxIDChainHash(), 0).Bytes())
						require.NoError(t, spendErr)
					}
				})
			}
			t.Cleanup(func() { store.afterSpendSelect = nil })

			spends, err := store.Spend(ctx, child, 1000)
			store.afterSpendSelect = nil

			require.Error(t, err, "a spend by a transaction the pruner just removed must be refused")
			require.Len(t, spends, 1)
			require.ErrorIs(t, spends[0].Err, errors.ErrUtxoSpendingTxPruned,
				"the refusal must be answered as a pruned replay, so the block paths can compensate")
			if !tc.alsoSpent {
				require.Nil(t, outputSpendingData(t, ctx, store, parent, 0),
					"the parent output must not record the pruned transaction's spend")
			}
		})
	}
}
