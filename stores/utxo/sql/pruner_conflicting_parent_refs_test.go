package sql

import (
	"fmt"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
	"github.com/stretchr/testify/require"
)

// TestPrunerClearsParentRefsOfReapedConflictingLoser verifies the pruner, in defensive mode only,
// removes every reference a surviving parent holds to a conflicting loser before it deletes
// the loser: the parent output spend and the parent conflicting_children entry.
func TestPrunerClearsParentRefsOfReapedConflictingLoser(t *testing.T) {
	for _, defensive := range []bool{false, true} {
		t.Run(fmt.Sprintf("defensive=%v", defensive), func(t *testing.T) {
			ResetPrunerServiceForTests()
			t.Cleanup(ResetPrunerServiceForTests)

			ctx := t.Context()

			store, parent := setup(ctx, t)
			store.settings.Pruner.UTXODefensiveEnabled = defensive

			loser := bt.NewTx()
			require.NoError(t, loser.From(parent.TxIDChainHash().String(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
			require.NoError(t, loser.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", parent.Outputs[0].Satoshis-1000))
			loser.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{})

			_, err := store.Create(ctx, parent, 0)
			require.NoError(t, err)

			_, err = store.Create(ctx, loser, 0)
			require.NoError(t, err)

			spends, err := store.Spend(ctx, loser, store.GetBlockHeight()+1)
			require.NoError(t, err)
			require.Len(t, spends, len(loser.Inputs))

			_, _, err = store.SetConflicting(ctx, []chainhash.Hash{*loser.TxIDChainHash()}, true)
			require.NoError(t, err)

			parentSpend := &utxo.Spend{TxID: parent.TxIDChainHash(), Vout: 0, UTXOHash: spends[0].UTXOHash}

			before, err := store.GetSpend(ctx, parentSpend)
			require.NoError(t, err)
			require.NotNil(t, before.SpendingData, "precondition: the parent output is spent by the loser")

			countChildRefs := func() int {
				var n int

				require.NoError(t, store.db.QueryRowContext(ctx, `
					SELECT COUNT(*) FROM conflicting_children
					WHERE transaction_id = (SELECT id FROM transactions WHERE hash = $1)`, parent.TxIDChainHash()[:]).Scan(&n))

				return n
			}

			require.Equal(t, 1, countChildRefs(), "precondition: the parent lists the loser as a conflicting child")

			provider, ok := any(store).(pruner.PrunerServiceProvider)
			require.True(t, ok)

			prunerSvc, err := provider.GetPrunerService()
			require.NoError(t, err)

			_, err = prunerSvc.Prune(ctx, store.GetBlockHeight()+1000, "test")
			require.NoError(t, err)

			_, err = store.Get(ctx, loser.TxIDChainHash())
			require.ErrorIs(t, err, errors.ErrTxNotFound, "the conflicting loser must be reaped")

			after, err := store.GetSpend(ctx, parentSpend)
			require.NoError(t, err)

			if defensive {
				require.Nil(t, after.SpendingData, "a surviving parent must not name the reaped loser as its spender")
				require.Equal(t, 0, countChildRefs(), "a surviving parent must not list the reaped loser as a conflicting child")
			} else {
				require.NotNil(t, after.SpendingData, "default mode keeps the existing behaviour")
			}
		})
	}
}
