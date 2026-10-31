package sql

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/stretchr/testify/require"
)

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
