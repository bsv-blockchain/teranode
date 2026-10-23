package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/stretchr/testify/require"
)

// Two transactions with different fees created in one statement keep their own fees. The create
// plan sorts its rows by txid, and the fee column has to move with them.
func TestCreatePlanKeepsEachTransactionsFee(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)

	const height = 950

	w := tests.BuildMultiWorkload(t, 0x95, 1, 4)
	w.StoreRoots(t, s, height-1)

	// Rebuild each level-0 transaction with its own fee: 200, 300, 400, 500.
	txs := make([]*bt.Tx, len(w.Txs))

	for i, old := range w.Txs {
		tx := bt.NewTx()
		tx.LockTime = old.LockTime

		var in uint64

		for _, input := range old.Inputs {
			require.NoError(t, tx.FromUTXOs(&bt.UTXO{TxIDHash: input.PreviousTxIDChainHash(), Vout: input.PreviousTxOutIndex,
				LockingScript: input.PreviousTxScript, Satoshis: input.PreviousTxSatoshis}))
			tx.Inputs[len(tx.Inputs)-1].UnlockingScript = input.UnlockingScript
			in += input.PreviousTxSatoshis
		}

		require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", in-uint64(200+100*i))) //nolint:gosec // test data
		txs[i] = tx
	}

	results, err := s.SpendAndCreateMulti(ctx, txs, height, utxo.WithIgnoreLocked(true))
	require.NoError(t, err)

	for i, tx := range txs {
		require.Equal(t, utxo.MultiTxCreated, results[i].Status, "tx %d: %v", i, results[i].Err)

		md, err := s.Get(ctx, tx.TxIDChainHash(), fields.Fee)
		require.NoError(t, err)
		require.Equal(t, uint64(200+100*i), md.Fee, "tx %d", i) //nolint:gosec // test data
	}
}
