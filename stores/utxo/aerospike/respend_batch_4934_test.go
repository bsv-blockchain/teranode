package aerospike_test

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestRespendBehindLoserInSameBatch is the Aerospike half of bitcoin-sv/teranode#4934.
//
// S0 already owns both parent outputs. A batch then carries a double-spend
// attempt on output 0 AHEAD of S0's idempotent re-spend. The filter used to
// award the outpoint by batch position, so the legitimate re-spend was refused
// with ErrSpent naming the double-spend attempt, which itself was refused by the
// backend as already spent -- both claims lost, each blamed on the other.
//
// A test-only adapter supplies an exact batch to both backend paths.
func TestRespendBehindLoserInSameBatch(t *testing.T) {
	cases := []struct {
		name              string
		utxoBatchSize     int
		enableExpressions bool
	}{
		{name: "lua", utxoBatchSize: 128, enableExpressions: false},
		{name: "expressions", utxoBatchSize: 1, enableExpressions: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.UtxoStore.UtxoBatchSize = tc.utxoBatchSize
			tSettings.UtxoStore.SpendBatcherSize = 1
			tSettings.UtxoStore.SpendBatcherDurationMillis = 1
			tSettings.UtxoStore.SpendWaitTimeout = 30 * time.Second
			tSettings.Aerospike.EnableSpendFilterExpressions = tc.enableExpressions

			client, store, storeCtx, deferFn := initAerospike(t, tSettings, ulogger.NewErrorTestLogger(t))
			t.Cleanup(deferFn)
			cleanDB(t, client)
			require.NoError(t, store.SetBlockHeight(2))

			parent := bt.NewTx()
			prev := chainhash.DoubleHashH([]byte("4934-as-" + tc.name))
			require.NoError(t, parent.From(prev.String(), 0,
				"76a91471d7dd96d9edda09180fe9d57a477b5acc9cad1188ac", 1e8))
			parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{bscript.OpTRUE})
			require.NoError(t, parent.PayToAddress("1BUBHgpCBWCsD2Ajbeu7ZgCgvMHsoYHm3W", 1000))
			require.NoError(t, parent.PayToAddress("1BUBHgpCBWCsD2Ajbeu7ZgCgvMHsoYHm3W", 2000))
			_, err := store.Create(storeCtx, parent, 1)
			require.NoError(t, err)

			spendOf := func(lockTime uint32, vouts ...uint32) *bt.Tx {
				tx := bt.NewTx()
				for _, v := range vouts {
					require.NoError(t, tx.From(parent.TxIDChainHash().String(), v,
						parent.Outputs[v].LockingScript.String(), parent.Outputs[v].Satoshis))
				}
				require.NoError(t, tx.PayToAddress("1BUBHgpCBWCsD2Ajbeu7ZgCgvMHsoYHm3W", 1))
				tx.LockTime = lockTime

				return tx
			}

			respend := spendOf(1, 0, 1) // legitimate spender of both outputs
			loser := spendOf(2, 0)      // double-spend attempt on output 0

			_, err = store.Spend(storeCtx, respend, 2)
			require.NoError(t, err, "initial spend must succeed")

			spends, err := utxo.GetSpends(respend)
			require.NoError(t, err)
			require.Len(t, spends, 2)
			loserSpends, err := utxo.GetSpends(loser)
			require.NoError(t, err)
			store.SendSpendBatchForTest(t, []*utxo.Spend{loserSpends[0], spends[0], spends[1]}, 2)
			require.NoError(t, spends[0].Err, "the persisted owner must be accepted")
			require.NoError(t, spends[1].Err)
			require.ErrorIs(t, loserSpends[0].Err, errors.ErrSpent)
			require.Contains(t, loserSpends[0].Err.Error(), respend.TxID())

			for _, sp := range spends {
				resp, getErr := store.GetSpend(storeCtx, sp)
				require.NoError(t, getErr)
				t.Logf("output %d: status=%v spendingData=%v", sp.Vout, resp.Status, resp.SpendingData)

				require.Equal(t, int(utxo.Status_SPENT), resp.Status,
					"output %d that S0 legitimately spent went back to unspent", sp.Vout)
				require.NotNil(t, resp.SpendingData, "output %d must name its spender", sp.Vout)
				require.Equal(t, respend.TxID(), resp.SpendingData.TxID.String(),
					"output %d is owned by %s, not by S0", sp.Vout, resp.SpendingData.TxID)
			}
			// Neither claim matches the stored owner. Both errors must name it.
			first, err := utxo.GetSpends(spendOf(3, 0))
			require.NoError(t, err)
			second, err := utxo.GetSpends(spendOf(4, 0))
			require.NoError(t, err)
			store.SendSpendBatchForTest(t, []*utxo.Spend{first[0], second[0]}, 2)
			for _, sp := range []*utxo.Spend{first[0], second[0]} {
				require.ErrorIs(t, sp.Err, errors.ErrSpent)
				var data *errors.UtxoSpentErrData
				require.True(t, errors.AsData(sp.Err, &data))
				require.Equal(t, respend.TxID(), data.SpendingData.TxID.String())
			}

		})
	}
}
