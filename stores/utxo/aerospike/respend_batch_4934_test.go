package aerospike_test

import (
	"context"
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
// Unlike the SQL package, sendSpendBatchLua is unexported here, so the batch is
// assembled the way this package already does it: a wide batcher window with
// drain mode off, and the two Spend calls issued a moment apart. Both the Lua and
// the expression spend paths are exercised.
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
			tSettings.UtxoStore.SpendBatcherSize = 8
			tSettings.UtxoStore.SpendBatcherDurationMillis = 5000
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

			// Loser first, then the re-spend, both inside the 5s window.
			loserErr := make(chan error, 1)
			go func() {
				_, e := store.Spend(context.Background(), loser, 2)
				loserErr <- e
			}()
			time.Sleep(50 * time.Millisecond)

			_, respendErr := store.Spend(storeCtx, respend, 2)
			lErr := <-loserErr
			t.Logf("loser err          : %v", lErr)
			t.Logf("re-spend err        : %v", respendErr)

			// The idempotent re-spend must be accepted.
			require.NoError(t, respendErr,
				"idempotent re-spend was refused -- the filter followed batch order, not persisted state")

			// The genuine double-spend is still refused, and blamed on S0.
			require.Error(t, lErr, "the double-spend attempt must still be refused")
			require.True(t, errors.Is(lErr, errors.ErrSpent), "expected ErrSpent for the loser, got: %v", lErr)
			require.Contains(t, lErr.Error(), respend.TxID(),
				"the loser must be blamed on the persisted spender, not on itself")

			// Both outputs must still be spent by S0.
			spends, err := utxo.GetSpends(respend)
			require.NoError(t, err)
			require.Len(t, spends, 2)

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
		})
	}
}
