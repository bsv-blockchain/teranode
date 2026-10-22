package sql

import (
	"context"
	"net/url"
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

// setupSharedBatch builds a store whose spend batcher actually accumulates more
// than one claim per batch, which is the precondition #4934 needs.
//
// The package's setup() forces BatcherDrainMode=true so batch callbacks fire
// immediately and every batch holds one item. Here drain mode is off and the
// window is wide, so a Spend issued just before another lands in the SAME batch.
func setupSharedBatch(ctx context.Context, t *testing.T, dbName string) *Store {
	t.Helper()
	initPrometheusMetrics()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.DBTimeout = 30 * time.Second
	tSettings.UtxoStore.SpendBatcherSize = 256
	tSettings.UtxoStore.SpendBatcherDurationMillis = 200
	tSettings.BatcherDrainMode = false

	utxoStoreURL, err := url.Parse("sqlitememory:///" + dbName)
	require.NoError(t, err)

	store, err := New(ctx, ulogger.TestLogger{}, tSettings, utxoStoreURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(context.Background()) })

	return store
}

// twoOutParent creates a parent with two outputs and returns it.
func twoOutParent(ctx context.Context, t *testing.T, store *Store, seed string) *bt.Tx {
	t.Helper()

	parent := bt.NewTx()
	prev := chainhash.DoubleHashH([]byte(seed))
	require.NoError(t, parent.From(prev.String(), 0,
		"76a91471d7dd96d9edda09180fe9d57a477b5acc9cad1188ac", 1e8))
	parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{bscript.OpTRUE})
	require.NoError(t, parent.PayToAddress("1BUBHgpCBWCsD2Ajbeu7ZgCgvMHsoYHm3W", 1000))
	require.NoError(t, parent.PayToAddress("1BUBHgpCBWCsD2Ajbeu7ZgCgvMHsoYHm3W", 2000))

	_, err := store.Create(ctx, parent, 1)
	require.NoError(t, err)

	return parent
}

// spendOf builds a spend of the given vouts of parent, distinguished by lockTime
// so each variant carries different spending data.
func spendOf(t *testing.T, parent *bt.Tx, lockTime uint32, vouts ...uint32) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	for _, v := range vouts {
		require.NoError(t, tx.From(parent.TxIDChainHash().String(), v,
			parent.Outputs[v].LockingScript.String(), parent.Outputs[v].Satoshis))
	}
	require.NoError(t, tx.PayToAddress("1BUBHgpCBWCsD2Ajbeu7ZgCgvMHsoYHm3W", 1))
	tx.LockTime = lockTime

	return tx
}

// TestRespendBehindLoserInSameBatchKeepsLegitimateSpends is the repro for
// bitcoin-sv/teranode#4934.
//
// S0 legitimately spends both of the parent's outputs. A later batch then
// carries a double-spend attempt S1 on output 0 AHEAD of an idempotent re-spend
// of S0. FilterConflictingDuplicateSpendClaims awards the outpoint to the first
// claim in the batch without consulting the persisted spender, so S0's
// idempotent re-spend is rejected with ErrSpent naming S1 -- a transaction that
// never spent anything. Store.Spend then treats that ErrSpent as an unwinnable
// failure (hasUnwinnableFailure) and un-spends output 1, which S0 had already
// re-spent successfully in the same batch.
//
// Two independent defects, both pinned here:
//   - the re-spend must not be rejected (it matches the persisted spender);
//   - output 1 must stay spent by S0 even if some other input is refused.
func TestRespendBehindLoserInSameBatchKeepsLegitimateSpends(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := setupSharedBatch(ctx, t, "4934respend")
	require.NoError(t, store.SetBlockHeight(2))

	parent := twoOutParent(ctx, t, store, "4934-parent")

	s0 := spendOf(t, parent, 1, 0, 1) // legitimate spender of both outputs
	s1 := spendOf(t, parent, 2, 0)    // double-spend attempt on output 0

	// S0 spends both outputs up front.
	_, err := store.Spend(ctx, s0, 2)
	require.NoError(t, err, "initial S0 spend must succeed")

	// S1 (the loser) and the idempotent re-spend of S0 must reach the store in
	// one batch, S1 first. S1's Spend blocks until its batch fires, so the 20ms
	// gap plus the 200ms window puts both claims together.
	errS1 := make(chan error, 1)
	go func() {
		_, e := store.Spend(context.Background(), s1, 2)
		errS1 <- e
	}()
	time.Sleep(20 * time.Millisecond)

	_, errS0 := store.Spend(ctx, s0, 2)
	s1Err := <-errS1
	t.Logf("S1 err         : %v", s1Err)
	t.Logf("S0 re-spend err: %v", errS0)

	// Defect 1: an idempotent re-spend is a supported path and must succeed.
	require.NoError(t, errS0,
		"idempotent re-spend of S0 was rejected -- the in-batch filter handed the outpoint to S1")

	// The #825 guarantee must survive: the genuine double-spend loser is still
	// refused in the same batch, and is now blamed on S0, the tx that really
	// holds the outpoint, rather than on itself.
	require.Error(t, s1Err, "the double-spend attempt on output 0 must still be refused")
	require.True(t, errors.Is(s1Err, errors.ErrSpent), "expected ErrSpent for the loser, got: %v", s1Err)
	require.Contains(t, s1Err.Error(), s0.TxID(),
		"the loser must be blamed on S0, the persisted spender")

	// Defect 2: both outputs must remain spent by S0. Output 1 is the one the
	// rollback clears, and it is a real consensus-state loss.
	spends, err := utxo.GetSpends(s0)
	require.NoError(t, err)
	require.Len(t, spends, 2)

	for _, sp := range spends {
		resp, getErr := store.GetSpend(ctx, sp)
		require.NoError(t, getErr)
		t.Logf("output %d: status=%v spendingData=%v", sp.Vout, resp.Status, resp.SpendingData)

		require.Equal(t, int(utxo.Status_SPENT), resp.Status,
			"output %d that S0 legitimately spent went back to unspent", sp.Vout)
		require.NotNil(t, resp.SpendingData, "output %d must name its spender", sp.Vout)
		require.Equal(t, s0.TxID(), resp.SpendingData.TxID.String(),
			"output %d is owned by %s, not by S0", sp.Vout, resp.SpendingData.TxID)
	}
}
