package aerospike_test

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestClose_DrainsQueuedSetLockedMultiRecord is the regression test for the
// lockedBatcher self-requeue shutdown hazard.
//
// setLockedBatch, when a master record reports child/extra records, used to
// re-Put each child back into lockedBatcher and block on its result. During a
// draining Close that re-Put hits an already-closed input channel (panic) and
// could never be serviced anyway (the worker that would handle it is the one
// shutting down — deadlock). The fix writes child records inline within the
// callback, matching the create path.
//
// To exercise the drain path deterministically we keep the locked batcher
// queued (drain mode off, large size, long window) so the SetLocked is flushed
// by Close itself, and use a small utxo batch size so a modest tx spans
// multiple records (ChildCount > 0, triggering the child path). A successful
// SetLocked plus a clean Close proves the inline handling works during drain;
// the pre-fix code would panic or wedge here.
func TestClose_DrainsQueuedSetLockedMultiRecord(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Aerospike integration test in short mode")
	}

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	// Park the SetLocked in the locked batcher until Close drains it. Other
	// batchers keep their defaults so Create completes promptly.
	tSettings.UtxoStore.LockedBatcherDrainMode = false
	tSettings.UtxoStore.LockedBatcherSize = 1024
	tSettings.UtxoStore.LockedBatcherDurationMillis = 60000 // 60s; Close fires first
	tSettings.UtxoStore.LockedBatcherTickerIntervalMillis = 0

	client, store, ctx, cleanup := initAerospike(t, tSettings, logger)
	defer cleanup()

	cleanDB(t, client)

	// Small utxo batch size so a 20-output tx spans several records, giving the
	// master a non-zero ChildCount and driving setLockedBatch's child path.
	store.SetUtxoBatchSize(5)

	tx := createTransactionWithOutputs(20)

	_, err := store.Create(ctx, tx, 100)
	require.NoError(t, err, "failed to create multi-record tx")

	// SetLocked parks in the locked batcher (drain mode off, 60s window) and is
	// only dispatched when Close drains the batcher.
	lockErr := make(chan error, 1)
	go func() {
		lockErr <- store.SetLocked(ctx, []chainhash.Hash{*tx.TxIDChainHash()}, true)
	}()

	// Give the goroutine time to enqueue before we close.
	time.Sleep(500 * time.Millisecond)

	closeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	require.NoError(t, store.Close(closeCtx),
		"Close must drain the queued multi-record SetLocked without error")

	select {
	case e := <-lockErr:
		require.NoError(t, e,
			"SetLocked must succeed once Close drained it (pre-fix self-requeue would panic/deadlock)")
	case <-time.After(10 * time.Second):
		t.Fatal("SetLocked did not return after Close drained the locked batcher — drain wedged")
	}
}

// TestClose_DrainsQueuedSpendCrossBatcher is the regression test for the
// batcher close-order inversion.
//
// The spend batcher's drain callback (sendSpendBatchLua -> processSpendBatchResults
// -> handleExtraRecords / handleSpendSignal) enqueues into setDAHBatcher and
// incrementBatcher when a tx is fully spent. If those consumer batchers are
// closed before the spend batcher, the spend drain Puts into a closed channel
// and go-batcher panics. The fix closes spend before setDAH/increment.
//
// We fully spend a multi-record tx through a spend batcher kept queued until
// Close (drain mode off, long window), so the spend callback — and its
// setDAH/increment enqueues — run during Close's drain. A successful Spend plus
// a clean Close proves the close order is correct; the pre-fix order panics.
func TestClose_DrainsQueuedSpendCrossBatcher(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Aerospike integration test in short mode")
	}

	const (
		batchSize  = 2
		numOutputs = 10 // > batchSize so the tx spans multiple records
	)

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.UtxoBatchSize = batchSize

	// Park the spend in the spend batcher until Close drains it; other batchers
	// keep defaults so Create completes promptly.
	tSettings.UtxoStore.SpendBatcherDrainMode = false
	tSettings.UtxoStore.SpendBatcherSize = 1024
	tSettings.UtxoStore.SpendBatcherDurationMillis = 60000 // 60s; Close fires first
	tSettings.UtxoStore.SpendBatcherTickerIntervalMillis = 0

	client, store, ctx, cleanup := initAerospike(t, tSettings, logger)
	defer cleanup()

	cleanDB(t, client)

	// Build and store a multi-record tx.
	largeTx := bt.NewTx()
	require.NoError(t, largeTx.From(
		"1111111111111111111111111111111111111111111111111111111111111111",
		0,
		"76a914000000000000000000000000000000000000000088ac",
		uint64(numOutputs*1000+1000),
	))
	for i := 0; i < numOutputs; i++ {
		require.NoError(t, largeTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))
	}

	_, err := store.Create(ctx, largeTx, 1)
	require.NoError(t, err, "failed to create multi-record tx")

	// Spending tx that spends every output, so the spend signals AllSpent and the
	// callback enqueues into setDAH/increment.
	spendingTx := bt.NewTx()
	for i := 0; i < numOutputs; i++ {
		require.NoError(t, spendingTx.From(
			largeTx.TxIDChainHash().String(),
			uint32(i),
			largeTx.Outputs[i].LockingScript.String(),
			largeTx.Outputs[i].Satoshis,
		))
	}
	require.NoError(t, spendingTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", uint64(numOutputs*1000-500)))

	// Spend parks in the spend batcher (drain mode off, 60s window) until Close.
	spendErr := make(chan error, 1)
	go func() {
		_, e := store.Spend(ctx, spendingTx, store.GetBlockHeight()+1)
		spendErr <- e
	}()

	time.Sleep(500 * time.Millisecond)

	closeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	require.NoError(t, store.Close(closeCtx),
		"Close must drain the queued spend (which enqueues into setDAH/increment) without error")

	select {
	case e := <-spendErr:
		require.NoError(t, e,
			"Spend must succeed once Close drained it (pre-fix close order panics on the setDAH/increment Put)")
	case <-time.After(10 * time.Second):
		t.Fatal("Spend did not return after Close drained the spend batcher — drain wedged")
	}
}
