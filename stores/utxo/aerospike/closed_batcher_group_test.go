package aerospike

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// TestSetLocked_ClosedBatcherDoesNotParkForFullTimeout is the regression test for
// the batch-enqueue accounting. PutBatchCtx sends the whole group in one channel
// send, so a Put-after-Close rejects every item at once. If the guard returned
// its error without completing them, group.Wait would park for the full
// batcherWait and then report a timeout — hiding the shutdown and stalling the
// caller for the whole budget.
//
// batcherWait is set high enough that a parked wait is unmistakable.
func TestSetLocked_ClosedBatcherDoesNotParkForFullTimeout(t *testing.T) {
	s := newTestStoreForGet(t)
	s.batcherWait = 30 * time.Second
	s.lockedBatcher = sendOnClosedBatcher[batchLocked]{}

	hashes := []chainhash.Hash{{0x01}, {0x02}, {0x03}}

	start := time.Now()

	var err error

	require.NotPanics(t, func() {
		err = s.SetLocked(context.Background(), hashes, true)
	}, "SetLocked must not propagate the batcher's send-on-closed-channel panic")

	elapsed := time.Since(start)

	require.Error(t, err)
	require.Contains(t, err.Error(), "shutting down")
	require.Less(t, elapsed, 5*time.Second,
		"every item must be completed on a rejected batch send; parking for batcherWait means the accounting is wrong")
}

// TestCreate_ClosedBatcherSurfacesShutdown drives the guard through the real
// Create path. Store.Close closes the store batcher FIRST, so it is dead for the
// whole remaining drain — a Create racing shutdown is the widest window of any
// site, and previously crashed the process.
func TestCreate_ClosedBatcherSurfacesShutdown(t *testing.T) {
	s := newTestStoreForGet(t)
	s.batcherWait = 30 * time.Second
	s.storeBatcher = sendOnClosedBatcher[BatchStoreItem]{}

	tx, err := bt.NewTxFromString("010000000000000000ef011c044c4db32b3da68aa54e3f30c71300db250e0b48ea740bd3897a8ea1a2cc9a020000006b483045022100c6177fa406ecb95817d3cdd3e951696439b23f8e888ef993295aa73046504029022052e75e7bfd060541be406ec64f4fc55e708e55c3871963e95bf9bd34df747ee041210245c6e32afad67f6177b02cfc2878fce2a28e77ad9ecbc6356960c020c592d867ffffffffd4c7a70c000000001976a914296b03a4dd56b3b0fe5706c845f2edff22e84d7388ac0301000000000000001976a914a4429da7462800dedc7b03a4fc77c363b8de40f588ac000000000000000024006a4c2042535620466175636574207c20707573682d7468652d627574746f6e2e617070d2c7a70c000000001976a914296b03a4dd56b3b0fe5706c845f2edff22e84d7388ac00000000")
	require.NoError(t, err)

	start := time.Now()

	var createErr error

	require.NotPanics(t, func() {
		_, createErr = s.Create(context.Background(), tx, 900000)
	}, "Create must not propagate the batcher's send-on-closed-channel panic")

	require.Error(t, createErr)
	require.Contains(t, createErr.Error(), "shutting down")
	require.Less(t, time.Since(start), 5*time.Second,
		"the item must be completed on a rejected send; parking for batcherWait means the accounting is wrong")
}

// TestSetDAHForChildRecords_ClosedBatcherSurfacesShutdown covers the per-item
// guard in the setDAH loop. Each child is its own channel send, so a shutdown
// race rejects them individually; completing each one lets the existing
// aggregate result read report the failure rather than waiting out batcherWait.
func TestSetDAHForChildRecords_ClosedBatcherSurfacesShutdown(t *testing.T) {
	s := newTestStoreForGet(t)
	s.batcherWait = 30 * time.Second
	s.setDAHBatcher = sendOnClosedBatcher[batchDAH]{}

	txID := &chainhash.Hash{0x07}

	start := time.Now()

	var err error

	require.NotPanics(t, func() {
		err = s.SetDAHForChildRecords(txID, 3, 900000)
	})

	require.Error(t, err)
	require.Less(t, time.Since(start), 5*time.Second,
		"each rejected child must be completed; parking for batcherWait means the accounting is wrong")
}

// TestIncrementSpentRecords_ClosedBatcherSurfacesShutdown covers the
// single-item guard on the increment batcher. SpendWaitTimeout is left at zero
// so the method's own 30s fallback applies — a parked wait would be obvious.
func TestIncrementSpentRecords_ClosedBatcherSurfacesShutdown(t *testing.T) {
	s := newTestStoreForGet(t)
	s.incrementBatcher = sendOnClosedBatcher[batchIncrement]{}

	txID := &chainhash.Hash{0x08}

	start := time.Now()

	var err error

	require.NotPanics(t, func() {
		_, err = s.IncrementSpentRecords(txID, 1, 900000)
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "shutting down")
	require.Less(t, time.Since(start), 5*time.Second)
}

// TestSpend_ClosedBatcherSurfacesShutdown drives the guard through the real Spend
// path — the hottest write path and the one this guard exists for. PutBatchCtx
// carries every input in one send, so a shutdown race rejects the whole group;
// all items must be completed or resolveSpendCompletions never sees them and the
// caller parks for the full SpendWaitTimeout.
func TestSpend_ClosedBatcherSurfacesShutdown(t *testing.T) {
	s := newTestStoreForGet(t)
	s.settings.UtxoStore.SpendWaitTimeout = 30 * time.Second
	s.spendBatcher = sendOnClosedBatcher[batchSpend]{}

	tx, err := bt.NewTxFromString("010000000000000000ef011c044c4db32b3da68aa54e3f30c71300db250e0b48ea740bd3897a8ea1a2cc9a020000006b483045022100c6177fa406ecb95817d3cdd3e951696439b23f8e888ef993295aa73046504029022052e75e7bfd060541be406ec64f4fc55e708e55c3871963e95bf9bd34df747ee041210245c6e32afad67f6177b02cfc2878fce2a28e77ad9ecbc6356960c020c592d867ffffffffd4c7a70c000000001976a914296b03a4dd56b3b0fe5706c845f2edff22e84d7388ac0301000000000000001976a914a4429da7462800dedc7b03a4fc77c363b8de40f588ac000000000000000024006a4c2042535620466175636574207c20707573682d7468652d627574746f6e2e617070d2c7a70c000000001976a914296b03a4dd56b3b0fe5706c845f2edff22e84d7388ac00000000")
	require.NoError(t, err)

	start := time.Now()

	var spendErr error

	require.NotPanics(t, func() {
		_, spendErr = s.Spend(context.Background(), tx, 900000)
	}, "Spend must not propagate the batcher's send-on-closed-channel panic")

	require.Error(t, spendErr)
	require.Less(t, time.Since(start), 5*time.Second,
		"every rejected input must be completed; parking for SpendWaitTimeout means the accounting is wrong")
}
