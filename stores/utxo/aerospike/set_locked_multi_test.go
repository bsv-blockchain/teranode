package aerospike_test

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// makeLockableTx builds a distinct, storable tx whose txid is derived from the
// given parent-outpoint hex, so each seed yields a different hash.
func makeLockableTx(t *testing.T, parentHex string) *bt.Tx {
	t.Helper()

	txn := bt.NewTx()
	require.NoError(t, txn.From(parentHex, 0, "76a914000000000000000000000000000000000000000088ac", 100_000))
	require.NoError(t, txn.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1_000))

	return txn
}

// TestStore_SetLocked_MultipleHashes exercises the public Store.SetLocked with
// more than one hash — the N>1 path (completion.NewGroup(len(txHashes)) + a
// single PutBatchCtx + one group.Wait + the first-non-nil-error reduction) that
// existing tests, which only ever pass a single hash, never covered.
func TestStore_SetLocked_MultipleHashes(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(func() {
		deferFn()
	})

	cleanDB(t, client)

	txs := []*bt.Tx{
		makeLockableTx(t, "1111111111111111111111111111111111111111111111111111111111111111"),
		makeLockableTx(t, "2222222222222222222222222222222222222222222222222222222222222222"),
		makeLockableTx(t, "3333333333333333333333333333333333333333333333333333333333333333"),
	}

	hashes := make([]chainhash.Hash, 0, len(txs))
	for _, txn := range txs {
		_, err := store.Create(ctx, txn, 101)
		require.NoError(t, err)

		hashes = append(hashes, *txn.TxIDChainHash())
	}

	// Lock all hashes in one batched call.
	require.NoError(t, store.SetLocked(ctx, hashes, true))

	for _, txn := range txs {
		m, err := store.Get(ctx, txn.TxIDChainHash())
		require.NoError(t, err)
		require.True(t, m.Locked, "tx %s must be locked after batched SetLocked(true)", txn.TxID())
	}

	// Unlock all hashes in one batched call.
	require.NoError(t, store.SetLocked(ctx, hashes, false))

	for _, txn := range txs {
		m, err := store.Get(ctx, txn.TxIDChainHash())
		require.NoError(t, err)
		require.False(t, m.Locked, "tx %s must be unlocked after batched SetLocked(false)", txn.TxID())
	}
}

// TestStore_SetLocked_PartialFailureFailsFast verifies the fail-fast behavior:
// a batch containing a hash that isn't in the store surfaces that hash's error
// from SetLocked (rather than silently succeeding or hanging). The failing item
// captures its error and cancels the shared wait.
func TestStore_SetLocked_PartialFailureFailsFast(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(func() {
		deferFn()
	})

	cleanDB(t, client)

	real1 := makeLockableTx(t, "4444444444444444444444444444444444444444444444444444444444444444")
	real2 := makeLockableTx(t, "5555555555555555555555555555555555555555555555555555555555555555")
	for _, txn := range []*bt.Tx{real1, real2} {
		_, err := store.Create(ctx, txn, 101)
		require.NoError(t, err)
	}

	// A hash that was never created — its setLocked hits the not-found path.
	bogus := chainhash.HashH([]byte("set-locked-bogus-hash-not-in-store"))

	hashes := []chainhash.Hash{*real1.TxIDChainHash(), bogus, *real2.TxIDChainHash()}

	err := store.SetLocked(ctx, hashes, true)
	require.Error(t, err, "a hash absent from the store must surface an error from the batched SetLocked")
}
