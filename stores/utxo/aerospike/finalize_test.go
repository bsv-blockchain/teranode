package aerospike_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	teranode_aerospike "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/stretchr/testify/require"
)

// TestFinalizeTransaction verifies that FinalizeTransaction clears the tentative
// creating state (making outputs spendable), and is idempotent / a no-op for
// transactions that were never created in the creating state.
func TestFinalizeTransaction(t *testing.T) {
	tx := uniqueTx(tests.Tx.Clone())

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	_, err := store.Create(ctx, tests.ParentTx, 100)
	require.NoError(t, err)

	_, err = store.Create(ctx, tx, 100, utxo.WithCreating(true))
	require.NoError(t, err)

	// Finalize clears the creating state...
	require.NoError(t, store.FinalizeTransaction(ctx, tx, attemptOf(t, ctx, store, tx.TxIDChainHash()), attemptOf(t, ctx, store, tx.TxIDChainHash())))

	got := &meta.Data{}
	require.NoError(t, store.GetMeta(ctx, tx.TxIDChainHash(), got))
	require.False(t, got.Creating, "finalize must clear the creating flag")

	// ...making the outputs spendable.
	child := spendableChildTx(t, tx, 0)
	_, err = store.Spend(ctx, child, 101)
	require.NoError(t, err)

	// The store recorded the lease of the caller whose write cleared the state. A repeat of the
	// finalize under that lease is told that the write was its own, so a caller whose answer was
	// lost can ask again. A caller under another lease, or under none, is told that it was not.
	creatorLease := attemptOf(t, ctx, store, tx.TxIDChainHash())
	require.NoError(t, store.FinalizeTransaction(ctx, tx, creatorLease, creatorLease))

	other := store.FinalizeTransaction(ctx, tx, creatorLease, []byte("another spender01"))
	require.ErrorIs(t, other, errors.ErrTxExists)
	require.True(t, utxo.IsAlreadyFinal(other), "another caller did not clear the state, got: %v", other)

	require.True(t, utxo.IsAlreadyFinal(store.FinalizeTransaction(ctx, tx, creatorLease, nil)), "a caller with no lease is never told the write was its own")

	require.NoError(t, store.GetMeta(ctx, tx.TxIDChainHash(), got))
	require.False(t, got.Creating)

	// A tx that was never created in the creating state is already final too.
	require.True(t, utxo.IsAlreadyFinal(store.FinalizeTransaction(ctx, tests.ParentTx, nil, nil)))
}

// TestFinalizeTransaction_OneCallerOwnsTheHandOff covers the creator of a record and a
// roll-forward of the same attempt, which can both reach the finalize. Exactly one of them gets
// nil, whichever send the server applied, and each can tell after a lost answer.
func TestFinalizeTransaction_OneCallerOwnsTheHandOff(t *testing.T) {
	tx := uniqueTx(tests.Tx.Clone())

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)

	hash := tx.TxIDChainHash()

	tentative := func(t *testing.T) (token, lease []byte) {
		t.Helper()
		cleanDB(t, client)

		_, err := store.Create(ctx, tests.ParentTx, 100)
		require.NoError(t, err)

		_, err = store.Create(ctx, tx, 100, utxo.WithCreating(true))
		require.NoError(t, err)

		token = attemptOf(t, ctx, store, hash)

		lease, took, err := store.TakeSpendLease(ctx, hash, token)
		require.NoError(t, err)
		require.True(t, took)

		return token, lease
	}

	t.Run("the roll-forward clears the state: it owns the hand-off, and the creator is told so on every try", func(t *testing.T) {
		token, lease := tentative(t)

		require.NoError(t, store.FinalizeTransaction(ctx, tx, token, lease))

		for try := 0; try < 2; try++ {
			lost := store.FinalizeTransaction(ctx, tx, token, token)
			require.ErrorIs(t, lost, errors.ErrTxExists)
			require.True(t, utxo.IsAlreadyFinal(lost), "try %d: the creator must not take the hand-off of a record that the roll-forward delivered", try)
		}

		require.NoError(t, store.FinalizeTransaction(ctx, tx, token, lease), "a repeat by the roll-forward is its own write")

		// The finalize took only the lease of its caller: the creator's is still on the record.
		require.Len(t, leasesOf(t, client, store, hash), 1)

		require.NoError(t, store.ReleaseSpendLease(ctx, hash, token, token))
		require.Empty(t, leasesOf(t, client, store, hash))
		require.NotContains(t, recordBins(t, client, store, hash, 0, fields.Spenders), fields.Spenders.String(), "a record with no spender carries no empty spenders bin")
	})

	t.Run("the creator clears the state: it owns the hand-off, and the roll-forward is told so", func(t *testing.T) {
		token, lease := tentative(t)

		require.NoError(t, store.FinalizeTransaction(ctx, tx, token, token))

		lost := store.FinalizeTransaction(ctx, tx, token, lease)
		require.True(t, utxo.IsAlreadyFinal(lost), "got: %v", lost)

		require.NoError(t, store.FinalizeTransaction(ctx, tx, token, token), "a repeat by the creator is its own write")

		// The lease of the roll-forward is still live on the final record.
		require.Len(t, leasesOf(t, client, store, hash), 1)

		require.NoError(t, store.ReleaseSpendLease(ctx, hash, token, lease))
		require.NotContains(t, recordBins(t, client, store, hash, 0, fields.Spenders), fields.Spenders.String())
	})

	t.Run("a finalize under a lease that is no longer on the record still clears the state", func(t *testing.T) {
		token, lease := tentative(t)

		require.NoError(t, store.ReleaseSpendLease(ctx, hash, token, lease))
		require.NoError(t, store.ReleaseSpendLease(ctx, hash, token, token))
		require.NotContains(t, recordBins(t, client, store, hash, 0, fields.Spenders), fields.Spenders.String(), "precondition: the record carries no spenders bin")

		require.NoError(t, store.FinalizeTransaction(ctx, tx, token, token))

		got := &meta.Data{}
		require.NoError(t, store.GetMeta(ctx, hash, got))
		require.False(t, got.Creating)
	})

	t.Run("a finalize whose answer is lost is its own write on the next try", func(t *testing.T) {
		for name, asCreator := range map[string]bool{"creator": true, "roll-forward": false} {
			token, lease := tentative(t)

			caller := lease
			if asCreator {
				caller = token
			}

			// The server applies the write and the caller gets an error in place of the answer.
			var lostOnce atomic.Bool

			store.SetBatchOperateFn(func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
				applied := client.BatchOperate(policy, records)

				if write, ok := records[0].(*aerospike.BatchWrite); ok && write.Policy != nil &&
					write.Policy.RecordExistsAction == aerospike.UPDATE_ONLY && lostOnce.CompareAndSwap(false, true) {
					return &aerospike.AerospikeError{ResultCode: types.TIMEOUT}
				}

				return applied
			})

			first := store.FinalizeTransaction(ctx, tx, token, caller)

			store.SetBatchOperateFn(nil)

			require.Error(t, first, name)
			require.False(t, utxo.IsAlreadyFinal(first), name)

			got := &meta.Data{}
			require.NoError(t, store.GetMeta(ctx, hash, got))
			require.False(t, got.Creating, "%s: the write was applied", name)

			require.NoError(t, store.FinalizeTransaction(ctx, tx, token, caller), "%s: the record is final under this caller's own write, so the repeat owns the hand-off", name)
		}
	})
}

// cancelledAfterBatchContext reports itself cancelled through Err once the flag is set, and never
// closes Done, so a FinalizeTransaction wait runs to batch completion and only then observes the
// cancellation. The flag is set after the batch write returns: a finalize whose caller has given up
// before the send is not sent at all (TestFinalizeTransaction_AnAbandonedFinalizeIsNotSent).
type cancelledAfterBatchContext struct {
	context.Context
	cancelled *atomic.Bool
}

func (cancelledAfterBatchContext) Done() <-chan struct{} { return nil }

func (c cancelledAfterBatchContext) Err() error {
	if c.cancelled.Load() {
		return context.Canceled
	}

	return nil
}

// TestFinalizeTransaction_ContextCancellation pins the return value when the caller's context is
// cancelled: before the call the finalize reports the context error; once the batch completed the
// finalize reports success because the record is finalized. The batch still runs for an item that
// was already enqueued, so the record state after a cancelled call is not asserted.
func TestFinalizeTransaction_ContextCancellation(t *testing.T) {
	tx := uniqueTx(tests.Tx.Clone())

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	t.Run("cancelled before the call", func(t *testing.T) {
		_, err := store.Create(ctx, tests.ParentTx, 100)
		require.NoError(t, err)

		_, err = store.Create(ctx, tx, 100, utxo.WithCreating(true))
		require.NoError(t, err)

		cancelledCtx, cancel := context.WithCancel(ctx)
		cancel()

		err = store.FinalizeTransaction(cancelledCtx, tx, attemptOf(t, ctx, store, tx.TxIDChainHash()), attemptOf(t, ctx, store, tx.TxIDChainHash()))
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("cancelled after the batch completed", func(t *testing.T) {
		cleanDB(t, client)

		_, err := store.Create(ctx, tests.ParentTx, 100)
		require.NoError(t, err)

		_, err = store.Create(ctx, tx, 100, utxo.WithCreating(true))
		require.NoError(t, err)

		// Read before the hook is installed: the read of the token is a batch of its own.
		token := attemptOf(t, ctx, store, tx.TxIDChainHash())

		var cancelled atomic.Bool

		store.SetBatchOperateFn(func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
			applied := client.BatchOperate(policy, records)
			cancelled.Store(true)

			return applied
		})
		t.Cleanup(func() { store.SetBatchOperateFn(nil) })

		err = store.FinalizeTransaction(cancelledAfterBatchContext{Context: ctx, cancelled: &cancelled}, tx, token, token)
		require.NoError(t, err, "a finalize that landed must not be reported as failed")

		got := &meta.Data{}
		require.NoError(t, store.GetMeta(ctx, tx.TxIDChainHash(), got))
		require.False(t, got.Creating, "the batch must have finalized the record")
	})
}

// TestFinalizeTransaction_MultiRecord finalizes a paginated (multi-record) tx.
func TestFinalizeTransaction_MultiRecord(t *testing.T) {
	tx := uniqueTx(tests.Tx.Clone())

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.UtxoBatchSize = 2 // tx has 6 outputs → multiple records

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	_, err := store.Create(ctx, tests.ParentTx, 100)
	require.NoError(t, err)

	_, err = store.Create(ctx, tx, 100, utxo.WithCreating(true))
	require.NoError(t, err)

	require.NoError(t, store.FinalizeTransaction(ctx, tx, attemptOf(t, ctx, store, tx.TxIDChainHash()), attemptOf(t, ctx, store, tx.TxIDChainHash())))

	got := &meta.Data{}
	require.NoError(t, store.GetMeta(ctx, tx.TxIDChainHash(), got))
	require.False(t, got.Creating, "finalize must clear the creating flag on a multi-record tx")

	child := spendableChildTx(t, tx, 0)
	_, err = store.Spend(ctx, child, 101)
	require.NoError(t, err)
}

// TestFinalizeTransaction_RollingBack pins that a record whose rollback has begun cannot be
// finalized: the finalize reports ErrTxNotFound and leaves the creating bin, so the outputs
// stay unspendable until the rollback owner deletes the record.
func TestFinalizeTransaction_RollingBack(t *testing.T) {
	tx := uniqueTx(tests.Tx.Clone())

	for name, batchSize := range map[string]int{"single record": 0, "multi record": 2} {
		t.Run(name, func(t *testing.T) {
			logger := ulogger.NewErrorTestLogger(t)
			tSettings := test.CreateBaseTestSettings(t)

			if batchSize > 0 {
				tSettings.UtxoStore.UtxoBatchSize = batchSize // tx has 6 outputs → multiple records
			}

			client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
			t.Cleanup(deferFn)
			cleanDB(t, client)

			_, err := store.Create(ctx, tests.ParentTx, 100)
			require.NoError(t, err)

			_, err = store.Create(ctx, tx, 100, utxo.WithCreating(true))
			require.NoError(t, err)

			began, err := beginAsCreator(ctx, store, tx.TxIDChainHash(), attemptOf(t, ctx, store, tx.TxIDChainHash()))
			require.NoError(t, err)
			require.True(t, began)

			err = store.FinalizeTransaction(ctx, tx, attemptOf(t, ctx, store, tx.TxIDChainHash()), attemptOf(t, ctx, store, tx.TxIDChainHash()))
			require.ErrorIs(t, err, errors.ErrTxNotFound, "a rolling-back record must not be finalized")
			require.Contains(t, err.Error(), "rolling back")

			got := &meta.Data{}
			require.NoError(t, store.GetMeta(ctx, tx.TxIDChainHash(), got))
			require.True(t, got.Creating, "the creating bin must survive the refused finalize")
			require.True(t, got.RollingBack)

			if batchSize > 0 {
				for idx := uint32(1); idx < 3; idx++ {
					bins := recordBins(t, client, store, tx.TxIDChainHash(), idx, fields.Creating)
					require.Equal(t, true, bins[fields.Creating.String()], "child record %d must be untouched by the refused finalize", idx)
				}
			}

			child := spendableChildTx(t, tx, 0)
			_, err = store.Spend(ctx, child, 101)
			require.ErrorIs(t, err, errors.ErrTxCreating, "the outputs of a rolling-back record stay unspendable")
		})
	}
}

// TestQueryStaleCreatingTxs verifies the sweeper query returns only creating-state
// txs whose unminedSince is strictly below the cutoff.
func TestQueryStaleCreatingTxs(t *testing.T) {
	tx := uniqueTx(tests.Tx.Clone())

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	_, err := store.Create(ctx, tests.ParentTx, 100)
	require.NoError(t, err)

	// Three distinct txids (clones differ by version) sharing the same shape.
	staleTx := tx.Clone()
	staleTx.Version = 10

	freshTx := tx.Clone()
	freshTx.Version = 20

	finalTx := tx.Clone()
	finalTx.Version = 30

	// unminedSince is written from the create blockHeight for unmined creates.
	_, err = store.Create(ctx, staleTx, 100, utxo.WithCreating(true))
	require.NoError(t, err)

	_, err = store.Create(ctx, freshTx, 200, utxo.WithCreating(true))
	require.NoError(t, err)

	_, err = store.Create(ctx, finalTx, 100) // not creating
	require.NoError(t, err)

	hashes, err := store.QueryStaleCreatingTxs(ctx, 150, 0)
	require.NoError(t, err)
	require.Len(t, hashes, 1, "only the stale creating tx (unminedSince=100 < 150) must be returned")
	require.Equal(t, *staleTx.TxIDChainHash(), hashes[0])

	// A cutoff above the fresh tx's height picks up both creating txs, never the finalized one.
	hashes, err = store.QueryStaleCreatingTxs(ctx, 250, 0)
	require.NoError(t, err)
	require.ElementsMatch(t, []chainhash.Hash{*staleTx.TxIDChainHash(), *freshTx.TxIDChainHash()}, hashes)
}

// TestCreatingRecordWithMinedBlockInfo_UnminedSinceLifecycle verifies the sweep can see a
// creating record created with mined block info, and that finalize hands it back as a plain
// mined record: unminedSince is the sweep's selector while the record is creating, and is
// removed again once the record has block IDs and is no longer creating.
func TestCreatingRecordWithMinedBlockInfo_UnminedSinceLifecycle(t *testing.T) {
	tx := uniqueTx(tests.Tx.Clone())

	for _, batchSize := range []int{0, 2} {
		name := "single-record"
		if batchSize > 0 {
			name = "multi-record"
		}

		t.Run(name, func(t *testing.T) {
			logger := ulogger.NewErrorTestLogger(t)
			tSettings := test.CreateBaseTestSettings(t)

			if batchSize > 0 {
				tSettings.UtxoStore.UtxoBatchSize = batchSize
			}

			client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
			t.Cleanup(deferFn)
			cleanDB(t, client)

			_, err := store.Create(ctx, tests.ParentTx, 100)
			require.NoError(t, err)

			_, err = store.Create(ctx, tx, 100, utxo.WithCreating(true), utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{
				BlockID: 7, BlockHeight: 100, SubtreeIdx: 0, OnLongestChain: true,
			}))
			require.NoError(t, err)

			_, unminedSince := readMasterBins(t, client, store.GetNamespace(), store.GetName(), tx.TxIDChainHash())
			require.Equal(t, 100, unminedSince, "a creating record with block IDs carries unminedSince for the sweep")

			stale, err := store.QueryStaleCreatingTxs(ctx, 150, 0)
			require.NoError(t, err)
			require.Contains(t, stale, *tx.TxIDChainHash())

			require.NoError(t, store.FinalizeTransaction(ctx, tx, attemptOf(t, ctx, store, tx.TxIDChainHash()), attemptOf(t, ctx, store, tx.TxIDChainHash())))

			_, unminedSince = readMasterBins(t, client, store.GetNamespace(), store.GetName(), tx.TxIDChainHash())
			require.Nil(t, unminedSince, "finalize removes unminedSince from a record that has block IDs")

			it, err := store.GetUnminedTxIterator()
			require.NoError(t, err)

			require.NotContains(t, drainConflictingIterator(t, ctx, it), *tx.TxIDChainHash(), "a finalized mined record is not unmined")

			stale, err = store.QueryStaleCreatingTxs(ctx, 150, 0)
			require.NoError(t, err)
			require.NotContains(t, stale, *tx.TxIDChainHash())
		})
	}
}

// TestFinalizeTransaction_KeepsUnminedSinceWithoutBlockIDs verifies finalize leaves
// unminedSince alone on a record that has no block IDs: such a record is genuinely unmined.
func TestFinalizeTransaction_KeepsUnminedSinceWithoutBlockIDs(t *testing.T) {
	tx := uniqueTx(tests.Tx.Clone())

	for _, batchSize := range []int{0, 2} {
		name := "single-record"
		if batchSize > 0 {
			name = "multi-record"
		}

		t.Run(name, func(t *testing.T) {
			logger := ulogger.NewErrorTestLogger(t)
			tSettings := test.CreateBaseTestSettings(t)

			if batchSize > 0 {
				tSettings.UtxoStore.UtxoBatchSize = batchSize
			}

			client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
			t.Cleanup(deferFn)
			cleanDB(t, client)

			_, err := store.Create(ctx, tests.ParentTx, 100)
			require.NoError(t, err)

			_, err = store.Create(ctx, tx, 100, utxo.WithCreating(true))
			require.NoError(t, err)

			require.NoError(t, store.FinalizeTransaction(ctx, tx, attemptOf(t, ctx, store, tx.TxIDChainHash()), attemptOf(t, ctx, store, tx.TxIDChainHash())))

			_, unminedSince := readMasterBins(t, client, store.GetNamespace(), store.GetName(), tx.TxIDChainHash())
			require.Equal(t, 100, unminedSince)

			got := &meta.Data{}
			require.NoError(t, store.GetMeta(ctx, tx.TxIDChainHash(), got))
			require.False(t, got.Creating)
		})
	}
}

// TestFinalizeTransaction_StaleAttemptIsRefused pins that a finalize carries the token of the
// attempt it recovers: one queued from an earlier attempt of the same txid is refused on the later
// attempt's record, on the master and on every pagination child, so the later attempt's creating
// bin stays until its own spends are made and it finalizes with its own token.
func TestFinalizeTransaction_StaleAttemptIsRefused(t *testing.T) {
	tx := uniqueTx(tests.Tx.Clone())

	for name, batchSize := range map[string]int{"single record": 0, "multi record": 2} {
		t.Run(name, func(t *testing.T) {
			logger := ulogger.NewErrorTestLogger(t)
			tSettings := test.CreateBaseTestSettings(t)

			if batchSize > 0 {
				tSettings.UtxoStore.UtxoBatchSize = batchSize // tx has 6 outputs → multiple records
			}

			client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
			t.Cleanup(deferFn)
			cleanDB(t, client)

			_, err := store.Create(ctx, tests.ParentTx, 100)
			require.NoError(t, err)

			hash := tx.TxIDChainHash()

			first, err := store.Create(ctx, tx, 100, utxo.WithCreating(true))
			require.NoError(t, err)

			began, err := beginAsCreator(ctx, store, hash, first.Attempt)
			require.NoError(t, err)
			require.True(t, began)
			require.NoError(t, store.ReleaseRollbackHold(ctx, hash, first.Attempt))
			require.NoError(t, store.EndCreateFirstRollback(ctx, hash, first.Attempt))

			cleared, err := store.ClearCreateFirstHold(ctx, hash, true)
			require.NoError(t, err)
			require.True(t, cleared)

			second, err := store.Create(ctx, tx, 100, utxo.WithCreating(true))
			require.NoError(t, err)
			require.NotEqual(t, first.Attempt, second.Attempt, "every attempt draws its own token")

			err = store.FinalizeTransaction(ctx, tx, first.Attempt, first.Attempt)
			require.ErrorIs(t, err, errors.ErrTxNotFound, "a finalize of the earlier attempt must be refused on the later attempt's record")
			require.Contains(t, err.Error(), "another attempt")

			require.ErrorIs(t, store.FinalizeTransaction(ctx, tx, nil, nil), errors.ErrTxNotFound, "a finalize with no token is not the later attempt's either")

			got := &meta.Data{}
			require.NoError(t, store.GetMeta(ctx, hash, got))
			require.True(t, got.Creating, "the later attempt's creating bin must survive the stale finalize")
			require.False(t, got.RollingBack)
			require.Equal(t, second.Attempt, got.Attempt)

			if batchSize > 0 {
				for idx := uint32(1); idx < 3; idx++ {
					bins := recordBins(t, client, store, hash, idx, fields.Creating, fields.Attempt)
					require.Equal(t, true, bins[fields.Creating.String()], "child record %d must keep creating through the stale finalize", idx)
					require.Equal(t, second.Attempt, bins[fields.Attempt.String()], "child record %d carries the later attempt's token", idx)
				}
			}

			require.NoError(t, store.FinalizeTransaction(ctx, tx, second.Attempt, second.Attempt), "the later attempt finalizes with its own token")
			require.NoError(t, store.GetMeta(ctx, hash, got))
			require.False(t, got.Creating)

			if batchSize > 0 {
				for idx := uint32(1); idx < 3; idx++ {
					bins := recordBins(t, client, store, hash, idx, fields.Creating)
					require.NotContains(t, bins, fields.Creating.String(), "child record %d is cleared by the finalize of its own attempt", idx)
				}
			}
		})
	}
}

// TestFinalizeTransaction_AdoptsSurvivingChildren pins that a create which writes the master of a
// multi-record transaction afresh gives the pagination children an earlier attempt left behind its
// own token, so its finalize, which is filtered on that token, clears them too instead of leaving
// them creating for good.
func TestFinalizeTransaction_AdoptsSurvivingChildren(t *testing.T) {
	tx := uniqueTx(tests.Tx.Clone())

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.UtxoBatchSize = 2 // tx has 6 outputs → multiple records

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	hash := tx.TxIDChainHash()

	first, err := store.Create(ctx, tx, 100, utxo.WithCreating(true))
	require.NoError(t, err)

	deleteMasterRecordByHand(t, client, store, hash)

	second, err := store.Create(ctx, tx, 100, utxo.WithCreating(true))
	require.NoError(t, err, "the master is created afresh and the children are already there")
	require.NotEqual(t, first.Attempt, second.Attempt)

	for idx := uint32(1); idx < 3; idx++ {
		bins := recordBins(t, client, store, hash, idx, fields.Attempt)
		require.Equal(t, second.Attempt, bins[fields.Attempt.String()], "child record %d is adopted by the new master's attempt", idx)
	}

	require.NoError(t, store.FinalizeTransaction(ctx, tx, second.Attempt, second.Attempt))

	for idx := uint32(1); idx < 3; idx++ {
		bins := recordBins(t, client, store, hash, idx, fields.Creating)
		require.NotContains(t, bins, fields.Creating.String(), "child record %d is cleared by the finalize", idx)
	}
}

// TestFinalizeTransaction_PaginatedRecordsItsCaller is the ownership rule on a transaction with
// pagination children, whose finalize goes through clearCreatingFlag.
func TestFinalizeTransaction_PaginatedRecordsItsCaller(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.UtxoBatchSize = 4

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	paginated := createTransactionWithOutputs(10)
	hash := paginated.TxIDChainHash()

	md, err := store.Create(ctx, paginated, 100, utxo.WithCreating(true))
	require.NoError(t, err)

	lease, took, err := store.TakeSpendLease(ctx, hash, md.Attempt)
	require.NoError(t, err)
	require.True(t, took)

	require.NoError(t, store.FinalizeTransaction(ctx, paginated, md.Attempt, lease))
	require.NoError(t, store.FinalizeTransaction(ctx, paginated, md.Attempt, lease), "a repeat is the caller's own write")

	lost := store.FinalizeTransaction(ctx, paginated, md.Attempt, md.Attempt)
	require.True(t, utxo.IsAlreadyFinal(lost), "the creator did not clear the state, got: %v", lost)

	for idx := uint32(0); idx <= 2; idx++ {
		require.NotContains(t, recordBins(t, client, store, hash, idx, fields.Creating), fields.Creating.String(), "record %d", idx)
	}
}

// TestFinalizeTransaction_AnAbandonedFinalizeIsNotSent covers a caller whose context ends while its
// finalize is queued. The write of such a finalize must not land later: it would clear the creating
// bin and stamp the record with the lease of a caller that has returned an error, and no caller would
// own the hand-off to block assembly, because every recovery path selects on the creating bin.
func TestFinalizeTransaction_AnAbandonedFinalizeIsNotSent(t *testing.T) {
	tx := uniqueTx(tests.Tx.Clone())

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	_, err := store.Create(ctx, tests.ParentTx, 100)
	require.NoError(t, err)

	_, err = store.Create(ctx, tx, 100, utxo.WithCreating(true))
	require.NoError(t, err)

	token := attemptOf(t, ctx, store, tx.TxIDChainHash())

	var writes atomic.Int32

	store.SetBatchOperateFn(func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
		for _, record := range records {
			if write, ok := record.(*aerospike.BatchWrite); ok && write.Policy != nil && write.Policy.RecordExistsAction == aerospike.UPDATE_ONLY {
				writes.Add(1)
			}
		}

		return client.BatchOperate(policy, records)
	})
	t.Cleanup(func() { store.SetBatchOperateFn(nil) })

	abandoned, cancel := context.WithCancel(ctx)
	cancel()

	require.Error(t, store.FinalizeTransaction(abandoned, tx, token, token), "the caller gave up")

	// The batcher sends what it holds after its batch duration; wait for it to have done so.
	time.Sleep(500 * time.Millisecond)

	require.Zero(t, writes.Load(), "the finalize of a caller that gave up must not be written")

	got := &meta.Data{}
	require.NoError(t, store.GetMeta(ctx, tx.TxIDChainHash(), got))
	require.True(t, got.Creating, "the record is still creating: a roll-forward or the sweep owns it")

	require.NoError(t, store.FinalizeTransaction(ctx, tx, token, token), "a live caller still finalizes it")
}

// TestFinalizeTransaction_AFinalizeThatTimedOutIsNotSent covers a caller that gives up without its
// context ending: the wait for the batcher runs out while the finalize is still queued. The write must
// not land later, or the record would be final and stamped with the lease of a caller that returned an
// error, and no caller would own the hand-off.
func TestFinalizeTransaction_AFinalizeThatTimedOutIsNotSent(t *testing.T) {
	tx := uniqueTx(tests.Tx.Clone())

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.LockedBatcherDurationMillis = 400 // the finalize waits in the batcher this long

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	_, err := store.Create(ctx, tests.ParentTx, 100)
	require.NoError(t, err)

	_, err = store.Create(ctx, tx, 100, utxo.WithCreating(true))
	require.NoError(t, err)

	token := attemptOf(t, ctx, store, tx.TxIDChainHash())

	var writes atomic.Int32

	store.SetBatchOperateFn(func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
		for _, record := range records {
			if write, ok := record.(*aerospike.BatchWrite); ok && write.Policy != nil && write.Policy.RecordExistsAction == aerospike.UPDATE_ONLY {
				writes.Add(1)
			}
		}

		return client.BatchOperate(policy, records)
	})
	t.Cleanup(func() { store.SetBatchOperateFn(nil) })

	store.SetBatcherWait(30 * time.Millisecond)

	err = store.FinalizeTransaction(ctx, tx, token, token)
	require.ErrorIs(t, err, errors.ErrServiceUnavailable, "the caller's context is live, and the wait for the batcher ran out")

	time.Sleep(1200 * time.Millisecond) // the batch window of 400 ms has passed

	require.Zero(t, writes.Load(), "the finalize of a caller that timed out must not be written")

	got := &meta.Data{}
	require.NoError(t, store.GetMeta(ctx, tx.TxIDChainHash(), got))
	require.True(t, got.Creating, "the record is still creating: a roll-forward or the sweep owns it")
}

// countMasterWrites counts the finalize writes of the master record of hash that reach the batch
// override, and runs after once the batch that carries the children of a paginated transaction is done.
func countMasterWrites(t *testing.T, client *uaerospike.Client, store *teranode_aerospike.Store, hash *chainhash.Hash, after func()) *atomic.Int32 {
	t.Helper()

	master, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), uaerospike.CalculateKeySourceInternal(hash, 0))
	require.NoError(t, err)

	var masterWrites atomic.Int32

	store.SetBatchOperateFn(func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
		children := false

		for _, record := range records {
			write, ok := record.(*aerospike.BatchWrite)
			if !ok || write.Policy == nil || write.Policy.RecordExistsAction != aerospike.UPDATE_ONLY {
				continue
			}

			if write.Key.Equals(master) {
				masterWrites.Add(1)
			} else {
				children = true
			}
		}

		err := client.BatchOperate(policy, records)

		if children && after != nil {
			after()
		}

		return err
	})
	t.Cleanup(func() { store.SetBatchOperateFn(nil) })

	return &masterWrites
}

// TestFinalizeTransaction_PaginatedMasterIsNotSentAfterItsCallerIsGone covers a paginated finalize
// whose caller runs out of time while the child records are being cleared. The master write clears
// the creating bin and stamps the lease, so it must not start after that deadline: the record would
// be final with no caller to own the hand-off, and no recovery path selects it once the bin is gone.
func TestFinalizeTransaction_PaginatedMasterIsNotSentAfterItsCallerIsGone(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.UtxoBatchSize = 4

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	paginated := createTransactionWithOutputs(10)
	hash := paginated.TxIDChainHash()

	md, err := store.Create(ctx, paginated, 100, utxo.WithCreating(true))
	require.NoError(t, err)

	lease, took, err := store.TakeSpendLease(ctx, hash, md.Attempt)
	require.NoError(t, err)
	require.True(t, took)

	callerCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	masterWrites := countMasterWrites(t, client, store, hash, func() {
		<-callerCtx.Done()
	})

	require.Error(t, store.FinalizeTransaction(callerCtx, paginated, md.Attempt, lease), "the caller ran out of time")
	require.Zero(t, masterWrites.Load(), "no master write may start after the deadline of its caller")

	got := &meta.Data{}
	require.NoError(t, store.GetMeta(ctx, hash, got))
	require.True(t, got.Creating, "the record is still creating: a roll-forward or the sweep owns it")

	require.NoError(t, store.FinalizeTransaction(ctx, paginated, md.Attempt, lease), "a live caller still finalizes it")
}

// failMasterFinalizeWrite makes the finalize write of the master record of hash fail with a transport
// error. With landed set, the write is sent first and its answer is lost, which is the doubtful
// outcome of a real transport failure. Writes to other records go through.
func failMasterFinalizeWrite(t *testing.T, client *uaerospike.Client, store *teranode_aerospike.Store, hash *chainhash.Hash, landed bool) {
	t.Helper()

	master, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), uaerospike.CalculateKeySourceInternal(hash, 0))
	require.NoError(t, err)

	store.SetBatchOperateFn(func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
		for _, record := range records {
			write, ok := record.(*aerospike.BatchWrite)
			if !ok || write.Policy == nil || write.Policy.RecordExistsAction != aerospike.UPDATE_ONLY || !write.Key.Equals(master) {
				continue
			}

			if landed {
				_ = client.BatchOperate(policy, records)
			}

			return aerospike.ErrNetwork
		}

		return client.BatchOperate(policy, records)
	})
	t.Cleanup(func() { store.SetBatchOperateFn(nil) })
}

// failAChildFinalizeWrite makes the finalize write of the last pagination child of hash fail, and
// lets the writes of the other children through: the partial child phase of a real batch failure.
func failAChildFinalizeWrite(t *testing.T, client *uaerospike.Client, store *teranode_aerospike.Store, hash *chainhash.Hash) {
	t.Helper()

	master, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), uaerospike.CalculateKeySourceInternal(hash, 0))
	require.NoError(t, err)

	store.SetBatchOperateFn(func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
		children := len(records) > 1

		for _, record := range records {
			write, ok := record.(*aerospike.BatchWrite)
			if !ok || write.Policy == nil || write.Policy.RecordExistsAction != aerospike.UPDATE_ONLY || write.Key.Equals(master) {
				children = false
			}
		}

		if !children {
			return client.BatchOperate(policy, records)
		}

		last := len(records) - 1

		if sendErr := client.BatchOperate(policy, records[:last]); sendErr != nil {
			return sendErr
		}

		records[last].BatchRec().Err = aerospike.ErrNetwork

		return nil
	})
	t.Cleanup(func() { store.SetBatchOperateFn(nil) })
}

// paginatedCreateFirst stores a parent with one output and the tentative record of a transaction
// that spends it and has ten outputs, which is three records at a batch size of four: the master
// holds outputs 0 to 3 and the pagination children outputs 4 to 7 and 8 to 9. The input is spent,
// as it is before any finalize runs.
func paginatedCreateFirst(t *testing.T, ctx context.Context, store *teranode_aerospike.Store, seed int) (*bt.Tx, *meta.Data) {
	t.Helper()

	require.NoError(t, store.SetBlockHeight(1000))

	parent := conflictingIndexParent(t, ctx, store, seed, 1)
	paginated := conflictingIndexTx(t, parent, []uint32{0}, 10)

	md, err := store.Create(ctx, paginated, 900, utxo.WithCreating(true))
	require.NoError(t, err)

	_, err = store.Spend(ctx, paginated, 900)
	require.NoError(t, err, "precondition: the attempt spent its input")

	return paginated, md
}

// requireNoRollbackCanClaim asserts that the record of the attempt is finalizing and that neither
// the claim of a rollback nor a whole rollback begins, with no spend lease left to refuse it for
// another reason, and that the pagination children and the spend of the input are still there.
func requireNoRollbackCanClaim(t *testing.T, ctx context.Context, client *uaerospike.Client, store *teranode_aerospike.Store, logger ulogger.Logger, paginated *bt.Tx, attempt []byte) {
	t.Helper()

	hash := paginated.TxIDChainHash()

	master := recordBins(t, client, store, hash, 0, fields.Creating, fields.Finalizing)
	require.Contains(t, master, fields.Creating.String(), "the master is still creating, so every recovery path still selects it")
	require.Equal(t, true, master[fields.Finalizing.String()], "the master carries the mark of a finalize that has begun")

	got := &meta.Data{}
	require.NoError(t, store.GetMeta(ctx, hash, got))
	require.True(t, got.Creating)
	require.True(t, got.Finalizing, "a read of the record reports the mark")

	began, err := beginAsCreator(ctx, store, hash, attempt)
	require.NoError(t, err)
	require.False(t, began, "no rollback may claim a record whose finalize has begun")

	live, err := store.SpendLeasesLive(ctx, hash, attempt)
	require.NoError(t, err)
	require.False(t, live, "the claim was refused for the mark, not for a spend lease")

	ended, err := utxo.RollBackCreatingEnded(ctx, logger, store, paginated, attempt)
	require.NoError(t, err)
	require.False(t, ended, "nothing is rolled back")

	for idx := uint32(1); idx <= 2; idx++ {
		require.NotNil(t, recordBins(t, client, store, hash, idx, fields.Attempt), "pagination child %d is not deleted", idx)
	}

	resp, err := store.GetSpend(ctx, spendHeightSpendsOf(t, paginated)[0])
	require.NoError(t, err)
	require.NotNil(t, resp.SpendingData, "the input of the attempt is still spent")
	require.Equal(t, hash.String(), resp.SpendingData.TxID.String())
}

// requireSpentBy asserts that the first input of spender is recorded as spent by it.
func requireSpentBy(t *testing.T, ctx context.Context, store *teranode_aerospike.Store, spender *bt.Tx, msg string) {
	t.Helper()

	resp, err := store.GetSpend(ctx, spendHeightSpendsOf(t, spender)[0])
	require.NoError(t, err)
	require.NotNil(t, resp.SpendingData, msg)
	require.Equal(t, spender.TxIDChainHash().String(), resp.SpendingData.TxID.String(), msg)
}

// TestFinalizeTransaction_PaginatedMasterWriteRefusedLeavesARecordNoRollbackCanClaim covers a
// paginated finalize whose child phase committed and whose master write did not. A child with no
// creating bin accepts a spend, because the spend gate is the bin of the record that holds the
// output, while the master is still creating. The end of a rollback deletes the children, so a
// rollback of that record would delete a page under the spend of a descendant. The finalize marks
// the master before it clears a child, and no rollback claims a marked master: the record can only
// be completed, which a repeat of the finalize does.
func TestFinalizeTransaction_PaginatedMasterWriteRefusedLeavesARecordNoRollbackCanClaim(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.UtxoBatchSize = 4

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	paginated, md := paginatedCreateFirst(t, ctx, store, 8201)
	hash := paginated.TxIDChainHash()

	failMasterFinalizeWrite(t, client, store, hash, false)

	require.Error(t, store.FinalizeTransaction(ctx, paginated, md.Attempt, md.Attempt), "the master write failed")

	store.SetBatchOperateFn(nil)

	for idx := uint32(1); idx <= 2; idx++ {
		require.NotContains(t, recordBins(t, client, store, hash, idx, fields.Creating), fields.Creating.String(), "precondition: pagination child %d is cleared, so its outputs are spendable", idx)
	}

	descendant := conflictingIndexTx(t, paginated, []uint32{5}, 1)

	_, err := store.Spend(ctx, descendant, 901)
	require.NoError(t, err, "precondition: a descendant spends an output of a cleared child")

	requireNoRollbackCanClaim(t, ctx, client, store, logger, paginated, md.Attempt)
	requireSpentBy(t, ctx, store, descendant, "the spend of the descendant is still in place")

	require.NoError(t, store.FinalizeTransaction(ctx, paginated, md.Attempt, md.Attempt), "a repeat of the finalize completes the record")

	for idx := uint32(0); idx <= 2; idx++ {
		require.NotContains(t, recordBins(t, client, store, hash, idx, fields.Creating), fields.Creating.String(), "record %d", idx)
	}

	require.NotContains(t, recordBins(t, client, store, hash, 0, fields.Finalizing), fields.Finalizing.String(), "the mark goes with the creating bin")
	requireSpentBy(t, ctx, store, descendant, "the spend of the descendant outlives the finalize")
}

// TestFinalizeTransaction_PaginatedPartialChildWriteLeavesARecordNoRollbackCanClaim covers the
// child phase that fails part-way: one child is cleared and spendable, another is not, and the
// master write is never sent. The mark is on the master before the first child is cleared, so this
// record cannot be rolled back either.
func TestFinalizeTransaction_PaginatedPartialChildWriteLeavesARecordNoRollbackCanClaim(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.UtxoBatchSize = 4

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	paginated, md := paginatedCreateFirst(t, ctx, store, 8202)
	hash := paginated.TxIDChainHash()

	failAChildFinalizeWrite(t, client, store, hash)

	require.Error(t, store.FinalizeTransaction(ctx, paginated, md.Attempt, md.Attempt), "one child write failed")

	store.SetBatchOperateFn(nil)

	require.NotContains(t, recordBins(t, client, store, hash, 1, fields.Creating), fields.Creating.String(), "precondition: the first child is cleared")
	require.Contains(t, recordBins(t, client, store, hash, 2, fields.Creating), fields.Creating.String(), "precondition: the second child is not")

	descendant := conflictingIndexTx(t, paginated, []uint32{5}, 1)

	_, err := store.Spend(ctx, descendant, 901)
	require.NoError(t, err, "precondition: a descendant spends an output of the cleared child")

	_, err = store.Spend(ctx, conflictingIndexTx(t, paginated, []uint32{9}, 1), 901)
	require.ErrorIs(t, err, errors.ErrTxCreating, "an output of the child that was not cleared is still gated")

	requireNoRollbackCanClaim(t, ctx, client, store, logger, paginated, md.Attempt)
	requireSpentBy(t, ctx, store, descendant, "the spend of the descendant is still in place")

	require.NoError(t, store.FinalizeTransaction(ctx, paginated, md.Attempt, md.Attempt), "a repeat of the finalize completes the record")

	for idx := uint32(0); idx <= 2; idx++ {
		require.NotContains(t, recordBins(t, client, store, hash, idx, fields.Creating), fields.Creating.String(), "record %d", idx)
	}

	require.NotContains(t, recordBins(t, client, store, hash, 0, fields.Finalizing), fields.Finalizing.String(), "the mark goes with the creating bin")
}

// A master write whose answer was lost may have landed. The record is then final: every child was
// cleared before the write, the mark went with the creating bin, and a repeat of the finalize by the
// same caller reads its own lease on the record and reports success.
func TestFinalizeTransaction_PaginatedMasterWriteThatLandedLeavesAFinalRecord(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.UtxoBatchSize = 4

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	paginated, md := paginatedCreateFirst(t, ctx, store, 8203)
	hash := paginated.TxIDChainHash()

	lease, took, err := store.TakeSpendLease(ctx, hash, md.Attempt)
	require.NoError(t, err)
	require.True(t, took)

	failMasterFinalizeWrite(t, client, store, hash, true)

	require.Error(t, store.FinalizeTransaction(ctx, paginated, md.Attempt, lease), "the answer of the master write was lost")

	store.SetBatchOperateFn(nil)

	for idx := uint32(0); idx <= 2; idx++ {
		require.NotContains(t, recordBins(t, client, store, hash, idx, fields.Creating), fields.Creating.String(), "record %d: the write landed, so the record is final", idx)
	}

	require.NotContains(t, recordBins(t, client, store, hash, 0, fields.Finalizing), fields.Finalizing.String(), "a final record carries no mark")

	require.NoError(t, store.FinalizeTransaction(ctx, paginated, md.Attempt, lease), "the repeat finds its own lease on the final record")

	other := store.FinalizeTransaction(ctx, paginated, md.Attempt, md.Attempt)
	require.True(t, utxo.IsAlreadyFinal(other), "another caller did not clear the state, got: %v", other)
}

// TestMarkFinalizing_IsRefusedForARecordItsAttemptCannotComplete pins the filter of the write that
// marks a master finalizing. The mark is what refuses a rollback, so it must never land on a record
// whose rollback has begun, or on the record of another attempt: the children of such a record are
// not cleared by this finalize, and a mark would leave a rolling back record that reads as one that
// can only be completed.
func TestMarkFinalizing_IsRefusedForARecordItsAttemptCannotComplete(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.UtxoBatchSize = 4

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	t.Run("a rollback that began first refuses the mark and keeps every child gated", func(t *testing.T) {
		paginated, md := paginatedCreateFirst(t, ctx, store, 8204)
		hash := paginated.TxIDChainHash()

		began, err := beginAsCreator(ctx, store, hash, md.Attempt)
		require.NoError(t, err)
		require.True(t, began, "precondition: the rollback owns the record")

		done, err := store.MarkFinalizingForTest(ctx, hash, md.Attempt, md.Attempt)
		require.ErrorIs(t, err, errors.ErrTxNotFound)
		require.False(t, done)

		require.ErrorIs(t, store.FinalizeTransaction(ctx, paginated, md.Attempt, md.Attempt), errors.ErrTxNotFound)

		require.NotContains(t, recordBins(t, client, store, hash, 0, fields.Finalizing), fields.Finalizing.String(), "a rolling back master is not marked")

		for idx := uint32(1); idx <= 2; idx++ {
			require.Contains(t, recordBins(t, client, store, hash, idx, fields.Creating), fields.Creating.String(), "child %d is still gated", idx)
		}
	})

	t.Run("the record of another attempt is not marked", func(t *testing.T) {
		paginated, _ := paginatedCreateFirst(t, ctx, store, 8205)
		hash := paginated.TxIDChainHash()

		done, err := store.MarkFinalizingForTest(ctx, hash, []byte("another attempt!"), nil)
		require.ErrorIs(t, err, errors.ErrTxNotFound)
		require.False(t, done)

		require.NotContains(t, recordBins(t, client, store, hash, 0, fields.Finalizing), fields.Finalizing.String())
	})

	t.Run("a record with no master is reported as gone", func(t *testing.T) {
		absent := conflictingIndexTx(t, conflictingIndexParent(t, ctx, store, 8206, 1), []uint32{0}, 10)

		done, err := store.MarkFinalizingForTest(ctx, absent.TxIDChainHash(), []byte("an attempt token"), nil)
		require.ErrorIs(t, err, errors.ErrTxNotFound)
		require.False(t, done)
	})

	t.Run("a record that is final under the lease of the caller is done, and under another lease is not", func(t *testing.T) {
		paginated, md := paginatedCreateFirst(t, ctx, store, 8207)
		hash := paginated.TxIDChainHash()

		require.NoError(t, store.FinalizeTransaction(ctx, paginated, md.Attempt, md.Attempt))

		done, err := store.MarkFinalizingForTest(ctx, hash, md.Attempt, md.Attempt)
		require.NoError(t, err)
		require.True(t, done, "an earlier send of the same finalize cleared the record")

		_, err = store.MarkFinalizingForTest(ctx, hash, md.Attempt, []byte("some other lease"))
		require.True(t, utxo.IsAlreadyFinal(err), "got: %v", err)

		require.NotContains(t, recordBins(t, client, store, hash, 0, fields.Finalizing), fields.Finalizing.String(), "a final record is not marked")
	})

	t.Run("a caller that has run out of time sends no mark", func(t *testing.T) {
		paginated, md := paginatedCreateFirst(t, ctx, store, 8208)
		hash := paginated.TxIDChainHash()

		gone, cancel := context.WithCancel(ctx)
		cancel()

		_, err := store.MarkFinalizingForTest(gone, hash, md.Attempt, md.Attempt)
		require.ErrorIs(t, err, context.Canceled)

		require.NotContains(t, recordBins(t, client, store, hash, 0, fields.Finalizing), fields.Finalizing.String())
	})
}

// TestFinalizeTransaction_AFinalizeThatCannotEndLeavesTheAttemptOpenToARollback covers the other
// side of the mark. It takes the rollback away, so a finalize that can never clear every child must
// not write it: a child that another attempt owns, or one that is gone. Such a record cannot be
// completed, and with the mark on it nothing could roll it back either. The children are read
// before the mark, the finalize fails with no child cleared, and the rollback of the attempt runs.
func TestFinalizeTransaction_AFinalizeThatCannotEndLeavesTheAttemptOpenToARollback(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.UtxoBatchSize = 4

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	childKey := func(t *testing.T, hash *chainhash.Hash, idx uint32) *aerospike.Key {
		key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), uaerospike.CalculateKeySourceInternal(hash, idx))
		require.NoError(t, err)

		return key
	}

	for name, breakChild := range map[string]func(t *testing.T, hash *chainhash.Hash){
		"a child under the token of another attempt": func(t *testing.T, hash *chainhash.Hash) {
			policy := aerospike.NewWritePolicy(0, 0)
			policy.RecordExistsAction = aerospike.UPDATE_ONLY

			require.Nil(t, client.PutBins(policy, childKey(t, hash, 2), aerospike.NewBin(fields.Attempt.String(), []byte("another attempt!"))))
		},
		"a child that is gone": func(t *testing.T, hash *chainhash.Hash) {
			existed, err := client.Delete(aerospike.NewWritePolicy(0, 0), childKey(t, hash, 2))
			require.Nil(t, err)
			require.True(t, existed)
		},
	} {
		t.Run(name, func(t *testing.T) {
			paginated, md := paginatedCreateFirst(t, ctx, store, 8300+len(name))
			hash := paginated.TxIDChainHash()

			breakChild(t, hash)

			require.Error(t, store.FinalizeTransaction(ctx, paginated, md.Attempt, md.Attempt), "the finalize cannot clear every child")

			master := recordBins(t, client, store, hash, 0, fields.Creating, fields.Finalizing)
			require.Contains(t, master, fields.Creating.String())
			require.NotContains(t, master, fields.Finalizing.String(), "a finalize that cannot end does not mark the master")
			require.Contains(t, recordBins(t, client, store, hash, 1, fields.Creating), fields.Creating.String(), "the child that the finalize could have cleared is left as it was")

			require.NoError(t, store.ReleaseSpendLease(ctx, hash, md.Attempt, md.Attempt), "the creator releases its lease when its finalize fails")

			ended, err := utxo.RollBackCreatingEnded(ctx, logger, store, paginated, md.Attempt)
			require.NoError(t, err)
			require.True(t, ended, "the attempt is rolled back")

			requireHeld(t, ctx, store, hash, "the rollback leaves a hold")

			resp, err := store.GetSpend(ctx, spendHeightSpendsOf(t, paginated)[0])
			require.NoError(t, err)
			require.Nil(t, resp.SpendingData, "the input is free again")
		})
	}
}

// TestRollForwardCreating_CompletesAFinalizingRecordWhoseParentRefusesTheRepeat covers the recovery
// of a record whose finalize began and did not end. No rollback can claim it, so it must not depend
// on a repeat of the spend either: the store refuses the repeat of a spend for what the parent is
// today, before it sees that the output is already spent by this transaction.
func TestRollForwardCreating_CompletesAFinalizingRecordWhoseParentRefusesTheRepeat(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.UtxoBatchSize = 4

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	paginated, md := paginatedCreateFirst(t, ctx, store, 8209)
	hash := paginated.TxIDChainHash()
	parent := paginated.Inputs[0].PreviousTxIDChainHash()

	failMasterFinalizeWrite(t, client, store, hash, false)

	require.Error(t, store.FinalizeTransaction(ctx, paginated, md.Attempt, md.Attempt), "the master write failed")

	store.SetBatchOperateFn(nil)

	require.NoError(t, store.ReleaseSpendLease(ctx, hash, md.Attempt, md.Attempt), "the creator releases its lease when its finalize fails")
	require.NoError(t, store.SetLocked(ctx, []chainhash.Hash{*parent}, true))

	_, err := store.Spend(ctx, paginated, 901)
	require.ErrorIs(t, err, errors.ErrTxLocked, "precondition: the repeat of the spend is refused although the output is spent by this transaction")

	spends, completed, err := utxo.RollForwardCreating(ctx, logger, store, paginated, md.Attempt, 901)
	require.NoError(t, err)
	require.True(t, completed, "this call finalized the record")
	require.Len(t, spends, 1)

	for idx := uint32(0); idx <= 2; idx++ {
		require.NotContains(t, recordBins(t, client, store, hash, idx, fields.Creating), fields.Creating.String(), "record %d", idx)
	}

	require.NotContains(t, recordBins(t, client, store, hash, 0, fields.Finalizing), fields.Finalizing.String())
	requireSpentBy(t, ctx, store, paginated, "the input is still spent by the transaction")
}

// A creating record whose finalize has not begun still depends on its re-spend: a locked parent
// refuses it and the record stays creating, for a later pass or for a rollback.
func TestRollForwardCreating_ARecordThatIsNotFinalizingStillNeedsItsSpend(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.UtxoBatchSize = 4

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	paginated, md := paginatedCreateFirst(t, ctx, store, 8210)
	hash := paginated.TxIDChainHash()
	parent := paginated.Inputs[0].PreviousTxIDChainHash()

	require.NoError(t, store.ReleaseSpendLease(ctx, hash, md.Attempt, md.Attempt))
	require.NoError(t, store.SetLocked(ctx, []chainhash.Hash{*parent}, true))

	_, completed, err := utxo.RollForwardCreating(ctx, logger, store, paginated, md.Attempt, 901)
	require.ErrorIs(t, err, errors.ErrTxLocked)
	require.False(t, completed)

	for idx := uint32(0); idx <= 2; idx++ {
		require.Contains(t, recordBins(t, client, store, hash, idx, fields.Creating), fields.Creating.String(), "record %d is still creating", idx)
	}

	began, err := beginAsCreator(ctx, store, hash, md.Attempt)
	require.NoError(t, err)
	require.True(t, began, "a record whose finalize has not begun can still be rolled back")
}

// TestFinalizeTransaction_PaginatedFinalizeOfALiveCaller is the other half: a caller that is still
// waiting gets its paginated finalize written, the master once.
func TestFinalizeTransaction_PaginatedFinalizeOfALiveCaller(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.UtxoBatchSize = 4

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	paginated := createTransactionWithOutputs(10)
	hash := paginated.TxIDChainHash()

	md, err := store.Create(ctx, paginated, 100, utxo.WithCreating(true))
	require.NoError(t, err)

	lease, took, err := store.TakeSpendLease(ctx, hash, md.Attempt)
	require.NoError(t, err)
	require.True(t, took)

	callerCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	masterWrites := countMasterWrites(t, client, store, hash, nil)

	require.NoError(t, store.FinalizeTransaction(callerCtx, paginated, md.Attempt, lease))
	require.Equal(t, int32(1), masterWrites.Load())

	for idx := uint32(0); idx <= 2; idx++ {
		require.NotContains(t, recordBins(t, client, store, hash, idx, fields.Creating), fields.Creating.String(), "record %d", idx)
	}
}

// TestFinalizeTransaction_CallerWithNoDeadlineIsBoundedByTheWait covers a caller whose context has
// no deadline. It waits no longer than the batcher wait, so no finalize write, of a single record
// or of a paginated transaction, may start after that: each write carries only the time left.
func TestFinalizeTransaction_CallerWithNoDeadlineIsBoundedByTheWait(t *testing.T) {
	tx := uniqueTx(tests.Tx.Clone())

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.UtxoBatchSize = 4

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	const wait = 20 * time.Second

	store.SetBatcherWait(wait)

	_, err := store.Create(ctx, tests.ParentTx, 100)
	require.NoError(t, err)

	candidates := map[string]*bt.Tx{"single record": tx, "paginated": createTransactionWithOutputs(10)}

	for name, candidate := range candidates {
		_, err = store.Create(ctx, candidate, 100, utxo.WithCreating(true))
		require.NoError(t, err, name)

		token := attemptOf(t, ctx, store, candidate.TxIDChainHash())

		var (
			finalizeWrites atomic.Int32
			unbounded      atomic.Int32
		)

		store.SetBatchOperateFn(func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
			for _, record := range records {
				if write, ok := record.(*aerospike.BatchWrite); ok && write.Policy != nil && write.Policy.RecordExistsAction == aerospike.UPDATE_ONLY {
					finalizeWrites.Add(1)

					if policy.TotalTimeout == 0 || policy.TotalTimeout > wait {
						unbounded.Add(1)
					}
				}
			}

			return client.BatchOperate(policy, records)
		})

		require.NoError(t, store.FinalizeTransaction(ctx, candidate, token, token), name)
		require.NotZero(t, finalizeWrites.Load(), name)
		require.Zero(t, unbounded.Load(), "%s: a finalize write carries more time than the caller waits", name)
	}

	store.SetBatchOperateFn(nil)
}
