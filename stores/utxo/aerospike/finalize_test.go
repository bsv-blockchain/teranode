package aerospike_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestFinalizeTransaction verifies that FinalizeTransaction clears the tentative
// creating state (making outputs spendable), and is idempotent / a no-op for
// transactions that were never created in the creating state.
func TestFinalizeTransaction(t *testing.T) {
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

// cancelledAfterStartContext reports itself cancelled through Err but never closes Done, so a
// FinalizeTransaction wait runs to batch completion and only then observes the cancellation.
type cancelledAfterStartContext struct {
	context.Context
}

func (cancelledAfterStartContext) Done() <-chan struct{} { return nil }

func (cancelledAfterStartContext) Err() error { return context.Canceled }

// TestFinalizeTransaction_ContextCancellation pins the return value when the caller's context is
// cancelled: before the call the finalize reports the context error; once the batch completed the
// finalize reports success because the record is finalized. The batch still runs for an item that
// was already enqueued, so the record state after a cancelled call is not asserted.
func TestFinalizeTransaction_ContextCancellation(t *testing.T) {
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

		err = store.FinalizeTransaction(cancelledAfterStartContext{Context: ctx}, tx, attemptOf(t, ctx, store, tx.TxIDChainHash()), attemptOf(t, ctx, store, tx.TxIDChainHash()))
		require.NoError(t, err, "a finalize that landed must not be reported as failed")

		got := &meta.Data{}
		require.NoError(t, store.GetMeta(ctx, tx.TxIDChainHash(), got))
		require.False(t, got.Creating, "the batch must have finalized the record")
	})
}

// TestFinalizeTransaction_MultiRecord finalizes a paginated (multi-record) tx.
func TestFinalizeTransaction_MultiRecord(t *testing.T) {
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
