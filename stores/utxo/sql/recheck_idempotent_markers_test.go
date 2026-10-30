package sql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/stretchr/testify/require"
)

// TestRecheckIdempotentMarkersKeepsAnsweredItemsOnFailure: when the marker
// re-check's statement fails, every item of the batch is answered at once. An
// item the batch had already refused must keep that refusal. Overwriting it
// with a storage error turned a pruned-replay rejection, which is final, into
// an error the caller retries, as the commit and DAH failure paths of the same
// batch already avoid.
//
// The statement is made to fail by running it on a transaction that has
// already been rolled back, which fails the same way on both engines.
func TestRecheckIdempotentMarkersKeepsAnsweredItemsOnFailure(t *testing.T) {
	forEachBackend(t, func(t *testing.T, ctx context.Context, store *Store) {
		newItem := func(seed byte) *batchSpend {
			txID := chainhash.HashH([]byte{seed})
			spender := chainhash.HashH([]byte{seed, 0x01})

			return &batchSpend{
				spend: &utxo.Spend{TxID: &txID, Vout: 0, SpendingData: spendpkg.NewSpendingData(&spender, 0)},
				errCh: make(chan error, 1),
			}
		}

		refused, pending := newItem(0x01), newItem(0x02)
		batch := []*batchSpend{refused, pending}
		resultMap := map[int]*spendSelectResult{0: {batchIdx: 0, transactionID: 1}, 1: {batchIdx: 1, transactionID: 2}}

		replay := errors.NewUtxoSpendingTxPrunedError("[Spend] invalid spend: spending transaction was pruned")
		validationErrors := map[int]error{0: replay}

		txn, err := store.db.BeginTx(ctx, nil)
		require.NoError(t, err)
		require.NoError(t, txn.Rollback())

		retryable, aborted := store.recheckIdempotentMarkers(txn, batch, resultMap, []int{1}, validationErrors)
		require.False(t, retryable)
		require.True(t, aborted, "fixture: the re-check statement must fail")

		require.ErrorIs(t, <-refused.errCh, errors.ErrUtxoSpendingTxPruned, "an item already refused keeps its refusal")
		require.ErrorIs(t, <-pending.errCh, errors.ErrStorageError, "an item not yet answered gets the storage error")
	})
}

// TestFailSpendBatchKeepsAnsweredItems: every failure arm of the bulk spend
// path answers the batch through failSpendBatch. A pruned-replay refusal must
// survive a failure on a sibling input's statement. As a storage error it was
// retried, and utxo.AnyPrunedReplay no longer saw it, so the rollback reversed
// the transaction's idempotent matches, which are confirmed historical spends.
func TestFailSpendBatchKeepsAnsweredItems(t *testing.T) {
	newItem := func(seed byte) *batchSpend {
		txID := chainhash.HashH([]byte{seed})
		spender := chainhash.HashH([]byte{seed, 0x01})

		return &batchSpend{
			spend: &utxo.Spend{TxID: &txID, Vout: 0, SpendingData: spendpkg.NewSpendingData(&spender, 0)},
			errCh: make(chan error, 1),
		}
	}

	pruned, spent, pending := newItem(0x01), newItem(0x02), newItem(0x03)
	batch := []*batchSpend{pruned, spent, pending}

	validationErrors := map[int]error{
		0: errors.NewUtxoSpendingTxPrunedError("[Spend] invalid spend: spending transaction was pruned"),
		1: errors.NewUtxoSpentError(*spent.spend.TxID, 0, chainhash.Hash{}, spent.spend.SpendingData),
	}

	failSpendBatch(batch, validationErrors, "[Spend] failed: scanning bulk UPDATE results", errors.NewProcessingError("driver error"))

	pruned.spend.Err, spent.spend.Err, pending.spend.Err = <-pruned.errCh, <-spent.errCh, <-pending.errCh

	require.ErrorIs(t, pruned.spend.Err, errors.ErrUtxoSpendingTxPruned, "a pruned replay keeps its refusal")
	require.ErrorIs(t, spent.spend.Err, errors.ErrSpent, "a spent refusal keeps its refusal")
	require.ErrorIs(t, pending.spend.Err, errors.ErrStorageError, "an item not yet answered gets the storage error")
	require.True(t, utxo.AnyPrunedReplay([]*utxo.Spend{pruned.spend, spent.spend, pending.spend}),
		"the rollback must still see the pruned replay and hold back historical spends")
}
