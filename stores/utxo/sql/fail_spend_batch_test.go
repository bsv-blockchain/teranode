package sql

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/stretchr/testify/require"
)

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
