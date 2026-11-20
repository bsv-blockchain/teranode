package sql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/require"
)

// TestTxInpointsErrorSameOnBothReadPaths pins that a failure building
// TxInpoints reaches the caller on both read paths (issue #1830).
// getUnbatched returned it; batchDecorateChunk discarded it and handed back
// empty TxInpoints with no error, so the answer depended on
// utxostore_getBatcherSize. go-subtree never fails here today, so the test
// makes it fail.
func TestTxInpointsErrorSameOnBothReadPaths(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, tx := setup(ctx, t)

	_, err := store.Create(ctx, tx, 12345)
	require.NoError(t, err)

	hash := tx.TxIDChainHash()

	original := newTxInpointsFromInputs
	newTxInpointsFromInputs = func([]*bt.Input) (subtree.TxInpoints, error) {
		return subtree.TxInpoints{}, errors.NewProcessingError("inputs rejected")
	}
	t.Cleanup(func() { newTxInpointsFromInputs = original })

	for _, bins := range [][]fields.FieldName{{fields.TxInpoints}, utxo.MetaFields} {
		_, err := store.getUnbatched(ctx, hash, bins)
		require.Error(t, err, "unbatched %v", bins)
		require.True(t, errors.Is(err, errors.ErrProcessing), "unbatched %v: %v", bins, err)

		items := []*utxo.UnresolvedMetaData{{Hash: *hash, Idx: 0}}
		require.NoError(t, store.BatchDecorate(ctx, items, bins...))
		require.Error(t, items[0].Err, "batched %v", bins)
		require.True(t, errors.Is(items[0].Err, errors.ErrProcessing), "batched %v: %v", bins, items[0].Err)
		require.Nil(t, items[0].Data, "batched %v: no partial data alongside the error", bins)
	}
}
