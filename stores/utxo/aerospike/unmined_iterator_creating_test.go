package aerospike_test

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	teranode_aerospike "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// drainUnmined returns the set of tx hashes emitted by the default-mode unmined iterator.
func drainUnmined(t *testing.T, ctx context.Context, store *teranode_aerospike.Store) map[chainhash.Hash]struct{} {
	t.Helper()

	it, err := store.GetUnminedTxIterator()
	require.NoError(t, err)

	seen := make(map[chainhash.Hash]struct{})

	for {
		batch, err := it.Next(ctx)
		require.NoError(t, err)

		if batch == nil {
			break
		}

		for _, u := range batch {
			if u == nil || u.Skip || u.Node == nil {
				continue
			}

			seen[u.Hash] = struct{}{}
		}
	}

	return seen
}

// TestUnminedIteratorSkipsCreatingRecords verifies that a tx still in the create-first
// "creating" state is NOT restored into block assembly by the default unmined iterator,
// and becomes visible once finalized.
func TestUnminedIteratorSkipsCreatingRecords(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	require.NoError(t, store.SetBlockHeight(100))

	// txA: normal unmined tx (must be emitted).
	txA := tx.Clone()
	txA.Version = 1

	// txB: unmined tx in the creating state (must NOT be emitted until finalized).
	txB := tx.Clone()
	txB.Version = 2

	_, err := store.Create(ctx, txA, 100)
	require.NoError(t, err)

	_, err = store.Create(ctx, txB, 100, utxo.WithCreating(true))
	require.NoError(t, err)

	seen := drainUnmined(t, ctx, store)
	require.Contains(t, seen, *txA.TxIDChainHash(), "a normal unmined tx must be restored")
	require.NotContains(t, seen, *txB.TxIDChainHash(), "a creating-state tx must not be restored into block assembly")

	// After finalize, txB becomes a normal unmined tx and is emitted.
	require.NoError(t, store.FinalizeTransaction(ctx, txB, attemptOf(t, ctx, store, txB.TxIDChainHash())))

	seen = drainUnmined(t, ctx, store)
	require.Contains(t, seen, *txB.TxIDChainHash(), "a finalized tx must be restored on the next pass")
}

// TestUnminedIteratorsSkipHolds verifies that the hold a rolled back create-first attempt leaves,
// which carries the unminedSince selector but no transaction data, is neither restored into block
// assembly by the default iterator nor an error for it or for the pruner's iterator.
func TestUnminedIteratorsSkipHolds(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	require.NoError(t, store.SetBlockHeight(100))

	txA := tx.Clone()
	txA.Version = 1

	held := tx.Clone()
	held.Version = 2

	_, err := store.Create(ctx, txA, 100)
	require.NoError(t, err)

	rolledBackHold(t, ctx, store, held)

	seen := drainUnmined(t, ctx, store)
	require.Contains(t, seen, *txA.TxIDChainHash(), "a normal unmined tx must be restored")
	require.NotContains(t, seen, *held.TxIDChainHash(), "a hold must not be restored into block assembly")

	it, err := store.GetPrunableUnminedTxIterator(200)
	require.NoError(t, err)

	for {
		batch, nextErr := it.Next(ctx)
		require.NoError(t, nextErr, "a hold must not fail the pruner's iterator")

		if batch == nil {
			break
		}
	}
}
