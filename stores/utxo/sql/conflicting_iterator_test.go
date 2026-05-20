package sql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSQLGetConflictingTxIterator verifies the new iterator only yields
// conflicting=true rows and is complementary to GetUnminedTxIterator.
func TestSQLGetConflictingTxIterator(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, tx := setup(ctx, t)

	// Create the tx. No conflicting flag yet.
	_, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)

	// Before marking conflicting: iterator should be empty.
	it, err := store.GetConflictingTxIterator()
	require.NoError(t, err)

	batch, err := it.Next(ctx)
	require.NoError(t, err)
	assert.Empty(t, batch)
	require.NoError(t, it.Close())

	// Flip the conflicting flag directly.
	_, err = store.db.ExecContext(ctx,
		`UPDATE transactions SET conflicting = true WHERE hash = $1`,
		tx.TxIDChainHash()[:])
	require.NoError(t, err)

	// Now iterator should return it.
	it2, err := store.GetConflictingTxIterator()
	require.NoError(t, err)

	collected := make([]*utxo.UnminedTransaction, 0)
	for {
		batch, err := it2.Next(ctx)
		require.NoError(t, err)
		if batch == nil {
			break
		}
		collected = append(collected, batch...)
	}
	require.NoError(t, it2.Close())

	require.Len(t, collected, 1)
	assert.Equal(t, tx.TxIDChainHash().String(), collected[0].Hash.String())
}
