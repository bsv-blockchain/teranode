package sql

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueryOldUnminedTransactions(t *testing.T) {
	ctx := context.Background()
	store, _ := setup(ctx, t)

	// Create test transactions
	txHashes := make([]chainhash.Hash, 0, 5)
	// Create some unmined transactions at different heights
	heights := []uint32{100, 200, 300, 400, 500}
	for i, height := range heights {
		tx := bt.NewTx()
		// Add a unique output to make each transaction different
		err := tx.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", uint64(1000*(i+1))) //nolint:gosec
		require.NoError(t, err)

		_, err = store.Create(ctx, tx, height)
		require.NoError(t, err)

		txHashes = append(txHashes, *tx.TxIDChainHash())
	}

	// Create a mined transaction (should not be returned)
	minedTx := bt.NewTx()
	err := minedTx.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 9999)
	require.NoError(t, err)

	_, err = store.Create(ctx, minedTx, 150, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{
		BlockID:     1,
		BlockHeight: 150,
		SubtreeIdx:  0,
	}))
	require.NoError(t, err)

	t.Run("returns transactions older than cutoff", func(t *testing.T) {
		// Query for transactions with stored_at_height <= 300
		oldTxs, err := store.QueryOldUnminedTransactions(ctx, 300)
		require.NoError(t, err)

		// Should return transactions at heights 100, 200, 300
		assert.Len(t, oldTxs, 3)

		// Verify the correct transactions were returned
		returnedMap := make(map[string]bool)
		for _, hash := range oldTxs {
			returnedMap[hash.String()] = true
		}

		assert.True(t, returnedMap[txHashes[0].String()])  // height 100
		assert.True(t, returnedMap[txHashes[1].String()])  // height 200
		assert.True(t, returnedMap[txHashes[2].String()])  // height 300
		assert.False(t, returnedMap[txHashes[3].String()]) // height 400 - should not be included
		assert.False(t, returnedMap[txHashes[4].String()]) // height 500 - should not be included
	})

	t.Run("returns empty when all transactions are newer", func(t *testing.T) {
		oldTxs, err := store.QueryOldUnminedTransactions(ctx, 50)
		require.NoError(t, err)
		assert.Empty(t, oldTxs)
	})

	t.Run("returns all unmined when cutoff is very high", func(t *testing.T) {
		oldTxs, err := store.QueryOldUnminedTransactions(ctx, 1000)
		require.NoError(t, err)
		// Should return all 5 unmined transactions (not the mined one)
		assert.Len(t, oldTxs, 5)
	})

	t.Run("does not return mined transactions", func(t *testing.T) {
		oldTxs, err := store.QueryOldUnminedTransactions(ctx, 1000)
		require.NoError(t, err)

		// Verify mined transaction is not included
		for _, hash := range oldTxs {
			assert.NotEqual(t, minedTx.TxIDChainHash().String(), hash.String())
		}
	})
}

func TestQueryOldUnminedTransactions_EdgeCases(t *testing.T) {
	ctx := context.Background()

	t.Run("handles empty database", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		settings := test.CreateBaseTestSettings()
		settings.UtxoStore.DBTimeout = 30 * time.Second

		storeURL, err := url.Parse("sqlitememory:///test_query_empty")
		require.NoError(t, err)

		store, err := New(ctx, logger, settings, storeURL)
		require.NoError(t, err)

		oldTxs, err := store.QueryOldUnminedTransactions(ctx, 100)
		require.NoError(t, err)
		assert.Empty(t, oldTxs)
	})

	t.Run("handles cutoff at 0", func(t *testing.T) {
		store, tx := setup(ctx, t)

		_, err := store.Create(ctx, tx, 0)
		require.NoError(t, err)

		oldTxs, err := store.QueryOldUnminedTransactions(ctx, 0)
		require.NoError(t, err)
		assert.Len(t, oldTxs, 1)
	})
}
