package sql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMarkTransactionsOnLongestChain(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	utxoStore, tx := setup(ctx, t)

	// Set initial block height
	const initialBlockHeight = uint32(100)
	const newBlockHeight = uint32(101)
	err := utxoStore.SetBlockHeight(initialBlockHeight)
	require.NoError(t, err)

	// Create a second test transaction
	tx2, err := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff17032dff0c2f71646c6e6b2f5e931c7f7b6199adf35e1300ffffffff01d15fa012000000001976a91417db35d440a673a218e70a5b9d07f895facf50d288ac00000000")
	require.NoError(t, err)

	tx1Hash := tx.TxIDChainHash()
	tx2Hash := tx2.TxIDChainHash()

	t.Run("create transactions first", func(t *testing.T) {
		// Create transactions in the store first
		_, err := utxoStore.Create(ctx, tx, initialBlockHeight)
		require.NoError(t, err)

		_, err = utxoStore.Create(ctx, tx2, initialBlockHeight, utxo.WithSetCoinbase(true))
		require.NoError(t, err)

		// Verify transactions exist and have initial unminedSince values
		meta1, err := utxoStore.Get(ctx, tx1Hash, fields.UnminedSince)
		require.NoError(t, err)
		assert.Equal(t, initialBlockHeight, meta1.UnminedSince)

		meta2, err := utxoStore.Get(ctx, tx2Hash, fields.UnminedSince)
		require.NoError(t, err)
		assert.Equal(t, initialBlockHeight, meta2.UnminedSince)
	})

	t.Run("mark as on longest chain", func(t *testing.T) {
		// Mark transactions as being on the longest chain
		txHashes := []chainhash.Hash{*tx1Hash, *tx2Hash}
		err := utxoStore.MarkTransactionsOnLongestChain(ctx, txHashes, true)
		require.NoError(t, err)

		// Verify unminedSince field is unset (should be 0 for unmarked/mined)
		meta1, err := utxoStore.Get(ctx, tx1Hash, fields.UnminedSince)
		require.NoError(t, err)
		assert.Equal(t, uint32(0), meta1.UnminedSince)

		meta2, err := utxoStore.Get(ctx, tx2Hash, fields.UnminedSince)
		require.NoError(t, err)
		assert.Equal(t, uint32(0), meta2.UnminedSince)
	})

	t.Run("mark as not on longest chain", func(t *testing.T) {
		// Update block height to simulate chain progression
		err := utxoStore.SetBlockHeight(newBlockHeight)
		require.NoError(t, err)

		// Mark transactions as NOT being on the longest chain
		txHashes := []chainhash.Hash{*tx1Hash, *tx2Hash}
		err = utxoStore.MarkTransactionsOnLongestChain(ctx, txHashes, false)
		require.NoError(t, err)

		// Verify unmined_since was stamped (backdated so the pruner scan sees it at once)
		expected := utxo.BackdatedUnminedSince(newBlockHeight, utxoStore.settings.UtxoStore.UnminedTxRetention)

		meta1, err := utxoStore.Get(ctx, tx1Hash, fields.UnminedSince)
		require.NoError(t, err)
		assert.Equal(t, expected, meta1.UnminedSince)

		meta2, err := utxoStore.Get(ctx, tx2Hash, fields.UnminedSince)
		require.NoError(t, err)
		assert.Equal(t, expected, meta2.UnminedSince)
	})

	t.Run("switch back to longest chain", func(t *testing.T) {
		// Mark transactions as being back on the longest chain
		txHashes := []chainhash.Hash{*tx1Hash, *tx2Hash}
		err := utxoStore.MarkTransactionsOnLongestChain(ctx, txHashes, true)
		require.NoError(t, err)

		// Verify unminedSince field is unset again
		meta1, err := utxoStore.Get(ctx, tx1Hash, fields.UnminedSince)
		require.NoError(t, err)
		assert.Equal(t, uint32(0), meta1.UnminedSince)

		meta2, err := utxoStore.Get(ctx, tx2Hash, fields.UnminedSince)
		require.NoError(t, err)
		assert.Equal(t, uint32(0), meta2.UnminedSince)
	})

	t.Run("empty transaction list", func(t *testing.T) {
		// Test with empty transaction list - should not error
		err := utxoStore.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{}, true)
		require.NoError(t, err)

		err = utxoStore.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{}, false)
		require.NoError(t, err)
	})

	t.Run("single transaction", func(t *testing.T) {
		const testBlockHeight = uint32(200)
		err := utxoStore.SetBlockHeight(testBlockHeight)
		require.NoError(t, err)

		// Mark only one transaction as not on longest chain
		err = utxoStore.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*tx1Hash}, false)
		require.NoError(t, err)

		// Verify only tx1 has updated unminedSince
		meta1, err := utxoStore.Get(ctx, tx1Hash, fields.UnminedSince)
		require.NoError(t, err)
		assert.Equal(t, utxo.BackdatedUnminedSince(testBlockHeight, utxoStore.settings.UtxoStore.UnminedTxRetention), meta1.UnminedSince)

		// tx2 should still have unminedSince = 0 from previous test
		meta2, err := utxoStore.Get(ctx, tx2Hash, fields.UnminedSince)
		require.NoError(t, err)
		assert.Equal(t, uint32(0), meta2.UnminedSince)

		// Mark tx1 back on longest chain
		err = utxoStore.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*tx1Hash}, true)
		require.NoError(t, err)

		meta1, err = utxoStore.Get(ctx, tx1Hash, fields.UnminedSince)
		require.NoError(t, err)
		assert.Equal(t, uint32(0), meta1.UnminedSince)
	})
}

func TestMarkTransactionsOnLongestChain_NonExistent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	utxoStore, _ := setup(ctx, t)

	t.Run("non-existent transactions", func(t *testing.T) {
		// Create random transaction hashes that don't exist in the store
		nonExistentHash1, err := chainhash.NewHashFromStr("1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
		require.NoError(t, err)

		nonExistentHash2, err := chainhash.NewHashFromStr("fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321")
		require.NoError(t, err)

		nonExistentHashes := []chainhash.Hash{*nonExistentHash1, *nonExistentHash2}

		// This should not error for SQL implementation - it will just update 0 rows
		err = utxoStore.MarkTransactionsOnLongestChain(ctx, nonExistentHashes, true)
		require.NoError(t, err)

		err = utxoStore.MarkTransactionsOnLongestChain(ctx, nonExistentHashes, false)
		require.NoError(t, err)
	})
}

// TestMarkTransactionsOnLongestChain_KeepsEarlierUnminedSince mirrors the Aerospike test
// for issue 1768: an existing unmined_since must survive a not-on-longest-chain mark, and
// only a NULL unmined_since is stamped with the current height.
func TestMarkTransactionsOnLongestChain_KeepsEarlierUnminedSince(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	utxoStore, tx := setup(ctx, t)

	const createdAt = uint32(100)
	const muchLater = uint32(1305)

	retention := utxoStore.settings.UtxoStore.UnminedTxRetention
	require.Greater(t, muchLater, retention+createdAt, "test needs headroom above the retention window")
	require.NoError(t, utxoStore.SetBlockHeight(createdAt))

	txHash := tx.TxIDChainHash()

	_, err := utxoStore.Create(ctx, tx, createdAt)
	require.NoError(t, err)

	meta, err := utxoStore.Get(ctx, txHash, fields.UnminedSince)
	require.NoError(t, err)
	require.Equal(t, createdAt, meta.UnminedSince)

	require.NoError(t, utxoStore.SetBlockHeight(muchLater))

	require.NoError(t, utxoStore.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*txHash}, false))

	meta, err = utxoStore.Get(ctx, txHash, fields.UnminedSince)
	require.NoError(t, err)
	assert.Equal(t, createdAt, meta.UnminedSince, "existing unmined_since must not be pushed forward")

	require.NoError(t, utxoStore.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*txHash}, true))

	meta, err = utxoStore.Get(ctx, txHash, fields.UnminedSince)
	require.NoError(t, err)
	require.Zero(t, meta.UnminedSince)

	require.NoError(t, utxoStore.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*txHash}, false))

	meta, err = utxoStore.Get(ctx, txHash, fields.UnminedSince)
	require.NoError(t, err)
	assert.Equal(t, muchLater-retention, meta.UnminedSince, "NULL unmined_since is stamped backdated by unminedTxRetention")
}
