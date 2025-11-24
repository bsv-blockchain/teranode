package blockassembly

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWaitForBestBlockHeaderUpdate tests the waitForBestBlockHeaderUpdate method directly
func TestWaitForBestBlockHeaderUpdate(t *testing.T) {
	t.Run("should return when header updates", func(t *testing.T) {
		common := testutil.NewCommonTestSetup(t)
		subtreeStore := testutil.NewMemoryBlobStore()
		blockchainClient := testutil.NewMemorySQLiteBlockchainClient(common.Logger, common.Settings, t)
		utxoStore := testutil.NewSQLiteMemoryUTXOStore(common.Ctx, common.Logger, common.Settings, t)
		_ = utxoStore.SetBlockHeight(0)

		server := New(common.Logger, common.Settings, nil, utxoStore, subtreeStore, blockchainClient)
		server.SetSkipWaitForPendingBlocks(true)
		require.NoError(t, server.Init(common.Ctx))

		// Create initial block header
		bits, _ := model.NewNBitFromString("1d00ffff")
		initialHeader := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           *bits,
			Nonce:          0,
		}

		// Store initial header
		server.blockAssembler.setBestBlockHeader(initialHeader, 0)
		previousHash := initialHeader.Hash()

		// Update header in background after delay
		go func() {
			time.Sleep(50 * time.Millisecond)
			newHeader := &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  previousHash,
				HashMerkleRoot: &chainhash.Hash{},
				Timestamp:      uint32(time.Now().Unix()),
				Bits:           *bits,
				Nonce:          1,
			}
			server.blockAssembler.setBestBlockHeader(newHeader, 1)
		}()

		// Call the wait method
		start := time.Now()
		err := server.waitForBestBlockHeaderUpdate(context.Background(), previousHash)
		elapsed := time.Since(start)

		assert.NoError(t, err)
		assert.Less(t, elapsed, 100*time.Millisecond, "should return quickly after update")
		assert.Greater(t, elapsed, 40*time.Millisecond, "should wait for the update")

		// Verify header was actually updated
		currentHeader, _ := server.blockAssembler.CurrentBlock()
		currentHash := currentHeader.Hash()
		assert.False(t, currentHash.IsEqual(previousHash), "header should have changed")
	})

	t.Run("should timeout when header doesn't update", func(t *testing.T) {
		common := testutil.NewCommonTestSetup(t)
		subtreeStore := testutil.NewMemoryBlobStore()
		blockchainClient := testutil.NewMemorySQLiteBlockchainClient(common.Logger, common.Settings, t)
		utxoStore := testutil.NewSQLiteMemoryUTXOStore(common.Ctx, common.Logger, common.Settings, t)
		_ = utxoStore.SetBlockHeight(0)

		server := New(common.Logger, common.Settings, nil, utxoStore, subtreeStore, blockchainClient)
		server.SetSkipWaitForPendingBlocks(true)
		require.NoError(t, server.Init(common.Ctx))

		// Create and store initial header
		bits, _ := model.NewNBitFromString("1d00ffff")
		initialHeader := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           *bits,
			Nonce:          0,
		}
		server.blockAssembler.setBestBlockHeader(initialHeader, 0)
		previousHash := initialHeader.Hash()

		// Don't update the header - simulate stuck scenario
		// Use a short timeout context for faster test
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		start := time.Now()
		err := server.waitForBestBlockHeaderUpdate(ctx, previousHash)
		elapsed := time.Since(start)

		// Should return without error (timeout is not an error in this case)
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, elapsed, 100*time.Millisecond, "should wait full timeout")
		assert.Less(t, elapsed, 150*time.Millisecond, "should not wait much longer than timeout")

		// Verify header didn't change
		currentHeader, _ := server.blockAssembler.CurrentBlock()
		currentHash := currentHeader.Hash()
		assert.True(t, currentHash.IsEqual(previousHash), "header should not have changed")
	})

	t.Run("should return immediately if header already updated", func(t *testing.T) {
		common := testutil.NewCommonTestSetup(t)
		subtreeStore := testutil.NewMemoryBlobStore()
		blockchainClient := testutil.NewMemorySQLiteBlockchainClient(common.Logger, common.Settings, t)
		utxoStore := testutil.NewSQLiteMemoryUTXOStore(common.Ctx, common.Logger, common.Settings, t)
		_ = utxoStore.SetBlockHeight(0)

		server := New(common.Logger, common.Settings, nil, utxoStore, subtreeStore, blockchainClient)
		server.SetSkipWaitForPendingBlocks(true)
		require.NoError(t, server.Init(common.Ctx))

		// Create two different headers
		bits, _ := model.NewNBitFromString("1d00ffff")
		oldHeader := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           *bits,
			Nonce:          0,
		}

		newHeader := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  oldHeader.Hash(),
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           *bits,
			Nonce:          1,
		}

		// Store the new header (already updated)
		server.blockAssembler.setBestBlockHeader(newHeader, 1)

		// Call wait with the old hash
		start := time.Now()
		err := server.waitForBestBlockHeaderUpdate(context.Background(), oldHeader.Hash())
		elapsed := time.Since(start)

		assert.NoError(t, err)
		// The implementation has a 10ms ticker, so in the worst case we wait one tick plus overhead.
		// On a busy system, this can take longer due to scheduling delays.
		assert.Less(t, elapsed, 100*time.Millisecond, "should return quickly after detecting update")
	})

	t.Run("should handle context cancellation", func(t *testing.T) {
		common := testutil.NewCommonTestSetup(t)
		subtreeStore := testutil.NewMemoryBlobStore()
		blockchainClient := testutil.NewMemorySQLiteBlockchainClient(common.Logger, common.Settings, t)
		utxoStore := testutil.NewSQLiteMemoryUTXOStore(common.Ctx, common.Logger, common.Settings, t)
		_ = utxoStore.SetBlockHeight(0)

		server := New(common.Logger, common.Settings, nil, utxoStore, subtreeStore, blockchainClient)
		server.SetSkipWaitForPendingBlocks(true)
		require.NoError(t, server.Init(common.Ctx))

		// Create and store header
		bits, _ := model.NewNBitFromString("1d00ffff")
		header := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           *bits,
			Nonce:          0,
		}

		// Calculate hash before storing to avoid concurrent access to Hash() method
		headerHash := header.Hash()
		server.blockAssembler.setBestBlockHeader(header, 0)

		// Create already cancelled context
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		// Should return immediately due to cancelled context
		start := time.Now()
		err := server.waitForBestBlockHeaderUpdate(ctx, headerHash)
		elapsed := time.Since(start)

		assert.NoError(t, err) // Returns nil on timeout/cancellation
		assert.Less(t, elapsed, 10*time.Millisecond, "should return immediately on cancelled context")
	})
}
