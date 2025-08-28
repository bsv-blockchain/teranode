package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_SqlGetChainTips(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("genesis block only", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		tips, err := s.GetChainTips(context.Background())
		require.NoError(t, err)
		require.Len(t, tips, 1)

		// Genesis block should be the only tip
		tip := tips[0]
		assert.Equal(t, uint32(0), tip.Height)
		assert.Equal(t, "0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206", tip.Hash)
		assert.Equal(t, uint32(0), tip.Branchlen)
		assert.Equal(t, "active", tip.Status)
	})

	t.Run("single chain - multiple blocks", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		// Store blocks in sequence: genesis -> block1 -> block2
		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)
		err = s.SetBlockProcessedAt(context.Background(), block1.Hash())
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)
		err = s.SetBlockProcessedAt(context.Background(), block2.Hash())
		require.NoError(t, err)

		tips, err := s.GetChainTips(context.Background())
		require.NoError(t, err)
		require.Len(t, tips, 1)

		// Block2 should be the only tip (active main chain)
		tip := tips[0]
		assert.Equal(t, uint32(2), tip.Height)
		assert.Equal(t, "484e58c7bf0208d787314710535ef7be8ca31748bc9fef5e1ee2de67ebda757a", tip.Hash)
		assert.Equal(t, uint32(0), tip.Branchlen)
		assert.Equal(t, "active", tip.Status)
	})

	t.Run("fork scenario - two competing chains", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		// Store main chain: genesis -> block1 -> block2
		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)
		err = s.SetBlockProcessedAt(context.Background(), block1.Hash())
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)
		err = s.SetBlockProcessedAt(context.Background(), block2.Hash())
		require.NoError(t, err)

		// Store alternative block at height 2 (fork from block1)
		_, _, err = s.StoreBlock(context.Background(), blockAlternative2, "")
		require.NoError(t, err)
		err = s.SetBlockProcessedAt(context.Background(), blockAlternative2.Hash())
		require.NoError(t, err)

		tips, err := s.GetChainTips(context.Background())
		require.NoError(t, err)
		require.Len(t, tips, 2)

		// Sort tips by height for consistent testing
		if tips[0].Height < tips[1].Height {
			tips[0], tips[1] = tips[1], tips[0]
		}

		// Main chain tip (block2) should be active
		mainTip := tips[0]
		assert.Equal(t, uint32(2), mainTip.Height)
		assert.Equal(t, "484e58c7bf0208d787314710535ef7be8ca31748bc9fef5e1ee2de67ebda757a", mainTip.Hash)
		assert.Equal(t, uint32(0), mainTip.Branchlen)
		assert.Equal(t, "active", mainTip.Status)

		// Alternative chain tip should be a valid fork
		forkTip := tips[1]
		assert.Equal(t, uint32(2), forkTip.Height)
		assert.Equal(t, blockAlternative2.Header.Hash().String(), forkTip.Hash)
		assert.Equal(t, uint32(1), forkTip.Branchlen) // 1 block away from main chain
		assert.Equal(t, "valid-fork", forkTip.Status)
	})

	t.Run("longer fork becomes main chain", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		// Store main chain: genesis -> block1 -> block2
		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)
		err = s.SetBlockProcessedAt(context.Background(), block1.Hash())
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)
		err = s.SetBlockProcessedAt(context.Background(), block2.Hash())
		require.NoError(t, err)

		// Store alternative block at height 2
		_, _, err = s.StoreBlock(context.Background(), blockAlternative2, "")
		require.NoError(t, err)
		err = s.SetBlockProcessedAt(context.Background(), blockAlternative2.Hash())
		require.NoError(t, err)

		// Create a longer alternative chain by adding block3 on top of blockAlternative2
		block3OnFork := createBlock3OnFork(blockAlternative2)
		_, _, err = s.StoreBlock(context.Background(), block3OnFork, "")
		require.NoError(t, err)

		tips, err := s.GetChainTips(context.Background())
		require.NoError(t, err)
		require.Len(t, tips, 2)

		// Sort tips by height for consistent testing
		if tips[0].Height < tips[1].Height {
			tips[0], tips[1] = tips[1], tips[0]
		}

		// The longer fork should now be the active chain
		activeTip := tips[0]
		assert.Equal(t, uint32(3), activeTip.Height)
		assert.Equal(t, block3OnFork.Header.Hash().String(), activeTip.Hash)
		assert.Equal(t, uint32(0), activeTip.Branchlen)
		assert.Equal(t, "active", activeTip.Status)

		// The original chain should now be a fork
		forkTip := tips[1]
		assert.Equal(t, uint32(2), forkTip.Height)
		assert.Equal(t, "484e58c7bf0208d787314710535ef7be8ca31748bc9fef5e1ee2de67ebda757a", forkTip.Hash)
		assert.Equal(t, uint32(1), forkTip.Branchlen)
		assert.Equal(t, "valid-fork", forkTip.Status)
	})

	t.Run("invalid block scenario", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		// Store main chain
		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)
		err = s.SetBlockProcessedAt(context.Background(), block1.Hash())
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)
		err = s.SetBlockProcessedAt(context.Background(), block2.Hash())
		require.NoError(t, err)

		// Store alternative block
		_, _, err = s.StoreBlock(context.Background(), blockAlternative2, "")
		require.NoError(t, err)
		err = s.SetBlockProcessedAt(context.Background(), blockAlternative2.Hash())
		require.NoError(t, err)

		// Invalidate the alternative block
		err = s.InvalidateBlock(context.Background(), blockAlternative2.Header.Hash())
		require.NoError(t, err)

		tips, err := s.GetChainTips(context.Background())
		require.NoError(t, err)
		require.Len(t, tips, 2)

		// Sort tips by status to get consistent ordering
		if tips[0].Status == "invalid" {
			tips[0], tips[1] = tips[1], tips[0]
		}

		// Main chain should still be active
		activeTip := tips[0]
		assert.Equal(t, uint32(2), activeTip.Height)
		assert.Equal(t, "484e58c7bf0208d787314710535ef7be8ca31748bc9fef5e1ee2de67ebda757a", activeTip.Hash)
		assert.Equal(t, uint32(0), activeTip.Branchlen)
		assert.Equal(t, "active", activeTip.Status)

		// Alternative block should be marked as invalid
		invalidTip := tips[1]
		assert.Equal(t, uint32(2), invalidTip.Height)
		assert.Equal(t, blockAlternative2.Header.Hash().String(), invalidTip.Hash)
		assert.Equal(t, uint32(1), invalidTip.Branchlen)
		assert.Equal(t, "invalid", invalidTip.Status)
	})

	t.Run("multiple forks from same parent", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		// Store base chain: genesis -> block1
		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)
		err = s.SetBlockProcessedAt(context.Background(), block1.Hash())
		require.NoError(t, err)

		// Create multiple competing blocks at height 2
		_, _, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)
		err = s.SetBlockProcessedAt(context.Background(), block2.Hash())
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), blockAlternative2, "")
		require.NoError(t, err)
		err = s.SetBlockProcessedAt(context.Background(), blockAlternative2.Hash())
		require.NoError(t, err)

		// Create another alternative block at height 2
		block2Alternative3 := createAlternativeBlock2()
		_, _, err = s.StoreBlock(context.Background(), block2Alternative3, "")
		require.NoError(t, err)
		err = s.SetBlockProcessedAt(context.Background(), block2Alternative3.Hash())
		require.NoError(t, err)

		tips, err := s.GetChainTips(context.Background())
		require.NoError(t, err)
		require.Len(t, tips, 3)

		// Count the different statuses
		activeCount := 0
		forkCount := 0

		for _, tip := range tips {
			assert.Equal(t, uint32(2), tip.Height) // All should be at height 2

			if tip.Status == "active" {
				activeCount++

				assert.Equal(t, uint32(0), tip.Branchlen)
			} else if tip.Status == "valid-fork" {
				forkCount++

				assert.Equal(t, uint32(1), tip.Branchlen)
			}
		}

		// Should have exactly 1 active tip and 2 fork tips
		assert.Equal(t, 1, activeCount)
		assert.Equal(t, 2, forkCount)
	})

	t.Run("deep fork scenario", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		// Build main chain: genesis -> block1 -> block2 -> block3
		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)
		err = s.SetBlockProcessedAt(context.Background(), block1.Hash())
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)
		err = s.SetBlockProcessedAt(context.Background(), block2.Hash())
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block3, "")
		require.NoError(t, err)

		// Create a fork from block1 (2 blocks deep)
		forkBlock2 := createAlternativeBlock2()
		_, _, err = s.StoreBlock(context.Background(), forkBlock2, "")
		require.NoError(t, err)
		err = s.SetBlockProcessedAt(context.Background(), forkBlock2.Hash())
		require.NoError(t, err)

		forkBlock3 := createBlock3OnFork(forkBlock2)
		_, _, err = s.StoreBlock(context.Background(), forkBlock3, "")
		require.NoError(t, err)
		err = s.SetBlockProcessedAt(context.Background(), forkBlock3.Hash())
		require.NoError(t, err)

		tips, err := s.GetChainTips(context.Background())
		require.NoError(t, err)
		require.Len(t, tips, 2)

		// Sort by height
		if tips[0].Height < tips[1].Height {
			tips[0], tips[1] = tips[1], tips[0]
		}

		// Main chain tip
		mainTip := tips[0]
		assert.Equal(t, uint32(3), mainTip.Height)
		assert.Equal(t, block3.Header.Hash().String(), mainTip.Hash)
		assert.Equal(t, uint32(0), mainTip.Branchlen)
		assert.Equal(t, "active", mainTip.Status)

		// Fork tip should have branch length of 2 (forked 2 blocks ago)
		forkTip := tips[1]
		assert.Equal(t, uint32(3), forkTip.Height)
		assert.Equal(t, forkBlock3.Header.Hash().String(), forkTip.Hash)
		assert.Equal(t, uint32(2), forkTip.Branchlen)
		assert.Equal(t, "valid-fork", forkTip.Status)
	})

	t.Run("fork scenario - alternative block not processed", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		// Store main chain: genesis -> block1 -> block2
		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)
		err = s.SetBlockProcessedAt(context.Background(), block1.Hash())
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)
		err = s.SetBlockProcessedAt(context.Background(), block2.Hash())
		require.NoError(t, err)

		// Store alternative block at height 2 (fork from block1) without processing it
		_, _, err = s.StoreBlock(context.Background(), blockAlternative2, "")
		require.NoError(t, err)

		tips, err := s.GetChainTips(context.Background())
		require.NoError(t, err)
		require.Len(t, tips, 2)

		// Find the main chain tip and the fork tip
		var mainTip, forkTip *model.ChainTip

		for _, tip := range tips {
			if tip.Hash == block2.Hash().String() {
				mainTip = tip
			} else if tip.Hash == blockAlternative2.Hash().String() {
				forkTip = tip
			}
		}

		require.NotNil(t, mainTip)
		require.NotNil(t, forkTip)

		// Main chain tip should be active
		assert.Equal(t, uint32(2), mainTip.Height)
		assert.Equal(t, block2.Hash().String(), mainTip.Hash)
		assert.Equal(t, uint32(0), mainTip.Branchlen)
		assert.Equal(t, "active", mainTip.Status)

		// Fork tip should be headers-only since it's not processed
		assert.Equal(t, uint32(2), forkTip.Height)
		assert.Equal(t, blockAlternative2.Hash().String(), forkTip.Hash)
		assert.Equal(t, uint32(1), forkTip.Branchlen)
		assert.Equal(t, "headers-only", forkTip.Status)
	})
}

// Helper function to create block3 on top of a given parent block
func createBlock3OnFork(parentBlock *model.Block) *model.Block {
	// Create a new coinbase transaction for block 3
	coinbaseTx3Fork, _ := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff17030300002f6d312d65752fb670097da68d1b768d8b21f6ffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000")

	merkleRoot, _ := chainhash.NewHashFromStr("d1de05a65845a49ad63eed887c4cf7cc824e02b5d10de82829f740b748b9737f")
	bits, _ := model.NewNBitFromString("207fffff")
	subtree, _ := chainhash.NewHashFromStr("0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098")

	return &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			Timestamp:      1729259730, // Different timestamp
			Nonce:          2,          // Different nonce
			HashPrevBlock:  parentBlock.Header.Hash(),
			HashMerkleRoot: merkleRoot,
			Bits:           *bits,
		},
		Height:           parentBlock.Height + 1,
		CoinbaseTx:       coinbaseTx3Fork,
		TransactionCount: 1,
		Subtrees: []*chainhash.Hash{
			subtree,
		},
	}
}

// Helper function to create another alternative block at height 2
func createAlternativeBlock2() *model.Block {
	// Create a different coinbase transaction
	coinbaseTx2Alt, _ := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff17030200002f6d312d65752f605f77009f74384816a31808ffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000")

	merkleRoot, _ := chainhash.NewHashFromStr("b881339d3b500bcaceb5d2f1225f45edd77e846805ddffe27788fc06f218f177")
	bits, _ := model.NewNBitFromString("207fffff")
	subtree, _ := chainhash.NewHashFromStr("0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098")

	return &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			Timestamp:      1231469750, // Different timestamp
			Nonce:          1639830030, // Different nonce
			HashPrevBlock:  block1.Header.Hash(),
			HashMerkleRoot: merkleRoot,
			Bits:           *bits,
		},
		Height:           2,
		CoinbaseTx:       coinbaseTx2Alt,
		TransactionCount: 1,
		Subtrees: []*chainhash.Hash{
			subtree,
		},
	}
}

func BenchmarkGetChainTipsPerformance(b *testing.B) {
	tSettings := test.CreateBaseTestSettings(b)

	storeURL, err := url.Parse("sqlitememory:///")
	if err != nil {
		b.Fatalf("failed to parse store URL: %v", err)
	}

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	if err != nil {
		b.Fatalf("failed to create SQL store: %v", err)
	}

	// Setup: create a chain with a fork (similar to the fork scenario in tests)
	_, _, err = s.StoreBlock(context.Background(), block1, "")
	if err != nil {
		b.Fatalf("failed to store block1: %v", err)
	}

	err = s.SetBlockProcessedAt(context.Background(), block1.Hash())
	if err != nil {
		b.Fatalf("failed to set block1 processed: %v", err)
	}

	_, _, err = s.StoreBlock(context.Background(), block2, "")
	if err != nil {
		b.Fatalf("failed to store block2: %v", err)
	}

	err = s.SetBlockProcessedAt(context.Background(), block2.Hash())
	if err != nil {
		b.Fatalf("failed to set block2 processed: %v", err)
	}

	_, _, err = s.StoreBlock(context.Background(), blockAlternative2, "")
	if err != nil {
		b.Fatalf("failed to store blockAlternative2: %v", err)
	}

	err = s.SetBlockProcessedAt(context.Background(), blockAlternative2.Hash())
	if err != nil {
		b.Fatalf("failed to set blockAlternative2 processed: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := s.GetChainTips(context.Background())
		if err != nil {
			b.Fatalf("GetChainTips failed: %v", err)
		}
	}
}
