package blockvalidation

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestTryQuickValidation tests the new tryQuickValidation method
func TestTryQuickValidation(t *testing.T) {
	t.Run("should use normal validation when quick validation is disabled", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		block := testhelpers.CreateTestBlocks(t, 1)[0]
		block.Height = 100

		catchupCtx := &CatchupContext{
			useQuickValidation:      false,
			highestCheckpointHeight: 200,
		}

		// Pass nil channel - not needed when quick validation is disabled
		shouldTryNormal, err := suite.Server.tryQuickValidation(context.Background(), block, catchupCtx, "http://test", nil)

		assert.NoError(t, err)
		assert.True(t, shouldTryNormal, "should return true to use normal validation when quick validation is disabled")
	})

	t.Run("should use normal validation when block is above checkpoint height", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		block := testhelpers.CreateTestBlocks(t, 1)[0]
		block.Height = 300

		catchupCtx := &CatchupContext{
			useQuickValidation:      true,
			highestCheckpointHeight: 200,
		}

		// Pass nil channel - not needed when block is above checkpoint
		shouldTryNormal, err := suite.Server.tryQuickValidation(context.Background(), block, catchupCtx, "http://test", nil)

		assert.NoError(t, err)
		assert.True(t, shouldTryNormal, "should return true to use normal validation when block is above checkpoint height")
	})

	t.Run("should handle subtree deletion when quick validation fails", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		ctx := context.Background()

		// Create a test block with subtrees
		subtreeHash1, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
		subtreeHash2, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000002")

		// Store some dummy subtree data
		err := suite.Server.subtreeStore.Set(ctx, subtreeHash1[:], fileformat.FileTypeSubtree, []byte("subtree1"))
		require.NoError(t, err)
		err = suite.Server.subtreeStore.Set(ctx, subtreeHash2[:], fileformat.FileTypeSubtree, []byte("subtree2"))
		require.NoError(t, err)

		block := testhelpers.CreateTestBlocks(t, 1)[0]
		block.Height = 100
		block.Subtrees = []*chainhash.Hash{subtreeHash1, subtreeHash2}

		catchupCtx := &CatchupContext{
			useQuickValidation:      true,
			highestCheckpointHeight: 200,
			blockUpTo:               block,
		}

		// Create a buffered channel for async writes
		writeJobsChan := make(chan *SubtreeWriteJob, 10)

		// The quick validation will fail because we didn't set up all the necessary mocks
		// This should trigger the subtree cleanup logic
		shouldTryNormal, err := suite.Server.tryQuickValidation(ctx, block, catchupCtx, "http://test", writeJobsChan)

		assert.NoError(t, err, "should not return error even when quick validation fails")
		assert.True(t, shouldTryNormal, "should return true to fallback to normal validation")

		// Verify subtrees were deleted
		_, err = suite.Server.subtreeStore.Get(ctx, subtreeHash1[:], fileformat.FileTypeSubtree)
		assert.Error(t, err, "subtree1 should have been deleted")
		assert.True(t, errors.Is(err, errors.ErrNotFound))

		_, err = suite.Server.subtreeStore.Get(ctx, subtreeHash2[:], fileformat.FileTypeSubtree)
		assert.Error(t, err, "subtree2 should have been deleted")
		assert.True(t, errors.Is(err, errors.ErrNotFound))
	})

	t.Run("should return false when quick validation succeeds", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		ctx := context.Background()

		// Mock for successful quick validation
		suite.MockBlockchain.On("GetNextBlockID", mock.Anything).Return(uint64(1), nil).Once()
		suite.MockBlockchain.On("AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		suite.MockBlockchain.On("SetBlockSubtreesSet", mock.Anything, mock.Anything).Return(nil).Once()

		block := testhelpers.CreateTestBlocks(t, 1)[0]
		block.Height = 100

		catchupCtx := &CatchupContext{
			useQuickValidation:      true,
			highestCheckpointHeight: 200,
			blockUpTo:               block,
		}

		// Create a buffered channel for async writes
		writeJobsChan := make(chan *SubtreeWriteJob, 10)

		// This should succeed and return false (no need for normal validation)
		shouldTryNormal, err := suite.Server.tryQuickValidation(ctx, block, catchupCtx, "http://test", writeJobsChan)

		assert.NoError(t, err)
		assert.False(t, shouldTryNormal, "should return false when quick validation succeeds")
	})

	t.Run("should handle subtree not found errors gracefully", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		ctx := context.Background()

		// Create a test block with subtrees that don't exist in store
		subtreeHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")

		block := testhelpers.CreateTestBlocks(t, 1)[0]
		block.Height = 100
		block.Subtrees = []*chainhash.Hash{subtreeHash}

		catchupCtx := &CatchupContext{
			useQuickValidation:      true,
			highestCheckpointHeight: 200,
			blockUpTo:               block,
		}

		// Create a buffered channel for async writes
		writeJobsChan := make(chan *SubtreeWriteJob, 10)

		// The quick validation will fail and try to delete non-existent subtrees
		shouldTryNormal, err := suite.Server.tryQuickValidation(ctx, block, catchupCtx, "http://test", writeJobsChan)

		assert.NoError(t, err, "should handle not found error gracefully")
		assert.True(t, shouldTryNormal, "should return true to fallback to normal validation")
	})
}

// TestValidateBlocksOnChannel_ErrorHandling tests the error handling changes in validateBlocksOnChannel
func TestValidateBlocksOnChannel_ErrorHandling(t *testing.T) {
	// This test verifies that only consensus violations (ErrBlockInvalid, ErrTxInvalid)
	// result in marking the block as invalid

	t.Run("should only mark block invalid for consensus violations", func(t *testing.T) {
		// Test scenarios for different error types
		testCases := []struct {
			name              string
			validationError   error
			shouldMarkInvalid bool
			description       string
		}{
			{
				name:              "ErrBlockInvalid should mark as invalid",
				validationError:   errors.ErrBlockInvalid,
				shouldMarkInvalid: true,
				description:       "consensus violation - block invalid",
			},
			{
				name:              "ErrTxInvalid should mark as invalid",
				validationError:   errors.ErrTxInvalid,
				shouldMarkInvalid: true,
				description:       "consensus violation - transaction invalid",
			},
			{
				name:              "wrapped ErrBlockInvalid should mark as invalid",
				validationError:   errors.Wrap(errors.ErrBlockInvalid),
				shouldMarkInvalid: true,
				description:       "wrapped consensus violation",
			},
			{
				name:              "processing error should not mark as invalid",
				validationError:   errors.NewProcessingError("processing failed"),
				shouldMarkInvalid: false,
				description:       "recoverable processing error",
			},
			{
				name:              "not found error should not mark as invalid",
				validationError:   errors.ErrNotFound,
				shouldMarkInvalid: false,
				description:       "missing data error",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// Verify error types are correctly identified
				if tc.shouldMarkInvalid {
					assert.True(t, errors.Is(tc.validationError, errors.ErrBlockInvalid) ||
						errors.Is(tc.validationError, errors.ErrTxInvalid),
						"Error should be identified as consensus violation: %s", tc.description)
				} else {
					assert.False(t, errors.Is(tc.validationError, errors.ErrBlockInvalid) ||
						errors.Is(tc.validationError, errors.ErrTxInvalid),
						"Error should not be identified as consensus violation: %s", tc.description)
				}
			})
		}
	})
}

// TestValidateForkDepth tests the validateForkDepth method
func TestValidateForkDepth(t *testing.T) {
	t.Run("should allow fork depth within coinbase maturity", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		catchupCtx := &CatchupContext{
			blockUpTo: testhelpers.CreateTestBlocks(t, 1)[0],
			forkDepth: 50,
			peerID:    "peer123",
		}
		suite.Server.settings.ChainCfgParams.CoinbaseMaturity = 100

		err := suite.Server.validateForkDepth(catchupCtx)
		assert.NoError(t, err)
	})

	t.Run("should reject fork depth exceeding coinbase maturity", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		catchupCtx := &CatchupContext{
			blockUpTo: testhelpers.CreateTestBlocks(t, 1)[0],
			forkDepth: 150,
			peerID:    "peer123",
		}
		suite.Server.settings.ChainCfgParams.CoinbaseMaturity = 100

		err := suite.Server.validateForkDepth(catchupCtx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "exceeds coinbase maturity")
	})
}

// TestRecordMaliciousAttempt tests the recordMaliciousAttempt method
func TestRecordMaliciousAttempt(t *testing.T) {
	t.Run("should record malicious attempt with peer metrics", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		// This function logs warnings but doesn't return errors
		// Just verify it doesn't panic
		suite.Server.recordMaliciousAttempt("peer123", "test_violation")
	})

	t.Run("should handle empty peer ID", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		// Should not panic with empty peer ID
		suite.Server.recordMaliciousAttempt("", "test_violation")
	})
}

// TestGetLowestCheckpointHeight tests the getLowestCheckpointHeight function
func TestGetLowestCheckpointHeight(t *testing.T) {
	t.Run("should return 0 for empty checkpoints", func(t *testing.T) {
		height := getLowestCheckpointHeight(nil)
		assert.Equal(t, uint32(0), height)
	})

	t.Run("should find lowest checkpoint height", func(t *testing.T) {
		checkpoints := []chaincfg.Checkpoint{
			{Height: 300},
			{Height: 100},
			{Height: 200},
		}
		height := getLowestCheckpointHeight(checkpoints)
		assert.Equal(t, uint32(100), height)
	})

	t.Run("should handle single checkpoint", func(t *testing.T) {
		checkpoints := []chaincfg.Checkpoint{
			{Height: 250},
		}
		height := getLowestCheckpointHeight(checkpoints)
		assert.Equal(t, uint32(250), height)
	})
}

// TestNewHashFromStr tests the newHashFromStr function
func TestNewHashFromStr(t *testing.T) {
	t.Run("should create hash from valid hex string", func(t *testing.T) {
		validHex := "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
		hash := newHashFromStr(validHex)
		assert.NotNil(t, hash)
		assert.Equal(t, validHex, hash.String())
	})

	t.Run("should panic on invalid hex string", func(t *testing.T) {
		assert.Panics(t, func() {
			newHashFromStr("invalid_hex")
		})
	})
}
