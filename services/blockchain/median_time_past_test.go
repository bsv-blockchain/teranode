package blockchain

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/stretchr/testify/require"
)

// TestGetMedianTimePastForHeights_Single tests MTP retrieval for a single height.
func TestGetMedianTimePastForHeights_Single(t *testing.T) {
	tests := []struct {
		name           string
		height         uint32
		expectedMTP    uint32
		expectError    bool
		setupBlocks    int
		blockTimestamp func(height int) uint32
	}{
		{
			name:        "height less than 11 returns 0",
			height:      5,
			expectedMTP: 0,
			expectError: false,
			setupBlocks: 10,
			blockTimestamp: func(height int) uint32 {
				return uint32(1000000 + height*600) // 600 seconds apart
			},
		},
		{
			name:        "height exactly 11 calculates MTP",
			height:      11,
			expectedMTP: 0, // Will be calculated from actual blocks
			expectError: false,
			setupBlocks: 12,
			blockTimestamp: func(height int) uint32 {
				return uint32(1000000 + height*600)
			},
		},
		{
			name:        "height greater than 11 calculates MTP",
			height:      20,
			expectedMTP: 0, // Will be calculated from actual blocks
			expectError: false,
			setupBlocks: 21,
			blockTimestamp: func(height int) uint32 {
				return uint32(1000000 + height*600)
			},
		},
		{
			name:        "MTP with non-sequential timestamps",
			height:      15,
			expectedMTP: 0, // Will be calculated from actual blocks
			expectError: false,
			setupBlocks: 16,
			blockTimestamp: func(height int) uint32 {
				// Create timestamps that are not in perfect order
				timestamps := []uint32{
					1000000, 1000100, 1000300, 1000200, 1000500,
					1000400, 1000700, 1000600, 1000900, 1000800,
					1001100, 1001000, 1001300, 1001200, 1001500, 1001400,
				}
				if height < len(timestamps) {
					return timestamps[height]
				}
				return uint32(1000000 + height*600)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Re-setup for each test case to have a fresh blockchain
			ctx := setup(t)

			// Override CSVHeight to 0 so MTP is calculated for all blocks in tests
			// (MainNet has CSVHeight=419328, but our test blocks start at height 1)
			ctx.server.settings.ChainCfgParams.CSVHeight = 0

			// Setup blocks for this test
			// Note: Genesis block (height 0) is already created by Init(), so we start at height 1
			var timestamps []uint32

			// Get genesis block timestamp
			genesisHeader, _, err := ctx.server.store.GetBestBlockHeader(context.Background())
			require.NoError(t, err)
			timestamps = append(timestamps, genesisHeader.Timestamp)

			for i := 1; i < tt.setupBlocks; i++ {
				ts := tt.blockTimestamp(i)
				timestamps = append(timestamps, ts)

				block := createTestBlockAtHeight(ctx, t, uint32(i), ts)
				_, _, err := ctx.server.store.StoreBlock(context.Background(), block, "")
				require.NoError(t, err)
			}

			// Get MTP
			mtps, err := ctx.server.GetMedianTimePastForHeights(context.Background(), []uint32{tt.height})

			if tt.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Len(t, mtps, 1)
			mtp := mtps[0]

			// For height < 11, MTP should be 0
			if tt.height < MedianTimeBlocks {
				require.Equal(t, uint32(0), mtp)
				return
			}

			// For height >= 11, calculate expected MTP manually
			// MTP of block N uses blocks [N-11, N-1] (previous 11 blocks)
			startHeight := tt.height - MedianTimeBlocks
			endHeight := tt.height - 1

			// Get timestamps for the range
			relevantTimestamps := make([]time.Time, 0, MedianTimeBlocks)
			for h := startHeight; h <= endHeight; h++ {
				relevantTimestamps = append(relevantTimestamps, time.Unix(int64(timestamps[h]), 0))
			}

			// Calculate expected median
			expectedMedianTime, err := model.CalculateMedianTimestamp(relevantTimestamps)
			require.NoError(t, err)

			expectedMTP := uint32(expectedMedianTime.Unix())
			require.Equal(t, expectedMTP, mtp, "MTP should match calculated median for height %d", tt.height)
		})
	}
}

// TestGetMedianTimePastForHeights_Batch tests batch MTP retrieval.
func TestGetMedianTimePastForHeights_Batch(t *testing.T) {
	ctx := setup(t)

	// Override CSVHeight to 0 so MTP is calculated for all blocks in tests
	// (MainNet has CSVHeight=419328, but our test blocks start at height 1)
	ctx.server.settings.ChainCfgParams.CSVHeight = 0

	// Get genesis block timestamp (height 0 is auto-created)
	genesisHeader, _, err := ctx.server.store.GetBestBlockHeader(context.Background())
	require.NoError(t, err)

	// Setup 25 blocks with known timestamps
	var timestamps []uint32
	timestamps = append(timestamps, genesisHeader.Timestamp) // height 0

	for i := 1; i < 25; i++ {
		ts := uint32(1000000 + i*600) // 600 seconds apart
		timestamps = append(timestamps, ts)

		block := createTestBlockAtHeight(ctx, t, uint32(i), ts)
		_, _, err := ctx.server.store.StoreBlock(context.Background(), block, "")
		require.NoError(t, err)
	}

	tests := []struct {
		name        string
		heights     []uint32
		expectError bool
	}{
		{
			name:        "empty heights returns empty result",
			heights:     []uint32{},
			expectError: false,
		},
		{
			name:        "single height",
			heights:     []uint32{15},
			expectError: false,
		},
		{
			name:        "multiple heights including heights < 11",
			heights:     []uint32{5, 11, 15, 20},
			expectError: false,
		},
		{
			name:        "sequential heights",
			heights:     []uint32{11, 12, 13, 14, 15},
			expectError: false,
		},
		{
			name:        "non-sequential heights",
			heights:     []uint32{20, 15, 11, 18},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mtps, err := ctx.server.GetMedianTimePastForHeights(context.Background(), tt.heights)

			if tt.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, len(tt.heights), len(mtps), "Result length should match input length")

			// Verify each MTP individually
			for i, height := range tt.heights {
				expected, err := ctx.server.GetMedianTimePastForHeights(context.Background(), []uint32{height})
				require.NoError(t, err)
				require.Len(t, expected, 1)
				require.Equal(t, expected[0], mtps[i], "MTP at index %d for height %d should match individual calculation", i, height)
			}
		})
	}
}

// TestMedianTimePastStorageAndRetrieval tests that MTP is correctly stored and retrieved.
func TestMedianTimePastStorageAndRetrieval(t *testing.T) {
	ctx := setup(t)

	// Override CSVHeight to 0 so MTP is calculated for all blocks in tests
	// (MainNet has CSVHeight=419328, but our test blocks start at height 1)
	ctx.server.settings.ChainCfgParams.CSVHeight = 0

	// Get genesis block (height 0 is auto-created)
	genesisHeader, genesisMeta, err := ctx.server.store.GetBestBlockHeader(context.Background())
	require.NoError(t, err)

	// Create and store blocks with known timestamps
	numBlocks := 20
	var blockHashes []*chainhash.Hash
	var expectedMTPs []uint32

	// Add genesis to tracking
	blockHashes = append(blockHashes, genesisHeader.Hash())
	expectedMTPs = append(expectedMTPs, genesisMeta.MedianTimePast) // Should be 0

	for i := 1; i < numBlocks; i++ {
		ts := uint32(1000000 + i*600)
		block := createTestBlockAtHeight(ctx, t, uint32(i), ts)

		_, _, err := ctx.server.store.StoreBlock(context.Background(), block, "")
		require.NoError(t, err)

		blockHashes = append(blockHashes, block.Hash())

		// Calculate expected MTP for this height
		if uint32(i) < MedianTimeBlocks {
			expectedMTPs = append(expectedMTPs, 0)
		} else {
			blockMTPs, err := ctx.server.GetMedianTimePastForHeights(context.Background(), []uint32{uint32(i)})
			require.NoError(t, err)
			require.Len(t, blockMTPs, 1)
			expectedMTPs = append(expectedMTPs, blockMTPs[0])
		}
	}

	// Retrieve blocks and verify MTP values
	for i := 0; i < numBlocks; i++ {
		header, meta, err := ctx.server.store.GetBlockHeader(context.Background(), blockHashes[i])
		require.NoError(t, err)
		require.NotNil(t, header)
		require.NotNil(t, meta)

		require.Equal(t, expectedMTPs[i], meta.MedianTimePast,
			"Stored MTP for block at height %d should match calculated value", i)
	}

	// Also test GetBestBlockHeader
	bestHeader, bestMeta, err := ctx.server.store.GetBestBlockHeader(context.Background())
	require.NoError(t, err)
	require.NotNil(t, bestHeader)
	require.NotNil(t, bestMeta)
	require.Equal(t, expectedMTPs[numBlocks-1], bestMeta.MedianTimePast,
		"Best block MTP should match expected value")
}

// TestMedianTimePastCSVHeight tests that MTP returns 0 before CSVHeight activation.
func TestMedianTimePastCSVHeight(t *testing.T) {
	ctx := setup(t)

	// Set CSVHeight to 20 (BIP113 activates at height 20)
	ctx.server.settings.ChainCfgParams.CSVHeight = 20

	// Create 25 blocks
	for i := 1; i < 25; i++ {
		ts := uint32(1000000 + i*600)
		block := createTestBlockAtHeight(ctx, t, uint32(i), ts)
		_, _, err := ctx.server.store.StoreBlock(context.Background(), block, "")
		require.NoError(t, err)
	}

	t.Run("MTP returns 0 before CSVHeight", func(t *testing.T) {
		// Height 15 is below CSVHeight (20), so MTP should be 0
		mtps, err := ctx.server.GetMedianTimePastForHeights(context.Background(), []uint32{15})
		require.NoError(t, err)
		require.Len(t, mtps, 1)
		require.Equal(t, uint32(0), mtps[0], "MTP should be 0 before CSVHeight activation")
	})

	t.Run("MTP returns 0 at CSVHeight if not enough blocks", func(t *testing.T) {
		// Height 20 is at CSVHeight, but needs 11 previous blocks (heights 9-19)
		// which are available, so MTP should be calculated
		mtps, err := ctx.server.GetMedianTimePastForHeights(context.Background(), []uint32{20})
		require.NoError(t, err)
		require.Len(t, mtps, 1)
		require.Greater(t, mtps[0], uint32(0), "MTP should be calculated at CSVHeight if enough blocks exist")
	})

	t.Run("MTP calculated after CSVHeight", func(t *testing.T) {
		// Height 22 is above CSVHeight and has enough blocks
		mtps, err := ctx.server.GetMedianTimePastForHeights(context.Background(), []uint32{22})
		require.NoError(t, err)
		require.Len(t, mtps, 1)
		require.Greater(t, mtps[0], uint32(0), "MTP should be calculated after CSVHeight")
	})
}

// TestMedianTimePastEdgeCases tests edge cases for MTP calculation.
func TestMedianTimePastEdgeCases(t *testing.T) {
	ctx := setup(t)

	// Override CSVHeight to 0 so MTP is calculated for all blocks in tests
	// (MainNet has CSVHeight=419328, but our test blocks start at height 1)
	ctx.server.settings.ChainCfgParams.CSVHeight = 0

	t.Run("all blocks with same timestamp", func(t *testing.T) {
		constantTimestamp := uint32(1000000)

		// Start from height 1 (genesis is height 0, auto-created)
		for i := 1; i < 15; i++ {
			block := createTestBlockAtHeight(ctx, t, uint32(i), constantTimestamp)
			_, _, err := ctx.server.store.StoreBlock(context.Background(), block, "")
			require.NoError(t, err)
		}

		mtps, err := ctx.server.GetMedianTimePastForHeights(context.Background(), []uint32{12})
		require.NoError(t, err)
		require.Len(t, mtps, 1)
		// MTP should be close to constant timestamp (genesis might have different timestamp)
		require.Greater(t, mtps[0], uint32(0), "MTP should be calculated when blocks have same timestamp")
	})

	t.Run("timestamps in reverse order", func(t *testing.T) {
		// Reset by creating new context
		ctx = setup(t)

		// Start from height 1 (genesis is height 0, auto-created)
		for i := 1; i < 15; i++ {
			// Timestamps decreasing
			ts := uint32(1010000 - i*100)
			block := createTestBlockAtHeight(ctx, t, uint32(i), ts)
			_, _, err := ctx.server.store.StoreBlock(context.Background(), block, "")
			require.NoError(t, err)
		}

		// MTP should still work correctly even with reverse timestamps
		mtps, err := ctx.server.GetMedianTimePastForHeights(context.Background(), []uint32{12})
		require.NoError(t, err)
		require.Len(t, mtps, 1)
		require.Greater(t, mtps[0], uint32(0), "MTP should be calculated even with reverse timestamps")
	})
}

// createTestBlockAtHeight creates a test block at a specific height with a specific timestamp.
// Note: height must be >= 1 since genesis (height 0) is created automatically by Init()
func createTestBlockAtHeight(ctx *testContext, t *testing.T, height uint32, timestamp uint32) *model.Block {
	require.Greater(t, height, uint32(0), "height must be >= 1, genesis is auto-created")

	// Get the previous block header
	headers, _, err := ctx.server.store.GetBlockHeadersByHeight(context.Background(), height-1, height-1)
	require.NoError(t, err)
	require.Len(t, headers, 1)
	prevHash := headers[0].Hash()

	// Create coinbase transaction
	coinbaseTx := bt.NewTx()

	// Create coinbase input using the From method
	err = coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0)
	require.NoError(t, err)

	// Set the unlocking script with block height data
	heightBytes := []byte{
		0x03,
		byte(height & 0xff),
		byte((height >> 8) & 0xff),
		byte((height >> 16) & 0xff),
	}
	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes(heightBytes)
	coinbaseTx.Inputs[0].SequenceNumber = 0xffffffff

	// Add a coinbase output
	err = coinbaseTx.AddP2PKHOutputFromAddress("mrs6FYWPcb441b4qfcEPyvLvzj64WHtwCU", 5000000000)
	require.NoError(t, err)

	// Create merkle root from coinbase tx
	merkleRoot := chainhash.HashH(coinbaseTx.Bytes())

	// Create block header
	header := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  prevHash,
		HashMerkleRoot: &merkleRoot,
		Timestamp:      timestamp,
		Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d}, // mainnet genesis bits 0x1d00ffff in little endian
		Nonce:          uint32(height),                     // Use height as nonce for uniqueness
	}

	// Create subtree hash
	subtreeHash := chainhash.HashH([]byte("subtree"))
	subtrees := []*chainhash.Hash{&subtreeHash}

	return &model.Block{
		Header:           header,
		CoinbaseTx:       coinbaseTx,
		Subtrees:         subtrees,
		TransactionCount: 1,
		SizeInBytes:      1000,
	}
}
