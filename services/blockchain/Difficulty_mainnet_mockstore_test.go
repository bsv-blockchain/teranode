package blockchain

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"io"
	"math/big"
	"os"
	"testing"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockchain/work"
	"github.com/bitcoin-sv/teranode/stores/blockchain"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/stretchr/testify/require"
)

// TestDifficultyCalculationWithMockStore tests difficulty calculation using MockStore
// This test uses the proper Store interface just like production code
func TestDifficultyCalculationWithMockStore(t *testing.T) {
	// Chainwork for block 886000 (the block before our first header)
	startChainworkHex := "0000000000000000000000000000000000000000016354e91e00b76e48f14aee"
	startChainwork := new(big.Int)
	chainworkBytes, err := hex.DecodeString(startChainworkHex)
	require.NoError(t, err)
	startChainwork.SetBytes(chainworkBytes)

	// Read all headers from file first
	headers, err := readMainnetHeadersForMockStore("./886001_888000_headers.bin")
	require.NoError(t, err, "Failed to read headers")
	require.Len(t, headers, 2000, "Expected 2000 headers")

	// Create and populate MockStore
	store := blockchain.NewMockStore()
	currentChainwork := new(big.Int).Set(startChainwork)
	startHeight := uint32(886001)

	// Populate the MockStore with blocks
	for i, header := range headers {
		// Calculate chainwork for this block
		blockWork := work.CalcBlockWork(binary.LittleEndian.Uint32(header.Bits.CloneBytes()))
		currentChainwork = new(big.Int).Add(currentChainwork, blockWork)

		// Create a Block object
		block := &model.Block{
			Header: header,
			Height: startHeight + uint32(i),
		}

		// Store the block
		hash := header.Hash()
		store.Blocks[*hash] = block
		store.BlockExists[*hash] = true
		store.BlockByHeight[block.Height] = block

		// Store chainwork
		chainworkBytes := currentChainwork.Bytes()
		paddedChainwork := make([]byte, 32)
		copy(paddedChainwork[32-len(chainworkBytes):], chainworkBytes)
		store.BlockChainWork[*hash] = paddedChainwork

		// Update best block
		if store.BestBlock == nil || block.Height > store.BestBlock.Height {
			store.BestBlock = block
		}
	}

	// Create difficulty calculator with the MockStore
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams = &chaincfg.MainNetParams
	d, err := NewDifficulty(store, ulogger.TestLogger{}, tSettings)
	require.NoError(t, err)

	// Skip first 146 blocks as we need history for difficulty calculation
	startIndex := 146
	ctx := context.Background()

	// Test all blocks
	passCount := 0
	failCount := 0
	for i := startIndex; i < len(headers); i++ {
		currentHeight := startHeight + uint32(i)

		// When calculating difficulty for block at height i, we use block i-1 as the reference
		// This simulates calculating difficulty for the NEXT block after i-1
		prevBlockHash := headers[i-1].Hash()

		// Get the ancestor block 144 blocks back from the previous block
		ancestorHash, err := store.GetHashOfAncestorBlock(ctx, prevBlockHash, DifficultyAdjustmentWindow)
		require.NoError(t, err, "Failed to get ancestor for block %d", currentHeight)

		// Get suitable blocks using the store (same as production)
		firstSuitableBlock, err := store.GetSuitableBlock(ctx, ancestorHash)
		require.NoError(t, err, "Failed to get first suitable block for height %d", currentHeight)

		// For last suitable block, also use the previous block (i-1) as in production
		lastSuitableBlock, err := store.GetSuitableBlock(ctx, prevBlockHash)
		require.NoError(t, err, "Failed to get last suitable block for height %d", currentHeight)

		// Calculate difficulty using Teranode's actual implementation
		calculatedNBits, err := d.computeTarget(firstSuitableBlock, lastSuitableBlock)
		require.NoError(t, err, "Failed to compute target for block %d", currentHeight)

		// Get actual nBits from the header
		actualNBits := binary.LittleEndian.Uint32(headers[i].Bits.CloneBytes())
		calculatedUint32 := binary.LittleEndian.Uint32(calculatedNBits.CloneBytes())

		// Verify exact match
		if calculatedUint32 == actualNBits {
			passCount++
		} else {
			failCount++
			t.Errorf("Height %d: nBits mismatch - calculated=0x%08x, actual=0x%08x (diff=%d)",
				currentHeight, calculatedUint32, actualNBits, int32(calculatedUint32)-int32(actualNBits))
		}
	}

	t.Logf("Tested %d blocks: %d passed, %d failed", passCount+failCount, passCount, failCount)
	require.Equal(t, 0, failCount, "All difficulty calculations must match exactly")
}

// readMainnetHeadersForMockStore reads block headers from a binary file
func readMainnetHeadersForMockStore(filename string) ([]*model.BlockHeader, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, errors.NewProcessingError("failed to open file", err)
	}
	defer file.Close()

	var headers []*model.BlockHeader

	for {
		// Read 80 bytes for each header
		headerBytes := make([]byte, 80)
		n, err := file.Read(headerBytes)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, errors.NewProcessingError("failed to read header", err)
		}
		if n != 80 {
			break // End of file
		}

		// Parse the header using the model package
		header, err := model.NewBlockHeaderFromBytes(headerBytes)
		if err != nil {
			return nil, errors.NewProcessingError("failed to parse header", err)
		}
		headers = append(headers, header)
	}

	return headers, nil
}
