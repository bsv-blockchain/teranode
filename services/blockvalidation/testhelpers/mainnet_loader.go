package testhelpers

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// TestBlockData stores the block data loaded from file in JSON format
type TestBlockData struct {
	Blocks     []json.RawMessage `json:"blocks"` // Full blocks as JSON
	Count      int               `json:"count"`
	Generated  string            `json:"generated"`
	Source     string            `json:"source"`
	StartBlock int               `json:"start_block"`
	EndBlock   int               `json:"end_block"`
}

var (
	// Cache for loaded mainnet blocks to avoid repeated file reads
	cachedMainnetBlocks     []*model.Block
	cachedMainnetHeaders    []*model.BlockHeader
	cachedMainnetBlocksOnce sync.Once
	cachedMainnetBlocksErr  error
)

// loadMainnetBlocks loads real mainnet blocks from the JSON file.
// The blocks are cached after first load to improve test performance.
//
// Returns:
//   - []*model.Block: Loaded blocks
//   - []*model.BlockHeader: Just the headers
//   - error: If loading or parsing fails
func loadMainnetBlocks() ([]*model.Block, []*model.BlockHeader, error) {
	cachedMainnetBlocksOnce.Do(func() {
		// Build path to testdata file
		testdataPath := filepath.Join("testdata", "mainnet_blocks.json")

		// Read the file
		data, err := os.ReadFile(testdataPath)
		if err != nil {
			cachedMainnetBlocksErr = errors.NewProcessingError("failed to read mainnet blocks file %s: %w", testdataPath, err)
			return
		}

		// Parse JSON
		var testData TestBlockData
		if err := json.Unmarshal(data, &testData); err != nil {
			cachedMainnetBlocksErr = errors.NewProcessingError("failed to parse mainnet blocks JSON: %w", err)
			return
		}

		// Convert JSON blocks to model.Block and extract headers
		blocks := make([]*model.Block, len(testData.Blocks))
		headers := make([]*model.BlockHeader, len(testData.Blocks))

		for i, blockJSON := range testData.Blocks {
			// Parse the JSON block
			var blockData map[string]interface{}
			if err := json.Unmarshal(blockJSON, &blockData); err != nil {
				cachedMainnetBlocksErr = errors.NewProcessingError("failed to parse block %d JSON: %w", i, err)
				return
			}

			// Extract header fields
			headerData, ok := blockData["header"].(map[string]interface{})
			if !ok {
				cachedMainnetBlocksErr = errors.NewProcessingError("block %d missing header", i)
				return
			}

			// Build the header from JSON fields
			header := &model.BlockHeader{}

			if v, ok := headerData["version"].(float64); ok {
				header.Version = uint32(v)
			}

			if hashStr, ok := headerData["hash_prev_block"].(string); ok {
				hash, err := chainhash.NewHashFromStr(hashStr)
				if err != nil {
					cachedMainnetBlocksErr = errors.NewProcessingError("block %d: invalid prev hash: %w", i, err)
					return
				}
				header.HashPrevBlock = hash
			}

			if hashStr, ok := headerData["hash_merkle_root"].(string); ok {
				hash, err := chainhash.NewHashFromStr(hashStr)
				if err != nil {
					cachedMainnetBlocksErr = errors.NewProcessingError("block %d: invalid merkle root: %w", i, err)
					return
				}
				header.HashMerkleRoot = hash
			}

			if v, ok := headerData["timestamp"].(float64); ok {
				header.Timestamp = uint32(v)
			}

			if bitsStr, ok := headerData["bits"].(string); ok {
				nBits, err := model.NewNBitFromString(bitsStr)
				if err != nil {
					cachedMainnetBlocksErr = errors.NewProcessingError("block %d: invalid bits: %w", i, err)
					return
				}
				header.Bits = *nBits
			}

			if v, ok := headerData["nonce"].(float64); ok {
				header.Nonce = uint32(v)
			}

			headers[i] = header

			// Create block with header and height
			height := uint32(i) // Heights are 0-indexed
			if h, ok := blockData["height"].(float64); ok {
				height = uint32(h)
			}

			blocks[i] = &model.Block{
				Header: header,
				Height: height,
			}
		}

		cachedMainnetBlocks = blocks
		cachedMainnetHeaders = headers
	})

	if cachedMainnetBlocksErr != nil {
		return nil, nil, cachedMainnetBlocksErr
	}

	return cachedMainnetBlocks, cachedMainnetHeaders, nil
}

// GetMainnetHeaders returns a slice of real mainnet headers with valid proof of work.
// All headers are from the actual Bitcoin mainnet with proper parent-child relationships.
// Data is cached in memory after first load for performance.
//
// Parameters:
//   - t: Testing instance for assertions
//   - count: Number of headers needed
//
// Returns:
//   - []*model.BlockHeader: Slice of headers with length == count
func GetMainnetHeaders(t *testing.T, count int) []*model.BlockHeader {
	t.Helper()

	_, headers, err := loadMainnetBlocks()
	require.NoError(t, err, "Failed to load mainnet blocks")
	require.GreaterOrEqual(t, len(headers), count, "Not enough mainnet headers available (need %d, have %d)", count, len(headers))

	// Return the requested number of headers from cache
	return headers[:count]
}

// GetMainnetBlocks returns a slice of real mainnet blocks with valid proof of work.
// Data is cached in memory after first load for performance.
//
// Parameters:
//   - t: Testing instance for assertions
//   - count: Number of blocks needed
//
// Returns:
//   - []*model.Block: Slice of blocks with length == count
func GetMainnetBlocks(t *testing.T, count int) []*model.Block {
	t.Helper()

	blocks, _, err := loadMainnetBlocks()
	require.NoError(t, err, "Failed to load mainnet blocks")
	require.GreaterOrEqual(t, len(blocks), count, "Not enough mainnet blocks available (need %d, have %d)", count, len(blocks))

	// Return the requested number of blocks from cache
	return blocks[:count]
}

// GetMainnetHeadersRange returns a slice of real mainnet headers within a specific range.
//
// Parameters:
//   - t: Testing instance for assertions
//   - start: Starting index (inclusive)
//   - end: Ending index (exclusive)
//
// Returns:
//   - []*model.BlockHeader: Slice of headers from start to end
func GetMainnetHeadersRange(t *testing.T, start, end int) []*model.BlockHeader {
	t.Helper()

	require.Less(t, start, end, "Start index must be less than end index")

	_, headers, err := loadMainnetBlocks()
	require.NoError(t, err, "Failed to load mainnet blocks")
	require.GreaterOrEqual(t, len(headers), end, "Not enough mainnet headers available (need up to index %d, have %d)", end, len(headers))

	return headers[start:end]
}

// GetMainnetBlocksRange returns a slice of real mainnet blocks within a specific range.
//
// Parameters:
//   - t: Testing instance for assertions
//   - start: Starting index (inclusive)
//   - end: Ending index (exclusive)
//
// Returns:
//   - []*model.Block: Slice of blocks from start to end
func GetMainnetBlocksRange(t *testing.T, start, end int) []*model.Block {
	t.Helper()

	require.Less(t, start, end, "Start index must be less than end index")

	blocks, _, err := loadMainnetBlocks()
	require.NoError(t, err, "Failed to load mainnet blocks")
	require.GreaterOrEqual(t, len(blocks), end, "Not enough mainnet blocks available (need up to index %d, have %d)", end, len(blocks))

	return blocks[start:end]
}
