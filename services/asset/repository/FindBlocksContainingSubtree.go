package repository

import (
	"context"
	"errors" //nolint:depguard
	"fmt"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// FindBlocksContainingSubtree finds all blocks that contain the specified subtree.
// It returns arrays of block IDs, block heights and corresponding subtree indices.
//
// Parameters:
//   - ctx: Context for the operation
//   - subtreeHash: The hash of the subtree to find
//
// Returns:
//   - []uint32: Array of block IDs containing the subtree
//   - []uint32: Array of block heights containing the subtree
//   - []int: Array of subtree indices within each block
//   - error: Any error encountered during the search
func (repo *Repository) FindBlocksContainingSubtree(ctx context.Context, subtreeHash *chainhash.Hash) ([]uint32, []uint32, []int, error) {
	if subtreeHash == nil {
		return nil, nil, nil, errors.New("subtree hash cannot be nil")
	}

	// Check if subtree exists
	exists, err := repo.GetSubtreeExists(ctx, subtreeHash)
	if err != nil {
		return nil, nil, nil, errors.New(fmt.Sprintf("failed to check subtree existence: %s", err.Error()))
	}

	if !exists {
		return nil, nil, nil, errors.New(fmt.Sprintf("subtree with hash %s does not exist", subtreeHash.String()))
	}

	// Use the blockchain client to find blocks containing this subtree
	// This uses an optimized SQL query in the blockchain store layer
	// Limit to last 100 blocks for performance (matches previous behavior)
	const maxSearchBlocks = 100

	blocks, err := repo.BlockchainClient.FindBlocksContainingSubtree(ctx, subtreeHash, maxSearchBlocks)
	if err != nil {
		return nil, nil, nil, errors.New(fmt.Sprintf("failed to find blocks containing subtree: %s", err.Error()))
	}

	if len(blocks) == 0 {
		return nil, nil, nil, errors.New(fmt.Sprintf("subtree %s not found in any recent blocks", subtreeHash.String()))
	}

	// Process the blocks to extract IDs, heights, and find subtree indices
	var blockIDs []uint32
	var blockHeights []uint32
	var subtreeIndices []int

	for _, block := range blocks {
		// Find the subtree index within this block
		for i, blockSubtreeHash := range block.Subtrees {
			if blockSubtreeHash.IsEqual(subtreeHash) {
				blockIDs = append(blockIDs, block.ID)
				blockHeights = append(blockHeights, block.Height)
				subtreeIndices = append(subtreeIndices, i)
				break
			}
		}
	}

	return blockIDs, blockHeights, subtreeIndices, nil
}
