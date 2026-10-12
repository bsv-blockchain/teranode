package netsync

import (
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
)

// partitionLegacyBlock determines how to partition a legacy block's transactions
// into subtrees so the resulting block satisfies model.Block.CheckMerkleRoot's
// rules: every non-final subtree shares the same power-of-two length, and the
// final subtree's length is at most that.
//
// Returns (subtreeSize, K, finalLeafCount):
//   - K is the number of subtrees
//   - subtreeSize is the capacity (and Length) of each non-final subtree (always
//     a power of two ≤ maxItems)
//   - finalLeafCount is the actual leaf count of the final subtree, in [1, subtreeSize]
//
// For totalLeaves ≤ maxItems, returns (totalLeaves, 1, totalLeaves), preserving
// today's single-subtree behaviour for small blocks.
//
// For larger totalLeaves, the partition is fixed: subtreeSize = maxItems and the
// final subtree holds the remainder. Because the duplicate-when-odd rule already
// pads the merkle tree internally at every level, the final subtree's leaf count
// can be any value in [1, subtreeSize] — it does not need to be a power of two.
// This avoids the pre-#901 degenerate case where adversarial leaf counts (e.g.
// 900,679 transactions) forced the partitioner all the way down to subtreeSize=2
// and produced hundreds of thousands of tiny subtrees.
func partitionLegacyBlock(totalLeaves, maxItems int) (subtreeSize, K, finalLeafCount int, err error) {
	if totalLeaves <= 0 {
		return 0, 0, 0, errors.NewProcessingError("partitionLegacyBlock: totalLeaves must be > 0, got %d", totalLeaves)
	}

	if maxItems <= 0 {
		return 0, 0, 0, errors.NewProcessingError("partitionLegacyBlock: maxItems must be > 0, got %d", maxItems)
	}

	if !subtreepkg.IsPowerOfTwo(maxItems) {
		return 0, 0, 0, errors.NewProcessingError("partitionLegacyBlock: maxItems must be a power of two, got %d", maxItems)
	}

	if totalLeaves <= maxItems {
		return totalLeaves, 1, totalLeaves, nil
	}

	subtreeSize = maxItems
	K = (totalLeaves + subtreeSize - 1) / subtreeSize
	finalLeafCount = totalLeaves - (K-1)*subtreeSize

	return subtreeSize, K, finalLeafCount, nil
}
