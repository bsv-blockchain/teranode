package netsync

import (
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
)

// partitionLegacyBlock determines how to partition a legacy block's transactions
// into subtrees so the resulting block satisfies model.Block.CheckMerkleRoot's
// rules: all non-final subtrees must share the same length, the final subtree
// must be at most that length and a power of two if smaller.
//
// Returns (subtreeSize, K, finalLeafCount):
//   - K is the number of subtrees
//   - subtreeSize is the capacity (and Length) of each non-final subtree
//   - finalLeafCount is the actual leaf count of the final subtree (== subtreeSize
//     if the final subtree is full, smaller power-of-two otherwise)
//
// For totalLeaves <= maxItems, returns (totalLeaves, 1, totalLeaves), preserving
// today's single-subtree behaviour.
//
// For larger totalLeaves, picks the largest power-of-two s ≤ maxItems such that
// totalLeaves - (ceil(totalLeaves/s)-1)*s is a power of two. Always terminates
// because s=1 yields r=1 which is power-of-two.
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

	for s := maxItems; s >= 1; s /= 2 {
		kCalc := (totalLeaves + s - 1) / s
		r := totalLeaves - (kCalc-1)*s

		if r == s || subtreepkg.IsPowerOfTwo(r) {
			return s, kCalc, r, nil
		}
	}

	return 0, 0, 0, errors.NewProcessingError("partitionLegacyBlock: no valid partition found for totalLeaves=%d, maxItems=%d", totalLeaves, maxItems)
}
