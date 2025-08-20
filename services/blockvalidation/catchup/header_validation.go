// Package catchup provides header validation functions for Bitcoin block headers.
package catchup

import (
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
)

// ValidateHeaderProofOfWork validates that a block header meets the proof of work requirement.
// It checks that the block hash is less than or equal to the target difficulty.
func ValidateHeaderProofOfWork(header *model.BlockHeader) error {
	// Use the built-in method to check if header meets target difficulty
	ok, hashNum, err := header.HasMetTargetDifficulty()

	if !ok {
		return errors.NewNetworkInvalidResponseError(
			"[validateHeaderProofOfWork][%s] block header fails proof of work",
			header.Hash().String(), hashNum, err,
		)
	}

	return nil
}

// ValidateHeaderMerkleRoot performs basic validation of the merkle root.
// Full merkle root validation requires all transactions, so this just checks for obvious issues.
func ValidateHeaderMerkleRoot(header *model.BlockHeader) error {
	// Check that merkle root is not zero (unless it's the genesis block)
	if header.HashMerkleRoot.IsEqual(&chainhash.Hash{}) {
		// Genesis block is allowed to have zero merkle root in some test networks
		if header.HashPrevBlock.IsEqual(&chainhash.Hash{}) {
			return nil
		}
		return errors.NewNetworkInvalidResponseError(
			"block header %s has invalid zero merkle root",
			header.Hash().String(),
		)
	}

	return nil
}

// ValidateHeaderTimestamp performs basic timestamp validation.
// More comprehensive validation requires comparing with median time past.
func ValidateHeaderTimestamp(header *model.BlockHeader) error {
	// Reject blocks with timestamp too far in the future (2 hours)
	maxTime := time.Now().Add(2 * time.Hour)
	headerTime := time.Unix(int64(header.Timestamp), 0)

	if headerTime.After(maxTime) {
		return errors.NewNetworkInvalidResponseError(
			"block header %s timestamp %v is too far in the future (max %v)",
			header.Hash().String(), headerTime, maxTime,
		)
	}

	// Very old timestamps might indicate an issue, but we need to be careful
	// during initial sync where we process historical blocks
	minTime := time.Date(2009, 1, 3, 0, 0, 0, 0, time.UTC) // Bitcoin genesis
	if headerTime.Before(minTime) {
		return errors.NewNetworkInvalidResponseError(
			"block header %s timestamp %v is before Bitcoin genesis",
			header.Hash().String(), headerTime,
		)
	}

	return nil
}

// validateHeaderAgainstCheckpoints validates a header against known checkpoints.
//
// Parameters:
//   - header: Header to validate
//   - height: Block height
//   - checkpoints: Known good blocks at specific heights
//
// Returns:
//   - error: If header conflicts with a checkpoint
//
// Checkpoints are hardcoded known-good blocks that prevent following
// alternative chains before those points. If a header at a checkpoint
// height doesn't match the expected hash, it's rejected as malicious.
func ValidateHeaderAgainstCheckpoints(header *model.BlockHeader, height uint32, checkpoints []chaincfg.Checkpoint) error {
	// No checkpoints to validate against
	if len(checkpoints) == 0 {
		return nil
	}

	// Find checkpoint at this height
	for _, checkpoint := range checkpoints {
		if uint32(checkpoint.Height) == height {
			// We have a checkpoint at this height, verify the hash matches
			if !header.Hash().IsEqual(checkpoint.Hash) {
				return errors.NewNetworkPeerMaliciousError(
					"block header %s at height %d conflicts with checkpoint %s",
					header.Hash().String(), height, checkpoint.Hash.String(),
				)
			}

			// Header matches checkpoint
			return nil
		}
	}

	// No checkpoint at this height
	return nil
}

// ValidateHeaderAgainstCheckpoints validates a header against known checkpoints.
//
// Parameters:
//   - ctx: Context for cancellation
//   - headers: Batch of headers to validate
//
// Returns:
//   - error: If any header fails validation
//
// Validates each header for:
// - Proof of work
// - Merkle root sanity
// - Timestamp bounds
// - Checkpoint conflicts (if height is known)
//
// Processes headers individually with context cancellation checks.
