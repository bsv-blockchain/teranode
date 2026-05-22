package bump

import (
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util/merkleproof"
)

// ComputeCoinbaseBUMP computes the coinbase transaction's merkle proof in BUMP format (BRC-74).
// It builds the proof from the coinbase (at subtree index 0, tx index 0) to the block merkle root.
// subtree0 is the first subtree containing the coinbase transaction.
// subtreeHashes are the root hashes of all subtrees in the block.
func ComputeCoinbaseBUMP(subtree0 *subtreepkg.Subtree, subtreeHashes []*chainhash.Hash, blockHeight uint32) ([]byte, error) {
	if subtree0 == nil || len(subtreeHashes) == 0 {
		return nil, errors.NewInvalidArgumentError("subtree0 is nil or subtreeHashes is empty")
	}

	// Get the within-subtree proof: coinbase is at index 0 of subtree 0
	subtreeProofPtrs, err := subtree0.GetMerkleProof(0)
	if err != nil {
		return nil, errors.NewProcessingError("failed to get subtree merkle proof", err)
	}

	// Get the block-level proof: from subtree 0's root to block merkle root
	blockProofPtrs, _, err := merkleproof.GenerateBlockMerkleProof(subtreeHashes, 0)
	if err != nil {
		return nil, errors.NewProcessingError("failed to generate block merkle proof", err)
	}

	// Convert pointer slices to value slices for MerkleProof struct
	subtreeProof := make([]chainhash.Hash, len(subtreeProofPtrs))
	for i, h := range subtreeProofPtrs {
		subtreeProof[i] = *h
	}

	blockProof := make([]chainhash.Hash, len(blockProofPtrs))
	for i, h := range blockProofPtrs {
		blockProof[i] = *h
	}

	proof := &merkleproof.MerkleProof{
		TxID:             subtree0.Nodes[0].Hash,
		BlockHeight:      blockHeight,
		SubtreeIndex:     0,
		TxIndexInSubtree: 0,
		SubtreeProof:     subtreeProof,
		BlockProof:       blockProof,
	}

	bumpFormat, err := ConvertToBUMP(proof)
	if err != nil {
		return nil, errors.NewProcessingError("failed to convert to BUMP format", err)
	}

	bumpBytes, err := bumpFormat.EncodeBinary()
	if err != nil {
		return nil, errors.NewProcessingError("failed to encode BUMP binary", err)
	}

	return bumpBytes, nil
}
