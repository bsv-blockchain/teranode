package merkleproof

import (
	"errors" //nolint:depguard
	"fmt"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	terr "github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
)

// MerkleProof represents a complete merkle proof for a transaction in a block.
// It contains all necessary information to verify that a transaction is included
// in a specific block following the SPV (Simplified Payment Verification) protocol.
//
// The proof consists of two parts:
// 1. SubtreeProof: The merkle path from the transaction to its subtree root
// 2. BlockProof: The merkle path from the subtree root to the block's merkle root
type MerkleProof struct {
	// TxID is the transaction hash being proven
	TxID chainhash.Hash

	// BlockHash is the hash of the block containing the transaction
	BlockHash chainhash.Hash

	// BlockHeight is the height of the block in the blockchain
	BlockHeight uint32

	// MerkleRoot is the merkle root of the block (from block header)
	MerkleRoot chainhash.Hash

	// SubtreeIndex is the position of the subtree in the block's subtree array
	SubtreeIndex int

	// TxIndexInSubtree is the position of the transaction within its subtree
	TxIndexInSubtree int

	// SubtreeRoot is the merkle root hash of the subtree containing the transaction
	SubtreeRoot chainhash.Hash

	// SubtreeProof contains the sibling hashes needed to compute from tx to subtree root
	// Each element is a hash of a sibling node in the merkle tree
	SubtreeProof []chainhash.Hash

	// BlockProof contains the sibling hashes needed to compute from subtree root to block merkle root
	// Each element is a hash of a sibling subtree root
	BlockProof []chainhash.Hash

	// Flags indicates whether each hash in the complete path is a left (0) or right (1) sibling
	// First part corresponds to SubtreeProof, second part to BlockProof
	Flags []int
}

// TxMetaData represents minimal transaction metadata needed for merkle proof construction.
// This is a simplified version to avoid import cycles with the stores/utxo package.
type TxMetaData struct {
	// BlockIDs contains the internal database block IDs where this transaction appears
	BlockIDs []uint32

	// BlockHeights contains the block heights where this transaction appears
	BlockHeights []uint32

	// SubtreeIdxs contains the subtree indexes where this transaction appears
	SubtreeIdxs []int
}

// MerkleProofConstructor provides methods for constructing merkle proofs.
// It requires access to block and subtree data through repository interfaces.
type MerkleProofConstructor interface {
	// GetTxMeta retrieves transaction metadata including block and subtree information
	GetTxMeta(txHash *chainhash.Hash) (*TxMetaData, error)

	// GetBlockByID retrieves a block by its internal database ID
	GetBlockByID(id uint64) (*model.Block, error)

	// GetBlockHeader retrieves a block header by its hash
	GetBlockHeader(blockHash *chainhash.Hash) (*model.BlockHeader, error)

	// GetSubtree retrieves subtree data by its hash
	GetSubtree(subtreeHash *chainhash.Hash) (*subtree.Subtree, error)

	// FindBlocksContainingSubtree finds all blocks that contain the specified subtree
	// Returns arrays of block IDs, block heights and corresponding subtree indices
	FindBlocksContainingSubtree(subtreeHash *chainhash.Hash) ([]uint32, []uint32, []int, error)
}

// ConstructMerkleProof constructs a complete merkle proof for a given transaction.
// It builds the proof path from the transaction through its subtree to the block's merkle root.
//
// Parameters:
//   - txID: The transaction hash to create a proof for
//   - repo: Repository interface providing access to blockchain data
//
// Returns:
//   - *MerkleProof: Complete merkle proof structure
//   - error: Any error encountered during proof construction
func ConstructMerkleProof(txID *chainhash.Hash, repo MerkleProofConstructor) (*MerkleProof, error) {
	if txID == nil {
		return nil, terr.NewInvalidArgumentError("transaction ID cannot be nil")
	}

	// Get transaction metadata
	txMeta, err := repo.GetTxMeta(txID)
	if err != nil {
		return nil, terr.NewProcessingError("failed to get transaction metadata", err)
	}

	// Check if transaction is in any block
	if len(txMeta.BlockIDs) == 0 || len(txMeta.BlockHeights) == 0 || len(txMeta.SubtreeIdxs) == 0 {
		return nil, terr.NewProcessingError("transaction not in any block")
	}

	// Use the first block containing the transaction
	blockID := txMeta.BlockIDs[0]
	blockHeight := txMeta.BlockHeights[0]
	subtreeIdx := txMeta.SubtreeIdxs[0]

	// Get block data using block ID instead of height for better performance
	block, err := repo.GetBlockByID(uint64(blockID))
	if err != nil {
		return nil, terr.NewProcessingError("failed to get block data", err)
	}

	// Validate subtree index
	if subtreeIdx < 0 || subtreeIdx >= len(block.Subtrees) {
		return nil, terr.NewProcessingError("invalid subtree index")
	}

	// Get the subtree hash
	subtreeHash := block.Subtrees[subtreeIdx]

	// Get the subtree data
	subtreeData, err := repo.GetSubtree(subtreeHash)
	if err != nil {
		return nil, terr.NewProcessingError("failed to get subtree data", err)
	}

	// Find the transaction index within the subtree
	txIndexInSubtree := -1
	for i, node := range subtreeData.Nodes {
		if node.Hash.IsEqual(txID) {
			txIndexInSubtree = i
			break
		}
	}

	if txIndexInSubtree == -1 {
		return nil, terr.NewProcessingError("transaction not found in subtree")
	}

	// Generate merkle proof within the subtree
	subtreeProof, err := subtreeData.GetMerkleProof(txIndexInSubtree)
	if err != nil {
		return nil, terr.NewProcessingError("failed to generate subtree merkle proof", err)
	}

	// Generate proof from subtree root to block merkle root
	blockProof, flags, err := generateBlockMerkleProof(block.Subtrees, subtreeIdx)
	if err != nil {
		return nil, terr.NewProcessingError("failed to generate block merkle proof", err)
	}

	// Get block hash and header
	blockHash := block.Hash()
	blockHeader, err := repo.GetBlockHeader(blockHash)
	if err != nil {
		return nil, terr.NewProcessingError("failed to get block header", err)
	}

	// Convert proof hashes from pointers to values
	subtreeProofHashes := make([]chainhash.Hash, len(subtreeProof))
	for i, hash := range subtreeProof {
		subtreeProofHashes[i] = *hash
	}

	blockProofHashes := make([]chainhash.Hash, len(blockProof))
	for i, hash := range blockProof {
		blockProofHashes[i] = *hash
	}

	// Build the complete proof structure
	proof := &MerkleProof{
		TxID:             *txID,
		BlockHash:        *blockHash,
		BlockHeight:      blockHeight,
		MerkleRoot:       *blockHeader.HashMerkleRoot,
		SubtreeIndex:     subtreeIdx,
		TxIndexInSubtree: txIndexInSubtree,
		SubtreeRoot:      *subtreeHash,
		SubtreeProof:     subtreeProofHashes,
		BlockProof:       blockProofHashes,
		Flags:            flags,
	}

	return proof, nil
}

// ConstructSubtreeMerkleProof constructs a merkle proof for a given subtree to the block's merkle root.
// This is used when you have a subtree hash and want to prove its inclusion in a block.
//
// Parameters:
//   - subtreeHash: The subtree hash to create a proof for
//   - repo: Repository interface providing access to blockchain data
//
// Returns:
//   - *MerkleProof: Merkle proof structure with only block-level proof populated
//   - error: Any error encountered during proof construction
func ConstructSubtreeMerkleProof(subtreeHash *chainhash.Hash, repo MerkleProofConstructor) (*MerkleProof, error) {
	if subtreeHash == nil {
		return nil, terr.NewInvalidArgumentError("subtree hash cannot be nil")
	}

	// Find blocks containing this subtree
	blockIDs, blockHeights, subtreeIndices, err := repo.FindBlocksContainingSubtree(subtreeHash)
	if err != nil {
		return nil, terr.NewProcessingError("failed to find blocks containing subtree", err)
	}

	// Check if subtree is in any block
	if len(blockIDs) == 0 || len(blockHeights) == 0 || len(subtreeIndices) == 0 {
		return nil, terr.NewProcessingError("subtree not found in any block")
	}

	// Use the first block containing the subtree
	blockID := blockIDs[0]
	blockHeight := blockHeights[0]
	subtreeIdx := subtreeIndices[0]

	// Get block data using block ID instead of height for better performance
	block, err := repo.GetBlockByID(uint64(blockID))
	if err != nil {
		return nil, terr.NewProcessingError("failed to get block data", err)
	}

	// Validate subtree index
	if subtreeIdx < 0 || subtreeIdx >= len(block.Subtrees) {
		return nil, terr.NewProcessingError("invalid subtree index")
	}

	// Verify the subtree hash matches
	if !block.Subtrees[subtreeIdx].IsEqual(subtreeHash) {
		return nil, terr.NewProcessingError("subtree hash mismatch")
	}

	// Generate proof from subtree root to block merkle root
	blockProof, flags, err := generateBlockMerkleProof(block.Subtrees, subtreeIdx)
	if err != nil {
		return nil, terr.NewProcessingError("failed to generate block merkle proof", err)
	}

	// Get block hash and header
	blockHash := block.Hash()
	blockHeader, err := repo.GetBlockHeader(blockHash)
	if err != nil {
		return nil, terr.NewProcessingError("failed to get block header", err)
	}

	// Convert proof hashes from pointers to values
	blockProofHashes := make([]chainhash.Hash, len(blockProof))
	for i, hash := range blockProof {
		blockProofHashes[i] = *hash
	}

	// Build the proof structure for subtree-only proof
	proof := &MerkleProof{
		TxID:             chainhash.Hash{}, // Empty for subtree proofs
		BlockHash:        *blockHash,
		BlockHeight:      blockHeight,
		MerkleRoot:       *blockHeader.HashMerkleRoot,
		SubtreeIndex:     subtreeIdx,
		TxIndexInSubtree: -1, // -1 indicates this is a subtree proof, not a transaction proof
		SubtreeRoot:      *subtreeHash,
		SubtreeProof:     []chainhash.Hash{}, // Empty for subtree proofs
		BlockProof:       blockProofHashes,
		Flags:            flags,
	}

	return proof, nil
}

// VerifyMerkleProof verifies a merkle proof and returns whether it's valid.
// It reconstructs the merkle root from the transaction hash using the proof path
// and compares it with the expected merkle root.
//
// Parameters:
//   - proof: The merkle proof to verify
//
// Returns:
//   - bool: True if the proof is valid, false otherwise
//   - *chainhash.Hash: The block hash if the proof is valid, nil otherwise
//   - error: Any error encountered during verification
func VerifyMerkleProof(proof *MerkleProof) (bool, *chainhash.Hash, error) {
	if proof == nil {
		return false, nil, terr.NewInvalidArgumentError("proof cannot be nil")
	}

	// Start with the transaction hash
	currentHash := proof.TxID

	// Apply subtree proof path
	for i, proofHash := range proof.SubtreeProof {
		var combined []byte

		// Check if we have a corresponding flag (for proper ordering)
		if i < len(proof.Flags) {
			if proof.Flags[i] == 0 {
				// Sibling is on the left
				combined = append(proofHash.CloneBytes(), currentHash.CloneBytes()...)
			} else {
				// Sibling is on the right
				combined = append(currentHash.CloneBytes(), proofHash.CloneBytes()...)
			}
		} else {
			// Default to standard ordering (current || sibling)
			combined = append(currentHash.CloneBytes(), proofHash.CloneBytes()...)
		}

		currentHash = chainhash.DoubleHashH(combined)
	}

	// Verify we reached the subtree root
	if !currentHash.IsEqual(&proof.SubtreeRoot) {
		return false, nil, nil
	}

	// Continue with block proof path
	currentHash = proof.SubtreeRoot
	flagOffset := len(proof.SubtreeProof)

	for i, proofHash := range proof.BlockProof {
		var combined []byte

		// Check if we have a corresponding flag
		flagIndex := flagOffset + i
		if flagIndex < len(proof.Flags) {
			if proof.Flags[flagIndex] == 0 {
				// Sibling is on the left
				combined = append(proofHash.CloneBytes(), currentHash.CloneBytes()...)
			} else {
				// Sibling is on the right
				combined = append(currentHash.CloneBytes(), proofHash.CloneBytes()...)
			}
		} else {
			// Default to standard ordering
			combined = append(currentHash.CloneBytes(), proofHash.CloneBytes()...)
		}

		currentHash = chainhash.DoubleHashH(combined)
	}

	// Verify we reached the expected merkle root
	if !currentHash.IsEqual(&proof.MerkleRoot) {
		return false, nil, nil
	}

	return true, &proof.BlockHash, nil
}

// VerifyMerkleProofForCoinbase verifies a merkle proof specifically for a coinbase transaction.
// Coinbase transactions are always at position 0 in the merkle tree and may require special handling.
//
// Parameters:
//   - proof: The merkle proof to verify
//
// Returns:
//   - bool: True if the proof is valid for a coinbase transaction
//   - *chainhash.Hash: The block hash if valid, nil otherwise
//   - error: Any error encountered during verification
func VerifyMerkleProofForCoinbase(proof *MerkleProof) (bool, *chainhash.Hash, error) {
	if proof == nil {
		return false, nil, terr.NewInvalidArgumentError("proof cannot be nil")
	}

	// Coinbase must be at index 0 in the first subtree
	if proof.SubtreeIndex != 0 || proof.TxIndexInSubtree != 0 {
		return false, nil, errors.New(fmt.Sprintf("invalid coinbase position: subtree %d, index %d",
			proof.SubtreeIndex, proof.TxIndexInSubtree))
	}

	// Use standard verification
	return VerifyMerkleProof(proof)
}

// generateBlockMerkleProof generates the merkle proof from a subtree to the block's merkle root.
// It builds a merkle tree from all subtree roots and returns the proof path for the specified subtree.
//
// Parameters:
//   - subtrees: Array of all subtree hashes in the block
//   - subtreeIndex: Index of the target subtree
//
// Returns:
//   - []*chainhash.Hash: Array of sibling hashes forming the proof path
//   - []int: Flags indicating if each hash is a left (0) or right (1) sibling
//   - error: Any error encountered during proof generation
func generateBlockMerkleProof(subtrees []*chainhash.Hash, subtreeIndex int) ([]*chainhash.Hash, []int, error) {
	if len(subtrees) == 0 {
		return nil, nil, terr.NewProcessingError("no subtrees in block")
	}

	if subtreeIndex < 0 || subtreeIndex >= len(subtrees) {
		return nil, nil, terr.NewProcessingError("invalid subtree index")
	}

	// Handle special case of single subtree
	if len(subtrees) == 1 {
		return []*chainhash.Hash{}, []int{}, nil
	}

	// Generate proof path
	proof := make([]*chainhash.Hash, 0)
	flags := make([]int, 0)

	currentIndex := subtreeIndex
	currentLevel := subtrees

	for len(currentLevel) > 1 {
		// Get sibling index
		siblingIndex := currentIndex ^ 1

		// Add sibling to proof if it exists
		if siblingIndex < len(currentLevel) {
			proof = append(proof, currentLevel[siblingIndex])
			// Flag: 0 if sibling is on the left, 1 if on the right
			if siblingIndex < currentIndex {
				flags = append(flags, 0)
			} else {
				flags = append(flags, 1)
			}
		} else {
			// No sibling, duplicate current node (Bitcoin merkle tree behavior)
			proof = append(proof, currentLevel[currentIndex])
			flags = append(flags, 1) // Treat as right sibling
		}

		// Move to next level
		currentIndex >>= 1

		// Build next level by hashing pairs
		nextLevel := make([]*chainhash.Hash, 0)
		for i := 0; i < len(currentLevel); i += 2 {
			if i+1 < len(currentLevel) {
				// Hash pair
				combined := append(currentLevel[i].CloneBytes(), currentLevel[i+1].CloneBytes()...)
				hash := chainhash.DoubleHashH(combined)
				nextLevel = append(nextLevel, &hash)
			} else {
				// Odd node, duplicate it
				combined := append(currentLevel[i].CloneBytes(), currentLevel[i].CloneBytes()...)
				hash := chainhash.DoubleHashH(combined)
				nextLevel = append(nextLevel, &hash)
			}
		}
		currentLevel = nextLevel
	}

	return proof, flags, nil
}
