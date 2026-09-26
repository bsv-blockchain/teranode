package merkleproof

import (
	"errors" //nolint:depguard
	"fmt"
	"math/bits"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	terr "github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
)

const errMsgInvalidSubtreeIndex = "invalid subtree index"

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

	// IsBlockOnMainChain reports whether the given internal block ID at the given
	// height is part of the current best chain. The height lets implementations
	// route the lookup through height-windowed caches.
	IsBlockOnMainChain(blockID, blockHeight uint32) (bool, error)
}

// BlockRoots holds the block-level values a merkle proof needs that can only be derived from the
// block's first and final subtrees:
//
//   - FirstRoot: the root of subtree 0 with the coinbase placeholder replaced by the real coinbase
//     txid, or nil when subtree 0 carries no placeholder.
//   - LastRoot: the lifted (padded) root of an incomplete final subtree, or nil when the final
//     subtree is complete.
//   - TargetHeight: the first subtree's height, which the lift is computed against.
//
// All three are a pure function of the block: a block's subtree list is fixed by its header and
// subtrees are content-addressed, so an entry never needs invalidating.
type BlockRoots struct {
	FirstRoot    *chainhash.Hash
	LastRoot     *chainhash.Hash
	TargetHeight int
}

// BlockRootsCache is an optional extension of MerkleProofConstructor. When the repository passed to
// ConstructMerkleProof also implements it, the block-level roots are memoized per block, so repeat
// proof requests against the same block deserialize only the transaction's own subtree instead of
// up to three complete subtrees. Implementations must be safe for concurrent use.
type BlockRootsCache interface {
	// BlockRoots returns the memoized roots for the given block, and whether an entry was present.
	BlockRoots(blockHash *chainhash.Hash) (*BlockRoots, bool)

	// SetBlockRoots memoizes the roots for the given block.
	SetBlockRoots(blockHash *chainhash.Hash, roots *BlockRoots)
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
//
// Only main-chain entries are considered; transactions found exclusively in orphan
// blocks return a TxNotFoundError so clients don't receive unverifiable proofs.
func ConstructMerkleProof(txID *chainhash.Hash, repo MerkleProofConstructor) (*MerkleProof, error) {
	if txID == nil {
		return nil, terr.NewInvalidArgumentError("transaction ID cannot be nil")
	}

	// Get transaction metadata
	txMeta, err := repo.GetTxMeta(txID)
	if err != nil {
		// Unknown hash: real stores signal a missing key with ErrTxNotFound.
		// Deliberately return a plain NotFoundError WITHOUT wrapping the cause —
		// teranode's errors.Is matches codes through the wrapped chain, so
		// carrying ERR_TX_NOT_FOUND here would misreport an unknown transaction
		// as an orphan-only transaction at the HTTP layer.
		if terr.Is(err, terr.ErrTxNotFound) || terr.Is(err, terr.ErrNotFound) {
			return nil, terr.NewNotFoundError("transaction %s not found", txID.String())
		}
		return nil, terr.NewProcessingError("failed to get transaction metadata", err)
	}

	// Check if transaction is in any block
	if len(txMeta.BlockIDs) == 0 || len(txMeta.BlockHeights) == 0 || len(txMeta.SubtreeIdxs) == 0 {
		return nil, terr.NewNotFoundError("transaction not in any block")
	}

	// Guard against malformed parallel arrays before iteration.
	if len(txMeta.BlockHeights) != len(txMeta.BlockIDs) || len(txMeta.SubtreeIdxs) != len(txMeta.BlockIDs) {
		return nil, terr.NewProcessingError("malformed tx meta: parallel arrays length mismatch")
	}

	// Find the entry that is on the current main chain. If a tx appears in a fork
	// plus the main chain, the main-chain entry is the only one a client can verify
	// against a header it knows about.
	// Best-effort: a reorg between this main-chain check and the GetBlockByID
	// call below could return a proof against a re-orphaned block. SPV clients
	// should re-fetch on header divergence.
	mainChainIdx := -1
	for i, id := range txMeta.BlockIDs {
		onChain, err := repo.IsBlockOnMainChain(id, txMeta.BlockHeights[i])
		if err != nil {
			return nil, terr.NewProcessingError("failed to check main chain", err)
		}
		if onChain {
			mainChainIdx = i
			break
		}
	}
	if mainChainIdx == -1 {
		return nil, terr.NewTxNotFoundError("transaction not in main chain")
	}

	blockID := txMeta.BlockIDs[mainChainIdx]
	blockHeight := txMeta.BlockHeights[mainChainIdx]
	subtreeIdx := txMeta.SubtreeIdxs[mainChainIdx]

	// Get block data using block ID instead of height for better performance
	block, err := repo.GetBlockByID(uint64(blockID))
	if err != nil {
		return nil, terr.NewProcessingError("failed to get block data", err)
	}

	// Validate subtree index
	if subtreeIdx < 0 || subtreeIdx >= len(block.Subtrees) {
		return nil, terr.NewProcessingError(errMsgInvalidSubtreeIndex)
	}

	// Get the subtree hash
	subtreeHash := block.Subtrees[subtreeIdx]

	// Get the subtree data
	subtreeData, err := repo.GetSubtree(subtreeHash)
	if err != nil {
		return nil, terr.NewProcessingError("failed to get subtree data", err)
	}

	// The first subtree of a block stores a coinbase placeholder at index 0, because the coinbase
	// txid is not known until the block is assembled. The block header's merkle root is computed
	// with that placeholder replaced by the real coinbase txid (see model.Block.CheckMerkleRoot),
	// so the proof must mirror that replacement or it will never reconstruct the header root.
	var coinbaseHash *chainhash.Hash
	if block.CoinbaseTx != nil {
		coinbaseHash = block.CoinbaseTx.TxIDChainHash()
	}

	// Find the transaction index within the subtree
	txIndexInSubtree := -1
	for i, node := range subtreeData.Nodes {
		if node.Hash.IsEqual(txID) {
			txIndexInSubtree = i
			break
		}
	}

	// The coinbase transaction itself is stored as the placeholder in the first subtree, so a direct
	// hash lookup fails. Treat a request for the coinbase txid as index 0 of the first subtree.
	if txIndexInSubtree == -1 && subtreeIdx == 0 && coinbaseHash != nil &&
		txID.IsEqual(coinbaseHash) && hasCoinbasePlaceholder(subtreeData) {
		txIndexInSubtree = 0
	}

	if txIndexInSubtree == -1 {
		return nil, terr.NewProcessingError("transaction not found in subtree")
	}

	// The block-level roots (coinbase-replaced first root, lifted final root and the height the
	// lift is measured against) are a pure function of the block, but deriving them costs up to two
	// extra complete-subtree deserializations on top of the transaction's own subtree. When the
	// repository offers a cache, reuse them across requests for the same block.
	lastIdx := len(block.Subtrees) - 1
	blockHash := block.Hash()

	// Fetched here (rather than only when building the proof) so a freshly derived root can be
	// checked against it before caching.
	blockHeader, err := repo.GetBlockHeader(blockHash)
	if err != nil {
		return nil, terr.NewProcessingError("failed to get block header", err)
	}

	rootsCache, _ := repo.(BlockRootsCache)

	roots, cached := blockRootsFromCache(rootsCache, blockHash)
	if !cached {
		roots, err = deriveBlockRoots(repo, block, subtreeData, subtreeIdx, lastIdx, coinbaseHash)
		if err != nil {
			return nil, err
		}

		// A bad derivation (e.g. via the SubtreeToCheck fallback in GetSubtree returning the wrong
		// bytes for a subtree it can no longer identify) must not be memoized: an uncached miss only
		// affects the request that hit it, but a cached one poisons every proof for this block until
		// eviction. Verify the reconstructed top root against the header before writing the cache.
		candidateRoots := buildEffectiveRoots(block.Subtrees, roots.FirstRoot, roots.LastRoot, lastIdx)

		topRoot, err := computeTopRoot(candidateRoots)
		if err != nil {
			return nil, err
		}

		if !topRoot.IsEqual(blockHeader.HashMerkleRoot) {
			return nil, terr.NewProcessingError("derived block roots do not reconstruct the block header merkle root")
		}

		if rootsCache != nil {
			rootsCache.SetBlockRoots(blockHash, roots)
		}
	}

	firstRoot, lastRoot, targetHeight := roots.FirstRoot, roots.LastRoot, roots.TargetHeight

	// Build the effective subtree-root leaves for the block-level (top) merkle tree: the placeholder
	// root of the first subtree is replaced with the coinbase-replaced root, and an incomplete final
	// subtree's root is replaced with its lifted root.
	effectiveRoots := buildEffectiveRoots(block.Subtrees, firstRoot, lastRoot, lastIdx)

	// Generate the subtree-internal proof. For the first subtree, build it from a coinbase-replaced
	// clone so the proof carries the real coinbase txid rather than the placeholder. Duplicate() copies
	// the node slice, so the cached/stored subtree is never mutated.
	proofSubtree := subtreeData
	if subtreeIdx == 0 && firstRoot != nil {
		clone := subtreeData.Duplicate()
		clone.ReplaceRootNode(coinbaseHash, 0, uint64(block.CoinbaseTx.Size())) //nolint:gosec
		proofSubtree = clone
	}

	subtreeProof, err := proofSubtree.GetMerkleProof(txIndexInSubtree)
	if err != nil {
		return nil, terr.NewProcessingError("failed to generate subtree merkle proof", err)
	}

	// Generate proof from subtree root to block merkle root using the effective roots.
	blockProof, blockFlags, err := GenerateBlockMerkleProof(effectiveRoots, subtreeIdx)
	if err != nil {
		return nil, terr.NewProcessingError("failed to generate block merkle proof", err)
	}

	// Convert subtree proof hashes from pointers to values and compute their left/right flags from the
	// transaction index parity at each level (the previous code only carried block-level flags, which
	// left odd-index transactions with the wrong sibling ordering during verification).
	subtreeProofHashes := make([]chainhash.Hash, 0, len(subtreeProof))
	subtreeFlags := make([]int, 0, len(subtreeProof))
	levelIndex := txIndexInSubtree

	for _, hash := range subtreeProof {
		subtreeProofHashes = append(subtreeProofHashes, *hash)

		if levelIndex%2 == 0 {
			subtreeFlags = append(subtreeFlags, 1) // sibling is on the right
		} else {
			subtreeFlags = append(subtreeFlags, 0) // sibling is on the left
		}

		levelIndex >>= 1
	}

	// The root this subtree contributes to the top-level tree.
	subtreeRootForProof := subtreeHash
	if subtreeIdx == 0 && firstRoot != nil {
		subtreeRootForProof = firstRoot
	}

	// When the transaction lives in an incomplete final subtree, the subtree-internal proof only
	// reaches the subtree's natural root, but the top tree uses the lifted root. Append the lift levels
	// (each a self-hash, H(h,h)) so verification reaches the lifted root that the top tree expects.
	if subtreeIdx == lastIdx && lastRoot != nil {
		actualHeight := bits.Len(uint(proofSubtree.Length() - 1))
		current := *proofSubtree.RootHash()

		for h := actualHeight; h < targetHeight; h++ {
			subtreeProofHashes = append(subtreeProofHashes, current)
			subtreeFlags = append(subtreeFlags, 1) // duplicate: combined = current || current

			combined := append(current.CloneBytes(), current.CloneBytes()...)
			current = chainhash.DoubleHashH(combined)
		}

		subtreeRootForProof = lastRoot
	}

	blockProofHashes := make([]chainhash.Hash, len(blockProof))
	for i, hash := range blockProof {
		blockProofHashes[i] = *hash
	}

	// Flags cover the subtree-level path first, then the block-level path (the order in which
	// VerifyMerkleProof consumes them).
	flags := append(subtreeFlags, blockFlags...)

	// Build the complete proof structure
	proof := &MerkleProof{
		TxID:             *txID,
		BlockHash:        *blockHash,
		BlockHeight:      blockHeight,
		MerkleRoot:       *blockHeader.HashMerkleRoot,
		SubtreeIndex:     subtreeIdx,
		TxIndexInSubtree: txIndexInSubtree,
		SubtreeRoot:      *subtreeRootForProof,
		SubtreeProof:     subtreeProofHashes,
		BlockProof:       blockProofHashes,
		Flags:            flags,
	}

	return proof, nil
}

// hasCoinbasePlaceholder reports whether a subtree stores the coinbase placeholder at index 0, as
// the first subtree of a block does.
func hasCoinbasePlaceholder(st *subtree.Subtree) bool {
	return st != nil && len(st.Nodes) > 0 && st.Nodes[0].Hash.Equal(subtree.CoinbasePlaceholderHashValue)
}

// buildEffectiveRoots returns the block-level subtree-root leaves used for the top merkle tree: the
// placeholder root of the first subtree replaced with firstRoot (coinbase-replaced), and the final
// subtree's root replaced with lastRoot (lifted), when either is non-nil. Returns subtrees unmodified
// when both are nil.
func buildEffectiveRoots(subtrees []*chainhash.Hash, firstRoot, lastRoot *chainhash.Hash, lastIdx int) []*chainhash.Hash {
	if firstRoot == nil && lastRoot == nil {
		return subtrees
	}

	effective := make([]*chainhash.Hash, len(subtrees))
	copy(effective, subtrees)

	if firstRoot != nil {
		effective[0] = firstRoot
	}

	if lastRoot != nil {
		effective[lastIdx] = lastRoot
	}

	return effective
}

// computeTopRoot reconstructs the block-level merkle root from the given subtree-root leaves, using
// the same pairwise-hash reduction (odd node duplicated) that GenerateBlockMerkleProof's proof path
// and VerifyMerkleProof's reconstruction both assume.
func computeTopRoot(roots []*chainhash.Hash) (*chainhash.Hash, error) {
	if len(roots) == 0 {
		return nil, terr.NewProcessingError("no subtrees in block")
	}

	if len(roots) == 1 {
		return roots[0], nil
	}

	nodes := make([]subtree.Node, len(roots))
	for i, r := range roots {
		nodes[i] = subtree.Node{Hash: *r}
	}

	store, err := subtree.BuildMerkleTreeStoreFromBytes(nodes)
	if err != nil {
		return nil, terr.NewProcessingError("failed to compute block merkle root", err)
	}

	root := (*store)[len(*store)-1]

	return &root, nil
}

// blockRootsFromCache reads the memoized block roots, tolerating a nil cache and a nil entry.
func blockRootsFromCache(cache BlockRootsCache, blockHash *chainhash.Hash) (*BlockRoots, bool) {
	if cache == nil {
		return nil, false
	}

	roots, ok := cache.BlockRoots(blockHash)
	if !ok || roots == nil {
		return nil, false
	}

	return roots, true
}

// deriveBlockRoots computes the block-level root overrides the top-level merkle tree needs:
//
//   - the first subtree's root with the coinbase placeholder replaced by the real coinbase txid
//     (the block header's merkle root is computed that way, see model.Block.CheckMerkleRoot);
//   - the final subtree's lifted root when it is incomplete, so it occupies the slot of a
//     full-capacity subtree.
//
// It loads at most the block's first and final subtrees, reusing subtreeData when the transaction's
// own subtree is one of them.
func deriveBlockRoots(repo MerkleProofConstructor, block *model.Block, subtreeData *subtree.Subtree,
	subtreeIdx, lastIdx int, coinbaseHash *chainhash.Hash) (*BlockRoots, error) {
	firstRoot, targetLength, targetHeight, err := deriveFirstSubtreeRoot(repo, block, subtreeData, subtreeIdx, lastIdx, coinbaseHash)
	if err != nil {
		return nil, err
	}

	roots := &BlockRoots{FirstRoot: firstRoot, TargetHeight: targetHeight}

	// targetLength is zero for a single-subtree block: there is no final subtree to lift.
	if targetLength == 0 {
		return roots, nil
	}

	lastSubtree := subtreeData
	if subtreeIdx != lastIdx {
		lastSubtree, err = repo.GetSubtree(block.Subtrees[lastIdx])
		if err != nil {
			return nil, terr.NewProcessingError("failed to get final subtree data", err)
		}
	}

	if lastSubtree.Length() < targetLength {
		roots.LastRoot, err = lastSubtree.RootHashPadded(targetHeight)
		if err != nil {
			return nil, terr.NewProcessingError("failed to pad final subtree", err)
		}
	}

	return roots, nil
}

// deriveFirstSubtreeRoot loads the block's first subtree and returns only the values derived from
// it. Keeping the load inside its own frame means the first subtree becomes unreachable before the
// final subtree is materialized, so a single proof request never holds three complete subtrees at
// once. targetLength is zero when the block has no final subtree to lift.
func deriveFirstSubtreeRoot(repo MerkleProofConstructor, block *model.Block, subtreeData *subtree.Subtree,
	subtreeIdx, lastIdx int, coinbaseHash *chainhash.Hash) (firstRoot *chainhash.Hash, targetLength, targetHeight int, err error) {
	firstSubtree := subtreeData
	if subtreeIdx != 0 {
		firstSubtree, err = repo.GetSubtree(block.Subtrees[0])
		if err != nil {
			return nil, 0, 0, terr.NewProcessingError("failed to get first subtree data", err)
		}
	}

	if coinbaseHash != nil && hasCoinbasePlaceholder(firstSubtree) {
		firstRoot, err = firstSubtree.RootHashWithReplaceRootNode(coinbaseHash, 0, uint64(block.CoinbaseTx.Size())) //nolint:gosec
		if err != nil {
			return nil, 0, 0, terr.NewProcessingError("failed to replace coinbase placeholder in first subtree", err)
		}
	}

	if lastIdx > 0 && firstSubtree != nil {
		targetLength = firstSubtree.Length()
		targetHeight = firstSubtree.Height
	}

	return firstRoot, targetLength, targetHeight, nil
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

// GenerateBlockMerkleProof generates the merkle proof from a subtree to the block's merkle root.
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
func GenerateBlockMerkleProof(subtrees []*chainhash.Hash, subtreeIndex int) ([]*chainhash.Hash, []int, error) {
	if len(subtrees) == 0 {
		return nil, nil, terr.NewProcessingError("no subtrees in block")
	}

	if subtreeIndex < 0 || subtreeIndex >= len(subtrees) {
		return nil, nil, terr.NewProcessingError(errMsgInvalidSubtreeIndex)
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
