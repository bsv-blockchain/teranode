package httpimpl

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/asset/repository"
	"github.com/bsv-blockchain/teranode/util/merkleproof"
	lru "github.com/hashicorp/golang-lru/v2"
)

// blockRootsCacheEntries bounds the per-block merkle-proof root cache. Each
// entry is two hashes and an int, so even at this size the cache costs well
// under a megabyte — trivial next to the complete subtrees it saves loading.
const blockRootsCacheEntries = 4096

// blockRootsCache memoizes the block-level roots a merkle proof derives from a
// block's first and final subtrees. Without it, every proof request for a
// transaction in a middle subtree deserializes three complete subtrees (up to
// a million nodes each) to produce a few hundred bytes of proof; with it, only
// the transaction's own subtree is read after the first request for that block.
//
// Entries are immutable: a block's subtree list is fixed by its header and
// subtrees are content-addressed, so a reorg cannot change what a block hash
// maps to. Nothing invalidates; the LRU bound is the only eviction.
type blockRootsCache struct {
	entries *lru.Cache[chainhash.Hash, *merkleproof.BlockRoots]
}

// newBlockRootsCache builds the bounded cache. lru.New only fails for a
// non-positive size, so with a constant size the cache is always present; a nil
// entries map would simply disable caching.
func newBlockRootsCache() *blockRootsCache {
	entries, _ := lru.New[chainhash.Hash, *merkleproof.BlockRoots](blockRootsCacheEntries)

	return &blockRootsCache{entries: entries}
}

// BlockRoots returns the memoized roots for a block, if present.
func (c *blockRootsCache) BlockRoots(blockHash *chainhash.Hash) (*merkleproof.BlockRoots, bool) {
	if c == nil || c.entries == nil || blockHash == nil {
		return nil, false
	}

	return c.entries.Get(*blockHash)
}

// SetBlockRoots memoizes the roots for a block.
func (c *blockRootsCache) SetBlockRoots(blockHash *chainhash.Hash, roots *merkleproof.BlockRoots) {
	if c == nil || c.entries == nil || blockHash == nil || roots == nil {
		return
	}

	c.entries.Add(*blockHash, roots)
}

// merkleProofAdapter adapts the repository.Interface to implement merkleproof.MerkleProofConstructor
type merkleProofAdapter struct {
	ctx        context.Context
	repo       repository.Interface
	cache      *mainChainCache
	blockRoots *blockRootsCache
}

// newMerkleProofAdapter creates a new adapter that allows the repository to be used with merkleproof functions.
// The optional cache short-circuits the per-request CheckBlockIsInCurrentChain gRPC call.
func newMerkleProofAdapter(ctx context.Context, repo repository.Interface, cache *mainChainCache) *merkleProofAdapter {
	return &merkleProofAdapter{
		ctx:        ctx,
		repo:       repo,
		cache:      cache,
		blockRoots: cache.blockRootsCache(),
	}
}

// BlockRoots implements merkleproof.BlockRootsCache, letting proof construction
// skip re-deriving a block's first/final subtree roots on every request.
func (a *merkleProofAdapter) BlockRoots(blockHash *chainhash.Hash) (*merkleproof.BlockRoots, bool) {
	return a.blockRoots.BlockRoots(blockHash)
}

// SetBlockRoots implements merkleproof.BlockRootsCache.
func (a *merkleProofAdapter) SetBlockRoots(blockHash *chainhash.Hash, roots *merkleproof.BlockRoots) {
	a.blockRoots.SetBlockRoots(blockHash, roots)
}

// GetTxMeta retrieves transaction metadata and converts it to the simplified format
func (a *merkleProofAdapter) GetTxMeta(txHash *chainhash.Hash) (*merkleproof.TxMetaData, error) {
	txMeta, err := a.repo.GetTxMeta(a.ctx, txHash)
	if err != nil {
		return nil, err
	}

	// The store can return no metadata and no error — the aerospike batch path
	// does that for the coinbase placeholder hash.
	if txMeta == nil {
		return nil, errors.NewTxNotFoundError("%s not found", txHash.String())
	}

	// Convert to simplified format
	return &merkleproof.TxMetaData{
		BlockIDs:     txMeta.BlockIDs,
		BlockHeights: txMeta.BlockHeights,
		SubtreeIdxs:  txMeta.SubtreeIdxs,
	}, nil
}

// GetBlockByID retrieves a block by its ID
func (a *merkleProofAdapter) GetBlockByID(id uint64) (*model.Block, error) {
	return a.repo.GetBlockByID(a.ctx, id)
}

// GetBlockHeader retrieves a block header by its hash
func (a *merkleProofAdapter) GetBlockHeader(blockHash *chainhash.Hash) (*model.BlockHeader, error) {
	header, _, err := a.repo.GetBlockHeader(a.ctx, blockHash)
	return header, err
}

// GetSubtree retrieves subtree data by its hash
func (a *merkleProofAdapter) GetSubtree(subtreeHash *chainhash.Hash) (*subtree.Subtree, error) {
	return a.repo.GetSubtree(a.ctx, subtreeHash)
}

// FindBlocksContainingSubtree finds all blocks that contain the specified subtree
func (a *merkleProofAdapter) FindBlocksContainingSubtree(subtreeHash *chainhash.Hash) ([]uint32, []uint32, []int, error) {
	// The repository method now returns both block IDs and heights
	return a.repo.FindBlocksContainingSubtree(a.ctx, subtreeHash)
}

// IsBlockOnMainChain reports whether the given internal block ID at the given
// height is part of the current best chain.
// Uses the in-process cache when available to avoid a gRPC round-trip per request;
// falls back to a direct CheckBlockIsInCurrentChain call when no cache is wired in
// (e.g. unit tests that construct the adapter directly).
func (a *merkleProofAdapter) IsBlockOnMainChain(blockID, blockHeight uint32) (bool, error) {
	if a.cache != nil {
		return a.cache.IsOnMainChain(a.ctx, blockID, blockHeight)
	}
	return a.repo.GetBlockchainClient().CheckBlockIsInCurrentChain(a.ctx, []uint32{blockID})
}
