package httpimpl

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/asset/repository"
	"github.com/bsv-blockchain/teranode/util/merkleproof"
)

// merkleProofAdapter adapts the repository.Interface to implement merkleproof.MerkleProofConstructor
type merkleProofAdapter struct {
	ctx   context.Context
	repo  repository.Interface
	cache *mainChainCache
}

// newMerkleProofAdapter creates a new adapter that allows the repository to be used with merkleproof functions.
// The optional cache short-circuits the per-request CheckBlockIsInCurrentChain gRPC call.
func newMerkleProofAdapter(ctx context.Context, repo repository.Interface, cache *mainChainCache) *merkleProofAdapter {
	return &merkleProofAdapter{
		ctx:   ctx,
		repo:  repo,
		cache: cache,
	}
}

// GetTxMeta retrieves transaction metadata and converts it to the simplified format
func (a *merkleProofAdapter) GetTxMeta(txHash *chainhash.Hash) (*merkleproof.TxMetaData, error) {
	txMeta, err := a.repo.GetTxMeta(a.ctx, txHash)
	if err != nil {
		return nil, err
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
