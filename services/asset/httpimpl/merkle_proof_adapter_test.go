package httpimpl

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/merkleproof"
	"github.com/stretchr/testify/require"
)

// TestMerkleProofAdapterImplementsBlockRootsCache pins the wiring: proof
// construction only skips re-deriving a block's first/final subtree roots if the
// adapter is recognised as a merkleproof.BlockRootsCache.
func TestMerkleProofAdapterImplementsBlockRootsCache(t *testing.T) {
	var _ merkleproof.BlockRootsCache = (*merkleProofAdapter)(nil)

	c := newMainChainCache(nil, ulogger.TestLogger{}, 3)
	adapter := newMerkleProofAdapter(context.Background(), nil, c)

	blockHash := chainhash.DoubleHashH([]byte("block"))
	roots := &merkleproof.BlockRoots{TargetHeight: 3}

	_, ok := adapter.BlockRoots(&blockHash)
	require.False(t, ok, "cold block must miss")

	adapter.SetBlockRoots(&blockHash, roots)

	got, ok := adapter.BlockRoots(&blockHash)
	require.True(t, ok)
	require.Same(t, roots, got)

	// A second adapter (i.e. a later request) shares the cache — that sharing is
	// the whole point.
	other := newMerkleProofAdapter(context.Background(), nil, c)
	got, ok = other.BlockRoots(&blockHash)
	require.True(t, ok)
	require.Same(t, roots, got)
}

// TestMerkleProofAdapterBlockRootsNoCache covers the degraded wiring: when the
// main-chain cache failed to start it is nil, and proof construction must still
// work, just without memoization.
func TestMerkleProofAdapterBlockRootsNoCache(t *testing.T) {
	adapter := newMerkleProofAdapter(context.Background(), nil, nil)

	blockHash := chainhash.DoubleHashH([]byte("block"))

	adapter.SetBlockRoots(&blockHash, &merkleproof.BlockRoots{TargetHeight: 3})

	_, ok := adapter.BlockRoots(&blockHash)
	require.False(t, ok, "without a cache every lookup must miss, never panic")

	_, ok = adapter.BlockRoots(nil)
	require.False(t, ok)
}
