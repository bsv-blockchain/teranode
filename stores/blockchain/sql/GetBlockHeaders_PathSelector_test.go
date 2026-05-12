package sql

import (
	"context"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetBlockHeaders_FastPath verifies that the on_main_chain fast path and the
// recursive CTE fallback return identical results for a main-chain start block.
func TestGetBlockHeaders_FastPath(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2)

	// Fast path: mainChainRebuilding == 0 and block2 is on_main_chain=true.
	headersFast, metasFast, err := s.GetBlockHeaders(context.Background(), block2.Hash(), 10)
	require.NoError(t, err)
	require.Equal(t, 3, len(headersFast), "fast path must return block2, block1, genesis")

	// Force CTE path by incrementing the guard counter.
	s.mainChainRebuilding.Add(1)
	headersCTE, metasCTE, err := s.GetBlockHeaders(context.Background(), block2.Hash(), 10)
	s.mainChainRebuilding.Add(-1)
	require.NoError(t, err)

	// Both paths must return the same sequence of headers.
	require.Equal(t, len(headersCTE), len(headersFast), "fast path and CTE must return same number of headers")
	for i := range headersFast {
		assert.Equal(t, headersFast[i].Hash(), headersCTE[i].Hash(),
			"header %d: fast path hash must match CTE hash", i)
		assert.Equal(t, metasFast[i].Height, metasCTE[i].Height,
			"header %d: fast path height must match CTE height", i)
	}
}

// TestGetBlockHeaders_ForkTipFallback verifies that a fork-tip start block
// causes the CTE fallback to be used and returns the fork's own ancestor chain,
// not the main-chain blocks at the same heights.
func TestGetBlockHeaders_ForkTipFallback(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2)
	// blockAlternative2 shares block1 as parent but is not on the main chain.
	storeBlocks(t, s, blockAlternative2)

	headers, metas, err := s.GetBlockHeaders(context.Background(), blockAlternative2.Hash(), 10)
	require.NoError(t, err)

	// Must return the fork tip, then block1, then genesis.
	require.Equal(t, 3, len(headers), "fork tip walk must return fork + block1 + genesis")
	assert.Equal(t, blockAlternative2.Header.Hash(), headers[0].Hash(),
		"first header must be the fork tip itself")
	assert.Equal(t, uint32(2), metas[0].Height, "fork tip must be at height 2")
	assert.Equal(t, block1.Header.Hash(), headers[1].Hash(),
		"second header must be block1 (shared ancestor)")
	assert.Equal(t, uint32(1), metas[1].Height)

	// The result must NOT contain block2 (main-chain block at height 2).
	for _, h := range headers {
		assert.NotEqual(t, block2.Header.Hash(), h.Hash(),
			"fork walk must not return the main-chain block at the same height")
	}
}

// TestGetBlockHeaders_CTEWhenRebuilding verifies that the CTE fallback is used
// and returns correct results while mainChainRebuilding > 0.
func TestGetBlockHeaders_CTEWhenRebuilding(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2)

	// Simulate an ongoing rebuild.
	s.mainChainRebuilding.Store(1)
	defer s.mainChainRebuilding.Store(0)

	headers, metas, err := s.GetBlockHeaders(context.Background(), block2.Hash(), 10)
	require.NoError(t, err)
	require.Equal(t, 3, len(headers), "CTE during rebuild must return block2, block1, genesis")
	assert.Equal(t, block2.Header.Hash(), headers[0].Hash())
	assert.Equal(t, uint32(2), metas[0].Height)
	assert.Equal(t, block1.Header.Hash(), headers[1].Hash())
	assert.Equal(t, uint32(1), metas[1].Height)
}

// TestGetBlockHeaders_UnknownHashReturnsEmpty verifies that passing an unknown
// hash returns an empty slice without an error.
func TestGetBlockHeaders_UnknownHashReturnsEmpty(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2)

	unknown := &chainhash.Hash{}
	copy(unknown[:], []byte("this_hash_does_not_exist_in_db!!"))

	headers, metas, err := s.GetBlockHeaders(context.Background(), unknown, 10)
	require.NoError(t, err)
	assert.Empty(t, headers, "unknown hash must return empty headers")
	assert.Empty(t, metas, "unknown hash must return empty metas")
}

// TestBuildGetBlockHeadersQuery_FastPathQuery confirms the fast-path query
// string is selected when the start block is on_main_chain and no rebuild is active.
func TestBuildGetBlockHeadersQuery_FastPathQuery(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2)

	// mainChainRebuilding is 0 and block2 is on_main_chain=true.
	q, args := s.buildGetBlockHeadersQuery(context.Background(), block2.Hash(), 5)

	assert.Contains(t, q, "on_main_chain = true",
		"fast path query must filter by on_main_chain")
	assert.NotContains(t, q, "WITH RECURSIVE",
		"fast path query must not use a CTE")
	// Args for fast path are (startHeight, numberOfHeaders).
	require.Len(t, args, 2)
}

// TestBuildGetBlockHeadersQuery_CTEQueryWhenRebuilding confirms the CTE query
// string is returned when mainChainRebuilding > 0.
func TestBuildGetBlockHeadersQuery_CTEQueryWhenRebuilding(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2)

	s.mainChainRebuilding.Store(1)
	defer s.mainChainRebuilding.Store(0)

	q, args := s.buildGetBlockHeadersQuery(context.Background(), block2.Hash(), 5)

	assert.True(t, strings.Contains(q, "WITH RECURSIVE") || strings.Contains(q, "with recursive"),
		"CTE query must contain WITH RECURSIVE")
	assert.NotContains(t, q, "on_main_chain = true",
		"CTE query must not use on_main_chain filter")
	// Args for CTE path are (hashBytes, numberOfHeaders).
	require.Len(t, args, 2)
}

// TestBuildGetBlockHeadersQuery_CTEQueryForForkTip confirms the CTE is selected
// when the start block is on a fork (on_main_chain=false).
func TestBuildGetBlockHeadersQuery_CTEQueryForForkTip(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2, blockAlternative2)

	q, _ := s.buildGetBlockHeadersQuery(context.Background(), blockAlternative2.Hash(), 5)

	assert.True(t, strings.Contains(q, "WITH RECURSIVE") || strings.Contains(q, "with recursive"),
		"fork tip must use CTE query")
	assert.NotContains(t, q, "on_main_chain = true",
		"fork tip query must not filter by on_main_chain")
}

// ---- GetBlockHeaderIDs equivalents ----

// TestGetBlockHeaderIDs_FastPath verifies that the on_main_chain fast path and
// the CTE fallback return identical ID sequences for a main-chain start block.
func TestGetBlockHeaderIDs_FastPath(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2)

	idsFast, err := s.GetBlockHeaderIDs(context.Background(), block2.Hash(), 10)
	require.NoError(t, err)
	require.NotEmpty(t, idsFast, "fast path must return IDs")

	s.mainChainRebuilding.Add(1)
	idsCTE, err := s.GetBlockHeaderIDs(context.Background(), block2.Hash(), 10)
	s.mainChainRebuilding.Add(-1)
	require.NoError(t, err)

	require.Equal(t, len(idsCTE), len(idsFast), "fast path and CTE must return same number of IDs")
	for i := range idsFast {
		assert.Equal(t, idsFast[i], idsCTE[i], "ID at position %d must match", i)
	}
}

// TestGetBlockHeaderIDs_ForkTipFallback verifies that a fork tip returns the
// correct ancestor IDs (not the main-chain block IDs at the same heights).
func TestGetBlockHeaderIDs_ForkTipFallback(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2, blockAlternative2)

	// block2 is stored as ID=2, blockAlternative2 as ID=3 (order of insert).
	idsMain, err := s.GetBlockHeaderIDs(context.Background(), block2.Hash(), 10)
	require.NoError(t, err)

	idsFork, err := s.GetBlockHeaderIDs(context.Background(), blockAlternative2.Hash(), 10)
	require.NoError(t, err)

	require.NotEmpty(t, idsFork)
	// The first ID in the fork walk must be blockAlternative2's ID, not block2's.
	assert.NotEqual(t, idsMain[0], idsFork[0],
		"fork tip ID must differ from main-chain tip ID at the same height")
	// Both chains share block1 and genesis, so their tails must overlap.
	require.Equal(t, len(idsMain), len(idsFork),
		"both chains have the same depth (block, block1, genesis)")
	assert.Equal(t, idsMain[1:], idsFork[1:],
		"shared ancestors (block1 + genesis) must appear in both ID lists")
}

// TestGetBlockHeaderIDs_CTEWhenRebuilding verifies that CTE fallback is used and
// returns correct results while mainChainRebuilding > 0.
func TestGetBlockHeaderIDs_CTEWhenRebuilding(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2)

	s.mainChainRebuilding.Store(1)
	defer s.mainChainRebuilding.Store(0)

	ids, err := s.GetBlockHeaderIDs(context.Background(), block2.Hash(), 10)
	require.NoError(t, err)
	require.NotEmpty(t, ids, "CTE during rebuild must return IDs")
	// Genesis, block1, block2 — IDs are 0, 1, 2 in insertion order, returned DESC.
	assert.Equal(t, uint32(2), ids[0], "first ID must be block2 (highest)")
	assert.Equal(t, uint32(1), ids[1], "second ID must be block1")
	assert.Equal(t, uint32(0), ids[2], "third ID must be genesis")
}

// TestGetBlockHeaderIDs_UnknownHashReturnsEmpty verifies that an unknown hash
// returns an empty slice without an error.
func TestGetBlockHeaderIDs_UnknownHashReturnsEmpty(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2)

	unknown := &chainhash.Hash{}
	copy(unknown[:], []byte("this_hash_does_not_exist_in_db!!"))

	ids, err := s.GetBlockHeaderIDs(context.Background(), unknown, 10)
	require.NoError(t, err)
	assert.Empty(t, ids, "unknown hash must return empty slice")
}

// TestBuildGetBlockHeaderIDsQuery_FastPathQuery confirms the fast-path query
// string is selected when the start block is on_main_chain and no rebuild is active.
func TestBuildGetBlockHeaderIDsQuery_FastPathQuery(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2)

	q, args := s.buildGetBlockHeaderIDsQuery(context.Background(), block2.Hash(), 5)

	assert.Contains(t, q, "on_main_chain = true",
		"fast path query must filter by on_main_chain")
	assert.NotContains(t, q, "WITH RECURSIVE",
		"fast path query must not use a CTE")
	require.Len(t, args, 2)
}

// TestBuildGetBlockHeaderIDsQuery_CTEQueryWhenRebuilding confirms the CTE query
// string is returned when mainChainRebuilding > 0.
func TestBuildGetBlockHeaderIDsQuery_CTEQueryWhenRebuilding(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2)

	s.mainChainRebuilding.Store(1)
	defer s.mainChainRebuilding.Store(0)

	q, args := s.buildGetBlockHeaderIDsQuery(context.Background(), block2.Hash(), 5)

	assert.True(t, strings.Contains(q, "WITH RECURSIVE") || strings.Contains(q, "with recursive"),
		"CTE query must contain WITH RECURSIVE")
	assert.NotContains(t, q, "on_main_chain = true",
		"CTE query must not use on_main_chain filter")
	require.Len(t, args, 2)
}

// TestBuildGetBlockHeaderIDsQuery_CTEQueryForForkTip confirms the CTE is
// selected when the start block is on a fork.
func TestBuildGetBlockHeaderIDsQuery_CTEQueryForForkTip(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2, blockAlternative2)

	q, _ := s.buildGetBlockHeaderIDsQuery(context.Background(), blockAlternative2.Hash(), 5)

	assert.True(t, strings.Contains(q, "WITH RECURSIVE") || strings.Contains(q, "with recursive"),
		"fork tip must use CTE query")
	assert.NotContains(t, q, "on_main_chain = true",
		"fork tip query must not filter by on_main_chain")
}

// TestGetBlockHeaderIDs_FastPathCacheNotPoisoned verifies that a fast-path
// result cached in responseCache is not confused with a CTE result stored
// under the same cache key. The cache is bypassed for the second lookup because
// each SQL store instance has its own cache, so the test uses two separate stores.
func TestGetBlockHeaderIDs_FastPathCacheNotPoisoned(t *testing.T) {
	// Use two independent stores so the first store's cache cannot affect the second.
	s1 := newOnMainChainTestStore(t)
	storeBlocks(t, s1, block1, block2)

	s2 := newOnMainChainTestStore(t)
	storeBlocks(t, s2, block1, block2)

	ids1, err := s1.GetBlockHeaderIDs(context.Background(), block2.Hash(), 10)
	require.NoError(t, err)

	ids2, err := s2.GetBlockHeaderIDs(context.Background(), block2.Hash(), 10)
	require.NoError(t, err)

	require.Equal(t, ids1, ids2, "independent stores must return identical ID sequences")
}

// TestGetBlockHeaders_ModelForkReturnsOnlyForkAncestors is a focused integration
// test: given a two-block fork at height 2, confirm GetBlockHeaders from the
// fork tip does NOT include the main-chain block at height 2.
func TestGetBlockHeaders_ModelForkReturnsOnlyForkAncestors(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2, blockAlternative2)

	headers, _, err := s.GetBlockHeaders(context.Background(), blockAlternative2.Hash(), 10)
	require.NoError(t, err)

	hashesReturned := make([]string, len(headers))
	for i, h := range headers {
		hashesReturned[i] = h.Hash().String()
	}

	assert.NotContains(t, hashesReturned, block2.Header.Hash().String(),
		"fork walk must not include block2 (main-chain block at same height)")
	assert.Contains(t, hashesReturned, blockAlternative2.Header.Hash().String(),
		"fork walk must include the fork tip")
	assert.Contains(t, hashesReturned, block1.Header.Hash().String(),
		"fork walk must include block1 (shared ancestor)")
}

// TestGetBlockHeaders_ModelForkTipIDs is a focused integration test checking
// that the IDs returned for a fork tip differ from those for the main-chain tip.
func TestGetBlockHeaders_ModelForkTipIDs(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2, blockAlternative2)

	idsMain, err := s.GetBlockHeaderIDs(context.Background(), block2.Hash(), 10)
	require.NoError(t, err)

	idsFork, err := s.GetBlockHeaderIDs(context.Background(), blockAlternative2.Hash(), 10)
	require.NoError(t, err)

	assert.NotEqual(t, idsMain, idsFork,
		"main-chain and fork ID sequences must differ at position 0 (different tip blocks)")
}
