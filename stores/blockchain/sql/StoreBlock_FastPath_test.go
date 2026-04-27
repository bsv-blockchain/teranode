package sql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestStoreBlock_NormalExtend_FastPath verifies that the sequential seeding hot
// path (each new block extends the current best) does not issue a post-insert
// getBestBlockID query per block.
//
// Regression guard for the seeder perf issue: prior to the fast path, every
// StoreBlock call did an unconditional post-insert getBestBlockID to detect
// forks/reorgs. Combined with ResetResponseCache wiping the pre-insert cache
// entry, this caused two Postgres round-trips per block. On a 900k-block
// mainnet seed against a Docker-local Postgres (~1-2ms RTT), this alone added
// 30-60 min of pure network wait.
//
// Expected steady-state: exactly one DB hit per block (the pre-insert call),
// because the post-insert classification is skipped when onMainChain=true and
// the new best block ID/hash is primed into the cache for the next iteration.
func TestStoreBlock_NormalExtend_FastPath(t *testing.T) {
	s := newOnMainChainTestStore(t)

	// Store block1 — this is the first non-genesis block. The pre-insert
	// getBestBlockID may hit the DB if the cache is cold; what matters is that
	// subsequent blocks don't keep hitting the DB.
	storeBlocks(t, s, block1)
	baseline := s.bestBlockIDQueries.Load()

	// Now store 5 more blocks sequentially. Each is a normal extend: new block
	// parent == current best. The hot path must NOT hit the DB for fork
	// detection after each insert.
	forkB2 := createBlock3OnFork(block1)
	forkB3 := createBlock3OnFork(forkB2)
	forkB4 := createBlock3OnFork(forkB3)
	forkB5 := createBlock3OnFork(forkB4)
	forkB6 := createBlock3OnFork(forkB5)
	storeBlocks(t, s, forkB2, forkB3, forkB4, forkB5, forkB6)

	extras := s.bestBlockIDQueries.Load() - baseline

	// Fast-path target: zero DB hits across 5 sequential normal extends.
	// - pre-insert call hits the cache (primed by previous StoreBlock)
	// - post-insert call is skipped entirely
	// Any non-zero value means either the post-insert skip or the cache priming
	// has regressed.
	require.EqualValuesf(t, 0, extras,
		"normal-extend fast path must not hit the DB; got %d extra getBestBlockID DB hits across 5 blocks", extras)

	// Also assert correctness wasn't sacrificed for the perf win.
	require.True(t, getOnMainChain(t, s, block1.Hash().CloneBytes()), "block1 on main chain")
	require.True(t, getOnMainChain(t, s, forkB2.Hash().CloneBytes()), "forkB2 on main chain")
	require.True(t, getOnMainChain(t, s, forkB6.Hash().CloneBytes()), "forkB6 (tip) on main chain")
}

// TestStoreBlock_Fork_StillClassifies verifies that the fast-path skip is gated
// correctly: a non-extending insert (fork) must still run the post-insert
// classification so on_main_chain flags remain consistent.
func TestStoreBlock_Fork_StillClassifies(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2)

	// blockAlternative2 has the same parent as block2 but less chain_work
	// (older timestamp) — it is a fork that must be stored with
	// on_main_chain=false. This requires the full (non-fast) path.
	storeBlocks(t, s, blockAlternative2)

	require.True(t, getOnMainChain(t, s, block2.Hash().CloneBytes()),
		"block2 (best) on main chain")
	require.False(t, getOnMainChain(t, s, blockAlternative2.Hash().CloneBytes()),
		"fork block not on main chain — fast-path must not have been taken")
}

// TestStoreBlock_NormalExtend_PrimesCache verifies the cache-priming half of
// the fast path: after a normal extend, getBestBlockID must not hit the DB on
// the very next call. If priming regresses, the next StoreBlock's pre-insert
// lookup would miss and re-hit Postgres.
func TestStoreBlock_NormalExtend_PrimesCache(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1)

	before := s.bestBlockIDQueries.Load()
	id, hash, err := s.getBestBlockID(context.Background())
	after := s.bestBlockIDQueries.Load()

	require.NoError(t, err)
	require.EqualValues(t, before, after, "StoreBlock must prime the getBestBlockID cache so the next call is a cache hit")
	require.NotZero(t, id, "primed cache must contain a valid ID")
	require.Equal(t, block1.Hash().String(), hash.String(), "primed cache must point at the new best block")
}
