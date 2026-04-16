package sql

import (
	"context"
	"database/sql"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// getOnMainChain reads the on_main_chain flag directly from the database for the block
// with the given hash. Returns false if the block does not exist.
func getOnMainChain(t *testing.T, s *SQL, hashBytes []byte) bool {
	t.Helper()
	var v bool
	err := s.db.QueryRow(`SELECT on_main_chain FROM blocks WHERE hash = $1`, hashBytes).Scan(&v)
	if err == sql.ErrNoRows {
		return false
	}
	require.NoError(t, err)
	return v
}

// TestOnMainChain_Genesis verifies that the genesis block is always marked on_main_chain.
func TestOnMainChain_Genesis(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	genesisHash := tSettings.ChainCfgParams.GenesisHash
	assert.True(t, getOnMainChain(t, s, genesisHash[:]), "genesis must be on_main_chain")
}

// TestOnMainChain_NormalExtend verifies that a block extending the main chain gets
// on_main_chain = true in its INSERT (no separate UPDATE needed).
func TestOnMainChain_NormalExtend(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	_, _, err = s.StoreBlock(context.Background(), block1, "peer")
	require.NoError(t, err)

	_, _, err = s.StoreBlock(context.Background(), block2, "peer")
	require.NoError(t, err)

	_, _, err = s.StoreBlock(context.Background(), block3, "peer")
	require.NoError(t, err)

	assert.True(t, getOnMainChain(t, s, block1.Hash().CloneBytes()), "block1 must be on_main_chain")
	assert.True(t, getOnMainChain(t, s, block2.Hash().CloneBytes()), "block2 must be on_main_chain")
	assert.True(t, getOnMainChain(t, s, block3.Hash().CloneBytes()), "block3 must be on_main_chain")
}

// TestOnMainChain_ForkBlock verifies that a fork block (non-best) is NOT on_main_chain.
func TestOnMainChain_ForkBlock(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	_, _, err = s.StoreBlock(context.Background(), block1, "peer")
	require.NoError(t, err)

	_, _, err = s.StoreBlock(context.Background(), block2, "peer")
	require.NoError(t, err)

	// blockAlternative2 has same parent as block2 but less chain_work (older timestamp) —
	// it is a fork that doesn't become the best block.
	_, _, err = s.StoreBlock(context.Background(), blockAlternative2, "peer")
	require.NoError(t, err)

	assert.True(t, getOnMainChain(t, s, block2.Hash().CloneBytes()), "block2 (best) must be on_main_chain")
	assert.False(t, getOnMainChain(t, s, blockAlternative2.Hash().CloneBytes()), "fork block must NOT be on_main_chain")
}

// TestOnMainChain_InvalidBlock verifies that blocks stored with WithInvalid are NOT on_main_chain.
func TestOnMainChain_InvalidBlock(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	_, _, err = s.StoreBlock(context.Background(), block1, "peer", options.WithInvalid(true))
	require.NoError(t, err)

	assert.False(t, getOnMainChain(t, s, block1.Hash().CloneBytes()), "invalid block must NOT be on_main_chain")
}

// TestOnMainChain_InvalidateBlock verifies that InvalidateBlock clears on_main_chain for the
// invalidated block and that the previous block remains on the main chain.
func TestOnMainChain_InvalidateBlock(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	_, _, err = s.StoreBlock(context.Background(), block1, "peer")
	require.NoError(t, err)
	_, _, err = s.StoreBlock(context.Background(), block2, "peer")
	require.NoError(t, err)
	_, _, err = s.StoreBlock(context.Background(), block3, "peer")
	require.NoError(t, err)

	_, err = s.InvalidateBlock(context.Background(), block3.Hash())
	require.NoError(t, err)

	assert.True(t, getOnMainChain(t, s, block1.Hash().CloneBytes()), "block1 still on main chain after block3 invalidated")
	assert.True(t, getOnMainChain(t, s, block2.Hash().CloneBytes()), "block2 still on main chain after block3 invalidated")
	assert.False(t, getOnMainChain(t, s, block3.Hash().CloneBytes()), "invalidated block3 must NOT be on_main_chain")
}

// TestOnMainChain_RevalidateBlock verifies that RevalidateBlock restores on_main_chain for a
// block if it becomes the best chain after revalidation.
func TestOnMainChain_RevalidateBlock(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	_, _, err = s.StoreBlock(context.Background(), block1, "peer")
	require.NoError(t, err)
	_, _, err = s.StoreBlock(context.Background(), block2, "peer")
	require.NoError(t, err)
	_, _, err = s.StoreBlock(context.Background(), block3, "peer")
	require.NoError(t, err)

	// Invalidate then revalidate block3
	_, err = s.InvalidateBlock(context.Background(), block3.Hash())
	require.NoError(t, err)
	assert.False(t, getOnMainChain(t, s, block3.Hash().CloneBytes()), "block3 off-chain after invalidation")

	err = s.RevalidateBlock(context.Background(), block3.Hash())
	require.NoError(t, err)
	assert.True(t, getOnMainChain(t, s, block3.Hash().CloneBytes()), "block3 back on main chain after revalidation")
}

// TestOnMainChain_StartupRebuild verifies that rebuildOnMainChainFlag correctly restores
// on_main_chain flags from scratch. This simulates crash recovery where flags were left
// in a partial state (all cleared to false).
func TestOnMainChain_StartupRebuild(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	_, _, err = s.StoreBlock(context.Background(), block1, "peer")
	require.NoError(t, err)
	_, _, err = s.StoreBlock(context.Background(), block2, "peer")
	require.NoError(t, err)

	// Simulate a crash mid-rebuild: zero out all flags
	_, err = s.db.Exec(`UPDATE blocks SET on_main_chain = false`)
	require.NoError(t, err)

	assert.False(t, getOnMainChain(t, s, tSettings.ChainCfgParams.GenesisHash[:]), "pre-condition: flags are cleared")
	assert.False(t, getOnMainChain(t, s, block1.Hash().CloneBytes()), "pre-condition: flags are cleared")
	assert.False(t, getOnMainChain(t, s, block2.Hash().CloneBytes()), "pre-condition: flags are cleared")

	// Startup rebuild should restore correct flags
	s.responseCache.DeleteAll()
	err = s.rebuildOnMainChainFlag(context.Background())
	require.NoError(t, err)

	assert.True(t, getOnMainChain(t, s, tSettings.ChainCfgParams.GenesisHash[:]), "genesis on_main_chain after rebuild")
	assert.True(t, getOnMainChain(t, s, block1.Hash().CloneBytes()), "block1 on_main_chain after rebuild")
	assert.True(t, getOnMainChain(t, s, block2.Hash().CloneBytes()), "block2 on_main_chain after rebuild")
}

// TestOnMainChain_ReorgClearsOldChain verifies that when a fork grows longer and becomes
// the new main chain (reorg), all blocks on the old chain have on_main_chain = false
// and all blocks on the new chain have on_main_chain = true.
func TestOnMainChain_ReorgClearsOldChain(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Build main chain: genesis → block1 → block2 → block3
	_, _, err = s.StoreBlock(context.Background(), block1, "peer")
	require.NoError(t, err)
	_, _, err = s.StoreBlock(context.Background(), block2, "peer")
	require.NoError(t, err)
	_, _, err = s.StoreBlock(context.Background(), block3, "peer")
	require.NoError(t, err)

	assert.True(t, getOnMainChain(t, s, block1.Hash().CloneBytes()), "block1 initially on main chain")
	assert.True(t, getOnMainChain(t, s, block2.Hash().CloneBytes()), "block2 initially on main chain")
	assert.True(t, getOnMainChain(t, s, block3.Hash().CloneBytes()), "block3 initially on main chain")

	// Build a competing fork: genesis → block1 → altBlock2 → forkBlock3 → forkBlock4
	// The fork must have more chain_work than the main chain to trigger a reorg.
	_, _, err = s.StoreBlock(context.Background(), blockAlternative2, "peer")
	require.NoError(t, err)

	forkBlock3 := createBlock3OnFork(blockAlternative2)
	_, _, err = s.StoreBlock(context.Background(), forkBlock3, "peer")
	require.NoError(t, err)

	forkBlock4 := createBlock3OnFork(forkBlock3)
	_, _, err = s.StoreBlock(context.Background(), forkBlock4, "peer")
	require.NoError(t, err)

	// forkBlock4 should now be the best block (more chain_work due to one extra block).
	// The old chain (block2, block3) must be off-chain; the new fork must be on-chain.
	assert.True(t, getOnMainChain(t, s, block1.Hash().CloneBytes()), "block1 (common ancestor) still on main chain")
	assert.False(t, getOnMainChain(t, s, block2.Hash().CloneBytes()), "block2 off-chain after reorg")
	assert.False(t, getOnMainChain(t, s, block3.Hash().CloneBytes()), "block3 off-chain after reorg")
	assert.True(t, getOnMainChain(t, s, blockAlternative2.Hash().CloneBytes()), "altBlock2 on new main chain")
	assert.True(t, getOnMainChain(t, s, forkBlock3.Hash().CloneBytes()), "forkBlock3 on new main chain")
	assert.True(t, getOnMainChain(t, s, forkBlock4.Hash().CloneBytes()), "forkBlock4 (new tip) on main chain")
}

// TestOnMainChain_LongFork verifies on_main_chain correctness across a multi-block reorg
// where the fork is 3 blocks deep before surpassing the main chain.
func TestOnMainChain_LongFork(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Main chain: genesis → block1 → block2
	_, _, err = s.StoreBlock(context.Background(), block1, "peer")
	require.NoError(t, err)
	_, _, err = s.StoreBlock(context.Background(), block2, "peer")
	require.NoError(t, err)

	// Fork from block1: genesis → block1 → altBlock2 → forkB3 → forkB4 → forkB5
	// By the time forkB5 is added the fork has more work and causes a reorg.
	_, _, err = s.StoreBlock(context.Background(), blockAlternative2, "peer")
	require.NoError(t, err)
	forkB3 := createBlock3OnFork(blockAlternative2)
	_, _, err = s.StoreBlock(context.Background(), forkB3, "peer")
	require.NoError(t, err)
	forkB4 := createBlock3OnFork(forkB3)
	_, _, err = s.StoreBlock(context.Background(), forkB4, "peer")
	require.NoError(t, err)
	forkB5 := createBlock3OnFork(forkB4)
	_, _, err = s.StoreBlock(context.Background(), forkB5, "peer")
	require.NoError(t, err)

	// After the reorg: block2 should be off-chain; the entire 4-block fork should be on-chain.
	assert.True(t, getOnMainChain(t, s, block1.Hash().CloneBytes()), "block1 (common ancestor) still on main chain")
	assert.False(t, getOnMainChain(t, s, block2.Hash().CloneBytes()), "block2 cleared after long fork reorg")
	assert.True(t, getOnMainChain(t, s, blockAlternative2.Hash().CloneBytes()), "altBlock2 on new chain")
	assert.True(t, getOnMainChain(t, s, forkB3.Hash().CloneBytes()), "forkB3 on new chain")
	assert.True(t, getOnMainChain(t, s, forkB4.Hash().CloneBytes()), "forkB4 on new chain")
	assert.True(t, getOnMainChain(t, s, forkB5.Hash().CloneBytes()), "forkB5 (tip) on new chain")
}

// TestOnMainChain_InvalidBlockFork verifies that blocks on a fork that gets invalidated
// have on_main_chain = false and the original main chain is unaffected.
func TestOnMainChain_InvalidBlockFork(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Main chain: genesis → block1 → block2 → block3
	_, _, err = s.StoreBlock(context.Background(), block1, "peer")
	require.NoError(t, err)
	_, _, err = s.StoreBlock(context.Background(), block2, "peer")
	require.NoError(t, err)
	_, _, err = s.StoreBlock(context.Background(), block3, "peer")
	require.NoError(t, err)

	// Invalidate block2 — this should cascade to block3 as well
	_, err = s.InvalidateBlock(context.Background(), block2.Hash())
	require.NoError(t, err)

	// After invalidating block2: block1 is the new best, block2 and block3 are off-chain
	assert.True(t, getOnMainChain(t, s, block1.Hash().CloneBytes()), "block1 is new tip after invalidation")
	assert.False(t, getOnMainChain(t, s, block2.Hash().CloneBytes()), "block2 invalidated, off-chain")
	assert.False(t, getOnMainChain(t, s, block3.Hash().CloneBytes()), "block3 invalidated (child of invalid block2), off-chain")
}

// TestOnMainChain_ConsistentWithGetBlockByHeight verifies that the fast-path query
// (on_main_chain = true) returns the same block as the CTE fallback.
func TestOnMainChain_ConsistentWithGetBlockByHeight(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	_, _, err = s.StoreBlock(context.Background(), block1, "peer")
	require.NoError(t, err)
	_, _, err = s.StoreBlock(context.Background(), block2, "peer")
	require.NoError(t, err)
	_, _, err = s.StoreBlock(context.Background(), block3, "peer")
	require.NoError(t, err)

	for _, height := range []uint32{1, 2, 3} {
		// Fast path (mainChainRebuilding = false by default)
		fastBlock, err := s.GetBlockByHeight(context.Background(), height)
		require.NoError(t, err, "height=%d fast path", height)

		// CTE fallback
		s.mainChainRebuilding.Store(true)
		cteBlock, err := s.GetBlockByHeight(context.Background(), height)
		s.mainChainRebuilding.Store(false)
		require.NoError(t, err, "height=%d CTE path", height)

		assert.Equal(t, fastBlock.Hash().String(), cteBlock.Hash().String(),
			"fast path and CTE must return the same block at height %d", height)
	}
}
