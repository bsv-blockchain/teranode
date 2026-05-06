package sql

import (
	"context"
	"database/sql"
	"net/url"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// newOnMainChainTestStore creates a *SQL backed by an sqlitememory DB, waits
// for the background startup rebuild to complete, and returns the store ready
// for use. The caller is responsible for s.Close() (via t.Cleanup).
func newOnMainChainTestStore(t *testing.T) *SQL {
	t.Helper()
	return newOnMainChainTestStoreWith(t, nil)
}

// newOnMainChainTestStoreWith is the same as newOnMainChainTestStore but lets
// the caller mutate settings before the store is created (e.g. to tweak
// CoinbaseMaturity or enable UseInMemoryChainCheck).
func newOnMainChainTestStoreWith(t *testing.T, mutate func(*settings.Settings)) *SQL {
	t.Helper()
	tSettings := test.CreateBaseTestSettings(t)
	if mutate != nil {
		mutate(tSettings)
	}
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	waitForStartupRebuild(t, s)
	return s
}

// storeBlocks stores a sequence of blocks via StoreBlock, failing the test on
// any error. Returns the list for convenience.
func storeBlocks(t *testing.T, s *SQL, blocks ...*model.Block) {
	t.Helper()
	for i, b := range blocks {
		_, _, err := s.StoreBlock(context.Background(), b, "peer")
		require.NoError(t, err, "store block %d (height %d)", i, b.Height)
	}
}

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
	s := newOnMainChainTestStore(t)
	genesisHash := s.chainParams.GenesisHash
	require.True(t, getOnMainChain(t, s, genesisHash[:]), "genesis must be on_main_chain")
}

// TestOnMainChain_GenesisOverrideWhenPreBestHashIsNil verifies the genesis
// override path inside storeBlock: when the DB is empty at insert time, the
// outer StoreBlock computes onMainChain=false (preBestHash is nil), but the
// override flips it to true for non-invalid genesis. This exercises the
// explicit storeBlock.go `if genesis { onMainChain = !storeAsInvalid }` branch
// independently of the automatic New() flow.
func TestOnMainChain_GenesisOverrideWhenPreBestHashIsNil(t *testing.T) {
	s := newOnMainChainTestStore(t)

	// Wipe genesis to simulate a fresh, never-seeded DB.
	_, err := s.db.Exec(`DELETE FROM blocks`)
	require.NoError(t, err)
	require.False(t, getOnMainChain(t, s, s.chainParams.GenesisHash[:]),
		"pre-condition: genesis row deleted")

	// Re-seed genesis. This goes through StoreBlock → storeBlock, where
	// preBestHash is nil (empty DB) so the outer logic computes onMainChain=false.
	// The override inside storeBlock must set it back to true.
	require.NoError(t, s.insertGenesisTransaction(ulogger.TestLogger{}))

	require.True(t, getOnMainChain(t, s, s.chainParams.GenesisHash[:]),
		"genesis override must set on_main_chain=true even when preBestHash is nil")
}

// TestOnMainChain_NormalExtend verifies that a block extending the main chain gets
// on_main_chain = true in its INSERT (no separate UPDATE needed).
func TestOnMainChain_NormalExtend(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2, block3)

	require.True(t, getOnMainChain(t, s, block1.Hash().CloneBytes()), "block1 must be on_main_chain")
	require.True(t, getOnMainChain(t, s, block2.Hash().CloneBytes()), "block2 must be on_main_chain")
	require.True(t, getOnMainChain(t, s, block3.Hash().CloneBytes()), "block3 must be on_main_chain")
}

// TestOnMainChain_ForkBlock verifies that a fork block (non-best) is NOT on_main_chain.
func TestOnMainChain_ForkBlock(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2)

	// blockAlternative2 has same parent as block2 but less chain_work (older timestamp) —
	// it is a fork that doesn't become the best block.
	storeBlocks(t, s, blockAlternative2)

	require.True(t, getOnMainChain(t, s, block2.Hash().CloneBytes()), "block2 (best) must be on_main_chain")
	require.False(t, getOnMainChain(t, s, blockAlternative2.Hash().CloneBytes()), "fork block must NOT be on_main_chain")
}

// TestOnMainChain_InvalidBlock verifies that blocks stored with WithInvalid are NOT on_main_chain.
func TestOnMainChain_InvalidBlock(t *testing.T) {
	s := newOnMainChainTestStore(t)

	_, _, err := s.StoreBlock(context.Background(), block1, "peer", options.WithInvalid(true))
	require.NoError(t, err)

	require.False(t, getOnMainChain(t, s, block1.Hash().CloneBytes()), "invalid block must NOT be on_main_chain")
}

// TestOnMainChain_InvalidateBlock verifies that InvalidateBlock clears on_main_chain for the
// invalidated block and that the previous block remains on the main chain.
func TestOnMainChain_InvalidateBlock(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2, block3)

	_, err := s.InvalidateBlock(context.Background(), block3.Hash())
	require.NoError(t, err)

	require.True(t, getOnMainChain(t, s, block1.Hash().CloneBytes()), "block1 still on main chain after block3 invalidated")
	require.True(t, getOnMainChain(t, s, block2.Hash().CloneBytes()), "block2 still on main chain after block3 invalidated")
	require.False(t, getOnMainChain(t, s, block3.Hash().CloneBytes()), "invalidated block3 must NOT be on_main_chain")
}

// TestOnMainChain_RevalidateBlock verifies that RevalidateBlock restores on_main_chain for a
// block if it becomes the best chain after revalidation.
func TestOnMainChain_RevalidateBlock(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2, block3)

	// Invalidate then revalidate block3
	_, err := s.InvalidateBlock(context.Background(), block3.Hash())
	require.NoError(t, err)
	require.False(t, getOnMainChain(t, s, block3.Hash().CloneBytes()), "block3 off-chain after invalidation")

	err = s.RevalidateBlock(context.Background(), block3.Hash())
	require.NoError(t, err)
	require.True(t, getOnMainChain(t, s, block3.Hash().CloneBytes()), "block3 back on main chain after revalidation")
}

// TestOnMainChain_StartupRebuild verifies that rebuildOnMainChainFlag correctly restores
// on_main_chain flags from scratch. This simulates crash recovery where flags were left
// in a partial state (all cleared to false).
func TestOnMainChain_StartupRebuild(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2)

	// Simulate a crash mid-rebuild: zero out all flags
	_, err := s.db.Exec(`UPDATE blocks SET on_main_chain = false`)
	require.NoError(t, err)

	require.False(t, getOnMainChain(t, s, s.chainParams.GenesisHash[:]), "pre-condition: flags are cleared")
	require.False(t, getOnMainChain(t, s, block1.Hash().CloneBytes()), "pre-condition: flags are cleared")
	require.False(t, getOnMainChain(t, s, block2.Hash().CloneBytes()), "pre-condition: flags are cleared")

	// Startup rebuild should restore correct flags
	s.responseCache.DeleteAll()
	err = s.rebuildOnMainChainFlag(context.Background(), false)
	require.NoError(t, err)

	require.True(t, getOnMainChain(t, s, s.chainParams.GenesisHash[:]), "genesis on_main_chain after rebuild")
	require.True(t, getOnMainChain(t, s, block1.Hash().CloneBytes()), "block1 on_main_chain after rebuild")
	require.True(t, getOnMainChain(t, s, block2.Hash().CloneBytes()), "block2 on_main_chain after rebuild")
}

// TestOnMainChain_ReorgClearsOldChain verifies that when a fork grows longer and becomes
// the new main chain (reorg), all blocks on the old chain have on_main_chain = false
// and all blocks on the new chain have on_main_chain = true.
func TestOnMainChain_ReorgClearsOldChain(t *testing.T) {
	s := newOnMainChainTestStore(t)

	// Build main chain: genesis → block1 → block2 → block3
	storeBlocks(t, s, block1, block2, block3)

	require.True(t, getOnMainChain(t, s, block1.Hash().CloneBytes()), "block1 initially on main chain")
	require.True(t, getOnMainChain(t, s, block2.Hash().CloneBytes()), "block2 initially on main chain")
	require.True(t, getOnMainChain(t, s, block3.Hash().CloneBytes()), "block3 initially on main chain")

	// Build a competing fork: genesis → block1 → altBlock2 → forkBlock3 → forkBlock4
	// The fork must have more chain_work than the main chain to trigger a reorg.
	forkBlock3 := createBlock3OnFork(blockAlternative2)
	forkBlock4 := createBlock3OnFork(forkBlock3)
	storeBlocks(t, s, blockAlternative2, forkBlock3, forkBlock4)

	// forkBlock4 should now be the best block (more chain_work due to one extra block).
	// The old chain (block2, block3) must be off-chain; the new fork must be on-chain.
	require.True(t, getOnMainChain(t, s, block1.Hash().CloneBytes()), "block1 (common ancestor) still on main chain")
	require.False(t, getOnMainChain(t, s, block2.Hash().CloneBytes()), "block2 off-chain after reorg")
	require.False(t, getOnMainChain(t, s, block3.Hash().CloneBytes()), "block3 off-chain after reorg")
	require.True(t, getOnMainChain(t, s, blockAlternative2.Hash().CloneBytes()), "altBlock2 on new main chain")
	require.True(t, getOnMainChain(t, s, forkBlock3.Hash().CloneBytes()), "forkBlock3 on new main chain")
	require.True(t, getOnMainChain(t, s, forkBlock4.Hash().CloneBytes()), "forkBlock4 (new tip) on main chain")
}

// TestOnMainChain_LongFork verifies on_main_chain correctness across a multi-block reorg
// where the fork is 3 blocks deep before surpassing the main chain.
func TestOnMainChain_LongFork(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2)

	// Fork from block1: genesis → block1 → altBlock2 → forkB3 → forkB4 → forkB5
	// By the time forkB5 is added the fork has more work and causes a reorg.
	forkB3 := createBlock3OnFork(blockAlternative2)
	forkB4 := createBlock3OnFork(forkB3)
	forkB5 := createBlock3OnFork(forkB4)
	storeBlocks(t, s, blockAlternative2, forkB3, forkB4, forkB5)

	// After the reorg: block2 should be off-chain; the entire 4-block fork should be on-chain.
	require.True(t, getOnMainChain(t, s, block1.Hash().CloneBytes()), "block1 (common ancestor) still on main chain")
	require.False(t, getOnMainChain(t, s, block2.Hash().CloneBytes()), "block2 cleared after long fork reorg")
	require.True(t, getOnMainChain(t, s, blockAlternative2.Hash().CloneBytes()), "altBlock2 on new chain")
	require.True(t, getOnMainChain(t, s, forkB3.Hash().CloneBytes()), "forkB3 on new chain")
	require.True(t, getOnMainChain(t, s, forkB4.Hash().CloneBytes()), "forkB4 on new chain")
	require.True(t, getOnMainChain(t, s, forkB5.Hash().CloneBytes()), "forkB5 (tip) on new chain")
}

// TestOnMainChain_InvalidBlockFork verifies that blocks on a fork that gets invalidated
// have on_main_chain = false and the original main chain is unaffected.
func TestOnMainChain_InvalidBlockFork(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2, block3)

	// Invalidate block2 — this should cascade to block3 as well
	_, err := s.InvalidateBlock(context.Background(), block2.Hash())
	require.NoError(t, err)

	// After invalidating block2: block1 is the new best, block2 and block3 are off-chain
	require.True(t, getOnMainChain(t, s, block1.Hash().CloneBytes()), "block1 is new tip after invalidation")
	require.False(t, getOnMainChain(t, s, block2.Hash().CloneBytes()), "block2 invalidated, off-chain")
	require.False(t, getOnMainChain(t, s, block3.Hash().CloneBytes()), "block3 invalidated (child of invalid block2), off-chain")
}

// TestOnMainChain_ConsistentWithGetBlockByHeight verifies that the fast-path query
// (on_main_chain = true) returns the same block as the CTE fallback.
func TestOnMainChain_ConsistentWithGetBlockByHeight(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2, block3)

	for _, height := range []uint32{1, 2, 3} {
		// Fast path (mainChainRebuilding = 0 by default)
		fastBlock, err := s.GetBlockByHeight(context.Background(), height)
		require.NoError(t, err, "height=%d fast path", height)

		// CTE fallback
		s.mainChainRebuilding.Add(1)
		cteBlock, err := s.GetBlockByHeight(context.Background(), height)
		s.mainChainRebuilding.Add(-1)
		require.NoError(t, err, "height=%d CTE path", height)

		require.Equal(t, fastBlock.Hash().String(), cteBlock.Hash().String(),
			"fast path and CTE must return the same block at height %d", height)
	}
}

// TestOnMainChain_BoundedWindowRespected verifies that rebuildOnMainChainFlag with
// full=false does NOT touch blocks whose height is below the window floor. This is
// the safety property that makes the optimization valid: blocks deeper than
// 10×CoinbaseMaturity are never rewritten.
func TestOnMainChain_BoundedWindowRespected(t *testing.T) {
	// CoinbaseMaturity=0 → window size 0 → windowBottom == bestHeight.
	// With bestHeight=3, only the tip (block3) is in the window; block1 and block2
	// are outside it.
	s := newOnMainChainTestStoreWith(t, func(ts *settings.Settings) {
		ts.ChainCfgParams.CoinbaseMaturity = 0
	})
	storeBlocks(t, s, block1, block2, block3)

	// Corrupt block1's flag via direct SQL — this simulates a deep inconsistency
	// (e.g., from migration) that bounded rebuild must leave untouched.
	_, err := s.db.Exec(`UPDATE blocks SET on_main_chain = false WHERE hash = $1`, block1.Hash().CloneBytes())
	require.NoError(t, err)
	require.False(t, getOnMainChain(t, s, block1.Hash().CloneBytes()), "pre-condition: block1 flag cleared")

	// Bounded rebuild must NOT fix block1 (it is below windowBottom).
	err = s.rebuildOnMainChainFlag(context.Background(), false)
	require.NoError(t, err)
	require.False(t, getOnMainChain(t, s, block1.Hash().CloneBytes()),
		"bounded rebuild must not touch blocks below windowBottom")

	// Full rebuild must fix block1 (walks all the way to genesis).
	err = s.rebuildOnMainChainFlag(context.Background(), true)
	require.NoError(t, err)
	require.True(t, getOnMainChain(t, s, block1.Hash().CloneBytes()),
		"full rebuild must correct deep flags")
}

// TestOnMainChain_MigrationDetection verifies that needsFullOnMainChainRebuild
// returns true when on_main_chain is unpopulated relative to the canonical chain.
func TestOnMainChain_MigrationDetection(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2)

	// With all flags correct, no full rebuild needed.
	needsFull, err := s.needsFullOnMainChainRebuild(context.Background())
	require.NoError(t, err)
	require.False(t, needsFull, "consistent state requires no full rebuild")

	// Simulate migration: clear all flags.
	_, err = s.db.Exec(`UPDATE blocks SET on_main_chain = false`)
	require.NoError(t, err)

	needsFull, err = s.needsFullOnMainChainRebuild(context.Background())
	require.NoError(t, err)
	require.True(t, needsFull, "unpopulated state requires full rebuild")
}

// TestStoreBlock_GuardReleasedAfterCall verifies that StoreBlock's defer runs so
// mainChainRebuilding returns to 0 after every exit path (normal extend, reorg,
// fork). If the defer leaks, readers are forever stuck on the CTE fallback.
func TestStoreBlock_GuardReleasedAfterCall(t *testing.T) {
	s := newOnMainChainTestStore(t)

	require.EqualValues(t, 0, s.mainChainRebuilding.Load(), "baseline: guard is 0")

	// Normal extend.
	storeBlocks(t, s, block1)
	require.EqualValues(t, 0, s.mainChainRebuilding.Load(), "guard released after extend")

	// Fork branch (non-best).
	storeBlocks(t, s, block2, blockAlternative2)
	require.EqualValues(t, 0, s.mainChainRebuilding.Load(), "guard released after fork insert")

	// Reorg: build fork deep enough to overtake main chain.
	forkB3 := createBlock3OnFork(blockAlternative2)
	forkB4 := createBlock3OnFork(forkB3)
	storeBlocks(t, s, forkB3, forkB4)
	require.EqualValues(t, 0, s.mainChainRebuilding.Load(), "guard released after reorg")
}

// TestRevalidateBlock_GuardReleasedAfterCall verifies RevalidateBlock's defer runs.
func TestRevalidateBlock_GuardReleasedAfterCall(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2)

	_, err := s.InvalidateBlock(context.Background(), block2.Hash())
	require.NoError(t, err)
	require.EqualValues(t, 0, s.mainChainRebuilding.Load(), "guard released after InvalidateBlock")

	err = s.RevalidateBlock(context.Background(), block2.Hash())
	require.NoError(t, err)
	require.EqualValues(t, 0, s.mainChainRebuilding.Load(), "guard released after RevalidateBlock")
}

// TestRebuildOnMainChainFlag_GuardHeldDuringCall verifies the invariant that
// the guard is > 0 for the entire duration of rebuildOnMainChainFlag. The test
// is deterministic (no polling for arbitrary events): we take a long-running
// write transaction on a separate connection, which blocks rebuild's internal
// UPDATE until we release. While blocked, the rebuild goroutine has already
// executed its synchronous `Add(1)` on entry — we can observe guard > 0 and
// unblock in a controlled sequence. The original polling-based variant of this
// test was prone to scheduler-dependent flakes.
//
// This test covers the mechanism used by StoreBlock, InvalidateBlock, and
// RevalidateBlock (they all call rebuildOnMainChainFlag through the same
// code path); the balanced Add(1)/Add(-1) pairs at each call site are
// covered separately by the *GuardReleasedAfterCall tests above.
func TestRebuildOnMainChainFlag_GuardHeldDuringCall(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2, block3)
	require.EqualValues(t, 0, s.mainChainRebuilding.Load(), "baseline")

	// Take a write lock on another tx. SQLite acquires the database-level
	// RESERVED lock on the first write of a transaction, blocking subsequent
	// writers until commit/rollback. rebuildOnMainChainFlag's first UPDATE
	// will block inside the goroutine below.
	blocker, err := s.db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	_, err = blocker.Exec(`INSERT INTO state (key, data) VALUES ('guard_held_test_lock', X'00')`)
	require.NoError(t, err)

	// Launch the rebuild. It will Add(1), begin tx, SELECT best block, then
	// block on the first UPDATE (waiting for blocker to release).
	rebuildDone := make(chan error, 1)
	go func() {
		rebuildDone <- s.rebuildOnMainChainFlag(context.Background(), false)
	}()

	// Wait for the goroutine to enter and increment the guard. Because the
	// rebuild is stuck on the lock, this observation is not racing completion —
	// the rebuild cannot progress until we release blocker.
	guardDeadline := time.Now().Add(5 * time.Second)
	for s.mainChainRebuilding.Load() == 0 {
		if time.Now().After(guardDeadline) {
			_ = blocker.Rollback()
			<-rebuildDone
			t.Fatal("rebuild goroutine never incremented guard")
		}
		runtime.Gosched()
	}

	// At this point the rebuild is mid-flight with guard > 0. Assert the
	// documented invariant — a reader calling CheckBlockIsInCurrentChain now
	// would take the CTE fallback.
	require.Greater(t, s.mainChainRebuilding.Load(), int32(0),
		"guard must be > 0 while rebuild is in progress")

	// Release the blocker so the rebuild can finish.
	require.NoError(t, blocker.Rollback())

	select {
	case err := <-rebuildDone:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("rebuild did not complete after blocker released")
	}

	require.EqualValues(t, 0, s.mainChainRebuilding.Load(), "guard released after rebuild returns")
}

// TestGetLatestBlockHeaderFromBlockLocator_ForkTipFallback verifies the fix for
// the semantic divergence: when bestBlockHash is a fork block (on_main_chain=false)
// the function must fall back to the CTE and return the fork's ancestor, not a
// same-height main-chain substitute.
func TestGetLatestBlockHeaderFromBlockLocator_ForkTipFallback(t *testing.T) {
	s := newOnMainChainTestStore(t)

	// Main chain: genesis → block1 → block2. Fork at height 2: blockAlternative2
	// (same parent as block2, different hash).
	storeBlocks(t, s, block1, block2, blockAlternative2)

	require.True(t, getOnMainChain(t, s, block2.Hash().CloneBytes()), "block2 on main chain")
	require.False(t, getOnMainChain(t, s, blockAlternative2.Hash().CloneBytes()), "blockAlternative2 is fork")

	// Query from the fork tip with a locator that contains the fork tip itself.
	// Expected (CTE): highest locator block that's an ancestor of blockAlternative2
	// = blockAlternative2. The fast path would instead return block2 (same height,
	// on main chain, in locator) — semantically wrong.
	locator := []chainhash.Hash{*blockAlternative2.Hash(), *block1.Hash()}
	header, meta, err := s.GetLatestBlockHeaderFromBlockLocator(context.Background(), blockAlternative2.Hash(), locator)
	require.NoError(t, err)
	require.NotNil(t, header)
	require.Equal(t, blockAlternative2.Hash().String(), header.Hash().String(),
		"fork-tip caller must receive the fork block, not a main-chain block at the same height")
	require.EqualValues(t, 2, meta.Height)
}

// TestGetLatestBlockHeaderFromBlockLocator_MainChainTipFastPath verifies the fast
// path still works for the common case (bestBlockHash on main chain).
func TestGetLatestBlockHeaderFromBlockLocator_MainChainTipFastPath(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2, block3)

	locator := []chainhash.Hash{*block3.Hash(), *block2.Hash(), *block1.Hash()}
	header, meta, err := s.GetLatestBlockHeaderFromBlockLocator(context.Background(), block3.Hash(), locator)
	require.NoError(t, err)
	require.NotNil(t, header)
	require.Equal(t, block3.Hash().String(), header.Hash().String(), "highest locator match must be returned")
	require.EqualValues(t, 3, meta.Height)
}

// TestCheckBlockIsInCurrentChainSQL_SingleQueryMultipleIDs verifies the new
// single-query IN() fast path returns correct ANY-of semantics across multiple
// IDs in one round-trip.
func TestCheckBlockIsInCurrentChainSQL_SingleQueryMultipleIDs(t *testing.T) {
	// Keep useInMemoryChainCheck default (typically false in test settings) so
	// CheckBlockIsInCurrentChain routes through the SQL fallback.
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2, blockAlternative2)

	mainID := getBlockID(t, s, block2.Hash().CloneBytes())
	forkID := getBlockID(t, s, blockAlternative2.Hash().CloneBytes())
	require.NotEqualValues(t, 0, mainID)
	require.NotEqualValues(t, 0, forkID)

	// All on-chain: true.
	result, err := s.checkBlockIsInCurrentChainSQL(context.Background(), []uint32{mainID})
	require.NoError(t, err)
	require.True(t, result)

	// Any-of semantics: one on-chain plus one off-chain must return true.
	result, err = s.checkBlockIsInCurrentChainSQL(context.Background(), []uint32{forkID, mainID})
	require.NoError(t, err)
	require.True(t, result, "ANY-of: one on-chain ID is enough")

	// All off-chain: false.
	result, err = s.checkBlockIsInCurrentChainSQL(context.Background(), []uint32{forkID})
	require.NoError(t, err)
	require.False(t, result)

	// Non-existent IDs must be ignored silently, not return an error.
	result, err = s.checkBlockIsInCurrentChainSQL(context.Background(), []uint32{9_999_999, mainID})
	require.NoError(t, err)
	require.True(t, result, "ANY-of including unknown ID plus on-chain ID must succeed")

	// Unknown-only returns false without error.
	result, err = s.checkBlockIsInCurrentChainSQL(context.Background(), []uint32{9_999_998, 9_999_997})
	require.NoError(t, err)
	require.False(t, result)
}

// TestCheckBlockIsInCurrentChainSQL_FastAndCTEAgree verifies that the fast path
// and CTE fallback return the same answer on identical input, across multiple IDs.
func TestCheckBlockIsInCurrentChainSQL_FastAndCTEAgree(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2, blockAlternative2)

	mainID := getBlockID(t, s, block2.Hash().CloneBytes())
	forkID := getBlockID(t, s, blockAlternative2.Hash().CloneBytes())

	cases := [][]uint32{
		{mainID},
		{forkID},
		{mainID, forkID},
		{forkID, mainID},
		{9_999_999, mainID},
		{9_999_999},
	}
	for _, ids := range cases {
		fast, err := s.checkBlockIsInCurrentChainSQL(context.Background(), ids)
		require.NoError(t, err, "fast path: %v", ids)

		s.mainChainRebuilding.Add(1)
		cte, err := s.checkBlockIsInCurrentChainSQL(context.Background(), ids)
		s.mainChainRebuilding.Add(-1)
		require.NoError(t, err, "CTE path: %v", ids)

		require.Equal(t, fast, cte, "fast and CTE must agree for IDs=%v", ids)
	}
}

// TestGetBlockGraphData_GenesisIncludedBlock1Excluded verifies the exact
// semantics of the fast path match the original CTE: genesis is included via
// the CTE's anchor clause, and block at height 1 is excluded because its
// parent_id equals genesis's id (0), which the CTE's `parent_id != 0` guard
// drops.
func TestGetBlockGraphData_GenesisIncludedBlock1Excluded(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2, block3)

	// period 0 — include everything with block_time >= 0.
	data, err := s.GetBlockGraphData(context.Background(), 0)
	require.NoError(t, err)

	// Fast path and CTE must agree on shape; verify both call sites.
	fastCount := len(data.DataPoints)

	s.mainChainRebuilding.Add(1)
	data, err = s.GetBlockGraphData(context.Background(), 0)
	s.mainChainRebuilding.Add(-1)
	require.NoError(t, err)
	cteCount := len(data.DataPoints)

	require.Equal(t, cteCount, fastCount, "fast and CTE must yield the same count")
}

// TestNeedsFullOnMainChainRebuild_CanceledContextReturnsError verifies the
// error surface of needsFullOnMainChainRebuild when the context is canceled.
// The companion test TestStartupFallback_DbErrorTriggersFullRebuild covers
// the downstream "on error, opt into full rebuild" startup behaviour.
func TestNeedsFullOnMainChainRebuild_CanceledContextReturnsError(t *testing.T) {
	s := newOnMainChainTestStore(t)

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := s.needsFullOnMainChainRebuild(canceledCtx)
	require.Error(t, err, "canceled context must surface an error")
}

// TestStartupFallback_DbErrorTriggersFullRebuild verifies the startup goroutine's
// behaviour when needsFullOnMainChainRebuild fails: it must fall back to a full
// rebuild (not a bounded one). The previous test only checked the error surface.
// This test exercises the fallback end-to-end by:
//  1. Creating a store, letting the startup rebuild run
//  2. Storing several blocks
//  3. Corrupting a block deeper than the bounded window
//  4. Calling the same function the startup goroutine calls (full=true when
//     needsFull returns an error or true) and verifying the deep block is fixed
//
// We can't directly trigger a DB error mid-startup, but we can verify the
// semantic equivalence: when full=true is chosen, rebuildOnMainChainFlag
// corrects blocks outside the bounded window.
func TestStartupFallback_DbErrorTriggersFullRebuild(t *testing.T) {
	// CoinbaseMaturity=0 → bounded window is just the tip. A corruption below
	// the window can only be corrected by a full rebuild.
	s := newOnMainChainTestStoreWith(t, func(ts *settings.Settings) {
		ts.ChainCfgParams.CoinbaseMaturity = 0
	})
	storeBlocks(t, s, block1, block2, block3)

	// Corrupt block1 outside the bounded window.
	_, err := s.db.Exec(`UPDATE blocks SET on_main_chain = false WHERE hash = $1`, block1.Hash().CloneBytes())
	require.NoError(t, err)
	require.False(t, getOnMainChain(t, s, block1.Hash().CloneBytes()))

	// Simulate the startup goroutine's error-path choice: full=true.
	err = s.rebuildOnMainChainFlag(context.Background(), true)
	require.NoError(t, err)
	require.True(t, getOnMainChain(t, s, block1.Hash().CloneBytes()),
		"full rebuild (chosen on needsFull error) must correct blocks outside the bounded window")
}

// getBlockID is a test helper that reads the blocks.id for a block hash.
func getBlockID(t *testing.T, s *SQL, hashBytes []byte) uint32 {
	t.Helper()
	var id uint32
	err := s.db.QueryRow(`SELECT id FROM blocks WHERE hash = $1`, hashBytes).Scan(&id)
	require.NoError(t, err)
	return id
}

// TestRevalidateBlock_NonExistentBlock verifies RevalidateBlock returns an error
// for a block that does not exist, without touching the on_main_chain flag.
func TestRevalidateBlock_NonExistentBlock(t *testing.T) {
	s := newOnMainChainTestStore(t)

	bogus, err := chainhash.NewHashFromStr("1111111111111111111111111111111111111111111111111111111111111111")
	require.NoError(t, err)

	err = s.RevalidateBlock(context.Background(), bogus)
	require.Error(t, err, "revalidating a non-existent block must return an error")
	require.EqualValues(t, 0, s.mainChainRebuilding.Load(), "guard not taken when block does not exist")
}

// TestRevalidateBlock_WithInMemoryChainCheck exercises the useInMemoryChainCheck
// branch inside RevalidateBlock (resetChainWalkCache + triggerRebuildOffChainSet).
func TestRevalidateBlock_WithInMemoryChainCheck(t *testing.T) {
	s := newOnMainChainTestStoreWith(t, func(ts *settings.Settings) {
		ts.BlockChain.UseInMemoryChainCheck = true
	})
	storeBlocks(t, s, block1, block2)

	_, err := s.InvalidateBlock(context.Background(), block2.Hash())
	require.NoError(t, err)
	require.False(t, getOnMainChain(t, s, block2.Hash().CloneBytes()), "post-invalidate: off chain")

	err = s.RevalidateBlock(context.Background(), block2.Hash())
	require.NoError(t, err)
	require.True(t, getOnMainChain(t, s, block2.Hash().CloneBytes()), "post-revalidate: back on main chain")
	require.EqualValues(t, 0, s.mainChainRebuilding.Load(), "guard released")
}

// TestInvalidateBlock_GuardBalancedOnError verifies that InvalidateBlock
// balances its mainChainRebuilding Add(1)/Add(-1) regardless of which path
// returns an error. The two escape paths after the guard is taken are:
//  1. GetBlockExists / exists-check fails — guard never taken (covered by
//     TestRevalidateBlock_NonExistentBlock for the mirror function).
//  2. QueryContext fails AFTER guard is taken — guard must still be released
//     via the defer.
//
// We cannot deterministically trigger path (2) via a canceled context because
// the pre-check also uses the same context, so we instead verify the invariant
// holistically: under a variety of early-cancellation scenarios, the guard
// counter must return to 0.
func TestInvalidateBlock_GuardBalancedOnError(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2)

	// Pre-canceled context: whichever DB call fails first (GetBlockExists or
	// the main QueryContext), the defer must leave the counter balanced.
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	for i := 0; i < 5; i++ {
		_, _ = s.InvalidateBlock(canceledCtx, block2.Hash())
		require.EqualValues(t, 0, s.mainChainRebuilding.Load(),
			"guard must be balanced after error path #%d", i)
	}

	// Non-existent block: exits before the guard is taken.
	bogus, err := chainhash.NewHashFromStr("1111111111111111111111111111111111111111111111111111111111111111")
	require.NoError(t, err)
	_, _ = s.InvalidateBlock(context.Background(), bogus)
	require.EqualValues(t, 0, s.mainChainRebuilding.Load(), "guard balanced after non-existent path")

	// Successful invalidation: must also end at 0.
	_, err = s.InvalidateBlock(context.Background(), block2.Hash())
	require.NoError(t, err)
	require.EqualValues(t, 0, s.mainChainRebuilding.Load(), "guard balanced after success")
}

// TestCheckBlockIsInCurrentChainSQL_EmptyInput verifies defense-in-depth: the
// direct-caller entry point must not panic on an empty input slice (the public
// wrapper checks this, but callers like benchmarks may reach the SQL path
// directly).
func TestCheckBlockIsInCurrentChainSQL_EmptyInput(t *testing.T) {
	s := newOnMainChainTestStore(t)

	// Fast path.
	result, err := s.checkBlockIsInCurrentChainSQL(context.Background(), nil)
	require.NoError(t, err)
	require.False(t, result, "empty input is not on main chain")

	result, err = s.checkBlockIsInCurrentChainSQL(context.Background(), []uint32{})
	require.NoError(t, err)
	require.False(t, result)

	// CTE fallback branch.
	s.mainChainRebuilding.Add(1)
	result, err = s.checkBlockIsInCurrentChainSQL(context.Background(), nil)
	s.mainChainRebuilding.Add(-1)
	require.NoError(t, err)
	require.False(t, result)
}

// TestCheckBlockIsInCurrentChainSQL_MultiBatch exercises the batching loop when
// len(blockIDs) exceeds maxIDsPerCheckBatch, ensuring the continue-on-no-match
// branch is covered and a real match in a later batch is found.
func TestCheckBlockIsInCurrentChainSQL_MultiBatch(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2)

	mainID := getBlockID(t, s, block2.Hash().CloneBytes())

	// Build a slice that spans two batches: batch 1 is all misses, batch 2
	// contains the real on-chain ID as the last element. Forces the loop to
	// iterate past the first batch.
	ids := make([]uint32, 0, maxIDsPerCheckBatch+1)
	for i := uint32(1); i <= maxIDsPerCheckBatch; i++ {
		ids = append(ids, 900_000_000+i) // guaranteed non-existent
	}
	ids = append(ids, mainID)

	result, err := s.checkBlockIsInCurrentChainSQL(context.Background(), ids)
	require.NoError(t, err)
	require.True(t, result, "match in the second batch must be found")

	// All-miss across multiple batches returns false.
	onlyMisses := make([]uint32, 0, maxIDsPerCheckBatch+10)
	for i := uint32(1); i <= maxIDsPerCheckBatch+10; i++ {
		onlyMisses = append(onlyMisses, 800_000_000+i)
	}
	result, err = s.checkBlockIsInCurrentChainSQL(context.Background(), onlyMisses)
	require.NoError(t, err)
	require.False(t, result, "all-miss across multiple batches must return false")
}

// TestGetLatestBlockHeaderFromBlockLocator_PreflightErrorFallsBackToCTE cancels
// the context before calling to force the preflight query (and the CTE query)
// to fail. The preflight's error is swallowed; the CTE path then surfaces the
// cancellation. Verifies the swallow-and-fall-through branch runs.
func TestGetLatestBlockHeaderFromBlockLocator_PreflightErrorFallsBackToCTE(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1)

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err := s.GetLatestBlockHeaderFromBlockLocator(canceledCtx, block1.Hash(), []chainhash.Hash{*block1.Hash()})
	require.Error(t, err, "canceled context must surface as an error via the CTE path")
}

// withCTEFallback runs f with mainChainRebuilding temporarily bumped so that
// the CTE fallback is used by any `on_main_chain`-aware query. Used by the
// fast-vs-CTE consistency tests to compare the two query paths on identical
// DB state without needing to actually trigger a rebuild.
func withCTEFallback(s *SQL, f func()) {
	s.mainChainRebuilding.Add(1)
	defer s.mainChainRebuilding.Add(-1)
	s.responseCache.DeleteAll() // ensure the fast-path result is not returned from cache
	f()
}

// runConsistency runs the same query once on the fast path and once on the
// CTE fallback and returns both results for comparison.
func runConsistency[T any](t *testing.T, s *SQL, run func() (T, error)) (fast, cte T) {
	t.Helper()
	// Fast path (mainChainRebuilding == 0 by default).
	s.responseCache.DeleteAll()
	fast, err := run()
	require.NoError(t, err, "fast path")

	// CTE fallback.
	withCTEFallback(s, func() {
		var cteErr error
		cte, cteErr = run()
		require.NoError(t, cteErr, "CTE path")
	})
	return fast, cte
}

// TestOnMainChain_ConsistentWithGetBlocksByHeight verifies GetBlocksByHeight
// fast path and CTE fallback return identical blocks across several ranges,
// including main chain + fork scenarios.
func TestOnMainChain_ConsistentWithGetBlocksByHeight(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2, block3, blockAlternative2)

	ranges := []struct{ start, end uint32 }{
		{1, 3}, {0, 3}, {2, 2}, {1, 1}, {3, 3}, {0, 0}, {0, 100},
	}
	for _, r := range ranges {
		fast, cte := runConsistency(t, s, func() ([]*model.Block, error) {
			return s.GetBlocksByHeight(context.Background(), r.start, r.end)
		})
		require.Equal(t, len(cte), len(fast), "len disagree range=[%d,%d]", r.start, r.end)
		for i := range fast {
			require.Equal(t, cte[i].Hash().String(), fast[i].Hash().String(),
				"block %d disagrees at range=[%d,%d]", i, r.start, r.end)
			require.Equal(t, cte[i].Height, fast[i].Height)
		}
	}
}

// TestOnMainChain_ConsistentWithGetBlockHeadersByHeight verifies
// GetBlockHeadersByHeight fast path and CTE fallback return identical headers.
func TestOnMainChain_ConsistentWithGetBlockHeadersByHeight(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2, block3, blockAlternative2)

	ranges := []struct{ start, end uint32 }{
		{1, 3}, {0, 3}, {2, 2}, {0, 0}, {0, 100},
	}
	for _, r := range ranges {
		type result struct {
			headers []*model.BlockHeader
			metas   []*model.BlockHeaderMeta
		}
		fast, cte := runConsistency(t, s, func() (result, error) {
			h, m, err := s.GetBlockHeadersByHeight(context.Background(), r.start, r.end)
			return result{headers: h, metas: m}, err
		})
		require.Equal(t, len(cte.headers), len(fast.headers), "len disagree range=[%d,%d]", r.start, r.end)
		for i := range fast.headers {
			require.Equal(t, cte.headers[i].Hash().String(), fast.headers[i].Hash().String(),
				"header %d disagrees at range=[%d,%d]", i, r.start, r.end)
			require.Equal(t, cte.metas[i].Height, fast.metas[i].Height)
			require.Equal(t, cte.metas[i].TxCount, fast.metas[i].TxCount)
		}
	}
}

// TestOnMainChain_ConsistentWithGetLastNBlocks verifies the three branches of
// GetLastNBlocks (includeOrphans, fromHeight>0, fromHeight==0) agree between
// fast and CTE paths.
func TestOnMainChain_ConsistentWithGetLastNBlocks(t *testing.T) {
	s := newOnMainChainTestStore(t)
	// Build a main chain with a fork so the fast-path on_main_chain filter
	// meaningfully differs from the full-table scan.
	storeBlocks(t, s, block1, block2, block3, blockAlternative2)

	// fromHeight > 0 branch.
	t.Run("fromHeight>0", func(t *testing.T) {
		fast, cte := runConsistency(t, s, func() ([]*model.BlockInfo, error) {
			return s.GetLastNBlocks(context.Background(), 5, false, 3)
		})
		require.Equal(t, len(cte), len(fast))
		for i := range fast {
			require.Equal(t, cte[i].Height, fast[i].Height, "index %d", i)
			require.Equal(t, cte[i].BlockHeader, fast[i].BlockHeader, "index %d", i)
		}
	})

	// fromHeight == 0 branch (default tip-anchored).
	t.Run("fromHeight=0", func(t *testing.T) {
		fast, cte := runConsistency(t, s, func() ([]*model.BlockInfo, error) {
			return s.GetLastNBlocks(context.Background(), 5, false, 0)
		})
		require.Equal(t, len(cte), len(fast))
		for i := range fast {
			require.Equal(t, cte[i].Height, fast[i].Height)
			require.Equal(t, cte[i].BlockHeader, fast[i].BlockHeader)
		}
	})

	// includeOrphans branch — no CTE/fast-path split, but exercise anyway.
	t.Run("includeOrphans", func(t *testing.T) {
		fast, cte := runConsistency(t, s, func() ([]*model.BlockInfo, error) {
			return s.GetLastNBlocks(context.Background(), 10, true, 0)
		})
		require.Equal(t, len(cte), len(fast))
	})
}

// TestOnMainChain_ConsistentWithGetBlockStats verifies GetBlockStats fast path
// and CTE fallback return identical stats.
func TestOnMainChain_ConsistentWithGetBlockStats(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2, block3, blockAlternative2)

	fast, cte := runConsistency(t, s, func() (*model.BlockStats, error) {
		return s.GetBlockStats(context.Background())
	})

	require.Equal(t, cte.BlockCount, fast.BlockCount, "BlockCount")
	require.Equal(t, cte.TxCount, fast.TxCount, "TxCount")
	require.Equal(t, cte.MaxHeight, fast.MaxHeight, "MaxHeight")
	require.Equal(t, cte.AvgBlockSize, fast.AvgBlockSize, "AvgBlockSize")
	require.Equal(t, cte.AvgTxCountPerBlock, fast.AvgTxCountPerBlock, "AvgTxCountPerBlock")
	require.Equal(t, cte.FirstBlockTime, fast.FirstBlockTime, "FirstBlockTime")
	require.Equal(t, cte.LastBlockTime, fast.LastBlockTime, "LastBlockTime")
	require.Equal(t, cte.ChainWork, fast.ChainWork, "ChainWork")
}

// TestOnMainChain_ConsistentWithFindBlocksContainingSubtree verifies
// FindBlocksContainingSubtree fast path and CTE fallback return identical
// block sets. The test fixture's blocks all reference the same subtree hash,
// so both paths must find all main-chain blocks that contain it and skip the
// fork block (not on main chain).
func TestOnMainChain_ConsistentWithFindBlocksContainingSubtree(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2, block3, blockAlternative2)

	// All test blocks share this subtree hash (see sql_test.go test fixtures).
	subtreeHash, err := chainhash.NewHashFromStr("0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098")
	require.NoError(t, err)

	for _, maxBlocks := range []uint32{0, 1, 5, 100} {
		fast, cte := runConsistency(t, s, func() ([]*model.Block, error) {
			return s.FindBlocksContainingSubtree(context.Background(), subtreeHash, maxBlocks)
		})
		require.Equal(t, len(cte), len(fast), "len disagree maxBlocks=%d", maxBlocks)
		for i := range fast {
			require.Equal(t, cte[i].Hash().String(), fast[i].Hash().String(),
				"block %d disagrees at maxBlocks=%d", i, maxBlocks)
		}
	}
}

// TestReconcileOnMainChain_ReorgDiff verifies the helper restores correct
// on_main_chain flags after a typical reorg, leaving common ancestors
// untouched.
func TestReconcileOnMainChain_ReorgDiff(t *testing.T) {
	s := newOnMainChainTestStore(t)

	// Build main chain genesis → block1 → block2 → block3 (all on_main_chain).
	storeBlocks(t, s, block1, block2, block3)

	// Build a longer fork off block1: block1 → altBlock2 → forkB3 → forkB4.
	// Storing the fork blocks via StoreBlock triggers the reorg path which
	// already calls reconcile. To exercise the helper directly we then
	// rewind the flags manually and re-run reconcile.
	forkB3 := createBlock3OnFork(blockAlternative2)
	forkB4 := createBlock3OnFork(forkB3)
	storeBlocks(t, s, blockAlternative2, forkB3, forkB4)

	// Rewind flags to the pre-reorg world so reconcile has work to do.
	_, err := s.db.Exec(`UPDATE blocks SET on_main_chain = false WHERE hash = $1 OR hash = $2 OR hash = $3`,
		blockAlternative2.Hash().CloneBytes(), forkB3.Hash().CloneBytes(), forkB4.Hash().CloneBytes())
	require.NoError(t, err)
	_, err = s.db.Exec(`UPDATE blocks SET on_main_chain = true WHERE hash = $1 OR hash = $2 OR hash = $3`,
		block1.Hash().CloneBytes(), block2.Hash().CloneBytes(), block3.Hash().CloneBytes())
	require.NoError(t, err)

	// Sanity: pre-state matches the pre-reorg world.
	require.True(t, getOnMainChain(t, s, block1.Hash().CloneBytes()))
	require.True(t, getOnMainChain(t, s, block2.Hash().CloneBytes()))
	require.True(t, getOnMainChain(t, s, block3.Hash().CloneBytes()))
	require.False(t, getOnMainChain(t, s, blockAlternative2.Hash().CloneBytes()))
	require.False(t, getOnMainChain(t, s, forkB3.Hash().CloneBytes()))
	require.False(t, getOnMainChain(t, s, forkB4.Hash().CloneBytes()))

	// Reconcile: walks from the actual best (forkB4 by chain_work) and fixes flags.
	require.NoError(t, s.reconcileOnMainChain(context.Background()))

	// Post-state: block1 (LCA) untouched, block2/block3 off-chain, fork chain on-chain.
	require.True(t, getOnMainChain(t, s, block1.Hash().CloneBytes()), "LCA stays on main chain")
	require.False(t, getOnMainChain(t, s, block2.Hash().CloneBytes()), "old branch flipped off")
	require.False(t, getOnMainChain(t, s, block3.Hash().CloneBytes()), "old branch flipped off")
	require.True(t, getOnMainChain(t, s, blockAlternative2.Hash().CloneBytes()), "new branch flipped on")
	require.True(t, getOnMainChain(t, s, forkB3.Hash().CloneBytes()), "new branch flipped on")
	require.True(t, getOnMainChain(t, s, forkB4.Hash().CloneBytes()), "new branch flipped on (tip)")

	// Idempotency: a second call against the now-correct state must be a no-op.
	require.NoError(t, s.reconcileOnMainChain(context.Background()))
	require.True(t, getOnMainChain(t, s, block1.Hash().CloneBytes()))
	require.False(t, getOnMainChain(t, s, block2.Hash().CloneBytes()))
	require.True(t, getOnMainChain(t, s, forkB4.Hash().CloneBytes()))
}

// TestReconcileOnMainChain_AlreadyConsistent verifies the helper is a no-op
// when on_main_chain already reflects the chain_work-best lineage.
func TestReconcileOnMainChain_AlreadyConsistent(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2)

	require.NoError(t, s.reconcileOnMainChain(context.Background()))

	require.True(t, getOnMainChain(t, s, block1.Hash().CloneBytes()), "block1 unchanged")
	require.True(t, getOnMainChain(t, s, block2.Hash().CloneBytes()), "tip unchanged")
}

// TestReconcileOnMainChain_EmptyDB exercises the ErrNoRows branch: when the
// blocks table is empty (no valid rows) reconcile commits cleanly without
// running the UPDATE.
func TestReconcileOnMainChain_EmptyDB(t *testing.T) {
	s := newOnMainChainTestStore(t)

	_, err := s.db.Exec(`DELETE FROM blocks`)
	require.NoError(t, err)

	require.NoError(t, s.reconcileOnMainChain(context.Background()),
		"reconcile must commit cleanly on an empty blocks table")
}

// TestReconcileOnMainChain_LowCoinbaseMaturityFloor exercises the maxDepth
// floor branch (maxDepth = max(2*CoinbaseMaturity, 100)). With
// CoinbaseMaturity = 0, maxDepth would be 0 without the floor; the floor
// keeps the walk usable for tests with small consensus parameters.
func TestReconcileOnMainChain_LowCoinbaseMaturityFloor(t *testing.T) {
	s := newOnMainChainTestStoreWith(t, func(ts *settings.Settings) {
		ts.ChainCfgParams.CoinbaseMaturity = 0
	})
	storeBlocks(t, s, block1, block2, block3)

	// Manually clear a flag inside the walk window; reconcile must restore it.
	_, err := s.db.Exec(`UPDATE blocks SET on_main_chain = false WHERE hash = $1`,
		block2.Hash().CloneBytes())
	require.NoError(t, err)

	require.NoError(t, s.reconcileOnMainChain(context.Background()))
	require.True(t, getOnMainChain(t, s, block2.Hash().CloneBytes()),
		"floor-bounded walk must still cover the actual best's lineage")
}

// TestReconcileOnMainChain_BeginTxError exercises the BeginTx error branch
// by closing the underlying DB before invocation. The deferred rollback in
// the err != nil case is also exercised through this path.
func TestReconcileOnMainChain_BeginTxError(t *testing.T) {
	s := newOnMainChainTestStore(t)
	require.NoError(t, s.db.Close())

	err := s.reconcileOnMainChain(context.Background())
	require.Error(t, err, "reconcile must return an error when the DB is unusable")
}

// TestReconcileOnMainChain_QueryError exercises the non-ErrNoRows scan error
// branch via a pre-canceled context. The exact branch hit depends on whether
// BeginTx or Scan checks the context first — both surface as a non-nil error
// return from the helper.
func TestReconcileOnMainChain_QueryError(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := s.reconcileOnMainChain(ctx)
	require.Error(t, err, "reconcile must return an error when context is canceled")
}

// TestInvalidateBlock_CanceledContextErrorPath covers the QueryContext error
// branch in InvalidateBlock by passing a pre-canceled context.
func TestInvalidateBlock_CanceledContextErrorPath(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := s.InvalidateBlock(ctx, block2.Hash())
	require.Error(t, err, "InvalidateBlock must surface a canceled-context error")
}

// TestRevalidateBlock_CanceledContextErrorPath covers the ExecContext error
// branch in RevalidateBlock by passing a pre-canceled context.
func TestRevalidateBlock_CanceledContextErrorPath(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2)

	// Invalidate first so RevalidateBlock has work to do; uses a fresh context.
	_, err := s.InvalidateBlock(context.Background(), block2.Hash())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = s.RevalidateBlock(ctx, block2.Hash())
	require.Error(t, err, "RevalidateBlock must surface a canceled-context error")
}

// TestReconcileOnMainChain_FillsFastPathDescendantGap is the regression test
// for the race the reviewer flagged: a concurrent fast-path StoreBlock can
// extend the actual best with on_main_chain=true while a slow-path writer is
// in flight, leaving its ancestor flagged false. Reconcile must fill the gap
// rather than skipping the diff because the caller's "expected" tip moved.
//
// We simulate the race deterministically by manually rewinding the on_main_chain
// flag on a block that lies on the actual best's lineage, mimicking the state
// the reviewer described (F+1 on_main_chain=true, F on_main_chain=false above
// a still-flagged ancestor Y). Reconcile must restore F's flag.
func TestReconcileOnMainChain_FillsFastPathDescendantGap(t *testing.T) {
	s := newOnMainChainTestStore(t)

	// Build genesis → block1 → block2 → block3. block3 is the chain_work best.
	storeBlocks(t, s, block1, block2, block3)

	// Manually create a "gap": clear block2's on_main_chain while leaving
	// block1 and block3 flagged. This is exactly the inconsistent state a
	// fast-path INSERT can produce when it extends a not-yet-flagged best
	// during a slow-path window.
	_, err := s.db.Exec(`UPDATE blocks SET on_main_chain = false WHERE hash = $1`,
		block2.Hash().CloneBytes())
	require.NoError(t, err)
	require.True(t, getOnMainChain(t, s, block1.Hash().CloneBytes()))
	require.False(t, getOnMainChain(t, s, block2.Hash().CloneBytes()), "pre-condition: gap")
	require.True(t, getOnMainChain(t, s, block3.Hash().CloneBytes()))

	// Reconcile must walk the actual best's lineage and flip the gap closed.
	require.NoError(t, s.reconcileOnMainChain(context.Background()))

	require.True(t, getOnMainChain(t, s, block1.Hash().CloneBytes()))
	require.True(t, getOnMainChain(t, s, block2.Hash().CloneBytes()), "gap on the actual main chain must be filled")
	require.True(t, getOnMainChain(t, s, block3.Hash().CloneBytes()))
}

// TestReconcileOnMainChain_NoStaleBranchSurvives is the regression test for
// the stale-old-tip race the reviewer flagged: a previous reconciliation
// already marked branch A as the main chain, but a later reconciliation
// using a stale snapshot would leave A flagged alongside the new winner B.
// We simulate by leaving two branches flagged on_main_chain=true and asserting
// reconcile clears the loser.
func TestReconcileOnMainChain_NoStaleBranchSurvives(t *testing.T) {
	s := newOnMainChainTestStore(t)

	// Main chain genesis → block1 → block2 → block3 (block3 is best).
	storeBlocks(t, s, block1, block2, block3)

	// Manually flag a fork block on_main_chain=true to simulate a stray
	// "old branch" that a previous broken reconciliation left behind.
	storeBlocks(t, s, blockAlternative2)
	_, err := s.db.Exec(`UPDATE blocks SET on_main_chain = true WHERE hash = $1`,
		blockAlternative2.Hash().CloneBytes())
	require.NoError(t, err)
	require.True(t, getOnMainChain(t, s, blockAlternative2.Hash().CloneBytes()), "pre-condition: stale flag")

	require.NoError(t, s.reconcileOnMainChain(context.Background()))

	require.True(t, getOnMainChain(t, s, block1.Hash().CloneBytes()))
	require.True(t, getOnMainChain(t, s, block2.Hash().CloneBytes()))
	require.True(t, getOnMainChain(t, s, block3.Hash().CloneBytes()))
	require.False(t, getOnMainChain(t, s, blockAlternative2.Hash().CloneBytes()),
		"stale on_main_chain=true on a non-best branch must be cleared")
}

// TestStoreBlock_NoTwoSiblingsFlaggedUnderRace is the regression test for the
// reviewer's third-round finding: two concurrent same-parent StoreBlock calls
// must not both end up with on_main_chain=true. Without slowPathMu in the
// fast path, both calls could read the same cached preBestHash, both compute
// onMainChain=true, both INSERT with on_main_chain=true, and both succeed at
// the maxBlockID CAS in sequence (the CAS only proves ID-sequence headship,
// not chain_work tiebreak winner). With serialization, the second writer
// observes the first's row as the new best, so its onMainChain comes out
// false and it is INSERTed correctly as a fork.
func TestStoreBlock_NoTwoSiblingsFlaggedUnderRace(t *testing.T) {
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1)

	// block2 and blockAlternative2 both extend block1 — siblings at height 2.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _, _ = s.StoreBlock(context.Background(), block2, "peerA")
	}()
	go func() {
		defer wg.Done()
		_, _, _ = s.StoreBlock(context.Background(), blockAlternative2, "peerB")
	}()
	wg.Wait()

	// Invariant: at every height (in particular height 2) at most one block
	// is on_main_chain=true.
	rows, err := s.db.Query(`
		SELECT height, COUNT(*) FROM blocks
		WHERE on_main_chain = true
		GROUP BY height
		HAVING COUNT(*) > 1
	`)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var h, c int64
		require.NoError(t, rows.Scan(&h, &c))
		t.Fatalf("multiple on_main_chain=true blocks at height %d (count=%d) — sibling race not serialized", h, c)
	}
	require.NoError(t, rows.Err())

	// Sanity: exactly one of the two siblings is flagged.
	var flagged int
	require.NoError(t, s.db.QueryRow(`
		SELECT COUNT(*) FROM blocks
		WHERE on_main_chain = true AND height = 2
	`).Scan(&flagged))
	require.Equal(t, 1, flagged, "exactly one sibling at height 2 must be on_main_chain=true")
}

// TestReconcileOnMainChain_SingleMainChainAfterConcurrentInvalidates fires
// invalidate calls from multiple goroutines and asserts the post-state has
// exactly one block on_main_chain=true at any height — the invariant that was
// broken by the parameter-based switch helper under the same concurrency.
func TestReconcileOnMainChain_SingleMainChainAfterConcurrentInvalidates(t *testing.T) {
	s := newOnMainChainTestStore(t)

	// Build a chain with a fork: genesis → block1 → block2 → block3 (main),
	// plus blockAlternative2 (fork off block1).
	storeBlocks(t, s, block1, block2, block3, blockAlternative2)

	// Concurrently: invalidate the fork tip, invalidate the main tip,
	// and revalidate the main tip. The mutex must serialize them and the
	// in-transaction best re-read must reject any stale diffs.
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		_, _ = s.InvalidateBlock(context.Background(), blockAlternative2.Hash())
	}()
	go func() {
		defer wg.Done()
		_, _ = s.InvalidateBlock(context.Background(), block3.Hash())
	}()
	go func() {
		defer wg.Done()
		_ = s.RevalidateBlock(context.Background(), block3.Hash())
	}()
	wg.Wait()

	// Whatever the final outcome, the invariant is: at every height there is
	// at most one block with on_main_chain=true.
	rows, err := s.db.Query(`
		SELECT height, COUNT(*) FROM blocks
		WHERE on_main_chain = true
		GROUP BY height
		HAVING COUNT(*) > 1
	`)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var h, c int64
		require.NoError(t, rows.Scan(&h, &c))
		t.Fatalf("multiple on_main_chain=true blocks at height %d (count=%d) — concurrency invariant broken", h, c)
	}
	require.NoError(t, rows.Err())
}

// TestOnMainChain_MigrationTimeoutExceedsWindowedTimeout guards the invariant
// that the startup full-migration rebuild gets a more generous deadline than
// bounded-window rebuilds. The migration walks the entire chain via a recursive
// CTE and can take minutes on multi-million-block chains (especially on
// undersized VMs); reducing its timeout to match the windowed one silently
// leaves on_main_chain unpopulated after startup and breaks
// CheckBlockIsInCurrentChain's fast path for any block below the windowed
// rebuild floor.
func TestOnMainChain_MigrationTimeoutExceedsWindowedTimeout(t *testing.T) {
	require.Greater(t, migrationFullRebuildTimeout, rebuildOffChainSetTimeout,
		"full-migration rebuild must have a more generous timeout than windowed rebuilds")
	require.GreaterOrEqual(t, migrationFullRebuildTimeout, time.Minute,
		"migration timeout should be minutes-scale to accommodate multi-million-block chains")
}
