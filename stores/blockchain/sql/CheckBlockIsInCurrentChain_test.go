package sql

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCheckBlockIsInCurrentChain_TransientlyFalseFlagStillOnChain reproduces the
// spurious-invalidation bug: on_main_chain can be transiently false on a block
// that IS on the best chain (a slow-path StoreBlock whose reconcileOnMainChain
// failed, or a startup rebuild that exhausted its retries). CheckBlockIsInCurrentChain
// must not report such a block as off-chain, because checkOldBlockIDs escalates a
// negative into a PERMANENT ValidateBlock invalidation that the self-healing flag
// never gets to undo. Both the SQL route (flag fast-path miss → parent_id CTE
// confirm) and the in-memory route (off-chain set is rebuilt from the same bad
// flag, so the candidate must still be SQL-confirmed) must return true.
func TestCheckBlockIsInCurrentChain_TransientlyFalseFlagStillOnChain(t *testing.T) {
	for _, useInMemory := range []bool{false, true} {
		t.Run(fmt.Sprintf("useInMemoryChainCheck=%v", useInMemory), func(t *testing.T) {
			s := newOnMainChainTestStoreWith(t, func(st *settings.Settings) {
				st.BlockChain.UseInMemoryChainCheck = useInMemory
			})
			storeBlocks(t, s, block1, block2, block3)

			var block2ID uint32
			require.NoError(t, s.db.QueryRow(`SELECT id FROM blocks WHERE hash = $1`, block2.Hash()[:]).Scan(&block2ID))

			// Precondition: block2 is genuinely on the main chain.
			ok, err := s.CheckBlockIsInCurrentChain(context.Background(), []uint32{block2ID})
			require.NoError(t, err)
			require.True(t, ok, "precondition: block2 must be on the main chain")

			// Simulate the transient inconsistency: flip on_main_chain to false on a
			// block that is still reachable from the best block via parent_id.
			_, err = s.db.Exec(`UPDATE blocks SET on_main_chain = false WHERE id = $1`, block2ID)
			require.NoError(t, err)

			// The in-memory off-chain set is rebuilt from the on_main_chain flags, so
			// after a bad flag the set would contain this block. Reflect that to
			// exercise the in-memory negative path that previously rejected without
			// confirmation.
			if useInMemory {
				s.offChainBlockIDsMu.Lock()
				s.offChainBlockIDs = map[uint32]struct{}{block2ID: {}}
				s.offChainBlockIDsMu.Unlock()
			}

			// Only the flag changed; block2 is still on the best chain. It MUST still
			// be reported on-chain — a false here is the bug.
			ok, err = s.CheckBlockIsInCurrentChain(context.Background(), []uint32{block2ID})
			require.NoError(t, err)
			require.True(t, ok, "transiently-false on_main_chain must not make an on-chain block report off-chain")
		})
	}
}

func TestCheckBlockIsInCurrentChain_EmptyBlockIDs(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close(context.Background())

	result, err := s.CheckBlockIsInCurrentChain(context.Background(), []uint32{})
	require.NoError(t, err)
	assert.False(t, result, "Empty block IDs should return false")
}

func TestCheckBlockIsInCurrentChain_SingleBlockInChain(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close(context.Background())

	blockID, _, err := s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)

	result, err := s.CheckBlockIsInCurrentChain(context.Background(), []uint32{uint32(blockID)})
	require.NoError(t, err)
	assert.True(t, result, "Block in main chain should return true")
}

func TestCheckBlockIsInCurrentChain_MultipleBlocksInChain(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close(context.Background())

	blockID1, _, err := s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)

	blockID2, _, err := s.StoreBlock(context.Background(), block2, "")
	require.NoError(t, err)

	blockID3, _, err := s.StoreBlock(context.Background(), block3, "")
	require.NoError(t, err)

	blockIDs := []uint32{uint32(blockID1), uint32(blockID2), uint32(blockID3)}
	result, err := s.CheckBlockIsInCurrentChain(context.Background(), blockIDs)
	require.NoError(t, err)
	assert.True(t, result, "All blocks in main chain should return true")
}

func TestCheckBlockIsInCurrentChain_NonExistentBlockID(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close(context.Background())

	_, _, err = s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)

	// Non-existent block IDs above maxBlockID are rejected by the upper-bound
	// check and correctly return false.
	result, err := s.CheckBlockIsInCurrentChain(context.Background(), []uint32{999999})
	require.NoError(t, err)
	assert.False(t, result, "Non-existent block IDs above maxBlockID should return false")
}

func TestCheckBlockIsInCurrentChain_InMemory_ContextCancellation(t *testing.T) {
	s := newStoreWithInMemoryChainCheck(t)
	defer s.Close(context.Background())

	blockID, _, err := s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// The NEGATIVE fast path is fully in-memory: an id above maxBlockID is rejected
	// without any query, so a cancelled context has no effect.
	result, err := s.CheckBlockIsInCurrentChain(ctx, []uint32{999999})
	assert.NoError(t, err)
	assert.False(t, result)

	// A would-be-positive (a real on-chain id) is now confirmed against the
	// authoritative on_main_chain flag so a non-existent id can't be mistaken for
	// on-chain. That confirmation is a DB query, so a cancelled context surfaces as
	// an error rather than an unverified true.
	_, err = s.CheckBlockIsInCurrentChain(ctx, []uint32{uint32(blockID)})
	assert.Error(t, err)
}

func TestCheckBlockIsInCurrentChain_InMemory_ClosedDB(t *testing.T) {
	s := newStoreWithInMemoryChainCheck(t)

	// Store a block so maxBlockID is > 0, then close
	blockID, _, err := s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)

	s.Close(context.Background())

	// Negative fast path is in-memory: an above-maxBlockID id is rejected without
	// touching the (now closed) DB.
	result, err := s.CheckBlockIsInCurrentChain(context.Background(), []uint32{999999})
	assert.NoError(t, err)
	assert.False(t, result)

	// A positive candidate is confirmed against on_main_chain, which needs the DB;
	// with the DB closed this surfaces an error instead of an unverified true.
	_, err = s.CheckBlockIsInCurrentChain(context.Background(), []uint32{uint32(blockID)})
	assert.Error(t, err)
}

func TestCheckBlockIsInCurrentChain_InMemory_PhantomBelowMaxID(t *testing.T) {
	s := newStoreWithInMemoryChainCheck(t)
	defer s.Close(context.Background())

	_, _, err := s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)

	// Commit block2 under a high explicit id, leaving a large gap of non-existent
	// ids below maxBlockID (simulating an orphaned/phantom id-sequence gap).
	const highID = 100000
	committed, _, err := s.StoreBlock(context.Background(), block2, "", options.WithID(highID))
	require.NoError(t, err)
	require.Equal(t, uint64(highID), committed)

	// A phantom id (<= maxBlockID, no row, not in the off-chain set) must be
	// rejected: it has no on_main_chain=true row, identical to the SQL route.
	// Pre-fix the in-memory path wrongly returned true here — a toggled/untoggled
	// consensus split.
	result, err := s.CheckBlockIsInCurrentChain(context.Background(), []uint32{highID - 1})
	require.NoError(t, err)
	assert.False(t, result, "non-existent id below maxBlockID must not be treated as on-chain")

	// Sanity: the real committed on-chain id still resolves true.
	result, err = s.CheckBlockIsInCurrentChain(context.Background(), []uint32{highID})
	require.NoError(t, err)
	assert.True(t, result)
}

// TestCheckBlockIsInCurrentChain_InMemory_UninitialisedMaxBlockID reproduces the
// 2026-06-18 mainnet cascade. On a restart mid-catchup the startup
// rebuildOffChainSet timed out on a cold cache and returned before setting
// maxBlockID, leaving the atomic at 0 once the rebuild guard was released. With
// maxID==0 the in-memory path dropped every committed parent id as "above the
// highest id" and returned (false, nil) — a false negative that checkOldBlockIDs
// escalates into a PERMANENT block invalidation, which froze the chain. A real
// chain's MAX(id) is never 0 (genesis is committed as id 1), so maxID==0 means
// "uninitialised" and must fall through to the authoritative parent_id CTE rather
// than reject. Pre-fix this returned false; post-fix it returns true.
func TestCheckBlockIsInCurrentChain_InMemory_UninitialisedMaxBlockID(t *testing.T) {
	s := newStoreWithInMemoryChainCheck(t)
	defer s.Close(context.Background())

	_, _, err := s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)

	block2ID, _, err := s.StoreBlock(context.Background(), block2, "")
	require.NoError(t, err)

	// Precondition: block2 is genuinely on the main chain (maxBlockID populated).
	ok, err := s.CheckBlockIsInCurrentChain(context.Background(), []uint32{uint32(block2ID)})
	require.NoError(t, err)
	require.True(t, ok, "precondition: block2 must be on the main chain")

	// Simulate the post-timeout startup window: the rebuild guard is released (so
	// the in-memory path is active) but maxBlockID was never set.
	require.Zero(t, s.mainChainRebuilding.Load(), "in-memory path requires guard==0")
	s.maxBlockID.Store(0)

	// A committed, on-chain parent id must NOT be reported off-chain just because
	// maxBlockID is uninitialised. Pre-fix this returned (false, nil) and the
	// caller permanently invalidated the block.
	ok, err = s.CheckBlockIsInCurrentChain(context.Background(), []uint32{uint32(block2ID)})
	require.NoError(t, err)
	require.True(t, ok, "uninitialised maxBlockID (0) must fall through to the CTE, not reject an on-chain block")
}

func TestCheckBlockIsInCurrentChain_MixedOnChainAndOffChain(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close(context.Background())

	// Build main chain: genesis -> block1 -> block2
	blockID1, _, err := s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)

	blockID2, _, err := s.StoreBlock(context.Background(), block2, "")
	require.NoError(t, err)

	// Store a fork block at the same height as block2 (off-chain)
	forkID, _, err := s.StoreBlock(context.Background(), blockAlternative2, "")
	require.NoError(t, err)

	// Mixed: one on-chain block + one off-chain block should return true (ANY-of semantics).
	// This matches the old CTE behavior where the chain walk returned true if ANY input
	// block was found. Required by BlockValidation.checkOldBlockIDs which passes candidate
	// block IDs for a transaction across forks.
	result, err := s.CheckBlockIsInCurrentChain(context.Background(), []uint32{uint32(blockID1), uint32(forkID)})
	require.NoError(t, err)
	assert.True(t, result, "Mixed on-chain and off-chain should return true (ANY-of semantics)")

	// All on-chain should still return true
	result, err = s.CheckBlockIsInCurrentChain(context.Background(), []uint32{uint32(blockID1), uint32(blockID2)})
	require.NoError(t, err)
	assert.True(t, result, "All on-chain blocks should return true")

	// Single off-chain block should return false
	result, err = s.CheckBlockIsInCurrentChain(context.Background(), []uint32{uint32(forkID)})
	require.NoError(t, err)
	assert.False(t, result, "Single off-chain block should return false")
}

func TestCheckBlockIsInCurrentChain_InvalidatedBlock(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close(context.Background())

	blockID1, _, err := s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)

	blockID2, _, err := s.StoreBlock(context.Background(), block2, "")
	require.NoError(t, err)

	// Invalidate block2 — it should now be in the off-chain set
	_, err = s.InvalidateBlock(context.Background(), block2.Header.Hash())
	require.NoError(t, err)

	// block1 should still be on-chain
	result, err := s.CheckBlockIsInCurrentChain(context.Background(), []uint32{uint32(blockID1)})
	require.NoError(t, err)
	assert.True(t, result, "Valid block should still be in chain")

	// block2 should now be off-chain (invalidated)
	result, err = s.CheckBlockIsInCurrentChain(context.Background(), []uint32{uint32(blockID2)})
	require.NoError(t, err)
	assert.False(t, result, "Invalidated block should be off-chain")
}

// newStoreWithInMemoryChainCheck creates a SQL store with useInMemoryChainCheck enabled
// and waits for the startup rebuild goroutine to complete before returning. Tests that
// rely on the in-memory chain check being authoritative (e.g. DB-independence tests)
// must run after the startup guard has been released.
func newStoreWithInMemoryChainCheck(t *testing.T) *SQL {
	t.Helper()
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockChain.UseInMemoryChainCheck = true
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	waitForStartupRebuild(t, s)
	return s
}

// waitForStartupRebuild blocks until the startup rebuild goroutine has released
// its guard, or fails the test after 5 seconds. Use this in tests that need
// deterministic behaviour from the fast-path (guard == 0) or that call Close()
// and want to avoid noisy "database is closed" logs from the still-running
// startup goroutine.
func waitForStartupRebuild(tb testing.TB, s *SQL) {
	tb.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for s.mainChainRebuilding.Load() > 0 {
		if time.Now().After(deadline) {
			tb.Fatal("startup rebuild did not complete within 5 seconds")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestCheckBlockIsInCurrentChain_InMemory_SingleBlockInChain(t *testing.T) {
	s := newStoreWithInMemoryChainCheck(t)
	defer s.Close(context.Background())

	blockID, _, err := s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)

	result, err := s.CheckBlockIsInCurrentChain(context.Background(), []uint32{uint32(blockID)})
	require.NoError(t, err)
	assert.True(t, result, "Block in main chain should return true (in-memory path)")
}

func TestCheckBlockIsInCurrentChain_InMemory_MultipleBlocksInChain(t *testing.T) {
	s := newStoreWithInMemoryChainCheck(t)
	defer s.Close(context.Background())

	blockID1, _, err := s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)

	blockID2, _, err := s.StoreBlock(context.Background(), block2, "")
	require.NoError(t, err)

	blockID3, _, err := s.StoreBlock(context.Background(), block3, "")
	require.NoError(t, err)

	blockIDs := []uint32{uint32(blockID1), uint32(blockID2), uint32(blockID3)}
	result, err := s.CheckBlockIsInCurrentChain(context.Background(), blockIDs)
	require.NoError(t, err)
	assert.True(t, result, "All blocks in main chain should return true (in-memory path)")
}

func TestCheckBlockIsInCurrentChain_InMemory_NonExistentBlockID(t *testing.T) {
	s := newStoreWithInMemoryChainCheck(t)
	defer s.Close(context.Background())

	_, _, err := s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)

	result, err := s.CheckBlockIsInCurrentChain(context.Background(), []uint32{999999})
	require.NoError(t, err)
	assert.False(t, result, "Non-existent block IDs above maxBlockID should return false (in-memory path)")
}

func TestCheckBlockIsInCurrentChain_InMemory_MixedOnChainAndOffChain(t *testing.T) {
	s := newStoreWithInMemoryChainCheck(t)
	defer s.Close(context.Background())

	blockID1, _, err := s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)

	blockID2, _, err := s.StoreBlock(context.Background(), block2, "")
	require.NoError(t, err)

	forkID, _, err := s.StoreBlock(context.Background(), blockAlternative2, "")
	require.NoError(t, err)

	// Mixed: ANY-of semantics
	result, err := s.CheckBlockIsInCurrentChain(context.Background(), []uint32{uint32(blockID1), uint32(forkID)})
	require.NoError(t, err)
	assert.True(t, result, "Mixed on-chain and off-chain should return true (in-memory path)")

	result, err = s.CheckBlockIsInCurrentChain(context.Background(), []uint32{uint32(blockID1), uint32(blockID2)})
	require.NoError(t, err)
	assert.True(t, result, "All on-chain blocks should return true (in-memory path)")

	result, err = s.CheckBlockIsInCurrentChain(context.Background(), []uint32{uint32(forkID)})
	require.NoError(t, err)
	assert.False(t, result, "Single off-chain block should return false (in-memory path)")
}

func TestCheckBlockIsInCurrentChain_InMemory_GenesisOnly(t *testing.T) {
	// When only genesis exists, maxBlockID is 0 (genesis has id=0).
	// Non-zero IDs should return false, not be incorrectly treated as on-chain.
	s := newStoreWithInMemoryChainCheck(t)
	defer s.Close(context.Background())

	result, err := s.CheckBlockIsInCurrentChain(context.Background(), []uint32{1})
	require.NoError(t, err)
	assert.False(t, result, "Non-existent ID should return false when only genesis exists")

	result, err = s.CheckBlockIsInCurrentChain(context.Background(), []uint32{999})
	require.NoError(t, err)
	assert.False(t, result, "Non-existent ID should return false when only genesis exists")

	// Genesis block (id=0) should be on-chain
	result, err = s.CheckBlockIsInCurrentChain(context.Background(), []uint32{0})
	require.NoError(t, err)
	assert.True(t, result, "Genesis block should be on-chain")
}

func TestCheckBlockIsInCurrentChain_InMemory_InvalidatedBlock(t *testing.T) {
	s := newStoreWithInMemoryChainCheck(t)
	defer s.Close(context.Background())

	blockID1, _, err := s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)

	blockID2, _, err := s.StoreBlock(context.Background(), block2, "")
	require.NoError(t, err)

	_, err = s.InvalidateBlock(context.Background(), block2.Header.Hash())
	require.NoError(t, err)

	result, err := s.CheckBlockIsInCurrentChain(context.Background(), []uint32{uint32(blockID1)})
	require.NoError(t, err)
	assert.True(t, result, "Valid block should still be in chain (in-memory path)")

	result, err = s.CheckBlockIsInCurrentChain(context.Background(), []uint32{uint32(blockID2)})
	require.NoError(t, err)
	assert.False(t, result, "Invalidated block should be off-chain (in-memory path)")
}
