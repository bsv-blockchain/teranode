package sql

import (
	"context"
	"fmt"
	"net/url"
	"sync/atomic"
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

	// A real on-chain id is now answered from the forked set, which is also fully
	// in-memory, so a cancelled context has no effect on it either. Only the
	// about-to-reject path (every candidate in the forked set) still queries.
	result, err = s.CheckBlockIsInCurrentChain(ctx, []uint32{uint32(blockID)})
	assert.NoError(t, err)
	assert.True(t, result)
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

	// A positive is answered from the forked set, so a closed database does not stop
	// it. TestCheckBlockIsInCurrentChain_InMemory_OnChainIDAnsweredFromMemory is the
	// dedicated test for that contract; this one only checks the closed database does
	// not turn it into an error.
	result, err = s.CheckBlockIsInCurrentChain(context.Background(), []uint32{uint32(blockID)})
	assert.NoError(t, err)
	assert.True(t, result)
}

// TestCheckBlockIsInCurrentChain_GapIDDivergesBetweenRoutes documents, rather than
// hides, the one input on which the two routes disagree.
//
// A gap id is an id at or below maxBlockID with no committed blocks row. The SQL
// route rejects it, because there is no on_main_chain row and the parent_id walk
// never reaches it. The forked-set route accepts it, because it is not in the forked
// set and the route treats absence as proof of membership.
//
// This is issue 1055, and it is accepted deliberately. Gap ids DO exist and they are
// created by ordinary running, so do not read the paragraphs below as a claim that they
// cannot happen. They can, and the reason this is still safe is measured, not argued.
//
// MEASURED on the Hetzner boxes, 2026-09-01. Gap ids present:
//
//	SELECT (SELECT MAX(id) FROM blocks) + 1 - (SELECT COUNT(*) FROM blocks) FROM blocks;
//	mainnet 184, testnet 0, teratestnet 0    (genesis commits at id 0, hence the +1)
//
// mainnet's 184 sit in 12 interior runs of 1 to 31 ids. Every run follows a multi-minute
// pause in block storage and is exactly as long as the number of blocks that were in
// flight when the node stopped. They are restart scars: an id is reserved for a block,
// the node stops before writing anything about that block, and on restart the block is
// re-processed under a fresh id. Block HEIGHTS are unbroken across every run, so no block
// is missing. Only ids were burned.
//
// Nothing references them, which is the property that makes the accept safe. The utxoset
// store packs a transaction's blocks into tx_ident.membership as 12-byte big-endian
// triples of block id, height and subtree index. Decoding those around all 12 runs, across
// all 8 partitions: 381,599 stamps examined, ZERO pointing at an id with no blocks row.
// Transactions reference id 271114 and then jump to 271117; the burned 271115 and 271116
// appear on nothing. Every transaction carries exactly one block (0 rows with
// length(membership) > 12 out of 18M on one partition), so a transaction's created_height
// is the height of the block stamped on it and that scan could not have missed a stamp.
//
// So the failure that burns an id lands BEFORE any stamping. The wrong answer exists and
// is currently unreachable, because reaching it needs something to hold a burned id and
// hand it to this function. Nothing does.
//
// Do NOT re-derive this. Two separate reviews have now spent significant time proving that
// gap ids exist, which was never in doubt, and stopping there. The question that decides
// safety is whether anything REFERENCES one, and the query above plus the membership
// decode answers it in minutes. If you want to re-check it on a node, run those two, not a
// fresh argument about block-id assignment.
//
// What is NOT a guarantee: the protection is where a crash lands, not a rule the code
// enforces. Two paths can stamp an id that then goes nowhere. storeInvalidBlock re-commits
// a pre-assigned block under a fresh id, abandoning the id already written onto its
// transactions (services/blockvalidation/BlockValidation.go). The gRPC AddBlock handler
// passes a caller-supplied id straight through with no bound and no continuity check
// (services/blockchain/Server.go). Four production call sites pass options.WithID, so an
// earlier version of this comment claiming none did was simply wrong. If either path ever
// fires, the burned id becomes referenced and reachable, permanently, and the asset server
// reads block ids straight off transaction metadata.
//
// The shadow comparison is the standing instrument for that: while
// blockchain_chain_check_shadow_compare is on, every in-memory answer is checked against
// the SQL answer and a mismatch is logged and counted. This test builds the mismatch on
// purpose so the two routes' behaviour is written down and a reviewer does not have to
// reconstruct it.
func TestCheckBlockIsInCurrentChain_GapIDDivergesBetweenRoutes(t *testing.T) {
	const highID = 100000

	newStoreWithGap := func(t *testing.T, useInMemory bool) *SQL {
		t.Helper()
		s := newOnMainChainTestStoreWith(t, func(st *settings.Settings) {
			st.BlockChain.UseInMemoryChainCheck = useInMemory
			st.BlockChain.ChainCheckShadowCompare = false
		})
		_, _, err := s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)

		// Commit block2 under a high explicit id, leaving a large run of ids below
		// maxBlockID that belong to no block.
		committed, _, err := s.StoreBlock(context.Background(), block2, "", options.WithID(highID))
		require.NoError(t, err)
		require.Equal(t, uint64(highID), committed)

		return s
	}

	t.Run("SQL route rejects a gap id", func(t *testing.T) {
		s := newStoreWithGap(t, false)

		result, err := s.CheckBlockIsInCurrentChain(context.Background(), []uint32{highID - 1})
		require.NoError(t, err)
		require.False(t, result, "the authoritative route must reject an id with no committed row")
	})

	t.Run("forked-set route accepts a gap id", func(t *testing.T) {
		s := newStoreWithGap(t, true)

		result, err := s.CheckBlockIsInCurrentChain(context.Background(), []uint32{highID - 1})
		require.NoError(t, err)
		require.True(t, result, "absence from the forked set is taken as proof of main-chain membership")
	})

	t.Run("both routes accept the real committed id", func(t *testing.T) {
		for _, useInMemory := range []bool{false, true} {
			s := newStoreWithGap(t, useInMemory)

			result, err := s.CheckBlockIsInCurrentChain(context.Background(), []uint32{highID})
			require.NoError(t, err)
			require.True(t, result)
		}
	})
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

// TestCheckBlockIsInCurrentChain_InMemory_OnChainIDAnsweredFromMemory pins the
// forked-set fast path. An id that is at or below maxBlockID and absent from the
// off-chain (forked) set is on the main chain, and the store must say so with no
// database round trip at all. Proven by closing the database first: the answer
// still comes back true. Before this change the same call confirmed every
// candidate against on_main_chain and so errored on a closed database.
func TestCheckBlockIsInCurrentChain_InMemory_OnChainIDAnsweredFromMemory(t *testing.T) {
	s := newStoreWithInMemoryChainCheck(t)

	blockID, _, err := s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)

	s.Close(context.Background())

	result, err := s.CheckBlockIsInCurrentChain(context.Background(), []uint32{uint32(blockID)})
	require.NoError(t, err)
	require.True(t, result, "committed id absent from the forked set must be answered from memory")
}

// TestCheckBlockIsInCurrentChain_ShadowCompare_CountsDisagreement covers the soak
// instrument. While blockchain_chain_check_shadow_compare is on, every answer the
// forked-set route gives is also computed the authoritative way and the two are
// compared. The comparison exists to measure whether the two routes ever disagree
// on a live node, so it must count mismatches and must never change the answer.
func TestCheckBlockIsInCurrentChain_ShadowCompare_CountsDisagreement(t *testing.T) {
	const highID = 100000

	s := newOnMainChainTestStoreWith(t, func(st *settings.Settings) {
		st.BlockChain.UseInMemoryChainCheck = true
		st.BlockChain.ChainCheckShadowCompare = true
	})

	_, _, err := s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)

	committed, _, err := s.StoreBlock(context.Background(), block2, "", options.WithID(highID))
	require.NoError(t, err)
	require.Equal(t, uint64(highID), committed)

	// Both routes agree that the committed id is on the main chain.
	result, err := s.CheckBlockIsInCurrentChain(context.Background(), []uint32{highID})
	require.NoError(t, err)
	require.True(t, result)
	require.Zero(t, s.chainCheckShadowMismatches.Load(), "agreeing answers must not be counted")

	// A gap id is where they differ. The answer stays the in-memory one and the
	// mismatch is counted.
	result, err = s.CheckBlockIsInCurrentChain(context.Background(), []uint32{highID - 1})
	require.NoError(t, err)
	require.True(t, result, "the shadow comparison must not change the answer")
	require.Equal(t, uint64(1), s.chainCheckShadowMismatches.Load())
}

// TestCheckBlockIsInCurrentChain_ShadowCompare_SurvivesSQLFailure pins the other
// half of "must never change the answer". The shadow query is best effort: if it
// cannot run, the in-memory answer still stands. Anything else would make turning
// the instrument on strictly worse than leaving it off, which is the opposite of
// what a soak is for.
func TestCheckBlockIsInCurrentChain_ShadowCompare_SurvivesSQLFailure(t *testing.T) {
	s := newStoreWithInMemoryChainCheck(t)
	s.chainCheckShadowCompare = true

	blockID, _, err := s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)

	s.Close(context.Background())

	result, err := s.CheckBlockIsInCurrentChain(context.Background(), []uint32{uint32(blockID)})
	require.NoError(t, err, "a failed shadow query must not surface as an error")
	require.True(t, result)
}

// TestCheckBlockIsInCurrentChain_ShadowCompare_CountsEveryComparison makes a quiet
// soak readable. Zero mismatches means nothing on its own: it looks identical to a
// path that never ran. Counting the comparisons as well is what separates "the two
// routes agreed 40,000 times" from "the forked-set route was never reached".
func TestCheckBlockIsInCurrentChain_ShadowCompare_CountsEveryComparison(t *testing.T) {
	s := newOnMainChainTestStoreWith(t, func(st *settings.Settings) {
		st.BlockChain.UseInMemoryChainCheck = true
		st.BlockChain.ChainCheckShadowCompare = true
	})

	storeBlocks(t, s, block1, block2, block3)

	var block2ID uint32
	require.NoError(t, s.db.QueryRow(`SELECT id FROM blocks WHERE hash = $1`, block2.Hash()[:]).Scan(&block2ID))

	require.Zero(t, s.chainCheckShadowChecks.Load())

	for i := 0; i < 3; i++ {
		result, err := s.CheckBlockIsInCurrentChain(context.Background(), []uint32{block2ID})
		require.NoError(t, err)
		require.True(t, result)
	}

	require.Equal(t, uint64(3), s.chainCheckShadowChecks.Load(), "every agreeing comparison must be counted too")
	require.Zero(t, s.chainCheckShadowMismatches.Load())
}

// TestCheckBlockIsInCurrentChain_RebuildGuardSuppressesForkedSetRoute pins the
// invariant that every caller mutating on_main_chain leans on.
//
// maxBlockID is advanced before the forked set is rebuilt, so between the two a
// block that has just moved off the main chain is inside the id<=maxBlockID range
// and not yet in the forked set. The in-memory route reads "not forked" as "on the
// main chain", so it would answer true for a block that is not on it. What closes
// that window is mainChainRebuilding: while it is held, CheckBlockIsInCurrentChain
// must not answer from the forked set at all, and must go to the flag-free SQL.
//
// A gap id stands in for the mid-rebuild block here, because it is the input on
// which the two routes are known to disagree: guard clear it comes back true from
// the forked set, guard held it comes back false from SQL. Both halves are asserted,
// so removing the guard from CheckBlockIsInCurrentChain fails this test rather than
// silently widening the window StoreBlock, InvalidateBlock and RevalidateBlock all
// hold the guard to cover.
func TestCheckBlockIsInCurrentChain_RebuildGuardSuppressesForkedSetRoute(t *testing.T) {
	const highID = 100000

	s := newOnMainChainTestStoreWith(t, func(st *settings.Settings) {
		st.BlockChain.UseInMemoryChainCheck = true
		st.BlockChain.ChainCheckShadowCompare = false
	})

	_, _, err := s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)

	committed, _, err := s.StoreBlock(context.Background(), block2, "", options.WithID(highID))
	require.NoError(t, err)
	require.Equal(t, uint64(highID), committed)

	// Wait out any rebuild StoreBlock kicked off, so the guard state under test is
	// the one this test sets rather than a leftover.
	for s.mainChainRebuilding.Load() > 0 {
		time.Sleep(time.Millisecond)
	}

	// Guard clear: the forked-set route answers, and a gap id comes back true.
	result, err := s.CheckBlockIsInCurrentChain(context.Background(), []uint32{highID - 1})
	require.NoError(t, err)
	require.True(t, result, "precondition: with the guard clear the forked-set route answers")

	// Guard held: the forked-set route must be suppressed and SQL must answer.
	s.mainChainRebuilding.Add(1)
	defer s.mainChainRebuilding.Add(-1)

	result, err = s.CheckBlockIsInCurrentChain(context.Background(), []uint32{highID - 1})
	require.NoError(t, err)
	require.False(t, result, "while a rebuild is in flight the forked set must not be trusted")

	// A genuinely on-chain id is still on-chain under the guard: the guard suppresses
	// the fast path, it does not turn correct positives into invalidations.
	result, err = s.CheckBlockIsInCurrentChain(context.Background(), []uint32{highID})
	require.NoError(t, err)
	require.True(t, result)
}

// TestCheckBlockIsInCurrentChain_UnbuiltForkedSetDefersToSQL covers the failure the
// forked-set route is most exposed to, because it is the one the store already expects
// to happen: rebuildOffChainSet timing out on a cold cache during catchup.
//
// mainChainRebuilding is held across the startup rebuild, but it is released whether
// that rebuild succeeded or failed, and maxBlockID survives a failure because
// refreshMaxBlockID runs first and is a cheap index-only scan. So both of the earlier
// escapes in CheckBlockIsInCurrentChain pass while offChainBlockIDs is still the empty
// map New() made, and an empty forked set makes every committed id look on-chain.
//
// That is the false-positive direction this route introduced. A block that is genuinely
// off the chain must not come back on-chain just because the set failed to build, so the
// route has to defer to SQL until a rebuild has actually completed once.
func TestCheckBlockIsInCurrentChain_UnbuiltForkedSetDefersToSQL(t *testing.T) {
	s := newOnMainChainTestStoreWith(t, func(st *settings.Settings) {
		st.BlockChain.UseInMemoryChainCheck = true
		st.BlockChain.ChainCheckShadowCompare = false
	})

	blockID1, _, err := s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)

	blockID2, _, err := s.StoreBlock(context.Background(), block2, "")
	require.NoError(t, err)

	// Take block2 genuinely off the chain, so the two routes agree about it and the
	// only thing under test is whether the forked set is trusted.
	_, err = s.InvalidateBlock(context.Background(), block2.Header.Hash())
	require.NoError(t, err)

	for s.mainChainRebuilding.Load() > 0 {
		time.Sleep(time.Millisecond)
	}

	s.offChainBlockIDsMu.RLock()
	_, forked := s.offChainBlockIDs[uint32(blockID2)]
	s.offChainBlockIDsMu.RUnlock()
	require.True(t, forked, "precondition: the invalidated block must be in the forked set")

	// Reproduce a startup whose rebuild never completed: maxBlockID is populated, the
	// guard is clear, and the forked set is the empty map New() made.
	s.offChainBlockIDsMu.Lock()
	s.offChainBlockIDs = make(map[uint32]struct{})
	s.offChainBlockIDsMu.Unlock()
	s.lastSuccessfulRebuild.Store(0)

	require.Zero(t, s.mainChainRebuilding.Load(), "the guard must be clear for this to be the case under test")
	require.NotZero(t, s.maxBlockID.Load(), "maxBlockID survives a failed rebuild")

	// Absence from an unbuilt forked set is not proof of anything. SQL must answer, and
	// SQL knows the invalidated block is off the chain.
	result, err := s.CheckBlockIsInCurrentChain(context.Background(), []uint32{uint32(blockID2)})
	require.NoError(t, err)
	require.False(t, result, "an unbuilt forked set must not turn an off-chain block into an on-chain one")

	// The still-on-chain block is unaffected: deferring to SQL suppresses the fast path,
	// it does not turn correct positives into invalidations.
	result, err = s.CheckBlockIsInCurrentChain(context.Background(), []uint32{uint32(blockID1)})
	require.NoError(t, err)
	require.True(t, result)

	// Once a rebuild completes, the route is trusted again and still agrees with SQL.
	require.NoError(t, s.rebuildOffChainSet(context.Background()))
	require.NotZero(t, s.lastSuccessfulRebuild.Load(), "a completed rebuild must stamp its own success")

	result, err = s.CheckBlockIsInCurrentChain(context.Background(), []uint32{uint32(blockID2)})
	require.NoError(t, err)
	require.False(t, result)
}

// countingErrorLogger counts Errorf calls so a test can assert how much a path logs,
// not just what it computes. Everything else is TestLogger's.
type countingErrorLogger struct {
	ulogger.TestLogger

	errors atomic.Uint64
}

func (l *countingErrorLogger) Errorf(format string, args ...interface{}) {
	l.errors.Add(1)
}

// TestCheckBlockIsInCurrentChain_ShadowCompare_SamplesRepeatMismatches pins the third
// property of the soak instrument, next to "never changes the answer" and "a failed
// shadow query is not an error": a disagreement that repeats must not drown the node.
//
// A systematic mismatch fires on every call that touches the offending id. Unsampled,
// that is one error line per request, and this instrument defaults on. What must stay
// exact is the counter, because the two-minute totals line is what a soak is read from.
func TestCheckBlockIsInCurrentChain_ShadowCompare_SamplesRepeatMismatches(t *testing.T) {
	const (
		highID = 100000
		calls  = 60
	)

	s := newOnMainChainTestStoreWith(t, func(st *settings.Settings) {
		st.BlockChain.UseInMemoryChainCheck = true
		st.BlockChain.ChainCheckShadowCompare = true
	})

	_, _, err := s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)

	committed, _, err := s.StoreBlock(context.Background(), block2, "", options.WithID(highID))
	require.NoError(t, err)
	require.Equal(t, uint64(highID), committed)

	logger := &countingErrorLogger{}
	s.logger = logger

	// A gap id disagrees on every call, which is the systematic case.
	for i := 0; i < calls; i++ {
		result, err := s.CheckBlockIsInCurrentChain(context.Background(), []uint32{highID - 1})
		require.NoError(t, err)
		require.True(t, result, "sampling must not change the answer")
	}

	require.Equal(t, uint64(calls), s.chainCheckShadowMismatches.Load(), "every mismatch must be counted")
	require.Equal(t, uint64(10), logger.errors.Load(), "only the first ten mismatches may be logged at error")
}

func TestShouldLogShadowMismatch(t *testing.T) {
	for _, total := range []uint64{1, 2, 9, 10, 1000, 2000, 100000} {
		require.True(t, shouldLogShadowMismatch(total), "mismatch #%d must be logged", total)
	}

	for _, total := range []uint64{11, 12, 999, 1001, 99999} {
		require.False(t, shouldLogShadowMismatch(total), "mismatch #%d must be sampled out", total)
	}
}
