package sql

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckBlockIsInCurrentChain_EmptyBlockIDs(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

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
	defer s.Close()

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
	defer s.Close()

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
	defer s.Close()

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
	defer s.Close()

	blockID, _, err := s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// The off-chain set lookup is fully in-memory — cancelled context has no effect.
	result, err := s.CheckBlockIsInCurrentChain(ctx, []uint32{uint32(blockID)})
	assert.NoError(t, err)
	assert.True(t, result, "In-memory lookup should succeed even with cancelled context")
}

func TestCheckBlockIsInCurrentChain_InMemory_ClosedDB(t *testing.T) {
	s := newStoreWithInMemoryChainCheck(t)

	// Store a block so maxBlockID is > 0, then close
	blockID, _, err := s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)

	s.Close()

	// The off-chain set lookup is fully in-memory — closed DB has no effect.
	result, err := s.CheckBlockIsInCurrentChain(context.Background(), []uint32{uint32(blockID)})
	assert.NoError(t, err)
	assert.True(t, result, "In-memory lookup should succeed even with closed DB")
}

func TestCheckBlockIsInCurrentChain_MixedOnChainAndOffChain(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

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
	defer s.Close()

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
func waitForStartupRebuild(t *testing.T, s *SQL) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for s.mainChainRebuilding.Load() > 0 {
		if time.Now().After(deadline) {
			t.Fatal("startup rebuild did not complete within 5 seconds")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestCheckBlockIsInCurrentChain_InMemory_SingleBlockInChain(t *testing.T) {
	s := newStoreWithInMemoryChainCheck(t)
	defer s.Close()

	blockID, _, err := s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)

	result, err := s.CheckBlockIsInCurrentChain(context.Background(), []uint32{uint32(blockID)})
	require.NoError(t, err)
	assert.True(t, result, "Block in main chain should return true (in-memory path)")
}

func TestCheckBlockIsInCurrentChain_InMemory_MultipleBlocksInChain(t *testing.T) {
	s := newStoreWithInMemoryChainCheck(t)
	defer s.Close()

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
	defer s.Close()

	_, _, err := s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)

	result, err := s.CheckBlockIsInCurrentChain(context.Background(), []uint32{999999})
	require.NoError(t, err)
	assert.False(t, result, "Non-existent block IDs above maxBlockID should return false (in-memory path)")
}

func TestCheckBlockIsInCurrentChain_InMemory_MixedOnChainAndOffChain(t *testing.T) {
	s := newStoreWithInMemoryChainCheck(t)
	defer s.Close()

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
	defer s.Close()

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
	defer s.Close()

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
