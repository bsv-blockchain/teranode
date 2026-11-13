package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckBlockIsAncestorOfBlock_EmptyBlockIDs(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close(context.Background())

	// Store a block to have a valid hash
	_, _, err = s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)

	// Test with empty block IDs array
	result, err := s.CheckBlockIsAncestorOfBlock(context.Background(), []uint32{}, block1.Header.Hash())
	require.NoError(t, err)
	assert.False(t, result, "Empty block IDs should return false")
}

func TestCheckBlockIsAncestorOfBlock_SingleBlockIsAncestor(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close(context.Background())

	// Store blocks in sequence to build a chain
	blockID1, _, err := s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)

	_, _, err = s.StoreBlock(context.Background(), block2, "")
	require.NoError(t, err)

	_, _, err = s.StoreBlock(context.Background(), block3, "")
	require.NoError(t, err)

	// Check if block1 is an ancestor of block3
	result, err := s.CheckBlockIsAncestorOfBlock(context.Background(), []uint32{uint32(blockID1)}, block3.Header.Hash())
	require.NoError(t, err)
	assert.True(t, result, "Block1 should be an ancestor of block3")
}

func TestCheckBlockIsAncestorOfBlock_BlockIsNotAncestor(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close(context.Background())

	// Store blocks
	_, _, err = s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)

	blockID2, _, err := s.StoreBlock(context.Background(), block2, "")
	require.NoError(t, err)

	// Check if block2 is an ancestor of block1 (it's not - block2 comes after block1)
	result, err := s.CheckBlockIsAncestorOfBlock(context.Background(), []uint32{uint32(blockID2)}, block1.Header.Hash())
	require.NoError(t, err)
	assert.False(t, result, "Block2 should NOT be an ancestor of block1")
}

func TestCheckBlockIsAncestorOfBlock_NonExistentBlockID(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close(context.Background())

	// Store a block
	_, _, err = s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)

	// Check with a non-existent block ID
	nonExistentID := uint32(999999)
	result, err := s.CheckBlockIsAncestorOfBlock(context.Background(), []uint32{nonExistentID}, block1.Header.Hash())
	require.NoError(t, err)
	assert.False(t, result, "Non-existent block ID should return false")
}

func TestCheckBlockIsAncestorOfBlock_NonExistentBlockHash(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close(context.Background())

	// Store a block
	blockID1, _, err := s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)

	// Check with a non-existent block hash - should return error
	_, err = s.CheckBlockIsAncestorOfBlock(context.Background(), []uint32{uint32(blockID1)}, block2.Header.Hash())
	require.Error(t, err, "Non-existent block hash should return error")
	assert.Contains(t, err.Error(), "BLOCK_NOT_FOUND")
}

func TestCheckBlockIsAncestorOfBlock_MultipleBlockIDsOneIsAncestor(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close(context.Background())

	// Store blocks in sequence
	blockID1, _, err := s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)

	_, _, err = s.StoreBlock(context.Background(), block2, "")
	require.NoError(t, err)

	_, _, err = s.StoreBlock(context.Background(), block3, "")
	require.NoError(t, err)

	// Check with multiple IDs - one is an ancestor, one is not
	nonExistentID := uint32(999999)
	result, err := s.CheckBlockIsAncestorOfBlock(context.Background(), []uint32{uint32(blockID1), nonExistentID}, block3.Header.Hash())
	require.NoError(t, err)
	assert.True(t, result, "Should return true if ANY block ID is an ancestor")
}

func TestCheckBlockIsAncestorOfBlock_SameBlockAsTarget(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close(context.Background())

	// Store blocks
	_, _, err = s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)

	blockID2, _, err := s.StoreBlock(context.Background(), block2, "")
	require.NoError(t, err)

	// Check if block2 is an ancestor of itself (should be true - it's in its own ancestry path)
	result, err := s.CheckBlockIsAncestorOfBlock(context.Background(), []uint32{uint32(blockID2)}, block2.Header.Hash())
	require.NoError(t, err)
	assert.True(t, result, "Block should be in its own ancestry (includes self)")
}

func TestCheckBlockIsAncestorOfBlock_ContextCancellation(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close(context.Background())

	// Store blocks
	blockID1, _, err := s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)

	_, _, err = s.StoreBlock(context.Background(), block2, "")
	require.NoError(t, err)

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Test with cancelled context
	_, err = s.CheckBlockIsAncestorOfBlock(ctx, []uint32{uint32(blockID1)}, block2.Header.Hash())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "context canceled")
}

func TestCheckBlockIsAncestorOfBlock_ClosedDatabase(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	// Store a block first
	_, _, err = s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)

	// Close the database connection to simulate error
	s.Close(context.Background())

	// Test should fail when trying to access closed database
	result, err := s.CheckBlockIsAncestorOfBlock(context.Background(), []uint32{1}, block1.Header.Hash())
	assert.Error(t, err)
	assert.False(t, result)
	assert.Contains(t, err.Error(), "sql: database is closed")
}

// TestCheckBlockIsAncestorOfBlock_ForkPointFastPathMatchesWalk checks the fork-point fast path
// against the flag-free walk for every anchor and candidate on a chain with a two-block fork:
// genesis, block1, block2, block3 on the main chain, and blockAlternative2 then blockAlternative3
// on block1. With the guard clear and the flags right, the fast path must give the walk's answer
// for every pair, true and false. The pairs that tell a fork-point cap from a looser rule are a
// main-chain block above the fork point asked of a fork anchor (block2 asked of blockAlternative3:
// height 2 is at or below the anchor's height 3 but above the fork point's height 1), and a
// main-chain block above a main-chain anchor (block3 asked of block1).
func TestCheckBlockIsAncestorOfBlock_ForkPointFastPathMatchesWalk(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close(context.Background())

	ctx := context.Background()

	id1, _, err := s.StoreBlock(ctx, block1, "")
	require.NoError(t, err)
	id2, _, err := s.StoreBlock(ctx, block2, "")
	require.NoError(t, err)
	id3, _, err := s.StoreBlock(ctx, block3, "")
	require.NoError(t, err)
	idAlt2, _, err := s.StoreBlock(ctx, blockAlternative2, "")
	require.NoError(t, err)

	blockAlternative3 := createBlock3OnFork(blockAlternative2)
	idAlt3, _, err := s.StoreBlock(ctx, blockAlternative3, "")
	require.NoError(t, err)

	gapID := uint32(idAlt3 + 1000) // no blocks row

	anchors := map[string]*chainhash.Hash{
		"block1":            block1.Hash(),
		"block2":            block2.Hash(),
		"block3":            block3.Hash(),
		"blockAlternative2": blockAlternative2.Hash(),
		"blockAlternative3": blockAlternative3.Hash(),
	}
	candidates := map[string]uint32{
		"block1":            uint32(id1),
		"block2":            uint32(id2),
		"block3":            uint32(id3),
		"blockAlternative2": uint32(idAlt2),
		"blockAlternative3": uint32(idAlt3),
		"gap":               gapID,
	}

	// anchor>candidate: true when the candidate is the anchor or one of its ancestors.
	want := map[string]bool{
		"block1>block1": true,
		"block2>block1": true, "block2>block2": true,
		"block3>block1": true, "block3>block2": true, "block3>block3": true,
		"blockAlternative2>block1": true, "blockAlternative2>blockAlternative2": true,
		"blockAlternative3>block1": true, "blockAlternative3>blockAlternative2": true, "blockAlternative3>blockAlternative3": true,
	}

	for anchorName, anchor := range anchors {
		for candName, candID := range candidates {
			key := anchorName + ">" + candName

			fast, err := s.ancestorViaForkPoint(ctx, []uint32{candID}, anchor)
			require.NoError(t, err, key)

			mainProbe, err := s.ancestorOnMainChainBelowAnchor(ctx, []uint32{candID}, anchor)
			require.NoError(t, err, key)

			full, err := s.CheckBlockIsAncestorOfBlock(ctx, []uint32{candID}, anchor)
			require.NoError(t, err, key)

			s.mainChainRebuilding.Add(1)
			walk, err := s.CheckBlockIsAncestorOfBlock(ctx, []uint32{candID}, anchor)
			s.mainChainRebuilding.Add(-1)
			require.NoError(t, err, key)

			require.Equal(t, want[key], walk, "walk answer for %s", key)
			require.Equal(t, walk, full, "CheckBlockIsAncestorOfBlock differs from the walk for %s", key)
			require.Equal(t, walk, fast, "fork-point fast path differs from the walk for %s", key)

			if mainProbe {
				require.True(t, walk, "main-chain probe answered true where the walk says false for %s", key)
			}
		}
	}
}

// TestCheckBlockIsAncestorOfBlock_RebuildGuardSkipsFastPath pins the mainChainRebuilding guard on
// the fast path. While a rebuild or a fork-clear repair holds the guard, a fork row can still carry
// on_main_chain = true. Here blockAlternative2, on the fork from block1, is flagged true by hand and
// asked of block3, and block2 is asked of blockAlternative3, the fork block on blockAlternative2.
// The probe alone is fooled both times, so the guard is what keeps the answers false.
func TestCheckBlockIsAncestorOfBlock_RebuildGuardSkipsFastPath(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close(context.Background())

	ctx := context.Background()

	_, _, err = s.StoreBlock(ctx, block1, "")
	require.NoError(t, err)
	id2, _, err := s.StoreBlock(ctx, block2, "")
	require.NoError(t, err)
	_, _, err = s.StoreBlock(ctx, block3, "")
	require.NoError(t, err)
	idAlt2, _, err := s.StoreBlock(ctx, blockAlternative2, "")
	require.NoError(t, err)

	blockAlternative3 := createBlock3OnFork(blockAlternative2)
	_, _, err = s.StoreBlock(ctx, blockAlternative3, "")
	require.NoError(t, err)

	_, err = s.db.ExecContext(ctx, `UPDATE blocks SET on_main_chain = true WHERE id = $1`, idAlt2)
	require.NoError(t, err)

	fooled, err := s.ancestorViaForkPoint(ctx, []uint32{uint32(idAlt2)}, block3.Hash())
	require.NoError(t, err)
	require.True(t, fooled, "precondition: the stale flag makes the probe answer true")

	// A fork anchor is fooled the same way: the walk from blockAlternative3 stops at the stale
	// blockAlternative2, so block2 looks to be at or below the fork point.
	foolingForkAnchor, err := s.ancestorViaForkPoint(ctx, []uint32{uint32(id2)}, blockAlternative3.Hash())
	require.NoError(t, err)
	require.True(t, foolingForkAnchor, "precondition: the stale flag fools the fork-anchored probe too")

	s.mainChainRebuilding.Add(1)
	defer s.mainChainRebuilding.Add(-1)

	isAncestor, err := s.CheckBlockIsAncestorOfBlock(ctx, []uint32{uint32(idAlt2)}, block3.Hash())
	require.NoError(t, err)
	require.False(t, isAncestor, "a fork block was answered as an ancestor of block3 while the rebuild guard was held")

	isAncestor, err = s.CheckBlockIsAncestorOfBlock(ctx, []uint32{uint32(id2)}, blockAlternative3.Hash())
	require.NoError(t, err)
	require.False(t, isAncestor, "a main-chain block above the fork point was answered as an ancestor of blockAlternative3 while the rebuild guard was held")
}
