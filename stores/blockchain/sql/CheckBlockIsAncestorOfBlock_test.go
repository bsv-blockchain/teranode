package sql

import (
	"context"
	"net/url"
	"testing"

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
	defer s.Close()

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
	defer s.Close()

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
	defer s.Close()

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
	defer s.Close()

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
	defer s.Close()

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
	defer s.Close()

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
	defer s.Close()

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
	defer s.Close()

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
	s.Close()

	// Test should fail when trying to access closed database
	result, err := s.CheckBlockIsAncestorOfBlock(context.Background(), []uint32{1}, block1.Header.Hash())
	assert.Error(t, err)
	assert.False(t, result)
	assert.Contains(t, err.Error(), "sql: database is closed")
}
