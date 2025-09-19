package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper function to create test hash
func createTestBlockHash(s string) *chainhash.Hash {
	hash := chainhash.DoubleHashH([]byte(s))
	return &hash
}

// Test successful database query with no cache hit
func TestGetForkedBlockHeaders_DatabaseSuccess(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	tSettings := test.CreateBaseTestSettings(t)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	// Store some test blocks to create forks
	ctx := context.Background()

	// Store block1 (this will be our main chain block)
	_, _, err = s.StoreBlock(ctx, block1, "test_peer")
	require.NoError(t, err)

	// Store block2 (this will be our fork block)
	_, _, err = s.StoreBlock(ctx, block2, "test_peer")
	require.NoError(t, err)

	// Test with block1's hash (should return forked headers not in block1's chain)
	block1Hash := block1.Header.Hash()
	headers, metas, err := s.GetForkedBlockHeaders(ctx, block1Hash, 10)

	// Assertions
	require.NoError(t, err)
	assert.NotNil(t, headers)
	assert.NotNil(t, metas)
	assert.Len(t, headers, len(metas))
}

// Test when hash doesn't exist - should return all blocks since none are in the nonexistent chain
func TestGetForkedBlockHeaders_NonExistentHash(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	tSettings := test.CreateBaseTestSettings(t)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	// Use a hash that doesn't exist in database - the recursive CTE will return no blocks,
	// so all blocks in the database will be considered "forked" from this non-existent chain
	nonExistentHash := createTestBlockHash("non-existent")

	headers, metas, err := s.GetForkedBlockHeaders(context.Background(), nonExistentHash, 10)

	// Should return the genesis block and no error (since genesis is not part of non-existent chain)
	require.NoError(t, err)
	assert.NotEmpty(t, headers) // Will contain genesis block
	assert.NotEmpty(t, metas)
	assert.Equal(t, len(headers), len(metas))
}

// Test context cancellation
func TestGetForkedBlockHeaders_ContextCancellation(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	tSettings := test.CreateBaseTestSettings(t)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	testHash := createTestBlockHash("test")

	_, _, err = s.GetForkedBlockHeaders(ctx, testHash, 10)

	// Should return an error due to cancelled context
	assert.Error(t, err)
}

// Test empty result set (different from ErrNoRows - when query succeeds but returns no rows)
func TestGetForkedBlockHeaders_EmptyResultSet(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	tSettings := test.CreateBaseTestSettings(t)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	// Store a single block - this means there are no forked blocks
	ctx := context.Background()
	_, _, err = s.StoreBlock(ctx, block1, "test_peer")
	require.NoError(t, err)

	// Query for forked headers - should return empty since there's only one chain
	block1Hash := block1.Header.Hash()
	headers, metas, err := s.GetForkedBlockHeaders(ctx, block1Hash, 10)

	// Should return empty slices and no error
	require.NoError(t, err)
	assert.Empty(t, headers)
	assert.Empty(t, metas)
}

// Test with multiple forked headers
func TestGetForkedBlockHeaders_MultipleForks(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	tSettings := test.CreateBaseTestSettings(t)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	ctx := context.Background()

	// Store multiple blocks to create different chains
	_, _, err = s.StoreBlock(ctx, block1, "test_peer")
	require.NoError(t, err)

	_, _, err = s.StoreBlock(ctx, block2, "test_peer")
	require.NoError(t, err)

	_, _, err = s.StoreBlock(ctx, block3, "test_peer")
	require.NoError(t, err)

	// Get forked headers from block1's perspective
	block1Hash := block1.Header.Hash()
	headers, metas, err := s.GetForkedBlockHeaders(ctx, block1Hash, 10)

	require.NoError(t, err)
	assert.NotNil(t, headers)
	assert.NotNil(t, metas)
	assert.Equal(t, len(headers), len(metas))

	// Verify headers and metas have valid data
	for i, header := range headers {
		assert.NotNil(t, header.HashPrevBlock)
		assert.NotNil(t, header.HashMerkleRoot)
		assert.Greater(t, header.Version, uint32(0))

		meta := metas[i]
		assert.Greater(t, meta.Height, uint32(0))
		assert.Greater(t, meta.ID, uint32(0))
	}
}

// Test limiting number of headers
func TestGetForkedBlockHeaders_LimitHeaders(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	tSettings := test.CreateBaseTestSettings(t)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	ctx := context.Background()

	// Store test blocks
	_, _, err = s.StoreBlock(ctx, block1, "test_peer")
	require.NoError(t, err)

	_, _, err = s.StoreBlock(ctx, block2, "test_peer")
	require.NoError(t, err)

	// Test with limit of 1
	block1Hash := block1.Header.Hash()
	headers, metas, err := s.GetForkedBlockHeaders(ctx, block1Hash, 1)

	require.NoError(t, err)
	assert.NotNil(t, headers)
	assert.NotNil(t, metas)

	// Should respect the limit
	assert.LessOrEqual(t, len(headers), 1)
	assert.Equal(t, len(headers), len(metas))
}

// Test with zero limit
func TestGetForkedBlockHeaders_ZeroLimit(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	tSettings := test.CreateBaseTestSettings(t)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	testHash := createTestBlockHash("test")

	headers, metas, err := s.GetForkedBlockHeaders(context.Background(), testHash, 0)

	require.NoError(t, err)
	assert.Empty(t, headers)
	assert.Empty(t, metas)
}

// Test cache hit scenario by using a filled cache
func TestGetForkedBlockHeaders_CacheHit(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	tSettings := test.CreateBaseTestSettings(t)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	ctx := context.Background()

	// Store blocks to populate cache
	_, _, err = s.StoreBlock(ctx, block1, "test_peer")
	require.NoError(t, err)

	_, _, err = s.StoreBlock(ctx, block2, "test_peer")
	require.NoError(t, err)

	// First call to populate cache
	block1Hash := block1.Header.Hash()
	headers1, metas1, err1 := s.GetForkedBlockHeaders(ctx, block1Hash, 10)
	require.NoError(t, err1)

	// Second call should hit cache (if it exists) or return same result from DB
	headers2, metas2, err2 := s.GetForkedBlockHeaders(ctx, block1Hash, 10)
	require.NoError(t, err2)

	// Results should be consistent
	assert.Equal(t, len(headers1), len(headers2))
	assert.Equal(t, len(metas1), len(metas2))
}

// Test cache miss by using cache directly from fresh store
func TestGetForkedBlockHeaders_CacheMiss(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	tSettings := test.CreateBaseTestSettings(t)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	ctx := context.Background()

	// Store blocks
	_, _, err = s.StoreBlock(ctx, block1, "test_peer")
	require.NoError(t, err)

	_, _, err = s.StoreBlock(ctx, block2, "test_peer")
	require.NoError(t, err)

	// This should go directly to database since cache won't have the specific hash/limit combo
	block1Hash := block1.Header.Hash()
	headers, metas, err := s.GetForkedBlockHeaders(ctx, block1Hash, 10)

	require.NoError(t, err)
	assert.NotNil(t, headers)
	assert.NotNil(t, metas)
	assert.Equal(t, len(headers), len(metas))
}

// Test with alternative fork scenario
func TestGetForkedBlockHeaders_AlternativeFork(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	tSettings := test.CreateBaseTestSettings(t)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	ctx := context.Background()

	// Store the main chain blocks
	_, _, err = s.StoreBlock(ctx, block1, "test_peer")
	require.NoError(t, err)

	_, _, err = s.StoreBlock(ctx, block2, "test_peer")
	require.NoError(t, err)

	// Store an alternative block at same height as block2
	_, _, err = s.StoreBlock(ctx, blockAlternative2, "test_peer2")
	require.NoError(t, err)

	// Get forked headers from block2's perspective
	block2Hash := block2.Header.Hash()
	headers, metas, err := s.GetForkedBlockHeaders(ctx, block2Hash, 10)

	require.NoError(t, err)
	// Should find blockAlternative2 as it's not in block2's chain
	assert.NotNil(t, headers)
	assert.NotNil(t, metas)
}
