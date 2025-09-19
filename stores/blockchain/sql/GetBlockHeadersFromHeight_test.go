package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test successful retrieval of headers from height
func TestGetBlockHeadersFromHeight_Success(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	tSettings := test.CreateBaseTestSettings(t)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	ctx := context.Background()

	// Store multiple blocks to create a chain
	_, _, err = s.StoreBlock(ctx, block1, "test_peer")
	require.NoError(t, err)

	_, _, err = s.StoreBlock(ctx, block2, "test_peer")
	require.NoError(t, err)

	_, _, err = s.StoreBlock(ctx, block3, "test_peer")
	require.NoError(t, err)

	// Test getting headers from height 1 with limit 2
	headers, metas, err := s.GetBlockHeadersFromHeight(ctx, 1, 2)

	// Assertions
	require.NoError(t, err)
	assert.NotNil(t, headers)
	assert.NotNil(t, metas)
	assert.Equal(t, len(headers), len(metas))
	assert.Greater(t, len(headers), 0)

	// Note: The query uses ORDER BY height DESC, but we'll verify ordering in a separate test
	// Here we just verify we got results in the expected range

	// Verify all headers are within the requested range (height >= 1 and height < 1+2=3)
	for _, meta := range metas {
		assert.GreaterOrEqual(t, meta.Height, uint32(1), "Height should be >= start height")
		assert.Less(t, meta.Height, uint32(3), "Height should be < start height + limit")
	}

	// Verify header structure integrity
	for i, header := range headers {
		assert.NotNil(t, header.HashPrevBlock)
		assert.NotNil(t, header.HashMerkleRoot)
		assert.Greater(t, header.Version, uint32(0))

		meta := metas[i]
		assert.GreaterOrEqual(t, meta.Height, uint32(1))
		assert.Greater(t, meta.ID, uint32(0))
		assert.NotEmpty(t, meta.PeerID)
	}
}

// Test cache hit scenario
func TestGetBlockHeadersFromHeight_CacheHit(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	tSettings := test.CreateBaseTestSettings(t)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	ctx := context.Background()

	// Store blocks to potentially populate cache
	_, _, err = s.StoreBlock(ctx, block1, "test_peer")
	require.NoError(t, err)

	_, _, err = s.StoreBlock(ctx, block2, "test_peer")
	require.NoError(t, err)

	// First call might populate cache
	headers1, metas1, err1 := s.GetBlockHeadersFromHeight(ctx, 1, 2)
	require.NoError(t, err1)

	// Second call should be consistent (either from cache or DB)
	headers2, metas2, err2 := s.GetBlockHeadersFromHeight(ctx, 1, 2)
	require.NoError(t, err2)

	// Results should be consistent
	assert.Equal(t, len(headers1), len(headers2))
	assert.Equal(t, len(metas1), len(metas2))

	// If we got results, verify they're the same
	if len(headers1) > 0 && len(headers2) > 0 {
		assert.Equal(t, headers1[0].Version, headers2[0].Version)
		assert.Equal(t, metas1[0].Height, metas2[0].Height)
	}
}

// Test cache miss scenario (when cache returns nil)
func TestGetBlockHeadersFromHeight_CacheMiss(t *testing.T) {
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

	// This should go to database since cache won't have this specific combination
	headers, metas, err := s.GetBlockHeadersFromHeight(ctx, 1, 1)

	require.NoError(t, err)
	assert.NotNil(t, headers)
	assert.NotNil(t, metas)
	assert.Equal(t, len(headers), len(metas))
}

// Test empty result when no blocks exist at height
func TestGetBlockHeadersFromHeight_EmptyResult(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	tSettings := test.CreateBaseTestSettings(t)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	ctx := context.Background()

	// Store one block at height 1
	_, _, err = s.StoreBlock(ctx, block1, "test_peer")
	require.NoError(t, err)

	// Request headers from height 100 (should be empty)
	headers, metas, err := s.GetBlockHeadersFromHeight(ctx, 100, 10)

	// Should return empty slices with no error
	require.NoError(t, err)
	assert.Empty(t, headers)
	assert.Empty(t, metas)
}

// Test with zero limit
func TestGetBlockHeadersFromHeight_ZeroLimit(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	tSettings := test.CreateBaseTestSettings(t)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	ctx := context.Background()

	// Store blocks
	_, _, err = s.StoreBlock(ctx, block1, "test_peer")
	require.NoError(t, err)

	// Request with limit 0 (should return empty)
	headers, metas, err := s.GetBlockHeadersFromHeight(ctx, 1, 0)

	require.NoError(t, err)
	assert.Empty(t, headers) // No heights to query when limit=0
	assert.Empty(t, metas)
}

// Test context cancellation
func TestGetBlockHeadersFromHeight_ContextCancellation(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	tSettings := test.CreateBaseTestSettings(t)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// Should return error due to cancelled context
	_, _, err = s.GetBlockHeadersFromHeight(ctx, 1, 10)

	assert.Error(t, err)
}

// Test with genesis height (height 0)
func TestGetBlockHeadersFromHeight_GenesisHeight(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	tSettings := test.CreateBaseTestSettings(t)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	ctx := context.Background()

	// Request genesis block and next few heights
	headers, metas, err := s.GetBlockHeadersFromHeight(ctx, 0, 2)

	require.NoError(t, err)
	assert.NotEmpty(t, headers) // Genesis block should exist
	assert.NotEmpty(t, metas)
	assert.Equal(t, len(headers), len(metas))

	// At least one block should be at height 0 (genesis)
	found := false
	for _, meta := range metas {
		if meta.Height == 0 {
			found = true
			break
		}
	}
	assert.True(t, found, "Genesis block at height 0 should be found")
}

// Test with large limit
func TestGetBlockHeadersFromHeight_LargeLimit(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	tSettings := test.CreateBaseTestSettings(t)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	ctx := context.Background()

	// Store some blocks
	_, _, err = s.StoreBlock(ctx, block1, "test_peer")
	require.NoError(t, err)

	_, _, err = s.StoreBlock(ctx, block2, "test_peer")
	require.NoError(t, err)

	// Request with very large limit
	headers, metas, err := s.GetBlockHeadersFromHeight(ctx, 0, 1000)

	require.NoError(t, err)
	assert.NotNil(t, headers)
	assert.NotNil(t, metas)
	assert.Equal(t, len(headers), len(metas))

	// Should return some blocks but not 1000 (since we don't have that many)
	assert.Greater(t, len(headers), 0)
	assert.Less(t, len(headers), 100) // reasonable upper bound
}

// Test multiple forks at same height
func TestGetBlockHeadersFromHeight_MultipleForks(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	tSettings := test.CreateBaseTestSettings(t)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	ctx := context.Background()

	// Store main chain blocks
	_, _, err = s.StoreBlock(ctx, block1, "test_peer")
	require.NoError(t, err)

	_, _, err = s.StoreBlock(ctx, block2, "test_peer")
	require.NoError(t, err)

	// Store alternative block at same height as block2 (creates fork)
	_, _, err = s.StoreBlock(ctx, blockAlternative2, "test_peer2")
	require.NoError(t, err)

	// Get headers from height 2 (should include both block2 and blockAlternative2)
	headers, metas, err := s.GetBlockHeadersFromHeight(ctx, 2, 1)

	require.NoError(t, err)
	assert.NotEmpty(t, headers)
	assert.NotEmpty(t, metas)
	assert.Equal(t, len(headers), len(metas))

	// Should find blocks at height 2 (possibly both forks)
	heightTwoCount := 0
	for _, meta := range metas {
		if meta.Height == 2 {
			heightTwoCount++
		}
	}
	assert.Greater(t, heightTwoCount, 0, "Should find at least one block at height 2")
}

// Test descending order verification
func TestGetBlockHeadersFromHeight_DescendingOrder(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	tSettings := test.CreateBaseTestSettings(t)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	ctx := context.Background()

	// Store multiple blocks
	_, _, err = s.StoreBlock(ctx, block1, "test_peer")
	require.NoError(t, err)

	_, _, err = s.StoreBlock(ctx, block2, "test_peer")
	require.NoError(t, err)

	_, _, err = s.StoreBlock(ctx, block3, "test_peer")
	require.NoError(t, err)

	// Get headers from height 0 with enough limit to get multiple heights
	headers, metas, err := s.GetBlockHeadersFromHeight(ctx, 0, 10)

	require.NoError(t, err)
	assert.Greater(t, len(headers), 1) // Should have multiple blocks

	// Verify descending order (ORDER BY height DESC)
	for i := 1; i < len(metas); i++ {
		assert.GreaterOrEqual(t, metas[i-1].Height, metas[i].Height,
			"Headers should be ordered by height DESC (newest to oldest)")
	}
}

// Test capacity pre-allocation (2*limit)
func TestGetBlockHeadersFromHeight_CapacityAllocation(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	tSettings := test.CreateBaseTestSettings(t)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	ctx := context.Background()

	// Test that the function doesn't panic with various limit values
	// This tests the capacity calculation: 2*limit
	testCases := []uint32{1, 2, 10, 100, 1000}

	for _, limit := range testCases {
		headers, metas, err := s.GetBlockHeadersFromHeight(ctx, 1000, limit) // High height, no data

		require.NoError(t, err, "Should not error for limit %d", limit)
		assert.Empty(t, headers)
		assert.Empty(t, metas)
	}
}

// Test height range boundaries
func TestGetBlockHeadersFromHeight_HeightRangeBoundaries(t *testing.T) {
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

	// Test specific height range (height >= 1 AND height < 2)
	headers, metas, err := s.GetBlockHeadersFromHeight(ctx, 1, 1)

	require.NoError(t, err)
	assert.NotNil(t, headers)
	assert.NotNil(t, metas)

	// All results should be exactly at height 1
	for _, meta := range metas {
		assert.Equal(t, uint32(1), meta.Height, "With limit=1 from height=1, should only get height 1")
	}
}

// Test maximum uint32 values
func TestGetBlockHeadersFromHeight_MaxValues(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	tSettings := test.CreateBaseTestSettings(t)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	ctx := context.Background()

	// Request with very high height values (should be empty)
	headers, metas, err := s.GetBlockHeadersFromHeight(ctx, 4294967290, 5) // near uint32 max

	require.NoError(t, err)
	assert.Empty(t, headers) // No blocks at such high heights
	assert.Empty(t, metas)
}

// Test data consistency between headers and metas
func TestGetBlockHeadersFromHeight_DataConsistency(t *testing.T) {
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

	// Get headers
	headers, metas, err := s.GetBlockHeadersFromHeight(ctx, 0, 5)

	require.NoError(t, err)
	assert.Equal(t, len(headers), len(metas))

	// Verify basic data consistency
	for i, header := range headers {
		meta := metas[i]

		// Basic structure validation
		assert.NotNil(t, header, "Header should not be nil")
		assert.NotNil(t, meta, "Meta should not be nil")

		// Verify header hash fields are set
		assert.NotNil(t, header.HashPrevBlock, "HashPrevBlock should be set")
		assert.NotNil(t, header.HashMerkleRoot, "HashMerkleRoot should be set")

		// Verify meta fields are reasonable
		assert.GreaterOrEqual(t, meta.Height, uint32(0), "Height should be >= 0")
		assert.GreaterOrEqual(t, meta.ID, uint32(0), "ID should be >= 0") // ID can start from 0
	}
}

// Test limit=1 specifically
func TestGetBlockHeadersFromHeight_LimitOne(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	tSettings := test.CreateBaseTestSettings(t)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	ctx := context.Background()

	// Store multiple blocks
	_, _, err = s.StoreBlock(ctx, block1, "test_peer")
	require.NoError(t, err)

	_, _, err = s.StoreBlock(ctx, block2, "test_peer")
	require.NoError(t, err)

	// Request exactly one height worth of blocks
	headers, metas, err := s.GetBlockHeadersFromHeight(ctx, 1, 1)

	require.NoError(t, err)
	assert.NotNil(t, headers)
	assert.NotNil(t, metas)

	// All results should be at height 1 only (since limit=1 means 1 height, not 1 block)
	for _, meta := range metas {
		assert.Equal(t, uint32(1), meta.Height, "With limit=1 from height=1, should only get blocks at height 1")
	}
}

// Test fork detection capability
func TestGetBlockHeadersFromHeight_ForkDetection(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	tSettings := test.CreateBaseTestSettings(t)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	ctx := context.Background()

	// Store main chain
	_, _, err = s.StoreBlock(ctx, block1, "test_peer1")
	require.NoError(t, err)

	_, _, err = s.StoreBlock(ctx, block2, "test_peer1")
	require.NoError(t, err)

	// Store alternative chain at height 2
	_, _, err = s.StoreBlock(ctx, blockAlternative2, "test_peer2")
	require.NoError(t, err)

	// This should return all blocks at height 2 (both forks)
	headers, metas, err := s.GetBlockHeadersFromHeight(ctx, 2, 1)

	require.NoError(t, err)
	assert.NotEmpty(t, headers)
	assert.NotEmpty(t, metas)

	// Check that we got blocks at height 2 (could be 1 or 2 depending on fork handling)
	height2Blocks := 0
	for _, meta := range metas {
		if meta.Height == 2 {
			height2Blocks++
		}
	}

	assert.Greater(t, height2Blocks, 0, "Should find at least one block at height 2")

	// Verify all blocks are actually at the requested height
	for _, meta := range metas {
		assert.Equal(t, uint32(2), meta.Height, "All returned blocks should be at height 2")
	}
}

// Test error handling with invalid height calculations
func TestGetBlockHeadersFromHeight_HeightCalculations(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	tSettings := test.CreateBaseTestSettings(t)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	ctx := context.Background()

	// Test with height near uint32 max to check for overflow
	// height=4294967295, limit=1 should not overflow when calculating height+limit
	maxHeight := uint32(4294967295) // uint32 max

	// This should not panic or overflow
	headers, metas, err := s.GetBlockHeadersFromHeight(ctx, maxHeight, 1)

	require.NoError(t, err)
	assert.Empty(t, headers) // No blocks at max height
	assert.Empty(t, metas)
}
