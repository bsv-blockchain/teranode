package catchup

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createValidChain creates a valid chain of headers where each header's
// HashPrevBlock correctly references the previous header's Hash()
func createValidChain(t *testing.T, length int) []*model.BlockHeader {
	headers := make([]*model.BlockHeader, length)

	// Create NBit from bytes
	nBits, err := model.NewNBitFromSlice([]byte{0x1d, 0x00, 0xff, 0xff})
	require.NoError(t, err)

	// Create genesis block
	zeroHash := &chainhash.Hash{}
	headers[0] = &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  zeroHash,
		HashMerkleRoot: randomHash(),
		Timestamp:      1000,
		Bits:           *nBits,
		Nonce:          1,
	}

	// Create subsequent blocks with proper chain links
	for i := 1; i < length; i++ {
		headers[i] = &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  headers[i-1].Hash(), // Correctly reference previous
			HashMerkleRoot: randomHash(),
			Timestamp:      uint32(1000 + i),
			Bits:           *nBits,
			Nonce:          uint32(i),
		}
	}

	return headers
}

// createBrokenChain creates a chain with an invalid link at the specified position
func createBrokenChain(t *testing.T, length int, breakAt int) []*model.BlockHeader {
	headers := createValidChain(t, length)

	if breakAt > 0 && breakAt < length {
		// Break the chain by setting an invalid HashPrevBlock
		headers[breakAt].HashPrevBlock = randomHash()
	}

	return headers
}

func TestHeaderChainCache_BuildFromHeaders_ValidChain(t *testing.T) {
	logger := ulogger.TestLogger{}
	cache := NewHeaderChainCache(logger)

	// Test with valid chain
	headers := createValidChain(t, 10)
	err := cache.BuildFromHeaders(headers, 5)
	assert.NoError(t, err)

	// Verify cache was built
	assert.Equal(t, 10, cache.GetChainLength())

	// Verify all blocks are indexed
	for _, header := range headers {
		assert.True(t, cache.HasBlock(header.Hash()))
	}
}

func TestHeaderChainCache_BuildFromHeaders_BrokenChain(t *testing.T) {
	logger := ulogger.TestLogger{}
	cache := NewHeaderChainCache(logger)

	// Test with broken chain at position 5
	headers := createBrokenChain(t, 10, 5)
	err := cache.BuildFromHeaders(headers, 5)

	// Should fail with chain validation error
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "chain broken at position 5")

	// Cache should remain empty after failed build
	assert.Equal(t, 0, cache.GetChainLength())
}

func TestHeaderChainCache_BuildFromHeaders_EmptyHeaders(t *testing.T) {
	logger := ulogger.TestLogger{}
	cache := NewHeaderChainCache(logger)

	// Empty headers should be valid
	err := cache.BuildFromHeaders([]*model.BlockHeader{}, 5)
	assert.NoError(t, err)
	assert.Equal(t, 0, cache.GetChainLength())
}

func TestHeaderChainCache_GetValidationHeaders(t *testing.T) {
	logger := ulogger.TestLogger{}
	cache := NewHeaderChainCache(logger)

	// Build cache with 20 headers, requesting 5 previous headers for validation
	headers := createValidChain(t, 20)
	err := cache.BuildFromHeaders(headers, 5)
	require.NoError(t, err)

	// Test getting validation headers for block at position 10
	validationHeaders, found := cache.GetValidationHeaders(headers[10].Hash())
	assert.True(t, found)
	assert.Equal(t, 5, len(validationHeaders))

	// Verify headers are in reverse order (newest first)
	// Should be blocks 9, 8, 7, 6, 5
	assert.Equal(t, headers[9].Hash(), validationHeaders[0].Hash())
	assert.Equal(t, headers[8].Hash(), validationHeaders[1].Hash())
	assert.Equal(t, headers[7].Hash(), validationHeaders[2].Hash())
	assert.Equal(t, headers[6].Hash(), validationHeaders[3].Hash())
	assert.Equal(t, headers[5].Hash(), validationHeaders[4].Hash())
}

func TestHeaderChainCache_GetValidationHeaders_FirstBlock(t *testing.T) {
	logger := ulogger.TestLogger{}
	cache := NewHeaderChainCache(logger)

	headers := createValidChain(t, 10)
	err := cache.BuildFromHeaders(headers, 5)
	require.NoError(t, err)

	// First block should have no validation headers
	validationHeaders, found := cache.GetValidationHeaders(headers[0].Hash())
	assert.True(t, found)
	assert.Empty(t, validationHeaders)
}

func TestHeaderChainCache_GetValidationHeaders_NotInCache(t *testing.T) {
	logger := ulogger.TestLogger{}
	cache := NewHeaderChainCache(logger)

	headers := createValidChain(t, 10)
	err := cache.BuildFromHeaders(headers, 5)
	require.NoError(t, err)

	// Try to get headers for a block not in cache
	randomBlockHash := randomHash()
	validationHeaders, found := cache.GetValidationHeaders(randomBlockHash)
	assert.False(t, found)
	assert.Nil(t, validationHeaders)
}

func TestHeaderChainCache_VerifyChainConnection(t *testing.T) {
	logger := ulogger.TestLogger{}
	cache := NewHeaderChainCache(logger)

	headers := createValidChain(t, 10)
	err := cache.BuildFromHeaders(headers, 5)
	require.NoError(t, err)

	// Create NBit from bytes
	nBits, err := model.NewNBitFromSlice([]byte{0x1d, 0x00, 0xff, 0xff})
	require.NoError(t, err)

	// Create a new header that properly connects to the chain
	newHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  headers[9].Hash(), // Connect to last header
		HashMerkleRoot: randomHash(),
		Timestamp:      2000,
		Bits:           *nBits,
		Nonce:          999,
	}

	// Should verify successfully
	err = cache.VerifyChainConnection(newHeader)
	assert.NoError(t, err)
}

func TestHeaderChainCache_VerifyChainConnection_InvalidPrevious(t *testing.T) {
	logger := ulogger.TestLogger{}
	cache := NewHeaderChainCache(logger)

	headers := createValidChain(t, 10)
	err := cache.BuildFromHeaders(headers, 5)
	require.NoError(t, err)

	// Create NBit from bytes
	nBits, err := model.NewNBitFromSlice([]byte{0x1d, 0x00, 0xff, 0xff})
	require.NoError(t, err)

	// Create a new header with invalid previous block reference
	newHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  randomHash(), // Random hash not in chain
		HashMerkleRoot: randomHash(),
		Timestamp:      2000,
		Bits:           *nBits,
		Nonce:          999,
	}

	// Should fail verification
	err = cache.VerifyChainConnection(newHeader)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "previous block")
	assert.Contains(t, err.Error(), "not found in chain")
}

func TestHeaderChainCache_VerifyChainConnection_NilHeader(t *testing.T) {
	logger := ulogger.TestLogger{}
	cache := NewHeaderChainCache(logger)

	headers := createValidChain(t, 10)
	err := cache.BuildFromHeaders(headers, 5)
	require.NoError(t, err)

	// Test with nil header - should return error, not panic
	err = cache.VerifyChainConnection(nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "header cannot be nil")
}

func TestHeaderChainCache_VerifyChainConnection_NilHashPrevBlock(t *testing.T) {
	logger := ulogger.TestLogger{}
	cache := NewHeaderChainCache(logger)

	headers := createValidChain(t, 10)
	err := cache.BuildFromHeaders(headers, 5)
	require.NoError(t, err)

	// Create NBit from bytes
	nBits, err := model.NewNBitFromSlice([]byte{0x1d, 0x00, 0xff, 0xff})
	require.NoError(t, err)

	// Create a header with nil HashPrevBlock
	newHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  nil, // Nil previous block pointer
		HashMerkleRoot: randomHash(),
		Timestamp:      2000,
		Bits:           *nBits,
		Nonce:          999,
	}

	// Test with nil HashPrevBlock - should return error, not panic
	err = cache.VerifyChainConnection(newHeader)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "HashPrevBlock cannot be nil")
}

func TestHeaderChainCache_ChainWithReorg(t *testing.T) {
	logger := ulogger.TestLogger{}
	cache := NewHeaderChainCache(logger)

	// Create main chain
	mainChain := createValidChain(t, 10)

	// Create a fork starting at block 5
	// This simulates a chain reorganization scenario
	forkChain := make([]*model.BlockHeader, 8)
	copy(forkChain[:5], mainChain[:5]) // Copy first 5 blocks

	// Create NBit from bytes
	nBits, err := model.NewNBitFromSlice([]byte{0x1d, 0x00, 0xff, 0xff})
	require.NoError(t, err)

	// Create different blocks from position 5 onwards
	for i := 5; i < 8; i++ {
		forkChain[i] = &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  forkChain[i-1].Hash(),
			HashMerkleRoot: randomHash(),
			Timestamp:      uint32(2000 + i), // Different timestamps
			Bits:           *nBits,
			Nonce:          uint32(1000 + i), // Different nonces
		}
	}

	// Build cache with fork chain
	err = cache.BuildFromHeaders(forkChain, 5)
	assert.NoError(t, err)

	// Verify the fork chain is properly indexed
	for _, header := range forkChain {
		assert.True(t, cache.HasBlock(header.Hash()))
	}

	// Original chain blocks after fork point should not be in cache
	for i := 5; i < 10; i++ {
		assert.False(t, cache.HasBlock(mainChain[i].Hash()))
	}
}

func TestHeaderChainCache_GetCacheStats(t *testing.T) {
	logger := ulogger.TestLogger{}
	cache := NewHeaderChainCache(logger)

	headers := createValidChain(t, 20)
	err := cache.BuildFromHeaders(headers, 5)
	require.NoError(t, err)

	totalHeaders, validationSets := cache.GetCacheStats()
	assert.Equal(t, 20, totalHeaders)
	// All blocks have validation header entries (even if empty for first block)
	assert.Equal(t, 20, validationSets)
}

func TestHeaderChainCache_Clear(t *testing.T) {
	logger := ulogger.TestLogger{}
	cache := NewHeaderChainCache(logger)

	headers := createValidChain(t, 10)
	err := cache.BuildFromHeaders(headers, 5)
	require.NoError(t, err)

	// Verify cache has data
	assert.Equal(t, 10, cache.GetChainLength())

	// Clear the cache
	cache.Clear()

	// Verify cache is empty
	assert.Equal(t, 0, cache.GetChainLength())
	totalHeaders, validationSets := cache.GetCacheStats()
	assert.Equal(t, 0, totalHeaders)
	assert.Equal(t, 0, validationSets)

	// Verify blocks are no longer found
	for _, header := range headers {
		assert.False(t, cache.HasBlock(header.Hash()))
	}
}

func TestHeaderChainCache_ConcurrentAccess(t *testing.T) {
	logger := ulogger.TestLogger{}
	cache := NewHeaderChainCache(logger)

	headers := createValidChain(t, 100)
	err := cache.BuildFromHeaders(headers, 10)
	require.NoError(t, err)

	// Run concurrent reads
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(idx int) {
			for j := 0; j < 100; j++ {
				// Random read operations
				blockIdx := (idx + j) % len(headers)
				cache.HasBlock(headers[blockIdx].Hash())
				cache.GetValidationHeaders(headers[blockIdx].Hash())
				cache.GetChainLength()
				cache.GetCacheStats()
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify cache is still consistent
	assert.Equal(t, 100, cache.GetChainLength())
}

func TestHeaderChainCache_ValidationHeadersEdgeCases(t *testing.T) {
	logger := ulogger.TestLogger{}
	cache := NewHeaderChainCache(logger)

	t.Run("FewerHeadersThanRequested", func(t *testing.T) {
		// Only 3 headers but requesting 5 previous
		headers := createValidChain(t, 3)
		err := cache.BuildFromHeaders(headers, 5)
		require.NoError(t, err)

		// Block at position 2 should get both previous headers
		validationHeaders, found := cache.GetValidationHeaders(headers[2].Hash())
		assert.True(t, found)
		assert.Equal(t, 2, len(validationHeaders))
		assert.Equal(t, headers[1].Hash(), validationHeaders[0].Hash())
		assert.Equal(t, headers[0].Hash(), validationHeaders[1].Hash())
	})

	t.Run("ExactlyEnoughHeaders", func(t *testing.T) {
		// 6 headers, requesting 5 previous
		headers := createValidChain(t, 6)
		err := cache.BuildFromHeaders(headers, 5)
		require.NoError(t, err)

		// Last block should get exactly 5 previous headers
		validationHeaders, found := cache.GetValidationHeaders(headers[5].Hash())
		assert.True(t, found)
		assert.Equal(t, 5, len(validationHeaders))

		// Verify they're in correct reverse order
		for i := 0; i < 5; i++ {
			assert.Equal(t, headers[4-i].Hash(), validationHeaders[i].Hash())
		}
	})
}

// randomHash creates a random hash for testing
func randomHash() *chainhash.Hash {
	hash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	// Modify some bytes to make it pseudo-random
	bytes := hash.CloneBytes()
	bytes[0] = byte(testRandCounter)
	bytes[1] = byte(testRandCounter >> 8)
	testRandCounter++
	hash, _ = chainhash.NewHash(bytes)
	return hash
}

var testRandCounter int
