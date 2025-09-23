package catchup

import (
	"testing"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFindCommonAncestor_BasicCase tests finding a common ancestor in a simple case
func TestFindCommonAncestor_BasicCase(t *testing.T) {
	// Create a local chain
	localChain := make(map[chainhash.Hash]*model.BlockHeader)

	// Create headers for local chain
	header1 := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{0},
		HashMerkleRoot: &chainhash.Hash{1},
		Timestamp:      1000,
		Bits:           model.NBit{},
		Nonce:          1,
	}
	header2 := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  header1.Hash(),
		HashMerkleRoot: &chainhash.Hash{2},
		Timestamp:      2000,
		Bits:           model.NBit{},
		Nonce:          2,
	}
	header3 := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  header2.Hash(),
		HashMerkleRoot: &chainhash.Hash{3},
		Timestamp:      3000,
		Bits:           model.NBit{},
		Nonce:          3,
	}

	localChain[*header1.Hash()] = header1
	localChain[*header2.Hash()] = header2
	localChain[*header3.Hash()] = header3

	// Create lookup function
	lookupFunc := func(hash *chainhash.Hash) (*model.BlockHeader, bool) {
		header, exists := localChain[*hash]
		return header, exists
	}

	finder := NewCommonAncestorFinder(lookupFunc)

	// Create locator hashes (header3 and header1 are in our chain)
	locatorHashes := []*chainhash.Hash{
		header3.Hash(),
		header1.Hash(),
	}

	// Create remote headers that share header2 as common ancestor
	remoteHeader1 := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  header2.Hash(), // Branches from header2
		HashMerkleRoot: &chainhash.Hash{10},
		Timestamp:      3500,
		Bits:           model.NBit{},
		Nonce:          10,
	}
	remoteHeader2 := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  remoteHeader1.Hash(),
		HashMerkleRoot: &chainhash.Hash{11},
		Timestamp:      4000,
		Bits:           model.NBit{},
		Nonce:          11,
	}

	remoteHeaders := []*model.BlockHeader{header2, remoteHeader1, remoteHeader2}

	// Find common ancestor
	ancestor, index, err := finder.FindCommonAncestor(locatorHashes, remoteHeaders)

	require.NoError(t, err)
	require.NotNil(t, ancestor)
	assert.Equal(t, header2.Hash(), ancestor.Hash())
	assert.Equal(t, 0, index) // header2 is at index 0 in remoteHeaders
}

// TestFindCommonAncestor_NoCommonAncestor tests when there's no common ancestor
func TestFindCommonAncestor_NoCommonAncestor(t *testing.T) {
	// Create a local chain
	localChain := make(map[chainhash.Hash]*model.BlockHeader)

	header1 := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{0},
		HashMerkleRoot: &chainhash.Hash{1},
		Timestamp:      1000,
		Bits:           model.NBit{},
		Nonce:          1,
	}
	localChain[*header1.Hash()] = header1

	lookupFunc := func(hash *chainhash.Hash) (*model.BlockHeader, bool) {
		header, exists := localChain[*hash]
		return header, exists
	}

	finder := NewCommonAncestorFinder(lookupFunc)

	// Locator with hash from local chain
	locatorHashes := []*chainhash.Hash{
		header1.Hash(),
	}

	// Remote headers from completely different chain
	remoteHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{99}, // Different chain
		HashMerkleRoot: &chainhash.Hash{100},
		Timestamp:      5000,
		Bits:           model.NBit{},
		Nonce:          99,
	}

	remoteHeaders := []*model.BlockHeader{remoteHeader}

	// Should not find common ancestor
	ancestor, index, err := finder.FindCommonAncestor(locatorHashes, remoteHeaders)

	assert.Error(t, err)
	assert.Equal(t, ErrNoCommonAncestor, err)
	assert.Nil(t, ancestor)
	assert.Equal(t, -1, index)
}

// TestFindCommonAncestor_EmptyInputs tests with empty inputs
func TestFindCommonAncestor_EmptyInputs(t *testing.T) {
	lookupFunc := func(hash *chainhash.Hash) (*model.BlockHeader, bool) {
		return nil, false
	}

	finder := NewCommonAncestorFinder(lookupFunc)

	t.Run("EmptyLocatorHashes", func(t *testing.T) {
		remoteHeaders := []*model.BlockHeader{
			{
				Version:        1,
				HashPrevBlock:  &chainhash.Hash{1},
				HashMerkleRoot: &chainhash.Hash{2},
				Timestamp:      1000,
				Bits:           model.NBit{},
				Nonce:          1,
			},
		}

		ancestor, index, err := finder.FindCommonAncestor([]*chainhash.Hash{}, remoteHeaders)

		assert.Error(t, err)
		assert.Equal(t, ErrNoCommonAncestor, err)
		assert.Nil(t, ancestor)
		assert.Equal(t, -1, index)
	})

	t.Run("EmptyRemoteHeaders", func(t *testing.T) {
		locatorHashes := []*chainhash.Hash{
			{1, 2, 3},
		}

		ancestor, index, err := finder.FindCommonAncestor(locatorHashes, []*model.BlockHeader{})

		assert.Error(t, err)
		assert.Equal(t, ErrNoCommonAncestor, err)
		assert.Nil(t, ancestor)
		assert.Equal(t, -1, index)
	})

	t.Run("BothEmpty", func(t *testing.T) {
		ancestor, index, err := finder.FindCommonAncestor([]*chainhash.Hash{}, []*model.BlockHeader{})

		assert.Error(t, err)
		assert.Equal(t, ErrNoCommonAncestor, err)
		assert.Nil(t, ancestor)
		assert.Equal(t, -1, index)
	})
}

// TestFindCommonAncestor_LocatorInRemoteHeaders tests when locator hash is directly in remote headers
func TestFindCommonAncestor_LocatorInRemoteHeaders(t *testing.T) {
	// Create headers
	header1 := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{0},
		HashMerkleRoot: &chainhash.Hash{1},
		Timestamp:      1000,
		Bits:           model.NBit{},
		Nonce:          1,
	}
	header2 := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  header1.Hash(),
		HashMerkleRoot: &chainhash.Hash{2},
		Timestamp:      2000,
		Bits:           model.NBit{},
		Nonce:          2,
	}

	// Local chain has both headers
	localChain := make(map[chainhash.Hash]*model.BlockHeader)
	localChain[*header1.Hash()] = header1
	localChain[*header2.Hash()] = header2

	lookupFunc := func(hash *chainhash.Hash) (*model.BlockHeader, bool) {
		header, exists := localChain[*hash]
		return header, exists
	}

	finder := NewCommonAncestorFinder(lookupFunc)

	// Locator contains header2
	locatorHashes := []*chainhash.Hash{
		header2.Hash(),
	}

	// Remote headers also contain header2
	remoteHeaders := []*model.BlockHeader{header1, header2}

	// Should find header2 as common ancestor
	ancestor, index, err := finder.FindCommonAncestor(locatorHashes, remoteHeaders)

	require.NoError(t, err)
	require.NotNil(t, ancestor)
	assert.Equal(t, header2.Hash(), ancestor.Hash())
	assert.Equal(t, 1, index) // header2 is at index 1 in remoteHeaders
}

// TestFindCommonAncestor_WalkBackThroughParents tests walking back through parent links
func TestFindCommonAncestor_WalkBackThroughParents(t *testing.T) {
	// Create a chain: genesis -> header1 -> header2 -> header3
	genesis := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{0},
		Timestamp:      0,
		Bits:           model.NBit{},
		Nonce:          0,
	}
	header1 := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  genesis.Hash(),
		HashMerkleRoot: &chainhash.Hash{1},
		Timestamp:      1000,
		Bits:           model.NBit{},
		Nonce:          1,
	}
	header2 := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  header1.Hash(),
		HashMerkleRoot: &chainhash.Hash{2},
		Timestamp:      2000,
		Bits:           model.NBit{},
		Nonce:          2,
	}
	header3 := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  header2.Hash(),
		HashMerkleRoot: &chainhash.Hash{3},
		Timestamp:      3000,
		Bits:           model.NBit{},
		Nonce:          3,
	}

	// Local chain has genesis and header1
	localChain := make(map[chainhash.Hash]*model.BlockHeader)
	localChain[*genesis.Hash()] = genesis
	localChain[*header1.Hash()] = header1

	lookupFunc := func(hash *chainhash.Hash) (*model.BlockHeader, bool) {
		header, exists := localChain[*hash]
		return header, exists
	}

	finder := NewCommonAncestorFinder(lookupFunc)

	// Locator only has header1
	locatorHashes := []*chainhash.Hash{
		header1.Hash(),
	}

	// Remote headers have the full chain including genesis and header1
	remoteHeaders := []*model.BlockHeader{genesis, header1, header2, header3}

	// Should find header1 as common ancestor (latest common block)
	ancestor, index, err := finder.FindCommonAncestor(locatorHashes, remoteHeaders)

	require.NoError(t, err)
	require.NotNil(t, ancestor)
	assert.Equal(t, header1.Hash(), ancestor.Hash())
	assert.Equal(t, 1, index) // header1 is at index 1 in remoteHeaders
}

// TestFindCommonAncestorByHeaders_BasicCase tests finding common ancestor by comparing two header chains
func TestFindCommonAncestorByHeaders_BasicCase(t *testing.T) {
	// Create test headers
	header1 := createTestHeader(t, 1, &chainhash.Hash{0})
	header2 := createTestHeader(t, 2, header1.Hash())
	header3 := createTestHeader(t, 3, header2.Hash())
	header4 := createTestHeader(t, 4, header3.Hash())

	// Chain1: [header1, header2, header3]
	chain1 := []*model.BlockHeader{header1, header2, header3}

	// Chain2: [header2, header3, header4] (shares header2 and header3)
	chain2 := []*model.BlockHeader{header2, header3, header4}

	finder := NewCommonAncestorFinder(nil) // No lookup function needed for this method

	// Should find header2 as first common ancestor
	common, idx1, idx2 := finder.FindCommonAncestorByHeaders(chain1, chain2)

	require.NotNil(t, common)
	assert.Equal(t, header2.Hash(), common.Hash())
	assert.Equal(t, 1, idx1) // header2 is at index 1 in chain1
	assert.Equal(t, 0, idx2) // header2 is at index 0 in chain2
}

// TestFindCommonAncestorByHeaders_NoCommonAncestor tests when chains have no common headers
func TestFindCommonAncestorByHeaders_NoCommonAncestor(t *testing.T) {
	// Create completely different chains
	header1 := createTestHeader(t, 1, &chainhash.Hash{0})
	header2 := createTestHeader(t, 2, header1.Hash())

	header3 := createTestHeader(t, 3, &chainhash.Hash{99}) // Different parent
	header4 := createTestHeader(t, 4, header3.Hash())

	chain1 := []*model.BlockHeader{header1, header2}
	chain2 := []*model.BlockHeader{header3, header4}

	finder := NewCommonAncestorFinder(nil)

	common, idx1, idx2 := finder.FindCommonAncestorByHeaders(chain1, chain2)

	assert.Nil(t, common)
	assert.Equal(t, -1, idx1)
	assert.Equal(t, -1, idx2)
}

// TestFindCommonAncestorByHeaders_EmptyChains tests with empty chain inputs
func TestFindCommonAncestorByHeaders_EmptyChains(t *testing.T) {
	finder := NewCommonAncestorFinder(nil)

	header1 := createTestHeader(t, 1, &chainhash.Hash{0})
	nonEmptyChain := []*model.BlockHeader{header1}

	// Test empty chain1
	common, idx1, idx2 := finder.FindCommonAncestorByHeaders([]*model.BlockHeader{}, nonEmptyChain)
	assert.Nil(t, common)
	assert.Equal(t, -1, idx1)
	assert.Equal(t, -1, idx2)

	// Test empty chain2
	common, idx1, idx2 = finder.FindCommonAncestorByHeaders(nonEmptyChain, []*model.BlockHeader{})
	assert.Nil(t, common)
	assert.Equal(t, -1, idx1)
	assert.Equal(t, -1, idx2)

	// Test both empty
	common, idx1, idx2 = finder.FindCommonAncestorByHeaders([]*model.BlockHeader{}, []*model.BlockHeader{})
	assert.Nil(t, common)
	assert.Equal(t, -1, idx1)
	assert.Equal(t, -1, idx2)
}

// TestValidateChainConnection_ValidChain tests validation of a properly connected chain
func TestValidateChainConnection_ValidChain(t *testing.T) {
	finder := NewCommonAncestorFinder(nil)

	// Create a properly connected chain
	header1 := createTestHeader(t, 1, &chainhash.Hash{0})
	header2 := createTestHeader(t, 2, header1.Hash())
	header3 := createTestHeader(t, 3, header2.Hash())

	headers := []*model.BlockHeader{header1, header2, header3}

	// Should validate as properly connected
	assert.True(t, finder.ValidateChainConnection(headers, nil))

	// Should also validate when providing the correct start hash
	assert.True(t, finder.ValidateChainConnection(headers, &chainhash.Hash{0}))
}

// TestValidateChainConnection_InvalidConnection tests validation of improperly connected chain
func TestValidateChainConnection_InvalidConnection(t *testing.T) {
	finder := NewCommonAncestorFinder(nil)

	// Create headers that don't connect properly
	header1 := createTestHeader(t, 1, &chainhash.Hash{0})
	header2 := createTestHeader(t, 2, &chainhash.Hash{99}) // Wrong parent
	header3 := createTestHeader(t, 3, header2.Hash())

	headers := []*model.BlockHeader{header1, header2, header3}

	// Should fail validation due to broken connection
	assert.False(t, finder.ValidateChainConnection(headers, nil))
}

// TestValidateChainConnection_WrongStartHash tests validation with wrong start hash
func TestValidateChainConnection_WrongStartHash(t *testing.T) {
	finder := NewCommonAncestorFinder(nil)

	header1 := createTestHeader(t, 1, &chainhash.Hash{0})
	headers := []*model.BlockHeader{header1}

	// Should fail when start hash doesn't match
	wrongStartHash := &chainhash.Hash{99}
	assert.False(t, finder.ValidateChainConnection(headers, wrongStartHash))
}

// TestValidateChainConnection_EmptyHeaders tests validation with empty header list
func TestValidateChainConnection_EmptyHeaders(t *testing.T) {
	finder := NewCommonAncestorFinder(nil)

	// Empty headers should always validate
	assert.True(t, finder.ValidateChainConnection([]*model.BlockHeader{}, nil))
	assert.True(t, finder.ValidateChainConnection([]*model.BlockHeader{}, &chainhash.Hash{0}))
}

// TestGetBlockLocator_BasicCase tests creating a block locator
func TestGetBlockLocator_BasicCase(t *testing.T) {
	// Mock lookup function that returns hashes for heights
	lookupFunc := func(height uint32) *chainhash.Hash {
		if height <= 10 {
			hash := &chainhash.Hash{}
			hash[0] = byte(height) // Simple way to make unique hashes
			return hash
		}
		return nil
	}

	startHash := &chainhash.Hash{100}
	locator := GetBlockLocator(startHash, 10, lookupFunc)

	// Should start with the provided start hash
	assert.Equal(t, startHash, locator[0])

	// Should contain expected number of entries (start + backwards steps + genesis)
	assert.True(t, len(locator) > 1)

	// Should include genesis (height 0)
	genesis := &chainhash.Hash{}
	genesis[0] = 0
	assert.Contains(t, locator, genesis)
}

// TestGetBlockLocator_NilStartHash tests locator creation with nil start hash
func TestGetBlockLocator_NilStartHash(t *testing.T) {
	lookupFunc := func(height uint32) *chainhash.Hash {
		if height <= 5 {
			hash := &chainhash.Hash{}
			hash[0] = byte(height)
			return hash
		}
		return nil
	}

	locator := GetBlockLocator(nil, 5, lookupFunc)

	// Should not include nil start hash but should have other entries
	assert.True(t, len(locator) > 0)
	assert.NotContains(t, locator, nil)
}

// TestGetBlockLocator_NoHashesAvailable tests when lookup function returns no hashes
func TestGetBlockLocator_NoHashesAvailable(t *testing.T) {
	lookupFunc := func(height uint32) *chainhash.Hash {
		return nil // Always return nil
	}

	startHash := &chainhash.Hash{100}
	locator := GetBlockLocator(startHash, 10, lookupFunc)

	// Should only contain the start hash
	assert.Equal(t, 1, len(locator))
	assert.Equal(t, startHash, locator[0])
}

// TestGetBlockLocator_ExponentialSteps tests that step size increases exponentially
func TestGetBlockLocator_ExponentialSteps(t *testing.T) {
	var requestedHeights []uint32
	lookupFunc := func(height uint32) *chainhash.Hash {
		requestedHeights = append(requestedHeights, height)
		hash := &chainhash.Hash{}
		hash[0] = byte(height)
		return hash
	}

	startHash := &chainhash.Hash{100}
	locator := GetBlockLocator(startHash, 20, lookupFunc)

	// Should have requested heights with exponentially increasing gaps
	assert.True(t, len(requestedHeights) > 5)

	// Should always request genesis (height 0) at the end
	assert.Equal(t, uint32(0), requestedHeights[len(requestedHeights)-1])

	// Should trigger exponential step increase (after 5+ entries)
	// This ensures the exponential step logic is covered
	assert.True(t, len(locator) >= 6) // Should have start + enough entries to trigger exponential steps
}

// TestGetBlockLocator_LongChain tests with a longer chain to ensure exponential step increase
func TestGetBlockLocator_LongChain(t *testing.T) {
	var requestedHeights []uint32
	lookupFunc := func(height uint32) *chainhash.Hash {
		requestedHeights = append(requestedHeights, height)
		if height <= 100 {
			hash := &chainhash.Hash{}
			hash[0] = byte(height % 256)
			return hash
		}
		return nil
	}

	startHash := &chainhash.Hash{255}
	locator := GetBlockLocator(startHash, 100, lookupFunc)

	// Should include start hash
	assert.Equal(t, startHash, locator[0])

	// Should have at least 8 entries to trigger exponential stepping
	assert.True(t, len(locator) >= 8)

	// Should include genesis (height 0)
	genesis := &chainhash.Hash{}
	genesis[0] = 0
	assert.Contains(t, locator, genesis)

	// Verify exponential stepping occurred by checking requested heights
	// After the first 5 entries, steps should start doubling
	assert.True(t, len(requestedHeights) >= 8)
}

// TestGetBlockLocator_MaxLocatorLength tests locator behavior with long chains
func TestGetBlockLocator_MaxLocatorLength(t *testing.T) {
	lookupFunc := func(height uint32) *chainhash.Hash {
		if height <= 1000 {
			hash := &chainhash.Hash{}
			hash[0] = byte(height % 256)
			return hash
		}
		return nil
	}

	startHash := &chainhash.Hash{255}
	locator := GetBlockLocator(startHash, 1000, lookupFunc)

	// The algorithm stops at 10 entries in the loop, but can add genesis separately
	// So maximum should be 11 (10 from loop + genesis)
	t.Logf("Locator length: %d", len(locator))
	assert.True(t, len(locator) <= 11)

	// Should still include genesis
	genesis := &chainhash.Hash{}
	genesis[0] = 0
	assert.Contains(t, locator, genesis)

	// Should include start hash
	assert.Equal(t, startHash, locator[0])
}

// TestFindBestCommonAncestor_Success tests successful ancestor finding
func TestFindBestCommonAncestor_Success(t *testing.T) {
	// Create a local chain
	localChain := make(map[chainhash.Hash]*model.BlockHeader)

	header1 := createTestHeader(t, 1, &chainhash.Hash{0})
	header2 := createTestHeader(t, 2, header1.Hash())
	header3 := createTestHeader(t, 3, header2.Hash())

	localChain[*header1.Hash()] = header1
	localChain[*header2.Hash()] = header2
	localChain[*header3.Hash()] = header3

	lookupFunc := func(hash *chainhash.Hash) (*model.BlockHeader, bool) {
		header, exists := localChain[*hash]
		return header, exists
	}

	finder := NewCommonAncestorFinder(lookupFunc)

	// Remote headers include header2 (common ancestor)
	remoteHeaders := []*model.BlockHeader{header2}

	result, err := finder.FindBestCommonAncestor(header3, 3, remoteHeaders, 10)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, header2.Hash(), result.Header.Hash())
	assert.Equal(t, uint32(2), result.Height)
	assert.Equal(t, 1, result.LocalIndex)  // 1 step back from tip
	assert.Equal(t, 0, result.RemoteIndex) // Index 0 in remote headers
	assert.Equal(t, 1, result.Depth)       // 1 step back
}

// TestFindBestCommonAncestor_EmptyRemoteHeaders tests with empty remote headers
func TestFindBestCommonAncestor_EmptyRemoteHeaders(t *testing.T) {
	finder := NewCommonAncestorFinder(nil)

	localTip := createTestHeader(t, 1, &chainhash.Hash{0})

	result, err := finder.FindBestCommonAncestor(localTip, 1, []*model.BlockHeader{}, 10)

	assert.Error(t, err)
	assert.Equal(t, ErrNoCommonAncestor, err)
	assert.Nil(t, result)
}

// TestFindBestCommonAncestor_NoCommonAncestor tests when no common ancestor exists
func TestFindBestCommonAncestor_NoCommonAncestor(t *testing.T) {
	localChain := make(map[chainhash.Hash]*model.BlockHeader)

	header1 := createTestHeader(t, 1, &chainhash.Hash{0})
	localChain[*header1.Hash()] = header1

	lookupFunc := func(hash *chainhash.Hash) (*model.BlockHeader, bool) {
		header, exists := localChain[*hash]
		return header, exists
	}

	finder := NewCommonAncestorFinder(lookupFunc)

	// Remote header from different chain
	remoteHeader := createTestHeader(t, 99, &chainhash.Hash{99})
	remoteHeaders := []*model.BlockHeader{remoteHeader}

	result, err := finder.FindBestCommonAncestor(header1, 1, remoteHeaders, 10)

	assert.Error(t, err)
	assert.Equal(t, ErrNoCommonAncestor, err)
	assert.Nil(t, result)
}

// TestFindBestCommonAncestor_MaxDepthReached tests when max depth is reached
func TestFindBestCommonAncestor_MaxDepthReached(t *testing.T) {
	localChain := make(map[chainhash.Hash]*model.BlockHeader)

	// Create a longer chain
	var headers []*model.BlockHeader
	prev := &chainhash.Hash{0}
	for i := 1; i <= 5; i++ {
		header := createTestHeader(t, uint32(i), prev)
		headers = append(headers, header)
		localChain[*header.Hash()] = header
		prev = header.Hash()
	}

	lookupFunc := func(hash *chainhash.Hash) (*model.BlockHeader, bool) {
		header, exists := localChain[*hash]
		return header, exists
	}

	finder := NewCommonAncestorFinder(lookupFunc)

	// Remote header from different chain
	remoteHeader := createTestHeader(t, 99, &chainhash.Hash{99})
	remoteHeaders := []*model.BlockHeader{remoteHeader}

	// Set max depth to 2, should stop searching after 2 steps
	result, err := finder.FindBestCommonAncestor(headers[4], 5, remoteHeaders, 2)

	assert.Error(t, err)
	assert.Equal(t, ErrNoCommonAncestor, err)
	assert.Nil(t, result)
}

// TestFindBestCommonAncestor_LocalChainLookupFails tests when local chain lookup fails
func TestFindBestCommonAncestor_LocalChainLookupFails(t *testing.T) {
	// Lookup function that always fails
	lookupFunc := func(hash *chainhash.Hash) (*model.BlockHeader, bool) {
		return nil, false
	}

	finder := NewCommonAncestorFinder(lookupFunc)

	localTip := createTestHeader(t, 1, &chainhash.Hash{0})
	remoteHeader := createTestHeader(t, 1, &chainhash.Hash{0}) // Same hash
	remoteHeaders := []*model.BlockHeader{remoteHeader}

	// Even though hashes match, lookup fails so no result
	result, err := finder.FindBestCommonAncestor(localTip, 1, remoteHeaders, 10)

	assert.Error(t, err)
	assert.Equal(t, ErrNoCommonAncestor, err)
	assert.Nil(t, result)
}

// Helper function to create test headers
func createTestHeader(_ *testing.T, nonce uint32, prevHash *chainhash.Hash) *model.BlockHeader {
	return &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  prevHash,
		HashMerkleRoot: &chainhash.Hash{byte(nonce)}, // Use nonce to make unique
		Timestamp:      1000 + nonce,
		Bits:           model.NBit{},
		Nonce:          nonce,
	}
}
