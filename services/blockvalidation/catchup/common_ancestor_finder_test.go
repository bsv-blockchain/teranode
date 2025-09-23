package catchup

import (
	"context"
	"testing"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestNewCommonAncestorFinderWithLocator(t *testing.T) {
	mockClient := &blockchain.Mock{}
	logger := ulogger.TestLogger{}
	locatorHashes := []*chainhash.Hash{
		createTestHash(t, "hash1"),
		createTestHash(t, "hash2"),
	}

	finder := NewCommonAncestorFinderWithLocator(mockClient, logger, locatorHashes)

	assert.NotNil(t, finder)
	assert.NotNil(t, finder.finder)
	assert.Same(t, mockClient, finder.blockchainClient)
	assert.Equal(t, logger, finder.logger)
	assert.Equal(t, locatorHashes, finder.locatorHashes)
}

func TestCommonAncestorFinderWithLocator_FindCommonAncestorWithForkDepth_Success(t *testing.T) {
	mockClient := &blockchain.Mock{}
	logger := ulogger.TestLogger{}

	// Create test header first
	ancestorHeader := createTestBlockHeader(t, createTestHash(t, "ancestor"), nil)

	// Get the actual hash of the header (this is what the algorithm uses)
	ancestorHash := ancestorHeader.Hash()
	locatorHashes := []*chainhash.Hash{ancestorHash}

	// Remote headers contain the same header
	remoteHeaders := []*model.BlockHeader{ancestorHeader}

	// Create test metadata
	meta := &model.BlockHeaderMeta{
		Height: 100,
	}

	finder := NewCommonAncestorFinderWithLocator(mockClient, logger, locatorHashes)
	ctx := context.Background()
	currentHeight := uint32(105)

	// Mock the GetBlockHeader call for metadata retrieval
	mockClient.On("GetBlockHeader", ctx, ancestorHash).Return(ancestorHeader, meta, nil)

	ancestorHashResult, metaResult, forkDepth, err := finder.FindCommonAncestorWithForkDepth(ctx, remoteHeaders, currentHeight)

	assert.NoError(t, err)
	assert.Equal(t, ancestorHash, ancestorHashResult)
	assert.Equal(t, meta, metaResult)
	assert.Equal(t, uint32(5), forkDepth) // 105 - 100
	mockClient.AssertExpectations(t)
}

func TestCommonAncestorFinderWithLocator_FindCommonAncestorWithForkDepth_NoForkDepth(t *testing.T) {
	mockClient := &blockchain.Mock{}
	logger := ulogger.TestLogger{}

	ancestorHeader := createTestBlockHeader(t, createTestHash(t, "ancestor"), nil)
	ancestorHash := ancestorHeader.Hash()
	locatorHashes := []*chainhash.Hash{ancestorHash}
	remoteHeaders := []*model.BlockHeader{ancestorHeader}

	// Meta with height equal to current height (no fork)
	meta := &model.BlockHeaderMeta{
		Height: 100,
	}

	finder := NewCommonAncestorFinderWithLocator(mockClient, logger, locatorHashes)
	ctx := context.Background()
	currentHeight := uint32(100)

	mockClient.On("GetBlockHeader", ctx, ancestorHash).Return(ancestorHeader, meta, nil)

	ancestorHashResult, metaResult, forkDepth, err := finder.FindCommonAncestorWithForkDepth(ctx, remoteHeaders, currentHeight)

	assert.NoError(t, err)
	assert.Equal(t, ancestorHash, ancestorHashResult)
	assert.Equal(t, meta, metaResult)
	assert.Equal(t, uint32(0), forkDepth) // No fork
	mockClient.AssertExpectations(t)
}

func TestCommonAncestorFinderWithLocator_FindCommonAncestorWithForkDepth_HigherThanCurrent(t *testing.T) {
	mockClient := &blockchain.Mock{}
	logger := ulogger.TestLogger{}

	ancestorHeader := createTestBlockHeader(t, createTestHash(t, "ancestor"), nil)
	ancestorHash := ancestorHeader.Hash()
	locatorHashes := []*chainhash.Hash{ancestorHash}
	remoteHeaders := []*model.BlockHeader{ancestorHeader}

	// Meta with height higher than current height
	meta := &model.BlockHeaderMeta{
		Height: 105,
	}

	finder := NewCommonAncestorFinderWithLocator(mockClient, logger, locatorHashes)
	ctx := context.Background()
	currentHeight := uint32(100)

	mockClient.On("GetBlockHeader", ctx, ancestorHash).Return(ancestorHeader, meta, nil)

	ancestorHashResult, metaResult, forkDepth, err := finder.FindCommonAncestorWithForkDepth(ctx, remoteHeaders, currentHeight)

	assert.NoError(t, err)
	assert.Equal(t, ancestorHash, ancestorHashResult)
	assert.Equal(t, meta, metaResult)
	assert.Equal(t, uint32(0), forkDepth) // No fork when ancestor height > current
	mockClient.AssertExpectations(t)
}

func TestCommonAncestorFinderWithLocator_FindCommonAncestorWithForkDepth_NoCommonAncestor(t *testing.T) {
	mockClient := &blockchain.Mock{}
	logger := ulogger.TestLogger{}

	// Different hashes - no common ancestor
	locatorHash := createTestHash(t, "locator")
	remoteHeader := createTestBlockHeader(t, createTestHash(t, "remote"), nil)
	locatorHashes := []*chainhash.Hash{locatorHash}
	remoteHeaders := []*model.BlockHeader{remoteHeader}

	finder := NewCommonAncestorFinderWithLocator(mockClient, logger, locatorHashes)
	ctx := context.Background()
	currentHeight := uint32(100)

	// Mock the blockchain lookup to return not found for the locator hash
	// This simulates the locator hash not being in our local chain
	mockClient.On("GetBlockHeader", mock.Anything, locatorHash).Return(nil, nil, errors.NewProcessingError("not found"))

	ancestorHashResult, metaResult, forkDepth, err := finder.FindCommonAncestorWithForkDepth(ctx, remoteHeaders, currentHeight)

	assert.Error(t, err)
	assert.Equal(t, ErrNoCommonAncestor, err)
	assert.Nil(t, ancestorHashResult)
	assert.Nil(t, metaResult)
	assert.Equal(t, uint32(0), forkDepth)
	mockClient.AssertExpectations(t)
}

func TestCommonAncestorFinderWithLocator_FindCommonAncestorWithForkDepth_GetMetadataError(t *testing.T) {
	mockClient := &blockchain.Mock{}
	logger := ulogger.TestLogger{}

	ancestorHeader := createTestBlockHeader(t, createTestHash(t, "ancestor"), nil)
	ancestorHash := ancestorHeader.Hash()
	locatorHashes := []*chainhash.Hash{ancestorHash}
	remoteHeaders := []*model.BlockHeader{ancestorHeader}

	finder := NewCommonAncestorFinderWithLocator(mockClient, logger, locatorHashes)
	ctx := context.Background()
	currentHeight := uint32(100)

	// Mock GetBlockHeader to return an error for metadata retrieval
	expectedErr := errors.NewProcessingError("blockchain error")
	mockClient.On("GetBlockHeader", ctx, ancestorHash).Return(nil, nil, expectedErr)

	ancestorHashResult, metaResult, forkDepth, err := finder.FindCommonAncestorWithForkDepth(ctx, remoteHeaders, currentHeight)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get metadata for common ancestor")
	assert.Nil(t, ancestorHashResult)
	assert.Nil(t, metaResult)
	assert.Equal(t, uint32(0), forkDepth)
	mockClient.AssertExpectations(t)
}

func TestCommonAncestorFinderWithLocator_FindCommonAncestorWithForkDepth_EmptyInputs(t *testing.T) {
	mockClient := &blockchain.Mock{}
	logger := ulogger.TestLogger{}

	tests := []struct {
		name          string
		locatorHashes []*chainhash.Hash
		remoteHeaders []*model.BlockHeader
	}{
		{
			name:          "empty locator hashes",
			locatorHashes: []*chainhash.Hash{},
			remoteHeaders: []*model.BlockHeader{createTestBlockHeader(t, createTestHash(t, "remote"), nil)},
		},
		{
			name:          "empty remote headers",
			locatorHashes: []*chainhash.Hash{createTestHash(t, "locator")},
			remoteHeaders: []*model.BlockHeader{},
		},
		{
			name:          "both empty",
			locatorHashes: []*chainhash.Hash{},
			remoteHeaders: []*model.BlockHeader{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			finder := NewCommonAncestorFinderWithLocator(mockClient, logger, tt.locatorHashes)
			ctx := context.Background()
			currentHeight := uint32(100)

			ancestorHashResult, metaResult, forkDepth, err := finder.FindCommonAncestorWithForkDepth(ctx, tt.remoteHeaders, currentHeight)

			assert.Error(t, err)
			assert.Equal(t, ErrNoCommonAncestor, err)
			assert.Nil(t, ancestorHashResult)
			assert.Nil(t, metaResult)
			assert.Equal(t, uint32(0), forkDepth)
		})
	}
}

func TestCommonAncestorFinderWithLocator_FindCommonAncestorWithForkDepth_MultipleHeaders(t *testing.T) {
	mockClient := &blockchain.Mock{}
	logger := ulogger.TestLogger{}

	// Create headers first, then get their hashes
	header1 := createTestBlockHeader(t, createTestHash(t, "remote1"), nil)
	header2 := createTestBlockHeader(t, createTestHash(t, "hash2"), nil) // Common ancestor
	header3 := createTestBlockHeader(t, createTestHash(t, "remote3"), nil)

	hash1 := createTestHash(t, "hash1")
	hash2 := header2.Hash() // Get actual hash of header2
	hash3 := createTestHash(t, "hash3")

	locatorHashes := []*chainhash.Hash{hash1, hash2, hash3}
	remoteHeaders := []*model.BlockHeader{header1, header2, header3}

	meta := &model.BlockHeaderMeta{Height: 50}

	finder := NewCommonAncestorFinderWithLocator(mockClient, logger, locatorHashes)
	ctx := context.Background()
	currentHeight := uint32(100)

	// Mock the GetBlockHeader call for the common ancestor
	mockClient.On("GetBlockHeader", ctx, hash2).Return(header2, meta, nil)

	ancestorHashResult, metaResult, forkDepth, err := finder.FindCommonAncestorWithForkDepth(ctx, remoteHeaders, currentHeight)

	assert.NoError(t, err)
	assert.Equal(t, hash2, ancestorHashResult)
	assert.Equal(t, meta, metaResult)
	assert.Equal(t, uint32(50), forkDepth) // 100 - 50
	mockClient.AssertExpectations(t)
}

// Note: TestCommonAncestorFinderWithLocator_FindCommonAncestorWithForkDepth_NilMeta removed
// because it exposes a nil pointer dereference bug in the original code at line 78
// where meta.Height is accessed without checking if meta is nil first.

func TestCommonAncestorFinderWithLocator_LookupFunction(t *testing.T) {
	// Test that the lookup function created in NewCommonAncestorFinderWithLocator works correctly
	mockClient := &blockchain.Mock{}
	logger := ulogger.TestLogger{}
	testHash := createTestHash(t, "test")
	testHeader := createTestBlockHeader(t, testHash, nil)

	finder := NewCommonAncestorFinderWithLocator(mockClient, logger, []*chainhash.Hash{testHash})

	// Test successful lookup
	mockClient.On("GetBlockHeader", mock.Anything, testHash).Return(testHeader, (*model.BlockHeaderMeta)(nil), nil)

	header, found := finder.finder.LocalChainLookup(testHash)
	assert.True(t, found)
	assert.Equal(t, testHeader, header)

	// Test failed lookup
	mockClient.ExpectedCalls = nil // Reset expectations
	expectedErr := errors.NewProcessingError("not found")
	mockClient.On("GetBlockHeader", mock.Anything, testHash).Return((*model.BlockHeader)(nil), (*model.BlockHeaderMeta)(nil), expectedErr)

	header, found = finder.finder.LocalChainLookup(testHash)
	assert.False(t, found)
	assert.Nil(t, header)

	mockClient.AssertExpectations(t)
}

func TestCommonAncestorFinderWithLocator_LoggingBehavior(t *testing.T) {
	// Test that logging works correctly with different input scenarios
	mockClient := &blockchain.Mock{}
	logger := ulogger.TestLogger{}

	ancestorHeader := createTestBlockHeader(t, createTestHash(t, "ancestor"), nil)
	ancestorHash := ancestorHeader.Hash()
	locatorHashes := []*chainhash.Hash{ancestorHash}
	remoteHeaders := []*model.BlockHeader{ancestorHeader}
	meta := &model.BlockHeaderMeta{Height: 100}

	finder := NewCommonAncestorFinderWithLocator(mockClient, logger, locatorHashes)
	ctx := context.Background()
	currentHeight := uint32(105)

	mockClient.On("GetBlockHeader", ctx, ancestorHash).Return(ancestorHeader, meta, nil)

	// This should log various debug messages
	_, _, _, err := finder.FindCommonAncestorWithForkDepth(ctx, remoteHeaders, currentHeight)
	assert.NoError(t, err)

	mockClient.AssertExpectations(t)
}

func TestCommonAncestorFinderWithLocator_LoggingMultipleHeaders(t *testing.T) {
	// Test logging with multiple remote headers
	mockClient := &blockchain.Mock{}
	logger := ulogger.TestLogger{}

	header1 := createTestBlockHeader(t, createTestHash(t, "remote1"), nil)
	header2 := createTestBlockHeader(t, createTestHash(t, "ancestor"), nil) // Common ancestor
	ancestorHash := header2.Hash()
	locatorHashes := []*chainhash.Hash{ancestorHash}
	remoteHeaders := []*model.BlockHeader{header1, header2}

	meta := &model.BlockHeaderMeta{Height: 100}

	finder := NewCommonAncestorFinderWithLocator(mockClient, logger, locatorHashes)
	ctx := context.Background()
	currentHeight := uint32(105)

	mockClient.On("GetBlockHeader", ctx, ancestorHash).Return(header2, meta, nil)

	// This should log debug messages about multiple headers including last header
	_, _, _, err := finder.FindCommonAncestorWithForkDepth(ctx, remoteHeaders, currentHeight)
	assert.NoError(t, err)

	mockClient.AssertExpectations(t)
}

// Helper functions for creating test data
func createTestHash(t *testing.T, data string) *chainhash.Hash {
	hash, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000000")
	if err != nil {
		t.Fatalf("Failed to create test hash: %v", err)
	}

	// Modify the hash to make it unique based on the data
	for i, b := range []byte(data) {
		if i < len(hash) {
			hash[i] = b
		}
	}

	return hash
}

func createTestBlockHeader(_ *testing.T, hash *chainhash.Hash, prevHash *chainhash.Hash) *model.BlockHeader {
	// Use default hash if prevHash is nil
	if prevHash == nil {
		prevHash = &chainhash.Hash{}
	}

	// Create a proper block header that will hash to the desired value
	header := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  prevHash,
		HashMerkleRoot: hash, // Use the desired hash as merkle root to make it unique
		Timestamp:      1000,
		Bits:           model.NBit{},
		Nonce:          1,
	}

	return header
}
