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
