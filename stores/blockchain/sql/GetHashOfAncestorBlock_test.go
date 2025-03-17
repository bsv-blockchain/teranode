package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

const blockWindow = 144

func TestSQLGetHashOfAncestorBlock(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings()

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	// Create blockWindow+4 blocks (e.g. 148 blocks if blockWindow=144)
	// Each block points to previous block via HashPrevBlock
	blocks := generateBlocks(t, blockWindow+4)

	// Store all blocks in the database
	for _, block := range blocks {
		_, _, err = s.StoreBlock(context.Background(), block, "")
		require.NoError(t, err)
	}

	// Test cases to verify ancestor block retrieval
	testCases := []struct {
		name          string
		startIndex    int // Index of starting block
		depth         int // How many blocks to go back
		expectedIndex int // Index of expected ancestor block
	}{
		{
			name:          "go back 144 blocks from last block",
			startIndex:    len(blocks) - 1,               // Start from last block (index 147)
			depth:         blockWindow,                   // Go back 144 blocks
			expectedIndex: len(blocks) - 1 - blockWindow, // Should get block at index 3
		},
		{
			name:          "go back 2 blocks from middle",
			startIndex:    10,
			depth:         2,
			expectedIndex: 8,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			startBlock := blocks[tc.startIndex]
			expectedBlock := blocks[tc.expectedIndex]

			// Get ancestor block hash by traversing back 'depth' blocks
			ancestorHash, err := s.GetHashOfAncestorBlock(context.Background(), startBlock.Hash(), tc.depth)
			require.NoError(t, err)

			// Verify we got the correct ancestor
			require.Equal(t, expectedBlock.Hash(), ancestorHash,
				"Expected block at index %d when going back %d blocks from index %d",
				tc.expectedIndex, tc.depth, tc.startIndex)
		})
	}
}

func TestSQLGetHashOfAncestorBlockShort(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings()

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	// don't generate enough blocks
	blocks := generateBlocks(t, blockWindow-5)
	for _, block := range blocks {
		_, _, err = s.StoreBlock(context.Background(), block, "")
		require.NoError(t, err)
	}

	var expectedHash *chainhash.Hash

	ancestorHash, err := s.GetHashOfAncestorBlock(context.Background(), blocks[len(blocks)-1].Hash(), blockWindow)

	require.Error(t, err)
	require.Equal(t, expectedHash, ancestorHash)
}

func generateBlocks(t *testing.T, numberOfBlocks int) []*model.Block {
	var generateBlocks []*model.Block

	hashPrevBlock, err := chainhash.NewHashFromStr("0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206")
	require.NoError(t, err)

	coinbase, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)

	coinbase.Outputs = nil
	_ = coinbase.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 5000000000+300)

	subtree, err := util.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, subtree.AddNode(*coinbase.TxIDChainHash(), 0, 0))
	merkleRoot := subtree.RootHash()

	for i := 0; i < numberOfBlocks; i++ {
		// nolint: gosec
		block := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				Timestamp:      1231469665 + uint32(i),
				Nonce:          2573394689,
				HashPrevBlock:  hashPrevBlock,
				HashMerkleRoot: merkleRoot,
				Bits:           *bits,
			},
			CoinbaseTx:       coinbase,
			TransactionCount: 1,
			Subtrees: []*chainhash.Hash{
				subtree.RootHash(),
			},
		}
		generateBlocks = append(generateBlocks, block)
		hashPrevBlock = block.Hash()
	}

	return generateBlocks
}
