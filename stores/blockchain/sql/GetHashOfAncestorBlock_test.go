package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

const blockWindow = 144

func TestSQL_GetHashOfAncestorBlock(t *testing.T) {
	storeUrl, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeUrl)
	require.NoError(t, err)

	blocks := generateBlocks(t, blockWindow+4)
	for _, block := range blocks {
		_, err = s.StoreBlock(context.Background(), block, "")
		require.NoError(t, err)
	}

	ancestorHash, err := s.GetHashOfAncestorBlock(context.Background(), blocks[len(blocks)-1].Hash(), blockWindow)
	require.NoError(t, err)
	require.Equal(t, blocks[len(blocks)-blockWindow].Hash(), ancestorHash)
}
func TestSQL_GetHashOfAncestorBlock_short(t *testing.T) {
	storeUrl, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeUrl)
	require.NoError(t, err)

	// don't generate enough blocks
	blocks := generateBlocks(t, blockWindow-5)
	for _, block := range blocks {
		_, err = s.StoreBlock(context.Background(), block, "")
		require.NoError(t, err)
	}
	var expectedHash *chainhash.Hash
	require.NoError(t, err)

	ancestorHash, err := s.GetHashOfAncestorBlock(context.Background(), blocks[len(blocks)-1].Hash(), blockWindow)

	require.Error(t, err)
	require.Equal(t, expectedHash, ancestorHash)
}

func generateBlocks(t *testing.T, numberOfBlocks int) []*model.Block {

	var generateBlocks []*model.Block
	hashPrevBlock, err := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
	require.NoError(t, err)

	coinbaseHex := "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1703fb03002f6d322d75732f0cb6d7d459fb411ef3ac6d65ffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000"
	coinbase, err := bt.NewTxFromString(coinbaseHex)
	require.NoError(t, err)
	coinbase.Outputs = nil
	_ = coinbase.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 5000000000+300)

	subtree, err := util.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, subtree.AddNode(*coinbase.TxIDChainHash(), 0, 0))
	merkleRoot := subtree.RootHash()
	for i := 0; i < numberOfBlocks; i++ {
		block := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				Timestamp:      1231469665 + uint32(i),
				Nonce:          2573394689,
				HashPrevBlock:  hashPrevBlock,
				HashMerkleRoot: merkleRoot,
				Bits:           bits,
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
