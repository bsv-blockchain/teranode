package sql

import (
	"testing"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
)

var (
	hashPrevBlock, _  = chainhash.NewHashFromStr("0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206")
	hashMerkleRoot, _ = chainhash.NewHashFromStr("6c487efd5e078c65988f78a52f6d9438a7a9eaf1b9446f78e692e81e8d593970")
	bits, _           = model.NewNBitFromString("207fffff")
	coinbaseTx, _     = bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff17030100002f6d312d65752f29c267ffea1adb87f33b398fffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000")
	subtree, _        = chainhash.NewHashFromStr("0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098")

	block2PrevBlockHash, _  = chainhash.NewHashFromStr("39e0c1ec707ef27692c46df360109072388cca151b24054d2d70035d1034662a")
	block2MerkleRootHash, _ = chainhash.NewHashFromStr("b881339d3b500bcaceb5d2f1225f45edd77e846805ddffe27788fc06f218f177")
	coinbaseTx2, _          = bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff17030200002f6d312d65752f605f77009f74384816a31807ffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000")
	block3Hash, _           = chainhash.NewHashFromStr("3bb0ca67e7675a43534adecf058c38347cd59e2936eb7f6475a7f6354386af58")
	block3PrevBlockHash, _  = chainhash.NewHashFromStr("484e58c7bf0208d787314710535ef7be8ca31748bc9fef5e1ee2de67ebda757a")
	block3MerkleRootHash, _ = chainhash.NewHashFromStr("d1de05a65845a49ad63eed887c4cf7cc824e02b5d10de82829f740b748b9737f")
	coinbaseTx3, _          = bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff17030300002f6d312d65752fb670097da68d1b768d8b21f6ffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000")
	block1                  = &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			Timestamp:      1729259727,
			Nonce:          0,
			HashPrevBlock:  hashPrevBlock,
			HashMerkleRoot: hashMerkleRoot,
			Bits:           *bits,
		},
		Height:           1,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: 1,
		Subtrees: []*chainhash.Hash{
			subtree,
		},
	}
	block2 = &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			Timestamp:      1729259727,
			Nonce:          1,
			HashPrevBlock:  block2PrevBlockHash,
			HashMerkleRoot: block2MerkleRootHash,
			Bits:           *bits,
		},
		Height:           2,
		CoinbaseTx:       coinbaseTx2,
		TransactionCount: 1,
		Subtrees: []*chainhash.Hash{
			subtree,
		},
	}

	block3 = &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			Timestamp:      1729259727,
			Nonce:          1,
			HashPrevBlock:  block3PrevBlockHash,
			HashMerkleRoot: block3MerkleRootHash,
			Bits:           *bits,
		},
		CoinbaseTx:       coinbaseTx3,
		TransactionCount: 1,
		Subtrees: []*chainhash.Hash{
			subtree,
		},
	}
	blockAlternative2 = &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			Timestamp:      1231469744,
			Nonce:          1639830025,
			HashPrevBlock:  block2PrevBlockHash,
			HashMerkleRoot: block2MerkleRootHash,
			Bits:           *bits,
		},
		CoinbaseTx:       coinbaseTx2,
		TransactionCount: 1,
		Subtrees: []*chainhash.Hash{
			subtree,
		},
	}
)

func assertMainnetGenesis(t *testing.T, blockHeader *model.BlockHeader) {
	assert.Equal(t, "0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206", blockHeader.Hash().String())
	assert.Equal(t, uint32(1), blockHeader.Version)
	assert.Equal(t, &chainhash.Hash{}, blockHeader.HashPrevBlock)
	assert.Equal(t, "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b", blockHeader.HashMerkleRoot.String())
	assert.Equal(t, uint32(1231006505), blockHeader.Timestamp)
	assert.Equal(t, []byte{0xff, 0xff, 0x0, 0x1d}, blockHeader.Bits.CloneBytes())
	assert.Equal(t, uint32(2083236893), blockHeader.Nonce)
}

func assertRegtestGenesis(t *testing.T, blockHeader *model.BlockHeader) {
	assert.Equal(t, "0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206", blockHeader.Hash().String())
	assert.Equal(t, uint32(1), blockHeader.Version)
	assert.Equal(t, &chainhash.Hash{}, blockHeader.HashPrevBlock)
	assert.Equal(t, "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b", blockHeader.HashMerkleRoot.String())
	assert.Equal(t, uint32(1296688602), blockHeader.Timestamp)
	assert.Equal(t, []byte{0xff, 0xff, 0x7f, 0x20}, blockHeader.Bits.CloneBytes())
	assert.Equal(t, uint32(2), blockHeader.Nonce)
}
