package sql

import (
	"encoding/hex"
	"testing"

	"github.com/TAAL-GmbH/ubsv/model"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/stretchr/testify/assert"
)

var (
	hashPrevBlock, _  = chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
	hashMerkleRoot, _ = chainhash.NewHashFromStr("0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098")
	bits, _           = hex.DecodeString("1d00ffff")
	coinbaseTx, _     = bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000")
	subtree, _        = chainhash.NewHashFromStr("0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098")

	block2PrevBlockHash, _  = chainhash.NewHashFromStr("00000000839a8e6886ab5951d76f411475428afc90947ee320161bbf18eb6048")
	block2MerkleRootHash, _ = chainhash.NewHashFromStr("9b0fc92260312ce44e74ef369f5c66bbb85848f2eddd5a7a1cde251e54ccfdd5")
	coinbaseTx2, _          = bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d010bffffffff0100f2052a010000004341047211a824f55b505228e4c3d5194c1fcfaa15a456abdf37f9b9d97a4040afc073dee6c89064984f03385237d92167c13e236446b417ab79a0fcae412ae3316b77ac00000000")
	block1                  = &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			Timestamp:      1231469665,
			Nonce:          2573394689,
			HashPrevBlock:  hashPrevBlock,
			HashMerkleRoot: hashMerkleRoot,
			Bits:           model.NewNBitFromSlice(bits),
		},
		CoinbaseTx:       coinbaseTx,
		TransactionCount: 1,
		Subtrees: []*chainhash.Hash{
			subtree,
		},
	}
	block2 = &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			Timestamp:      1231469744,
			Nonce:          1639830024,
			HashPrevBlock:  block2PrevBlockHash,
			HashMerkleRoot: block2MerkleRootHash,
			Bits:           model.NewNBitFromSlice(bits),
		},
		CoinbaseTx:       coinbaseTx2,
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
			Bits:           model.NewNBitFromSlice(bits),
		},
		CoinbaseTx:       coinbaseTx2,
		TransactionCount: 1,
		Subtrees: []*chainhash.Hash{
			subtree,
		},
	}
)

func assertGenesis(t *testing.T, blockHeader *model.BlockHeader) {
	assert.Equal(t, "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f", blockHeader.Hash().String())
	assert.Equal(t, uint32(1), blockHeader.Version)
	assert.Equal(t, &chainhash.Hash{}, blockHeader.HashPrevBlock)
	assert.Equal(t, "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b", blockHeader.HashMerkleRoot.String())
	assert.Equal(t, uint32(1231006505), blockHeader.Timestamp)
	assert.Equal(t, []byte{0x1d, 0x0, 0xff, 0xff}, blockHeader.Bits.CloneBytes())
	assert.Equal(t, uint32(2083236893), blockHeader.Nonce)
}
