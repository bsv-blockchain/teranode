package sql

import (
	"context"
	"encoding/hex"
	"net/url"
	"testing"

	"github.com/TAAL-GmbH/ubsv/model"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// test data from block 1
var (
	hashPrevBlock, _  = chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
	hashMerkleRoot, _ = chainhash.NewHashFromStr("0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098")
	bits, _           = hex.DecodeString("1d00ffff")
	coinbaseTx, _     = bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000")
	subtree, _        = chainhash.NewHashFromStr("0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098")
)

func Test_StoreBlock(t *testing.T) {
	storeUrl, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(storeUrl)
	require.NoError(t, err)

	err = s.StoreBlock(context.Background(), &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			Timestamp:      1231469665,
			Nonce:          2573394689,
			HashPrevBlock:  hashPrevBlock,
			HashMerkleRoot: hashMerkleRoot,
			Bits:           bits,
		},
		CoinbaseTx:       coinbaseTx,
		TransactionCount: 1,
		Subtrees: []*chainhash.Hash{
			subtree,
		},
	})
	require.NoError(t, err)

	block2PrevBlockHash, err := chainhash.NewHashFromStr("00000000839a8e6886ab5951d76f411475428afc90947ee320161bbf18eb6048")
	require.NoError(t, err)

	block2MerkleRootHash, err := chainhash.NewHashFromStr("9b0fc92260312ce44e74ef369f5c66bbb85848f2eddd5a7a1cde251e54ccfdd5")
	require.NoError(t, err)

	coinbaseTx2, err := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d010bffffffff0100f2052a010000004341047211a824f55b505228e4c3d5194c1fcfaa15a456abdf37f9b9d97a4040afc073dee6c89064984f03385237d92167c13e236446b417ab79a0fcae412ae3316b77ac00000000")
	require.NoError(t, err)

	err = s.StoreBlock(context.Background(), &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			Timestamp:      1231469744,
			Nonce:          1639830024,
			HashPrevBlock:  block2PrevBlockHash,
			HashMerkleRoot: block2MerkleRootHash,
			Bits:           bits,
		},
		CoinbaseTx:       coinbaseTx2,
		TransactionCount: 1,
		Subtrees: []*chainhash.Hash{
			subtree,
		},
	})
	require.NoError(t, err)

}

func Test_getCumulativeChainWork(t *testing.T) {
	t.Run("block 1", func(t *testing.T) {
		work, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000000")
		require.NoError(t, err)

		chainWork, err := getCumulativeChainWork(work, &model.Block{
			Header: &model.BlockHeader{
				Bits: bits,
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "0000000000000000000000000000000000000000000000000000000100010001", chainWork.String())
	})

	t.Run("block 2", func(t *testing.T) {
		work, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000100010001")
		require.NoError(t, err)

		chainWork, err := getCumulativeChainWork(work, &model.Block{
			Header: &model.BlockHeader{
				Bits: bits,
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "0000000000000000000000000000000000000000000000000000000200020002", chainWork.String())
	})

	t.Run("block 796044", func(t *testing.T) {
		work, err := chainhash.NewHashFromStr("000000000000000000000000000000000000000001473b8614ab22c164d42204")
		require.NoError(t, err)

		bitsBytes, _ := hex.DecodeString("1810b7f0")

		chainWork, err := getCumulativeChainWork(work, &model.Block{
			Header: &model.BlockHeader{
				Bits: bitsBytes,
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "000000000000000000000000000000000000000001473b9564a2d255e87e7e86", chainWork.String())
	})
}
