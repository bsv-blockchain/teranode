package sql

import (
	"context"
	"math/big"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util/test"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStoreBlock(t *testing.T) {
	tSettings := test.CreateBaseTestSettings()

	t.Run("store block 1", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings.ChainCfgParams)
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)
	})

	t.Run("store invalid child", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings.ChainCfgParams)
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)

		err = s.InvalidateBlock(context.Background(), block1.Hash())
		require.NoError(t, err)

		var blockInvalid bool
		err = s.db.QueryRow("SELECT invalid FROM blocks WHERE hash = ?", block1.Hash().CloneBytes()).Scan(&blockInvalid)
		require.NoError(t, err)
		assert.True(t, blockInvalid)

		_, _, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)

		// block 2 should be invalid
		err = s.db.QueryRow("SELECT invalid FROM blocks WHERE hash = ?", block2.Hash().CloneBytes()).Scan(&blockInvalid)
		require.NoError(t, err)
		assert.True(t, blockInvalid)
	})
}

func TestGetCumulativeChainWork(t *testing.T) {
	tSettings := test.CreateBaseTestSettings()

	t.Run("block 1", func(t *testing.T) {
		work, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000000")
		require.NoError(t, err)

		chainWork, err := getCumulativeChainWork(work, &model.Block{
			Header: &model.BlockHeader{
				Bits: *bits,
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "0000000000000000000000000000000000000000000000000000000000000002", chainWork.String())
	})

	t.Run("block 2", func(t *testing.T) {
		work, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000100010001")
		require.NoError(t, err)

		chainWork, err := getCumulativeChainWork(work, &model.Block{
			Header: &model.BlockHeader{
				Bits: *bits,
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "0000000000000000000000000000000000000000000000000000000100010003", chainWork.String())
	})

	t.Run("block 796044", func(t *testing.T) {
		work, err := chainhash.NewHashFromStr("000000000000000000000000000000000000000001473b8614ab22c164d42204")
		require.NoError(t, err)

		nBit, _ := model.NewNBitFromString("1810b7f0")
		chainWork, err := getCumulativeChainWork(work, &model.Block{
			Header: &model.BlockHeader{
				Bits: *nBit,
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "000000000000000000000000000000000000000001473b9564a2d255e87e7e86", chainWork.String())
	})

	t.Run("verify chain work", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings.ChainCfgParams)
		require.NoError(t, err)

		h, _, _ := s.GetBestBlockHeader(context.Background())

		assertRegtestGenesis(t, h)

		firstBlock := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  h.Hash(),
				HashMerkleRoot: &chainhash.Hash{},
				Timestamp:      1231469744,
				Bits:           *bits,
				Nonce:          1639830024,
			},
			TransactionCount: 1,
			SizeInBytes:      1,
			Subtrees:         []*chainhash.Hash{},
			CoinbaseTx:       coinbaseTx,
		}
		_, _, err = s.StoreBlock(context.Background(), firstBlock, "")
		require.NoError(t, err)

		var chainWorkFirstBlock []byte

		err = s.db.QueryRow("SELECT chain_work FROM blocks WHERE hash = ?", firstBlock.Hash().CloneBytes()).Scan(&chainWorkFirstBlock)
		require.NoError(t, err)

		chainWorkFirstBlockBigInt := new(big.Int).SetBytes(chainWorkFirstBlock)
		t.Logf("first chain work: %s", chainWorkFirstBlockBigInt.String())

		secondBlock := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  firstBlock.Hash(),
				HashMerkleRoot: &chainhash.Hash{},
				Timestamp:      1231469744,
				Bits:           *bits,
				Nonce:          1639830024,
			},
			TransactionCount: 1,
			SizeInBytes:      1,
			Subtrees:         []*chainhash.Hash{},
			CoinbaseTx:       coinbaseTx2,
		}

		var chainWorkSecondBlock []byte

		_, _, err = s.StoreBlock(context.Background(), secondBlock, "")
		require.NoError(t, err)

		err = s.db.QueryRow("SELECT chain_work FROM blocks WHERE hash = ?", secondBlock.Hash().CloneBytes()).Scan(&chainWorkSecondBlock)
		require.NoError(t, err)

		chainWorkSecondBlockBigInt := new(big.Int).SetBytes(chainWorkSecondBlock)
		t.Logf("second chain work: %s", chainWorkSecondBlockBigInt.String())

		assert.True(t, chainWorkSecondBlockBigInt.Cmp(chainWorkFirstBlockBigInt) > 0)
	})

	t.Run("verify chain work invalidate blocks", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings.ChainCfgParams)
		require.NoError(t, err)

		h, _, _ := s.GetBestBlockHeader(context.Background())

		assertRegtestGenesis(t, h)

		firstBlock := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  h.Hash(),
				HashMerkleRoot: &chainhash.Hash{},
				Timestamp:      h.Timestamp + 1,
				Bits:           *bits,
				Nonce:          1639830024,
			},
			TransactionCount: 1,
			SizeInBytes:      1,
			Subtrees:         []*chainhash.Hash{},
			CoinbaseTx:       coinbaseTx,
		}
		_, _, err = s.StoreBlock(context.Background(), firstBlock, "")
		require.NoError(t, err)

		var chainWorkFirstBlock []byte

		err = s.db.QueryRow("SELECT chain_work FROM blocks WHERE hash = ?", firstBlock.Hash().CloneBytes()).Scan(&chainWorkFirstBlock)
		require.NoError(t, err)

		chainWorkFirstBlockBigInt := new(big.Int).SetBytes(chainWorkFirstBlock)
		t.Logf("first chain work: %s", chainWorkFirstBlockBigInt.String())

		secondBlock := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  firstBlock.Hash(),
				HashMerkleRoot: &chainhash.Hash{},
				Timestamp:      firstBlock.Header.Timestamp + 1,
				Bits:           *bits,
				Nonce:          1639830024,
			},
			TransactionCount: 1,
			SizeInBytes:      1,
			Subtrees:         []*chainhash.Hash{},
			CoinbaseTx:       coinbaseTx2,
		}

		var chainWorkSecondBlock []byte

		_, _, err = s.StoreBlock(context.Background(), secondBlock, "")
		require.NoError(t, err)

		err = s.db.QueryRow("SELECT chain_work FROM blocks WHERE hash = ?", secondBlock.Hash().CloneBytes()).Scan(&chainWorkSecondBlock)
		require.NoError(t, err)

		chainWorkSecondBlockBigInt := new(big.Int).SetBytes(chainWorkSecondBlock)
		t.Logf("second chain work: %s", chainWorkSecondBlockBigInt.String())

		assert.True(t, chainWorkSecondBlockBigInt.Cmp(chainWorkFirstBlockBigInt) > 0)

		thirdBlock := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  secondBlock.Hash(),
				HashMerkleRoot: &chainhash.Hash{},
				Timestamp:      secondBlock.Header.Timestamp + 1,
				Bits:           *bits,
				Nonce:          1844305925,
			},
			TransactionCount: 1,
			SizeInBytes:      1,
			Subtrees:         []*chainhash.Hash{},
			CoinbaseTx:       coinbaseTx2,
		}

		var chainWorkThirdBlock []byte

		_, _, err = s.StoreBlock(context.Background(), thirdBlock, "")
		require.NoError(t, err)

		err = s.db.QueryRow("SELECT chain_work FROM blocks WHERE hash = ?", thirdBlock.Hash().CloneBytes()).Scan(&chainWorkThirdBlock)
		require.NoError(t, err)

		chainWorkThirdBlockBigInt := new(big.Int).SetBytes(chainWorkThirdBlock)
		t.Logf("third chain work: %s", chainWorkThirdBlockBigInt.String())

		assert.True(t, chainWorkThirdBlockBigInt.Cmp(chainWorkSecondBlockBigInt) > 0)

		// Invalidate the second block
		err = s.InvalidateBlock(context.Background(), secondBlock.Hash())
		require.NoError(t, err)

		// get the best block header
		h, _, _ = s.GetBestBlockHeader(context.Background())
		// The best block header should be the first block after genesis
		assert.Equal(t, firstBlock.Hash().String(), h.Hash().String())

		// add a 4th block on top of the 3rd block
		fourthBlock := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  thirdBlock.Hash(),
				HashMerkleRoot: &chainhash.Hash{},
				Timestamp:      thirdBlock.Header.Timestamp + 1,
				Bits:           *bits,
				Nonce:          1844305925,
			},
			TransactionCount: 1,
			SizeInBytes:      1,
			Subtrees:         []*chainhash.Hash{},
			CoinbaseTx:       coinbaseTx2,
		}

		_, _, err = s.StoreBlock(context.Background(), fourthBlock, "")
		require.NoError(t, err)

		// get the best block header
		h, _, _ = s.GetBestBlockHeader(context.Background())
		// The best block header should be the first block after genesis
		assert.Equal(t, firstBlock.Hash().String(), h.Hash().String())

		// add a new block on top the chain tip
		fifthBlock := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  firstBlock.Hash(),
				HashMerkleRoot: &chainhash.Hash{},
				Timestamp:      fourthBlock.Header.Timestamp + 1,
				Bits:           *bits,
				Nonce:          1639830024,
			},
			TransactionCount: 1,
			SizeInBytes:      1,
			Subtrees:         []*chainhash.Hash{},
			CoinbaseTx:       coinbaseTx2,
		}

		_, _, err = s.StoreBlock(context.Background(), fifthBlock, "")
		require.NoError(t, err)

		// get the best block header
		h, _, _ = s.GetBestBlockHeader(context.Background())
		// The best block header should be the first block after genesis
		assert.Equal(t, fifthBlock.Hash().String(), h.Hash().String())

		// Get the chain work of the best block header
		var chainWork []byte
		err = s.db.QueryRow("SELECT chain_work FROM blocks WHERE hash = ?", h.Hash().CloneBytes()).Scan(&chainWork)
		require.NoError(t, err)

		chainWorkBigInt := new(big.Int).SetBytes(chainWork)
		t.Logf("forked chain work: %s", chainWorkBigInt.String())

		// assert that this chaiwork is the same as the third block
		assert.Equal(t, chainWorkSecondBlockBigInt.String(), chainWorkBigInt.String())
	})
}
