package blockchain

import (
	"context"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/model"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDifficultyAdjustmentShouldNotChangeDifficultyIfBlocksAreMinedInTime(t *testing.T) {
	os.Setenv("network", "mainnet")

	ctx := context.Background()
	storeURL, err := url.Parse("sqlitememory://")
	require.NoError(t, err)

	coinbaseTx, _ := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff17030100002f6d312d65752f29c267ffea1adb87f33b398fffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000")

	currentTime := time.Now().Unix()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams = &chaincfg.MainNetParams

	blockchainStore, err := blockchainstore.NewStore(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	d, err := NewDifficulty(blockchainStore, ulogger.TestLogger{}, tSettings)
	t.Logf("difficulty: %v", d.powLimitnBits.String())
	require.NoError(t, err)

	// nolint: gosec
	header := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  tSettings.ChainCfgParams.GenesisHash,
		HashMerkleRoot: &chainhash.Hash{},
		Nonce:          1,
		Bits:           *d.powLimitnBits,
		Timestamp:      uint32(currentTime),
	}

	block := &model.Block{
		Header:           header,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: 1,
		Subtrees:         []*chainhash.Hash{},
	}
	_, _, err = blockchainStore.StoreBlock(ctx, block, "")
	require.NoError(t, err)
	// Simulate mining 144 blocks
	prevBlockHash := header.Hash()

	for i := 0; i < 150; i++ {
		coinbaseTx, _ := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff17030200002f6d312d65752f605f77009f74384816a31807ffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000")
		currentTime += int64(tSettings.ChainCfgParams.TargetTimePerBlock.Seconds())
		// t.Logf("current time: %v", currentTime)

		// nolint: gosec
		header := &model.BlockHeader{
			Version:        1,
			Timestamp:      uint32(currentTime),
			Bits:           *d.powLimitnBits,
			Nonce:          1,
			HashPrevBlock:  prevBlockHash,
			HashMerkleRoot: &chainhash.Hash{},
		}
		block := &model.Block{
			Header:           header,
			CoinbaseTx:       coinbaseTx,
			TransactionCount: 1,
			Subtrees:         []*chainhash.Hash{},
		}
		_, _, err := blockchainStore.StoreBlock(ctx, block, "")
		require.NoError(t, err)

		prevBlockHash = header.Hash()
	}

	// Calculate next difficulty
	bestHeader, meta, err := blockchainStore.GetBestBlockHeader(ctx)
	require.NoError(t, err)

	newBits, err := d.CalcNextWorkRequired(ctx, bestHeader, meta.Height, time.Now().Unix())
	require.NoError(t, err)

	t.Logf("newBits: %v", newBits.String())

	// Assert that difficulty has changed
	assert.Equal(t, d.powLimitnBits, newBits)
}

// nolint: gosec
func TestDifficultyAdjustmentShouldChangeDifficultyIfBlocksAreMinedFasterThanExpected(t *testing.T) {
	os.Setenv("network", "mainnet")

	ctx := context.Background()

	storeURL, err := url.Parse("sqlitememory://")
	require.NoError(t, err)

	coinbaseTx, _ := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff17030100002f6d312d65752f29c267ffea1adb87f33b398fffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000")

	currentTime := time.Now().Unix()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams = &chaincfg.MainNetParams

	blockchainStore, err := blockchainstore.NewStore(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	d, err := NewDifficulty(blockchainStore, ulogger.TestLogger{}, tSettings)
	t.Logf("difficulty: %v", d.powLimitnBits.String())
	require.NoError(t, err)

	header := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  tSettings.ChainCfgParams.GenesisHash,
		HashMerkleRoot: &chainhash.Hash{},
		Nonce:          1,
		Bits:           *d.powLimitnBits,
		Timestamp:      uint32(currentTime),
	}
	block := &model.Block{
		Header:           header,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: 1,
		Subtrees:         []*chainhash.Hash{},
	}
	_, _, err = blockchainStore.StoreBlock(ctx, block, "")
	require.NoError(t, err)
	// Simulate mining 144 blocks
	prevBlockHash := header.Hash()

	for i := 0; i < 150; i++ {
		coinbaseTx, _ := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff17030200002f6d312d65752f605f77009f74384816a31807ffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000")
		currentTime += int64(tSettings.ChainCfgParams.TargetTimePerBlock.Seconds() / 10)
		// t.Logf("current time: %v", currentTime)
		header := &model.BlockHeader{
			Version:        1,
			Timestamp:      uint32(currentTime),
			Bits:           *d.powLimitnBits,
			Nonce:          1,
			HashPrevBlock:  prevBlockHash,
			HashMerkleRoot: &chainhash.Hash{},
		}

		block := &model.Block{
			Header:           header,
			CoinbaseTx:       coinbaseTx,
			TransactionCount: 1,
			Subtrees:         []*chainhash.Hash{},
		}

		_, _, err := blockchainStore.StoreBlock(ctx, block, "")
		require.NoError(t, err)

		prevBlockHash = header.Hash()
	}

	// Calculate next difficulty
	bestHeader, meta, err := blockchainStore.GetBestBlockHeader(ctx)
	require.NoError(t, err)

	newBits, err := d.CalcNextWorkRequired(ctx, bestHeader, meta.Height, time.Now().Unix())
	require.NoError(t, err)
	t.Logf("newBits: %v", newBits.String())

	// Assert that difficulty has changed
	assert.NotEqual(t, d.powLimitnBits, newBits)
}
