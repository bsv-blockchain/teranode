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
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/blockchain/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestDifficulty_GetHashOfAncestorBlock_TimeoutReturnsError verifies that when
// GetHashOfAncestorBlock fails with a non-ErrNotFound error (e.g., timeout),
// CalcNextWorkRequired returns an error instead of silently falling back to
// using the current block hash as the ancestor.
//
// This test was created to verify the fix for a production bug where:
// 1. GetHashOfAncestorBlock had a 1-second timeout that could be exceeded on busy databases
// 2. When timeout occurred, the code silently fell back to using blockHeader.Hash()
// 3. This caused firstSuitableBlock == lastSuitableBlock, making duration == 0
// 4. computeTarget() returned lastSuitableBits directly when duration == 0
// 5. Result: Wrong "expected" difficulty, causing valid blocks to be rejected
//
// The fix:
// 1. Removed the 1-second timeout from GetHashOfAncestorBlock
// 2. Changed Difficulty.CalcNextWorkRequired to only fallback for ErrNotFound (chain too short)
// 3. For other errors (timeouts, DB errors), it now returns an error
func TestDifficulty_GetHashOfAncestorBlock_TimeoutReturnsError(t *testing.T) {
	os.Setenv("network", "mainnet")

	ctx := context.Background()
	storeURL, err := url.Parse("sqlitememory://")
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams = &chaincfg.MainNetParams

	blockchainStore, err := blockchainstore.NewStore(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	d, err := NewDifficulty(blockchainStore, ulogger.TestLogger{}, tSettings)
	require.NoError(t, err)

	// Create 200 blocks (more than DifficultyAdjustmentWindow of 144)
	currentTime := time.Now().Unix()
	prevBlockHash := tSettings.ChainCfgParams.GenesisHash

	coinbaseTx, _ := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff17030100002f6d312d65752f29c267ffea1adb87f33b398fffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000")

	for i := 0; i < 200; i++ {
		currentTime += int64(tSettings.ChainCfgParams.TargetTimePerBlock.Seconds())
		header := &model.BlockHeader{
			Version:        1,
			Timestamp:      uint32(currentTime),
			Bits:           *d.powLimitnBits,
			Nonce:          uint32(i),
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

	// Get best block for difficulty calculation
	bestHeader, meta, err := blockchainStore.GetBestBlockHeader(ctx)
	require.NoError(t, err)

	// First, calculate difficulty with normal behavior (GetHashOfAncestorBlock works)
	nBits, err := d.CalcNextWorkRequired(ctx, bestHeader, meta.Height, time.Now().Unix())
	require.NoError(t, err)
	require.NotNil(t, nBits)
	t.Logf("Correct difficulty calculation (ancestor lookup works): %s", nBits.String())

	// Now simulate GetHashOfAncestorBlock timeout/failure
	// We wrap the store with a mock that returns context.DeadlineExceeded
	sqlStore := blockchainStore.(*sql.SQL)
	mockStore := &mockStoreWithFailingAncestor{SQL: sqlStore, failWithTimeout: true}
	difficultyWithMock, err := NewDifficulty(mockStore, ulogger.TestLogger{}, tSettings)
	require.NoError(t, err)

	_, err = difficultyWithMock.CalcNextWorkRequired(ctx, bestHeader, meta.Height, time.Now().Unix())
	require.Error(t, err, "CalcNextWorkRequired should return error when GetHashOfAncestorBlock fails with non-ErrNotFound")
	require.False(t, errors.Is(err, errors.ErrNotFound), "Error should not be ErrNotFound")

	t.Logf("FIX VERIFIED: When GetHashOfAncestorBlock fails with timeout:")
	t.Logf("  - Correct difficulty: %s", nBits.String())
	t.Logf("  - With timeout: returns error instead of wrong difficulty")
	t.Logf("  - Error: %v", err)
}

// TestDifficulty_GetHashOfAncestorBlock_ChainTooShortUsesFallback verifies that when
// GetHashOfAncestorBlock fails because the chain is too short (ErrNotFound),
// CalcNextWorkRequired correctly falls back to using the current block hash.
// This is the expected behavior for new chains.
func TestDifficulty_GetHashOfAncestorBlock_ChainTooShortUsesFallback(t *testing.T) {
	os.Setenv("network", "mainnet")

	ctx := context.Background()
	storeURL, err := url.Parse("sqlitememory://")
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams = &chaincfg.MainNetParams

	blockchainStore, err := blockchainstore.NewStore(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	d, err := NewDifficulty(blockchainStore, ulogger.TestLogger{}, tSettings)
	require.NoError(t, err)

	// Create only 50 blocks (less than DifficultyAdjustmentWindow of 144)
	currentTime := time.Now().Unix()
	prevBlockHash := tSettings.ChainCfgParams.GenesisHash

	coinbaseTx, _ := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff17030100002f6d312d65752f29c267ffea1adb87f33b398fffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000")

	for i := 0; i < 50; i++ {
		currentTime += int64(tSettings.ChainCfgParams.TargetTimePerBlock.Seconds())
		header := &model.BlockHeader{
			Version:        1,
			Timestamp:      uint32(currentTime),
			Bits:           *d.powLimitnBits,
			Nonce:          uint32(i),
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

	// Get best block
	bestHeader, meta, err := blockchainStore.GetBestBlockHeader(ctx)
	require.NoError(t, err)

	// This should succeed with fallback (chain too short is expected)
	nBits, err := d.CalcNextWorkRequired(ctx, bestHeader, meta.Height, time.Now().Unix())
	require.NoError(t, err, "CalcNextWorkRequired should succeed with fallback when chain is too short")
	require.NotNil(t, nBits)

	t.Logf("Short chain fallback works correctly: nBits=%s", nBits.String())
}

// mockStoreWithFailingAncestor wraps a real store but can simulate GetHashOfAncestorBlock failures
type mockStoreWithFailingAncestor struct {
	*sql.SQL
	failWithTimeout bool
}

func (m *mockStoreWithFailingAncestor) GetHashOfAncestorBlock(ctx context.Context, hash *chainhash.Hash, depth int) (*chainhash.Hash, error) {
	if m.failWithTimeout {
		// Simulate a timeout error (not ErrNotFound)
		return nil, context.DeadlineExceeded
	}
	return m.SQL.GetHashOfAncestorBlock(ctx, hash, depth)
}
