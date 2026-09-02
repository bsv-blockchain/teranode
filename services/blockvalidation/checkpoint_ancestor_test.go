package blockvalidation

// Tests for BlockValidation.checkpointConfirmedAncestor — the caller-supplied ancestry
// predicate that gates the below-checkpoint coinbase no-inflation skip in
// model.checkBlockRewardAndFees. The predicate must be TRUE only when the block is
// provably on the main chain that has reached and matched the pinned checkpoint hash,
// and FALSE (forcing full no-inflation enforcement) in the forward-checkpoint window,
// for a detached fork block, above the checkpoint, and on any lookup error.

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestBlockValidation_checkpointConfirmedAncestor(t *testing.T) {
	const checkpointHeight = uint32(1000)
	const blockHeight = uint32(300) // below the checkpoint

	nBits := model.NBit{0xff, 0xff, 0x00, 0x1d}
	zeroHash := &chainhash.Hash{}

	// Build three headers with distinct hashes (differ by Nonce).
	newHeader := func(nonce uint32) *model.BlockHeader {
		return &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  zeroHash,
			HashMerkleRoot: zeroHash,
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           nBits,
			Nonce:          nonce,
		}
	}

	cpHeader := newHeader(1000)  // the pinned checkpoint block
	bHeader := newHeader(300)    // the block under test
	forkHeader := newHeader(999) // a different block at the same height as b

	pinnedHash := cpHeader.Hash()

	settingsWith := func(cps []chaincfg.Checkpoint) *settings.Settings {
		return &settings.Settings{ChainCfgParams: &chaincfg.Params{Checkpoints: cps}}
	}

	checkpoints := []chaincfg.Checkpoint{{Height: int32(checkpointHeight), Hash: pinnedHash}}
	blockUnderTest := &model.Block{Header: bHeader, Height: blockHeight}

	// The predicate short-circuits unless the store supports the fast path, so every
	// case below uses a store that reports support; the store-gate itself is covered by
	// the checkBlockRewardAndFees unit tests.
	supportingStore := &utxo.MockUtxostore{SupportsOutpointOnlySpendResult: true}

	t.Run("confirmed ancestor: checkpoint reached and block on main chain -> true", func(t *testing.T) {
		mockBC := &blockchain.Mock{}
		mockBC.On("GetBlockHeadersFromHeight", mock.Anything, checkpointHeight, uint32(1)).
			Return([]*model.BlockHeader{cpHeader}, []*model.BlockHeaderMeta{{}}, nil)
		mockBC.On("GetBlockHeadersFromHeight", mock.Anything, blockHeight, uint32(1)).
			Return([]*model.BlockHeader{bHeader}, []*model.BlockHeaderMeta{{}}, nil)

		u := &BlockValidation{settings: settingsWith(checkpoints), blockchainClient: mockBC, utxoStore: supportingStore, logger: ulogger.TestLogger{}}
		require.True(t, u.checkpointConfirmedAncestor(context.Background(), blockUnderTest))
	})

	t.Run("forward-checkpoint window: main chain has not reached the pinned checkpoint -> false", func(t *testing.T) {
		mockBC := &blockchain.Mock{}
		// Main-chain block at the checkpoint height is NOT the pinned block (not yet synced there).
		mockBC.On("GetBlockHeadersFromHeight", mock.Anything, checkpointHeight, uint32(1)).
			Return([]*model.BlockHeader{forkHeader}, []*model.BlockHeaderMeta{{}}, nil)

		u := &BlockValidation{settings: settingsWith(checkpoints), blockchainClient: mockBC, utxoStore: supportingStore, logger: ulogger.TestLogger{}}
		require.False(t, u.checkpointConfirmedAncestor(context.Background(), blockUnderTest))
		// The block-height lookup must never happen once the checkpoint clause fails.
		mockBC.AssertNotCalled(t, "GetBlockHeadersFromHeight", mock.Anything, blockHeight, uint32(1))
	})

	t.Run("detached fork: checkpoint reached but block is not the main-chain block at its height -> false", func(t *testing.T) {
		mockBC := &blockchain.Mock{}
		mockBC.On("GetBlockHeadersFromHeight", mock.Anything, checkpointHeight, uint32(1)).
			Return([]*model.BlockHeader{cpHeader}, []*model.BlockHeaderMeta{{}}, nil)
		// Main-chain block at blockHeight is a DIFFERENT block than the one being validated.
		mockBC.On("GetBlockHeadersFromHeight", mock.Anything, blockHeight, uint32(1)).
			Return([]*model.BlockHeader{forkHeader}, []*model.BlockHeaderMeta{{}}, nil)

		u := &BlockValidation{settings: settingsWith(checkpoints), blockchainClient: mockBC, utxoStore: supportingStore, logger: ulogger.TestLogger{}}
		require.False(t, u.checkpointConfirmedAncestor(context.Background(), blockUnderTest))
	})

	t.Run("above the checkpoint: returns false without any blockchain query", func(t *testing.T) {
		mockBC := &blockchain.Mock{}
		mockBC.On("GetBlockHeadersFromHeight", mock.Anything, mock.Anything, mock.Anything).
			Return([]*model.BlockHeader{cpHeader}, []*model.BlockHeaderMeta{{}}, nil).Maybe()

		u := &BlockValidation{settings: settingsWith(checkpoints), blockchainClient: mockBC, utxoStore: supportingStore, logger: ulogger.TestLogger{}}
		aboveBlock := &model.Block{Header: bHeader, Height: checkpointHeight + 1}
		require.False(t, u.checkpointConfirmedAncestor(context.Background(), aboveBlock))
		mockBC.AssertNotCalled(t, "GetBlockHeadersFromHeight", mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("no checkpoints configured: returns false without any blockchain query", func(t *testing.T) {
		mockBC := &blockchain.Mock{}
		mockBC.On("GetBlockHeadersFromHeight", mock.Anything, mock.Anything, mock.Anything).
			Return([]*model.BlockHeader{cpHeader}, []*model.BlockHeaderMeta{{}}, nil).Maybe()

		u := &BlockValidation{settings: settingsWith(nil), blockchainClient: mockBC, utxoStore: supportingStore, logger: ulogger.TestLogger{}}
		require.False(t, u.checkpointConfirmedAncestor(context.Background(), blockUnderTest))
		mockBC.AssertNotCalled(t, "GetBlockHeadersFromHeight", mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("lookup error is fail-safe: returns false", func(t *testing.T) {
		mockBC := &blockchain.Mock{}
		mockBC.On("GetBlockHeadersFromHeight", mock.Anything, checkpointHeight, uint32(1)).
			Return([]*model.BlockHeader(nil), []*model.BlockHeaderMeta(nil), errors.NewServiceError("boom"))

		u := &BlockValidation{settings: settingsWith(checkpoints), blockchainClient: mockBC, utxoStore: supportingStore, logger: ulogger.TestLogger{}}
		require.False(t, u.checkpointConfirmedAncestor(context.Background(), blockUnderTest))
	})
}
