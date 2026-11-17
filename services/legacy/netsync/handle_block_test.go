package netsync

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestWaitForPreviousBlockMined(t *testing.T) {
	t.Run("returns immediately when parent is mined", func(t *testing.T) {
		blockchainClient := &blockchain.Mock{}
		prevHash := chainhash.HashH([]byte("prev-block"))
		blockchainClient.On("GetBlockIsMined", mock.Anything, &prevHash).Return(true, nil)

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.IsParentMinedRetryMaxRetry = 3
		tSettings.BlockValidation.IsParentMinedRetryBackoffMultiplier = 1
		tSettings.BlockValidation.IsParentMinedRetryBackoffDuration = time.Millisecond

		sm := &SyncManager{
			settings:         tSettings,
			logger:           ulogger.TestLogger{},
			blockchainClient: blockchainClient,
		}

		err := sm.waitForPreviousBlockMined(context.Background(), &prevHash, 100)
		require.NoError(t, err)
		blockchainClient.AssertNumberOfCalls(t, "GetBlockIsMined", 1)
	})

	t.Run("retries when parent is not mined yet then succeeds", func(t *testing.T) {
		blockchainClient := &blockchain.Mock{}
		prevHash := chainhash.HashH([]byte("prev-block"))

		// First two calls: not mined. Third call: mined.
		blockchainClient.On("GetBlockIsMined", mock.Anything, &prevHash).Return(false, nil).Times(2)
		blockchainClient.On("GetBlockIsMined", mock.Anything, &prevHash).Return(true, nil).Once()

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.IsParentMinedRetryMaxRetry = 5
		tSettings.BlockValidation.IsParentMinedRetryBackoffMultiplier = 1
		tSettings.BlockValidation.IsParentMinedRetryBackoffDuration = time.Millisecond

		sm := &SyncManager{
			settings:         tSettings,
			logger:           ulogger.TestLogger{},
			blockchainClient: blockchainClient,
		}

		err := sm.waitForPreviousBlockMined(context.Background(), &prevHash, 100)
		require.NoError(t, err)
		blockchainClient.AssertNumberOfCalls(t, "GetBlockIsMined", 3)
	})

	t.Run("retries on ErrBlockNotFound then succeeds", func(t *testing.T) {
		blockchainClient := &blockchain.Mock{}
		prevHash := chainhash.HashH([]byte("prev-block"))

		// First call: block not found. Second call: mined.
		blockchainClient.On("GetBlockIsMined", mock.Anything, &prevHash).Return(false, errors.ErrBlockNotFound).Once()
		blockchainClient.On("GetBlockIsMined", mock.Anything, &prevHash).Return(true, nil).Once()

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.IsParentMinedRetryMaxRetry = 5
		tSettings.BlockValidation.IsParentMinedRetryBackoffMultiplier = 1
		tSettings.BlockValidation.IsParentMinedRetryBackoffDuration = time.Millisecond

		sm := &SyncManager{
			settings:         tSettings,
			logger:           ulogger.TestLogger{},
			blockchainClient: blockchainClient,
		}

		err := sm.waitForPreviousBlockMined(context.Background(), &prevHash, 100)
		require.NoError(t, err)
		blockchainClient.AssertNumberOfCalls(t, "GetBlockIsMined", 2)
	})

	t.Run("fails after max retries exhausted", func(t *testing.T) {
		blockchainClient := &blockchain.Mock{}
		prevHash := chainhash.HashH([]byte("prev-block"))
		blockchainClient.On("GetBlockIsMined", mock.Anything, &prevHash).Return(false, nil)

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.IsParentMinedRetryMaxRetry = 2
		tSettings.BlockValidation.IsParentMinedRetryBackoffMultiplier = 1
		tSettings.BlockValidation.IsParentMinedRetryBackoffDuration = time.Millisecond

		sm := &SyncManager{
			settings:         tSettings,
			logger:           ulogger.TestLogger{},
			blockchainClient: blockchainClient,
		}

		err := sm.waitForPreviousBlockMined(context.Background(), &prevHash, 100)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not mined yet")
	})
}

func TestSyncManager_quickValidationAllowed(t *testing.T) {
	mainnetHighest := uint32(chaincfg.MainNetParams.Checkpoints[len(chaincfg.MainNetParams.Checkpoints)-1].Height)

	tests := []struct {
		name        string
		chainParams *chaincfg.Params
		height      uint32
		want        bool
	}{
		{
			name:        "nil chain params",
			chainParams: nil,
			height:      100,
			want:        false,
		},
		{
			name:        "regtest has no checkpoints",
			chainParams: &chaincfg.RegressionNetParams,
			height:      0,
			want:        false,
		},
		{
			// Height 0 is fail-closed since the gates collapsed onto
			// model.BelowCheckpoint: genesis carries only a coinbase and never flows
			// through the legacy fast path, so excluding it costs nothing and keeps
			// one boundary definition everywhere.
			name:        "mainnet height 0 fail-closed",
			chainParams: &chaincfg.MainNetParams,
			height:      0,
			want:        false,
		},
		{
			name:        "mainnet height equal to highest checkpoint is covered",
			chainParams: &chaincfg.MainNetParams,
			height:      mainnetHighest,
			want:        true,
		},
		{
			name:        "mainnet height one above highest checkpoint is not covered",
			chainParams: &chaincfg.MainNetParams,
			height:      mainnetHighest + 1,
			want:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sm := &SyncManager{chainParams: tt.chainParams}
			require.Equal(t, tt.want, sm.quickValidationAllowed(headerProven, tt.height))
		})
	}
}
