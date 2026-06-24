package blockvalidation

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestHandleNoNewHeaders covers the peer-selection short-circuit fix.
// When a peer returns zero new headers after common-ancestor filtering, the
// outer peer-selection loop uses the return value of catchup() to decide
// whether to try other peers. A peer on a dead fork can legitimately return
// zero new headers while blockUpTo is still unknown to us — in that case we
// must surface an error so the caller tries the next peer, not pretend we
// are already synced. The local-existence check uses GetBlockExists, which is
// a presence check against the blocks table (main chain and known off-chain
// blocks both count as "known locally").
func TestHandleNoNewHeaders(t *testing.T) {
	t.Run("TargetExistsLocallyReturnsNil", func(t *testing.T) {
		ctx := context.Background()
		server, mockBlockchainClient, _, cleanup := setupTestCatchupServer(t)
		defer cleanup()

		headers := testhelpers.CreateTestHeaders(t, 1)
		targetBlock := &model.Block{Header: headers[0], Height: 1000}

		mockBlockchainClient.On("GetBlockExists", mock.Anything, targetBlock.Hash()).
			Return(true, nil)

		err := server.handleNoNewHeaders(ctx, targetBlock)
		require.NoError(t, err, "already-synced peer with target known locally must return nil")
	})

	t.Run("TargetNotKnownLocallyReturnsError", func(t *testing.T) {
		ctx := context.Background()
		server, mockBlockchainClient, _, cleanup := setupTestCatchupServer(t)
		defer cleanup()

		headers := testhelpers.CreateTestHeaders(t, 1)
		targetBlock := &model.Block{Header: headers[0], Height: 1000}

		mockBlockchainClient.On("GetBlockExists", mock.Anything, targetBlock.Hash()).
			Return(false, nil)

		err := server.handleNoNewHeaders(ctx, targetBlock)
		require.Error(t, err, "forked peer that cannot reach target must surface an error so the caller tries another peer")
		require.Contains(t, err.Error(), "dead fork")
	})

	t.Run("ExistenceCheckFailureReturnsError", func(t *testing.T) {
		ctx := context.Background()
		server, mockBlockchainClient, _, cleanup := setupTestCatchupServer(t)
		defer cleanup()

		headers := testhelpers.CreateTestHeaders(t, 1)
		targetBlock := &model.Block{Header: headers[0], Height: 1000}

		storeErr := errors.NewServiceError("store unavailable")
		mockBlockchainClient.On("GetBlockExists", mock.Anything, mock.AnythingOfType("*chainhash.Hash")).
			Return(false, storeErr)

		err := server.handleNoNewHeaders(ctx, targetBlock)
		require.Error(t, err, "existence check failure must not be swallowed as success")
		require.Contains(t, err.Error(), "block existence check failed")
	})

	t.Run("UsesTargetBlockHash", func(t *testing.T) {
		ctx := context.Background()
		server, mockBlockchainClient, _, cleanup := setupTestCatchupServer(t)
		defer cleanup()

		headers := testhelpers.CreateTestHeaders(t, 1)
		targetBlock := &model.Block{Header: headers[0], Height: 1000}

		var observed *chainhash.Hash
		mockBlockchainClient.On("GetBlockExists", mock.Anything, mock.AnythingOfType("*chainhash.Hash")).
			Run(func(args mock.Arguments) {
				observed = args.Get(1).(*chainhash.Hash)
			}).
			Return(true, nil)

		err := server.handleNoNewHeaders(ctx, targetBlock)
		require.NoError(t, err)
		require.NotNil(t, observed)
		require.True(t, observed.IsEqual(targetBlock.Hash()),
			"handleNoNewHeaders must check the target block, not some other hash")
	})
}
