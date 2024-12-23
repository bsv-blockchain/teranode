//go:build test_all || test_services || test_services_blockchain

package blockchain

import (
	"context"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/teranode/chaincfg"
	blockchain_service "github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/settings"
	blockchain_store "github.com/bitcoin-sv/teranode/stores/blockchain"
	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test/mock_logger"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
)

// go test -v -tags test_services_blockchain ./test/...

func Test_GetSetFSMStateFromStore(t *testing.T) {
	connStr, teardown, err := helper.SetupTestPostgresContainer()
	require.NoError(t, err)

	defer func() {
		err := teardown()
		require.NoError(t, err)
	}()

	storeURL, err := url.Parse(connStr)
	require.NoError(t, err)

	blockchainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, storeURL, &chaincfg.MainNetParams)
	require.NoError(t, err)

	ctx := context.Background()
	logger := mock_logger.NewTestLogger()

	blockchainClient, err := blockchain_service.New(ctx, logger, getTestSettings(), blockchainStore, nil)
	require.NoError(t, err)

	err = blockchainClient.Init(ctx)
	require.NoError(t, err)

	resp, err := blockchainClient.GetFSMCurrentState(ctx, &emptypb.Empty{})
	require.NoError(t, err)
	require.Equal(t, "STOPPED", resp.State.String())

	t.Run("Get Initial FSM State", func(t *testing.T) {
		state, err := blockchainClient.GetStoreFSMState(ctx)
		require.NoError(t, err)
		require.Equal(t, "STOPPED", state)
	})

	t.Run("Alter current state to Running", func(t *testing.T) {
		_, err = blockchainClient.Run(ctx, &emptypb.Empty{})
		require.NoError(t, err)

		resp, err := blockchainClient.GetFSMCurrentState(ctx, &emptypb.Empty{})
		require.NoError(t, err)
		require.Equal(t, "RUNNING", resp.State.String())

		state, err := blockchainClient.GetStoreFSMState(ctx)
		require.NoError(t, err)
		require.Equal(t, "RUNNING", state)
	})

	t.Run("Alter current state to Catchup Blocks", func(t *testing.T) {
		_, err = blockchainClient.CatchUpBlocks(ctx, &emptypb.Empty{})
		require.NoError(t, err)

		resp, err := blockchainClient.GetFSMCurrentState(ctx, &emptypb.Empty{})
		require.NoError(t, err)
		require.Equal(t, "CATCHINGBLOCKS", resp.State.String())

		state, err := blockchainClient.GetStoreFSMState(ctx)
		require.NoError(t, err)
		require.Equal(t, "CATCHINGBLOCKS", state)
	})

	t.Run("Simulate re-initializing blockchain service", func(t *testing.T) {
		// Step 1 Simulate restarting the blockchain service
		blockchainClient.ResetFSMS()

		// Step 2 Re-initialize the blockchain service
		// This should restore the state to the last known state from DB
		err = blockchainClient.Init(ctx)
		require.NoError(t, err)

		resp, err = blockchainClient.GetFSMCurrentState(ctx, &emptypb.Empty{})
		require.NoError(t, err)
		require.Equal(t, "CATCHINGBLOCKS", resp.State.String())

		state, err := blockchainClient.GetStoreFSMState(ctx)
		require.NoError(t, err)
		require.Equal(t, "CATCHINGBLOCKS", state)
	})

	t.Run("Alter current state to Running again", func(t *testing.T) {
		_, err = blockchainClient.Run(ctx, &emptypb.Empty{})
		require.NoError(t, err)

		resp, err := blockchainClient.GetFSMCurrentState(ctx, &emptypb.Empty{})
		require.NoError(t, err)
		require.Equal(t, "RUNNING", resp.State.String())

		state, err := blockchainClient.GetStoreFSMState(ctx)
		require.NoError(t, err)
		require.Equal(t, "RUNNING", state)
	})
}

func getTestSettings() *settings.Settings {
	return &settings.Settings{
		ChainCfgParams: &chaincfg.RegressionNetParams,
	}
}
