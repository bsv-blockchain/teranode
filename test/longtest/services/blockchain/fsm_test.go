package blockchain

import (
	"context"
	"net/url"
	"testing"

	blockchain_service "github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/settings"
	blockchain_store "github.com/bitcoin-sv/teranode/stores/blockchain"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test/mocklogger"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
)

// go test -v -tags test_services_blockchain ./test/...

func Test_GetSetFSMStateFromStore(t *testing.T) {

	storeURL, err := url.Parse("sqlitememory://")
	require.NoError(t, err)

	tSettings := &settings.Settings{
		ChainCfgParams: &chaincfg.MainNetParams,
	}

	blockchainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	ctx := context.Background()
	logger := mocklogger.NewTestLogger()

	blockchainClient, err := blockchain_service.New(ctx, logger, getTestSettings(), blockchainStore, nil)
	require.NoError(t, err)

	err = blockchainClient.Init(ctx)
	require.NoError(t, err)

	resp, err := blockchainClient.GetFSMCurrentState(ctx, &emptypb.Empty{})
	require.NoError(t, err)
	require.Equal(t, "IDLE", resp.State.String())

	t.Run("Get Initial FSM State", func(t *testing.T) {
		state, err := blockchainClient.GetStoreFSMState(ctx)
		require.NoError(t, err)
		require.Equal(t, "IDLE", state)
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
