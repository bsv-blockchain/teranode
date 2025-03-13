//go:build test_all || test_services || test_fsm

package blockchain

import (
	"context"
	"testing"

	"github.com/bitcoin-sv/teranode/chaincfg"
	"github.com/bitcoin-sv/teranode/services/blockchain/blockchain_api"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/util/test/mocklogger"
	"github.com/stretchr/testify/require"
)

func Test_NewFiniteStateMachine(t *testing.T) {
	ctx := context.Background()
	logger := mocklogger.NewTestLogger()
	blockchainClient, err := New(ctx, logger, getTestSettings(), nil, nil)
	require.NoError(t, err)

	fsm := blockchainClient.NewFiniteStateMachine()
	require.NotNil(t, fsm)
	require.Equal(t, "IDLE", fsm.Current())
	require.True(t, fsm.Can(blockchain_api.FSMEventType_RUN.String()))

	// Test transitions
	t.Run("Transition from Idle to Running", func(t *testing.T) {
		err := fsm.Event(ctx, blockchain_api.FSMEventType_RUN.String())
		require.NoError(t, err)
		require.Equal(t, "RUNNING", fsm.Current())
		require.True(t, fsm.Can(blockchain_api.FSMEventType_CATCHUPBLOCKS.String()))
		require.True(t, fsm.Can(blockchain_api.FSMEventType_STOP.String()))
	})

	t.Run("Transition from Running to Catch up Blocks", func(t *testing.T) {
		err = fsm.Event(ctx, blockchain_api.FSMEventType_CATCHUPBLOCKS.String())
		require.NoError(t, err)
		require.Equal(t, "CATCHINGBLOCKS", fsm.Current())
		require.True(t, fsm.Can(blockchain_api.FSMEventType_STOP.String()))
	})
}

func getTestSettings() *settings.Settings {
	return &settings.Settings{
		ChainCfgParams: &chaincfg.RegressionNetParams,
	}
}
