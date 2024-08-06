package blockchain

import (
	"context"
	"testing"

	"github.com/bitcoin-sv/ubsv/services/blockchain/blockchain_api"
	"github.com/bitcoin-sv/ubsv/util/test/mock_logger"
	"github.com/stretchr/testify/require"
)

func Test_NewFiniteStateMachine(t *testing.T) {
	ctx := context.Background()
	logger := mock_logger.NewTestLogger()
	blockchainClient, err := New(ctx, logger, nil, nil, nil)
	require.NoError(t, err)

	fsm := blockchainClient.NewFiniteStateMachine()
	require.NotNil(t, fsm)
	require.Equal(t, "STOPPED", fsm.Current())
	require.True(t, fsm.Can(blockchain_api.FSMEventType_RUN.String()))

	// Test transitions
	t.Run("Transition from Stopped to Running", func(t *testing.T) {
		err := fsm.Event(ctx, blockchain_api.FSMEventType_RUN.String())
		require.NoError(t, err)
		require.Equal(t, "RUNNING", fsm.Current())
		require.True(t, fsm.Can(blockchain_api.FSMEventType_MINE.String()))
		require.True(t, fsm.Can(blockchain_api.FSMEventType_STOP.String()))
	})

	t.Run("Transition from Running to Mining", func(t *testing.T) {
		// Try to set the state to Runningm again
		err := fsm.Event(ctx, blockchain_api.FSMEventType_RUN.String())
		require.Error(t, err)

		// Transition to Mining
		err = fsm.Event(ctx, blockchain_api.FSMEventType_MINE.String())
		require.NoError(t, err)
		require.Equal(t, "MINING", fsm.Current())
		require.True(t, fsm.Can(blockchain_api.FSMEventType_CATCHUPBLOCKS.String()))
		require.True(t, fsm.Can(blockchain_api.FSMEventType_STOP.String()))
	})

	t.Run("Transition from Mining to Catch up Blocks", func(t *testing.T) {
		// Stop mining, transition to Running
		err = fsm.Event(ctx, blockchain_api.FSMEventType_CATCHUPBLOCKS.String())
		require.NoError(t, err)
		require.Equal(t, "CATCHINGBLOCKS", fsm.Current())
		require.True(t, fsm.Can(blockchain_api.FSMEventType_MINE.String()))
		require.True(t, fsm.Can(blockchain_api.FSMEventType_STOP.String()))
	})

	t.Run("Transition from Catch up Blocks to Catch up Transactions", func(t *testing.T) {
		require.Equal(t, "CATCHINGBLOCKS", fsm.Current())
		err = fsm.Event(ctx, blockchain_api.FSMEventType_CATCHUPTXS.String())
		require.Error(t, err)
		require.Equal(t, "CATCHINGBLOCKS", fsm.Current())
		require.True(t, fsm.Can(blockchain_api.FSMEventType_MINE.String()))
		require.True(t, fsm.Can(blockchain_api.FSMEventType_STOP.String()))
	})

	t.Run("Transition from Catch up Blocks to Mining", func(t *testing.T) {
		require.Equal(t, "CATCHINGBLOCKS", fsm.Current())
		err = fsm.Event(ctx, blockchain_api.FSMEventType_MINE.String())
		require.NoError(t, err)
		require.Equal(t, "MINING", fsm.Current())
		require.True(t, fsm.Can(blockchain_api.FSMEventType_CATCHUPBLOCKS.String()))
		require.True(t, fsm.Can(blockchain_api.FSMEventType_STOP.String()))
	})

	t.Run("Transition from Mining to Catch up Transactions", func(t *testing.T) {
		require.Equal(t, "MINING", fsm.Current())
		err = fsm.Event(ctx, blockchain_api.FSMEventType_CATCHUPTXS.String())
		require.NoError(t, err)
		require.Equal(t, "CATCHINGTXS", fsm.Current())
		require.True(t, fsm.Can(blockchain_api.FSMEventType_MINE.String()))
		require.True(t, fsm.Can(blockchain_api.FSMEventType_STOP.String()))
	})

	t.Run("Transition from Mining to Stopped", func(t *testing.T) {
		// Try to set the state to Stopped, again
		err = fsm.Event(ctx, blockchain_api.FSMEventType_STOP.String())
		require.NoError(t, err)
		require.Equal(t, "STOPPED", fsm.Current())
		require.True(t, fsm.Can(blockchain_api.FSMEventType_RUN.String()))
	})
}
