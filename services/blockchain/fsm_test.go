package blockchain

import (
	"context"
	blockchain_store "github.com/bitcoin-sv/ubsv/stores/blockchain"
	"github.com/ordishs/gocore"
	"testing"

	"github.com/bitcoin-sv/ubsv/services/blockchain/blockchain_api"
	"github.com/bitcoin-sv/ubsv/util/test/mock_logger"
	"github.com/stretchr/testify/require"
)

func Test_NewFiniteStateMachine(t *testing.T) {
	ctx := context.Background()
	logger := mock_logger.NewTestLogger()
	blockchainClient, err := New(ctx, logger, nil)
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
		require.True(t, fsm.Can(blockchain_api.FSMEventType_CATCHUPTXS.String()))
		require.True(t, fsm.Can(blockchain_api.FSMEventType_CATCHUPBLOCKS.String()))
		require.True(t, fsm.Can(blockchain_api.FSMEventType_STOP.String()))
	})

	t.Run("Transition from Running to Catch up Blocks", func(t *testing.T) {
		err = fsm.Event(ctx, blockchain_api.FSMEventType_CATCHUPBLOCKS.String())
		require.NoError(t, err)
		require.Equal(t, "CATCHINGBLOCKS", fsm.Current())
		require.True(t, fsm.Can(blockchain_api.FSMEventType_CATCHUPTXS.String()))
		require.True(t, fsm.Can(blockchain_api.FSMEventType_STOP.String()))
	})

	t.Run("Transition from Catch up Blocks to Catch up Transactions", func(t *testing.T) {
		require.Equal(t, "CATCHINGBLOCKS", fsm.Current())
		err = fsm.Event(ctx, blockchain_api.FSMEventType_CATCHUPTXS.String())
		require.NoError(t, err)
		require.Equal(t, "CATCHINGTXS", fsm.Current())
		require.True(t, fsm.Can(blockchain_api.FSMEventType_RUN.String()))
		require.True(t, fsm.Can(blockchain_api.FSMEventType_STOP.String()))
	})

	t.Run("Transition from Catch up Transactions to Running", func(t *testing.T) {
		require.Equal(t, "CATCHINGTXS", fsm.Current())
		err = fsm.Event(ctx, blockchain_api.FSMEventType_RUN.String())
		require.NoError(t, err)
		require.Equal(t, "RUNNING", fsm.Current())
		require.True(t, fsm.Can(blockchain_api.FSMEventType_CATCHUPBLOCKS.String()))
		require.True(t, fsm.Can(blockchain_api.FSMEventType_CATCHUPTXS.String()))
		require.True(t, fsm.Can(blockchain_api.FSMEventType_STOP.String()))
	})

	t.Run("Transition from Running to Catch up Transactions", func(t *testing.T) {
		require.Equal(t, "RUNNING", fsm.Current())
		err = fsm.Event(ctx, blockchain_api.FSMEventType_CATCHUPTXS.String())
		require.NoError(t, err)
		require.Equal(t, "CATCHINGTXS", fsm.Current())
		require.True(t, fsm.Can(blockchain_api.FSMEventType_RUN.String()))
		require.True(t, fsm.Can(blockchain_api.FSMEventType_STOP.String()))
	})
}

func Test_GetSetFSMStateFromStore(t *testing.T) {
	ctx := context.Background()
	logger := mock_logger.NewTestLogger()

	blockchainStoreURL, err, found := gocore.Config().GetURL("blockchain_store")
	require.NoError(t, err)
	require.True(t, found)

	blockchainStore, err := blockchain_store.NewStore(logger, blockchainStoreURL)
	require.NoError(t, err)

	blockchainClient, err := New(ctx, logger, blockchainStore)
	require.NoError(t, err)

	fsm := blockchainClient.NewFiniteStateMachine()
	require.NotNil(t, fsm)
	require.Equal(t, "STOPPED", fsm.Current())
	require.True(t, fsm.Can(blockchain_api.FSMEventType_RUN.String()))

	t.Run("Set and Get FSM State", func(t *testing.T) {
		err = blockchainClient.store.SetFSMState(ctx, FSMStateCATCHINGBLOCKS.String())
		require.NoError(t, err)
		state, err := blockchainClient.store.GetFSMState(ctx)
		require.NoError(t, err)
		require.Equal(t, "CATCHINGBLOCKS", state)
	})

	//t.Run("Set and Get FSM State with invalid state", func(t *testing.T) {
	//	err = blockchainClient.SetFSMState(ctx, "INVALID")
	//	require.Error(t, err)
	//	state, err := blockchainClient.GetFSMState(ctx)
	//	require.NoError(t, err)
	//	require.Equal(t, "STOPPED", state)
	//})
}
