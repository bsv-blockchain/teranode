package blockchain

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/services/blockchain/blockchain_api"
	blockchain_store "github.com/bitcoin-sv/ubsv/stores/blockchain"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util/test/mock_logger"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/protobuf/types/known/emptypb"
)

func Test_NewFiniteStateMachine(t *testing.T) {
	ctx := context.Background()
	logger := mock_logger.NewTestLogger()
	blockchainClient, err := New(ctx, logger, nil, nil)
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
	connStr, teardown, err := SetupPostgresContainer()
	require.NoError(t, err)

	defer func() {
		err := teardown()
		require.NoError(t, err)
	}()

	storeURL, err := url.Parse(connStr)
	require.NoError(t, err)

	blockchainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, storeURL)
	require.NoError(t, err)

	ctx := context.Background()
	logger := mock_logger.NewTestLogger()

	blockchainClient, err := New(ctx, logger, blockchainStore, nil)
	require.NoError(t, err)

	err = blockchainClient.Init(ctx)
	require.NoError(t, err)

	resp, err := blockchainClient.GetFSMCurrentState(ctx, &emptypb.Empty{})
	require.NoError(t, err)
	require.Equal(t, "STOPPED", resp.State.String())

	t.Run("Get Initial FSM State", func(t *testing.T) {
		state, err := blockchainClient.store.GetFSMState(ctx)
		require.NoError(t, err)
		require.Equal(t, "STOPPED", state)
	})

	t.Run("Alter current state to Running", func(t *testing.T) {
		_, err = blockchainClient.Run(ctx, &emptypb.Empty{})
		require.NoError(t, err)

		resp, err := blockchainClient.GetFSMCurrentState(ctx, &emptypb.Empty{})
		require.NoError(t, err)
		require.Equal(t, "RUNNING", resp.State.String())

		state, err := blockchainClient.store.GetFSMState(ctx)
		require.NoError(t, err)
		require.Equal(t, "RUNNING", state)
	})

	t.Run("Alter current state to Catchup Blocks", func(t *testing.T) {
		_, err = blockchainClient.CatchUpBlocks(ctx, &emptypb.Empty{})
		require.NoError(t, err)

		resp, err := blockchainClient.GetFSMCurrentState(ctx, &emptypb.Empty{})
		require.NoError(t, err)
		require.Equal(t, "CATCHINGBLOCKS", resp.State.String())

		state, err := blockchainClient.store.GetFSMState(ctx)
		require.NoError(t, err)
		require.Equal(t, "CATCHINGBLOCKS", state)
	})

	t.Run("Simulate re-initializing blockchain service", func(t *testing.T) {
		// Step 1 Simulate restarting the blockchain service
		blockchainClient.finiteStateMachine = nil

		// Step 2 Re-initialize the blockchain service
		// This should restore the state to the last known state from DB
		err = blockchainClient.Init(ctx)
		require.NoError(t, err)

		resp, err = blockchainClient.GetFSMCurrentState(ctx, &emptypb.Empty{})
		require.NoError(t, err)
		require.Equal(t, "CATCHINGBLOCKS", resp.State.String())

		state, err := blockchainClient.store.GetFSMState(ctx)
		require.NoError(t, err)
		require.Equal(t, "CATCHINGBLOCKS", state)
	})

	t.Run("Alter current state to Running again", func(t *testing.T) {
		_, err = blockchainClient.Run(ctx, &emptypb.Empty{})
		require.NoError(t, err)

		resp, err := blockchainClient.GetFSMCurrentState(ctx, &emptypb.Empty{})
		require.NoError(t, err)
		require.Equal(t, "RUNNING", resp.State.String())

		state, err := blockchainClient.store.GetFSMState(ctx)
		require.NoError(t, err)
		require.Equal(t, "RUNNING", state)
	})
}

func SetupPostgresContainer() (string, func() error, error) {
	ctx := context.Background()

	dbName := "testdb"
	dbUser := "postgres"
	dbPassword := "password"

	postgresC, err := postgres.Run(ctx,
		"docker.io/postgres:16-alpine",
		postgres.WithDatabase(dbName),
		postgres.WithUsername(dbUser),
		postgres.WithPassword(dbPassword),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Second)),
	)
	if err != nil {
		return "", nil, err
	}

	host, err := postgresC.Host(ctx)
	if err != nil {
		return "", nil, err
	}

	port, err := postgresC.MappedPort(ctx, "5432")
	if err != nil {
		return "", nil, err
	}

	connStr := fmt.Sprintf("postgres://postgres:password@%s:%s/testdb?sslmode=disable", host, port.Port())

	teardown := func() error {
		return postgresC.Terminate(ctx)
	}

	return connStr, teardown, nil
}
