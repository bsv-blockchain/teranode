package blockchain

import (
	"context"
	"net"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/stores/blockchain/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

func newPauseTestServer(t *testing.T) *Blockchain {
	t.Helper()
	ctx := context.Background()
	s := test.CreateBaseTestSettings(t)
	s.ChainCfgParams = &chaincfg.RegressionNetParams
	s.BlockChain.FSMStateChangeDelay = 0
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	store, err := sql.New(ulogger.TestLogger{}, storeURL, s)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(ctx)) })
	b, err := New(ctx, ulogger.TestLogger{}, s, store, nil)
	require.NoError(t, err)
	b.finiteStateMachine = b.NewFiniteStateMachine()
	b.finiteStateMachine.SetState(FSMStateCATCHINGBLOCKS.String())
	b.SetSubscriptionManagerReadyForTesting(true)
	require.NoError(t, store.SetFSMState(ctx, FSMStateCATCHINGBLOCKS.String()))
	return b
}

func TestFSMPause_OperatorStopAndResume(t *testing.T) {
	b := newPauseTestServer(t)
	ctx := context.Background()
	_, err := b.SendFSMEvent(ctx, &blockchain_api.SendFSMEventRequest{Event: blockchain_api.FSMEventType_STOP})
	require.NoError(t, err)
	require.Equal(t, FSMStateIDLE.String(), b.finiteStateMachine.Current())
	state, err := b.store.GetFSMState(ctx)
	require.NoError(t, err)
	require.Equal(t, FSMStateIDLE.String(), state)

	_, err = b.Run(ctx, &emptypb.Empty{})
	require.Error(t, err)
	require.ErrorIs(t, errors.UnwrapGRPC(err), errors.ErrStateError)
	_, err = b.CatchUpBlocks(ctx, &emptypb.Empty{})
	require.Error(t, err)
	require.ErrorIs(t, errors.UnwrapGRPC(err), errors.ErrStateError)
	require.Equal(t, FSMStateIDLE.String(), b.finiteStateMachine.Current())

	_, err = b.SendFSMEvent(ctx, &blockchain_api.SendFSMEventRequest{Event: blockchain_api.FSMEventType_CATCHUPBLOCKS})
	require.NoError(t, err)
	_, err = b.CatchUpBlocks(ctx, &emptypb.Empty{})
	require.NoError(t, err)
}

func TestFSMPause_AutomaticCallsPreserveIdle(t *testing.T) {
	for _, call := range []string{"Run", "CatchUpBlocks"} {
		t.Run(call, func(t *testing.T) {
			b := newPauseTestServer(t)
			ctx := context.Background()
			b.finiteStateMachine.SetState(FSMStateIDLE.String())
			require.NoError(t, b.store.SetFSMState(ctx, FSMStateIDLE.String()))
			var err error
			if call == "Run" {
				_, err = b.Run(ctx, &emptypb.Empty{})
			} else {
				_, err = b.CatchUpBlocks(ctx, &emptypb.Empty{})
			}
			require.Error(t, err)
			require.Equal(t, FSMStateIDLE.String(), b.finiteStateMachine.Current())
			state, err := b.store.GetFSMState(ctx)
			require.NoError(t, err)
			require.Equal(t, FSMStateIDLE.String(), state)
		})
	}
}

func TestFSMPause_ClientCacheCannotAuthorizeWork(t *testing.T) {
	b := newPauseTestServer(t)
	ctx := context.Background()
	b.finiteStateMachine.SetState(FSMStateIDLE.String())
	require.NoError(t, b.store.SetFSMState(ctx, FSMStateIDLE.String()))
	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	blockchain_api.RegisterBlockchainAPIServer(srv, b)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("passthrough:///pause-test", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	c := &Client{client: blockchain_api.NewBlockchainAPIClient(conn), logger: ulogger.TestLogger{}}
	catching := FSMStateCATCHINGBLOCKS
	c.fmsState.Store(&catching)
	require.Error(t, c.CatchUpBlocks(ctx), "stale CATCHINGBLOCKS must still consult authoritative IDLE")
	running := FSMStateRUNNING
	c.fmsState.Store(&running)
	require.Error(t, c.Run(ctx, "pause test"), "stale RUNNING must not fabricate successful RUN")
	_, err = b.SendFSMEvent(ctx, &blockchain_api.SendFSMEventRequest{Event: blockchain_api.FSMEventType_CATCHUPBLOCKS})
	require.NoError(t, err)
	idle := FSMStateIDLE
	c.fmsState.Store(&idle)
	require.NoError(t, c.CatchUpBlocks(ctx), "synthetic cached IDLE must not override explicit resume")
	require.NoError(t, c.Idle(ctx), "the Idle alias must also reach the authority")
	require.Equal(t, FSMStateIDLE.String(), b.finiteStateMachine.Current())
	_, err = b.SendFSMEvent(ctx, &blockchain_api.SendFSMEventRequest{Event: blockchain_api.FSMEventType_RUN})
	require.NoError(t, err)
	require.NoError(t, c.AdmitCatchupWork(ctx))
	require.Equal(t, FSMStateRUNNING.String(), b.finiteStateMachine.Current(), "admission must not change the operating state")
}

func TestFSMPause_FailedStopRemainsResumable(t *testing.T) {
	b, store := newFSMPersistenceTestBlockchain(t, blockchain_api.FSMStateType_CATCHINGBLOCKS)
	store.writeErr = errors.NewStorageError("pause write rejected")
	ctx := context.Background()
	_, err := b.SendFSMEvent(ctx, &blockchain_api.SendFSMEventRequest{Event: blockchain_api.FSMEventType_STOP})
	require.Error(t, err)
	require.Equal(t, FSMStateCATCHINGBLOCKS.String(), b.finiteStateMachine.Current())
	require.Empty(t, b.notifications)
	store.writeErr = nil
	_, err = b.SendFSMEvent(ctx, &blockchain_api.SendFSMEventRequest{Event: blockchain_api.FSMEventType_STOP})
	require.NoError(t, err)
	state, err := store.GetFSMState(ctx)
	require.NoError(t, err)
	require.Equal(t, FSMStateIDLE.String(), state)
}

func TestFSMPause_AdmissionRejectsDuringStopPersistence(t *testing.T) {
	b, store := newFSMPersistenceTestBlockchain(t, FSMStateCATCHINGBLOCKS)
	b.subscriptionManagerReady.Store(true)
	client := &Client{client: blockchain_api.NewBlockchainAPIClient(newFSMReadTestConnection(t, b))}
	// An admitted unit may finish later; its snapshot cannot authorize a successor.
	require.NoError(t, client.AdmitCatchupWork(t.Context()))
	writing := make(chan struct{})
	release := make(chan struct{})
	store.beforeWrite = func(context.Context) {
		close(writing)
		<-release
	}
	stopResult := make(chan error, 1)
	go func() {
		_, err := b.SendFSMEvent(context.Background(), &blockchain_api.SendFSMEventRequest{Event: FSMEventIDLE})
		stopResult <- err
	}()
	finishStop := sync.OnceFunc(func() { close(release); require.NoError(t, <-stopResult) })
	t.Cleanup(finishStop)
	select {
	case <-writing:
	case <-time.After(time.Second):
		t.Fatal("STOP persistence did not start")
	}
	require.Equal(t, FSMStateCATCHINGBLOCKS.String(), b.finiteStateMachine.Current())
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	err := client.AdmitCatchupWork(ctx)
	require.Equal(t, codes.Unavailable, status.Code(err), "busy authority must reject rather than wait behind STOP persistence")
	require.NoError(t, ctx.Err(), "admission must return before its deadline")
	require.NotErrorIs(t, err, ErrCatchupPaused, "unacknowledged STOP is not confirmed operator pause")
	finishStop()
	require.ErrorIs(t, client.AdmitCatchupWork(t.Context()), ErrCatchupPaused, "the next unit cannot use the pre-STOP admission")
	state, err := store.GetFSMState(t.Context())
	require.NoError(t, err)
	require.Equal(t, FSMStateIDLE.String(), state)
}

func TestFSMPause_AdmissionDoesNotWaitForNotification(t *testing.T) {
	b, store := newFSMPersistenceTestBlockchain(t, FSMStateCATCHINGBLOCKS)
	b.subscriptionManagerReady.Store(true)
	b.notifications = make(chan *blockchain_api.Notification)
	client := &Client{client: blockchain_api.NewBlockchainAPIClient(newFSMReadTestConnection(t, b))}
	stopResult := make(chan error, 1)
	go func() {
		_, err := b.SendFSMEvent(context.Background(), &blockchain_api.SendFSMEventRequest{Event: FSMEventIDLE})
		stopResult <- err
	}()
	finishStop := sync.OnceFunc(func() { <-b.notifications; require.NoError(t, <-stopResult) })
	t.Cleanup(finishStop)
	require.Eventually(t, func() bool { return b.finiteStateMachine.Current() == FSMStateIDLE.String() }, time.Second, time.Millisecond)
	persisted, err := store.GetFSMState(t.Context())
	require.NoError(t, err)
	require.Equal(t, FSMStateIDLE.String(), persisted)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	err = client.AdmitCatchupWork(ctx)
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.NoError(t, ctx.Err(), "a blocked notification consumer must not hold the admission RPC")
	finishStop()
	require.ErrorIs(t, client.AdmitCatchupWork(t.Context()), ErrCatchupPaused)
}

func TestFSMPause_AmbiguousStopDoesNotAuthorizeWork(t *testing.T) {
	b, store := newFSMPersistenceTestBlockchain(t, FSMStateCATCHINGBLOCKS)
	b.subscriptionManagerReady.Store(true)
	client := &Client{client: blockchain_api.NewBlockchainAPIClient(newFSMReadTestConnection(t, b))}
	store.writeErr = errors.NewStorageError("lost STOP write acknowledgement")
	store.commitBeforeError = true
	_, err := b.SendFSMEvent(t.Context(), &blockchain_api.SendFSMEventRequest{Event: FSMEventIDLE})
	require.Error(t, err)
	require.Equal(t, FSMStateCATCHINGBLOCKS.String(), b.finiteStateMachine.Current())
	persisted, err := store.GetFSMState(t.Context())
	require.NoError(t, err)
	require.Equal(t, FSMStateIDLE.String(), persisted)
	err = client.AdmitCatchupWork(t.Context())
	require.Equal(t, codes.Unavailable, status.Code(err), "in-memory CATCHINGBLOCKS must not grant work after an ambiguous STOP write")
	require.NotErrorIs(t, err, ErrCatchupPaused)
	require.Empty(t, b.notifications)
	store.writeErr = nil
	_, err = b.Idle(t.Context(), &emptypb.Empty{})
	require.NoError(t, err)
	require.ErrorIs(t, client.AdmitCatchupWork(t.Context()), ErrCatchupPaused)
}
