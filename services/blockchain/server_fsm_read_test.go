package blockchain

import (
	"context"
	"net"
	"testing"
	"time"

	teranodeerrors "github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

func newFSMReadTestConnection(t *testing.T, b *Blockchain, opts ...grpc.ServerOption) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer(opts...)
	blockchain_api.RegisterBlockchainAPIServer(server, b)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///fsm-read", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return conn
}

func TestReadFSMState_ReadyAuthority(t *testing.T) {
	for _, state := range []FSMStateType{FSMStateRUNNING, FSMStateIDLE, FSMStateCATCHINGBLOCKS} {
		t.Run(state.String(), func(t *testing.T) {
			b, _ := newFSMPersistenceTestBlockchain(t, state)
			b.subscriptionManagerReady.Store(true)
			conn := newFSMReadTestConnection(t, b)
			client := &Client{client: blockchain_api.NewBlockchainAPIClient(conn)}
			cached := FSMStateIDLE
			client.fmsState.Store(&cached)
			actual, err := client.ReadFSMState(context.Background())
			require.NoError(t, err)
			require.Equal(t, state, actual)
			require.Equal(t, FSMStateIDLE, *client.fmsState.Load(), "authoritative reads must not overwrite subscription safety state")
			require.Empty(t, b.notifications)
		})
	}
}

func TestReadFSMState_UnavailableAuthority(t *testing.T) {
	for _, scenario := range []string{"unready", "uninitialized", "unknown"} {
		t.Run(scenario, func(t *testing.T) {
			b, _ := newFSMPersistenceTestBlockchain(t, FSMStateRUNNING)
			b.subscriptionManagerReady.Store(true)
			switch scenario {
			case "unready":
				b.subscriptionManagerReady.Store(false)
			case "uninitialized":
				b.finiteStateMachine = nil
			case "unknown":
				b.finiteStateMachine.SetState("UNKNOWN")
			}
			client := &Client{client: blockchain_api.NewBlockchainAPIClient(newFSMReadTestConnection(t, b))}
			_, err := client.ReadFSMState(context.Background())
			require.Equal(t, codes.Unavailable, status.Code(err))
			b.finiteStateMachine = b.NewFiniteStateMachine()
			b.finiteStateMachine.SetState(FSMStateRUNNING.String())
			b.subscriptionManagerReady.Store(true)
			state, err := client.ReadFSMState(context.Background())
			require.NoError(t, err)
			require.Equal(t, FSMStateRUNNING, state)
		})
	}
}

func TestReadFSMState_UncertainPersistenceRequiresReconciliation(t *testing.T) {
	b, store := newFSMPersistenceTestBlockchain(t, FSMStateRUNNING)
	b.subscriptionManagerReady.Store(true)
	store.writeErr = teranodeerrors.NewStorageError("lost write acknowledgement")
	store.commitBeforeError = true
	_, err := b.Idle(context.Background(), &emptypb.Empty{})
	require.Error(t, err)
	client := &Client{client: blockchain_api.NewBlockchainAPIClient(newFSMReadTestConnection(t, b))}
	_, err = client.ReadFSMState(context.Background())
	require.Equal(t, codes.Unavailable, status.Code(err))
	persisted, err := store.GetFSMState(context.Background())
	require.NoError(t, err)
	require.Equal(t, "IDLE", persisted)
	require.Equal(t, "RUNNING", b.finiteStateMachine.Current())
	require.Empty(t, b.notifications)
	store.writeErr = nil
	_, err = b.Run(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	state, err := client.ReadFSMState(context.Background())
	require.NoError(t, err)
	require.Equal(t, FSMStateRUNNING, state)
	require.Empty(t, b.notifications, "read and same-state reconciliation must not notify")
}

func TestReadFSMState_DoesNotWaitForBlockedPersistence(t *testing.T) {
	b, store := newFSMPersistenceTestBlockchain(t, FSMStateRUNNING)
	b.subscriptionManagerReady.Store(true)
	entered := make(chan struct{})
	release := make(chan struct{})
	store.beforeWrite = func(context.Context) {
		close(entered)
		<-release
	}
	finished := make(chan error, 1)
	go func() {
		_, err := b.Idle(context.Background(), &emptypb.Empty{})
		finished <- err
	}()
	t.Cleanup(func() { close(release); require.NoError(t, <-finished) })
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("persistence did not start")
	}
	client := &Client{client: blockchain_api.NewBlockchainAPIClient(newFSMReadTestConnection(t, b))}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := client.ReadFSMState(ctx)
	require.Equal(t, codes.Unavailable, status.Code(err), "busy authority must reject immediately, not await persistence or the deadline")
}

func TestReadFSMState_DoesNotWaitForBlockedNotification(t *testing.T) {
	b, _ := newFSMPersistenceTestBlockchain(t, FSMStateRUNNING)
	b.subscriptionManagerReady.Store(true)
	b.notifications = make(chan *blockchain_api.Notification)
	finished := make(chan error, 1)
	go func() {
		_, err := b.Idle(context.Background(), &emptypb.Empty{})
		finished <- err
	}()
	t.Cleanup(func() { <-b.notifications; require.NoError(t, <-finished) })
	require.Eventually(t, func() bool {
		return b.finiteStateMachine.Current() == "IDLE"
	}, time.Second, time.Millisecond)
	client := &Client{client: blockchain_api.NewBlockchainAPIClient(newFSMReadTestConnection(t, b))}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := client.ReadFSMState(ctx)
	require.Equal(t, codes.Unavailable, status.Code(err), "unpublished transition must not block or certify a state")
}

func TestReadFSMState_CancellationAndDeadline(t *testing.T) {
	b, _ := newFSMPersistenceTestBlockchain(t, FSMStateRUNNING)
	b.subscriptionManagerReady.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := b.ReadFSMState(ctx, &emptypb.Empty{})
	require.Equal(t, codes.Canceled, status.Code(err))
	client := &Client{client: blockchain_api.NewBlockchainAPIClient(newFSMReadTestConnection(t, b))}
	_, err = client.ReadFSMState(ctx)
	require.Equal(t, codes.Canceled, status.Code(err))

	deadlineSeen := make(chan time.Duration, 1)
	conn := newFSMReadTestConnection(t, b, grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		deadline, ok := ctx.Deadline()
		if !ok {
			return nil, status.Error(codes.Internal, "missing RPC deadline")
		}
		deadlineSeen <- time.Until(deadline)
		return handler(ctx, req)
	}))
	client = &Client{client: blockchain_api.NewBlockchainAPIClient(conn)}
	_, err = client.ReadFSMState(context.Background())
	require.NoError(t, err)
	require.InDelta(t, fsmReadTimeout, <-deadlineSeen, float64(time.Second))
	shortCtx, shortCancel := context.WithTimeout(context.Background(), time.Second)
	defer shortCancel()
	_, err = client.ReadFSMState(shortCtx)
	require.NoError(t, err)
	require.LessOrEqual(t, <-deadlineSeen, time.Second)
}

func TestReadFSMState_NoCompatibilityFallback(t *testing.T) {
	b, _ := newFSMPersistenceTestBlockchain(t, FSMStateRUNNING)
	b.subscriptionManagerReady.Store(true)
	for _, code := range []codes.Code{codes.Unimplemented, codes.Unavailable, codes.DeadlineExceeded} {
		t.Run(code.String(), func(t *testing.T) {
			conn := newFSMReadTestConnection(t, b, grpc.UnaryInterceptor(func(context.Context, any, *grpc.UnaryServerInfo, grpc.UnaryHandler) (any, error) {
				return nil, status.Error(code, "injected transport failure")
			}))
			client := &Client{client: blockchain_api.NewBlockchainAPIClient(conn)}
			state := FSMStateRUNNING
			client.fmsState.Store(&state)
			_, err := client.ReadFSMState(context.Background())
			require.Equal(t, code, status.Code(err))
		})
	}
}

func TestLocalClientReadFSMState_DoesNotFabricateAuthority(t *testing.T) {
	client := &LocalClient{}
	_, err := client.ReadFSMState(context.Background())
	require.Equal(t, codes.Unavailable, status.Code(err))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = client.ReadFSMState(ctx)
	require.Equal(t, codes.Canceled, status.Code(err))
}
