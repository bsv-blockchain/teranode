package blockchain

import (
	"context"
	"testing"

	teranodeerrors "github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// reconciliationRPC routes the real Client through the real blockchain service;
// SQLite and the existing fault wrapper provide the actual persisted state.
type reconciliationRPC struct {
	blockchain_api.BlockchainAPIClient
	server *Blockchain
	calls  int
	runErr error
}

func (r *reconciliationRPC) Run(ctx context.Context, req *emptypb.Empty, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	r.calls++
	if r.runErr != nil {
		return nil, r.runErr
	}
	return r.server.Run(ctx, req)
}
func (r *reconciliationRPC) Idle(ctx context.Context, req *emptypb.Empty, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	r.calls++
	return r.server.Idle(ctx, req)
}
func (r *reconciliationRPC) CatchUpBlocks(ctx context.Context, req *emptypb.Empty, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	r.calls++
	return r.server.CatchUpBlocks(ctx, req)
}

func TestClient_ConvenienceCallsReconcileUncertainPersistence(t *testing.T) {
	for _, tt := range []struct {
		name  string
		state blockchain_api.FSMStateType
		event blockchain_api.FSMEventType
		call  func(*Client, context.Context) error
	}{
		{"idle after ambiguous run", FSMStateIDLE, blockchain_api.FSMEventType_RUN, (*Client).Idle},
		{"run after ambiguous stop", FSMStateRUNNING, blockchain_api.FSMEventType_STOP, func(c *Client, ctx context.Context) error { return c.Run(ctx, "test") }},
		{"catchup after ambiguous run", FSMStateCATCHINGBLOCKS, blockchain_api.FSMEventType_RUN, (*Client).CatchUpBlocks},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			b, store := newFSMPersistenceTestBlockchain(t, tt.state)
			store.writeErr = teranodeerrors.NewStorageError("lost write acknowledgement")
			store.commitBeforeError = true
			_, err := b.SendFSMEvent(ctx, &blockchain_api.SendFSMEventRequest{Event: tt.event})
			require.Error(t, err)
			require.Equal(t, tt.state.String(), b.finiteStateMachine.Current())
			persisted, err := store.GetFSMState(ctx)
			require.NoError(t, err)
			require.NotEqual(t, tt.state.String(), persisted)

			rpc := &reconciliationRPC{server: b}
			client := &Client{client: rpc, logger: ulogger.TestLogger{}}
			client.fmsState.Store(&tt.state)
			store.commitBeforeError = false
			require.Error(t, tt.call(client, ctx), "a failed reconciliation cannot report success")
			require.Equal(t, 1, rpc.calls, "cached state must not bypass the authority")
			persisted, err = store.GetFSMState(ctx)
			require.NoError(t, err)
			require.NotEqual(t, tt.state.String(), persisted)

			store.writeErr = nil
			require.NoError(t, tt.call(client, ctx))
			persisted, err = store.GetFSMState(ctx)
			require.NoError(t, err)
			require.Equal(t, tt.state.String(), persisted)
			require.Empty(t, b.notifications, "reconciliation is not a new transition")

			writes := 0
			store.beforeWrite = func(context.Context) { writes++ }
			require.NoError(t, tt.call(client, ctx))
			require.Zero(t, writes, "an acknowledged clean no-op needs no additional write")
		})
	}
}

func TestRun_AutomaticPromotionPreservesOperatorIdle(t *testing.T) {
	b, store := newFSMPersistenceTestBlockchain(t, FSMStateIDLE)
	_, err := b.Run(context.Background(), &emptypb.Empty{})
	require.ErrorContains(t, err, "automatic RUN")
	require.Equal(t, "IDLE", b.finiteStateMachine.Current())
	persisted, err := store.GetFSMState(context.Background())
	require.NoError(t, err)
	require.Equal(t, "IDLE", persisted)
	require.Empty(t, b.notifications)

	_, err = b.SendFSMEvent(context.Background(), &blockchain_api.SendFSMEventRequest{Event: blockchain_api.FSMEventType_RUN})
	require.NoError(t, err, "an explicit operator event may leave IDLE when checkpoint-safe")
	require.Equal(t, "RUNNING", b.finiteStateMachine.Current())
}

func TestClientRun_PreservesRetryableTransportStatus(t *testing.T) {
	for _, code := range []codes.Code{codes.Unavailable, codes.DeadlineExceeded} {
		t.Run(code.String(), func(t *testing.T) {
			rpc := &reconciliationRPC{runErr: status.Error(code, "transport failed")}
			client := &Client{client: rpc, logger: ulogger.TestLogger{}}
			require.Equal(t, code, status.Code(client.Run(context.Background(), "test")))
			require.Equal(t, 1, rpc.calls)
		})
	}
}
