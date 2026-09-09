package daemon

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
)

type daemonSetupFSMClient struct {
	blockchain.ClientI
	authority      *blockchain.Blockchain
	cached         blockchain.FSMStateType
	explicitCalls  int
	automaticCalls int
}

func (c *daemonSetupFSMClient) GetFSMCurrentState(context.Context) (*blockchain.FSMStateType, error) {
	return &c.cached, nil
}
func (c *daemonSetupFSMClient) SendFSMEvent(ctx context.Context, event blockchain_api.FSMEventType) error {
	c.explicitCalls++
	_, err := c.authority.SendFSMEvent(ctx, &blockchain_api.SendFSMEventRequest{Event: event})
	if err != nil {
		return errors.UnwrapGRPC(err)
	}
	return nil
}
func (c *daemonSetupFSMClient) Run(ctx context.Context, _ string) error {
	c.automaticCalls++
	_, err := c.authority.Run(ctx, &emptypb.Empty{})
	if err != nil {
		return errors.UnwrapGRPC(err)
	}
	return nil
}

func (c *daemonSetupFSMClient) CatchUpBlocks(ctx context.Context) error {
	c.automaticCalls++
	_, err := c.authority.CatchUpBlocks(ctx, &emptypb.Empty{})
	if err != nil {
		return errors.UnwrapGRPC(err)
	}
	return nil
}

func TestSetTestDaemonRunning_ExplicitSetupAndIdempotence(t *testing.T) {
	for _, state := range []blockchain.FSMStateType{blockchain.FSMStateIDLE, blockchain.FSMStateCATCHINGBLOCKS, blockchain.FSMStateRUNNING} {
		t.Run(state.String(), func(t *testing.T) {
			ctx := context.Background()
			settings := test.CreateBaseTestSettings(t)
			store, err := blockchainstore.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, settings)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close(ctx)) })
			authority, err := blockchain.New(ctx, ulogger.TestLogger{}, settings, store, nil, state.String())
			require.NoError(t, err)
			require.NoError(t, authority.Init(ctx))
			authority.SetSubscriptionManagerReadyForTesting(true)
			// Deliberately wrong cache in both directions; setup must reach authority.
			cached := blockchain.FSMStateRUNNING
			if state == blockchain.FSMStateRUNNING {
				cached = blockchain.FSMStateIDLE
			}
			client := &daemonSetupFSMClient{authority: authority, cached: cached}
			require.NoError(t, setTestDaemonRunning(ctx, client))
			persisted, err := store.GetFSMState(ctx)
			require.NoError(t, err)
			require.Equal(t, "RUNNING", persisted)
			require.Equal(t, 1, client.explicitCalls)
			require.NoError(t, setTestDaemonRunning(ctx, client), "already-running setup remains idempotent")
			persisted, err = store.GetFSMState(ctx)
			require.NoError(t, err)
			require.Equal(t, "RUNNING", persisted)
		})
	}
}

func TestSetTestDaemonCatchingBlocks_ExplicitSetupAndIdempotence(t *testing.T) {
	for _, state := range []blockchain.FSMStateType{blockchain.FSMStateIDLE, blockchain.FSMStateCATCHINGBLOCKS, blockchain.FSMStateRUNNING} {
		t.Run(state.String(), func(t *testing.T) {
			ctx := context.Background()
			settings := test.CreateBaseTestSettings(t)
			store, err := blockchainstore.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, settings)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close(ctx)) })
			authority, err := blockchain.New(ctx, ulogger.TestLogger{}, settings, store, nil, state.String())
			require.NoError(t, err)
			require.NoError(t, authority.Init(ctx))
			authority.SetSubscriptionManagerReadyForTesting(true)
			cached := blockchain.FSMStateCATCHINGBLOCKS
			if state == blockchain.FSMStateCATCHINGBLOCKS {
				cached = blockchain.FSMStateIDLE
			}
			client := &daemonSetupFSMClient{authority: authority, cached: cached}
			otherClient := &daemonSetupFSMClient{authority: authority, cached: cached}
			results := make(chan error, 2)
			// Concurrent explicit setup may race to enter CATCHINGBLOCKS; the
			// later request must confirm the authority's already-current state.
			go func() { results <- setTestDaemonCatchingBlocks(ctx, client) }()
			go func() { results <- setTestDaemonCatchingBlocks(ctx, otherClient) }()
			require.NoError(t, <-results)
			require.NoError(t, <-results)
			persisted, err := store.GetFSMState(ctx)
			require.NoError(t, err)
			require.Equal(t, "CATCHINGBLOCKS", persisted)
			require.Equal(t, 1, client.explicitCalls)
			require.NoError(t, setTestDaemonCatchingBlocks(ctx, client), "already-catching setup remains idempotent")
			persisted, err = store.GetFSMState(ctx)
			require.NoError(t, err)
			require.Equal(t, "CATCHINGBLOCKS", persisted)
			actual, err := authority.GetFSMCurrentState(ctx, &emptypb.Empty{})
			require.NoError(t, err)
			require.Equal(t, blockchain.FSMStateCATCHINGBLOCKS, actual.State)
		})
	}
}
