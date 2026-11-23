package blockvalidation

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Only failed writes are injected; successful writes and the FSM use SQLite.
type promotionFaultStore struct {
	blockchainstore.Store
	fail func() error
}

func (s *promotionFaultStore) SetFSMState(ctx context.Context, state string) error {
	if s.fail != nil {
		if err := s.fail(); err != nil {
			return err
		}
	}
	return s.Store.SetFSMState(ctx, state)
}

// LocalClient has no runtime FSM. Forward these methods to the real authority.
type promotionAuthorityClient struct {
	blockchain.ClientI
	authority   *blockchain.Blockchain
	runCalls    int
	beforeRun   func() error
	cachedReads int
	cachedState *blockchain.FSMStateType
}

func (c *promotionAuthorityClient) Run(ctx context.Context, _ string) error {
	c.runCalls++
	if c.beforeRun != nil {
		if err := c.beforeRun(); err != nil {
			return err
		}
	}
	_, err := c.authority.Run(ctx, &emptypb.Empty{})
	if err != nil {
		return errors.UnwrapGRPC(err)
	}
	return nil
}

func (c *promotionAuthorityClient) CatchUpBlocks(ctx context.Context) error {
	_, err := c.authority.CatchUpBlocks(ctx, &emptypb.Empty{})
	if err != nil {
		return errors.UnwrapGRPC(err)
	}
	return nil
}

func (c *promotionAuthorityClient) GetFSMCurrentState(ctx context.Context) (*blockchain.FSMStateType, error) {
	c.cachedReads++
	if c.cachedState != nil {
		return c.cachedState, nil
	}
	state, err := c.authority.GetFSMCurrentState(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, errors.UnwrapGRPC(err)
	}
	return &state.State, nil
}

func newPromotionAuthority(t *testing.T) (*Server, *promotionAuthorityClient, *promotionFaultStore, *CatchupContext) {
	t.Helper()
	settings := test.CreateBaseTestSettings(t)
	store, err := blockchainstore.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, settings)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
	faultStore := &promotionFaultStore{Store: store}
	authority, err := blockchain.New(context.Background(), ulogger.TestLogger{}, settings, faultStore, nil, blockchain.FSMStateCATCHINGBLOCKS.String())
	require.NoError(t, err)
	require.NoError(t, authority.Init(context.Background()))
	authority.SetSubscriptionManagerReadyForTesting(true)
	client, err := blockchain.NewLocalClient(ulogger.TestLogger{}, settings, store, nil, nil)
	require.NoError(t, err)
	adapter := &promotionAuthorityClient{ClientI: client, authority: authority}
	server := &Server{settings: settings, logger: ulogger.TestLogger{}, blockchainClient: adapter}
	return server, adapter, faultStore, &CatchupContext{blockUpTo: testhelpers.CreateTestBlockChain(t, 2)[1]}
}

func requirePromotionState(t *testing.T, client *promotionAuthorityClient, store *promotionFaultStore, want blockchain.FSMStateType) {
	t.Helper()
	state, err := client.GetFSMCurrentState(context.Background())
	require.NoError(t, err)
	require.Equal(t, want, *state)
	persisted, err := store.GetFSMState(context.Background())
	require.NoError(t, err)
	require.Equal(t, want.String(), persisted)
}

func TestRestoreFSMState_RetriesPersistenceFailure(t *testing.T) {
	server, client, store, catchupCtx := newPromotionAuthority(t)
	writes := 0
	store.fail = func() error {
		writes++
		if writes == 1 {
			return errors.NewStorageError("temporary database failure")
		}
		return nil
	}
	server.restoreFSMState(context.Background(), catchupCtx)
	requirePromotionState(t, client, store, blockchain.FSMStateRUNNING)
	require.Equal(t, 2, client.runCalls)
}

func TestRestoreFSMState_ExhaustedPersistenceRetriesRemainCatching(t *testing.T) {
	server, client, store, catchupCtx := newPromotionAuthority(t)
	store.fail = func() error { return errors.NewStorageError("database unavailable") }
	server.restoreFSMState(context.Background(), catchupCtx)
	requirePromotionState(t, client, store, blockchain.FSMStateCATCHINGBLOCKS)
	require.Equal(t, 3, client.runCalls)
}

func TestRestoreFSMState_CancellationStopsRetry(t *testing.T) {
	server, client, store, catchupCtx := newPromotionAuthority(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store.fail = func() error {
		cancel()
		return errors.NewStorageError("database unavailable")
	}
	server.restoreFSMState(ctx, catchupCtx)
	requirePromotionState(t, client, store, blockchain.FSMStateCATCHINGBLOCKS)
	require.Equal(t, 1, client.runCalls)
}

func TestRestoreFSMState_CancellationInterruptsBackoff(t *testing.T) {
	server, client, store, catchupCtx := newPromotionAuthority(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store.fail = func() error {
		timer := time.AfterFunc(50*time.Millisecond, cancel)
		t.Cleanup(func() { timer.Stop() })
		return errors.NewStorageError("database unavailable")
	}
	start := time.Now()
	server.restoreFSMState(ctx, catchupCtx)
	require.Less(t, time.Since(start), 800*time.Millisecond, "cancellation must interrupt the one-second backoff")
	requirePromotionState(t, client, store, blockchain.FSMStateCATCHINGBLOCKS)
	require.Equal(t, 1, client.runCalls)
}

func TestRestoreFSMState_TransientTransportFailure(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"unavailable", status.Error(codes.Unavailable, "temporary transport failure")},
		{"deadline", status.Error(codes.DeadlineExceeded, "temporary transport failure")},
		{"checkpoint read deadline", errors.UnwrapGRPC(errors.WrapGRPC(errors.NewStateError("tip read failed", context.DeadlineExceeded)))},
		{"checkpoint read storage failure", errors.UnwrapGRPC(errors.WrapGRPC(errors.NewStateError("tip read failed", errors.NewStorageError("temporary read failure"))))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, client, store, catchupCtx := newPromotionAuthority(t)
			client.beforeRun = func() error {
				if client.runCalls == 1 {
					return tc.err
				}
				return nil
			}
			server.restoreFSMState(context.Background(), catchupCtx)
			requirePromotionState(t, client, store, blockchain.FSMStateRUNNING)
			require.Equal(t, 2, client.runCalls)
		})
	}
}

func TestRestoreFSMState_DoesNotRetryStateRejection(t *testing.T) {
	server, client, store, catchupCtx := newPromotionAuthority(t)
	client.beforeRun = func() error { return errors.NewStateError("tip below highest checkpoint") }
	server.restoreFSMState(context.Background(), catchupCtx)
	requirePromotionState(t, client, store, blockchain.FSMStateCATCHINGBLOCKS)
	require.Equal(t, 1, client.runCalls)
}

func TestRestoreFSMState_IgnoresCachedIdle(t *testing.T) {
	server, client, store, catchupCtx := newPromotionAuthority(t)
	idle := blockchain.FSMStateIDLE
	client.cachedState = &idle
	server.restoreFSMState(context.Background(), catchupCtx)
	require.Zero(t, client.cachedReads, "cached/synthetic IDLE must not suppress authoritative promotion")
	client.cachedState = nil
	requirePromotionState(t, client, store, blockchain.FSMStateRUNNING)
}

func TestRestoreFSMState_RetryPreservesOperatorIdle(t *testing.T) {
	server, client, store, catchupCtx := newPromotionAuthority(t)
	client.beforeRun = func() error {
		if client.runCalls == 1 {
			// Another promotion completes, then an operator parks the node before
			// our retry. The retry must consult the authority after this STOP.
			_, err := client.authority.SendFSMEvent(context.Background(), &blockchain_api.SendFSMEventRequest{Event: blockchain_api.FSMEventType_RUN})
			require.NoError(t, err)
			_, err = client.authority.Idle(context.Background(), &emptypb.Empty{})
			require.NoError(t, err)
			return status.Error(codes.Unavailable, "lost transport response")
		}
		return nil
	}
	server.restoreFSMState(context.Background(), catchupCtx)
	requirePromotionState(t, client, store, blockchain.FSMStateIDLE)
	require.Equal(t, 2, client.runCalls, "IDLE refusal must stop further retries")
}
