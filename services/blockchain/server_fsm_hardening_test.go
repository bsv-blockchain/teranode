package blockchain

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/blockchain/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

func newFSMHardeningBlockchain(t *testing.T) *Blockchain {
	t.Helper()
	initPrometheusMetrics()
	tSettings := test.CreateBaseTestSettings(t)
	params := chaincfg.RegressionNetParams
	tSettings.ChainCfgParams = &params
	tSettings.BlockChain.FSMStateChangeDelay = 0
	logger := ulogger.TestLogger{}
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	store, err := sql.New(logger, storeURL, tSettings)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
	require.NoError(t, store.SetFSMState(context.Background(), "IDLE"))
	b := &Blockchain{
		logger: logger, store: store, settings: tSettings,
		notifications: make(chan *blockchain_api.Notification, 10),
		stats:         gocore.NewStat("blockchain-fsm-hardening-test"),
	}
	b.finiteStateMachine = b.NewFiniteStateMachine()
	return b
}

// Cancellation after admission must neither poison looplab's pending transition
// nor prevent the accepted state from being persisted.
func TestSendFSMEvent_AcceptedCancellationDoesNotWedge(t *testing.T) {
	b := newFSMHardeningBlockchain(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	resp, err := b.SendFSMEvent(ctx, &blockchain_api.SendFSMEventRequest{Event: blockchain_api.FSMEventType_RUN})
	require.NoError(t, err)
	require.Equal(t, blockchain_api.FSMStateType_RUNNING, resp.State)
	state, err := b.store.GetFSMState(context.Background())
	require.NoError(t, err)
	require.Equal(t, "RUNNING", state)
	resp, err = b.SendFSMEvent(context.Background(), &blockchain_api.SendFSMEventRequest{Event: blockchain_api.FSMEventType_STOP})
	require.NoError(t, err, "the next transition must remain usable after request cancellation")
	require.Equal(t, blockchain_api.FSMStateType_IDLE, resp.State)
}

type fsmHardeningWriteStore struct {
	blockchainstore.Store
	contexts chan context.Context
}

func (s *fsmHardeningWriteStore) SetFSMState(ctx context.Context, state string) error {
	s.contexts <- ctx
	return s.Store.SetFSMState(ctx, state)
}

func TestSendFSMEvent_ReleasesStoreContextBeforeResponseDelay(t *testing.T) {
	b := newFSMHardeningBlockchain(t)
	b.settings.BlockChain.FSMStateChangeDelay = time.Second
	b.stateChangeTimestamp = time.Now()
	contexts := make(chan context.Context, 1)
	b.store = &fsmHardeningWriteStore{Store: b.store, contexts: contexts}
	done := make(chan error, 1)
	go func() {
		_, err := b.SendFSMEvent(context.Background(), &blockchain_api.SendFSMEventRequest{Event: blockchain_api.FSMEventType_RUN})
		done <- err
	}()
	t.Cleanup(func() {
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(3 * time.Second):
			t.Error("FSM request did not finish")
		}
	})
	select {
	case storeCtx := <-contexts:
		select {
		case <-storeCtx.Done():
			require.ErrorIs(t, storeCtx.Err(), context.Canceled)
		case <-time.After(500 * time.Millisecond):
			t.Fatal("store context remained alive during the response delay")
		}
	case <-time.After(time.Second):
		t.Fatal("FSM request did not reach persistence")
	}
}

func TestSendFSMEvent_InTransitionReturnsStateError(t *testing.T) {
	b := newFSMHardeningBlockchain(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// Reproduce an already-wedged looplab FSM without routing through the fix.
	require.ErrorIs(t, b.finiteStateMachine.Event(ctx, "RUN"), context.Canceled)
	_, err := b.SendFSMEvent(context.Background(), &blockchain_api.SendFSMEventRequest{Event: blockchain_api.FSMEventType_RUN})
	require.Error(t, err)
	require.Equal(t, errors.ERR_STATE_ERROR, errors.UnwrapGRPC(err).Code())
}

// Only the checkpoint read is fault-injected; persistence uses real SQLite.
type fsmHardeningReadStore struct {
	blockchainstore.Store
	read func(context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error)
}

func (s *fsmHardeningReadStore) GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	return s.read(ctx)
}

func TestSendFSMEvent_CheckpointTimeoutReleasesTransitionLock(t *testing.T) {
	b := newFSMHardeningBlockchain(t)
	b.settings.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{{Height: 1}}
	b.settings.BlockChain.StoreDBTimeoutMillis = 20
	// The outer deadline bounds the regression run even before the fix exists.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b.store = &fsmHardeningReadStore{Store: b.store, read: func(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
		<-ctx.Done()
		return nil, nil, ctx.Err()
	}}
	_, err := b.SendFSMEvent(ctx, &blockchain_api.SendFSMEventRequest{Event: blockchain_api.FSMEventType_RUN})
	require.Error(t, err)
	require.NoError(t, ctx.Err(), "the server's DB timeout must expire before the caller deadline")
	require.Equal(t, "IDLE", b.finiteStateMachine.Current())
	state, err := b.store.GetFSMState(context.Background())
	require.NoError(t, err)
	require.Equal(t, "IDLE", state)
	resp, err := b.SendFSMEvent(context.Background(), &blockchain_api.SendFSMEventRequest{Event: blockchain_api.FSMEventType_CATCHUPBLOCKS})
	require.NoError(t, err, "timeout must release fsmMu for the next event")
	require.Equal(t, blockchain_api.FSMStateType_CATCHINGBLOCKS, resp.State)
}

func TestGuardRunBelowHighestCheckpoint_RespectsEarlierDeadline(t *testing.T) {
	b := newFSMHardeningBlockchain(t)
	b.settings.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{{Height: 1}}
	b.settings.BlockChain.StoreDBTimeoutMillis = 1000
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	deadline, _ := ctx.Deadline()
	b.store = &fsmHardeningReadStore{Store: b.store, read: func(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
		actual, ok := ctx.Deadline()
		require.True(t, ok)
		require.Equal(t, deadline, actual)
		<-ctx.Done()
		return nil, nil, ctx.Err()
	}}
	require.Error(t, b.guardRunBelowHighestCheckpoint(ctx))
	require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
}

func TestGuardRunBelowHighestCheckpoint_NonPositiveTimeoutStillBounded(t *testing.T) {
	for _, timeout := range []int{0, -1} {
		t.Run(time.Duration(timeout).String(), func(t *testing.T) {
			b := newFSMHardeningBlockchain(t)
			b.settings.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{{Height: 1}}
			b.settings.BlockChain.StoreDBTimeoutMillis = timeout
			b.store = &fsmHardeningReadStore{Store: b.store, read: func(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
				deadline, ok := ctx.Deadline()
				require.True(t, ok, "invalid configuration must not leave the FSM lock held indefinitely")
				require.Positive(t, time.Until(deadline))
				return nil, nil, context.DeadlineExceeded
			}}
			require.Error(t, b.guardRunBelowHighestCheckpoint(context.Background()))
		})
	}
}
