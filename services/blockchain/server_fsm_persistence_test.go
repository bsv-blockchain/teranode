package blockchain

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-chaincfg"
	teranodeerrors "github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/settings"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
)

// fsmPersistenceStore uses SQLite for all storage, injecting failures only at
// the FSM write boundary to exercise both definite and ambiguous write errors.
type fsmPersistenceStore struct {
	blockchainstore.Store
	writeErr          error
	commitBeforeError bool
	beforeWrite       func(context.Context)
}

func (s *fsmPersistenceStore) SetFSMState(ctx context.Context, state string) error {
	if s.beforeWrite != nil {
		s.beforeWrite(ctx)
	}
	if s.writeErr != nil && !s.commitBeforeError {
		return s.writeErr
	}
	if err := s.Store.SetFSMState(ctx, state); err != nil {
		return err
	}
	return s.writeErr
}

func newFSMPersistenceTestBlockchain(t *testing.T, state blockchain_api.FSMStateType) (*Blockchain, *fsmPersistenceStore) {
	t.Helper()
	initPrometheusMetrics()
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	tSettings := &settings.Settings{ChainCfgParams: &chaincfg.RegressionNetParams}
	store, err := blockchainstore.NewStore(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
	require.NoError(t, store.SetFSMState(context.Background(), state.String()))
	faultStore := &fsmPersistenceStore{Store: store}
	b := &Blockchain{
		logger: ulogger.TestLogger{}, store: faultStore, settings: tSettings,
		notifications: make(chan *blockchain_api.Notification, 10),
		stats:         gocore.NewStat("blockchain-fsm-persistence-test"),
	}
	b.finiteStateMachine = b.NewFiniteStateMachine()
	b.finiteStateMachine.SetState(state.String())
	return b, faultStore
}

func TestSendFSMEvent_PersistenceFailureDoesNotTransition(t *testing.T) {
	for _, tt := range []struct {
		name  string
		from  blockchain_api.FSMStateType
		event blockchain_api.FSMEventType
		to    blockchain_api.FSMStateType
	}{
		{"run", blockchain_api.FSMStateType_IDLE, blockchain_api.FSMEventType_RUN, blockchain_api.FSMStateType_RUNNING},
		{"catchup", blockchain_api.FSMStateType_RUNNING, blockchain_api.FSMEventType_CATCHUPBLOCKS, blockchain_api.FSMStateType_CATCHINGBLOCKS},
		{"stop", blockchain_api.FSMStateType_RUNNING, blockchain_api.FSMEventType_STOP, blockchain_api.FSMStateType_IDLE},
	} {
		t.Run(tt.name, func(t *testing.T) {
			b, store := newFSMPersistenceTestBlockchain(t, tt.from)
			store.writeErr = teranodeerrors.NewStorageError("injected FSM write failure")
			priorTimestamp := time.Now().Add(-time.Minute)
			b.stateChangeTimestamp = priorTimestamp
			req := &blockchain_api.SendFSMEventRequest{Event: tt.event}
			resp, err := b.SendFSMEvent(context.Background(), req)
			require.Error(t, err)
			require.ErrorContains(t, teranodeerrors.UnwrapGRPC(err), "injected FSM write failure")
			require.Equal(t, teranodeerrors.ERR_STORAGE_ERROR, teranodeerrors.UnwrapGRPC(err).Code())
			require.Nil(t, resp)
			require.Equal(t, tt.from.String(), b.finiteStateMachine.Current())
			persisted, err := store.GetFSMState(context.Background())
			require.NoError(t, err)
			require.Equal(t, tt.from.String(), persisted)
			require.Empty(t, b.notifications)
			require.Equal(t, priorTimestamp, b.stateChangeTimestamp)

			store.writeErr = nil
			resp, err = b.SendFSMEvent(context.Background(), req)
			require.NoError(t, err, "failed write must leave the FSM available for retry")
			require.Equal(t, tt.to, resp.State)
			persisted, err = store.GetFSMState(context.Background())
			require.NoError(t, err)
			require.Equal(t, tt.to.String(), persisted)
			require.Len(t, b.notifications, 1)
		})
	}
}

func TestSendFSMEvent_PersistsBeforeStateAndNotification(t *testing.T) {
	b, store := newFSMPersistenceTestBlockchain(t, blockchain_api.FSMStateType_IDLE)
	store.beforeWrite = func(ctx context.Context) {
		require.Equal(t, blockchain_api.FSMStateType_IDLE.String(), b.finiteStateMachine.Current())
		require.Empty(t, b.notifications, "subscribers must not observe success before the write completes")
		deadline, ok := ctx.Deadline()
		require.True(t, ok, "store write must have a bounded deadline")
		require.Positive(t, time.Until(deadline))
	}
	resp, err := b.SendFSMEvent(context.Background(), &blockchain_api.SendFSMEventRequest{Event: blockchain_api.FSMEventType_RUN})
	require.NoError(t, err)
	require.Equal(t, blockchain_api.FSMStateType_RUNNING, resp.State)
	require.Len(t, b.notifications, 1)
	notification := <-b.notifications
	require.Equal(t, blockchain_api.FSMStateType_RUNNING.String(), notification.Metadata.Metadata["destination"])
	persisted, err := store.GetFSMState(context.Background())
	require.NoError(t, err)
	require.Equal(t, blockchain_api.FSMStateType_RUNNING.String(), persisted)
}

func TestSendFSMEvent_AmbiguousWriteCanRetry(t *testing.T) {
	b, store := newFSMPersistenceTestBlockchain(t, blockchain_api.FSMStateType_RUNNING)
	store.writeErr = teranodeerrors.NewStorageError("injected lost write acknowledgement")
	store.commitBeforeError = true
	_, err := b.Idle(context.Background(), &emptypb.Empty{})
	require.Error(t, err)
	require.ErrorContains(t, teranodeerrors.UnwrapGRPC(err), "injected lost write acknowledgement")
	require.Equal(t, blockchain_api.FSMStateType_RUNNING.String(), b.finiteStateMachine.Current())
	require.Empty(t, b.notifications)
	// An error cannot establish whether the database committed. Do not roll back
	// the persisted state: an explicit retry rewrites the same target safely.
	persisted, err := store.GetFSMState(context.Background())
	require.NoError(t, err)
	require.Equal(t, blockchain_api.FSMStateType_IDLE.String(), persisted)
	store.writeErr = nil
	_, err = b.Idle(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.Equal(t, blockchain_api.FSMStateType_IDLE.String(), b.finiteStateMachine.Current())
	require.Len(t, b.notifications, 1)
}

func TestSendFSMEvent_CanceledCallerDuringPersistence(t *testing.T) {
	b, store := newFSMPersistenceTestBlockchain(t, blockchain_api.FSMStateType_IDLE)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store.beforeWrite = func(storeCtx context.Context) {
		cancel()
		require.NoError(t, storeCtx.Err(), "accepted write must survive caller cancellation")
	}
	resp, err := b.SendFSMEvent(ctx, &blockchain_api.SendFSMEventRequest{Event: blockchain_api.FSMEventType_RUN})
	require.NoError(t, err)
	require.Equal(t, blockchain_api.FSMStateType_RUNNING, resp.State)
	state, err := store.GetFSMState(context.Background())
	require.NoError(t, err)
	require.Equal(t, state, b.finiteStateMachine.Current())
	require.Len(t, b.notifications, 1)
}

func TestSendFSMEvent_PersistenceTimeoutDoesNotWedge(t *testing.T) {
	b, store := newFSMPersistenceTestBlockchain(t, blockchain_api.FSMStateType_IDLE)
	b.settings.BlockChain.StoreDBTimeoutMillis = 5
	store.beforeWrite = func(ctx context.Context) {
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
			t.Fatal("FSM write did not time out")
		}
	}
	req := &blockchain_api.SendFSMEventRequest{Event: blockchain_api.FSMEventType_RUN}
	resp, err := b.SendFSMEvent(context.Background(), req)
	require.Error(t, err)
	require.Nil(t, resp)
	require.Equal(t, blockchain_api.FSMStateType_IDLE.String(), b.finiteStateMachine.Current())
	require.Empty(t, b.notifications)
	store.beforeWrite = nil
	b.settings.BlockChain.StoreDBTimeoutMillis = 5000
	resp, err = b.SendFSMEvent(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, blockchain_api.FSMStateType_RUNNING, resp.State)
}
