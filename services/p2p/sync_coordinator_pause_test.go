package p2p

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/stores/blockchain/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// LocalClient's FSM accessor is a RUNNING placeholder. Inject that accessor
// while keeping the actual SQLite chain reads and real peer registry.
type syncCoordinatorStateClient struct {
	blockchain.ClientI
	state       func(context.Context) (*blockchain_api.FSMStateType, error)
	cachedState *blockchain_api.FSMStateType
}

func (c *syncCoordinatorStateClient) GetFSMCurrentState(ctx context.Context) (*blockchain_api.FSMStateType, error) {
	if c.cachedState != nil {
		return c.cachedState, nil
	}
	return c.state(ctx)
}

func (c *syncCoordinatorStateClient) ReadFSMState(ctx context.Context) (blockchain.FSMStateType, error) {
	state, err := c.state(ctx)
	if err != nil {
		return blockchain.FSMStateIDLE, err
	}
	if state == nil {
		return blockchain.FSMStateIDLE, errors.NewServiceError("FSM state unavailable")
	}
	return *state, nil
}

func (c *syncCoordinatorStateClient) GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	if c.ClientI == nil {
		return nil, nil, errors.NewServiceError("local tip unavailable")
	}
	return c.ClientI.GetBestBlockHeader(ctx)
}

func newPauseTestSyncCoordinator(t *testing.T) (*SyncCoordinator, *blockchain.CentralizedPeerRegistry, *sql.SQL) {
	t.Helper()
	sc, reg := newTestSyncCoordinator(t)
	tSettings := test.CreateBaseTestSettings(t)
	params := chaincfg.RegressionNetParams
	tSettings.ChainCfgParams = &params
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	store, err := sql.New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
	client, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, store, nil, nil)
	require.NoError(t, err)
	sc.blockchainClient = &syncCoordinatorStateClient{ClientI: client, state: func(ctx context.Context) (*blockchain_api.FSMStateType, error) {
		state, err := store.GetFSMState(ctx)
		if err != nil {
			return nil, err
		}
		value := blockchain_api.FSMStateType(blockchain_api.FSMStateType_value[state])
		return &value, nil
	}}
	reg.Register(&blockchain.PeerInfo{ID: "current", DataHubURL: "http://current", Height: 100, BlockHash: syncCoordinatorTestHash(t)})
	return sc, reg, store
}

func TestSyncCoordinator_PausePreservesPeerAndStallAgeOnResume(t *testing.T) {
	for _, path := range []string{"evaluation", "fsm monitor"} {
		t.Run(path, func(t *testing.T) {
			sc, reg, store := newPauseTestSyncCoordinator(t)
			sc.currentSyncPeer = "current"
			sc.lastSyncProgressTime = time.Now().Add(-time.Minute)
			require.NoError(t, store.SetFSMState(context.Background(), "IDLE"))
			check := sc.evaluateSyncPeer
			if path == "fsm monitor" {
				check = sc.checkFSMState
			}
			check()
			require.Equal(t, "current", sc.GetCurrentSyncPeer(), "paused elapsed time must not evict the peer")
			info, ok := reg.Get("current")
			require.True(t, ok)
			require.Zero(t, info.SyncAttemptCount)
			require.Zero(t, info.CatchupFailures)

			require.NoError(t, store.SetFSMState(context.Background(), "CATCHINGBLOCKS"))
			check()
			sc.evaluateSyncPeer()
			require.Equal(t, "current", sc.GetCurrentSyncPeer(), "resume must preserve the peer and its pre-pause progress age")
			_, age, timedOut := sc.syncPeerNoProgressTimedOut(time.Now())
			require.False(t, timedOut)
			require.GreaterOrEqual(t, age, time.Minute)
			require.Less(t, age, time.Minute+time.Second)

			sc.lastSyncProgressTime = time.Now().Add(-2 * defaultSyncPeerNoProgressLimit)
			sc.evaluateSyncPeer()
			require.Empty(t, sc.GetCurrentSyncPeer(), "a genuine later stall must still recover")
			info, ok = reg.Get("current")
			require.True(t, ok)
			require.Equal(t, int32(1), info.SyncAttemptCount)
		})
	}
}

func TestSyncCoordinator_IdleBreaksCompletionEdge(t *testing.T) {
	sc, _, store := newPauseTestSyncCoordinator(t)
	sc.currentSyncPeer = "current"
	sc.lastObservedFSMState = blockchain.FSMStateCATCHINGBLOCKS
	sc.lastObservedFSMStateSet = true
	sc.pendingCompletionPeer = "current"
	require.NoError(t, store.SetFSMState(context.Background(), "IDLE"))
	sc.checkFSMState()
	require.Equal(t, blockchain.FSMStateIDLE, sc.lastObservedFSMState)
	require.Empty(t, sc.pendingCompletionPeer)
	require.NoError(t, store.SetFSMState(context.Background(), "RUNNING"))
	sc.checkFSMState()
	require.Empty(t, sc.pendingCompletionPeer, "operator resume is not a completed catchup")
}

func TestSyncCoordinator_UnknownFSMTemporarilySuspendsEvaluation(t *testing.T) {
	for _, scenario := range []string{"nil state", "unknown state", "read error", "RPC timeout", "missing client"} {
		t.Run(scenario, func(t *testing.T) {
			sc, reg, _ := newPauseTestSyncCoordinator(t)
			sc.currentSyncPeer = "current"
			sc.lastSyncProgressTime = time.Now().Add(-2 * defaultSyncPeerNoProgressLimit)
			sc.rpcTimeout = 20 * time.Millisecond
			client := sc.blockchainClient.(*syncCoordinatorStateClient)
			client.state = func(ctx context.Context) (*blockchain_api.FSMStateType, error) {
				switch scenario {
				case "RPC timeout":
					<-ctx.Done()
					return nil, ctx.Err()
				case "nil state":
					return nil, nil
				case "read error":
					return nil, errors.NewServiceError("temporarily unavailable")
				default:
					state := blockchain_api.FSMStateType(999)
					return &state, nil
				}
			}
			if scenario == "missing client" {
				sc.blockchainClient = nil
			}
			sc.evaluateSyncPeer()
			require.Equal(t, "current", sc.GetCurrentSyncPeer())
			require.NotPanics(t, sc.checkFSMState)
			info, ok := reg.Get("current")
			require.True(t, ok)
			require.Zero(t, info.SyncAttemptCount)
		})
	}
}

func TestSyncCoordinator_PausedTriggersDoNotActivatePeer(t *testing.T) {
	for _, path := range []string{"direct trigger", "catchup failure", "disconnect"} {
		t.Run(path, func(t *testing.T) {
			sc, reg, store := newPauseTestSyncCoordinator(t)
			require.NoError(t, store.SetFSMState(context.Background(), "IDLE"))
			switch path {
			case "direct trigger":
				require.NoError(t, sc.TriggerSync())
			case "catchup failure":
				sc.HandleCatchupFailure("in-flight request failed")
			case "disconnect":
				checkedState := make(chan struct{}, 1)
				client := sc.blockchainClient.(*syncCoordinatorStateClient)
				readState := client.state
				client.state = func(ctx context.Context) (*blockchain_api.FSMStateType, error) {
					checkedState <- struct{}{}
					return readState(ctx)
				}
				id := mustNewPeerID(t)
				reg.Register(&blockchain.PeerInfo{ID: id.String()})
				sc.currentSyncPeer = id.String()
				sc.HandlePeerDisconnected(id)
				select {
				case <-checkedState:
				case <-time.After(5 * time.Second):
					t.Fatal("delayed disconnect trigger did not consult the FSM")
				}
				// Join the decision after its FSM read before inspecting activation.
				sc.decisionMu.Lock()
				sc.decisionMu.Unlock()
			}
			require.Empty(t, sc.GetCurrentSyncPeer())
			info, ok := reg.Get("current")
			require.True(t, ok)
			require.Zero(t, info.SyncAttemptCount)
			state, err := store.GetFSMState(context.Background())
			require.NoError(t, err)
			require.Equal(t, "IDLE", state, "coordinator must never resume the FSM")
		})
	}
}

func TestSyncCoordinator_StaleCachedIdleDoesNotPreventReplacement(t *testing.T) {
	for _, path := range []string{"trigger", "evaluation", "catchup failure"} {
		t.Run(path, func(t *testing.T) {
			sc, reg, store := newPauseTestSyncCoordinator(t)
			require.NoError(t, store.SetFSMState(context.Background(), "RUNNING"))
			idle := blockchain_api.FSMStateType_IDLE
			sc.blockchainClient.(*syncCoordinatorStateClient).cachedState = &idle
			switch path {
			case "trigger":
				require.NoError(t, sc.TriggerSync())
			case "evaluation":
				sc.currentSyncPeer = "removed"
				sc.evaluateSyncPeer()
			case "catchup failure":
				reg.Register(&blockchain.PeerInfo{ID: "failed"})
				sc.currentSyncPeer = "failed"
				sc.HandleCatchupFailure("request failed")
			}
			require.Equal(t, "current", sc.GetCurrentSyncPeer())
		})
	}
}

func TestSyncCoordinator_TransientFSMFailuresDoNotResetStallClock(t *testing.T) {
	sc, reg, store := newPauseTestSyncCoordinator(t)
	require.NoError(t, store.SetFSMState(context.Background(), "RUNNING"))
	sc.currentSyncPeer = "current"
	lastProgress := time.Now().Add(-2 * defaultSyncPeerNoProgressLimit)
	sc.lastSyncProgressTime = lastProgress
	client := sc.blockchainClient.(*syncCoordinatorStateClient)
	readState := client.state
	for range 3 {
		client.state = func(context.Context) (*blockchain_api.FSMStateType, error) {
			return nil, errors.NewServiceError("temporary read failure")
		}
		sc.evaluateSyncPeer()
		client.state = readState
		sc.decisionMu.Lock()
		require.NotNil(t, sc.activeSyncStateLocked())
		sc.decisionMu.Unlock()
		require.Equal(t, lastProgress, sc.lastSyncProgressTime)
	}
	sc.evaluateSyncPeer()
	require.Empty(t, sc.GetCurrentSyncPeer())
	info, ok := reg.Get("current")
	require.True(t, ok)
	require.Equal(t, int32(1), info.SyncAttemptCount)
}

func TestSyncCoordinator_UnavailableFSMStillClearsMissingPeer(t *testing.T) {
	sc, _, _ := newPauseTestSyncCoordinator(t)
	sc.currentSyncPeer = "removed"
	sc.blockchainClient.(*syncCoordinatorStateClient).state = func(context.Context) (*blockchain_api.FSMStateType, error) {
		return nil, errors.NewServiceError("blockchain restarting")
	}
	sc.evaluateSyncPeer()
	require.Empty(t, sc.GetCurrentSyncPeer())
}

func TestSyncCoordinator_PauseExcludesOnlyPausedProgressTime(t *testing.T) {
	for _, scenario := range []string{"prior progress", "initial deadline", "progress during pause"} {
		t.Run(scenario, func(t *testing.T) {
			sc, _, store := newPauseTestSyncCoordinator(t)
			sc.currentSyncPeer = "current"
			require.NoError(t, store.SetFSMState(context.Background(), "IDLE"))
			sc.decisionMu.Lock()
			defer sc.decisionMu.Unlock()
			require.Nil(t, sc.activeSyncStateLocked())
			// Advance a long confirmed pause without sleeping. The peer had
			// already used one minute of its delivery budget before STOP.
			sc.syncPausedAt = time.Now().Add(-2 * defaultSyncPeerNoProgressLimit)
			priorProgress := sc.syncPausedAt.Add(-time.Minute)
			wantAge := time.Minute
			switch scenario {
			case "prior progress":
				sc.lastSyncProgressTime = priorProgress
			case "initial deadline":
				sc.syncStartTime = priorProgress
			case "progress during pause":
				sc.lastSyncProgressTime = sc.syncPausedAt.Add(time.Minute)
				wantAge = 0
			}
			require.NoError(t, store.SetFSMState(context.Background(), "RUNNING"))
			require.NotNil(t, sc.activeSyncStateLocked())
			_, age, timedOut := sc.syncPeerNoProgressTimedOut(time.Now())
			require.False(t, timedOut)
			require.GreaterOrEqual(t, age, wantAge)
			require.Less(t, age, wantAge+time.Second)
			require.True(t, sc.syncPausedAt.IsZero())
		})
	}
}
