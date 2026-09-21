package netsync

import (
	"context"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Keep real chain reads and state transitions; vary only the client's cached
// observation to model notification loss and recovery.
type legacyPromotionClient struct {
	blockchain.ClientI
	authority   *blockchain.Blockchain
	observed    atomic.Pointer[blockchain.FSMStateType]
	readFailure atomic.Bool
	runCalls    atomic.Int32
}

func (c *legacyPromotionClient) GetFSMCurrentState(context.Context) (*blockchain.FSMStateType, error) {
	if c.readFailure.Load() {
		return nil, errors.NewServiceUnavailableError("state unavailable")
	}
	return c.observed.Load(), nil
}

func (c *legacyPromotionClient) Run(ctx context.Context, _ string) error {
	c.runCalls.Add(1)
	_, err := c.authority.Run(ctx, &emptypb.Empty{})
	if err != nil {
		return errors.UnwrapGRPC(err)
	}
	return nil
}

func newLegacyPromotionManager(t *testing.T, state blockchain.FSMStateType) (*SyncManager, *legacyPromotionClient) {
	t.Helper()
	settings := test.CreateBaseTestSettings(t)
	store, err := blockchainstore.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, settings)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
	block := testhelpers.CreateTestBlocks(t, 1)[0]
	block.Header.HashPrevBlock = settings.ChainCfgParams.GenesisHash
	block.Header.Timestamp = uint32(time.Now().Unix()) //nolint:gosec // Test block timestamp, well within uint32 until 2106.
	_, _, err = store.StoreBlock(context.Background(), block, "test")
	require.NoError(t, err)
	authority, err := blockchain.New(context.Background(), ulogger.TestLogger{}, settings, store, nil, state.String())
	require.NoError(t, err)
	require.NoError(t, authority.Init(context.Background()))
	authority.SetSubscriptionManagerReadyForTesting(true)
	local, err := blockchain.NewLocalClient(ulogger.TestLogger{}, settings, store, nil, nil)
	require.NoError(t, err)
	client := &legacyPromotionClient{ClientI: local, authority: authority}
	client.observed.Store(&state)
	manager := &SyncManager{
		ctx: context.Background(), logger: ulogger.TestLogger{}, settings: settings,
		chainParams: settings.ChainCfgParams, blockchainClient: client,
		peerStates: txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState](),
		msgChan:    make(chan interface{}), quit: make(chan struct{}), handlerDone: make(chan struct{}),
	}
	require.True(t, manager.current(), "fixture must exercise promotion on a caught-up chain")
	return manager, client
}

func startLegacyPromotionLoop(t *testing.T, manager *SyncManager) {
	t.Helper()
	go manager.blockHandler()
	t.Cleanup(func() {
		close(manager.quit)
		select {
		case <-manager.handlerDone:
		case <-time.After(time.Second):
			t.Error("legacy block handler did not stop")
		}
	})
}

func deliverLegacyPromotionEvent(t *testing.T, manager *SyncManager) {
	t.Helper()
	reply := make(chan bool, 1)
	select {
	case manager.msgChan <- isCurrentMsg{reply: reply}:
	case <-time.After(time.Second):
		t.Fatal("legacy message was not accepted")
	}
	select {
	case current := <-reply:
		require.True(t, current)
	case <-time.After(time.Second):
		t.Fatal("legacy message was not processed")
	}
}

func requireLegacyPromotionState(t *testing.T, client *legacyPromotionClient, want blockchain.FSMStateType) {
	t.Helper()
	state, err := client.authority.GetFSMCurrentState(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.Equal(t, want, state.State)
	persisted, err := client.authority.GetStoreFSMState(context.Background())
	require.NoError(t, err)
	require.Equal(t, want.String(), persisted)
}

func TestLegacyPromotionLoop_SkipsIdleAndResumesAfterCacheRecovery(t *testing.T) {
	manager, client := newLegacyPromotionManager(t, blockchain.FSMStateIDLE)
	startLegacyPromotionLoop(t, manager)
	for range 3 {
		deliverLegacyPromotionEvent(t, manager)
	}
	require.Zero(t, client.runCalls.Load(), "recurring messages while IDLE must not send refused RUN RPCs")
	requireLegacyPromotionState(t, client, blockchain.FSMStateIDLE)

	_, err := client.authority.SendFSMEvent(context.Background(), &blockchain_api.SendFSMEventRequest{Event: blockchain_api.FSMEventType_CATCHUPBLOCKS})
	require.NoError(t, err)
	deliverLegacyPromotionEvent(t, manager) // Still synthetic/cached IDLE.
	require.Zero(t, client.runCalls.Load())
	catching := blockchain.FSMStateCATCHINGBLOCKS
	client.observed.Store(&catching)
	deliverLegacyPromotionEvent(t, manager)
	require.Equal(t, int32(1), client.runCalls.Load())
	requireLegacyPromotionState(t, client, blockchain.FSMStateRUNNING)

	running := blockchain.FSMStateRUNNING
	client.observed.Store(&running)
	deliverLegacyPromotionEvent(t, manager)
	require.Equal(t, int32(1), client.runCalls.Load(), "cached RUNNING needs no automatic promotion")
}

func TestLegacyPromotionLoop_StaleCatchingCannotOverrideIdle(t *testing.T) {
	manager, client := newLegacyPromotionManager(t, blockchain.FSMStateIDLE)
	catching := blockchain.FSMStateCATCHINGBLOCKS
	client.observed.Store(&catching)
	startLegacyPromotionLoop(t, manager)
	deliverLegacyPromotionEvent(t, manager)
	require.Equal(t, int32(1), client.runCalls.Load())
	requireLegacyPromotionState(t, client, blockchain.FSMStateIDLE)
}

func TestLegacyPromotionLoop_UnknownObservationDoesNotPromote(t *testing.T) {
	for _, tc := range []struct {
		name   string
		state  *blockchain.FSMStateType
		failed bool
	}{
		{name: "missing"},
		{name: "read error", failed: true},
		{name: "unknown", state: func() *blockchain.FSMStateType { state := blockchain.FSMStateType(99); return &state }()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manager, client := newLegacyPromotionManager(t, blockchain.FSMStateCATCHINGBLOCKS)
			client.observed.Store(tc.state)
			client.readFailure.Store(tc.failed)
			startLegacyPromotionLoop(t, manager)
			deliverLegacyPromotionEvent(t, manager)
			require.Zero(t, client.runCalls.Load())
			requireLegacyPromotionState(t, client, blockchain.FSMStateCATCHINGBLOCKS)
		})
	}
}

func TestLegacyStartSync_SkipsIdlePromotion(t *testing.T) {
	manager, client := newLegacyPromotionManager(t, blockchain.FSMStateIDLE)
	config := peerpkg.Config{UserAgentName: "fsm-promotion-test", UserAgentVersion: "1.0", ChainParams: manager.chainParams}
	remote, peer, err := MakeConnectedPeers(t, config, config, 93)
	require.NoError(t, err)
	t.Cleanup(func() {
		peer.DisconnectWithInfo("test complete")
		remote.DisconnectWithInfo("test complete")
		peer.WaitForDisconnect()
		remote.WaitForDisconnect()
	})
	_, meta, err := client.GetBestBlockHeader(context.Background())
	require.NoError(t, err)
	peer.UpdateLastBlockHeight(int32(meta.Height)) //nolint:gosec // Fixture has one block above genesis.
	manager.peerStates.Set(peer, &peerSyncState{syncCandidate: true})
	for range 3 {
		manager.startSync()
	}
	require.Zero(t, client.runCalls.Load())
	requireLegacyPromotionState(t, client, blockchain.FSMStateIDLE)

	_, err = client.authority.SendFSMEvent(context.Background(), &blockchain_api.SendFSMEventRequest{Event: blockchain_api.FSMEventType_CATCHUPBLOCKS})
	require.NoError(t, err)
	catching := blockchain.FSMStateCATCHINGBLOCKS
	client.observed.Store(&catching)
	manager.startSync()
	require.Equal(t, int32(1), client.runCalls.Load())
	requireLegacyPromotionState(t, client, blockchain.FSMStateRUNNING)
}

func TestLegacyPromotionPrefilter_ReadFailureDoesNotSendRun(t *testing.T) {
	manager, client := newLegacyPromotionManager(t, blockchain.FSMStateCATCHINGBLOCKS)
	client.readFailure.Store(true)
	require.Error(t, manager.runIfCatchingBlocks("test"))
	require.Zero(t, client.runCalls.Load())
	requireLegacyPromotionState(t, client, blockchain.FSMStateCATCHINGBLOCKS)
}
