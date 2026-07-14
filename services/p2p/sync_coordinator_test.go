package p2p

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func newTestSyncCoordinator(t *testing.T) (*SyncCoordinator, *blockchain.CentralizedPeerRegistry) {
	t.Helper()

	tSettings := &settings.Settings{
		P2P: settings.P2PSettings{
			AllowPrunedNodeFallback:                   true,
			MaxUnvalidatedAdvertisedHeightLead:        10_000,
			MaxUnprovenSyncProbesPerBackoffWindow:     3,
			FullDeliveryFreshnessWindow:               24 * time.Hour,
			SyncCoordinatorPeriodicEvaluationInterval: 30 * time.Second,
		},
	}
	return newTestSyncCoordinatorWithSettings(t, tSettings)
}

func newTestSyncCoordinatorWithSettings(t *testing.T, tSettings *settings.Settings) (*SyncCoordinator, *blockchain.CentralizedPeerRegistry) {
	t.Helper()

	reg := blockchain.NewCentralizedPeerRegistry(blockchain.DefaultBanConfig())
	client := blockchain.NewLocalPeerRegistryClient(reg)

	sc := NewSyncCoordinator(
		context.Background(),
		ulogger.TestLogger{},
		tSettings,
		client,
		NewPeerSelector(ulogger.TestLogger{}, tSettings),
		nil, // blockchainClient — only the FSM monitor needs it; not exercised here
		nil, // kafka producer — only TriggerSync's send-to-kafka path uses it
	)
	sc.SetGetLocalHeightCallback(func() uint32 { return 0 })
	return sc, reg
}

func syncCoordinatorTestHash(t *testing.T) *chainhash.Hash {
	t.Helper()

	hash, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)
	return hash
}

func setSyncCoordinatorLocalTip(t *testing.T, sc *SyncCoordinator, height uint32, chainWork []byte) *blockchain.Mock {
	t.Helper()

	client := &blockchain.Mock{}
	client.On("GetBestBlockHeader", mock.Anything).Return(
		&model.BlockHeader{},
		&model.BlockHeaderMeta{Height: height, ChainWork: chainWork},
		nil,
	)
	sc.blockchainClient = client
	return client
}

func setSyncCoordinatorLocalTipError(t *testing.T, sc *SyncCoordinator, err error) *blockchain.Mock {
	t.Helper()

	client := &blockchain.Mock{}
	client.On("GetBestBlockHeader", mock.Anything).Return(nil, nil, err)
	sc.blockchainClient = client
	return client
}

func setSyncCoordinatorProbeBudget(sc *SyncCoordinator, budget int) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.unprovenProbeBudgetRemaining = budget
}

// filterEligiblePeersForTest resolves the local tip work and delegates to
// filterEligiblePeersWithTip, mirroring the compact call form previously offered by the
// (now-removed) production filterEligiblePeers helper, which had no non-test callers.
func filterEligiblePeersForTest(sc *SyncCoordinator, peers []*blockchain.PeerInfo, oldPeer string, localHeight uint32) []*blockchain.PeerInfo {
	tipHeight, localChainWork, localWorkOK := sc.getLocalTipWorkSafe(sc.ctx)
	if localWorkOK {
		localHeight = tipHeight
	}
	return sc.filterEligiblePeersWithTip(peers, oldPeer, localHeight, localChainWork, localWorkOK)
}

func syncCoordinatorProbeBudget(sc *SyncCoordinator) int {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.unprovenProbeBudgetRemaining
}

func TestSyncCoordinator_IsViableSyncCandidate(t *testing.T) {
	good := &blockchain.PeerInfo{DataHubURL: "http://x", ReputationScore: 50}
	require.True(t, isViableSyncCandidate(good))

	cases := []struct {
		name string
		p    *blockchain.PeerInfo
	}{
		{"banned", &blockchain.PeerInfo{IsBanned: true, DataHubURL: "x", Height: 1, ReputationScore: 50}},
		{"no url", &blockchain.PeerInfo{Height: 1, ReputationScore: 50}},
		{"low rep", &blockchain.PeerInfo{DataHubURL: "x", Height: 1, ReputationScore: 5}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.False(t, isViableSyncCandidate(c.p))
		})
	}
}

func TestSyncCoordinator_ListAllPeers(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)
	require.Empty(t, sc.listAllPeers())

	reg.Register(&blockchain.PeerInfo{ID: "a"})
	reg.Register(&blockchain.PeerInfo{ID: "b"})

	require.Len(t, sc.listAllPeers(), 2)
}

func TestSyncCoordinator_GetCurrentSyncPeer_DefaultsEmpty(t *testing.T) {
	sc, _ := newTestSyncCoordinator(t)
	require.Empty(t, sc.GetCurrentSyncPeer())
}

func TestSyncCoordinator_ClearSyncPeer(t *testing.T) {
	sc, _ := newTestSyncCoordinator(t)

	sc.mu.Lock()
	sc.currentSyncPeer = "preset-peer"
	sc.mu.Unlock()

	sc.ClearSyncPeer()
	require.Empty(t, sc.GetCurrentSyncPeer())
}

func TestSyncCoordinator_IsCaughtUp_NoPeers(t *testing.T) {
	sc, _ := newTestSyncCoordinator(t)
	require.True(t, sc.isCaughtUp(), "no peers means we are caught up")
}

func TestSyncCoordinator_IsCaughtUp_AheadPeerMakesUsBehind(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)

	reg.Register(&blockchain.PeerInfo{
		ID:               "ahead",
		DataHubURL:       "http://ahead",
		Height:           100,
		BlockHash:        syncCoordinatorTestHash(t),
		TransportType:    0,
		TransportTypeSet: false,
	})
	// Boost reputation past 20 so the peer is viable.
	for i := 0; i < 5; i++ {
		reg.UpdateMetrics("ahead", 0, 0, 0, true, false, false, 100)
	}

	require.False(t, sc.isCaughtUp())
}

func TestSyncCoordinator_IsCaughtUp_OnlyLowRepPeerIsCaughtUp(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)

	// Peer is ahead in height but reputation < 20 → not viable, so we are caught up.
	reg.Register(&blockchain.PeerInfo{ID: "low-rep", DataHubURL: "http://low-rep", Height: 100})
	// Register sets reputation to 50; drive it below 20 with a malicious event.
	reg.UpdateMetrics("low-rep", 0, 0, 0, false, false, true, 0)

	require.True(t, sc.isCaughtUp())
}

func TestSyncCoordinator_HandlePeerDisconnected_RemovesPeer(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)
	pid := mustNewPeerID(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String()})

	sc.HandlePeerDisconnected(pid)

	_, ok := reg.Get(pid.String())
	require.False(t, ok)
}

func TestSyncCoordinator_HandleCatchupFailure_NoSyncPeer(t *testing.T) {
	sc, _ := newTestSyncCoordinator(t)
	require.NotPanics(t, func() { sc.HandleCatchupFailure("test") })
}

func TestSyncCoordinator_GetPeer_ByLibp2pID(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)
	pid := mustNewPeerID(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String(), Height: 42})

	got, found := sc.getPeer(pid)
	require.True(t, found)
	require.Equal(t, uint32(42), got.Height)
}

func TestSyncCoordinator_BackoffLifecycle(t *testing.T) {
	sc, _ := newTestSyncCoordinator(t)

	// Not in backoff initially.
	require.False(t, sc.checkAndClearExpiredBackoff())

	// Enter backoff.
	sc.enterBackoffMode()
	sc.mu.RLock()
	require.True(t, sc.allPeersAttempted)
	sc.mu.RUnlock()

	// resetBackoff clears state.
	sc.resetBackoff()
	sc.mu.RLock()
	require.False(t, sc.allPeersAttempted)
	require.Equal(t, 1, sc.backoffMultiplier)
	sc.mu.RUnlock()
}

func TestSyncCoordinator_ConsiderReputationRecovery_NoCandidatesIsNoOp(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)

	// Register a healthy peer; ReconsiderBadPeers won't touch it.
	reg.Register(&blockchain.PeerInfo{ID: "healthy", DataHubURL: "http://h"})
	for i := 0; i < 5; i++ {
		reg.UpdateMetrics("healthy", 0, 0, 0, true, false, false, 100)
	}

	require.NotPanics(t, func() { sc.considerReputationRecovery() })
	got, _ := reg.Get("healthy")
	require.GreaterOrEqual(t, got.ReputationScore, 50.0, "healthy peer reputation untouched")
}

func TestSyncCoordinator_UpdatePeerInfo_RegistersPeer(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)
	pid := mustNewPeerID(t)

	sc.UpdatePeerInfo(pid, 200, nil, "http://updated")

	got, ok := reg.Get(pid.String())
	require.True(t, ok)
	require.Equal(t, uint32(200), got.Height)
	require.Equal(t, "http://updated", got.DataHubURL)
}

func TestSyncCoordinator_UpdateBanStatus_OnUnknownPeerNoPanic(t *testing.T) {
	sc, _ := newTestSyncCoordinator(t)
	pid := mustNewPeerID(t)
	require.NotPanics(t, func() { sc.UpdateBanStatus(pid) })
}

func TestSyncCoordinator_TriggerSync_NoEligiblePeersEntersBackoff(t *testing.T) {
	sc, _ := newTestSyncCoordinator(t)

	// No peers registered → selectNewSyncPeer returns "" → checkAllPeersAttempted runs.
	require.NoError(t, sc.TriggerSync())

	// Backoff should NOT be entered yet because there were 0 eligible candidates,
	// not because all candidates were recently attempted.
	sc.mu.RLock()
	require.False(t, sc.allPeersAttempted)
	sc.mu.RUnlock()
}

func TestSyncCoordinator_SelectNewSyncPeer_PrefersFullNode(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)
	sc.SetGetLocalHeightCallback(func() uint32 { return 50 })

	reg.Register(&blockchain.PeerInfo{ID: "pruned", DataHubURL: "http://p", Height: 100, BlockHash: syncCoordinatorTestHash(t), Storage: "pruned"})
	reg.Register(&blockchain.PeerInfo{ID: "full", DataHubURL: "http://f", Height: 100, BlockHash: syncCoordinatorTestHash(t), Storage: "full"})
	for _, id := range []string{"pruned", "full"} {
		for i := 0; i < 5; i++ {
			reg.UpdateMetrics(id, 0, 0, 0, true, false, false, 100)
		}
	}

	require.Equal(t, "full", sc.selectNewSyncPeer())
}

func TestSyncCoordinator_FilterEligiblePeers_DropsLowAndOldPeer(t *testing.T) {
	sc, _ := newTestSyncCoordinator(t)

	peers := []*blockchain.PeerInfo{
		{ID: "old", DataHubURL: "x", Height: 100, BlockHash: syncCoordinatorTestHash(t), ReputationScore: 80},
		{ID: "low", DataHubURL: "x", Height: 10, BlockHash: syncCoordinatorTestHash(t), ReputationScore: 80},
		{ID: "good", DataHubURL: "x", Height: 100, BlockHash: syncCoordinatorTestHash(t), ReputationScore: 80},
	}

	got := filterEligiblePeersForTest(sc, peers, "old", 50)

	require.Len(t, got, 1)
	require.Equal(t, "good", got[0].ID)
}

func TestSyncCoordinator_LogPeerList_NoPanicOnEmptyAndPopulated(t *testing.T) {
	sc, _ := newTestSyncCoordinator(t)
	require.NotPanics(t, func() { sc.logPeerList(nil) })
	require.NotPanics(t, func() {
		sc.logPeerList([]*blockchain.PeerInfo{{ID: "p", DataHubURL: "x", Height: 1}})
	})
}

func TestSyncCoordinator_LogCandidateList_NoPanic(t *testing.T) {
	sc, _ := newTestSyncCoordinator(t)
	require.NotPanics(t, func() {
		sc.logCandidateList([]*blockchain.PeerInfo{
			{ID: "fresh", DataHubURL: "x", Height: 1},
			{ID: "tried", DataHubURL: "x", Height: 1, LastSyncAttempt: time.Now().Add(-1 * time.Minute)},
		})
	})
}

func TestSyncCoordinator_SendSyncMessage_PeerNotFoundErrors(t *testing.T) {
	sc, _ := newTestSyncCoordinator(t)
	err := sc.sendSyncMessage("not-in-registry")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")
}

func TestSyncCoordinator_SendSyncMessage_NoBlockHashErrors(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)
	reg.Register(&blockchain.PeerInfo{ID: "p", DataHubURL: "x", Height: 100})

	err := sc.sendSyncMessage("p")
	require.Error(t, err)
	require.Contains(t, err.Error(), "no block hash")
}

func TestSyncCoordinator_EvaluateSyncPeer_NoSyncPeerReturns(t *testing.T) {
	sc, _ := newTestSyncCoordinator(t)
	require.NotPanics(t, func() { sc.evaluateSyncPeer() })
}

func TestSyncCoordinator_EvaluateSyncPeer_LowRepClearsSyncPeer(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)
	reg.Register(&blockchain.PeerInfo{ID: "p", DataHubURL: "http://p"})
	// Drive reputation below 20 via malicious mark.
	reg.UpdateMetrics("p", 0, 0, 0, false, false, true, 0)

	sc.mu.Lock()
	sc.currentSyncPeer = "p"
	sc.syncStartTime = time.Now()
	sc.mu.Unlock()

	sc.evaluateSyncPeer()

	require.Empty(t, sc.GetCurrentSyncPeer(), "low-rep sync peer must be cleared")
}

func TestSyncCoordinator_EvaluateSyncPeer_MissingPeerClears(t *testing.T) {
	sc, _ := newTestSyncCoordinator(t)

	sc.mu.Lock()
	sc.currentSyncPeer = "phantom"
	sc.syncStartTime = time.Now()
	sc.mu.Unlock()

	sc.evaluateSyncPeer()
	require.Empty(t, sc.GetCurrentSyncPeer())
}

func TestSyncCoordinator_SelectAndActivateNewPeer_NoEligibleEntersBackoff(t *testing.T) {
	sc, _ := newTestSyncCoordinator(t)

	require.NoError(t, sc.selectAndActivateNewPeer(50, ""))

	sc.mu.RLock()
	require.True(t, sc.allPeersAttempted, "no peers above local height should enter backoff")
	sc.mu.RUnlock()
}

func TestSyncCoordinator_SelectAndActivateNewPeer_ActivatesEligible(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)

	reg.Register(&blockchain.PeerInfo{ID: "good", DataHubURL: "http://g", Height: 100, BlockHash: syncCoordinatorTestHash(t), Storage: "full"})
	for i := 0; i < 5; i++ {
		reg.UpdateMetrics("good", 0, 0, 0, true, false, false, 100)
	}

	// selectAndActivateNewPeer fires sendSyncMessage; the coordinator records the peer
	// as the current sync target even without a Kafka producer in this test.
	require.NoError(t, sc.selectAndActivateNewPeer(50, ""))

	require.Equal(t, "good", sc.GetCurrentSyncPeer())
}

func TestSyncCoordinator_SelectAndActivateNewPeer_StoresIDEvenIfSendFails(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)
	setSyncCoordinatorLocalTip(t, sc, 10, []byte{0x02})

	reg.Register(&blockchain.PeerInfo{
		ID:                 "doomed-peer",
		DataHubURL:         "http://doomed",
		ValidatedBlockHash: syncCoordinatorTestHash(t),
		ValidatedChainWork: []byte{0x03},
	})

	err := sc.selectAndActivateNewPeer(10, "")

	require.Error(t, err)
	require.Equal(t, "doomed-peer", sc.GetCurrentSyncPeer())
	sc.mu.RLock()
	require.False(t, sc.syncStartTime.IsZero())
	sc.mu.RUnlock()
}

func TestSyncCoordinator_ColdStart_FarBehind_WithAdvertisedOnlyPeers_InitiatesSync(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)
	sc.SetGetLocalHeightCallback(func() uint32 { return 0 })
	setSyncCoordinatorLocalTip(t, sc, 0, []byte{0x01})

	reg.Register(&blockchain.PeerInfo{
		ID:         "advertised",
		DataHubURL: "http://advertised",
		Height:     10_000,
		BlockHash:  syncCoordinatorTestHash(t),
		Storage:    "full",
	})

	require.False(t, sc.isCaughtUp())

	err := sc.selectAndActivateNewPeer(0, "")
	require.NoError(t, err)
	require.Equal(t, "advertised", sc.GetCurrentSyncPeer())
}

func TestSyncCoordinator_ColdStart_RealDefaultSettings_AdvertisedOnlyPeerIsNotCaughtUp(t *testing.T) {
	tSettings := settings.NewSettings()
	sc, reg := newTestSyncCoordinatorWithSettings(t, tSettings)
	sc.SetGetLocalHeightCallback(func() uint32 { return 0 })
	setSyncCoordinatorLocalTip(t, sc, 0, []byte{0x01})

	reg.Register(&blockchain.PeerInfo{
		ID:         "advertised",
		DataHubURL: "http://advertised",
		Height:     10_000,
		BlockHash:  syncCoordinatorTestHash(t),
		Storage:    "full",
	})

	require.Equal(t, 3, syncCoordinatorProbeBudget(sc))
	require.False(t, sc.isCaughtUp())
}

func TestSyncCoordinator_StartupLocalChainWorkUnavailable_UsesBoundedAdvertisedProbe(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)
	sc.SetGetLocalHeightCallback(func() uint32 { return 0 })
	client := setSyncCoordinatorLocalTipError(t, sc, errors.NewProcessingError("chainwork unavailable"))
	state := blockchain_api.FSMStateType_RUNNING
	client.On("GetFSMCurrentState", mock.Anything).Return(&state, nil)

	reg.Register(&blockchain.PeerInfo{
		ID:         "advertised",
		DataHubURL: "http://advertised",
		Height:     100,
		BlockHash:  syncCoordinatorTestHash(t),
		Storage:    "full",
	})

	sc.checkFSMState(context.Background())

	require.Equal(t, "advertised", sc.GetCurrentSyncPeer())
}

func TestSyncCoordinator_InflatedAdvertisedOnlyPeer_ConsumesProbeBudgetAndBacksOff(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)
	sc.SetGetLocalHeightCallback(func() uint32 { return 0 })
	sc.settings.P2P.MaxUnprovenSyncProbesPerBackoffWindow = 1
	setSyncCoordinatorProbeBudget(sc, 1)

	reg.Register(&blockchain.PeerInfo{
		ID:         "inflated",
		DataHubURL: "http://inflated",
		Height:     10_000,
		BlockHash:  syncCoordinatorTestHash(t),
		Storage:    "full",
	})

	require.NoError(t, sc.selectAndActivateNewPeer(0, ""))
	require.Equal(t, "inflated", sc.GetCurrentSyncPeer())
	require.Equal(t, 0, syncCoordinatorProbeBudget(sc))

	sc.ClearSyncPeer()
	require.NoError(t, sc.selectAndActivateNewPeer(0, ""))

	sc.mu.RLock()
	require.True(t, sc.allPeersAttempted)
	sc.mu.RUnlock()
}

func TestSyncCoordinator_ConcurrentActivation_ClaimsOnceAndConsumesOneProbe(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)
	sc.SetGetLocalHeightCallback(func() uint32 { return 0 })
	sc.settings.P2P.MaxUnprovenSyncProbesPerBackoffWindow = 2
	setSyncCoordinatorProbeBudget(sc, 2)
	producer := kafka.NewKafkaAsyncProducerMock()
	sc.blocksKafkaProducerClient = producer

	reg.Register(&blockchain.PeerInfo{
		ID:         "racy",
		DataHubURL: "http://racy",
		Height:     100,
		BlockHash:  syncCoordinatorTestHash(t),
		Storage:    "full",
	})

	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_ = sc.selectAndActivateNewPeer(0, "")
		}()
	}

	close(start)
	wg.Wait()

	require.Equal(t, "racy", sc.GetCurrentSyncPeer())
	require.Equal(t, 1, syncCoordinatorProbeBudget(sc))
	require.Len(t, producer.PublishChannel(), 1)
}

func TestSyncCoordinator_ProbeBudgetResetsAfterValidatedProgress(t *testing.T) {
	sc, _ := newTestSyncCoordinator(t)

	sc.mu.Lock()
	sc.lastLocalChainWork = []byte{0x02}
	sc.unprovenProbeBudgetRemaining = 0
	sc.mu.Unlock()

	setSyncCoordinatorLocalTip(t, sc, 100, []byte{0x03})

	sc.refreshProbeBudgetFromLocalTip(context.Background())
	require.Equal(t, sc.settings.P2P.MaxUnprovenSyncProbesPerBackoffWindow, syncCoordinatorProbeBudget(sc))
}

func TestSyncCoordinator_UnprovenProbeBudget_NotConsumedByEligibilityChecks(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)
	setSyncCoordinatorProbeBudget(sc, 2)
	reg.Register(&blockchain.PeerInfo{
		ID:         "advertised",
		DataHubURL: "http://advertised",
		Height:     100,
		BlockHash:  syncCoordinatorTestHash(t),
		Storage:    "full",
	})

	require.False(t, sc.isCaughtUp())
	require.Len(t, filterEligiblePeersForTest(sc, sc.listAllPeers(), "", 0), 1)
	sc.checkAllPeersAttempted()

	require.Equal(t, 2, syncCoordinatorProbeBudget(sc))
}

func TestSyncCoordinator_SlowInitialCatchup_DoesNotBecomeCaughtUpWhenProbeBudgetExhausted(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)
	setSyncCoordinatorProbeBudget(sc, 0)
	reg.Register(&blockchain.PeerInfo{
		ID:         "active",
		DataHubURL: "http://active",
		Height:     100,
		BlockHash:  syncCoordinatorTestHash(t),
	})

	sc.mu.Lock()
	sc.currentSyncPeer = "active"
	sc.syncStartTime = time.Now()
	sc.mu.Unlock()

	require.False(t, sc.isCaughtUp())
}

func TestSyncCoordinator_IsCaughtUp_UsesValidatedChainWork(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)
	setSyncCoordinatorLocalTip(t, sc, 100, []byte{0x02})

	reg.Register(&blockchain.PeerInfo{
		ID:                 "validated",
		DataHubURL:         "http://validated",
		Height:             0,
		ValidatedBlockHash: syncCoordinatorTestHash(t),
		ValidatedChainWork: []byte{0x03},
	})

	require.False(t, sc.isCaughtUp())
}

// P1-b: an advertised-ahead peer keeps us NOT caught up regardless of the
// unproven-probe budget (fallback path, local tip unavailable). Treating an
// exhausted budget as caught-up wedges monitorFSM at slowMonitorInterval and
// makes the budget refill unreachable. Probe *activation* stays budget-gated
// elsewhere (filterEligiblePeersWithTip / claimSelectedSyncPeer).
func TestSyncCoordinator_IsCaughtUp_AdvertisedAheadPeerNotCaughtUpRegardlessOfBudget(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)
	reg.Register(&blockchain.PeerInfo{
		ID:         "advertised",
		DataHubURL: "http://advertised",
		Height:     100,
		BlockHash:  syncCoordinatorTestHash(t),
	})

	setSyncCoordinatorProbeBudget(sc, 1)
	require.False(t, sc.isCaughtUp())

	setSyncCoordinatorProbeBudget(sc, 0)
	require.False(t, sc.isCaughtUp())
}

func TestSyncCoordinator_FilterEligiblePeers_UsesValidatedWorkBeforeAdvertisedProbe(t *testing.T) {
	sc, _ := newTestSyncCoordinator(t)
	setSyncCoordinatorLocalTip(t, sc, 100, []byte{0x02})
	validatedHash := syncCoordinatorTestHash(t)

	peers := []*blockchain.PeerInfo{
		{
			ID:                 "validated",
			DataHubURL:         "http://validated",
			Height:             1,
			ReputationScore:    80,
			ValidatedBlockHash: validatedHash,
			ValidatedChainWork: []byte{0x03},
		},
		{
			ID:              "advertised",
			DataHubURL:      "http://advertised",
			Height:          150,
			BlockHash:       syncCoordinatorTestHash(t),
			ReputationScore: 80,
		},
	}

	got := filterEligiblePeersForTest(sc, peers, "", 100)

	require.Len(t, got, 1)
	require.Equal(t, "validated", got[0].ID)
	require.Greater(t, got[0].Height, uint32(100))
}

func TestSyncCoordinator_CheckAllPeersAttempted_UsesValidatedWorkAndProbeEligibility(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)
	setSyncCoordinatorLocalTip(t, sc, 100, []byte{0x02})
	reg.Register(&blockchain.PeerInfo{
		ID:                 "validated",
		DataHubURL:         "http://validated",
		Height:             0,
		ValidatedBlockHash: syncCoordinatorTestHash(t),
		ValidatedChainWork: []byte{0x03},
		LastSyncAttempt:    time.Now(),
	})

	sc.checkAllPeersAttempted()

	sc.mu.RLock()
	require.True(t, sc.allPeersAttempted)
	sc.mu.RUnlock()
}

func TestSyncCoordinator_EvaluateSyncPeer_ValidatedWorkCaughtUp(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)
	setSyncCoordinatorLocalTip(t, sc, 100, []byte{0x04})

	reg.Register(&blockchain.PeerInfo{
		ID:                 "current",
		DataHubURL:         "http://current",
		Height:             1_000,
		BlockHash:          syncCoordinatorTestHash(t),
		ValidatedBlockHash: syncCoordinatorTestHash(t),
		ValidatedChainWork: []byte{0x03},
		LastMessageTime:    time.Now(),
	})
	reg.Register(&blockchain.PeerInfo{
		ID:                 "better",
		DataHubURL:         "http://better",
		Height:             1_001,
		BlockHash:          syncCoordinatorTestHash(t),
		ValidatedBlockHash: syncCoordinatorTestHash(t),
		ValidatedChainWork: []byte{0x05},
		LastMessageTime:    time.Now(),
	})

	sc.mu.Lock()
	sc.currentSyncPeer = "current"
	sc.syncStartTime = time.Now()
	sc.mu.Unlock()

	sc.evaluateSyncPeer()

	require.Equal(t, "better", sc.GetCurrentSyncPeer())
}

func TestSyncCoordinator_HandleFSMTransition_DoesNotUseAdvertisedHeightAsFailureProof(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)
	setSyncCoordinatorLocalTip(t, sc, 100, []byte{0x02})
	reg.Register(&blockchain.PeerInfo{
		ID:         "advertised",
		DataHubURL: "http://advertised",
		Height:     1_000,
		BlockHash:  syncCoordinatorTestHash(t),
	})

	sc.mu.Lock()
	sc.currentSyncPeer = "advertised"
	sc.syncStartTime = time.Now()
	sc.mu.Unlock()

	state := blockchain_api.FSMStateType_RUNNING
	require.False(t, sc.handleFSMTransition(&state))
	require.Equal(t, "advertised", sc.GetCurrentSyncPeer())
}

func TestSyncCoordinator_HandleFSMTransition_ChattyNoProgressPeerTimesOutAndSelectsBetterPeer(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)
	setSyncCoordinatorLocalTip(t, sc, 100, []byte{0x02})

	reg.Register(&blockchain.PeerInfo{
		ID:         "current",
		DataHubURL: "http://current",
		Height:     200,
		BlockHash:  syncCoordinatorTestHash(t),
		Storage:    "full",
	})
	reg.Register(&blockchain.PeerInfo{
		ID:                 "better",
		DataHubURL:         "http://better",
		Height:             201,
		BlockHash:          syncCoordinatorTestHash(t),
		Storage:            "full",
		ValidatedBlockHash: syncCoordinatorTestHash(t),
		ValidatedChainWork: []byte{0x03},
	})
	reg.UpdateLastMessageTime("current")

	stalledAt := time.Now().Add(-defaultSyncPeerNoProgressLimit - time.Second)
	sc.mu.Lock()
	sc.currentSyncPeer = "current"
	sc.syncStartTime = stalledAt
	sc.lastSyncProgressTime = stalledAt
	sc.lastLocalChainWork = []byte{0x02}
	sc.mu.Unlock()

	state := blockchain_api.FSMStateType_RUNNING
	require.True(t, sc.handleFSMTransition(&state))

	require.Equal(t, "better", sc.GetCurrentSyncPeer())
	current, ok := reg.Get("current")
	require.True(t, ok)
	require.False(t, current.LastSyncAttempt.IsZero())
	require.Equal(t, 50.0, current.ReputationScore)
	require.WithinDuration(t, time.Now(), current.LastMessageTime, time.Minute)
}

// newPreemptionTestCoordinator wires an incumbent sync peer ("current") that is
// still ahead of local by validated work and a candidate ("better"), with the
// incumbent's last validated progress placed progressAge in the past. It reuses
// the standard harness; noProgressTimeout=0 exercises the 5m fallback.
func newPreemptionTestCoordinator(t *testing.T, noProgressTimeout time.Duration, incumbentWork, candidateWork []byte, progressAge time.Duration) (*SyncCoordinator, *blockchain.CentralizedPeerRegistry) {
	t.Helper()

	tSettings := &settings.Settings{
		P2P: settings.P2PSettings{
			AllowPrunedNodeFallback:                   true,
			MaxUnvalidatedAdvertisedHeightLead:        10_000,
			MaxUnprovenSyncProbesPerBackoffWindow:     3,
			FullDeliveryFreshnessWindow:               24 * time.Hour,
			SyncCoordinatorPeriodicEvaluationInterval: 30 * time.Second,
			SyncPeerNoProgressTimeout:                 noProgressTimeout,
		},
	}
	sc, reg := newTestSyncCoordinatorWithSettings(t, tSettings)
	setSyncCoordinatorLocalTip(t, sc, 100, []byte{0x02})

	reg.Register(&blockchain.PeerInfo{
		ID:                 "current",
		DataHubURL:         "http://current",
		Height:             200,
		BlockHash:          syncCoordinatorTestHash(t),
		Storage:            "full",
		ValidatedBlockHash: syncCoordinatorTestHash(t),
		ValidatedChainWork: incumbentWork,
	})
	reg.Register(&blockchain.PeerInfo{
		ID:                 "better",
		DataHubURL:         "http://better",
		Height:             201,
		BlockHash:          syncCoordinatorTestHash(t),
		Storage:            "full",
		ValidatedBlockHash: syncCoordinatorTestHash(t),
		ValidatedChainWork: candidateWork,
	})

	progressAt := time.Now().Add(-progressAge)
	sc.mu.Lock()
	sc.currentSyncPeer = "current"
	sc.syncStartTime = progressAt
	sc.lastSyncProgressTime = progressAt
	sc.lastSyncPeerBlocksReceived = 0
	sc.lastLocalChainWork = []byte{0x02}
	sc.mu.Unlock()

	return sc, reg
}

// P2-1(a): a materially-higher-work candidate preempts an incumbent that is still
// ahead of local by validated work once it has stalled past the guard window.
func TestSyncCoordinator_EvaluateSyncPeer_PreemptsForHigherWorkPeer(t *testing.T) {
	// Default 5m timeout → 2.5m guard; progressAge 3m is past the guard but short
	// of the hard no-progress eviction, and the candidate outranks the incumbent.
	sc, _ := newPreemptionTestCoordinator(t, 0, []byte{0x03}, []byte{0x05}, 3*time.Minute)

	sc.evaluateSyncPeer()

	require.Equal(t, "better", sc.GetCurrentSyncPeer(), "higher-work candidate must preempt a stalled incumbent")
}

// P2-1(b): strict comparison — a candidate whose validated work is not strictly
// greater than the incumbent's (equal OR lower) must never preempt, even when the
// incumbent has stalled well past the guard window.
func TestSyncCoordinator_EvaluateSyncPeer_DoesNotPreemptForNonHigherWorkPeer(t *testing.T) {
	cases := []struct {
		name          string
		candidateWork []byte
	}{
		{"equal work", []byte{0x03}},
		{"strictly lower work", []byte{0x01}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Incumbent work 0x03; stalled 3m (past the 2.5m guard at the 5m default).
			sc, _ := newPreemptionTestCoordinator(t, 0, []byte{0x03}, tc.candidateWork, 3*time.Minute)

			sc.evaluateSyncPeer()

			require.Equal(t, "current", sc.GetCurrentSyncPeer(), "non-higher-work candidate must not preempt")
		})
	}
}

// P2-1(c): a higher-work candidate exists, but the incumbent made validated
// progress within the guard window, so it must not be preempted mid-delivery.
func TestSyncCoordinator_EvaluateSyncPeer_DoesNotPreemptRecentlyProgressingPeer(t *testing.T) {
	// Default 5m timeout → 2.5m guard; progressAge 1m is below the guard.
	sc, _ := newPreemptionTestCoordinator(t, 0, []byte{0x03}, []byte{0x05}, 1*time.Minute)

	sc.evaluateSyncPeer()

	require.Equal(t, "current", sc.GetCurrentSyncPeer(), "recently-progressing peer must not be preempted")
}

// P2-1(d): the preemption guard tracks the configurable no-progress timeout, not
// the periodic-evaluation interval. The SAME progressAge preempts under a small
// timeout but not under a large one.
func TestSyncCoordinator_EvaluateSyncPeer_PreemptionTimingScalesWithConfig(t *testing.T) {
	const progressAge = 90 * time.Second

	// Small timeout (2m → 1m guard): 90s is past the guard → preempt.
	scSmall, _ := newPreemptionTestCoordinator(t, 2*time.Minute, []byte{0x03}, []byte{0x05}, progressAge)
	scSmall.evaluateSyncPeer()
	require.Equal(t, "better", scSmall.GetCurrentSyncPeer(), "small timeout: same progressAge preempts")

	// Large timeout (30m → 15m guard): the SAME 90s is below the guard → no preempt.
	scLarge, _ := newPreemptionTestCoordinator(t, 30*time.Minute, []byte{0x03}, []byte{0x05}, progressAge)
	scLarge.evaluateSyncPeer()
	require.Equal(t, "current", scLarge.GetCurrentSyncPeer(), "large timeout: same progressAge does not preempt")
}

func TestSyncCoordinator_EvaluateSyncPeer_BlockDeliveryKeepsNoProgressDeadlineFresh(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)
	setSyncCoordinatorLocalTip(t, sc, 100, []byte{0x02})

	reg.Register(&blockchain.PeerInfo{
		ID:         "current",
		DataHubURL: "http://current",
		Height:     200,
		BlockHash:  syncCoordinatorTestHash(t),
		Storage:    "full",
	})
	// A fully-validated block delivered by the sync peer bumps BlocksReceived; this
	// is the only signal that should refresh the no-progress deadline.
	reg.RecordBlockReceived("current", 0)

	stalledAt := time.Now().Add(-defaultSyncPeerNoProgressLimit - time.Second)
	sc.mu.Lock()
	sc.currentSyncPeer = "current"
	sc.syncStartTime = stalledAt
	sc.lastSyncProgressTime = stalledAt
	sc.lastSyncPeerBlocksReceived = 0
	sc.lastLocalChainWork = []byte{0x02}
	sc.mu.Unlock()

	sc.evaluateSyncPeer()

	require.Equal(t, "current", sc.GetCurrentSyncPeer())
	_, progressAge, timedOut := sc.syncPeerNoProgressTimedOut(time.Now())
	require.False(t, timedOut)
	require.Less(t, progressAge, defaultSyncPeerNoProgressLimit)
}

// P2-a: validated header work is credited before any block body is delivered, so
// it must not refresh the no-progress stall deadline. Only peer-attributable block
// delivery (BlocksReceived) does.
func TestSyncCoordinator_RecordSyncPeerBlockProgress_HeaderCreditDoesNotRefreshDeadline(t *testing.T) {
	sc, _ := newTestSyncCoordinator(t)

	stalledAt := time.Now().Add(-defaultSyncPeerNoProgressLimit - time.Second)
	sc.mu.Lock()
	sc.currentSyncPeer = "current"
	sc.syncStartTime = stalledAt
	sc.lastSyncProgressTime = stalledAt
	sc.lastSyncPeerBlocksReceived = 0
	sc.mu.Unlock()

	// No new block delivered (BlocksReceived unchanged) — the deadline stays stale.
	sc.recordSyncPeerBlockProgress("current", 0, time.Now())
	stalledPeer, progressAge, timedOut := sc.syncPeerNoProgressTimedOut(time.Now())
	require.True(t, timedOut, "header credit alone must not keep the deadline fresh")
	require.Equal(t, "current", stalledPeer)
	require.Greater(t, progressAge, defaultSyncPeerNoProgressLimit)

	// A delivered block DOES refresh it.
	sc.recordSyncPeerBlockProgress("current", 1, time.Now())
	_, _, timedOut = sc.syncPeerNoProgressTimedOut(time.Now())
	require.False(t, timedOut)
}

// P1-a: local best-tip chainwork advances from ordinary block gossip delivered by
// any peer. Such an advance must refill the probe budget but must NOT refresh a
// stalled sync peer's no-progress deadline, otherwise the peer pins the slot for
// as long as the network produces blocks.
func TestSyncCoordinator_LocalTipAdvanceDoesNotRefreshStallDeadline(t *testing.T) {
	sc, _ := newTestSyncCoordinator(t)
	setSyncCoordinatorProbeBudget(sc, 0)

	stalledAt := time.Now().Add(-defaultSyncPeerNoProgressLimit - time.Second)
	sc.mu.Lock()
	sc.currentSyncPeer = "stalled"
	sc.syncStartTime = stalledAt
	sc.lastSyncProgressTime = stalledAt
	sc.lastSyncPeerBlocksReceived = 0
	sc.lastLocalChainWork = []byte{0x02}
	sc.mu.Unlock()

	sc.resetProbeBudgetIfLocalChainWorkAdvanced([]byte{0x05})

	require.Equal(t, maxUnprovenProbeBudget(sc.settings), syncCoordinatorProbeBudget(sc),
		"local-tip advance should refill the probe budget")

	stalledPeer, progressAge, timedOut := sc.syncPeerNoProgressTimedOut(time.Now())
	require.True(t, timedOut, "stalled sync peer must still time out despite local-tip advance")
	require.Equal(t, "stalled", stalledPeer)
	require.Greater(t, progressAge, defaultSyncPeerNoProgressLimit)
}

// P1-b: when an advertised-ahead peer exists but the unproven-probe budget is
// spent and no peer is pinned, isCaughtUp must still report NOT caught up so
// monitorFSM keeps ticking fast and the budget refill stays reachable.
func TestSyncCoordinator_IsCaughtUp_AdvertisedAheadPeerNotCaughtUpWhenBudgetSpent(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)
	setSyncCoordinatorLocalTip(t, sc, 100, []byte{0x02})

	reg.Register(&blockchain.PeerInfo{
		ID:         "advertised",
		DataHubURL: "http://advertised",
		Height:     200,
		BlockHash:  syncCoordinatorTestHash(t),
	})

	setSyncCoordinatorProbeBudget(sc, 0)

	require.Empty(t, sc.GetCurrentSyncPeer())
	require.False(t, sc.isCaughtUp(),
		"an advertised-ahead peer means not caught up even with the probe budget spent")
}

// P2-b: a peer inside an active block-incomplete / full-storage penalty window
// loses its top-tier "ahead by validated work" eligibility, and regains it once
// the window expires.
func TestSyncCoordinator_PeerAheadByValidatedWork_PenaltyWindowSuppressesEligibility(t *testing.T) {
	local := []byte{0x02}
	ahead := &blockchain.PeerInfo{
		ValidatedBlockHash: syncCoordinatorTestHash(t),
		ValidatedChainWork: []byte{0x05},
	}
	require.True(t, peerAheadByValidatedWork(ahead, local))

	ahead.FullStoragePenaltyUntil = time.Now().Add(time.Hour)
	require.False(t, peerAheadByValidatedWork(ahead, local),
		"active penalty window must withhold validated-work eligibility")

	ahead.FullStoragePenaltyUntil = time.Now().Add(-time.Hour)
	require.True(t, peerAheadByValidatedWork(ahead, local),
		"expired penalty window must restore validated-work eligibility")
}

func TestSyncCoordinator_MaxUnvalidatedAdvertisedHeightLead_AllowsProbeAtTenThousand(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)
	sc.SetGetLocalHeightCallback(func() uint32 { return 100 })

	reg.Register(&blockchain.PeerInfo{
		ID:         "bounded",
		DataHubURL: "http://bounded",
		Height:     10_100,
		BlockHash:  syncCoordinatorTestHash(t),
	})

	require.False(t, sc.isCaughtUp())
	require.Equal(t, "bounded", sc.selectNewSyncPeer())
}

func TestSyncCoordinator_SendSyncTriggerToKafka_NilProducerNoOp(t *testing.T) {
	sc, _ := newTestSyncCoordinator(t)
	require.NotPanics(t, func() { sc.sendSyncTriggerToKafka("p", "abc") })
}

func TestSyncCoordinator_SendSyncTriggerToKafka_EmptyHashNoOp(t *testing.T) {
	sc, _ := newTestSyncCoordinator(t)
	require.NotPanics(t, func() { sc.sendSyncTriggerToKafka("p", "") })
}

func TestSyncCoordinator_StartStop_ExitsCleanly(t *testing.T) {
	sc, _ := newTestSyncCoordinator(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sc.Start(ctx)

	// Allow the goroutines to spin up briefly so they reach their select.
	time.Sleep(20 * time.Millisecond)

	doneCh := make(chan struct{})
	go func() {
		sc.Stop()
		close(doneCh)
	}()

	select {
	case <-doneCh:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return — coordinator goroutines leaked")
	}
}

func TestSyncCoordinator_CheckAndClearExpiredBackoff_NotInBackoff(t *testing.T) {
	sc, _ := newTestSyncCoordinator(t)
	require.False(t, sc.checkAndClearExpiredBackoff())
}

func TestSyncCoordinator_CheckAndClearExpiredBackoff_StillInWindow(t *testing.T) {
	sc, _ := newTestSyncCoordinator(t)
	sc.enterBackoffMode()
	require.True(t, sc.checkAndClearExpiredBackoff(),
		"freshly entered backoff must still be in its window")
}

// A stalled, still-ahead PROVEN incumbent must be preempted by an unproven candidate with
// strictly higher validated work. This is the profile the shipped preemption tests miss (they
// all use an unproven incumbent): before the atomic-swap fix, clear-then-reselect re-pinned the
// proven incumbent via the proven-first sort and reset its progress clock, defeating eviction.
func TestSyncCoordinator_EvaluateSyncPeer_PreemptsProvenIncumbentForHigherWorkCandidate(t *testing.T) {
	tSettings := &settings.Settings{
		P2P: settings.P2PSettings{
			AllowPrunedNodeFallback:                   true,
			MaxUnvalidatedAdvertisedHeightLead:        10_000,
			MaxUnprovenSyncProbesPerBackoffWindow:     3,
			FullDeliveryFreshnessWindow:               24 * time.Hour,
			SyncCoordinatorPeriodicEvaluationInterval: 30 * time.Second,
			SyncPeerNoProgressTimeout:                 0, // 5m default → 2.5m guard
		},
	}
	sc, reg := newTestSyncCoordinatorWithSettings(t, tSettings)
	setSyncCoordinatorLocalTip(t, sc, 100, []byte{0x02})

	// Proven incumbent: recorded full-block delivery inside the freshness window.
	reg.Register(&blockchain.PeerInfo{
		ID:                 "current",
		DataHubURL:         "http://current",
		Height:             200,
		Storage:            "full",
		BlockHash:          syncCoordinatorTestHash(t),
		ValidatedBlockHash: syncCoordinatorTestHash(t),
		ValidatedChainWork: []byte{0x03},
		BlocksReceived:     5,
		LastBlockTime:      time.Now().Add(-time.Minute),
	})
	// Unproven candidate with strictly higher validated work.
	reg.Register(&blockchain.PeerInfo{
		ID:                 "better",
		DataHubURL:         "http://better",
		Height:             201,
		Storage:            "full",
		BlockHash:          syncCoordinatorTestHash(t),
		ValidatedBlockHash: syncCoordinatorTestHash(t),
		ValidatedChainWork: []byte{0x05},
	})

	progressAt := time.Now().Add(-3 * time.Minute) // past 2.5m guard, before 5m hard eviction
	sc.mu.Lock()
	sc.currentSyncPeer = "current"
	sc.syncStartTime = progressAt
	sc.lastSyncProgressTime = progressAt
	sc.lastSyncPeerBlocksReceived = 5
	sc.lastLocalChainWork = []byte{0x02}
	sc.mu.Unlock()

	sc.evaluateSyncPeer()

	require.Equal(t, "better", sc.GetCurrentSyncPeer(),
		"unproven higher-work candidate must preempt a stalled proven incumbent")

	// The benched incumbent must be placed on the sync-attempt cooldown so it is not
	// immediately reselected if the candidate later clears.
	incumbent, ok := reg.Get("current")
	require.True(t, ok)
	require.False(t, incumbent.LastSyncAttempt.IsZero(),
		"benched incumbent must have a recorded sync attempt (cooldown)")
	require.WithinDuration(t, time.Now(), incumbent.LastSyncAttempt, 5*time.Second,
		"benched incumbent's sync-attempt timestamp must be fresh")
}

// A peer that is ahead by locally-validated work must be activated through the full
// TriggerSync path even when the unproven-probe budget is exhausted: the filter stage admits
// validated-ahead peers unconditionally, so the claim stage must not re-gate them.
func TestSyncCoordinator_TriggerSync_ActivatesValidatedAheadPeerDespiteExhaustedBudget(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)
	setSyncCoordinatorLocalTip(t, sc, 100, []byte{0x02})

	reg.Register(&blockchain.PeerInfo{
		ID:                 "ahead",
		DataHubURL:         "http://ahead",
		Height:             200,
		Storage:            "full",
		BlockHash:          syncCoordinatorTestHash(t),
		ValidatedBlockHash: syncCoordinatorTestHash(t),
		ValidatedChainWork: []byte{0x05}, // strictly greater than local 0x02
	})

	sc.mu.Lock()
	sc.unprovenProbeBudgetRemaining = 0 // exhausted
	sc.lastLocalChainWork = []byte{0x02}
	sc.mu.Unlock()

	require.NoError(t, sc.TriggerSync())
	require.Equal(t, "ahead", sc.GetCurrentSyncPeer(),
		"validated-ahead peer must activate despite an exhausted unproven-probe budget")
}

// A forced peer (operator override) must be activated through TriggerSync even when it is
// unproven and the probe budget is exhausted; the "bypasses all safety checks" contract must
// hold at the claim stage too.
func TestSyncCoordinator_TriggerSync_ActivatesForcedPeerDespiteExhaustedBudget(t *testing.T) {
	tSettings := &settings.Settings{
		P2P: settings.P2PSettings{
			AllowPrunedNodeFallback:                   true,
			MaxUnvalidatedAdvertisedHeightLead:        10_000,
			MaxUnprovenSyncProbesPerBackoffWindow:     3,
			FullDeliveryFreshnessWindow:               24 * time.Hour,
			SyncCoordinatorPeriodicEvaluationInterval: 30 * time.Second,
			ForceSyncPeer:                             "forced",
		},
	}
	sc, reg := newTestSyncCoordinatorWithSettings(t, tSettings)

	// Unproven peer: no validated work, no recorded full-block delivery.
	reg.Register(&blockchain.PeerInfo{
		ID:         "forced",
		DataHubURL: "http://forced",
		Height:     200,
		BlockHash:  syncCoordinatorTestHash(t),
	})

	sc.mu.Lock()
	sc.unprovenProbeBudgetRemaining = 0 // exhausted
	sc.mu.Unlock()

	require.NoError(t, sc.TriggerSync())
	require.Equal(t, "forced", sc.GetCurrentSyncPeer(),
		"forced peer must activate despite an exhausted unproven-probe budget")
}

func TestSyncCoordinator_MaxUnprovenProbeBudget_Clamp(t *testing.T) {
	cases := []struct {
		name       string
		configured int
		want       int
	}{
		{"negative clamps to zero", -5, 0},
		{"zero stays zero", 0, 0},
		{"positive is passed through", 3, 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &settings.Settings{P2P: settings.P2PSettings{MaxUnprovenSyncProbesPerBackoffWindow: tc.configured}}
			require.Equal(t, tc.want, maxUnprovenProbeBudget(s))
		})
	}

	require.Equal(t, 0, maxUnprovenProbeBudget(nil), "nil settings must yield a zero budget")
}
