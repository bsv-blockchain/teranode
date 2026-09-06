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
	sc.SetGetLocalHeightCallback(func(context.Context) uint32 { return 0 })
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
	tipHeight, localChainWork, localWorkOK := sc.getLocalTipWorkSafe()
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

// TestSyncCoordinator_IsCaughtUp_BlacklistedPeerIsCaughtUp: the caught-up
// determination must agree with selection about the operator blacklist. A
// blacklisted-but-ahead peer can never be selected by SelectSyncPeer, so if it
// still counted as a viable sync candidate here the node would report
// not-caught-up forever while every selection attempt fails.
func TestSyncCoordinator_IsCaughtUp_BlacklistedPeerIsCaughtUp(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)

	reg.Register(&blockchain.PeerInfo{
		ID:         "ahead",
		DataHubURL: "http://evil.example",
		Height:     100,
		BlockHash:  syncCoordinatorTestHash(t),
	})
	// Boost reputation past 20 so the peer is viable apart from the blacklist.
	for i := 0; i < 5; i++ {
		reg.UpdateMetrics("ahead", 0, 0, 0, true, false, false, 100)
	}

	// Control: the ahead peer keeps us not caught up while its host is allowed.
	require.False(t, sc.isCaughtUp(), "precondition: ahead non-blacklisted peer means not caught up")

	// Operator blacklists the host after the URL was stored in the registry.
	sc.settings.SubtreeValidation.BlacklistedBaseURLs = map[string]struct{}{"http://evil.example": {}}

	require.True(t, sc.isCaughtUp(), "blacklisted peer must not keep the node in a not-caught-up state")
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

// Exercises concurrent backoff writers against considerReputationRecovery's read of
// backoffMultiplier; fails under -race if the read is not synchronized.
func TestSyncCoordinator_ConsiderReputationRecovery_ConcurrentWithBackoffWriters(t *testing.T) {
	sc, _ := newTestSyncCoordinator(t)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			sc.enterBackoffMode()
			sc.mu.Lock()
			sc.lastAllPeersAttemptTime = time.Now().Add(-time.Hour) // force backoff expiry
			sc.mu.Unlock()
			sc.checkAndClearExpiredBackoff() // doubles backoffMultiplier
			sc.enterBackoffMode()
			sc.resetBackoff()
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			sc.considerReputationRecovery()
		}
	}()

	wg.Wait()
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
	sc.SetGetLocalHeightCallback(func(context.Context) uint32 { return 50 })

	reg.Register(&blockchain.PeerInfo{ID: "pruned", DataHubURL: "http://p", Height: 100, BlockHash: syncCoordinatorTestHash(t), Storage: "pruned"})
	reg.Register(&blockchain.PeerInfo{ID: "full", DataHubURL: "http://f", Height: 100, BlockHash: syncCoordinatorTestHash(t), Storage: "full"})
	for _, id := range []string{"pruned", "full"} {
		for i := 0; i < 5; i++ {
			reg.UpdateMetrics(id, 0, 0, 0, true, false, false, 100)
		}
	}

	require.Equal(t, "full", sc.selectNewSyncPeer())
}

func TestSyncCoordinator_SelectNewSyncPeer_MeritTiedSybilCannotCapture(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)
	setSyncCoordinatorLocalTip(t, sc, 10, []byte{0x02})

	// One attacker ID that sorts lexicographically first among four peers tied
	// on every merit criterion. Through the real coordinator selection path it
	// must not win every round; the removed peer-ID tiebreak gave it 100%
	// capture. P(some tied peer never wins in 100 rounds) <= 4 * 0.75^100,
	// so this cannot flake.
	ids := []string{"000000-attacker", "honest-a", "honest-b", "honest-c"}
	for _, id := range ids {
		reg.Register(&blockchain.PeerInfo{
			ID:                 id,
			DataHubURL:         "http://" + id,
			Height:             100,
			BlockHash:          syncCoordinatorTestHash(t),
			Storage:            "full",
			ValidatedHeight:    100,
			ValidatedBlockHash: syncCoordinatorTestHash(t),
			ValidatedChainWork: []byte{0x03},
		})
		for i := 0; i < 5; i++ {
			reg.UpdateMetrics(id, 0, 0, 0, true, false, false, 100)
		}
	}

	wins := map[string]int{}
	for range 100 {
		got := sc.selectNewSyncPeer()
		require.Contains(t, ids, got)
		wins[got]++
	}
	for _, id := range ids {
		require.Positive(t, wins[id], "every merit-tied peer must win at least once, got %v", wins)
	}
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
	sc.SetGetLocalHeightCallback(func(context.Context) uint32 { return 0 })
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
	sc.SetGetLocalHeightCallback(func(context.Context) uint32 { return 0 })
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
	sc.SetGetLocalHeightCallback(func(context.Context) uint32 { return 0 })
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

	sc.checkFSMState()

	require.Equal(t, "advertised", sc.GetCurrentSyncPeer())
}

func TestSyncCoordinator_InflatedAdvertisedOnlyPeer_ConsumesProbeBudgetAndBacksOff(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)
	sc.SetGetLocalHeightCallback(func(context.Context) uint32 { return 0 })
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
	sc.SetGetLocalHeightCallback(func(context.Context) uint32 { return 0 })
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

	sc.refreshProbeBudgetFromLocalTip()
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
	sc.SetGetLocalHeightCallback(func(context.Context) uint32 { return 100 })

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
		sc.Stop(context.Background())
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

func newTestSyncCoordinatorWithFSM(t *testing.T, state blockchain_api.FSMStateType) (*SyncCoordinator, *blockchain.CentralizedPeerRegistry, *blockchain.Mock) {
	t.Helper()
	reg := blockchain.NewCentralizedPeerRegistry(blockchain.DefaultBanConfig())
	client := blockchain.NewLocalPeerRegistryClient(reg)
	tSettings := &settings.Settings{P2P: settings.P2PSettings{
		AllowPrunedNodeFallback:                   true,
		SyncCoordinatorPeriodicEvaluationInterval: 30 * time.Second,
		HealthCheckEnabled:                        false, // test DataHubURL is unreachable; skip HTTP health check
		// Production defaults for the unproven-peer probe hardening (upstream
		// #1201); a zero probe budget would make unproven peers unselectable.
		MaxUnvalidatedAdvertisedHeightLead:    10_000,
		MaxUnprovenSyncProbesPerBackoffWindow: 3,
		FullDeliveryFreshnessWindow:           24 * time.Hour,
	}}
	bcMock := &blockchain.Mock{}
	st := state
	bcMock.On("GetFSMCurrentState", mock.Anything).Return(&st, nil)
	// checkFSMState refreshes the unproven-probe budget from the local tip
	// (upstream #1201), which reads the best block header from the client.
	bcMock.On("GetBestBlockHeader", mock.Anything).Return(
		&model.BlockHeader{},
		&model.BlockHeaderMeta{Height: 0, ChainWork: []byte{0x01}},
		nil,
	)

	sc := NewSyncCoordinator(context.Background(), ulogger.TestLogger{}, tSettings, client,
		NewPeerSelector(ulogger.TestLogger{}, tSettings), bcMock, nil)
	sc.SetGetLocalHeightCallback(func(context.Context) uint32 { return 0 })
	return sc, reg, bcMock
}

func TestSyncCoordinator_ProactiveInCatchingBlocks(t *testing.T) {
	sc, reg, _ := newTestSyncCoordinatorWithFSM(t, blockchain_api.FSMStateType_CATCHINGBLOCKS)

	// Register a viable peer well ahead of local height 0 (mirror the idiom
	// from TestSyncCoordinator_IsCaughtUp_AheadPeerMakesUsBehind):
	reg.Register(&blockchain.PeerInfo{ID: "ahead", DataHubURL: "http://ahead", Height: 100,
		BlockHash: syncCoordinatorTestHash(t)})
	for i := 0; i < 5; i++ {
		reg.UpdateMetrics("ahead", 0, 0, 0, true, false, false, 100)
	}

	sc.checkFSMState()

	require.Equal(t, "ahead", sc.GetCurrentSyncPeer(),
		"coordinator should proactively select a sync peer while in CATCHINGBLOCKS")
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

// HandleCatchupFailure must not clear the sync peer while another sync decision is
// in flight: pre-serialisation, its unconditional clear could evict a peer that a
// concurrent decision path was still working with, producing duplicate/conflicting
// activations. The blocked GetBestBlockHeader holds evaluateSyncPeer mid-decision
// (under decisionMu); HandleCatchupFailure must wait for it rather than clearing.
func TestSyncCoordinator_HandleCatchupFailure_WaitsForInFlightDecision(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)

	reg.Register(&blockchain.PeerInfo{
		ID:                 "peer-a",
		DataHubURL:         "http://peer-a",
		Height:             200,
		ReputationScore:    50,
		BlockHash:          syncCoordinatorTestHash(t),
		ValidatedBlockHash: syncCoordinatorTestHash(t),
		ValidatedChainWork: []byte{0x05},
	})

	sc.mu.Lock()
	sc.currentSyncPeer = "peer-a"
	sc.mu.Unlock()

	gate := make(chan struct{})
	entered := make(chan struct{}, 16)
	client := &blockchain.Mock{}
	client.On("GetBestBlockHeader", mock.Anything).Run(func(mock.Arguments) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-gate
	}).Return(&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 100, ChainWork: []byte{0x02}}, nil)
	sc.blockchainClient = client

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		sc.evaluateSyncPeer() // blocks in GetBestBlockHeader while holding decisionMu
	}()

	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("evaluateSyncPeer never reached GetBestBlockHeader")
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		sc.HandleCatchupFailure("test failure")
	}()

	// Give HandleCatchupFailure ample time to (wrongly) run its clear if it is not
	// serialised behind the in-flight decision.
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, "peer-a", sc.GetCurrentSyncPeer(),
		"HandleCatchupFailure must not clear the sync peer while another decision is in flight")

	close(gate)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("sync decisions deadlocked")
	}
}

// Hammers every decision entry point concurrently under -race to guard the
// decisionMu lock ordering: a regression that acquires decisionMu while holding
// mu (or re-enters it) hangs this test.
func TestSyncCoordinator_ConcurrentSyncDecisions_NoDeadlock(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)
	setSyncCoordinatorLocalTip(t, sc, 100, []byte{0x02})

	pid := mustNewPeerID(t)
	for _, id := range []string{"peer-a", "peer-b", pid.String()} {
		reg.Register(&blockchain.PeerInfo{
			ID:                 id,
			DataHubURL:         "http://" + id,
			Height:             200,
			ReputationScore:    50,
			BlockHash:          syncCoordinatorTestHash(t),
			ValidatedBlockHash: syncCoordinatorTestHash(t),
			ValidatedChainWork: []byte{0x05},
		})
	}

	var wg sync.WaitGroup
	for w := 0; w < 6; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				_ = sc.TriggerSync()
				sc.evaluateSyncPeer()
				sc.HandleCatchupFailure("stress")
				sc.UpdateBanStatus(pid)
				sc.ClearSyncPeer()
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("concurrent sync decisions deadlocked")
	}

	current := sc.GetCurrentSyncPeer()
	if current != "" {
		_, found := reg.Get(current)
		require.True(t, found, "current sync peer %s must be a registered peer", current)
	}
}

// TestSyncCoordinator_SendSyncTriggerToKafka_RefusesBlacklistedURL: the sync
// trigger reads the peer's DataHub URL straight from the registry, bypassing
// selection eligibility. A URL stored before its host was blacklisted (or
// belonging to a forced sync peer) must not be handed to block validation -
// the trigger is dropped instead.
func TestSyncCoordinator_SendSyncTriggerToKafka_RefusesBlacklistedURL(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)

	producer := kafka.NewKafkaAsyncProducerMock()
	sc.blocksKafkaProducerClient = producer

	reg.Register(&blockchain.PeerInfo{
		ID:         "peer",
		DataHubURL: "http://evil.example",
		BlockHash:  syncCoordinatorTestHash(t),
	})

	// Control: without a blacklist entry the trigger is published.
	sc.sendSyncTriggerToKafka("peer", syncCoordinatorTestHash(t).String())
	select {
	case <-producer.PublishChannel():
	default:
		t.Fatal("precondition: sync trigger must be published when the URL is not blacklisted")
	}

	// Operator blacklists the host after the URL was stored.
	sc.settings.SubtreeValidation.BlacklistedBaseURLs = map[string]struct{}{"http://evil.example": {}}

	sc.sendSyncTriggerToKafka("peer", syncCoordinatorTestHash(t).String())
	select {
	case published := <-producer.PublishChannel():
		t.Fatalf("sync trigger with blacklisted DataHubURL must not be published: %+v", published)
	default:
	}
}

// blockingRegistry wraps a real registry client but blocks ListPeers until the
// per-call context is done, recording whether that context carried a deadline.
// It exercises the boundedRPCContext used by every registry wrapper.
type blockingRegistry struct {
	blockchain.PeerRegistryClientI
	mu          sync.Mutex
	hadDeadline bool
}

func (b *blockingRegistry) ListPeers(ctx context.Context, _ *blockchain_api.TransportType, _ float64, _ uint32, _, _ bool) ([]*blockchain.PeerInfo, error) {
	_, ok := ctx.Deadline()
	b.mu.Lock()
	b.hadDeadline = ok
	b.mu.Unlock()
	<-ctx.Done()
	return nil, ctx.Err()
}

func (b *blockingRegistry) listPeersHadDeadline() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.hadDeadline
}

func TestSyncCoordinator_RegistryCallsAreTimeBounded(t *testing.T) {
	sc, _ := newTestSyncCoordinator(t)
	br := &blockingRegistry{PeerRegistryClientI: sc.registry}
	sc.registry = br
	sc.rpcTimeout = 50 * time.Millisecond

	result := make(chan []*blockchain.PeerInfo, 1)
	go func() {
		result <- sc.listAllPeers()
	}()

	select {
	case peers := <-result:
		require.Nil(t, peers, "a timed-out registry call must degrade to no peers")
	case <-time.After(5 * time.Second):
		t.Fatal("listAllPeers did not return; registry RPC context is unbounded")
	}
	require.True(t, br.listPeersHadDeadline(), "registry RPC context must carry a deadline")
}

// TestSyncCoordinator_StopUnblocksInFlightRPC parks a monitor goroutine inside a
// blockchain-client RPC that only returns on context cancellation, then verifies
// Stop() aborts the in-flight call and drains the goroutines without depending
// on the caller's context being cancelled or on the RPC timeout elapsing.
func TestSyncCoordinator_StopUnblocksInFlightRPC(t *testing.T) {
	sc, _ := newTestSyncCoordinator(t)
	sc.rpcTimeout = time.Minute // prove Stop's cancel does the unblocking, not the timeout

	inRPC := make(chan struct{})
	var once sync.Once
	client := &blockchain.Mock{}
	client.On("GetBestBlockHeader", mock.Anything).Run(func(args mock.Arguments) {
		once.Do(func() { close(inRPC) })
		<-args.Get(0).(context.Context).Done()
	}).Return(nil, nil, errors.NewServiceError("cancelled"))
	sc.blockchainClient = client

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sc.Start(ctx)

	select {
	case <-inRPC: // monitorFSM's first tick is parked inside GetBestBlockHeader
	case <-time.After(10 * time.Second):
		t.Fatal("monitor goroutine never reached the blockchain RPC")
	}

	done := make(chan struct{})
	go func() {
		sc.Stop(context.Background())
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not abort the in-flight RPC and drain the goroutines")
	}
}

func TestSyncCoordinator_StopDrainsGoroutinesAndIsIdempotent(t *testing.T) {
	sc, _ := newTestSyncCoordinator(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sc.Start(ctx)

	done := make(chan struct{})
	go func() {
		sc.Stop(context.Background())
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not drain the coordinator goroutines")
	}

	require.NotPanics(t, func() { sc.Stop(context.Background()) }, "Stop must be idempotent")
}

func TestSyncCoordinator_StopBeforeStartReturns(t *testing.T) {
	sc, _ := newTestSyncCoordinator(t)
	require.NotPanics(t, func() { sc.Stop(context.Background()) })
}

// TestSyncCoordinator_StopHonorsContextDeadlineWhenGoroutineStuck simulates a
// coordinator goroutine parked in a non-context-aware blocking call (e.g. a
// wedged Kafka producer Publish, which neither stopCh nor context cancellation
// can release) and verifies Stop returns when its context expires instead of
// hanging the whole server shutdown on wg.Wait.
func TestSyncCoordinator_StopHonorsContextDeadlineWhenGoroutineStuck(t *testing.T) {
	sc, _ := newTestSyncCoordinator(t)

	gate := make(chan struct{})
	sc.wg.Add(1)
	go func() {
		defer sc.wg.Done()
		<-gate // stands in for a blocking, non-context-aware call
	}()

	stopCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		sc.Stop(stopCtx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop ignored its context deadline while a goroutine was stuck")
	}

	// A repeated Stop while still stuck must also time out (sharing the single
	// wg watcher rather than leaking one per call) instead of blocking.
	stopCtx2, cancel2 := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel2()
	require.NotPanics(t, func() { sc.Stop(stopCtx2) })

	// Release the stuck goroutine; a further Stop must now drain fully.
	close(gate)
	require.NotPanics(t, func() { sc.Stop(context.Background()) })
}

// TestSyncCoordinator_LocalHeightCallbackIsTimeBounded parks the local-height
// callback against a hung RPC (blocking until its context is done, exactly as
// Server.getLocalHeight behaves against a hung blockchain service) and asserts
// a monitor-loop path through getLocalHeightSafe still returns, with the
// callback context carrying a deadline. Guards the ChiR2 wedge: this callback
// used to run an unbounded GetBestBlockHeader on the server-lifetime context.
func TestSyncCoordinator_LocalHeightCallbackIsTimeBounded(t *testing.T) {
	sc, _ := newTestSyncCoordinator(t)
	sc.rpcTimeout = 50 * time.Millisecond

	var mu sync.Mutex
	hadDeadline := false
	sc.SetGetLocalHeightCallback(func(ctx context.Context) uint32 {
		_, ok := ctx.Deadline()
		mu.Lock()
		hadDeadline = ok
		mu.Unlock()
		<-ctx.Done()
		return 0
	})

	result := make(chan bool, 1)
	go func() {
		result <- sc.isCaughtUp() // monitor-loop path; reaches the callback via getLocalHeightSafe
	}()

	select {
	case <-result:
	case <-time.After(5 * time.Second):
		t.Fatal("isCaughtUp did not return; the local-height callback context is unbounded")
	}

	mu.Lock()
	defer mu.Unlock()
	require.True(t, hadDeadline, "local-height callback context must carry a deadline")
}

// levelCapturingLogger counts Warnf/Debugf calls so tests can assert the level
// a message was logged at; everything else falls through to TestLogger.
type levelCapturingLogger struct {
	ulogger.TestLogger
	mu     sync.Mutex
	warns  int
	debugs int
}

func (l *levelCapturingLogger) Warnf(string, ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.warns++
}

func (l *levelCapturingLogger) Debugf(string, ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.debugs++
}

func (l *levelCapturingLogger) counts() (warns, debugs int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.warns, l.debugs
}

func TestSyncCoordinator_WarnfUnlessStopping_DowngradesToDebugAfterStop(t *testing.T) {
	logger := &levelCapturingLogger{}
	tSettings := &settings.Settings{}
	reg := blockchain.NewCentralizedPeerRegistry(blockchain.DefaultBanConfig())
	sc := NewSyncCoordinator(
		context.Background(),
		logger,
		tSettings,
		blockchain.NewLocalPeerRegistryClient(reg),
		NewPeerSelector(ulogger.TestLogger{}, tSettings),
		nil,
		nil,
	)

	sc.warnfUnlessStopping("running")
	warns, debugs := logger.counts()
	require.Equal(t, 1, warns, "before Stop the message must log at warn level")
	require.Zero(t, debugs)

	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	sc.Stop(stopCtx)

	sc.warnfUnlessStopping("stopping")
	warns, debugs = logger.counts()
	require.Equal(t, 1, warns, "after Stop no new warn-level messages may be emitted")
	require.Equal(t, 1, debugs, "after Stop the message must downgrade to debug level")
}

// registerSyncTestPeer registers a viable sync candidate for the FSM-latch tests.
func registerSyncTestPeer(t *testing.T, reg *blockchain.CentralizedPeerRegistry, id string, height uint32, validatedWork []byte) {
	t.Helper()

	info := &blockchain.PeerInfo{
		ID:         id,
		DataHubURL: "http://" + id,
		Height:     height,
		BlockHash:  syncCoordinatorTestHash(t),
		Storage:    "full",
	}
	if len(validatedWork) > 0 {
		info.ValidatedBlockHash = syncCoordinatorTestHash(t)
		info.ValidatedChainWork = validatedWork
	}
	reg.Register(info)
}

// claimSyncTestPeer installs peerID as the current sync peer as claimSelectedSyncPeer
// would, with the claim placed claimAge in the past.
func claimSyncTestPeer(sc *SyncCoordinator, peerID string, claimAge time.Duration, localWork []byte) {
	claimedAt := time.Now().Add(-claimAge)
	sc.mu.Lock()
	sc.currentSyncPeer = peerID
	sc.syncStartTime = claimedAt
	sc.lastSyncProgressTime = claimedAt
	sc.lastLocalChainWork = localWork
	sc.mu.Unlock()
}

// Regression for the missing FSM-state latch: a steady RUNNING tick (no
// CATCHINGBLOCKS -> RUNNING completion edge) must not judge the in-flight sync,
// even when the slot holder is credited with higher validated work from a
// previous run. The old code declared it failed and recycled the slot every 2s.
func TestSyncCoordinator_HandleFSMTransition_SteadyRunningTickDoesNotRecycleSlot(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)
	setSyncCoordinatorLocalTip(t, sc, 100, []byte{0x02})
	registerSyncTestPeer(t, reg, "current", 200, []byte{0x03})
	claimSyncTestPeer(sc, "current", 0, []byte{0x02})

	running := blockchain_api.FSMStateType_RUNNING
	for i := range 3 {
		require.False(t, sc.handleFSMTransition(&running), "steady RUNNING tick %d must not settle the sync", i)
		require.Equal(t, "current", sc.GetCurrentSyncPeer(), "slot must not be recycled on tick %d", i)
	}
}

// A CATCHINGBLOCKS -> RUNNING completion edge never issues a verdict: it cannot
// say whose catchup finished (the header phase runs in RUNNING, and the slot may
// have changed hands during another peer's header phase — the claim here
// predates the excursion, exactly the handover shape that used to be judged by
// the other peer's outcome). Verdicts come only from HandleCatchupSuccess.
func TestSyncCoordinator_HandleFSMTransition_CompletionEdgeDoesNotJudgeSlot(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)
	setSyncCoordinatorLocalTip(t, sc, 100, []byte{0x02})
	registerSyncTestPeer(t, reg, "current", 200, []byte{0x03})
	claimSyncTestPeer(sc, "current", time.Minute, []byte{0x02})

	catching := blockchain_api.FSMStateType_CATCHINGBLOCKS
	require.False(t, sc.handleFSMTransition(&catching))
	running := blockchain_api.FSMStateType_RUNNING
	for i := range 2 {
		require.False(t, sc.handleFSMTransition(&running), "tick %d: the edge must not judge the slot holder", i)
		require.Equal(t, "current", sc.GetCurrentSyncPeer(), "tick %d: slot must survive the completion edge", i)
	}
}

// An armed completion edge holds the fallback release off while the
// authoritative report is in flight, bounded by pendingCompletionExpiry; once
// the window expires the fallback becomes eligible again and releases a quiet
// level holder.
func TestSyncCoordinator_HandleFSMTransition_ArmedEdgeHoldsFallbackUntilExpiry(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)
	setSyncCoordinatorLocalTip(t, sc, 200, []byte{0x03})
	registerSyncTestPeer(t, reg, "current", 200, []byte{0x03})
	// Level and quiet past the guard: the fallback would release on this tick.
	claimSyncTestPeer(sc, "current", 3*time.Minute, []byte{0x03})

	catching := blockchain_api.FSMStateType_CATCHINGBLOCKS
	require.False(t, sc.handleFSMTransition(&catching))
	running := blockchain_api.FSMStateType_RUNNING
	require.False(t, sc.handleFSMTransition(&running), "armed edge must hold the fallback release off")
	require.Equal(t, "current", sc.GetCurrentSyncPeer())

	sc.pendingCompletionSince = time.Now().Add(-pendingCompletionExpiry - time.Second)
	require.False(t, sc.handleFSMTransition(&running), "the expiring tick disarms without releasing")
	require.Empty(t, sc.pendingCompletionPeer, "window must be disarmed after expiry")
	require.Equal(t, "current", sc.GetCurrentSyncPeer())

	require.True(t, sc.handleFSMTransition(&running), "after expiry the fallback must release the quiet level holder")
	require.Empty(t, sc.GetCurrentSyncPeer())
}

// A slot claimed while another peer's catchup is in its block phase must not be
// judged by that catchup's completion; since edges never issue verdicts, the
// fresh holder survives the excursion end untouched.
func TestSyncCoordinator_HandleFSMTransition_SlotClaimedDuringCatchupNotJudgedByItsCompletion(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)
	setSyncCoordinatorLocalTip(t, sc, 100, []byte{0x02})
	registerSyncTestPeer(t, reg, "fresh", 200, []byte{0x03})

	// Observe CATCHINGBLOCKS with the slot empty (the running catchup belongs to
	// an earlier, since-cleared peer), then claim a fresh peer mid-catchup.
	catching := blockchain_api.FSMStateType_CATCHINGBLOCKS
	require.False(t, sc.handleFSMTransition(&catching))
	claimSyncTestPeer(sc, "fresh", 0, []byte{0x02})

	running := blockchain_api.FSMStateType_RUNNING
	for i := range 2 {
		require.False(t, sc.handleFSMTransition(&running), "tick %d must not judge the freshly claimed slot", i)
		require.Equal(t, "fresh", sc.GetCurrentSyncPeer(), "tick %d must keep the freshly claimed slot", i)
	}
}

// HandleCatchupSuccess settles a completed sync from the authoritative block
// validation signal, covering catchups that never leave RUNNING and are therefore
// invisible to the FSM completion edge.
func TestSyncCoordinator_HandleCatchupSuccess_LevelPeerSettlesWithoutFSMEdge(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)
	setSyncCoordinatorLocalTip(t, sc, 200, []byte{0x03})
	registerSyncTestPeer(t, reg, "current", 200, []byte{0x03})
	claimSyncTestPeer(sc, "current", time.Minute, []byte{0x03})
	sc.mu.Lock()
	sc.backoffMultiplier = 8
	sc.mu.Unlock()

	sc.HandleCatchupSuccess("current", time.Minute)

	require.Empty(t, sc.GetCurrentSyncPeer(), "reported catchup success for a level peer must settle the sync")
	sc.mu.RLock()
	require.Equal(t, 1, sc.backoffMultiplier)
	sc.mu.RUnlock()
}

func TestSyncCoordinator_HandleCatchupSuccess_IgnoresNonSyncPeer(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)
	setSyncCoordinatorLocalTip(t, sc, 100, []byte{0x02})
	registerSyncTestPeer(t, reg, "current", 200, []byte{0x03})
	registerSyncTestPeer(t, reg, "other", 200, []byte{0x03})
	claimSyncTestPeer(sc, "current", time.Minute, []byte{0x02})

	sc.HandleCatchupSuccess("other", time.Minute)

	require.Equal(t, "current", sc.GetCurrentSyncPeer(), "success for a different peer must not touch the slot")
}

// If HandleCatchupSuccess settles a completion and the retrigger claims a new
// peer, the FSM completion edge observed afterwards must not evict that peer:
// the edge arms the fallback-suppression window for the fresh holder but never
// issues a verdict. (The window consumption inside HandleCatchupSuccess itself
// is pinned separately by
// TestSyncCoordinator_HandleCatchupSuccess_ConsumesDeferredEdge.)
func TestSyncCoordinator_HandleCatchupSuccess_TrailingEdgeDoesNotEvictFreshSlot(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)
	setSyncCoordinatorLocalTip(t, sc, 100, []byte{0x02})
	registerSyncTestPeer(t, reg, "done", 200, []byte{0x04})
	registerSyncTestPeer(t, reg, "next", 200, []byte{0x03})
	reg.RecordSyncAttempt("done")
	claimSyncTestPeer(sc, "done", 2*time.Minute, []byte{0x02})

	catching := blockchain_api.FSMStateType_CATCHINGBLOCKS
	require.False(t, sc.handleFSMTransition(&catching))

	// The success report arrives first: "done" is still ahead, so it is cleared
	// and the retrigger claims "next" (the only unbenched candidate).
	sc.HandleCatchupSuccess("done", time.Minute)
	require.Equal(t, "next", sc.GetCurrentSyncPeer())

	running := blockchain_api.FSMStateType_RUNNING
	require.False(t, sc.handleFSMTransition(&running), "the trailing completion edge must not judge the fresh slot")
	require.Equal(t, "next", sc.GetCurrentSyncPeer(), "fresh slot must survive the trailing FSM edge")
}

// Regression for the backoff ratchet: expiry cycles double the multiplier and
// clear allPeersAttempted, so the old flag-gated reset never fired in production
// ordering. resetBackoff must reset the multiplier unconditionally.
func TestSyncCoordinator_ResetBackoff_ResetsRatchetedMultiplierAfterExpiredWindows(t *testing.T) {
	sc, _ := newTestSyncCoordinator(t)

	for range 3 {
		sc.enterBackoffMode()
		sc.mu.Lock()
		sc.lastAllPeersAttemptTime = time.Now().Add(-time.Hour) // force window expiry
		sc.mu.Unlock()
		require.False(t, sc.checkAndClearExpiredBackoff())
	}
	sc.mu.RLock()
	require.Equal(t, 8, sc.backoffMultiplier)
	require.False(t, sc.allPeersAttempted, "expiry leaves the flag cleared — the production state at reset time")
	sc.mu.RUnlock()

	sc.resetBackoff()

	sc.mu.RLock()
	require.Equal(t, 1, sc.backoffMultiplier, "resetBackoff must de-escalate a ratcheted multiplier")
	sc.mu.RUnlock()
}

// A local chainwork advance refreshes the probe budget but deliberately does NOT
// de-escalate the backoff multiplier: gossip-driven advances are not attributable
// to the sync slot, and resetting on every advance would let each ~2s
// selection-failure cycle re-enter backoff (whose entry clears all sync-attempt
// cooldowns) during IBD.
func TestSyncCoordinator_LocalChainWorkAdvanceDoesNotResetBackoffMultiplier(t *testing.T) {
	sc, _ := newTestSyncCoordinator(t)
	sc.mu.Lock()
	sc.backoffMultiplier = 32
	sc.lastLocalChainWork = []byte{0x02}
	sc.unprovenProbeBudgetRemaining = 0
	sc.mu.Unlock()

	sc.resetProbeBudgetIfLocalChainWorkAdvanced([]byte{0x03})

	sc.mu.RLock()
	require.Equal(t, 32, sc.backoffMultiplier, "gossip-driven chainwork advance must not de-escalate the multiplier")
	require.Positive(t, sc.unprovenProbeBudgetRemaining, "chainwork advance must still refresh the probe budget")
	sc.mu.RUnlock()
}

// A level slot holder in steady RUNNING (no completion edge, no success signal —
// e.g. its trigger was rejected by block validation's single-flight guard) is
// quietly released by the fallback once it has gone the grace period without
// delivering a validated block, instead of pinning the slot until the
// no-progress deadline. The release is an inferred completion: it must not
// fabricate a success verdict, so backoff and the probe budget stay untouched
// and no sync attempt is recorded against the peer.
func TestSyncCoordinator_HandleFSMTransition_LevelQuietPeerFallbackReleasesAfterGrace(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)
	setSyncCoordinatorLocalTip(t, sc, 200, []byte{0x03})
	registerSyncTestPeer(t, reg, "current", 200, []byte{0x03})
	sc.mu.Lock()
	sc.backoffMultiplier = 16
	sc.unprovenProbeBudgetRemaining = 0
	sc.mu.Unlock()

	running := blockchain_api.FSMStateType_RUNNING

	// Inside the grace (default 5m no-progress -> 2.5m guard): no release.
	claimSyncTestPeer(sc, "current", time.Minute, []byte{0x03})
	require.False(t, sc.handleFSMTransition(&running), "level peer inside the grace must not be released")
	require.Equal(t, "current", sc.GetCurrentSyncPeer())

	// Past the grace but short of the 5m no-progress deadline: released quietly.
	claimSyncTestPeer(sc, "current", 3*time.Minute, []byte{0x03})
	require.True(t, sc.handleFSMTransition(&running), "quiet level peer past the grace must be released")
	require.Empty(t, sc.GetCurrentSyncPeer())

	sc.mu.RLock()
	require.Equal(t, 16, sc.backoffMultiplier, "an inferred completion must not reset backoff")
	require.Zero(t, sc.unprovenProbeBudgetRemaining, "an inferred completion must not refill the probe budget")
	sc.mu.RUnlock()
	got, ok := reg.Get("current")
	require.True(t, ok)
	require.True(t, got.LastSyncAttempt.IsZero(), "release must not record a sync attempt against the peer")
}

// The fallback keys on progress age, not claim age: a level peer that is
// actively delivering validated blocks keeps the slot no matter how long it has
// held it, matching evaluateSyncPeer's keep-while-caught-up policy.
func TestSyncCoordinator_HandleFSMTransition_FallbackKeepsActivelyDeliveringLevelPeer(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)
	setSyncCoordinatorLocalTip(t, sc, 200, []byte{0x03})
	registerSyncTestPeer(t, reg, "current", 200, []byte{0x03})

	// Claimed 5 minutes ago, but validated progress is as fresh as it can be.
	claimSyncTestPeer(sc, "current", 5*time.Minute, []byte{0x03})
	sc.mu.Lock()
	sc.lastSyncProgressTime = time.Now()
	sc.mu.Unlock()

	running := blockchain_api.FSMStateType_RUNNING
	require.False(t, sc.handleFSMTransition(&running), "actively delivering level peer must not be released")
	require.Equal(t, "current", sc.GetCurrentSyncPeer())
}

// The fallback never settles a slot holder that is still ahead by validated
// work — that would reintroduce the churn the latch removes.
func TestSyncCoordinator_HandleFSMTransition_FallbackDoesNotSettleAheadPeer(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)
	setSyncCoordinatorLocalTip(t, sc, 100, []byte{0x02})
	registerSyncTestPeer(t, reg, "current", 200, []byte{0x03})
	claimSyncTestPeer(sc, "current", 3*time.Minute, []byte{0x02})

	running := blockchain_api.FSMStateType_RUNNING
	require.False(t, sc.handleFSMTransition(&running))
	require.Equal(t, "current", sc.GetCurrentSyncPeer(), "ahead peer must be left to the completion edge or the no-progress deadline")
}

// A success report whose catchup started before the current claim (the peer was
// evicted and re-claimed while its old catchup kept running) must not judge the
// fresh claim.
func TestSyncCoordinator_HandleCatchupSuccess_StaleReportDoesNotJudgeFreshClaim(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)
	setSyncCoordinatorLocalTip(t, sc, 100, []byte{0x02})
	registerSyncTestPeer(t, reg, "current", 200, []byte{0x03})
	claimSyncTestPeer(sc, "current", 0, []byte{0x02})

	// The reported catchup ran 10 minutes, so it started well before the claim.
	sc.HandleCatchupSuccess("current", 10*time.Minute)

	require.Equal(t, "current", sc.GetCurrentSyncPeer(), "stale success must not touch the fresh claim")
}

// The latch fields are documented as decisionMu-guarded; exercise the FSM path
// and the success signal concurrently so -race can observe a violation.
func TestSyncCoordinator_LatchFields_ConcurrentFSMAndSuccessSignal(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)
	setSyncCoordinatorLocalTip(t, sc, 200, []byte{0x03})
	registerSyncTestPeer(t, reg, "current", 200, []byte{0x03})
	claimSyncTestPeer(sc, "current", time.Minute, []byte{0x03})

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		catching := blockchain_api.FSMStateType_CATCHINGBLOCKS
		running := blockchain_api.FSMStateType_RUNNING
		for range 100 {
			sc.decisionMu.Lock()
			sc.handleFSMTransition(&catching)
			sc.handleFSMTransition(&running)
			sc.decisionMu.Unlock()
		}
	}()
	go func() {
		defer wg.Done()
		for range 100 {
			sc.HandleCatchupSuccess("current", time.Minute)
		}
	}()
	wg.Wait()
}

// The monitor tick calls reputation recovery through a 30s throttle so the
// FSM latch (which stopped short-circuiting steady RUNNING ticks) does not
// turn it into a per-2s ReconsiderBadPeers RPC.
func TestSyncCoordinator_ConsiderReputationRecoveryThrottled_SamplesAtInterval(t *testing.T) {
	sc, _ := newTestSyncCoordinator(t)

	sc.decisionMu.Lock()
	defer sc.decisionMu.Unlock()

	sc.considerReputationRecoveryThrottled()
	first := sc.lastReputationRecovery
	require.False(t, first.IsZero())

	sc.considerReputationRecoveryThrottled()
	require.Equal(t, first, sc.lastReputationRecovery, "second call inside the interval must be skipped")

	sc.lastReputationRecovery = time.Now().Add(-reputationRecoveryMinInterval - time.Second)
	stale := sc.lastReputationRecovery
	sc.considerReputationRecoveryThrottled()
	require.NotEqual(t, stale, sc.lastReputationRecovery, "call past the interval must run")
}

// HandleCatchupSuccess disarms an armed completion window even when its own
// verdict defers for missing validated work: the report owns that completion,
// so the window must stop suppressing the fallback release, and nothing may
// settle the slot holder afterwards on the edge's behalf.
func TestSyncCoordinator_HandleCatchupSuccess_ConsumesDeferredEdge(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)
	setSyncCoordinatorLocalTip(t, sc, 100, []byte{0x02})
	registerSyncTestPeer(t, reg, "current", 1_000, nil) // no validated work yet
	claimSyncTestPeer(sc, "current", 2*time.Minute, []byte{0x02})

	catching := blockchain_api.FSMStateType_CATCHINGBLOCKS
	running := blockchain_api.FSMStateType_RUNNING
	require.False(t, sc.handleFSMTransition(&catching))
	require.False(t, sc.handleFSMTransition(&running), "the edge arms a window, never a verdict")
	require.Equal(t, "current", sc.pendingCompletionPeer, "edge window must be armed")

	// The success report's own verdict defers (still no validated work) but it
	// disarms the window on the way.
	sc.HandleCatchupSuccess("current", time.Minute)
	require.Empty(t, sc.pendingCompletionPeer, "success signal must disarm the edge window")
	require.Equal(t, "current", sc.GetCurrentSyncPeer())

	// Validated work arriving later must not produce an edge-derived verdict.
	require.NoError(t, reg.RecordValidatedPeerProgress("current", 1_000, syncCoordinatorTestHash(t), []byte{0x04}))
	require.False(t, sc.handleFSMTransition(&running), "disarmed window must not settle after the fact")
	require.Equal(t, "current", sc.GetCurrentSyncPeer())
}

// An armed completion window expires after pendingCompletionExpiry whether or
// not validated work ever arrives; the expiry disarms without touching the
// slot, and a credit landing later produces no edge-derived verdict.
func TestSyncCoordinator_HandleFSMTransition_ArmedEdgeWindowExpires(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)
	setSyncCoordinatorLocalTip(t, sc, 100, []byte{0x02})
	registerSyncTestPeer(t, reg, "current", 1_000, nil) // no validated work yet
	claimSyncTestPeer(sc, "current", time.Minute, []byte{0x02})

	catching := blockchain_api.FSMStateType_CATCHINGBLOCKS
	running := blockchain_api.FSMStateType_RUNNING
	require.False(t, sc.handleFSMTransition(&catching))
	require.False(t, sc.handleFSMTransition(&running))
	require.Equal(t, "current", sc.pendingCompletionPeer, "window must stay armed inside the expiry")

	sc.pendingCompletionSince = time.Now().Add(-pendingCompletionExpiry - time.Second)
	require.False(t, sc.handleFSMTransition(&running), "the expiring tick disarms without a verdict")
	require.Empty(t, sc.pendingCompletionPeer, "expired window must be disarmed")
	require.Equal(t, "current", sc.GetCurrentSyncPeer(), "expiry must leave the slot alone")

	require.NoError(t, reg.RecordValidatedPeerProgress("current", 1_000, syncCoordinatorTestHash(t), []byte{0x04}))
	require.False(t, sc.handleFSMTransition(&running), "late credit must not produce an edge-derived verdict")
	require.Equal(t, "current", sc.GetCurrentSyncPeer())
}

// A peer that is ahead by raw chainwork but demoted by an active full-storage
// penalty (it served header work it failed to back with a block body) reads as
// "not ahead" through peerAheadByValidatedWork; the fallback must not release it
// as a finished sync — it is left to the no-progress deadline, which benches it.
func TestSyncCoordinator_HandleFSMTransition_FallbackDoesNotReleasePenalizedAheadPeer(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)
	setSyncCoordinatorLocalTip(t, sc, 100, []byte{0x02})
	hash := syncCoordinatorTestHash(t)
	reg.Register(&blockchain.PeerInfo{
		ID:                      "current",
		DataHubURL:              "http://current",
		Height:                  200,
		BlockHash:               hash,
		ValidatedBlockHash:      hash,
		ValidatedChainWork:      []byte{0x03}, // ahead by raw chainwork
		FullStoragePenaltyUntil: time.Now().Add(time.Hour),
	})
	claimSyncTestPeer(sc, "current", 3*time.Minute, []byte{0x02})

	running := blockchain_api.FSMStateType_RUNNING
	require.False(t, sc.handleFSMTransition(&running), "penalized ahead peer must not be released as level")
	require.Equal(t, "current", sc.GetCurrentSyncPeer())
}
