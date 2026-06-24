package p2p

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

func newTestSyncCoordinator(t *testing.T) (*SyncCoordinator, *blockchain.CentralizedPeerRegistry) {
	t.Helper()

	reg := blockchain.NewCentralizedPeerRegistry(blockchain.DefaultBanConfig())
	client := blockchain.NewLocalPeerRegistryClient(reg)

	tSettings := &settings.Settings{
		P2P: settings.P2PSettings{
			AllowPrunedNodeFallback:                   true,
			SyncCoordinatorPeriodicEvaluationInterval: 30 * time.Second,
		},
	}

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

func TestSyncCoordinator_IsViableSyncCandidate(t *testing.T) {
	good := &blockchain.PeerInfo{
		DataHubURL: "http://x", Height: 100, ReputationScore: 50,
	}
	require.True(t, isViableSyncCandidate(good))

	cases := []struct {
		name string
		p    *blockchain.PeerInfo
	}{
		{"banned", &blockchain.PeerInfo{IsBanned: true, DataHubURL: "x", Height: 1, ReputationScore: 50}},
		{"no url", &blockchain.PeerInfo{Height: 1, ReputationScore: 50}},
		{"zero height", &blockchain.PeerInfo{DataHubURL: "x", ReputationScore: 50}},
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

	reg.Register(&blockchain.PeerInfo{ID: "pruned", DataHubURL: "http://p", Height: 100, Storage: "pruned"})
	reg.Register(&blockchain.PeerInfo{ID: "full", DataHubURL: "http://f", Height: 100, Storage: "full"})
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
		{ID: "old", DataHubURL: "x", Height: 100, ReputationScore: 80},
		{ID: "low", DataHubURL: "x", Height: 10, ReputationScore: 80},
		{ID: "good", DataHubURL: "x", Height: 100, ReputationScore: 80},
	}

	got := sc.filterEligiblePeers(peers, "old", 50)

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

	sc.selectAndActivateNewPeer(50, "")

	sc.mu.RLock()
	require.True(t, sc.allPeersAttempted, "no peers above local height should enter backoff")
	sc.mu.RUnlock()
}

func TestSyncCoordinator_SelectAndActivateNewPeer_ActivatesEligible(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)

	reg.Register(&blockchain.PeerInfo{ID: "good", DataHubURL: "http://g", Height: 100, Storage: "full"})
	for i := 0; i < 5; i++ {
		reg.UpdateMetrics("good", 0, 0, 0, true, false, false, 100)
	}

	// activateSyncPeer fires sendSyncMessage which fails (no block hash) but
	// the coordinator still records the peer as the current sync target.
	sc.selectAndActivateNewPeer(50, "")

	require.Equal(t, "good", sc.GetCurrentSyncPeer())
}

func TestSyncCoordinator_ActivateSyncPeer_StoresIDEvenIfSendFails(t *testing.T) {
	sc, _ := newTestSyncCoordinator(t)

	sc.activateSyncPeer("doomed-peer")

	require.Equal(t, "doomed-peer", sc.GetCurrentSyncPeer())
	sc.mu.RLock()
	require.False(t, sc.syncStartTime.IsZero())
	sc.mu.RUnlock()
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
