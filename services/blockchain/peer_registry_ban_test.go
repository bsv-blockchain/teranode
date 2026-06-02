package blockchain

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// shortBanConfig returns a BanConfig with very short durations for fast tests.
func shortBanConfig() BanConfig {
	return BanConfig{
		Threshold:     100,
		Duration:      time.Millisecond,
		DecayInterval: time.Millisecond,
		DecayAmount:   1,
		ReasonPoints: map[string]int32{
			"invalid_block": 10,
			"spam":          50,
		},
	}
}

// ---------------------------------------------------------------------------
// AddBanScore
// ---------------------------------------------------------------------------

func TestBanAddBanScore_BasicScoring(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	score, banned := r.AddBanScore("peer-1", "test", 25)
	require.Equal(t, int32(25), score)
	require.False(t, banned)

	score, banned = r.AddBanScore("peer-1", "test", 30)
	require.Equal(t, int32(55), score)
	require.False(t, banned)
}

func TestBanAddBanScore_ThresholdBan(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	// Add points up to just below threshold.
	score, banned := r.AddBanScore("peer-1", "test", 99)
	require.Equal(t, int32(99), score)
	require.False(t, banned)

	// Crossing the threshold should trigger a ban.
	score, banned = r.AddBanScore("peer-1", "test", 1)
	require.Equal(t, int32(100), score)
	require.True(t, banned)

	// Adding more points to an already-banned peer should NOT return banned=true
	// again (the return value signals a NEW ban only).
	score, banned = r.AddBanScore("peer-1", "test", 10)
	require.Equal(t, int32(110), score)
	require.False(t, banned, "banned should be false because peer was already banned")
}

func TestBanAddBanScore_DecayBeforeAdding(t *testing.T) {
	cfg := shortBanConfig()
	cfg.Threshold = 100
	cfg.DecayInterval = time.Millisecond
	cfg.DecayAmount = 5
	r := NewCentralizedPeerRegistry(cfg)

	// Register an initial score.
	score, _ := r.AddBanScore("peer-1", "test", 20)
	require.Equal(t, int32(20), score)

	// Wait long enough for at least one decay step.
	time.Sleep(5 * time.Millisecond)

	// Add more points; the previous score should have decayed.
	score, _ = r.AddBanScore("peer-1", "test", 10)
	require.Less(t, score, int32(30), "score should be less than 30 because decay was applied")
}

func TestBanAddBanScore_PeerInfoSync(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	// Register a peer so the PeerInfo map is populated.
	r.Register(&PeerInfo{ID: "peer-1"})

	r.AddBanScore("peer-1", "test", 40)

	got, ok := r.Get("peer-1")
	require.True(t, ok)
	require.Equal(t, int32(40), got.BanScore)
	require.False(t, got.IsBanned)

	// Push past threshold.
	r.AddBanScore("peer-1", "test", 70)

	got, ok = r.Get("peer-1")
	require.True(t, ok)
	require.Equal(t, int32(110), got.BanScore)
	require.True(t, got.IsBanned)
}

func TestBanAddBanScore_ConfigReasonLookup(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	// "invalid_block" is mapped to 10 points in DefaultBanConfig.
	// Even though we pass 9999, the config value should be used.
	score, _ := r.AddBanScore("peer-1", "invalid_block", 9999)
	require.Equal(t, int32(10), score)

	// Unknown reason should use the provided points.
	r2 := NewCentralizedPeerRegistry(DefaultBanConfig())
	score, _ = r2.AddBanScore("peer-2", "unknown_reason", 42)
	require.Equal(t, int32(42), score)
}

func TestBanAddBanScore_ReasonHistoryCappedAt20(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	// 25 separate reason additions; only the last 20 should remain.
	for i := 0; i < 25; i++ {
		r.AddBanScore("peer-1", "test", 1)
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	require.Len(t, r.banScores["peer-1"].Reasons, 20)
}

// ---------------------------------------------------------------------------
// IsBannedPeer
// ---------------------------------------------------------------------------

func TestBanIsBannedPeer_NotBanned(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	require.False(t, r.IsBannedPeer("nonexistent-peer"))
}

func TestBanIsBannedPeer_Banned(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	r.AddBanScore("peer-1", "test", 100)
	require.True(t, r.IsBannedPeer("peer-1"))
}

func TestBanIsBannedPeer_AutoUnban(t *testing.T) {
	cfg := shortBanConfig()
	cfg.Duration = time.Millisecond
	r := NewCentralizedPeerRegistry(cfg)

	// Register so PeerInfo sync can be tested too.
	r.Register(&PeerInfo{ID: "peer-1"})

	r.AddBanScore("peer-1", "test", 100)
	require.True(t, r.IsBannedPeer("peer-1"))

	// Wait for the ban to expire.
	time.Sleep(5 * time.Millisecond)

	require.False(t, r.IsBannedPeer("peer-1"), "ban should have expired")

	// PeerInfo should be updated by the auto-unban.
	got, ok := r.Get("peer-1")
	require.True(t, ok)
	require.False(t, got.IsBanned)
	require.Equal(t, int32(0), got.BanScore)
}

// ---------------------------------------------------------------------------
// ListBannedPeers
// ---------------------------------------------------------------------------

func TestBanListBannedPeers_Empty(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	banned := r.ListBannedPeers()
	require.Empty(t, banned)
}

func TestBanListBannedPeers_ReturnsBannedOnly(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	// Ban two peers, leave one with a score below threshold.
	r.AddBanScore("bad-1", "test", 100)
	r.AddBanScore("bad-2", "test", 150)
	r.AddBanScore("good-1", "test", 10)

	banned := r.ListBannedPeers()
	require.Len(t, banned, 2)
	require.ElementsMatch(t, []string{"bad-1", "bad-2"}, banned)
}

// ---------------------------------------------------------------------------
// ClearBannedPeers
// ---------------------------------------------------------------------------

func TestBanClearBannedPeers(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	r.Register(&PeerInfo{ID: "peer-1"})
	r.Register(&PeerInfo{ID: "peer-2"})

	r.AddBanScore("peer-1", "test", 100)
	r.AddBanScore("peer-2", "test", 100)

	require.Len(t, r.ListBannedPeers(), 2)

	r.ClearBannedPeers()

	require.Empty(t, r.ListBannedPeers())

	// Verify PeerInfo ban status is also reset.
	p1, ok := r.Get("peer-1")
	require.True(t, ok)
	require.False(t, p1.IsBanned)
	require.Equal(t, int32(0), p1.BanScore)

	p2, ok := r.Get("peer-2")
	require.True(t, ok)
	require.False(t, p2.IsBanned)
	require.Equal(t, int32(0), p2.BanScore)
}

// ---------------------------------------------------------------------------
// ReconsiderBadPeers
// ---------------------------------------------------------------------------

func TestBanReconsiderBadPeers_ResetsOldFailures(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	r.Register(&PeerInfo{ID: "old-bad"})
	// Drive reputation below the 20 threshold by recording a malicious event.
	r.UpdateMetrics("old-bad", 0, 0, 0, false, false, true, 0)

	got, _ := r.Get("old-bad")
	require.Less(t, got.ReputationScore, 20.0)

	// Backdate the last failure so it falls before the cooldown.
	r.mu.Lock()
	r.peers["old-bad"].LastInteractionFailure = time.Now().Add(-2 * time.Hour)
	r.mu.Unlock()

	count := r.ReconsiderBadPeers(1 * time.Hour)
	require.Equal(t, 1, count)

	got, _ = r.Get("old-bad")
	// Recovery sets reputation to 30 (below neutral 50, above threshold 20),
	// clears MaliciousCount, and stamps LastReputationReset.
	require.Equal(t, 30.0, got.ReputationScore)
	require.Equal(t, int64(0), got.MaliciousCount)
	require.False(t, got.LastReputationReset.IsZero())
	require.Equal(t, int32(1), got.ReputationResetCount)
}

func TestBanReconsiderBadPeers_ExponentialCooldownBlocksRepeatedReset(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	r.Register(&PeerInfo{ID: "repeat-bad"})
	r.UpdateMetrics("repeat-bad", 0, 0, 0, false, false, true, 0)

	r.mu.Lock()
	r.peers["repeat-bad"].LastInteractionFailure = time.Now().Add(-2 * time.Hour)
	r.mu.Unlock()

	// First reset succeeds.
	require.Equal(t, 1, r.ReconsiderBadPeers(1*time.Hour))

	// Drive reputation back below 20 and backdate the failure again.
	r.mu.Lock()
	r.peers["repeat-bad"].ReputationScore = 5
	r.peers["repeat-bad"].LastInteractionFailure = time.Now().Add(-2 * time.Hour)
	r.mu.Unlock()

	// Second attempt within the same cooldown window is blocked because
	// LastReputationReset is recent and the required cooldown is now 3× the base.
	require.Equal(t, 0, r.ReconsiderBadPeers(1*time.Hour))
}

func TestBanReconsiderBadPeers_IgnoresRecentFailures(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	r.Register(&PeerInfo{ID: "recent-bad"})
	r.UpdateMetrics("recent-bad", 0, 0, 0, false, false, true, 0)

	got, _ := r.Get("recent-bad")
	require.Less(t, got.ReputationScore, 30.0)

	// Do NOT backdate — the failure is recent.
	count := r.ReconsiderBadPeers(1 * time.Hour)
	require.Equal(t, 0, count)

	got, _ = r.Get("recent-bad")
	require.Less(t, got.ReputationScore, 30.0, "reputation should not have been reset")
}

func TestBanReconsiderBadPeers_ReturnsCount(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	// Create multiple bad peers with old failures.
	for _, id := range []string{"a", "b", "c"} {
		r.Register(&PeerInfo{ID: id})
		r.UpdateMetrics(id, 0, 0, 0, false, false, true, 0)
		r.mu.Lock()
		r.peers[id].LastInteractionFailure = time.Now().Add(-2 * time.Hour)
		r.mu.Unlock()
	}

	// One peer with a recent failure should not be reconsidered.
	r.Register(&PeerInfo{ID: "recent"})
	r.UpdateMetrics("recent", 0, 0, 0, false, false, true, 0)

	count := r.ReconsiderBadPeers(1 * time.Hour)
	require.Equal(t, 3, count)
}

// ---------------------------------------------------------------------------
// decayBanScores
// ---------------------------------------------------------------------------

func TestBanDecayBanScores_ScoresDecay(t *testing.T) {
	cfg := shortBanConfig()
	cfg.DecayInterval = time.Millisecond
	cfg.DecayAmount = 10
	r := NewCentralizedPeerRegistry(cfg)

	// Register a peer and give it a score.
	r.Register(&PeerInfo{ID: "peer-1"})
	r.AddBanScore("peer-1", "test", 50)

	// Wait for several decay intervals.
	time.Sleep(5 * time.Millisecond)

	r.decayBanScores()

	got, ok := r.Get("peer-1")
	require.True(t, ok)
	require.Less(t, got.BanScore, int32(50), "score should have decayed")
}

func TestBanDecayBanScores_ZeroScoreCleanup(t *testing.T) {
	cfg := shortBanConfig()
	cfg.DecayInterval = time.Millisecond
	cfg.DecayAmount = 100 // aggressive decay so it hits zero quickly
	r := NewCentralizedPeerRegistry(cfg)

	r.AddBanScore("peer-1", "test", 5)

	// Wait for decay to exceed the score.
	time.Sleep(5 * time.Millisecond)

	r.decayBanScores()

	// The entry should be cleaned up because score is 0 and peer is not banned.
	r.mu.RLock()
	_, exists := r.banScores["peer-1"]
	r.mu.RUnlock()
	require.False(t, exists, "zero-score unbanned entry should be removed")
}

func TestBanDecayBanScores_BannedEntryNotCleaned(t *testing.T) {
	cfg := shortBanConfig()
	cfg.DecayInterval = time.Millisecond
	cfg.DecayAmount = 100
	cfg.Duration = time.Hour // long ban so it doesn't expire during the test
	r := NewCentralizedPeerRegistry(cfg)

	// Ban the peer.
	r.AddBanScore("peer-1", "test", 100)
	require.True(t, r.IsBannedPeer("peer-1"))

	time.Sleep(5 * time.Millisecond)
	r.decayBanScores()

	// Even though score decayed to 0, the entry should remain because peer is still banned.
	r.mu.RLock()
	entry, exists := r.banScores["peer-1"]
	r.mu.RUnlock()
	require.True(t, exists, "banned entry should not be cleaned up")
	require.True(t, entry.Banned)
}

func TestBanAddBanScore_ExpiresStaleBanBeforeScoring(t *testing.T) {
	cfg := DefaultBanConfig()
	cfg.Duration = 5 * time.Millisecond
	r := NewCentralizedPeerRegistry(cfg)

	r.Register(&PeerInfo{ID: "p"})
	// Two strikes ban (50 + 50 = 100 = threshold).
	r.AddBanScore("p", "spam", 0)
	r.AddBanScore("p", "spam", 0)
	require.True(t, r.IsBannedPeer("p"))

	// Walk past the ban window.
	time.Sleep(20 * time.Millisecond)

	// Without an intervening IsBannedPeer call, the entry still has Banned=true
	// and Score>=Threshold. Adding a fresh score with the stale-expiry guard
	// must reset Banned/Score first, then re-arm the ban after the new strike.
	score, banned := r.AddBanScore("p", "spam", 0)
	require.Equal(t, int32(50), score, "stale ban must be expired before counting the new strike")
	require.False(t, banned, "single fresh strike below threshold must not re-ban")

	got, _ := r.Get("p")
	require.False(t, got.IsBanned)
	require.Equal(t, int32(50), got.BanScore)
}

func TestBanClose_StopsDecayGoroutineWithoutContextCancel(t *testing.T) {
	cfg := shortBanConfig()
	cfg.DecayInterval = time.Millisecond
	cfg.DecayAmount = 50
	r := NewCentralizedPeerRegistry(cfg)

	r.AddBanScore("p", "test", 80)
	r.StartBanDecay(context.Background())

	// Close must drive the goroutine to exit even though the supplied ctx is
	// never cancelled (this is the shutdown-race fix from the review feedback).
	doneCh := make(chan struct{})
	go func() {
		r.Close()
		close(doneCh)
	}()

	select {
	case <-doneCh:
	case <-time.After(time.Second):
		t.Fatal("Close did not return within 1s — goroutine likely leaked")
	}
}

func TestBanClose_IsIdempotent(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())
	r.StartBanDecay(context.Background())
	r.Close()
	require.NotPanics(t, func() { r.Close() }, "second Close must not panic")
}

func TestBanStartBanDecay_ContextCancellation(t *testing.T) {
	cfg := shortBanConfig()
	cfg.DecayInterval = time.Millisecond
	cfg.DecayAmount = 50
	r := NewCentralizedPeerRegistry(cfg)

	r.Register(&PeerInfo{ID: "peer-1"})
	r.AddBanScore("peer-1", "test", 80)

	ctx, cancel := context.WithCancel(context.Background())
	r.StartBanDecay(ctx)

	// Let the goroutine run a few ticks.
	time.Sleep(10 * time.Millisecond)

	// Score should have decayed by now.
	got, ok := r.Get("peer-1")
	require.True(t, ok)
	require.Less(t, got.BanScore, int32(80))

	// Cancel and verify the goroutine doesn't keep decaying.
	cancel()
	time.Sleep(5 * time.Millisecond)

	scoreAfterCancel, _ := r.Get("peer-1")
	time.Sleep(10 * time.Millisecond)
	scoreAfterWait, _ := r.Get("peer-1")
	require.Equal(t, scoreAfterCancel.BanScore, scoreAfterWait.BanScore,
		"score should stop decaying after context cancellation")
}

// ---------------------------------------------------------------------------
// Register reconciliation against surviving ban state
// ---------------------------------------------------------------------------

// TestRegister_ReconcilesSurvivingBanState confirms that a fresh Register call
// for a peer that was previously banned and then evicted from the in-memory
// peer map (TTL eviction, restart, cleanup) re-applies the persisted
// IsBanned/BanScore from banScores onto the new PeerInfo. Without this,
// catchup paths could reconnect to a peer the operator already banned.
func TestRegister_ReconcilesSurvivingBanState(t *testing.T) {
	cfg := DefaultBanConfig()
	r := NewCentralizedPeerRegistry(cfg)

	r.Register(&PeerInfo{ID: "peer-1"})
	// "spam" is 50; threshold is 100 → two hits to cross.
	_, _ = r.AddBanScore("peer-1", "spam", 0)
	_, banned := r.AddBanScore("peer-1", "spam", 0)
	require.True(t, banned)

	// Remove the peer record while leaving banScores intact (the documented
	// contract of Remove). banScores must outlive the in-memory peer entry.
	r.Remove("peer-1")
	_, ok := r.Get("peer-1")
	require.False(t, ok, "peer should be gone after Remove")
	require.True(t, r.IsBannedPeer("peer-1"), "ban must survive Remove")

	// Caller deliberately passes IsBanned=false / BanScore=0 to simulate a
	// catchup path that has no record of the prior ban. Register must trust
	// banScores over the caller's claims.
	r.Register(&PeerInfo{ID: "peer-1", IsBanned: false, BanScore: 0})
	info, ok := r.Get("peer-1")
	require.True(t, ok)
	require.True(t, info.IsBanned, "Register must reconcile IsBanned from banScores")
	require.Greater(t, info.BanScore, int32(0), "Register must reconcile BanScore from banScores")
}

// TestRegister_NoBanScoreSetsCleanState confirms the negative path: a peer
// with no banScores entry registers with IsBanned=false / BanScore=0 even if
// the caller passed stale truthy values.
func TestRegister_NoBanScoreSetsCleanState(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	// Caller is wrong/stale — should be ignored in favour of banScores (which
	// has no entry for this peer).
	r.Register(&PeerInfo{ID: "peer-1", IsBanned: true, BanScore: 99})
	info, ok := r.Get("peer-1")
	require.True(t, ok)
	require.False(t, info.IsBanned, "no banScores entry → IsBanned must be false")
	require.Equal(t, int32(0), info.BanScore, "no banScores entry → BanScore must be 0")
}

// ---------------------------------------------------------------------------
// Decay precision — LastDecay advances by exact multiples of DecayInterval
// ---------------------------------------------------------------------------

// TestBanDecay_LastDecayPreservesSubInterval confirms the precision fix: when
// elapsed time is N*interval + remainder, LastDecay advances by exactly
// N*interval, not to "now". Otherwise the remainder leaks on every call and
// bans last longer than configured.
func TestBanDecay_LastDecayPreservesSubInterval(t *testing.T) {
	cfg := DefaultBanConfig()
	cfg.DecayInterval = time.Second
	cfg.DecayAmount = 5
	r := NewCentralizedPeerRegistry(cfg)

	// Seed an entry with a known LastDecay anchor 3.5 seconds in the past.
	// AddBanScore will then see exactly 3 decay steps with 0.5s remainder.
	anchor := time.Now().Add(-3500 * time.Millisecond)
	r.banScores["peer-1"] = &banEntry{
		Score:     100,
		LastDecay: anchor,
	}

	r.AddBanScore("peer-1", "no-mapping", 0)

	entry := r.banScores["peer-1"]
	// Score: 100 - 3*5 = 85.
	require.Equal(t, int32(85), entry.Score)

	// LastDecay should be anchor + 3s = 0.5s ago (NOT "now"). Tolerate a few
	// ms of wall-clock jitter from this test's own runtime.
	expectedLastDecay := anchor.Add(3 * time.Second)
	drift := time.Since(entry.LastDecay) - 500*time.Millisecond
	require.Less(t, drift.Abs(), 100*time.Millisecond,
		"LastDecay must be anchor+3s; got drift %v from expected %v", drift, expectedLastDecay)
}

// TestBanDecay_NoDriftAcrossManyCalls drives many small decay calls and asserts
// the total ban-score loss matches (elapsed / interval) * amount, with no
// loss from the cumulative LastDecay-bleed bug the precision fix addresses.
func TestBanDecay_NoDriftAcrossManyCalls(t *testing.T) {
	cfg := DefaultBanConfig()
	cfg.DecayInterval = 10 * time.Millisecond
	cfg.DecayAmount = 1
	r := NewCentralizedPeerRegistry(cfg)

	start := time.Now()
	r.banScores["peer-1"] = &banEntry{
		Score:     1000,
		LastDecay: start,
	}

	// Tick decayBanScores 5 times across ~50ms — most calls will hit zero
	// decaySteps (elapsed < interval) and must not advance LastDecay.
	for i := 0; i < 5; i++ {
		time.Sleep(11 * time.Millisecond)
		r.decayBanScores()
	}

	entry := r.banScores["peer-1"]
	elapsed := time.Since(start)
	expectedSteps := int32(elapsed / cfg.DecayInterval)
	expectedScore := int32(1000) - expectedSteps*cfg.DecayAmount

	// Tolerate ±1 step worth of wall-clock jitter.
	require.InDelta(t, float64(expectedScore), float64(entry.Score), 2.0,
		"observed score %d; expected ~%d after %v elapsed", entry.Score, expectedScore, elapsed)
}

// ---------------------------------------------------------------------------
// SetLogger / log() concurrent access
// ---------------------------------------------------------------------------

// TestRegistry_SetLoggerNoRace exercises SetLogger and log() from concurrent
// goroutines under -race. atomic.Value Store/Load is the contract guarding
// the field; this is a guardrail against accidentally reverting to a plain
// field + r.mu mismatch.
func TestRegistry_SetLoggerNoRace(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 1000; i++ {
			_ = r.log()
		}
	}()

	for i := 0; i < 1000; i++ {
		r.SetLogger(ulogger.TestLogger{})
	}
	<-done

	require.NotNil(t, r.log())
}

// TestRegistry_SetLoggerNilIsNoop confirms that passing nil to SetLogger does
// not clobber a previously-installed logger, and that log() still returns
// the default fallback when no logger was ever set.
func TestRegistry_SetLoggerNilIsNoop(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())
	require.NotNil(t, r.log(), "default fallback when never set")

	r.SetLogger(ulogger.TestLogger{})
	first := r.log()
	r.SetLogger(nil)
	require.Equal(t, first, r.log(), "nil SetLogger must be a no-op")
}
