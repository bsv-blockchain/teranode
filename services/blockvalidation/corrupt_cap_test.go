package blockvalidation

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jellydator/ttlcache/v3"
	"github.com/stretchr/testify/require"
)

// TestOptimisticMiningDisabledForPeerPath pins the item-1 operator gate truth table
// (bitcoin-sv/teranode#4692): optimistic mining is enabled on the peer/catch-up paths ONLY when BOTH
// OptimisticMining AND OptimisticMiningPeerBlocks are set, so the global opt-out always wins and the
// new peer-blocks flag can never bypass it. In particular (false, true) must stay disabled.
func TestOptimisticMiningDisabledForPeerPath(t *testing.T) {
	cases := []struct {
		global   bool
		peer     bool
		disabled bool
	}{
		{global: false, peer: false, disabled: true}, // off
		{global: false, peer: true, disabled: true},  // off — global opt-out wins, no bypass
		{global: true, peer: false, disabled: true},  // off — today's default; peer opt-in not set
		{global: true, peer: true, disabled: false},  // on — both enabled = deliberate opt-in
	}

	for _, c := range cases {
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.OptimisticMining = c.global
		tSettings.BlockValidation.OptimisticMiningPeerBlocks = c.peer

		require.Equal(t, c.disabled, optimisticMiningDisabledForPeerPath(tSettings),
			"OptimisticMining=%v OptimisticMiningPeerBlocks=%v -> disabled should be %v", c.global, c.peer, c.disabled)
	}
}

// newCorruptCapServer builds a Server with just the corrupt-cap machinery wired, mirroring the
// NewServer construction (fixed-window ttlcache, no touch-on-hit).
func newCorruptCapServer(t *testing.T, cap int) *Server {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.MaxCorruptAttemptsPerBlock = cap

	return &Server{
		settings: tSettings,
		logger:   ulogger.TestLogger{},
		blockCorruptAttempts: ttlcache.New[corruptAttemptKey, int](
			ttlcache.WithTTL[corruptAttemptKey, int](10*time.Minute),
			ttlcache.WithDisableTouchOnHit[corruptAttemptKey, int](),
		),
	}
}

// capTestPeer is the single serving peer used by the single-peer cap tests below; the cap keys on
// (hash, peerID) (bitcoin-sv/teranode#4692), so a fixed identity exercises the per-identity budget.
const capTestPeer = "peerA"

// ck builds the (hash, peerID) cache key for direct cache assertions in the tests.
func ck(h chainhash.Hash, peerID string) corruptAttemptKey {
	return corruptAttemptKey{hash: h, peerID: peerID}
}

// TestCorruptAttemptCooldownFallback covers the cooldown helper's fallbacks (bitcoin-sv/teranode#4692):
// nil settings or a non-positive setting fall back to 10m so the ttlcache always has a finite window;
// a positive setting is honoured.
func TestCorruptAttemptCooldownFallback(t *testing.T) {
	require.Equal(t, 10*time.Minute, corruptAttemptCooldown(nil), "nil settings falls back to 10m")

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.CorruptAttemptCooldown = 0
	require.Equal(t, 10*time.Minute, corruptAttemptCooldown(tSettings), "non-positive falls back to 10m")

	tSettings.BlockValidation.CorruptAttemptCooldown = 90 * time.Second
	require.Equal(t, 90*time.Second, corruptAttemptCooldown(tSettings), "a positive setting is honoured")
}

// TestCorruptAttemptCap is the item-8 RUNNING per-hash cap (bitcoin-sv/teranode#4692): corrupt-body
// re-downloads for a single block hash are bounded by MaxCorruptAttemptsPerBlock and then reported
// exhausted (in cooldown); once the window expires the hash can be retried again (self-heal). A cap
// of <= 0 disables the bound.
func TestCorruptAttemptCap(t *testing.T) {
	u := newCorruptCapServer(t, 3)

	h := chainhash.HashH([]byte("corrupt-cap"))

	require.False(t, u.corruptAttemptsExhausted(&h, capTestPeer), "zero attempts is not exhausted")
	require.Equal(t, 1, u.recordCorruptAttempt(&h, capTestPeer))
	require.False(t, u.corruptAttemptsExhausted(&h, capTestPeer), "1/3 is below the cap")
	require.Equal(t, 2, u.recordCorruptAttempt(&h, capTestPeer))
	require.False(t, u.corruptAttemptsExhausted(&h, capTestPeer), "2/3 is below the cap")
	require.Equal(t, 3, u.recordCorruptAttempt(&h, capTestPeer))
	require.True(t, u.corruptAttemptsExhausted(&h, capTestPeer), "3/3 reaches the cap -> cooldown")

	// A different hash has its own independent budget.
	other := chainhash.HashH([]byte("corrupt-other"))
	require.False(t, u.corruptAttemptsExhausted(&other, capTestPeer))

	// Cooldown reset (simulating the fixed window expiring): the hash can be retried, proving the
	// cap rate-limits rather than condemns.
	u.blockCorruptAttempts.Delete(ck(h, capTestPeer))
	require.False(t, u.corruptAttemptsExhausted(&h, capTestPeer), "after the window expires the hash is retriable again (self-heal)")

	// Cap disabled (<= 0) never exhausts, regardless of attempt count (re-opens the DoS by design).
	u.settings.BlockValidation.MaxCorruptAttemptsPerBlock = 0
	for i := 0; i < 10; i++ {
		u.recordCorruptAttempt(&h, capTestPeer)
	}
	require.False(t, u.corruptAttemptsExhausted(&h, capTestPeer), "cap <= 0 disables the bound")
}

// TestAccountCorruptAttempt is the C1 fix: accountCorruptAttempt is the SINGLE accounting point in
// processBlockFound that both the worker route and the direct ProcessBlock gRPC route funnel
// through (bitcoin-sv/teranode#4692). A corrupt validation outcome records toward the cap; a genuine
// success clears it; a non-corrupt error leaves the counter untouched.
func TestAccountCorruptAttempt(t *testing.T) {
	u := newCorruptCapServer(t, 3)
	h := chainhash.HashH([]byte("account-corrupt"))

	// Corrupt outcomes accumulate toward the cap (this is the direct-ProcessBlock route that the
	// worker-only increment used to miss).
	u.accountCorruptAttempt(&h, capTestPeer, errors.NewBlockCorruptError("corrupt body"))
	u.accountCorruptAttempt(&h, capTestPeer, errors.NewBlockCorruptError("corrupt body"))
	require.False(t, u.corruptAttemptsExhausted(&h, capTestPeer), "2/3 is below the cap")
	u.accountCorruptAttempt(&h, capTestPeer, errors.NewBlockCorruptError("corrupt body"))
	require.True(t, u.corruptAttemptsExhausted(&h, capTestPeer), "3/3 reaches the cap")

	// A non-corrupt error must not touch the counter (stays exhausted).
	u.accountCorruptAttempt(&h, capTestPeer, errors.NewProcessingError("transient"))
	require.True(t, u.corruptAttemptsExhausted(&h, capTestPeer), "a non-corrupt error must not change the corrupt counter")

	// A genuine success clears it.
	u.accountCorruptAttempt(&h, capTestPeer, nil)
	require.False(t, u.corruptAttemptsExhausted(&h, capTestPeer), "a validation success clears the corrupt counter")
	require.Nil(t, u.blockCorruptAttempts.Get(ck(h, capTestPeer)))
}

// TestCorruptAttemptCap_NilCacheSafe verifies the helpers degrade gracefully when the cache was
// never initialised (a Server literal built without NewServer wiring): no panic, no cap. This is
// what makes the cap ban-score-independent AND safe in minimal deployments.
func TestCorruptAttemptCap_NilCacheSafe(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	u := &Server{settings: tSettings, logger: ulogger.TestLogger{}} // blockCorruptAttempts == nil

	h := chainhash.HashH([]byte("corrupt-nil"))
	require.NotPanics(t, func() {
		require.Equal(t, 0, u.recordCorruptAttempt(&h, capTestPeer))
		require.False(t, u.corruptAttemptsExhausted(&h, capTestPeer))
	})
}

// TestCorruptAttemptCap_WindowAnchoredToFirstFailure proves the fixed-window / no-wedge property
// (bitcoin-sv/teranode#4692, C3): the cooldown window runs from the FIRST corrupt failure and a
// repeat failure must not extend it, so a persistent attacker cannot suppress an honest body
// forever.
func TestCorruptAttemptCap_WindowAnchoredToFirstFailure(t *testing.T) {
	u := newCorruptCapServer(t, 5)
	h := chainhash.HashH([]byte("corrupt-window"))

	// Seed a first failure whose window is already partway through (30s remaining stands in for
	// "the original corrupt happened a while ago").
	u.blockCorruptAttempts.Set(ck(h, capTestPeer), 1, 30*time.Second)

	require.Equal(t, 2, u.recordCorruptAttempt(&h, capTestPeer))

	item := u.blockCorruptAttempts.Get(ck(h, capTestPeer))
	require.NotNil(t, item, "entry must still exist")
	require.Less(t, time.Until(item.ExpiresAt()), 2*time.Minute,
		"repeat failure must preserve the original (~30s) window, not reset it to a fresh ~10m one")
}

// TestCorruptAttemptCap_ClearedOnSuccess proves a recovering hash does not carry its accumulated
// corrupt count into a later, unrelated corruption (bitcoin-sv/teranode#4692): success clears the
// counter and its window.
func TestCorruptAttemptCap_ClearedOnSuccess(t *testing.T) {
	u := newCorruptCapServer(t, 3)
	h := chainhash.HashH([]byte("corrupt-recover"))

	require.Equal(t, 1, u.recordCorruptAttempt(&h, capTestPeer))
	require.Equal(t, 2, u.recordCorruptAttempt(&h, capTestPeer))
	require.Equal(t, 3, u.recordCorruptAttempt(&h, capTestPeer))
	require.True(t, u.corruptAttemptsExhausted(&h, capTestPeer), "3/3 reaches the cap")

	u.clearCorruptAttempts(&h, capTestPeer)

	require.Nil(t, u.blockCorruptAttempts.Get(ck(h, capTestPeer)), "counter must be gone after success")
	require.False(t, u.corruptAttemptsExhausted(&h, capTestPeer))
	require.Equal(t, 1, u.recordCorruptAttempt(&h, capTestPeer), "next corruption starts a fresh count")
}

// TestCorruptAttemptCap_HonestPeerNotWedged is the headline of the (hash, peerID) re-key
// (bitcoin-sv/teranode#4692, ordishs' NEW finding): a bad peer that exhausts its budget for a hash
// must NOT suppress that same hash from an honest peer. Keying on (hash, peerID) gives the honest
// peer a fresh budget, so the honest tip is never wedged.
func TestCorruptAttemptCap_HonestPeerNotWedged(t *testing.T) {
	u := newCorruptCapServer(t, 3)
	h := chainhash.HashH([]byte("honest-not-wedged"))

	const badPeer, honestPeer = "peerBad", "peerHonest"

	// The bad peer burns its whole budget on the honest tip hash.
	for i := 0; i < 3; i++ {
		u.recordCorruptAttempt(&h, badPeer)
	}
	require.True(t, u.corruptAttemptsExhausted(&h, badPeer), "the bad peer is capped for this hash")

	// The honest peer serving the SAME hash still has a full, independent budget — it is not wedged.
	require.False(t, u.corruptAttemptsExhausted(&h, honestPeer),
		"an honest peer must keep a fresh budget for the same hash (no honest-tip wedge)")
}

// TestCorruptAttemptCap_MultipleBadPeersIndependent proves each serving identity is capped
// independently (bitcoin-sv/teranode#4692, C4): N bad peers on one hash each accrue their own count;
// the aggregate is peers x cap and no single peer's delivery exceeds its own budget.
func TestCorruptAttemptCap_MultipleBadPeersIndependent(t *testing.T) {
	u := newCorruptCapServer(t, 3)
	h := chainhash.HashH([]byte("multi-bad-peers"))

	peers := []string{"peer1", "peer2", "peer3"}
	for _, p := range peers {
		require.Equal(t, 1, u.recordCorruptAttempt(&h, p), "each peer starts its own count at 1")
		require.Equal(t, 2, u.recordCorruptAttempt(&h, p))
		require.Equal(t, 3, u.recordCorruptAttempt(&h, p))
		require.True(t, u.corruptAttemptsExhausted(&h, p), "peer %s is independently capped", p)
	}

	// Each bucket holds exactly the cap — no cross-peer contamination inflated any single counter.
	for _, p := range peers {
		item := u.blockCorruptAttempts.Get(ck(h, p))
		require.NotNil(t, item)
		require.Equal(t, 3, item.Value(), "peer %s counted only its own deliveries", p)
	}
}

// TestCorruptAttemptCap_EmptyPeerIDSharedBucket covers the degraded deployment (bitcoin-sv/teranode#4692,
// C4): when the serving identity is empty (p2pClient nil / peer supplied no ID), all empty-identity
// deliveries share one (hash, "") bucket — the hard per-hash bound for that deployment — so the cap
// still holds. A distinct real identity is unaffected by the empty bucket.
func TestCorruptAttemptCap_EmptyPeerIDSharedBucket(t *testing.T) {
	u := newCorruptCapServer(t, 3)
	h := chainhash.HashH([]byte("empty-peer-bucket"))

	require.Equal(t, 1, u.recordCorruptAttempt(&h, ""))
	require.Equal(t, 2, u.recordCorruptAttempt(&h, ""))
	require.Equal(t, 3, u.recordCorruptAttempt(&h, ""))
	require.True(t, u.corruptAttemptsExhausted(&h, ""), "empty-identity deliveries share one bucket that still caps")

	// A named peer is a different bucket, unaffected by the exhausted empty bucket.
	require.False(t, u.corruptAttemptsExhausted(&h, "peerNamed"))
}
