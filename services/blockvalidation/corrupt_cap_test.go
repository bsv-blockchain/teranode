package blockvalidation

import (
	"strconv"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
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
		blockCorruptAttempts: ttlcache.New[blockAttemptKey, int](
			ttlcache.WithTTL[blockAttemptKey, int](10*time.Minute),
			ttlcache.WithDisableTouchOnHit[blockAttemptKey, int](),
			ttlcache.WithCapacity[blockAttemptKey, int](corruptAttemptsMaxTracked),
		),
		blockPolicyDeclineAttempts: ttlcache.New[blockAttemptKey, int](
			ttlcache.WithTTL[blockAttemptKey, int](10*time.Minute),
			ttlcache.WithDisableTouchOnHit[blockAttemptKey, int](),
			ttlcache.WithCapacity[blockAttemptKey, int](corruptAttemptsMaxTracked),
		),
	}
}

// capTestPeer is the single serving peer used by the single-peer cap tests below; the cap keys on
// (hash, peerID) (bitcoin-sv/teranode#4692), so a fixed identity exercises the per-identity budget.
const capTestPeer = "peerA"

// ck builds the (hash, peerID) cache key for direct cache assertions in the tests.
func ck(h chainhash.Hash, peerID string) blockAttemptKey {
	return blockAttemptKey{hash: h, peerID: peerID}
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

// TestAccountCorruptAttempt pins the single accounting point (bitcoin-sv/teranode#4692):
// accountCorruptAttempt is the ONE point in
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
// (bitcoin-sv/teranode#4692): the cooldown window runs from the FIRST corrupt failure and a
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

// TestCorruptAttemptCap_EmptyPeerIDUncapped pins the fail-open direction for an empty serving
// identity (bitcoin-sv/teranode#4692): an empty peerID is NOT capped. The record helpers refuse to
// store under the empty key (both the accountCorruptAttempt outcome path AND recordCorruptAttempt
// called directly), and the gate never reports an empty identity exhausted — so the cap-hit
// suppression that returns a corrupt error can never fire on an unidentified delivery and wedge an
// honest tip. The legacy route now supplies a real peer.Addr(), so a genuine per-peer cap applies
// there; the empty-identity path is deliberately uncapped.
func TestCorruptAttemptCap_EmptyPeerIDUncapped(t *testing.T) {
	u := newCorruptCapServer(t, 3)
	h := chainhash.HashH([]byte("empty-peer-uncapped"))

	// accountCorruptAttempt must not record under an empty key...
	for i := 0; i < 5; i++ {
		u.accountCorruptAttempt(&h, "", errors.NewBlockCorruptError("corrupt body"))
	}
	require.Nil(t, u.blockCorruptAttempts.Get(ck(h, "")), "accountCorruptAttempt must never record the empty key")

	// ...and recordCorruptAttempt called directly is also a no-op for the empty key (returns 0,
	// records nothing), so an empty identity can never be driven to exhaustion.
	require.Equal(t, 0, u.recordCorruptAttempt(&h, ""), "recordCorruptAttempt must not record under an empty key")
	require.Equal(t, 0, u.recordCorruptAttempt(&h, ""))
	require.Nil(t, u.blockCorruptAttempts.Get(ck(h, "")), "the empty key must still be absent")
	require.False(t, u.corruptAttemptsExhausted(&h, ""), "an empty peerID is uncapped (fail-open), never exhausted")

	// A named peer is capped normally, independent of the empty identity.
	require.False(t, u.corruptAttemptsExhausted(&h, "peerNamed"))
}

// TestCorruptAttemptCap_CacheIsBounded covers bitcoin-sv/teranode#4692: the
// per-(hash, peerID) map keys on a strictly larger space than its hash-only sibling, so it must carry
// a capacity like the legacy netsync twin does. Eviction can only LOOSEN the cap — an evicted pair
// starts a fresh window — so it can never make the node drop an honest block; the assertion below
// pins both halves: the map stays bounded, and the gate still works after an overflow.
func TestCorruptAttemptCap_CacheIsBounded(t *testing.T) {
	u := newCorruptCapServer(t, 3)

	for i := 0; i < corruptAttemptsMaxTracked*2; i++ {
		h := chainhash.HashH([]byte("bounded-cache-" + strconv.Itoa(i)))
		u.recordCorruptAttempt(&h, capTestPeer)
	}

	require.LessOrEqual(t, u.blockCorruptAttempts.Len(), corruptAttemptsMaxTracked,
		"the corrupt-attempt map must never exceed its configured capacity")

	// A pair recorded after the overflow is still gated correctly: eviction resets a window, it does
	// not break the gate.
	fresh := chainhash.HashH([]byte("bounded-cache-after-overflow"))
	for i := 0; i < 3; i++ {
		u.recordCorruptAttempt(&fresh, capTestPeer)
	}

	require.True(t, u.corruptAttemptsExhausted(&fresh, capTestPeer), "the gate still caps after an overflow")
}

// TestPolicyDeclineCap_SeparateBudget pins the two trackers as INDEPENDENT budgets
// (bitcoin-sv/teranode#4692). Sharing one bucket was rejected because every
// "corrupt" identifier, log line and metric on that path would become false for a policy decline;
// this test is what fails if a later edit merges them back.
func TestPolicyDeclineCap_SeparateBudget(t *testing.T) {
	u := newCorruptCapServer(t, 3)
	h := chainhash.HashH([]byte("separate-budgets"))

	// Policy declines up to the cap must not spend the corrupt budget.
	for i := 0; i < 3; i++ {
		u.recordPolicyDeclineAttempt(&h, capTestPeer)
	}

	require.True(t, u.policyDeclineAttemptsExhausted(&h, capTestPeer), "policy declines reach their own cap")
	require.False(t, u.corruptAttemptsExhausted(&h, capTestPeer), "policy declines must never spend the corrupt budget")

	// And the converse, on a different hash: corrupt failures must not suppress a policy-declined
	// fetch.
	other := chainhash.HashH([]byte("separate-budgets-converse"))
	for i := 0; i < 3; i++ {
		u.recordCorruptAttempt(&other, capTestPeer)
	}

	require.True(t, u.corruptAttemptsExhausted(&other, capTestPeer), "corrupt failures reach the corrupt cap")
	require.False(t, u.policyDeclineAttemptsExhausted(&other, capTestPeer), "corrupt failures must never spend the policy budget")
}

// TestPolicyDeclineCap_HonestPeerNotWedged mirrors the corrupt cap's honest-tip-wedge property for
// the policy-decline tracker. The size judged is the peer-supplied SizeInBytes varint, so a per-HASH
// suppression would let one peer inflate that varint and suppress the honest tip for the whole
// window; keying on (hash, peerID) means an inflated varint only spends that peer's own budget.
func TestPolicyDeclineCap_HonestPeerNotWedged(t *testing.T) {
	u := newCorruptCapServer(t, 3)
	h := chainhash.HashH([]byte("policy-honest-not-wedged"))

	const badPeer, honestPeer = "peerBad", "peerHonest"

	for i := 0; i < 3; i++ {
		u.recordPolicyDeclineAttempt(&h, badPeer)
	}

	require.True(t, u.policyDeclineAttemptsExhausted(&h, badPeer), "the inflating peer is capped for this hash")
	require.False(t, u.policyDeclineAttemptsExhausted(&h, honestPeer),
		"an honest peer must keep a fresh budget for the same hash (no honest-tip wedge)")
}

// TestPolicyDeclineCap_NilSafe pins the fail-open direction: a Server literal without the cache, or a
// non-positive cap, must never report "exhausted" — a missing config can never silently drop a block.
func TestPolicyDeclineCap_NilSafe(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	h := chainhash.HashH([]byte("policy-nil-safe"))

	u := &Server{settings: tSettings, logger: ulogger.TestLogger{}} // blockPolicyDeclineAttempts == nil
	require.Equal(t, 0, u.recordPolicyDeclineAttempt(&h, capTestPeer))
	require.False(t, u.policyDeclineAttemptsExhausted(&h, capTestPeer))

	disabled := newCorruptCapServer(t, 0)
	for i := 0; i < 5; i++ {
		disabled.recordPolicyDeclineAttempt(&h, capTestPeer)
	}

	require.False(t, disabled.policyDeclineAttemptsExhausted(&h, capTestPeer), "cap <= 0 disables the bound")
}

// TestExcessiveBlockSizeDeclined covers the shared predicate that keeps the RUNNING pre-fetch gate
// and the authoritative decline in ValidateBlockWithOptions from drifting apart.
func TestExcessiveBlockSizeDeclined(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	block := &model.Block{SizeInBytes: 1000}

	tSettings.Policy.ExcessiveBlockSize = 0
	require.False(t, excessiveBlockSizeDeclined(tSettings, block), "0 means unlimited")

	tSettings.Policy.ExcessiveBlockSize = 1000
	require.False(t, excessiveBlockSizeDeclined(tSettings, block), "the limit is inclusive")

	tSettings.Policy.ExcessiveBlockSize = 999
	require.True(t, excessiveBlockSizeDeclined(tSettings, block))

	require.False(t, excessiveBlockSizeDeclined(nil, block), "nil settings never declines")
	require.False(t, excessiveBlockSizeDeclined(tSettings, nil), "nil block never declines")
}
