package netsync

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// newCorruptCapSyncManager builds a SyncManager with just the legacy corrupt-cap machinery wired,
// mirroring the New() construction (retention == cooldown window).
func newCorruptCapSyncManager(t *testing.T, cap int) *SyncManager {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.MaxCorruptAttemptsPerBlock = cap

	return &SyncManager{
		logger:               ulogger.TestLogger{},
		settings:             tSettings,
		blockCorruptAttempts: expiringmap.New[legacyCorruptAttemptKey, *corruptAttemptState](legacyCorruptAttemptCooldown(tSettings)),
	}
}

// legacyCapTestPeer is the single serving peer address used by the single-peer cap tests; the cap
// keys on (hash, peerID) (bitcoin-sv/teranode#4692), so a fixed identity exercises its budget.
const legacyCapTestPeer = "peerA"

// lck builds the legacy (hash, peerID) map key for direct map assertions in the tests.
func lck(h chainhash.Hash, peerID string) legacyCorruptAttemptKey {
	return legacyCorruptAttemptKey{hash: h, peerID: peerID}
}

// TestLegacyCorruptAttemptCooldownFallback covers the legacy cooldown helper's fallbacks
// (bitcoin-sv/teranode#4692): nil settings or a non-positive setting fall back to 10m; a positive
// setting is honoured.
func TestLegacyCorruptAttemptCooldownFallback(t *testing.T) {
	require.Equal(t, 10*time.Minute, legacyCorruptAttemptCooldown(nil), "nil settings falls back to 10m")

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.CorruptAttemptCooldown = 0
	require.Equal(t, 10*time.Minute, legacyCorruptAttemptCooldown(tSettings), "non-positive falls back to 10m")

	tSettings.BlockValidation.CorruptAttemptCooldown = 90 * time.Second
	require.Equal(t, 90*time.Second, legacyCorruptAttemptCooldown(tSettings), "a positive setting is honoured")
}

// TestLegacyCorruptAttemptCap is the item-8 legacy per-hash cap (bitcoin-sv/teranode#4692): corrupt
// re-downloads for a single block hash on the netsync path are bounded by MaxCorruptAttemptsPerBlock
// and then reported exhausted (in cooldown), independently of the ban score. A cap of <= 0 disables
// the bound.
func TestLegacyCorruptAttemptCap(t *testing.T) {
	sm := newCorruptCapSyncManager(t, 3)

	h := chainhash.HashH([]byte("legacy-corrupt-cap"))

	require.False(t, sm.corruptBlockAttemptsExhausted(h, legacyCapTestPeer), "zero attempts is not exhausted")
	require.Equal(t, 1, sm.recordCorruptBlockAttempt(h, legacyCapTestPeer))
	require.False(t, sm.corruptBlockAttemptsExhausted(h, legacyCapTestPeer), "1/3 is below the cap")
	require.Equal(t, 2, sm.recordCorruptBlockAttempt(h, legacyCapTestPeer))
	require.False(t, sm.corruptBlockAttemptsExhausted(h, legacyCapTestPeer), "2/3 is below the cap")
	require.Equal(t, 3, sm.recordCorruptBlockAttempt(h, legacyCapTestPeer))
	require.True(t, sm.corruptBlockAttemptsExhausted(h, legacyCapTestPeer), "3/3 reaches the cap -> cooldown")

	// A different hash has its own independent budget.
	other := chainhash.HashH([]byte("legacy-corrupt-other"))
	require.False(t, sm.corruptBlockAttemptsExhausted(other, legacyCapTestPeer))

	// Cap disabled (<= 0) never exhausts.
	sm.settings.BlockValidation.MaxCorruptAttemptsPerBlock = 0
	for i := 0; i < 10; i++ {
		sm.recordCorruptBlockAttempt(h, legacyCapTestPeer)
	}
	require.False(t, sm.corruptBlockAttemptsExhausted(h, legacyCapTestPeer), "cap <= 0 disables the bound")
}

// TestLegacyCorruptAttemptCap_NilSafe verifies the helpers degrade gracefully when the map was
// never initialised (a SyncManager struct literal that bypasses New()): no panic, no cap.
func TestLegacyCorruptAttemptCap_NilSafe(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	sm := &SyncManager{logger: ulogger.TestLogger{}, settings: tSettings} // blockCorruptAttempts == nil

	h := chainhash.HashH([]byte("legacy-corrupt-nil"))
	require.NotPanics(t, func() {
		require.Equal(t, 0, sm.recordCorruptBlockAttempt(h, legacyCapTestPeer))
		require.False(t, sm.corruptBlockAttemptsExhausted(h, legacyCapTestPeer))
	})
}

// TestLegacyCorruptAttemptCap_WindowNotExtended proves the fixed-window property
// (bitcoin-sv/teranode#4692): the cooldown window is set once from the first corrupt delivery and a
// repeat delivery must not push it out, so a persistent attacker cannot suppress an honest body
// forever. Once the window lapses the counter resets (self-heal).
func TestLegacyCorruptAttemptCap_WindowNotExtended(t *testing.T) {
	sm := newCorruptCapSyncManager(t, 5)
	h := chainhash.HashH([]byte("legacy-corrupt-window"))

	// Seed a first failure whose logical window is already essentially expired.
	sm.blockCorruptAttempts.Set(lck(h, legacyCapTestPeer), &corruptAttemptState{count: 1, windowExpiry: time.Now().Add(-time.Second)})

	// A delivery after the window has lapsed resets the count and starts a fresh window.
	require.Equal(t, 1, sm.recordCorruptBlockAttempt(h, legacyCapTestPeer), "an expired window resets the count (self-heal)")

	st, ok := sm.blockCorruptAttempts.Get(lck(h, legacyCapTestPeer))
	require.True(t, ok)
	require.True(t, st.windowExpiry.After(time.Now()), "a fresh window is set from the new first failure")
}

// TestLegacyCorruptAttemptCap_ClearedOnSuccess proves a recovering hash does not carry a stale
// corrupt count (bitcoin-sv/teranode#4692): a successful store clears the counter.
func TestLegacyCorruptAttemptCap_ClearedOnSuccess(t *testing.T) {
	sm := newCorruptCapSyncManager(t, 3)
	h := chainhash.HashH([]byte("legacy-corrupt-recover"))

	require.Equal(t, 1, sm.recordCorruptBlockAttempt(h, legacyCapTestPeer))
	require.Equal(t, 2, sm.recordCorruptBlockAttempt(h, legacyCapTestPeer))
	require.Equal(t, 3, sm.recordCorruptBlockAttempt(h, legacyCapTestPeer))
	require.True(t, sm.corruptBlockAttemptsExhausted(h, legacyCapTestPeer), "3/3 reaches the cap")

	sm.clearCorruptBlockAttempts(h, legacyCapTestPeer)

	_, ok := sm.blockCorruptAttempts.Get(lck(h, legacyCapTestPeer))
	require.False(t, ok, "counter must be gone after success")
	require.False(t, sm.corruptBlockAttemptsExhausted(h, legacyCapTestPeer))
	require.Equal(t, 1, sm.recordCorruptBlockAttempt(h, legacyCapTestPeer), "next corruption starts a fresh count")
}

// TestLegacyCorruptAttemptCap_HonestPeerNotWedged is the legacy-path mirror of the (hash, peerID)
// re-key (bitcoin-sv/teranode#4692): a bad sync-peer that exhausts its budget for a hash must NOT
// suppress that same hash from an honest sync-peer after rotation. Each serving address keys its own
// budget, so the honest body is never wedged.
func TestLegacyCorruptAttemptCap_HonestPeerNotWedged(t *testing.T) {
	sm := newCorruptCapSyncManager(t, 3)
	h := chainhash.HashH([]byte("legacy-honest-not-wedged"))

	const badPeer, honestPeer = "1.2.3.4:8333", "5.6.7.8:8333"

	for i := 0; i < 3; i++ {
		sm.recordCorruptBlockAttempt(h, badPeer)
	}
	require.True(t, sm.corruptBlockAttemptsExhausted(h, badPeer), "the bad peer is capped for this hash")
	require.False(t, sm.corruptBlockAttemptsExhausted(h, honestPeer),
		"an honest sync-peer must keep a fresh budget for the same hash (no honest-tip wedge)")
}

// TestLegacyCorruptAttemptCap_EmptyPeerSharedBucket covers the degraded case (bitcoin-sv/teranode#4692):
// a peer with no address degrades to a single shared (hash, "") bucket — the hard per-hash bound for
// that deployment — so the cap still holds.
func TestLegacyCorruptAttemptCap_EmptyPeerSharedBucket(t *testing.T) {
	sm := newCorruptCapSyncManager(t, 3)
	h := chainhash.HashH([]byte("legacy-empty-peer"))

	require.Equal(t, 1, sm.recordCorruptBlockAttempt(h, ""))
	require.Equal(t, 2, sm.recordCorruptBlockAttempt(h, ""))
	require.Equal(t, 3, sm.recordCorruptBlockAttempt(h, ""))
	require.True(t, sm.corruptBlockAttemptsExhausted(h, ""), "empty-identity deliveries share one bucket that still caps")
	require.False(t, sm.corruptBlockAttemptsExhausted(h, "9.9.9.9:8333"), "a named peer is a different, unaffected bucket")
}
