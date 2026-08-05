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
		blockCorruptAttempts: expiringmap.New[chainhash.Hash, *corruptAttemptState](legacyCorruptAttemptCooldown(tSettings)),
	}
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

	require.False(t, sm.corruptBlockAttemptsExhausted(h), "zero attempts is not exhausted")
	require.Equal(t, 1, sm.recordCorruptBlockAttempt(h))
	require.False(t, sm.corruptBlockAttemptsExhausted(h), "1/3 is below the cap")
	require.Equal(t, 2, sm.recordCorruptBlockAttempt(h))
	require.False(t, sm.corruptBlockAttemptsExhausted(h), "2/3 is below the cap")
	require.Equal(t, 3, sm.recordCorruptBlockAttempt(h))
	require.True(t, sm.corruptBlockAttemptsExhausted(h), "3/3 reaches the cap -> cooldown")

	// A different hash has its own independent budget.
	other := chainhash.HashH([]byte("legacy-corrupt-other"))
	require.False(t, sm.corruptBlockAttemptsExhausted(other))

	// Cap disabled (<= 0) never exhausts.
	sm.settings.BlockValidation.MaxCorruptAttemptsPerBlock = 0
	for i := 0; i < 10; i++ {
		sm.recordCorruptBlockAttempt(h)
	}
	require.False(t, sm.corruptBlockAttemptsExhausted(h), "cap <= 0 disables the bound")
}

// TestLegacyCorruptAttemptCap_NilSafe verifies the helpers degrade gracefully when the map was
// never initialised (a SyncManager struct literal that bypasses New()): no panic, no cap.
func TestLegacyCorruptAttemptCap_NilSafe(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	sm := &SyncManager{logger: ulogger.TestLogger{}, settings: tSettings} // blockCorruptAttempts == nil

	h := chainhash.HashH([]byte("legacy-corrupt-nil"))
	require.NotPanics(t, func() {
		require.Equal(t, 0, sm.recordCorruptBlockAttempt(h))
		require.False(t, sm.corruptBlockAttemptsExhausted(h))
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
	sm.blockCorruptAttempts.Set(h, &corruptAttemptState{count: 1, windowExpiry: time.Now().Add(-time.Second)})

	// A delivery after the window has lapsed resets the count and starts a fresh window.
	require.Equal(t, 1, sm.recordCorruptBlockAttempt(h), "an expired window resets the count (self-heal)")

	st, ok := sm.blockCorruptAttempts.Get(h)
	require.True(t, ok)
	require.True(t, st.windowExpiry.After(time.Now()), "a fresh window is set from the new first failure")
}

// TestLegacyCorruptAttemptCap_ClearedOnSuccess proves a recovering hash does not carry a stale
// corrupt count (bitcoin-sv/teranode#4692): a successful store clears the counter.
func TestLegacyCorruptAttemptCap_ClearedOnSuccess(t *testing.T) {
	sm := newCorruptCapSyncManager(t, 3)
	h := chainhash.HashH([]byte("legacy-corrupt-recover"))

	require.Equal(t, 1, sm.recordCorruptBlockAttempt(h))
	require.Equal(t, 2, sm.recordCorruptBlockAttempt(h))
	require.Equal(t, 3, sm.recordCorruptBlockAttempt(h))
	require.True(t, sm.corruptBlockAttemptsExhausted(h), "3/3 reaches the cap")

	sm.clearCorruptBlockAttempts(h)

	_, ok := sm.blockCorruptAttempts.Get(h)
	require.False(t, ok, "counter must be gone after success")
	require.False(t, sm.corruptBlockAttemptsExhausted(h))
	require.Equal(t, 1, sm.recordCorruptBlockAttempt(h), "next corruption starts a fresh count")
}
