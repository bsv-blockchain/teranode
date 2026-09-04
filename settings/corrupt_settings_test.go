package settings

import (
	"testing"
	"time"

	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// TestCorruptBodySettings_LoaderReadsKeys guards the three corrupt-body settings
// (bitcoin-sv/teranode#4692) against the field-exists-but-loader-never-reads-it bug: each field
// carries a `key:`/`default:` struct tag, but a tag is documentation only — the field is populated
// ONLY by an explicit getBool/getInt/getDuration call in NewSettings(). Without those calls all
// three sit at their Go zero value and both headline fixes are inert (the cap gate returns false
// forever; the optimistic peer-blocks opt-in can never be true).
//
// A defaults-only assertion cannot catch this: OptimisticMiningPeerBlocks's documented default is
// false, which equals the Go zero, so a missing getBool would still "pass". The honest test is to
// set all three to NON-default values through the real settings path, call NewSettings(), and assert
// each arrived. A defaults check is kept too, but it is insufficient alone.
func TestCorruptBodySettings_LoaderReadsKeys(t *testing.T) {
	const (
		keyOptimisticPeer = "blockvalidation_optimistic_mining_peer_blocks"
		keyMaxAttempts    = "blockvalidation_max_corrupt_attempts_per_block"
		keyCooldown       = "blockvalidation_corrupt_attempt_cooldown"
	)

	// gocore resolves key.<context> first, stripping suffixes down to the base key. Set at the
	// precedence that wins under the ambient context so the test is hermetic in dev, docker.m, etc.
	ctx := gocore.Config().GetContext()
	winKey := func(key string) string {
		if ctx != "" {
			return key + "." + ctx
		}
		return key
	}

	// Default-contract guard — only meaningful under a context that carries no .conf override for
	// these keys. Insufficient ALONE (OptimisticMiningPeerBlocks default false == Go zero), kept as a
	// documentation guard only.
	if ctx == "" || ctx == "dev" {
		def := NewSettings()
		require.False(t, def.BlockValidation.OptimisticMiningPeerBlocks, "default OptimisticMiningPeerBlocks must be false")
		require.Equal(t, 3, def.BlockValidation.MaxCorruptAttemptsPerBlock, "default MaxCorruptAttemptsPerBlock must be 3")
		require.Equal(t, DefaultCorruptAttemptCooldown, def.BlockValidation.CorruptAttemptCooldown, "default CorruptAttemptCooldown must be 10m")
	}

	// Loader-wiring guard (all contexts): distinctive NON-default values set at the winning
	// precedence must be read back. This is what catches a missing getBool for OptimisticMiningPeerBlocks.
	gocore.Config().Set(winKey(keyOptimisticPeer), "true")
	gocore.Config().Set(winKey(keyMaxAttempts), "7")
	gocore.Config().Set(winKey(keyCooldown), "90s")
	t.Cleanup(func() {
		gocore.Config().Set(winKey(keyOptimisticPeer), "")
		gocore.Config().Set(winKey(keyMaxAttempts), "")
		gocore.Config().Set(winKey(keyCooldown), "")
	})

	s := NewSettings()
	require.True(t, s.BlockValidation.OptimisticMiningPeerBlocks,
		"loader must read %s (a missing getBool leaves it false — the inert-fix bug)", keyOptimisticPeer)
	require.Equal(t, 7, s.BlockValidation.MaxCorruptAttemptsPerBlock,
		"loader must read %s (a missing getInt leaves it 0 — the cap gate then never fires)", keyMaxAttempts)
	require.Equal(t, 90*time.Second, s.BlockValidation.CorruptAttemptCooldown,
		"loader must read %s (a missing getDuration leaves it 0)", keyCooldown)
}
