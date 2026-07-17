package settings

import (
	"testing"
	"time"

	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// TestOutpointBatcherMaxConcurrent_LoaderReadsKey guards the per-batcher
// concurrency override (#1187) against the field-exists-but-loader-never-reads-it
// bug: the field has a `key:` tag, but if NewSettings() does not call getInt for
// it the value stays at the Go zero value and the documented setting is silently
// unreadable.
//
// Default 0 == Go zero (0 = inherit the shared utxostore_batcherMaxConcurrent),
// so a default-value assertion alone would pass spuriously. The honest test is:
// set a non-zero override, call NewSettings(), assert the field changed.
func TestOutpointBatcherMaxConcurrent_LoaderReadsKey(t *testing.T) {
	const key = "utxostore_outpointBatcherMaxConcurrent"

	// gocore resolves key.<context> first, stripping suffixes down to the base
	// key. A runtime Set on the base key is therefore shadowed by any settings.conf
	// context override (docker.m pins this key to 16), which is what made the old
	// absolute-value assertions context-fragile. Set at the precedence that wins
	// under the *ambient* context so the test is hermetic in dev, docker.m, etc.
	ctx := gocore.Config().GetContext()
	winKey := key
	if ctx != "" {
		winKey = key + "." + ctx
	}

	// Default-contract guard (0 = inherit shared cap; shared cap unchanged) — only
	// meaningful under a context that carries no .conf override for these keys.
	if ctx == "" || ctx == "dev" {
		def := NewSettings()
		require.Equal(t, 0, def.UtxoStore.OutpointBatcherMaxConcurrent, "default must be 0 (inherit the shared cap)")
		require.Equal(t, 64, def.UtxoStore.BatcherMaxConcurrent, "shared cap default unchanged")
	}

	// Loader-wiring guard (all contexts): a distinctive value set at the winning
	// precedence must be read back — catches the field-exists-but-loader-never-
	// reads-it bug regardless of ambient context.
	gocore.Config().Set(winKey, "123")
	t.Cleanup(func() { gocore.Config().Set(winKey, "") })

	require.Equal(t, 123, NewSettings().UtxoStore.OutpointBatcherMaxConcurrent,
		"loader must read %s under context %q", key, ctx)
}

// TestLegacyBlockFailureBackoff_Defaults guards the loader entries for the
// block-level backoff durations (#1187). These have non-zero defaults, so a
// missing getDuration in NewSettings() would leave them at 0 — disabling the
// backoff (base 0) and giving the failure-tracking map a 0 TTL.
func TestLegacyBlockFailureBackoff_Defaults(t *testing.T) {
	tSettings := NewSettings()

	require.NotNil(t, tSettings)
	require.Equal(t, 5*time.Second, tSettings.Legacy.BlockFailureBackoffBase,
		"default BlockFailureBackoffBase must be 5s; a zero value disables the per-block backoff")
	require.Equal(t, 150*time.Second, tSettings.Legacy.BlockFailureBackoffMaxDuration,
		"default BlockFailureBackoffMaxDuration must be 150s; a zero value gives the failure map a 0 TTL")
	require.Less(t, tSettings.Legacy.BlockFailureBackoffMaxDuration, 180*time.Second,
		"backoff cap must stay below the 180s sync-peer stall window (maxLastBlockTime)")
}
