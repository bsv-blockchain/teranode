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
	// Default must be 0 (inherit the shared cap) so behaviour is byte-identical
	// to the pre-#1187 code until explicitly tuned; the shared knob is unchanged.
	def := NewSettings()
	require.Equal(t, 0, def.UtxoStore.OutpointBatcherMaxConcurrent)
	require.Equal(t, 64, def.UtxoStore.BatcherMaxConcurrent)

	const key = "utxostore_outpointBatcherMaxConcurrent"
	gocore.Config().Set(key, "14")
	t.Cleanup(func() { gocore.Config().Set(key, "") })

	s := NewSettings()
	require.Equal(t, 14, s.UtxoStore.OutpointBatcherMaxConcurrent)
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
