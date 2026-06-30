package settings

import (
	"testing"
	"time"

	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// TestPerBatcherMaxConcurrent_LoaderReadsAllKeys guards the per-batcher
// concurrency override settings (#1187) against the field-exists-but-loader-
// never-reads-it bug: each field has a `key:` tag, but if NewSettings() does not
// call getInt for it the value stays at the Go zero value and the documented
// setting is silently unreadable.
//
// Default 0 == Go zero (0 = inherit the shared utxostore_batcherMaxConcurrent),
// so a default-value assertion alone would pass spuriously. The honest test is:
// set a non-zero override, call NewSettings(), assert the field changed.
func TestPerBatcherMaxConcurrent_LoaderReadsAllKeys(t *testing.T) {
	type kv struct {
		key      string
		override string
		check    func(t *testing.T, s *Settings)
	}

	cases := []kv{
		{
			key:      "utxostore_storeBatcherMaxConcurrent",
			override: "11",
			check:    func(t *testing.T, s *Settings) { require.Equal(t, 11, s.UtxoStore.StoreBatcherMaxConcurrent) },
		},
		{
			key:      "utxostore_getBatcherMaxConcurrent",
			override: "12",
			check:    func(t *testing.T, s *Settings) { require.Equal(t, 12, s.UtxoStore.GetBatcherMaxConcurrent) },
		},
		{
			key:      "utxostore_spendBatcherMaxConcurrent",
			override: "13",
			check:    func(t *testing.T, s *Settings) { require.Equal(t, 13, s.UtxoStore.SpendBatcherMaxConcurrent) },
		},
		{
			key:      "utxostore_outpointBatcherMaxConcurrent",
			override: "14",
			check:    func(t *testing.T, s *Settings) { require.Equal(t, 14, s.UtxoStore.OutpointBatcherMaxConcurrent) },
		},
		{
			key:      "utxostore_incrementBatcherMaxConcurrent",
			override: "15",
			check:    func(t *testing.T, s *Settings) { require.Equal(t, 15, s.UtxoStore.IncrementBatcherMaxConcurrent) },
		},
		{
			key:      "utxostore_setDAHBatcherMaxConcurrent",
			override: "16",
			check:    func(t *testing.T, s *Settings) { require.Equal(t, 16, s.UtxoStore.SetDAHBatcherMaxConcurrent) },
		},
		{
			key:      "utxostore_lockedBatcherMaxConcurrent",
			override: "17",
			check:    func(t *testing.T, s *Settings) { require.Equal(t, 17, s.UtxoStore.LockedBatcherMaxConcurrent) },
		},
	}

	// All per-batcher overrides must default to 0 (inherit the shared cap) so
	// behaviour is byte-identical to the pre-split code until explicitly tuned.
	def := NewSettings()
	require.Equal(t, 0, def.UtxoStore.StoreBatcherMaxConcurrent)
	require.Equal(t, 0, def.UtxoStore.GetBatcherMaxConcurrent)
	require.Equal(t, 0, def.UtxoStore.SpendBatcherMaxConcurrent)
	require.Equal(t, 0, def.UtxoStore.OutpointBatcherMaxConcurrent)
	require.Equal(t, 0, def.UtxoStore.IncrementBatcherMaxConcurrent)
	require.Equal(t, 0, def.UtxoStore.SetDAHBatcherMaxConcurrent)
	require.Equal(t, 0, def.UtxoStore.LockedBatcherMaxConcurrent)
	// The shared knob default is unchanged.
	require.Equal(t, 64, def.UtxoStore.BatcherMaxConcurrent)

	for _, tc := range cases {
		t.Run(tc.key, func(t *testing.T) {
			gocore.Config().Set(tc.key, tc.override)
			t.Cleanup(func() { gocore.Config().Set(tc.key, "") })

			s := NewSettings()
			tc.check(t, s)
		})
	}
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
	require.Equal(t, 5*time.Minute, tSettings.Legacy.BlockFailureBackoffMaxDuration,
		"default BlockFailureBackoffMaxDuration must be 5m; a zero value gives the failure map a 0 TTL")
}
