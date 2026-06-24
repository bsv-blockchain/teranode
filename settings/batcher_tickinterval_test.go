package settings

import (
	"testing"

	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// TestBatcherTickInterval_LoaderReadsAllKeys guards the per-batcher
// SetTickInterval (go-batcher v2.0.3 fixed-cadence flushing) settings against
// the field-exists-but-loader-never-reads-it bug: each field has a `key:` tag,
// but if NewSettings() does not call getInt for it the value stays at Go zero
// and the documented setting is silently unreadable.
//
// Default 0 == Go zero, so a default-value assertion alone would pass
// spuriously. The honest test is: set a non-zero override, call NewSettings(),
// assert the field changed.
func TestBatcherTickInterval_LoaderReadsAllKeys(t *testing.T) {
	type kv struct {
		key      string
		override string
		check    func(t *testing.T, s *Settings)
	}

	cases := []kv{
		{
			key:      "utxostore_storeBatcherTickerIntervalMillis",
			override: "11",
			check:    func(t *testing.T, s *Settings) { require.Equal(t, 11, s.UtxoStore.StoreBatcherTickerIntervalMillis) },
		},
		{
			key:      "utxostore_getBatcherTickerIntervalMillis",
			override: "12",
			check:    func(t *testing.T, s *Settings) { require.Equal(t, 12, s.UtxoStore.GetBatcherTickerIntervalMillis) },
		},
		{
			key:      "utxostore_spendBatcherTickerIntervalMillis",
			override: "13",
			check:    func(t *testing.T, s *Settings) { require.Equal(t, 13, s.UtxoStore.SpendBatcherTickerIntervalMillis) },
		},
		{
			key:      "utxostore_outpointBatcherTickerIntervalMillis",
			override: "14",
			check:    func(t *testing.T, s *Settings) { require.Equal(t, 14, s.UtxoStore.OutpointBatcherTickerIntervalMillis) },
		},
		{
			key:      "utxostore_incrementBatcherTickerIntervalMillis",
			override: "15",
			check: func(t *testing.T, s *Settings) {
				require.Equal(t, 15, s.UtxoStore.IncrementBatcherTickerIntervalMillis)
			},
		},
		{
			key:      "utxostore_setDAHBatcherTickerIntervalMillis",
			override: "16",
			check:    func(t *testing.T, s *Settings) { require.Equal(t, 16, s.UtxoStore.SetDAHBatcherTickerIntervalMillis) },
		},
		{
			key:      "utxostore_lockedBatcherTickerIntervalMillis",
			override: "17",
			check:    func(t *testing.T, s *Settings) { require.Equal(t, 17, s.UtxoStore.LockedBatcherTickerIntervalMillis) },
		},
		{
			key:      "blockassembly_sendBatchTickerIntervalMillis",
			override: "18",
			check:    func(t *testing.T, s *Settings) { require.Equal(t, 18, s.BlockAssembly.SendBatchTickerIntervalMillis) },
		},
		{
			key:      "validator_sendBatchTickerIntervalMillis",
			override: "19",
			check:    func(t *testing.T, s *Settings) { require.Equal(t, 19, s.Validator.SendBatchTickerIntervalMillis) },
		},
		{
			key:      "validator_txmeta_kafka_batchTickerIntervalMillis",
			override: "20",
			check: func(t *testing.T, s *Settings) {
				require.Equal(t, 20, s.Validator.TxMetaKafkaBatchTickerIntervalMillis)
			},
		},
	}

	// All keys must default to 0 (disabled) so behaviour is unchanged until tuned.
	def := NewSettings()
	require.Equal(t, 0, def.UtxoStore.StoreBatcherTickerIntervalMillis)
	require.Equal(t, 0, def.UtxoStore.GetBatcherTickerIntervalMillis)
	require.Equal(t, 0, def.UtxoStore.SpendBatcherTickerIntervalMillis)
	require.Equal(t, 0, def.UtxoStore.OutpointBatcherTickerIntervalMillis)
	require.Equal(t, 0, def.UtxoStore.IncrementBatcherTickerIntervalMillis)
	require.Equal(t, 0, def.UtxoStore.SetDAHBatcherTickerIntervalMillis)
	require.Equal(t, 0, def.UtxoStore.LockedBatcherTickerIntervalMillis)
	require.Equal(t, 0, def.BlockAssembly.SendBatchTickerIntervalMillis)
	require.Equal(t, 0, def.Validator.SendBatchTickerIntervalMillis)
	require.Equal(t, 0, def.Validator.TxMetaKafkaBatchTickerIntervalMillis)

	for _, tc := range cases {
		t.Run(tc.key, func(t *testing.T) {
			gocore.Config().Set(tc.key, tc.override)
			t.Cleanup(func() { gocore.Config().Set(tc.key, "") })

			s := NewSettings()
			tc.check(t, s)
		})
	}
}
