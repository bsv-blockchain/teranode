package teranodecli

import (
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

func TestLoadUnminedBenchmarkRefusesLaneModeBeforeStoreOpen(t *testing.T) {
	const key = "utxostore_laneMigration"
	old, had := gocore.Config().Get(key)
	gocore.Config().Set(key, "shadow")
	t.Cleanup(func() {
		if had {
			gocore.Config().Set(key, old)
		} else {
			gocore.Config().Unset(key)
		}
	})
	// Parsing this URL would fail with a processing error. Lane mode must be
	// refused first, without connecting to or populating an Aerospike store.
	err := runLoadUnminedBenchmark(1, "", "", "aerospike://db host/utxo")
	require.ErrorIs(t, err, errors.ErrConfiguration)
}
