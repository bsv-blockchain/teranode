package aerospike_test

import (
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
)

// TestGetSpendHashMismatch runs the shared parity case: a caller-supplied UTXO hash that is not the
// stored one gives ErrUtxoHashMismatch, as on the SQL and pebble stores.
func TestGetSpendHashMismatch(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	_, store, _, deferFn := initAerospike(t, tSettings, ulogger.NewErrorTestLogger(t))
	t.Cleanup(deferFn)

	tests.GetSpendHashMismatch(t, store)
}
