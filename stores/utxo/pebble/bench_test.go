package pebble

import (
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	pebbledb "github.com/cockroachdb/pebble/v2"
)

func BenchmarkSuite(b *testing.B) {
	store := newTestStore(b)
	tests.Benchmark(b, store)
}

func BenchmarkSuiteNoSync(b *testing.B) {
	store := newTestStore(b)
	store.sync = pebbledb.NoSync
	tests.Benchmark(b, store)
}
