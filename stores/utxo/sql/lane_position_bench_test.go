package sql

import (
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/util/lane"
)

// BenchmarkCreateOnlyLanePosition is BenchmarkCreateOnly with a lane position on
// every record: the store's own cost of lane positions being on.
func BenchmarkCreateOnlyLanePosition(b *testing.B) {
	stamped := func(i int) []utxo.CreateOption {
		return []utxo.CreateOption{utxo.WithLanePosition(lane.Pack(7, uint64(i)+1))} //nolint:gosec // i counts up from 0
	}

	b.Run("sqlite", func(b *testing.B) { runCreateBench(b, benchSQLStore(b, false), 0xc4, stamped) })
	b.Run("postgres", func(b *testing.B) { runCreateBench(b, benchSQLStore(b, true), 0xc5, stamped) })
}
