package validator

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/stretchr/testify/require"
)

// benchRangeSource hands out ranges with no network, so the benchmark's flag-on
// figure excludes the background round trip to the blockchain service.
type benchRangeSource struct {
	mu   sync.Mutex
	high uint64
}

func (s *benchRangeSource) ReserveLaneRange(context.Context, string, uint8, bool) (lane.Range, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	r := lane.Range{Lane: 7, First: s.high + 1, Last: s.high + 10_000_000}
	s.high = r.Last

	return r, nil
}

// BenchmarkValidateNewTxLanePosition is BenchmarkValidateNewTx with lane positions
// on: the store takes its positions from an allocator, as the daemon sets it up.
func BenchmarkValidateNewTxLanePosition(b *testing.B) {
	runValidateNewTxBench(b, func(s utxo.Store) utxo.Store {
		a := lane.NewAllocator(&benchRangeSource{}, "bench", filepath.Join(b.TempDir(), "lane"), nil)
		b.Cleanup(func() { require.NoError(b, a.Close()) })

		taker, ok := utxo.As[utxo.LanePositionTaker](s)
		require.True(b, ok)
		taker.SetLanePositionSource(a)

		return s
	})
}
