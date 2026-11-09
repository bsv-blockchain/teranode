package validator

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/lanestamp"
	"github.com/bsv-blockchain/teranode/ulogger"
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
// on: the wrapper the daemon installs between the validator and the store, and the
// allocator behind it.
func BenchmarkValidateNewTxLanePosition(b *testing.B) {
	runValidateNewTxBench(b, func(s utxo.Store) utxo.Store {
		a := lane.NewAllocator(&benchRangeSource{}, "bench", filepath.Join(b.TempDir(), "lane"), nil)
		b.Cleanup(a.Close)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		require.NoError(b, a.Start(ctx))

		return lanestamp.New(s, a, ulogger.TestLogger{})
	})
}
