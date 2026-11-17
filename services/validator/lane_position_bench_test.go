package validator

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/stretchr/testify/require"
)

// benchLeaseSource grants with no network, so the benchmark's flag-on figure excludes the
// background round trip to the blockchain service.
type benchLeaseSource struct {
	mu   sync.Mutex
	high uint64
}

func (s *benchLeaseSource) grant() lane.Grant {
	s.mu.Lock()
	defer s.mu.Unlock()

	g := lane.Grant{Lane: 7, First: s.high + 1, Last: s.high + 10_000_000}
	s.high = g.Last

	return g
}

func (s *benchLeaseSource) OpenLaneLease(context.Context, string) (lane.LeaseID, lane.Grant, error) {
	return lane.LeaseID{7}, s.grant(), nil
}

func (s *benchLeaseSource) RenewLaneLease(_ context.Context, r lane.Renewal) (lane.RenewResult, error) {
	if !r.WantGrant {
		return lane.RenewResult{}, nil
	}

	return lane.RenewResult{Granted: true, Grant: s.grant()}, nil
}

func (s *benchLeaseSource) ReleaseLaneLease(context.Context, lane.Release) (bool, error) {
	return false, nil
}

// BenchmarkValidateNewTxLanePosition is BenchmarkValidateNewTx with lane positions
// on: the store takes its positions from an allocator, as the daemon sets it up.
func BenchmarkValidateNewTxLanePosition(b *testing.B) {
	runValidateNewTxBench(b, func(s utxo.Store) utxo.Store {
		a := lane.NewAllocator(&benchLeaseSource{}, "bench", lane.Timings{Renew: 30 * time.Second, Stop: 2 * time.Minute, GrantDuration: 2 * time.Minute}, nil)
		b.Cleanup(func() { require.NoError(b, a.Close()) })

		taker, ok := utxo.As[utxo.LanePositionTaker](s)
		require.True(b, ok)
		taker.SetLanePositionSource(a)

		return s
	})
}
