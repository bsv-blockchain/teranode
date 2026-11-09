package lanestamp_test

import (
	"context"
	"net/url"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/lanestamp"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// source is the blockchain service at the RangeSource boundary: lane 7,
// ranges of 1,000 positions counting up from 1.
type source struct {
	mu   sync.Mutex
	high uint64
	down bool
}

func (s *source) ReserveLaneRange(context.Context, string, uint8, bool) (lane.Range, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.down {
		return lane.Range{}, errors.NewServiceUnavailableError("down")
	}

	r := lane.Range{Lane: 7, First: s.high + 1, Last: s.high + 1000}
	s.high = r.Last

	return r, nil
}

func newStores(t *testing.T, src lane.RangeSource, start bool) (inner, stamped utxostore.Store) {
	t.Helper()

	ctx := context.Background()

	u, err := url.Parse("sqlitememory:///lanestamp")
	require.NoError(t, err)

	s, err := sql.New(ctx, ulogger.TestLogger{}, test.CreateBaseTestSettings(t), u)
	require.NoError(t, err)

	a := lane.NewAllocator(src, "pod-3", filepath.Join(t.TempDir(), "lane"), nil)
	t.Cleanup(a.Close)

	if start {
		startCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		require.NoError(t, a.Start(startCtx))
	}

	return s, lanestamp.New(s, a, nil)
}

// The pruner service and block assembly find what the real store can do through
// utxo.As. A wrapper that hides it stops the pruner starting with the flag on.
func TestTheWrapperDoesNotHideTheWrappedStore(t *testing.T) {
	_, stamped := newStores(t, &source{}, false)

	_, ok := utxostore.As[pruner.PrunerServiceProvider](stamped)
	require.True(t, ok, "the pruner service reaches its provider through the wrapper")

	_, ok = utxostore.As[*sql.Store](stamped)
	require.True(t, ok)
}

// nettingStore answers NetsBelowCheckpoint the way the utxoset store does.
type nettingStore struct {
	utxostore.Store
	nets bool
}

func (s *nettingStore) NetsBelowCheckpoint() bool { return s.nets }

// utxo.NetsBelowCheckpoint is a bare assertion, not an Unwrap walk, and it picks the
// below-checkpoint route. The wrapper must give the wrapped store's answer, both ways.
func TestTheWrapperForwardsNetsBelowCheckpoint(t *testing.T) {
	inner, err := nullstore.NewNullStore()
	require.NoError(t, err)

	wrap := func(store utxostore.Store) utxostore.Store {
		a := lane.NewAllocator(&source{}, "pod-3", filepath.Join(t.TempDir(), "lane"), nil)
		t.Cleanup(a.Close)

		return lanestamp.New(store, a, nil)
	}

	require.True(t, utxostore.NetsBelowCheckpoint(wrap(&nettingStore{Store: inner, nets: true})))
	require.False(t, utxostore.NetsBelowCheckpoint(wrap(&nettingStore{Store: inner, nets: false})))
	require.False(t, utxostore.NetsBelowCheckpoint(wrap(inner)), "a store that is no netter does not net")
}

func lanePosition(t *testing.T, db utxostore.Store, tx *bt.Tx) uint64 {
	t.Helper()

	md, err := db.Get(context.Background(), tx.TxIDChainHash(), fields.LanePosition)
	require.NoError(t, err)

	return md.LanePosition
}

func TestEveryCreateThroughTheWrapperGetsTheNextPosition(t *testing.T) {
	ctx := context.Background()
	inner, stamped := newStores(t, &source{}, true)

	w := tests.BuildMultiWorkload(t, 0xe0, 2, 2)

	for _, root := range w.Roots {
		_, _, err := stamped.SpendAndCreate(ctx, root, 900, utxostore.WithCreateOnly())
		require.NoError(t, err)
	}

	// A spend-only call creates nothing, so it takes no position.
	_, _, err := stamped.SpendAndCreate(ctx, w.Txs[0], 901, utxostore.WithSpendOnly())
	require.NoError(t, err)

	_, _, err = stamped.SpendAndCreate(ctx, w.Txs[0], 901, utxostore.WithCreateOnly())
	require.NoError(t, err)

	results, err := stamped.SpendAndCreateMulti(ctx, w.Txs[1:], 901, utxostore.WithIgnoreLocked(true))
	require.NoError(t, err)
	require.Len(t, results, len(w.Txs)-1)

	want := uint64(1)
	for _, tx := range append(w.Roots, w.Txs...) {
		require.Equal(t, lane.Pack(7, want), lanePosition(t, inner, tx))
		want++
	}
}

// The allocator starts on the first create, through either entry point. Nothing else in the
// process starts it, so without this a process would stamp nothing.
func TestTheFirstCreateStartsTheAllocator(t *testing.T) {
	ctx := context.Background()

	t.Run("SpendAndCreate", func(t *testing.T) {
		inner, stamped := newStores(t, &source{}, false)
		root := tests.BuildMultiWorkload(t, 0xe3, 1, 1).Roots[0]

		_, _, err := stamped.SpendAndCreate(ctx, root, 900, utxostore.WithCreateOnly())
		require.NoError(t, err)
		require.Equal(t, lane.Pack(7, 1), lanePosition(t, inner, root))
	})

	t.Run("SpendAndCreateMulti", func(t *testing.T) {
		inner, stamped := newStores(t, &source{}, false)
		w := tests.BuildMultiWorkload(t, 0xe4, 2, 2)
		w.StoreRoots(t, inner, 900)

		results, err := stamped.SpendAndCreateMulti(ctx, w.Txs, 901, utxostore.WithIgnoreLocked(true))
		require.NoError(t, err)
		require.Len(t, results, len(w.Txs))

		for i, tx := range w.Txs {
			require.Equal(t, lane.Pack(7, uint64(i)+1), lanePosition(t, inner, tx)) //nolint:gosec // test data
		}
	})
}

// countingSource is a source that counts its calls, so a test can tell whether the allocator
// ever started.
type countingSource struct {
	source
	calls atomic.Int64
}

func (s *countingSource) ReserveLaneRange(ctx context.Context, holder string, wantLane uint8, hasLane bool) (lane.Range, error) {
	s.calls.Add(1)

	return s.source.ReserveLaneRange(ctx, holder, wantLane, hasLane)
}

// A spend-only call creates nothing, so it must not take a lane or wait for a range.
func TestASpendOnlyCallDoesNotStartTheAllocator(t *testing.T) {
	ctx := context.Background()
	src := &countingSource{}
	inner, stamped := newStores(t, src, false)

	w := tests.BuildMultiWorkload(t, 0xe5, 1, 1)
	w.StoreRoots(t, inner, 900)

	_, _, err := stamped.SpendAndCreate(ctx, w.Txs[0], 901, utxostore.WithSpendOnly())
	require.NoError(t, err)

	// An empty list creates nothing either. The store's answer to it is not what this checks.
	_, _ = stamped.SpendAndCreateMulti(ctx, nil, 901)

	require.Zero(t, src.calls.Load(), "a call that creates nothing asks for no range")
}

func TestACreateWithNoRangeInHandHasNoLanePosition(t *testing.T) {
	ctx := context.Background()
	inner, stamped := newStores(t, &source{down: true}, false)

	w := tests.BuildMultiWorkload(t, 0xe1, 1, 1)

	_, _, err := stamped.SpendAndCreate(ctx, w.Roots[0], 900, utxostore.WithCreateOnly())
	require.NoError(t, err, "a create does not fail for the blockchain service: the first waits for the start bound, then goes out without a position")

	// Only the first create in the process waits. With the service still down, the wait must
	// not repeat, or every create would stall for the whole outage. The bound is half the start
	// wait rather than something tighter, so a loaded machine does not make it flaky.
	began := time.Now()

	results, err := stamped.SpendAndCreateMulti(ctx, w.Txs, 901, utxostore.WithIgnoreLocked(true))
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Less(t, time.Since(began), 2500*time.Millisecond, "the second create waited for a range again")

	require.Equal(t, lane.None, lanePosition(t, inner, w.Roots[0]))
	require.Equal(t, lane.None, lanePosition(t, inner, w.Txs[0]))
}

// BenchmarkSpendAndCreate times what lane positions add to every create in a
// process: the wrapper's option scan, one allocator Next and one more option.
// The null store underneath does no work, so the difference is all stamping.
func BenchmarkSpendAndCreate(b *testing.B) {
	inner, err := nullstore.NewNullStore()
	require.NoError(b, err)

	a := lane.NewAllocator(&source{}, "bench", filepath.Join(b.TempDir(), "lane"), nil)
	b.Cleanup(a.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(b, a.Start(ctx))

	tx := tests.BuildMultiWorkload(b, 0xe2, 1, 1).Txs[0]

	for _, c := range []struct {
		name  string
		store utxostore.Store
	}{{"off", inner}, {"on", lanestamp.New(inner, a, nil)}} {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				if _, _, err := c.store.SpendAndCreate(context.Background(), tx, 901, utxostore.WithIgnoreLocked(true)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
