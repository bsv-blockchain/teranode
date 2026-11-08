package lanestamp_test

import (
	"context"
	"net/url"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/lanestamp"
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

func TestACreateWithNoRangeInHandHasNoLanePosition(t *testing.T) {
	ctx := context.Background()
	inner, stamped := newStores(t, &source{down: true}, false)

	w := tests.BuildMultiWorkload(t, 0xe1, 1, 1)

	_, _, err := stamped.SpendAndCreate(ctx, w.Roots[0], 900, utxostore.WithCreateOnly())
	require.NoError(t, err, "a create never waits on, or fails for, the blockchain service")

	results, err := stamped.SpendAndCreateMulti(ctx, w.Txs, 901, utxostore.WithIgnoreLocked(true))
	require.NoError(t, err)
	require.Len(t, results, 1)

	require.Equal(t, lane.None, lanePosition(t, inner, w.Roots[0]))
	require.Equal(t, lane.None, lanePosition(t, inner, w.Txs[0]))
}
