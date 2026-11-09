package daemon

import (
	"context"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/lanestamp"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/stretchr/testify/require"
)

// laneSource answers for the blockchain service: lane 9, ranges of 1,000 positions.
type laneSource struct {
	high atomic.Uint64
}

func (s *laneSource) ReserveLaneRange(context.Context, string, uint8, bool) (lane.Range, error) {
	last := s.high.Add(1000)

	return lane.Range{Lane: 9, First: last - 999, Last: last}, nil
}

// closeCountingStore counts Close calls on the store GetUtxoStore opened.
type closeCountingStore struct {
	utxostore.Store
	closes atomic.Int64
}

func (s *closeCountingStore) Close(ctx context.Context) error {
	s.closes.Add(1)

	return s.Store.Close(ctx)
}

// laneStoreFixture points the two seams of GetUtxoStore at a sqlitememory store and a test range
// source, and restores them afterwards. Those seams are package vars, so these tests do not run
// in parallel.
func laneStoreFixture(t *testing.T, name string, lanePositions bool, source func() (lane.RangeSource, error)) (*settings.Settings, *closeCountingStore) {
	t.Helper()

	appSettings := settings.NewSettings()
	appSettings.UtxoStore.LanePositions = lanePositions
	appSettings.DataFolder = t.TempDir()
	appSettings.ClientName = "lane-test"

	storeURL, err := url.Parse("sqlitememory:///" + name)
	require.NoError(t, err)

	inner, err := utxosql.New(context.Background(), ulogger.TestLogger{}, appSettings, storeURL)
	require.NoError(t, err)

	opened := &closeCountingStore{Store: inner}

	origStore, origSource := newMainUtxoStore, laneRangeSource

	t.Cleanup(func() { newMainUtxoStore, laneRangeSource = origStore, origSource })

	newMainUtxoStore = func(context.Context, ulogger.Logger, *settings.Settings) (utxostore.Store, error) {
		return opened, nil
	}

	laneRangeSource = func(context.Context, *Stores, ulogger.Logger, *settings.Settings) (lane.RangeSource, error) {
		return source()
	}

	return appSettings, opened
}

// With the flag off nothing about lane positions runs: no range source is asked for, and the
// store comes back unwrapped.
func TestGetUtxoStoreWithLanePositionsOffDoesNotWrap(t *testing.T) {
	appSettings, opened := laneStoreFixture(t, "lane_off", false, func() (lane.RangeSource, error) {
		require.Fail(t, "with the flag off nothing asks for a range source")

		return nil, nil
	})

	d := &Stores{}

	store, err := d.GetUtxoStore(context.Background(), ulogger.TestLogger{}, appSettings)
	require.NoError(t, err)
	require.Same(t, opened, store)

	_, wrapped := utxostore.As[*lanestamp.Store](store)
	require.False(t, wrapped)
}

// With the flag on the process's one store is wrapped once, and a create takes a position.
func TestGetUtxoStoreWithLanePositionsOnWrapsOnce(t *testing.T) {
	ctx := context.Background()

	appSettings, opened := laneStoreFixture(t, "lane_on", true, func() (lane.RangeSource, error) {
		return &laneSource{}, nil
	})

	d := &Stores{}

	store, err := d.GetUtxoStore(ctx, ulogger.TestLogger{}, appSettings)
	require.NoError(t, err)

	t.Cleanup(func() { require.NoError(t, store.Close(ctx)) })

	_, wrapped := utxostore.As[*lanestamp.Store](store)
	require.True(t, wrapped, "the wrapper is on the store every service gets")

	again, err := d.GetUtxoStore(ctx, ulogger.TestLogger{}, appSettings)
	require.NoError(t, err)
	require.Same(t, store, again, "one store, so one allocator and one lane per process")

	root := tests.BuildMultiWorkload(t, 0xe6, 1, 1).Roots[0]

	_, _, err = store.SpendAndCreate(ctx, root, 900, utxostore.WithCreateOnly())
	require.NoError(t, err)

	md, err := opened.Get(ctx, root.TxIDChainHash(), fields.LanePosition)
	require.NoError(t, err)
	require.Equal(t, lane.Pack(9, 1), md.LanePosition)
}

// A wrap that fails must close the store it opened and leave the singleton unset, so a retry
// starts clean rather than returning a nil store or leaking the first one.
func TestGetUtxoStoreClosesTheStoreWhenTheWrapFails(t *testing.T) {
	ctx := context.Background()

	failing := true

	appSettings, opened := laneStoreFixture(t, "lane_fail", true, func() (lane.RangeSource, error) {
		if failing {
			return nil, errors.NewServiceError("no blockchain client")
		}

		return &laneSource{}, nil
	})

	d := &Stores{}

	store, err := d.GetUtxoStore(ctx, ulogger.TestLogger{}, appSettings)
	require.Error(t, err)
	require.Nil(t, store)
	require.Nil(t, d.mainUtxoStore, "a failed wrap leaves the singleton unset")
	require.Equal(t, int64(1), opened.closes.Load(), "the store just opened is closed")

	failing = false

	store, err = d.GetUtxoStore(ctx, ulogger.TestLogger{}, appSettings)
	require.NoError(t, err, "a retry starts clean")
	require.NotNil(t, store)

	_, wrapped := utxostore.As[*lanestamp.Store](store)
	require.True(t, wrapped)
}
