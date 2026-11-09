package daemon

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
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

// Unwrap lets utxostore.As find the real store's lane position hook.
func (s *closeCountingStore) Unwrap() utxostore.Store {
	return s.Store
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

// With the flag off nothing about lane positions runs: no range source is asked for, and a create
// writes no position.
func TestGetUtxoStoreWithLanePositionsOffGivesNoSource(t *testing.T) {
	appSettings, opened := laneStoreFixture(t, "lane_off", false, func() (lane.RangeSource, error) {
		require.Fail(t, "with the flag off nothing asks for a range source")

		return nil, nil
	})

	d := &Stores{}

	store, err := d.GetUtxoStore(context.Background(), ulogger.TestLogger{}, appSettings)
	require.NoError(t, err)
	require.Same(t, opened, store)

	root := tests.BuildMultiWorkload(t, 0xe5, 1, 1).Roots[0]

	_, _, err = store.SpendAndCreate(context.Background(), root, 900, utxostore.WithCreateOnly())
	require.NoError(t, err)

	md, err := opened.Get(context.Background(), root.TxIDChainHash(), fields.LanePosition)
	require.NoError(t, err)
	require.Equal(t, lane.None, md.LanePosition)
}

// With the flag on the process's one store gets one source, and a create takes a position.
func TestGetUtxoStoreWithLanePositionsOnGivesTheStoreASource(t *testing.T) {
	ctx := context.Background()

	appSettings, opened := laneStoreFixture(t, "lane_on", true, func() (lane.RangeSource, error) {
		return &laneSource{}, nil
	})

	d := &Stores{}

	store, err := d.GetUtxoStore(ctx, ulogger.TestLogger{}, appSettings)
	require.NoError(t, err)

	t.Cleanup(func() { require.NoError(t, store.Close(ctx)) })

	again, err := d.GetUtxoStore(ctx, ulogger.TestLogger{}, appSettings)
	require.NoError(t, err)
	require.Same(t, store, again, "one store, so one allocator and one lane per process")

	root := tests.BuildMultiWorkload(t, 0xe6, 1, 1).Roots[0]

	_, _, err = store.SpendAndCreate(ctx, root, 900, utxostore.WithCreateOnly())
	require.NoError(t, err)

	md, err := opened.Get(ctx, root.TxIDChainHash(), fields.LanePosition)
	require.NoError(t, err)
	require.Equal(t, lane.Pack(9, 1), md.LanePosition)

	saved, err := os.ReadFile(filepath.Join(appSettings.DataFolder, "lane", "lane-test.lane"))
	require.NoError(t, err)
	require.Equal(t, "9\n", string(saved), "the lane is saved under the data folder for the next start")

	// The daemon owns the allocator's background fetch, and closes it with its other clients.
	var allocators int

	for _, client := range d.constructedClients {
		if allocator, ok := client.(*lane.Allocator); ok {
			allocators++

			require.NoError(t, allocator.Close())
		}
	}

	require.Equal(t, 1, allocators, "the allocator is retained for close, once")
}

// plainStore has only what utxo.Store promises, so it cannot take lane positions and has nothing to
// unwrap to.
type plainStore struct {
	utxostore.Store
}

// Lane positions on with a store that cannot write them, such as the null store, is not an error.
// That store takes no lane, because nothing asks for a range source.
func TestGetUtxoStoreWithLanePositionsOnLeavesAStoreThatCannotTakeThem(t *testing.T) {
	appSettings, _ := laneStoreFixture(t, "lane_no_taker", true, func() (lane.RangeSource, error) {
		require.Fail(t, "a store that cannot write lane positions asks for no range source")

		return nil, nil
	})

	inner, err := nullstore.NewNullStore()
	require.NoError(t, err)

	plain := &plainStore{Store: inner}

	newMainUtxoStore = func(context.Context, ulogger.Logger, *settings.Settings) (utxostore.Store, error) {
		return plain, nil
	}

	d := &Stores{}

	store, err := d.GetUtxoStore(context.Background(), ulogger.TestLogger{}, appSettings)
	require.NoError(t, err)
	require.Same(t, plain, store)
	require.Empty(t, d.constructedClients, "there is no allocator to close")
}

// A client name is free text, and the lane file's name is made from it. It must stay in the lane
// folder whatever the name holds.
func TestGetUtxoStoreKeepsTheLaneFileUnderItsFolder(t *testing.T) {
	ctx := context.Background()

	appSettings, opened := laneStoreFixture(t, "lane_escape", true, func() (lane.RangeSource, error) {
		return &laneSource{}, nil
	})
	appSettings.ClientName = "../../escape"

	d := &Stores{}

	store, err := d.GetUtxoStore(ctx, ulogger.TestLogger{}, appSettings)
	require.NoError(t, err)

	t.Cleanup(func() { require.NoError(t, store.Close(ctx)) })

	_, _, err = store.SpendAndCreate(ctx, tests.BuildMultiWorkload(t, 0xe7, 1, 1).Roots[0], 900, utxostore.WithCreateOnly())
	require.NoError(t, err)
	require.Zero(t, opened.closes.Load())

	for _, client := range d.constructedClients {
		if allocator, ok := client.(*lane.Allocator); ok {
			require.NoError(t, allocator.Close())
		}
	}

	files, err := filepath.Glob(filepath.Join(appSettings.DataFolder, "lane", "*"))
	require.NoError(t, err)
	require.Equal(t, []string{filepath.Join(appSettings.DataFolder, "lane", "______escape.lane")}, files)

	entries, err := os.ReadDir(appSettings.DataFolder)
	require.NoError(t, err)
	require.Len(t, entries, 1, "nothing is written beside the lane folder")

	_, err = os.Stat(filepath.Join(filepath.Dir(appSettings.DataFolder), "escape.lane"))
	require.True(t, os.IsNotExist(err), "nothing is written above the data folder")
}

// A failure to get a range source must close the store it opened and leave the singleton unset, so a retry
// starts clean rather than returning a nil store or leaking the first one.
func TestGetUtxoStoreClosesTheStoreWhenTheSourceFails(t *testing.T) {
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
	require.Nil(t, d.mainUtxoStore, "a failure leaves the singleton unset")
	require.Equal(t, int64(1), opened.closes.Load(), "the store just opened is closed")

	failing = false

	store, err = d.GetUtxoStore(ctx, ulogger.TestLogger{}, appSettings)
	require.NoError(t, err, "a retry starts clean")
	require.NotNil(t, store)
}
