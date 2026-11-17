package daemon

import (
	"context"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

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

// laneSource answers for the blockchain service: a lease on lane 9, grants of 1,000 positions.
type laneSource struct {
	high  atomic.Uint64
	opens atomic.Int64
}

func (s *laneSource) grant() lane.Grant {
	last := s.high.Add(1000)

	return lane.Grant{Lane: 9, First: last - 999, Last: last}
}

func (s *laneSource) OpenLaneLease(context.Context, string) (lane.LeaseID, lane.Grant, error) {
	s.opens.Add(1)

	return lane.LeaseID{9}, s.grant(), nil
}

func (s *laneSource) RenewLaneLease(_ context.Context, r lane.Renewal) (lane.RenewResult, error) {
	if !r.WantGrant {
		return lane.RenewResult{}, nil
	}

	return lane.RenewResult{Granted: true, Grant: s.grant()}, nil
}

func (s *laneSource) ReleaseLaneLease(context.Context, lane.Release) (bool, error) {
	return false, nil
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

// laneStoreFixture points the two seams of GetUtxoStore at a sqlitememory store and a test lease
// source, and restores them afterwards. Those seams are package vars, so these tests do not run
// in parallel.
func laneStoreFixture(t *testing.T, name string, lanePositions bool, source func() (lane.LeaseSource, error)) (*settings.Settings, *closeCountingStore) {
	t.Helper()

	appSettings := settings.NewSettings()
	if lanePositions {
		appSettings.UtxoStore.LaneMigration = settings.LaneMigrationShadow
	}
	appSettings.DataFolder = t.TempDir()
	appSettings.ClientName = "lane-test"

	storeURL, err := url.Parse("sqlitememory:///" + name)
	require.NoError(t, err)

	inner, err := utxosql.New(context.Background(), ulogger.TestLogger{}, appSettings, storeURL)
	require.NoError(t, err)

	opened := &closeCountingStore{Store: inner}

	origStore, origSource := newMainUtxoStore, laneLeaseSource

	t.Cleanup(func() { newMainUtxoStore, laneLeaseSource = origStore, origSource })

	newMainUtxoStore = func(context.Context, ulogger.Logger, *settings.Settings) (utxostore.Store, error) {
		return opened, nil
	}

	laneLeaseSource = func(context.Context, *Stores, ulogger.Logger, *settings.Settings) (lane.LeaseSource, error) {
		return source()
	}

	return appSettings, opened
}

// With the flag off nothing about lane positions runs: no lease source is asked for, and a create
// writes no position.
func TestGetUtxoStoreWithLaneMigrationOffGivesNoSource(t *testing.T) {
	appSettings, opened := laneStoreFixture(t, "lane_off", false, func() (lane.LeaseSource, error) {
		require.Fail(t, "with the flag off nothing asks for a lease source")

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

// At shadow the process's one store gets one source, and a create takes a position.
func TestGetUtxoStoreWithLaneMigrationShadowGivesTheStoreASource(t *testing.T) {
	ctx := context.Background()

	appSettings, opened := laneStoreFixture(t, "lane_on", true, func() (lane.LeaseSource, error) {
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

	// The daemon owns the allocator's lease, and closes it with its other clients.
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

// Shadow with a store that cannot write them, such as the null store, is not an error.
// That store takes no lane, because nothing asks for a lease source.
func TestGetUtxoStoreWithLaneMigrationShadowLeavesAStoreThatCannotTakeThem(t *testing.T) {
	appSettings, _ := laneStoreFixture(t, "lane_no_taker", true, func() (lane.LeaseSource, error) {
		require.Fail(t, "a store that cannot write lane positions asks for no lease source")

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

// A failure to get a lease source must close the store it opened and leave the singleton unset, so a retry
// starts clean rather than returning a nil store or leaking the first one.
func TestGetUtxoStoreClosesTheStoreWhenTheSourceFails(t *testing.T) {
	ctx := context.Background()

	failing := true

	appSettings, opened := laneStoreFixture(t, "lane_fail", true, func() (lane.LeaseSource, error) {
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

// A step this build does not implement fails before the store opens, so a node configured for a
// later step never runs a step it only half has, and utxoset never gains its column for it.
func TestGetUtxoStoreRefusesLaneMigrationRows(t *testing.T) {
	appSettings, opened := laneStoreFixture(t, "lane_rows", false, func() (lane.LeaseSource, error) {
		require.Fail(t, "a refused step asks for no lease source")

		return nil, nil
	})
	appSettings.UtxoStore.LaneMigration = settings.LaneMigrationRows

	newMainUtxoStore = func(context.Context, ulogger.Logger, *settings.Settings) (utxostore.Store, error) {
		require.Fail(t, "a refused step opens no store")

		return nil, nil
	}

	d := &Stores{}

	store, err := d.GetUtxoStore(context.Background(), ulogger.TestLogger{}, appSettings)
	require.Error(t, err)
	require.Contains(t, err.Error(), "utxostore_laneMigration=rows")
	require.Nil(t, store)
	require.Nil(t, d.mainUtxoStore)
	require.Zero(t, opened.closes.Load())
}

// Lease timings that would let two holders use one lane fail the store's open at shadow.
func TestGetUtxoStoreRefusesLeaseTimingsWhereStopIsNotBeforeClose(t *testing.T) {
	appSettings, _ := laneStoreFixture(t, "lane_timings", true, func() (lane.LeaseSource, error) {
		return &laneSource{}, nil
	})
	appSettings.BlockChain.LaneLeaseStopAfter = appSettings.BlockChain.LaneLeaseCloseAfter

	d := &Stores{}

	_, err := d.GetUtxoStore(context.Background(), ulogger.TestLogger{}, appSettings)
	require.Error(t, err)
	require.Contains(t, err.Error(), "blockchain_laneLeaseStopAfter")
}

// closeLaneStore closes the allocator the daemon retained, then the store, when the test ends.
func closeLaneStore(t *testing.T, d *Stores, store utxostore.Store) {
	t.Helper()

	t.Cleanup(func() {
		for _, client := range d.constructedClients {
			if allocator, ok := client.(*lane.Allocator); ok {
				require.NoError(t, allocator.Close())
			}
		}

		require.NoError(t, store.Close(context.Background()))
	})
}

// The allocator goes to the real store, before any test wrapper. A wrapper that hides the store's
// lane position hook must not leave the store without a source.
func TestGetUtxoStoreGivesTheSourceToTheStoreBeneathAWrapper(t *testing.T) {
	ctx := context.Background()

	appSettings, opened := laneStoreFixture(t, "lane_wrapped", true, func() (lane.LeaseSource, error) {
		return &laneSource{}, nil
	})

	d := &Stores{wrappers: StoreWrappers{UtxoStore: func(inner utxostore.Store) utxostore.Store {
		return &plainStore{Store: inner}
	}}}

	store, err := d.GetUtxoStore(ctx, ulogger.TestLogger{}, appSettings)
	require.NoError(t, err)
	closeLaneStore(t, d, store)

	root := tests.BuildMultiWorkload(t, 0xe7, 1, 1).Roots[0]

	_, _, err = store.SpendAndCreate(ctx, root, 900, utxostore.WithCreateOnly())
	require.NoError(t, err)

	md, err := opened.Get(ctx, root.TxIDChainHash(), fields.LanePosition)
	require.NoError(t, err)
	require.Equal(t, lane.Pack(9, 1), md.LanePosition)
}

// The allocator takes its lease on the first create, so a process that never creates records
// never takes a lane.
func TestGetUtxoStoreTakesNoLeaseUntilTheFirstCreate(t *testing.T) {
	ctx := context.Background()
	source := &laneSource{}

	appSettings, _ := laneStoreFixture(t, "lane_lazy", true, func() (lane.LeaseSource, error) {
		return source, nil
	})

	d := &Stores{}

	store, err := d.GetUtxoStore(ctx, ulogger.TestLogger{}, appSettings)
	require.NoError(t, err)
	closeLaneStore(t, d, store)

	// Give an eager start time to reach the source.
	time.Sleep(200 * time.Millisecond)
	require.Zero(t, source.opens.Load(), "no create yet, so no lease")

	root := tests.BuildMultiWorkload(t, 0xe8, 1, 1).Roots[0]

	_, _, err = store.SpendAndCreate(ctx, root, 900, utxostore.WithCreateOnly())
	require.NoError(t, err)
	require.Equal(t, int64(1), source.opens.Load(), "the first create opens the lease")
}

// The allocator is built with the grant duration setting. A grant must serve many creates inside
// that duration: a duration of zero would age the first grant out at its first position.
func TestGetUtxoStoreBuildsTheAllocatorWithTheGrantDuration(t *testing.T) {
	ctx := context.Background()

	appSettings, opened := laneStoreFixture(t, "lane_grant_duration", true, func() (lane.LeaseSource, error) {
		return &laneSource{}, nil
	})
	appSettings.BlockChain.LaneGrantDuration = time.Hour

	d := &Stores{}

	store, err := d.GetUtxoStore(ctx, ulogger.TestLogger{}, appSettings)
	require.NoError(t, err)
	closeLaneStore(t, d, store)

	for index, seed := range []byte{0xe9, 0xea} {
		root := tests.BuildMultiWorkload(t, seed, 1, 1).Roots[0]

		createCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		_, _, err = store.SpendAndCreate(createCtx, root, 900, utxostore.WithCreateOnly())

		cancel()
		require.NoError(t, err)

		md, err := opened.Get(ctx, root.TxIDChainHash(), fields.LanePosition)
		require.NoError(t, err)
		require.Equal(t, lane.Pack(9, uint64(index)+1), md.LanePosition)
	}
}

// closingLaneSource is a lease source with a Close, as the real blockchain client has, so the daemon
// would retain it if it were wired to.
type closingLaneSource struct {
	laneSource
}

func (s *closingLaneSource) Close() error { return nil }

// The allocator runs on its three timings, each from its own setting, and is the only client the
// daemon retains for it: the allocator closes its own lease client after the release, so a daemon
// that also closed that client would close it before the release could go out.
func TestGetUtxoStoreBuildsTheAllocatorFromItsLeaseSettings(t *testing.T) {
	appSettings, _ := laneStoreFixture(t, "lane_timings_wired", true, func() (lane.LeaseSource, error) {
		return &closingLaneSource{}, nil
	})
	appSettings.BlockChain.LaneLeaseRenewInterval = 7 * time.Second
	appSettings.BlockChain.LaneLeaseStopAfter = 11 * time.Second
	appSettings.BlockChain.LaneLeaseCloseAfter = 13 * time.Second
	appSettings.BlockChain.LaneGrantDuration = 17 * time.Second

	d := &Stores{}

	store, err := d.GetUtxoStore(context.Background(), ulogger.TestLogger{}, appSettings)
	require.NoError(t, err)
	closeLaneStore(t, d, store)

	require.Len(t, d.constructedClients, 1, "only the allocator is retained, not its lease client")

	allocator, ok := d.constructedClients[0].(*lane.Allocator)
	require.True(t, ok)
	require.Equal(t, lane.Timings{Renew: 7 * time.Second, Stop: 11 * time.Second, GrantDuration: 17 * time.Second}, allocator.Timings())
}
