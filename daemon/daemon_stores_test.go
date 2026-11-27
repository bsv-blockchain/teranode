package daemon

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/stores/utxo/pebble"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/stretchr/testify/require"
)

type txmetaCaptureProducer struct {
	recordingProducer
	starts   int
	messages []*kafka.Message
}

func (p *txmetaCaptureProducer) Start(context.Context, chan *kafka.Message) {
	p.starts++
}

func (p *txmetaCaptureProducer) TryPublish(message *kafka.Message) bool {
	p.messages = append(p.messages, message)
	return true
}

func TestGetUtxoStoreWiresCreatingConflictDeletes(t *testing.T) {
	originalStore, originalProducer := newMainUtxoStore, getKafkaTxmetaEvictionAsyncProducer
	t.Cleanup(func() {
		newMainUtxoStore = originalStore
		getKafkaTxmetaEvictionAsyncProducer = originalProducer
	})

	raw := &aerospike.Store{}
	producer := &txmetaCaptureProducer{}
	newMainUtxoStore = func(context.Context, ulogger.Logger, *settings.Settings) (utxostore.Store, error) {
		return raw, nil
	}
	getKafkaTxmetaEvictionAsyncProducer = func(context.Context, ulogger.Logger, *settings.Settings) (kafka.KafkaAsyncProducerI, error) {
		return producer, nil
	}

	stores := &Stores{}
	appSettings := &settings.Settings{}
	appSettings.UtxoStore.LaneMigration = settings.LaneMigrationOff
	store, err := stores.GetUtxoStore(context.Background(), ulogger.TestLogger{}, appSettings)
	require.NoError(t, err)
	require.Same(t, raw, store)
	require.Equal(t, 1, producer.starts)

	hash := chainhash.Hash{1}
	require.NotNil(t, raw.CreatingConflictEvictor())
	raw.CreatingConflictEvictor()([]chainhash.Hash{hash})
	require.Len(t, producer.messages, 1)
	require.Equal(t, hash[:], producer.messages[0].Key)
	require.Equal(t, txmetacache.EncodeDeleteBatch([]chainhash.Hash{hash}), producer.messages[0].Value)
}

// laneSource answers for the blockchain service: a lease on lane 9, grants of 1,000 positions.
type laneSource struct {
	high   atomic.Uint64
	opens  atomic.Int64
	holder atomic.Value // the holder named on the last open
}

func (s *laneSource) grant() lane.Grant {
	last := s.high.Add(1000)

	return lane.Grant{Lane: 9, First: last - 999, Last: last}
}

func (s *laneSource) OpenLaneLease(_ context.Context, holder string) (lane.LeaseID, lane.Grant, error) {
	s.opens.Add(1)
	s.holder.Store(holder)

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

func storeWithoutLanePositions(t *testing.T, name string, appSettings *settings.Settings) utxostore.Store {
	t.Helper()

	if name == "null" {
		store, err := nullstore.NewNullStore()
		require.NoError(t, err)

		return store
	}

	storeURL, err := url.Parse("pebble:///lane_no_taker")
	require.NoError(t, err)

	store, err := pebble.New(context.Background(), ulogger.TestLogger{}, appSettings, storeURL)
	require.NoError(t, err)

	return store
}

func TestGetUtxoStoreWithLaneMigrationShadowRefusesAStoreThatCannotTakeThem(t *testing.T) {
	for _, name := range []string{"null", "pebble"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			var built int

			appSettings, supported := laneStoreFixture(t, "lane_retry_"+name, true, func() (lane.LeaseSource, error) {
				built++

				return &laneSource{}, nil
			})
			t.Cleanup(func() { require.NoError(t, supported.Close(ctx)) })

			opened := &closeCountingStore{Store: storeWithoutLanePositions(t, name, appSettings)}
			t.Cleanup(func() {
				if opened.closes.Load() == 0 {
					require.NoError(t, opened.Close(ctx))
				}
			})
			newMainUtxoStore = func(context.Context, ulogger.Logger, *settings.Settings) (utxostore.Store, error) {
				return opened, nil
			}

			d := &Stores{}
			store, err := d.GetUtxoStore(ctx, ulogger.TestLogger{}, appSettings)
			require.Error(t, err)
			require.ErrorIs(t, err, errors.ErrConfiguration)
			require.Contains(t, err.Error(), "utxostore_laneMigration=shadow")
			require.Nil(t, store)
			require.Nil(t, d.mainUtxoStore, "a refused store must not become the singleton")
			require.Nil(t, d.rawUtxoStore)
			require.Equal(t, int64(1), opened.closes.Load(), "the refused store is closed")
			require.Zero(t, built, "no lease client is built for an unsupported store")
			require.Empty(t, d.constructedClients, "no allocator is retained")

			newMainUtxoStore = func(context.Context, ulogger.Logger, *settings.Settings) (utxostore.Store, error) {
				return supported, nil
			}
			store, err = d.GetUtxoStore(ctx, ulogger.TestLogger{}, appSettings)
			require.NoError(t, err, "a retry can open a supported store")
			require.Same(t, supported, store)
			require.Equal(t, 1, built)
			require.Len(t, d.constructedClients, 1)
			require.NoError(t, d.constructedClients[0].Close())
		})
	}
}

func TestGetUtxoStoreWithLaneMigrationOffAllowsAStoreThatCannotTakeThem(t *testing.T) {
	for _, name := range []string{"null", "pebble"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			appSettings, unused := laneStoreFixture(t, "lane_off_"+name, false, func() (lane.LeaseSource, error) {
				require.Fail(t, "migration off must not build a lease client")

				return nil, nil
			})
			t.Cleanup(func() { require.NoError(t, unused.Close(ctx)) })

			opened := &closeCountingStore{Store: storeWithoutLanePositions(t, name, appSettings)}
			t.Cleanup(func() { require.NoError(t, opened.Close(ctx)) })
			newMainUtxoStore = func(context.Context, ulogger.Logger, *settings.Settings) (utxostore.Store, error) {
				return opened, nil
			}

			d := &Stores{}
			store, err := d.GetUtxoStore(ctx, ulogger.TestLogger{}, appSettings)
			require.NoError(t, err)
			require.Same(t, opened, store)
			require.Zero(t, opened.closes.Load(), "the accepted store remains open")
			require.Empty(t, d.constructedClients)
		})
	}
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
// The timings are checked before the lease client is built, so a misconfigured process dials
// nothing.
func TestGetUtxoStoreRefusesLeaseTimingsWhereStopIsNotBeforeClose(t *testing.T) {
	var built atomic.Int64

	appSettings, _ := laneStoreFixture(t, "lane_timings", true, func() (lane.LeaseSource, error) {
		built.Add(1)

		return &laneSource{}, nil
	})
	appSettings.BlockChain.LaneLeaseStopAfter = appSettings.BlockChain.LaneLeaseCloseAfter

	d := &Stores{}

	_, err := d.GetUtxoStore(context.Background(), ulogger.TestLogger{}, appSettings)
	require.Error(t, err)
	require.Contains(t, err.Error(), "blockchain_laneLeaseStopAfter")
	require.Zero(t, built.Load(), "no lease client is built for timings that are refused")
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

	// The holder names the process and its host, so two pods with one client name stay apart in
	// lane_lease and lane_handout.
	hostname, err := os.Hostname()
	require.NoError(t, err)
	require.Equal(t, appSettings.ClientName+"@"+hostname, source.holder.Load())
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

// refusingReleaseSource is laneSource whose release always fails, as one sent to a blockchain
// service that has already stopped does.
type refusingReleaseSource struct {
	laneSource
}

func (s *refusingReleaseSource) ReleaseLaneLease(context.Context, lane.Release) (bool, error) {
	return false, errors.NewServiceUnavailableError("blockchain service stopped")
}

// lockedWarnRecorder keeps every warning logged through it. Unlike warnRecorder it is safe to log
// to from the allocator's own goroutine.
type lockedWarnRecorder struct {
	ulogger.TestLogger
	mu    sync.Mutex
	warns []string
}

func (l *lockedWarnRecorder) Warnf(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.warns = append(l.warns, fmt.Sprintf(format, args...))
}

func (l *lockedWarnRecorder) warnings() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return append([]string(nil), l.warns...)
}

// The allocator logs through the daemon's logger, so a release that fails at shutdown, the case of a
// process that also runs the blockchain service, is seen in the log rather than lost.
func TestGetUtxoStoreGivesTheAllocatorTheDaemonLogger(t *testing.T) {
	ctx := context.Background()

	appSettings, _ := laneStoreFixture(t, "lane_logger", true, func() (lane.LeaseSource, error) {
		return &refusingReleaseSource{}, nil
	})

	logger := &lockedWarnRecorder{}
	d := &Stores{}

	store, err := d.GetUtxoStore(ctx, logger, appSettings)
	require.NoError(t, err)

	t.Cleanup(func() { require.NoError(t, store.Close(ctx)) })

	root := tests.BuildMultiWorkload(t, 0xe7, 1, 1).Roots[0]
	_, _, err = store.SpendAndCreate(ctx, root, 900, utxostore.WithCreateOnly())
	require.NoError(t, err)

	require.Len(t, d.constructedClients, 1)
	require.NoError(t, d.constructedClients[0].(*lane.Allocator).Close())

	require.Condition(t, func() bool {
		for _, w := range logger.warnings() {
			if strings.Contains(w, "could not release the lease on lane 9") {
				return true
			}
		}

		return false
	}, "the failed release is logged through the daemon's logger, got %q", logger.warnings())
}
