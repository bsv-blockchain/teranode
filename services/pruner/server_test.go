package pruner

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// fakePrunerService is a minimal stub implementing pruner.Service. It records
// every height passed to Prune so tests can distinguish "the processor
// reached the store" from "the already-processed guard dropped the signal
// before Phase 2."
type fakePrunerService struct {
	mu    sync.Mutex
	calls []uint32
}

func (f *fakePrunerService) Start(ctx context.Context) {}

func (f *fakePrunerService) Prune(ctx context.Context, height uint32, blockHashStr string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, height)
	return 0, nil
}

func (f *fakePrunerService) AddObserver(observer pruner.Observer) {}

func (f *fakePrunerService) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// TestFallbackTickerFiresWithoutNotifications verifies that with
// pruner_fallbackTickerSeconds set, notificationWorker re-fires a pruneSignal
// carrying the last known persisted height even when zero notifications are
// delivered on the subscription channel. This guards against a stalled block
// persister (or a transient early-exit skip in prunerProcessor) stranding
// deletable rows with no notification ever arriving to retry them.
func TestFallbackTickerFiresWithoutNotifications(t *testing.T) {
	s := &Server{
		logger:      ulogger.New("test"),
		pruneNotify: make(chan pruneSignal, 1),
		settings: &settings.Settings{
			Pruner: settings.PrunerSettings{
				FallbackTickerSeconds: 1,
				BlockTrigger:          settings.PrunerBlockTriggerOnBlockPersisted,
			},
		},
	}
	s.lastPersistedHeight.Store(100)

	// The fallback ticker must not fire until a real block hash has been
	// captured from a notification (see the doc comment on fireFallbackTick).
	// Seed one here so this test isolates ticker-firing behaviour from that
	// separate guard, which is covered by TestFallbackTickerSkipsWithoutHash.
	seedHash := chainhash.Hash{0xAA}
	s.lastBlockHash.Store(&seedHash)

	subscriptionCh := make(chan *blockchain_api.Notification) // never written to

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	go s.notificationWorker(ctx, subscriptionCh)

	select {
	case sig := <-s.pruneNotify:
		require.Equal(t, uint32(100), sig.blockHeight)
		require.Equal(t, seedHash, sig.blockHash)
	case <-ctx.Done():
		t.Fatal("fallback ticker never fired")
	}
}

// TestFallbackTickerSkipsWithoutHash verifies that the fallback ticker does
// not fire at all until a real block hash has been observed from a
// notification, even when a target height is already known. This matters in
// both trigger modes: prunerProcessor's waitForBlockMinedStatus runs whenever
// blockAssemblyClient is configured (which it always is in production),
// regardless of pruner_block_trigger. A synthetic zero hash would make that
// wait retry against a hash matching no real block for up to
// BlockAssemblyWaitTimeout instead of safely skipping the tick.
func TestFallbackTickerSkipsWithoutHash(t *testing.T) {
	s := &Server{
		logger:      ulogger.New("test"),
		pruneNotify: make(chan pruneSignal, 1),
		settings: &settings.Settings{
			Pruner: settings.PrunerSettings{
				FallbackTickerSeconds: 1,
				BlockTrigger:          settings.PrunerBlockTriggerOnBlockPersisted,
			},
		},
	}
	s.lastPersistedHeight.Store(100)
	// No hash seeded — s.lastBlockHash is left at its zero value (nil).

	subscriptionCh := make(chan *blockchain_api.Notification)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go s.notificationWorker(ctx, subscriptionCh)

	select {
	case sig := <-s.pruneNotify:
		t.Fatalf("fallback ticker fired without a captured hash: %+v", sig)
	case <-ctx.Done():
		// Expected: no signal fires without a captured hash.
	}
}

// TestFallbackTickerDisabledWhenZero verifies that FallbackTickerSeconds=0
// (the "disabled" contract value) never fires, even with a known height and
// hash, and even though notificationWorker itself must still exit cleanly on
// context cancellation.
func TestFallbackTickerDisabledWhenZero(t *testing.T) {
	s := &Server{
		logger:      ulogger.New("test"),
		pruneNotify: make(chan pruneSignal, 1),
		settings: &settings.Settings{
			Pruner: settings.PrunerSettings{
				FallbackTickerSeconds: 0,
				BlockTrigger:          settings.PrunerBlockTriggerOnBlockPersisted,
			},
		},
	}
	s.lastPersistedHeight.Store(100)
	seedHash := chainhash.Hash{0xBB}
	s.lastBlockHash.Store(&seedHash)

	subscriptionCh := make(chan *blockchain_api.Notification)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		s.notificationWorker(ctx, subscriptionCh)
		close(done)
	}()

	select {
	case sig := <-s.pruneNotify:
		t.Fatalf("fallback ticker fired while disabled (FallbackTickerSeconds=0): %+v", sig)
	case <-ctx.Done():
		// Expected: no signal while disabled.
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("notificationWorker did not exit after context cancellation")
	}
}

// TestForceSignalBypassesAlreadyProcessedGuard verifies prunerProcessor's
// core guard-bypass contract directly: a plain (non-force) signal at a
// height that has already been processed is dropped by the
// blockHeight <= lastProcessedHeight guard, exactly as before this change,
// but a force signal at that SAME height bypasses only that guard and
// reaches Prune again. This is the fix for the strand scenario the fallback
// ticker exists for: the underlying DAH sweep keeps stamping rows due at or
// below a frozen trigger height even while no new (higher) notification
// arrives, so recovering them requires Prune to actually re-run at the same
// height rather than being silently dropped as "already done."
func TestForceSignalBypassesAlreadyProcessedGuard(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fakeSvc := &fakePrunerService{}

	s := &Server{
		ctx:           ctx,
		logger:        ulogger.New("test"),
		pruneNotify:   make(chan pruneSignal, 1),
		blobNotify:    make(chan pruneSignal, 1),
		prunerService: fakeSvc,
		settings: &settings.Settings{
			Pruner: settings.PrunerSettings{
				SkipPreserveParents:             true,
				SkipProcessExpiredPreservations: true,
			},
		},
	}

	go s.prunerProcessor(ctx)

	// First signal at height 500 processes normally and reaches the store.
	s.pruneNotify <- pruneSignal{blockHeight: 500, blockHash: chainhash.Hash{0x01}}
	require.Eventually(t, func() bool { return fakeSvc.callCount() == 1 }, 2*time.Second, 10*time.Millisecond,
		"first signal at height 500 should have reached Prune")
	require.Equal(t, uint32(500), s.lastProcessedHeight.Load())

	// A non-force duplicate at the same height must still be dropped by the
	// guard — this path is unchanged by the fix.
	s.pruneNotify <- pruneSignal{blockHeight: 500, blockHash: chainhash.Hash{0x01}}
	time.Sleep(200 * time.Millisecond)
	require.Equal(t, 1, fakeSvc.callCount(), "non-force duplicate at an already-processed height must still be dropped")

	// A force signal at the same height (what fireFallbackTick sends) must
	// bypass the guard and re-run Prune.
	s.pruneNotify <- pruneSignal{blockHeight: 500, blockHash: chainhash.Hash{0x01}, force: true}
	require.Eventually(t, func() bool { return fakeSvc.callCount() == 2 }, 2*time.Second, 10*time.Millisecond,
		"force signal at an already-processed height should bypass the guard and reach Prune")
}

// TestFallbackTickerReRunsAtSameHeightAfterProcessing is the end-to-end
// version of the guard-bypass fix: it wires the real notificationWorker
// ticker together with the real prunerProcessor (no manually-constructed
// force signal), with no blockchain notifications ever delivered, and
// asserts that Prune is called more than once at the frozen height. This is
// exactly the incident scenario: lastPersistedHeight is frozen (as it would
// be with a stalled block persister), yet the ticker must still cause a
// second real pass over the same height.
func TestFallbackTickerReRunsAtSameHeightAfterProcessing(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	fakeSvc := &fakePrunerService{}

	s := &Server{
		ctx:           ctx,
		logger:        ulogger.New("test"),
		pruneNotify:   make(chan pruneSignal, 1),
		blobNotify:    make(chan pruneSignal, 1),
		prunerService: fakeSvc,
		settings: &settings.Settings{
			Pruner: settings.PrunerSettings{
				FallbackTickerSeconds:           1,
				BlockTrigger:                    settings.PrunerBlockTriggerOnBlockPersisted,
				SkipPreserveParents:             true,
				SkipProcessExpiredPreservations: true,
			},
		},
	}
	s.lastPersistedHeight.Store(500)
	seedHash := chainhash.Hash{0xCC}
	s.lastBlockHash.Store(&seedHash)

	go s.prunerProcessor(ctx)

	subscriptionCh := make(chan *blockchain_api.Notification) // never written to
	go s.notificationWorker(ctx, subscriptionCh)

	// The first tick processes height 500 for real.
	require.Eventually(t, func() bool { return fakeSvc.callCount() >= 1 }, 3*time.Second, 10*time.Millisecond,
		"expected the fallback ticker to trigger an initial pass at height 500")
	require.Equal(t, uint32(500), s.lastProcessedHeight.Load())

	// lastPersistedHeight is still frozen at 500 (no notification ever
	// arrives to change it). A second tick must still reach Prune again,
	// not be dropped as a duplicate of an already-processed height.
	require.Eventually(t, func() bool { return fakeSvc.callCount() >= 2 }, 3*time.Second, 10*time.Millisecond,
		"fallback ticker signal did not bypass the already-processed guard on a second tick")
}

// TestSeedLastBlockHashArmsTickerAfterRestart covers the restart gap raised
// in review: a pruner that (re)starts while the block persister is already
// stalled loads lastPersistedHeight from blockchain state (as Init does via
// GetState) but never receives a single notification afterward, since the
// persister that would send BlockPersisted notifications is the very thing
// that's stalled. Without seedLastBlockHash, lastBlockHash would stay nil
// forever and fireFallbackTick would return early on every tick, permanently
// disarming the exact mechanism meant to survive this scenario.
//
// This drives seedLastBlockHash directly -- the same call Init makes, right
// after loading lastPersistedHeight -- against a mocked blockchainClient,
// then confirms the ticker fires from that seeded state alone with zero
// notifications ever delivered.
func TestSeedLastBlockHashArmsTickerAfterRestart(t *testing.T) {
	seedHeader := &model.BlockHeader{
		HashPrevBlock:  &chainhash.Hash{0x01},
		HashMerkleRoot: &chainhash.Hash{0x02},
	}

	blockchainMock := &blockchain.Mock{}
	blockchainMock.On("GetBlockHeadersFromHeight", mock.Anything, uint32(500), uint32(1)).
		Return([]*model.BlockHeader{seedHeader}, []*model.BlockHeaderMeta{{}}, nil)

	s := &Server{
		logger:           ulogger.New("test"),
		blockchainClient: blockchainMock,
		pruneNotify:      make(chan pruneSignal, 1),
		settings: &settings.Settings{
			Pruner: settings.PrunerSettings{
				FallbackTickerSeconds: 1,
				BlockTrigger:          settings.PrunerBlockTriggerOnBlockPersisted,
			},
		},
	}
	// Mimics what Init does before calling seedLastBlockHash: lastPersistedHeight
	// is already loaded from GetState.
	s.lastPersistedHeight.Store(500)

	s.seedLastBlockHash(context.Background(), 500)
	require.NotNil(t, s.lastBlockHash.Load(), "seedLastBlockHash should have populated lastBlockHash")
	require.True(t, seedHeader.Hash().IsEqual(s.lastBlockHash.Load()), "seeded hash should match the header returned by GetBlockHeadersFromHeight")

	// No notifications ever arrive (the persister is stalled) -- the seeded
	// state alone must be enough to arm the ticker.
	subscriptionCh := make(chan *blockchain_api.Notification)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	go s.notificationWorker(ctx, subscriptionCh)

	select {
	case sig := <-s.pruneNotify:
		require.Equal(t, uint32(500), sig.blockHeight)
		require.True(t, sig.force)
	case <-ctx.Done():
		t.Fatal("fallback ticker never fired from restart-style seeded state")
	}
}

// TestSeedLastBlockHashDoesNotClobberLiveValue covers the ordering hazard
// raised in re-review: Init starts notificationWorker BEFORE calling
// seedLastBlockHash, and GetBlockHeadersFromHeight is a network round trip,
// so a real notification can race ahead and Store a fresher hash while the
// seed call is still in flight. If the seed then did an unconditional
// Store, its later-arriving-but-older hash would silently clobber the
// live one -- benign in most cases (both are usually close together and
// waitForBlockMinedStatus tolerates some slop), but still a plain ordering
// bug. seedLastBlockHash now uses CompareAndSwap(nil, seeded), so it must
// never overwrite a value that's already been set.
//
// This simulates the race deterministically: store a "live" hash first (as
// if a notification had already won the race), then call seedLastBlockHash
// with a mocked blockchainClient that would return a *different* hash, and
// assert the live value survives untouched.
func TestSeedLastBlockHashDoesNotClobberLiveValue(t *testing.T) {
	liveHash := chainhash.Hash{0xEE}

	seedHeader := &model.BlockHeader{
		HashPrevBlock:  &chainhash.Hash{0x03},
		HashMerkleRoot: &chainhash.Hash{0x04},
	}
	require.False(t, seedHeader.Hash().IsEqual(&liveHash), "test fixture sanity check: seed and live hashes must differ")

	blockchainMock := &blockchain.Mock{}
	blockchainMock.On("GetBlockHeadersFromHeight", mock.Anything, uint32(500), uint32(1)).
		Return([]*model.BlockHeader{seedHeader}, []*model.BlockHeaderMeta{{}}, nil)

	s := &Server{
		logger:           ulogger.New("test"),
		blockchainClient: blockchainMock,
	}

	// A live notification has already won the race and claimed lastBlockHash
	// before the (slower, in-flight) seed call returns.
	s.lastBlockHash.Store(&liveHash)

	s.seedLastBlockHash(context.Background(), 500)

	require.Equal(t, liveHash, *s.lastBlockHash.Load(), "seedLastBlockHash must not clobber a hash a live notification already set")
}

// TestBlockNotificationReArmsHashWithoutSignalInOnBlockPersistedMode verifies
// part 1 of the restart-gap fix: in OnBlockPersisted mode, a Block
// notification -- which must not itself drive pruning in that mode -- still
// updates lastBlockHash, without ever enqueueing a pruneSignal. This is what
// keeps the ticker re-armed while the chain keeps moving even if the block
// persister itself has stalled and stopped sending BlockPersisted
// notifications.
func TestBlockNotificationReArmsHashWithoutSignalInOnBlockPersistedMode(t *testing.T) {
	s := &Server{
		logger:      ulogger.New("test"),
		pruneNotify: make(chan pruneSignal, 1),
		settings: &settings.Settings{
			Pruner: settings.PrunerSettings{
				BlockTrigger: settings.PrunerBlockTriggerOnBlockPersisted,
			},
		},
	}

	subscriptionCh := make(chan *blockchain_api.Notification, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go s.notificationWorker(ctx, subscriptionCh)

	blockHash := chainhash.Hash{0xDD}
	subscriptionCh <- &blockchain_api.Notification{
		Type: model.NotificationType_Block,
		Hash: blockHash[:],
	}

	require.Eventually(t, func() bool {
		h := s.lastBlockHash.Load()
		return h != nil && *h == blockHash
	}, time.Second, 10*time.Millisecond, "Block notification should update lastBlockHash even in OnBlockPersisted mode")

	select {
	case sig := <-s.pruneNotify:
		t.Fatalf("Block notification must not enqueue a pruneSignal in OnBlockPersisted mode: %+v", sig)
	case <-time.After(200 * time.Millisecond):
		// Expected: no signal enqueued.
	}
}
