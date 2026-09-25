package pruner

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	blockchainsql "github.com/bsv-blockchain/teranode/stores/blockchain/sql"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/chainancestry"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// The stamp depth every test below assumes: twice a coinbase maturity of 100, rounded up to a
// whole 288-block window. Window w is stampable once the tip is at least 288*w + 287 + 288.
const testStampDepth = 288

// testChain is a sqlitememory blockchain store with a linked chain stored into it, and a
// local client over it. It is a real store, never a mock: the freshness check, the ancestry
// build and the uncached tip read all run the store's own SQL.
type testChain struct {
	t      *testing.T
	store  *blockchainsql.SQL
	client blockchain.ClientI
	bits   model.NBit
	hashes []chainhash.Hash // by height, 0 is genesis
	ids    []uint32         // by height
}

func newTestChain(t *testing.T, height uint32) *testChain {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)

	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	store, err := blockchainsql.New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	client, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, store, nil, nil)
	require.NoError(t, err)

	genesis, genesisMeta, err := store.GetBestBlockHeader(context.Background())
	require.NoError(t, err)

	c := &testChain{t: t, store: store, client: client, bits: genesis.Bits,
		hashes: []chainhash.Hash{*genesis.Hash()}, ids: []uint32{genesisMeta.ID}}

	for h := uint32(1); h <= height; h++ {
		id, hash := c.storeBlock(c.hashes[h-1], h, 0)
		c.hashes = append(c.hashes, hash)
		c.ids = append(c.ids, id)
	}

	return c
}

// storeBlock stores one block on the given parent. salt makes a second block at the same
// height on the same parent distinct, which is how a fork is built.
func (c *testChain) storeBlock(prev chainhash.Hash, height, salt uint32) (uint32, chainhash.Hash) {
	c.t.Helper()

	merkle := chainhash.HashH([]byte(fmt.Sprintf("block %d salt %d", height, salt)))
	prevCopy := prev

	blk := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			Timestamp:      1296688602 + height*600 + salt,
			Nonce:          salt,
			HashPrevBlock:  &prevCopy,
			HashMerkleRoot: &merkle,
			Bits:           c.bits,
		},
		Height:           height,
		TransactionCount: 1,
	}

	id, gotHeight, err := c.store.StoreBlock(context.Background(), blk, "test_peer")
	require.NoError(c.t, err)
	require.Equal(c.t, height, gotHeight)

	// Block validation marks a block mined once its containment is recorded, and the store
	// refuses to stamp a window with a main-chain block still unmarked. Every block here is.
	require.NoError(c.t, c.store.SetBlockMinedSet(context.Background(), blk.Hash()))

	return uint32(id), *blk.Hash() //nolint:gosec // a test id is small
}

// forkPast stores a competing branch off the parent of the current best tip that reaches one
// block higher than it, so the best chain switches to a branch on which the old tip is not an
// ancestor. It returns the new tip's hash.
func (c *testChain) forkPast(tipHeight uint32) chainhash.Hash {
	c.t.Helper()

	_, forkTip := c.storeBlock(c.hashes[tipHeight-1], tipHeight, 1)
	_, forkTip = c.storeBlock(forkTip, tipHeight+1, 1)

	return forkTip
}

// bestTip reads the uncached best header, as the worker does.
func (c *testChain) bestTip() (chainhash.Hash, uint32) {
	c.t.Helper()

	header, meta, err := c.client.GetBestBlockHeaderUncached(context.Background())
	require.NoError(c.t, err)

	return *header.Hash(), meta.Height
}

// countingClient is the local client with one call counted. It delegates everything else, so
// it is the real store answering; only the count is added.
type countingClient struct {
	blockchain.ClientI
	parentLinkFetches atomic.Int32
}

func (c *countingClient) GetBlockHeadersByParentLinks(ctx context.Context, hash *chainhash.Hash, n uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	c.parentLinkFetches.Add(1)

	return c.ClientI.GetBlockHeadersByParentLinks(ctx, hash, n)
}

// fakeStamper stands in for the utxoset store's stamp side. It records the exact call
// sequence the worker makes and advances its floors the way the store does, so a test can
// assert on which windows and pages ran, with which ancestry and which live tip. It is a fake
// of the store the worker drives, not of the chain the worker reads.
type fakeStamper struct {
	mu        sync.Mutex
	depth     uint32
	floors    pruner.StampFloors
	pages     int
	open      bool
	lockHeld  bool // another instance holds the session lock
	opened    int
	closed    int
	begins    []uint32
	pageCalls []fakePage
	completes []fakeComplete
	ancs      []*chainancestry.Ancestry
	onPage    func(wLo uint32, page int)
}

type fakePage struct {
	wLo  uint32
	page int
}

type fakeComplete struct {
	wLo, liveTip uint32
}

func newFakeStamper(pages int) *fakeStamper {
	return &fakeStamper{depth: testStampDepth, pages: pages}
}

func (f *fakeStamper) StampDepth() uint32 { return f.depth }

func (f *fakeStamper) WindowBlocks() uint32 { return 288 }

func (f *fakeStamper) Floors(context.Context) (pruner.StampFloors, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.floors, nil
}

func (f *fakeStamper) OpenDrain(context.Context) (pruner.StampDrain, pruner.StampFloors, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.open || f.lockHeld {
		return nil, pruner.StampFloors{}, pruner.ErrStampDrainBusy
	}

	f.open = true
	f.opened++

	return f, f.floors, nil
}

func (f *fakeStamper) BeginWindow(_ context.Context, wLo uint32, anc *chainancestry.Ancestry) (pruner.StampWindowState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if wLo != f.floors.StampCompleteFloor {
		return 0, errors.NewProcessingError("window %d is not the next to stamp; the floor is %d", wLo, f.floors.StampCompleteFloor)
	}

	if anc.Hi() < wLo+287+f.depth {
		return pruner.StampWindowNotDeep, nil
	}

	f.begins = append(f.begins, wLo)
	f.ancs = append(f.ancs, anc)
	f.floors.StampFence = wLo + 288

	return pruner.StampWindowReady, nil
}

func (f *fakeStamper) StampPage(_ context.Context, wLo uint32, anc *chainancestry.Ancestry, page int) (pruner.StampPageResult, error) {
	f.mu.Lock()
	f.pageCalls = append(f.pageCalls, fakePage{wLo, page})
	f.ancs = append(f.ancs, anc)
	hook := f.onPage
	f.mu.Unlock()

	if hook != nil {
		hook(wLo, page)
	}

	return pruner.StampPageResult{}, nil
}

func (f *fakeStamper) CompleteWindow(_ context.Context, wLo uint32, anc *chainancestry.Ancestry, liveTip uint32) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.completes = append(f.completes, fakeComplete{wLo, liveTip})
	f.ancs = append(f.ancs, anc)
	f.floors.StampCompleteFloor = wLo + 288

	return nil
}

func (f *fakeStamper) PagesPerWindow() int { return f.pages }

func (f *fakeStamper) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.open = false
	f.closed++

	return nil
}

func (f *fakeStamper) snapshot() (begins []uint32, pages []fakePage, completes []fakeComplete, opened, closed int) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]uint32(nil), f.begins...), append([]fakePage(nil), f.pageCalls...),
		append([]fakeComplete(nil), f.completes...), f.opened, f.closed
}

func newStampTestServer(t *testing.T, client blockchain.ClientI, st pruner.Stamper) *Server {
	t.Helper()
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	return &Server{
		ctx:              ctx,
		logger:           ulogger.New("test"),
		settings:         &settings.Settings{Pruner: settings.PrunerSettings{BlockAssemblyWaitTimeout: time.Second}},
		blockchainClient: client,
		stamper:          st,
		pruneNotify:      make(chan pruneSignal, 1),
		blobNotify:       make(chan pruneSignal, 1),
		stampNotify:      make(chan pruneSignal, 1),
		stampRetry:       20 * time.Millisecond,
	}
}

func notificationAt(c *testChain, height uint32) *pruneSignal {
	return &pruneSignal{blockHeight: height, blockHash: c.hashes[height]}
}

// A tip at 1151 makes windows 0, 1 and 2 stampable and leaves window 3 short. One wake drains
// all three, lowest first, completes each with the tip it read just before, and exits with
// nothing left to stamp.
func TestStampDrainStampsEveryStampableWindowAndExitsWithZeroLag(t *testing.T) {
	chain := newTestChain(t, 1151)
	fake := newFakeStamper(3)
	s := newStampTestServer(t, chain.client, fake)

	report := s.runStampDrain(s.ctx, notificationAt(chain, 1151), false)

	require.Equal(t, "completed", report.outcome)
	require.Equal(t, 3, report.windows)
	require.Equal(t, 9, report.pages)
	require.Equal(t, uint32(0), report.residualLag)
	require.False(t, report.rearm(), "nothing left to stamp, so no retry timer")

	begins, pages, completes, opened, closed := fake.snapshot()
	require.Equal(t, []uint32{0, 288, 576}, begins, "lowest first")
	require.Equal(t, []fakeComplete{{0, 1151}, {288, 1151}, {576, 1151}}, completes)
	require.Len(t, pages, 9)
	require.Equal(t, fakePage{0, 0}, pages[0])
	require.Equal(t, fakePage{576, 2}, pages[8])
	require.Equal(t, 1, opened)
	require.Equal(t, 1, closed, "the drain is closed on exit")
	require.Equal(t, float64(0), testutil.ToFloat64(stampResidualLag))
}

// ST-13: one ancestry per drain, however many windows it covers. Every store call of the drain
// receives the same value, built from one walk of parent links from the tip down to the
// completion floor.
func TestStampDrainBuildsOneAncestryHoweverManyWindows(t *testing.T) {
	chain := newTestChain(t, 1151)
	counting := &countingClient{ClientI: chain.client}
	fake := newFakeStamper(2)
	s := newStampTestServer(t, counting, fake)

	report := s.runStampDrain(s.ctx, notificationAt(chain, 1151), false)
	require.Equal(t, 3, report.windows)

	require.Equal(t, int32(1), counting.parentLinkFetches.Load(), "one parent-link walk for the whole drain")

	fake.mu.Lock()
	defer fake.mu.Unlock()

	require.NotEmpty(t, fake.ancs)

	for _, anc := range fake.ancs {
		require.Same(t, fake.ancs[0], anc, "every call sees the one ancestry")
	}

	require.Equal(t, uint32(0), fake.ancs[0].Lo())
	require.Equal(t, uint32(1151), fake.ancs[0].Hi())
	require.Equal(t, chain.hashes[1151], fake.ancs[0].Anchor())

	id, ok := fake.ancs[0].BlockID(100)
	require.True(t, ok)
	require.Equal(t, chain.ids[100], id, "the ids are the store's own")
}

// ST-18: the chain switches branches between two pages. The freshness check before the next
// page sees a new tip hash, asks whether the drain's anchor is an ancestor of it, hears no, and
// abandons the drain. The pages already committed stand, no completion record is written, the
// drain is closed and counted, and the retry timer is owed.
func TestStampDrainAbandonsWhenTheChainSwitchesBetweenPages(t *testing.T) {
	chain := newTestChain(t, 575)
	fake := newFakeStamper(4)
	s := newStampTestServer(t, chain.client, fake)

	fake.onPage = func(_ uint32, page int) {
		if page == 1 {
			chain.forkPast(575)
		}
	}

	abandonedBefore := testutil.ToFloat64(stampDrainsAbandoned)

	report := s.runStampDrain(s.ctx, notificationAt(chain, 575), false)

	require.Equal(t, "abandoned", report.outcome)
	require.Equal(t, 0, report.windows, "no window completed")
	require.Equal(t, 2, report.pages, "pages 0 and 1 committed before the switch was seen")
	require.Equal(t, uint32(1), report.residualLag, "window 0 is still stampable at the new tip")
	require.True(t, report.rearm())

	_, pages, completes, _, closed := fake.snapshot()
	require.Equal(t, []fakePage{{0, 0}, {0, 1}}, pages)
	require.Empty(t, completes)
	require.Equal(t, 1, closed)
	require.Equal(t, abandonedBefore+1, testutil.ToFloat64(stampDrainsAbandoned))
	require.Equal(t, float64(1), testutil.ToFloat64(stampResidualLag))
}

// A block that only extends the chain between pages is not a switch: the anchor is still an
// ancestor of the new tip, so the same ancestry holds and the drain carries on. The tip handed
// to the completion call is the one read just before it, which is the new tip, not the anchor.
func TestStampDrainCarriesOnWhenTheChainOnlyGrew(t *testing.T) {
	chain := newTestChain(t, 575)
	fake := newFakeStamper(3)
	s := newStampTestServer(t, chain.client, fake)

	fake.onPage = func(_ uint32, page int) {
		if page == 1 {
			chain.storeBlock(chain.hashes[575], 576, 0)
		}
	}

	report := s.runStampDrain(s.ctx, notificationAt(chain, 575), false)

	require.Equal(t, "completed", report.outcome)
	require.Equal(t, 1, report.windows)

	_, _, completes, _, _ := fake.snapshot()
	require.Equal(t, []fakeComplete{{0, 576}}, completes, "liveTip is the uncached tip read before the call")
}

// ST-14: a service with no blockchain client counts the wake and stamps nothing. The store
// is never opened, so no window gets a completion record and none can drop.
func TestStampWakeWithNoBlockchainClientStampsNothing(t *testing.T) {
	fake := newFakeStamper(2)
	s := newStampTestServer(t, nil, fake)

	before := getCounterValue(t, stampWakesSkipped, "no_chain_client")

	report := s.runStampDrain(s.ctx, &pruneSignal{blockHeight: 1151}, false)

	require.Equal(t, "no_chain_client", report.outcome)
	require.False(t, report.rearm(), "a missing client does not change on a timer")
	require.Equal(t, before+1, getCounterValue(t, stampWakesSkipped, "no_chain_client"))

	_, _, _, opened, _ := fake.snapshot()
	require.Equal(t, 0, opened)
}

// A second wake while another instance holds the session lock is skipped before it costs a
// chain read, counted, and retried on the timer.
func TestStampWakeIsSkippedWhileAnotherDrainHoldsTheSessionLock(t *testing.T) {
	chain := newTestChain(t, 575)
	counting := &countingClient{ClientI: chain.client}
	fake := newFakeStamper(2)
	fake.lockHeld = true
	s := newStampTestServer(t, counting, fake)

	before := getCounterValue(t, stampWakesSkipped, "lock_held")

	report := s.runStampDrain(s.ctx, notificationAt(chain, 575), false)

	require.Equal(t, "lock_held", report.outcome)
	require.True(t, report.rearm())
	require.Equal(t, before+1, getCounterValue(t, stampWakesSkipped, "lock_held"))
	require.Equal(t, int32(0), counting.parentLinkFetches.Load(), "skipped before any chain read")

	_, _, _, opened, _ := fake.snapshot()
	require.Equal(t, 0, opened)
}

// A notification whose hash is neither the tip nor an ancestor of it described a block that is
// no longer on the best chain. That is counted as a stale anchor hint, and the drain still runs
// from the tip it read itself.
func TestStampDrainCountsAStaleAnchorHint(t *testing.T) {
	chain := newTestChain(t, 575)
	// A competing block at 575 stored second: equal chain work, so the original stays best.
	_, forkHash := chain.storeBlock(chain.hashes[574], 575, 1)
	tipHash, _ := chain.bestTip()
	require.Equal(t, chain.hashes[575], tipHash)

	fake := newFakeStamper(2)
	s := newStampTestServer(t, chain.client, fake)

	before := testutil.ToFloat64(stampStaleAnchorHints)

	report := s.runStampDrain(s.ctx, &pruneSignal{blockHeight: 575, blockHash: forkHash}, false)

	require.Equal(t, "completed", report.outcome)
	require.Equal(t, 1, report.windows)
	require.Equal(t, before+1, testutil.ToFloat64(stampStaleAnchorHints))
}

// A drain that ends early arms a 60-second timer, and the timer runs a drain with no
// notification. Here the first drain is abandoned by a chain switch, so nothing else would
// ever wake the worker; the timer does, and the second drain completes the window on the new
// branch.
func TestStampWorkerRetriesOnItsTimerAfterAnAbandonedDrain(t *testing.T) {
	chain := newTestChain(t, 575)
	fake := newFakeStamper(3)
	s := newStampTestServer(t, chain.client, fake)

	var forked atomic.Bool

	fake.onPage = func(_ uint32, page int) {
		if page == 1 && forked.CompareAndSwap(false, true) {
			chain.forkPast(575)
		}
	}

	timerBefore := testutil.ToFloat64(stampTimerDrains)

	go s.stampWorker(s.ctx)

	s.stampNotify <- *notificationAt(chain, 575)

	require.Eventually(t, func() bool {
		_, _, completes, _, _ := fake.snapshot()

		return len(completes) == 1
	}, 5*time.Second, 10*time.Millisecond)

	_, _, completes, opened, closed := fake.snapshot()
	require.Equal(t, uint32(0), completes[0].wLo)
	require.Equal(t, uint32(576), completes[0].liveTip, "the second drain read the new branch's tip")
	require.Equal(t, 2, opened, "one abandoned drain, one timer-driven drain")
	require.Equal(t, 2, closed)
	require.GreaterOrEqual(t, testutil.ToFloat64(stampTimerDrains), timerBefore+1)
}

// The existing pruner goroutine forwards its signal to the stamp worker only after the start
// conditions pass, on a one-slot channel that keeps the latest signal.
func TestPrunerProcessorForwardsTheWakeToTheStampWorker(t *testing.T) {
	fake := newFakeStamper(2)
	s := newStampTestServer(t, nil, fake)
	s.settings.Pruner.MinBlockHeight = 100

	go s.prunerProcessor(s.ctx)

	s.pruneNotify <- pruneSignal{blockHeight: 50}

	select {
	case sig := <-s.stampNotify:
		t.Fatalf("a signal below the minimum height must not reach the stamp worker, got %d", sig.blockHeight)
	case <-time.After(100 * time.Millisecond):
	}

	s.pruneNotify <- pruneSignal{blockHeight: 150}

	select {
	case sig := <-s.stampNotify:
		require.Equal(t, uint32(150), sig.blockHeight)
	case <-time.After(time.Second):
		t.Fatal("the stamp worker was not woken")
	}
}

// The worker exists only for a store that implements the stamp interface, found by type
// assertion exactly as the pruner provider is.
func TestFindStamperUsesATypeAssertion(t *testing.T) {
	s := &Server{utxoStore: &utxo.MockUtxostore{}}
	require.Nil(t, s.findStamper(), "the mock store does not stamp")

	s = &Server{utxoStore: stampingStore{&utxo.MockUtxostore{}, newFakeStamper(1)}}
	require.NotNil(t, s.findStamper())
}

// stampingStore is a utxo store that also stamps, for the type-assertion test only.
type stampingStore struct {
	utxo.Store
	pruner.Stamper
}
