package subtreeprocessor

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/stores/blob/null"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// Issue 2269, plan PR 3: with blockassembly_storedPrefixTemplate on, a mining template is cut at the
// first chained subtree whose root is not in the stored set; with it off nothing changes.

// storeRequests answers every store request of an unstarted processor and records the roots, so a
// test sees which subtrees were sent to the store and decides itself which count as stored.
type storeRequests struct {
	mu      sync.Mutex
	roots   []chainhash.Hash
	results []chan StoreResult // the Result channel of each request, nil when it had none
	cleanup []func()           // the OnStorageComplete of each request
}

func (r *storeRequests) has(root chainhash.Hash) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, got := range r.roots {
		if got == root {
			return true
		}
	}

	return false
}

func (r *storeRequests) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.roots)
}

// newStoredPrefixProcessor builds a processor with subtrees of itemsPerSubtree leaves, its block
// header set (templates are only published then) and set as its stored set. It is not started, so a
// test drives the processor's state directly.
func newStoredPrefixProcessor(t testing.TB, itemsPerSubtree int, set *StoredSet, mutate ...func(*SubtreeProcessor)) (*SubtreeProcessor, *storeRequests) {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockAssembly.InitialMerkleItemsPerSubtree = itemsPerSubtree

	log := &storeRequests{}
	newSubtreeChan := make(chan NewSubtreeRequest, 64)
	done := make(chan struct{})

	t.Cleanup(func() { close(done) })

	go func() {
		for {
			select {
			case req := <-newSubtreeChan:
				log.mu.Lock()
				log.roots = append(log.roots, *req.Subtree.RootHash())
				log.results = append(log.results, req.Result)
				log.cleanup = append(log.cleanup, req.OnStorageComplete)
				log.mu.Unlock()

				if req.ErrChan != nil {
					req.ErrChan <- nil
				}
			case <-done:
				return
			}
		}
	}()

	logger := ulogger.NewErrorTestLogger(t)
	subtreeStore, _ := null.New(logger)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(t.Context(), logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	var opts []Options
	if set != nil {
		opts = append(opts, WithStoredSet(set))
	}

	stp, err := NewSubtreeProcessor(t.Context(), ulogger.TestLogger{}, tSettings, subtreeStore, nil, utxoStore, newSubtreeChan, opts...)
	require.NoError(t, err)

	stp.InitCurrentBlockHeader(&model.BlockHeader{Version: 1, HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}})

	for _, m := range mutate {
		m(stp)
	}

	return stp, log
}

// addStoredPrefixTxs adds n new transactions, so the chain grows by about n/itemsPerSubtree
// subtrees.
func addStoredPrefixTxs(t testing.TB, stp *SubtreeProcessor, n int) {
	t.Helper()

	parent := chainhash.HashH([]byte("stored-prefix-parent"))

	for i := range n {
		hash := chainhash.HashH([]byte{byte(i), byte(i >> 8), 's', 'p'})
		require.NoError(t, stp.addNode(subtreepkg.Node{Hash: hash, Fee: 1, SizeInBytes: 100},
			&subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{parent}}, true))
	}
}

func chainRoot(stp *SubtreeProcessor, i int) chainhash.Hash {
	return *stp.chainedSubtrees[i].RootHash()
}

// requireTemplateStored is the stored-set oracle: no template the processor publishes references a
// chained subtree outside the stored set, and it is a prefix of the chain.
func requireTemplateStored(t testing.TB, stp *SubtreeProcessor, set *StoredSet) {
	t.Helper()

	data := stp.GetPrecomputedMiningData()
	if data == nil {
		return
	}

	require.LessOrEqual(t, len(data.Subtrees), len(stp.chainedSubtrees))

	for i, st := range data.Subtrees {
		require.Same(t, stp.chainedSubtrees[i], st, "the template is a prefix of the chain")
		require.True(t, set.Has(*st.RootHash()), "template subtree %d is not stored", i)
	}
}

func TestStoredPrefix_FlagOffPublishesTheWholeChain(t *testing.T) {
	stp, _ := newStoredPrefixProcessor(t, 8, nil)
	addStoredPrefixTxs(t, stp, 40)
	require.GreaterOrEqual(t, len(stp.chainedSubtrees), 3)

	data := stp.GetPrecomputedMiningData()
	require.NotNil(t, data)
	require.Len(t, data.Subtrees, len(stp.chainedSubtrees), "no set: nothing is cut")
}

func TestStoredPrefix_TemplateStopsAtTheFirstSubtreeNotStored(t *testing.T) {
	set := NewStoredSet()
	stp, _ := newStoredPrefixProcessor(t, 8, set)
	addStoredPrefixTxs(t, stp, 40)
	require.GreaterOrEqual(t, len(stp.chainedSubtrees), 4)

	// the first and the third are stored; the second is not
	set.Add(chainRoot(stp, 0))
	set.Add(chainRoot(stp, 2))

	data := stp.GetPrecomputedMiningData()
	require.Len(t, data.Subtrees, 1, "the cut is at the first gap, not at the stored count")
	requireTemplateStored(t, stp, set)

	set.Add(chainRoot(stp, 1))
	require.Len(t, stp.GetPrecomputedMiningData().Subtrees, 3, "the template grows as the gap is filled")
	requireTemplateStored(t, stp, set)

	for i := range stp.chainedSubtrees {
		set.Add(chainRoot(stp, i))
	}

	require.Len(t, stp.GetPrecomputedMiningData().Subtrees, len(stp.chainedSubtrees))
}

func TestStoredPrefix_LimitZeroEmptyChainStillServesTheOpenSubtree(t *testing.T) {
	set := NewStoredSet()
	stp, _ := newStoredPrefixProcessor(t, 8, set)
	addStoredPrefixTxs(t, stp, 3)
	require.Empty(t, stp.chainedSubtrees)

	require.Nil(t, stp.GetPrecomputedMiningData(), "nothing is published before the first subtree completes")

	snapshot := stp.incompleteSubtreeSnapshot()
	require.NotNil(t, snapshot, "with nothing chained the open subtree is the template")
	require.Len(t, snapshot.Subtrees, 1)
}

func TestStoredPrefix_LimitZeroNonEmptyChainIsAnEmptyBlockNotTheOpenSubtree(t *testing.T) {
	set := NewStoredSet()
	stp, _ := newStoredPrefixProcessor(t, 8, set)
	addStoredPrefixTxs(t, stp, 20)
	require.NotEmpty(t, stp.chainedSubtrees)
	require.Greater(t, stp.currentSubtree.Load().Length(), 1, "the open subtree holds transactions")

	require.Empty(t, stp.GetPrecomputedMiningData().Subtrees, "nothing stored: the cut is at 0")

	// GetMiningCandidate asks for the snapshot when the slice is empty; it must refuse, or the
	// candidate would be the open subtree alone, which does not follow the chained ones
	require.Nil(t, stp.incompleteSubtreeSnapshot(), "the chain is non-empty")
}

// The guard reads chainedSubtreeCount, which the bulk builders resync by hand: after one, a stale 0
// would let the open subtree out alone.
func TestStoredPrefix_GuardHoldsAfterABulkBuild(t *testing.T) {
	set := NewStoredSet()
	stp, _ := newStoredPrefixProcessor(t, 8, set)

	nodes := make([]subtreepkg.Node, 20)
	for i := range nodes {
		nodes[i] = subtreepkg.Node{Hash: chainhash.HashH([]byte{byte(i), 'b'}), Fee: 1, SizeInBytes: 100}
	}

	require.NoError(t, stp.bulkBuildSubtrees(t.Context(), nodes, 8))
	require.NotEmpty(t, stp.chainedSubtrees)
	stp.updatePrecomputedMiningData()

	require.Empty(t, stp.GetPrecomputedMiningData().Subtrees)
	require.Nil(t, stp.incompleteSubtreeSnapshot())
}

func TestStoredPrefix_NoIncompleteSnapshotRoundTripWhileChained(t *testing.T) {
	stp, _ := newStoredPrefixProcessor(t, 8, NewStoredSet())
	addStoredPrefixTxs(t, stp, 20)

	// not started: a request to the processor goroutine would wait for its 5 s timeout
	start := time.Now()
	require.Nil(t, stp.GetIncompleteSubtreeMiningData(context.Background()))
	require.Less(t, time.Since(start), time.Second, "a refusal known in advance must not wait for the processor")
}

// A rechain builds new subtrees with new roots; the roots of the old ones are in the set, the new
// ones are not until stored.
func TestStoredPrefix_RechainNewRootsAreAbsentUntilStored(t *testing.T) {
	set := NewStoredSet()
	stp, requests := newStoredPrefixProcessor(t, 8, set)
	addStoredPrefixTxs(t, stp, 60)
	require.GreaterOrEqual(t, len(stp.chainedSubtrees), 5)

	for i := range stp.chainedSubtrees {
		set.Add(chainRoot(stp, i))
	}

	require.Len(t, stp.GetPrecomputedMiningData().Subtrees, len(stp.chainedSubtrees))

	oldRoots := make([]chainhash.Hash, len(stp.chainedSubtrees))
	for i := range oldRoots {
		oldRoots[i] = chainRoot(stp, i)
	}

	// a tx of the third subtree is removed: from there on the chain is rebuilt
	hole := stp.chainedSubtrees[2].Nodes[3].Hash
	require.NoError(t, stp.removeTxFromSubtrees(t.Context(), hole))
	stp.updatePrecomputedMiningData()

	require.Equal(t, oldRoots[0], chainRoot(stp, 0))
	require.Equal(t, oldRoots[1], chainRoot(stp, 1))
	require.NotEqual(t, oldRoots[2], chainRoot(stp, 2))

	require.Len(t, stp.GetPrecomputedMiningData().Subtrees, 2, "the new roots are absent until stored")
	requireTemplateStored(t, stp, set)

	// the rechain sent every rebuilt subtree to the store (audit: reChainSubtrees, through
	// addNode and processCompleteSubtree)
	require.Eventually(t, func() bool {
		for i := 2; i < len(stp.chainedSubtrees); i++ {
			if !requests.has(chainRoot(stp, i)) {
				return false
			}
		}

		return true
	}, 5*time.Second, 5*time.Millisecond, "every rebuilt chained subtree has a store request")
}

// BenchmarkGetPrecomputedMiningData_StoredPrefix is the cost the cut adds to a read of the published
// template, with the whole chain stored (the common case: nothing is cut, no copy is made).
func BenchmarkGetPrecomputedMiningData_StoredPrefix(b *testing.B) {
	for _, subtrees := range []int{10, 100, 500} {
		for _, on := range []bool{false, true} {
			name := "off"
			if on {
				name = "on"
			}

			b.Run(fmt.Sprintf("subtrees=%d/%s", subtrees, name), func(b *testing.B) {
				var set *StoredSet
				if on {
					set = NewStoredSet()
				}

				stp, _ := newStoredPrefixProcessor(b, 8, set)
				addStoredPrefixTxs(b, stp, subtrees*8)

				if on {
					for i := range stp.chainedSubtrees {
						set.Add(chainRoot(stp, i))
					}
				}

				b.ReportAllocs()
				b.ResetTimer()

				for range b.N {
					if stp.GetPrecomputedMiningData() == nil {
						b.Fatal("no template")
					}
				}
			})
		}
	}
}

// The cut is kept between reads while the set and the published data stand still; it must not
// outlive a change to either.
func TestStoredPrefix_ACachedCutFollowsTheSetAndThePublishedData(t *testing.T) {
	set := NewStoredSet()
	stp, _ := newStoredPrefixProcessor(t, 8, set)
	addStoredPrefixTxs(t, stp, 40)

	for i := range stp.chainedSubtrees {
		set.Add(chainRoot(stp, i))
	}

	full := len(stp.chainedSubtrees)
	require.Len(t, stp.GetPrecomputedMiningData().Subtrees, full)
	require.Same(t, stp.GetPrecomputedMiningData(), stp.GetPrecomputedMiningData(), "no copy per read")

	// roots dropped from the set (pruned) cut the template again
	keep := map[chainhash.Hash]struct{}{chainRoot(stp, 0): {}, chainRoot(stp, 1): {}}
	set.Retain(keep)
	require.Len(t, stp.GetPrecomputedMiningData().Subtrees, 2)

	// a new publication is not answered from the old cut
	set.Add(chainRoot(stp, 2))
	require.Len(t, stp.GetPrecomputedMiningData().Subtrees, 3)

	stp.updatePrecomputedMiningData()
	require.Len(t, stp.GetPrecomputedMiningData().Subtrees, 3)
}

// Issue 2269, plan PR 3, audit: under the cut a chained subtree that no store request was ever made
// for is outside the stored set for good, and every template is empty. For each path that puts a
// subtree into chainedSubtrees, a store request is made for every subtree it chains:
//
//   - processCompleteSubtree (every addNode and addNodesDirectly, so a reset's reload, a block's
//     own subtrees and reChainSubtrees): sends the request at the point it chains
//   - parallelBuildRemainderSubtrees: sends in its side-effect pass
//   - enforceBlockMaxSize and the move-back build (bulkBuildSubtrees append without a send): the
//     chain is sent by storeAndAnnounceChainedSubtrees right after, in enforceBlockMaxSize and in
//     reorgBlocks (which refuses a reorg with nothing to move forward)
//
// The tests below pin each of them. reChainSubtrees is pinned in stored_prefix_cut_test.go.

// requestRecorder records the root of every request, and adds the ones keep accepts to the set, as a
// store worker does for the stores that finish.
type requestRecorder struct {
	mu    sync.Mutex
	roots map[chainhash.Hash]struct{}
	set   *StoredSet
	keep  func(root chainhash.Hash) bool
}

func (r *requestRecorder) consume(req NewSubtreeRequest) {
	root := *req.Subtree.RootHash()

	r.mu.Lock()
	if r.roots == nil {
		r.roots = make(map[chainhash.Hash]struct{})
	}

	r.roots[root] = struct{}{}
	r.mu.Unlock()

	if r.set != nil && (r.keep == nil || r.keep(root)) {
		r.set.Add(root)
	}

	answerStored(req)
}

func (r *requestRecorder) requireRequested(t *testing.T, chain []*subtreepkg.Subtree) {
	t.Helper()

	require.NotEmpty(t, chain)

	r.mu.Lock()
	defer r.mu.Unlock()

	for i, st := range chain {
		_, ok := r.roots[*st.RootHash()]
		require.True(t, ok, "chained subtree %d was never sent to the store", i)
	}
}

func TestStoredPrefixAudit_EnforceBlockMaxSizeSendsTheSubtreesItBuilds(t *testing.T) {
	set := NewStoredSet()
	stp, h := newBlockMaxSizeProcessor(t, 64, model.BlockHeaderSize+blockMaxSizeTestBudget, WithStoredSet(set))

	addBlockMaxSizeTestTxs(t, stp, sizes(200, blockMaxSizeTestTxSize)...)

	oldRoots := make(map[chainhash.Hash]struct{})
	for _, st := range stp.chainedSubtrees {
		oldRoots[*st.RootHash()] = struct{}{}
	}

	require.NoError(t, stp.enforceBlockMaxSize(t.Context()))

	sent := make(map[chainhash.Hash]struct{})
	for _, req := range h.requests() {
		sent[*req.Subtree.RootHash()] = struct{}{}
	}

	require.NotEmpty(t, stp.chainedSubtrees)

	for i, st := range stp.chainedSubtrees {
		_, rebuilt := oldRoots[*st.RootHash()]
		require.False(t, rebuilt, "the resize built new subtrees")

		_, ok := sent[*st.RootHash()]
		require.True(t, ok, "rebuilt subtree %d was never sent to the store", i)
	}

	// and the template waits for them
	require.Empty(t, stp.GetPrecomputedMiningData().Subtrees)
	require.Nil(t, stp.incompleteSubtreeSnapshot())
}

func TestStoredPrefixAudit_AResetsReloadSendsTheSubtreesItChains(t *testing.T) {
	set := NewStoredSet()
	stp, requests := newStoredPrefixProcessor(t, 8, set)

	stp.Start(context.Background())
	t.Cleanup(func() { stp.Stop(context.Background()) })

	reload := func() error {
		batch := make([]*utxo.UnminedTransaction, 40)
		nodes := make([]subtreepkg.Node, len(batch))
		inpoints := make([]subtreepkg.TxInpoints, len(batch))

		for i := range batch {
			nodes[i] = subtreepkg.Node{Hash: chainhash.HashH([]byte{byte(i), 'r', 'l'}), Fee: 1, SizeInBytes: 100}
			inpoints[i] = voutZeroInpoints(chainhash.HashH([]byte("reload-parent")))
			batch[i] = &utxo.UnminedTransaction{Node: &nodes[i], TxInpoints: &inpoints[i], CreatedAt: i}
		}

		return stp.AddNodesDirectly(batch, true)
	}

	resp := stp.Reset(stp.GetCurrentBlockHeader(), nil, nil, false, reload)
	require.NoError(t, resp.Err)

	chain := stp.GetChainedSubtrees()
	require.NotEmpty(t, chain)

	for i, st := range chain {
		root := *st.RootHash()
		require.Eventually(t, func() bool { return requests.has(root) }, 5*time.Second, time.Millisecond,
			"chained subtree %d was never sent to the store", i)
	}

	require.Empty(t, stp.GetPrecomputedMiningData().Subtrees, "nothing stored yet")
}

// reorgBlocks refuses a reorg with no block to move forward, so every move-back build is followed by
// the send of the whole chain before the marks.
func TestStoredPrefixAudit_AReorgSendsEverySubtreeItChains(t *testing.T) {
	set := NewStoredSet()
	rec := &requestRecorder{set: set, keep: func(root chainhash.Hash) bool { return root[0]%2 == 0 }}

	cfg := smallScenarioConfig()
	cfg.storedSet = set
	cfg.storeConsumer = rec.consume
	sc := newReorgScenario(t, cfg)

	require.NoError(t, sc.run(t))

	rec.requireRequested(t, sc.stp.chainedSubtrees)

	// and no template the processor holds goes past what is stored: the oracle
	requireTemplateStored(t, sc.stp, set)
	require.Less(t, len(sc.stp.GetPrecomputedMiningData().Subtrees), len(sc.stp.chainedSubtrees),
		"half of the roots are withheld, so the template is cut")
}

// Issue 2269, plan PR 3: the cut makes a stuck store visible in every template, so the processor
// prunes the set, sends a failed or stuck subtree again from memory, and exports the age of the
// oldest unstored subtree. All of it on a tick, on the processor goroutine, between handlers.

func oldestUnstoredSeconds() float64 {
	return promtestutil.ToFloat64(prometheusSubtreeProcessorStoredPrefixOldestUnstored)
}

// newWatchedProcessor is a processor with a chain of about 5 unstored subtrees.
func newWatchedProcessor(t *testing.T, mutate ...func(*SubtreeProcessor)) (*SubtreeProcessor, *StoredSet, *storeRequests) {
	t.Helper()

	set := NewStoredSet()
	stp, requests := newStoredPrefixProcessor(t, 8, set, mutate...)
	addStoredPrefixTxs(t, stp, 40)
	require.GreaterOrEqual(t, len(stp.chainedSubtrees), 4)

	// the sends of the build itself are not what the test looks at
	require.Eventually(t, func() bool { return requests.count() >= len(stp.chainedSubtrees) }, 5*time.Second, time.Millisecond)

	requests.reset()

	requests.mu.Lock()
	requests.results, requests.cleanup = nil, nil
	requests.mu.Unlock()

	return stp, set, requests
}

func (r *storeRequests) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.roots = nil // the results stay: a test settles the sends of earlier rounds
}

// settleAll settles every send so far, as failed: the store ended and the watch may send again.
func (r *storeRequests) settleAll() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, result := range r.results {
		if result != nil {
			select {
			case result <- StoreResult{Err: errors.NewStorageError("settled")}:
			default:
			}
		}
	}
}

func (r *storeRequests) snapshot() []chainhash.Hash {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]chainhash.Hash(nil), r.roots...)
}

func (r *storeRequests) requireSent(t *testing.T, want ...chainhash.Hash) {
	t.Helper()

	require.Eventually(t, func() bool { return r.count() >= len(want) }, 5*time.Second, time.Millisecond, "the requests were sent")
	time.Sleep(20 * time.Millisecond) // no more than asked for
	require.Equal(t, want, r.snapshot())
	r.reset()
}

func (r *storeRequests) requireNoneSent(t *testing.T) {
	t.Helper()

	time.Sleep(30 * time.Millisecond)
	require.Empty(t, r.snapshot())
}

func TestStoredPrefixWatch_PrunesTheSetToTheChain(t *testing.T) {
	stp, set, _ := newWatchedProcessor(t)

	stranger := chainhash.HashH([]byte("a subtree that left the chain"))
	set.Add(stranger)
	set.Add(chainRoot(stp, 0))
	set.Finish(chainhash.HashH([]byte("another stranger")), StoreResult{Err: errors.NewStorageError("x")}, false)

	stp.maintainStoredPrefix(time.Now())

	require.False(t, set.Has(stranger), "a root that left the chain is dropped, so the set cannot outgrow it")
	require.True(t, set.Has(chainRoot(stp, 0)))
	require.False(t, set.HasFailed(chainhash.HashH([]byte("another stranger"))))
}

func TestStoredPrefixWatch_GaugeIsTheAgeOfTheOldestUnstoredSubtree(t *testing.T) {
	stp, set, _ := newWatchedProcessor(t)
	t0 := time.Now()

	stp.maintainStoredPrefix(t0)
	require.Zero(t, oldestUnstoredSeconds())

	stp.maintainStoredPrefix(t0.Add(40 * time.Second))
	require.InDelta(t, 40, oldestUnstoredSeconds(), 0.5)

	set.Add(chainRoot(stp, 0))
	stp.maintainStoredPrefix(t0.Add(50 * time.Second))
	require.InDelta(t, 50, oldestUnstoredSeconds(), 0.5, "the next unstored subtree has been waiting as long")

	for i := range stp.chainedSubtrees {
		set.Add(chainRoot(stp, i))
	}

	stp.maintainStoredPrefix(t0.Add(60 * time.Second))
	require.Zero(t, oldestUnstoredSeconds(), "everything stored")
}

func TestStoredPrefixWatch_AStuckPrefixIsSentAgainAndTheGaugeShowsIt(t *testing.T) {
	stp, set, requests := newWatchedProcessor(t)
	t0 := time.Now()

	stp.maintainStoredPrefix(t0)
	stp.maintainStoredPrefix(t0.Add(149 * time.Second))
	requests.requireNoneSent(t)

	stp.maintainStoredPrefix(t0.Add(151 * time.Second))
	requests.requireSent(t, chainRoot(stp, 0))
	require.InDelta(t, 151, oldestUnstoredSeconds(), 0.5, "the alert: the prefix has not advanced")

	// not again at once
	stp.maintainStoredPrefix(t0.Add(152 * time.Second))
	requests.requireNoneSent(t)

	// stored since: the next stuck one is the second
	set.Add(chainRoot(stp, 0))
	stp.maintainStoredPrefix(t0.Add(153 * time.Second))
	stp.maintainStoredPrefix(t0.Add(310 * time.Second))
	requests.requireSent(t, chainRoot(stp, 1))
}

func TestStoredPrefixWatch_AdvancingPrefixIsLeftAlone(t *testing.T) {
	stp, set, requests := newWatchedProcessor(t)
	t0 := time.Now()

	stp.maintainStoredPrefix(t0)

	for i := range 3 {
		set.Add(chainRoot(stp, i))
		stp.maintainStoredPrefix(t0.Add(time.Duration(100*(i+1)) * time.Second))
	}

	requests.requireNoneSent(t)
}

func TestStoredPrefixWatch_NothingIsSentWhileARetryIsInFlight(t *testing.T) {
	stp, set, requests := newWatchedProcessor(t)
	t0 := time.Now()

	set.RetryStarted(chainRoot(stp, 0))
	stp.maintainStoredPrefix(t0)
	stp.maintainStoredPrefix(t0.Add(400 * time.Second))
	requests.requireNoneSent(t)

	set.Finish(chainRoot(stp, 0), StoreResult{Err: errors.NewStorageError("exhausted")}, true)
	stp.maintainStoredPrefix(t0.Add(401 * time.Second))
	requests.requireSent(t, chainRoot(stp, 0)) // failed for good, and the retry is over
}

func TestStoredPrefixWatch_ASubtreeWhoseRetriesRanOutIsSentAgainAtOnceWhileItIsChained(t *testing.T) {
	stp, set, requests := newWatchedProcessor(t)
	t0 := time.Now()

	set.Finish(chainRoot(stp, 2), StoreResult{Err: errors.NewStorageError("exhausted")}, true)

	stp.maintainStoredPrefix(t0)
	requests.requireSent(t, chainRoot(stp, 2))

	// a deterministic failure does not spin
	set.Finish(chainRoot(stp, 2), StoreResult{Err: errors.NewStorageError("exhausted")}, false)
	stp.maintainStoredPrefix(t0.Add(time.Second))
	requests.requireNoneSent(t)

	requests.settleAll()
	stp.maintainStoredPrefix(t0.Add(31 * time.Second))
	requests.requireSent(t, chainRoot(stp, 2))

	// one that left the chain is not sent
	set.Finish(chainhash.HashH([]byte("gone")), StoreResult{Err: errors.NewStorageError("exhausted")}, false)
	stp.maintainStoredPrefix(t0.Add(40 * time.Second))
	requests.requireNoneSent(t)
}

func TestStoredPrefixWatch_ARequestIsSentWithoutWaitingForTheStore(t *testing.T) {
	stp, set, _ := newWatchedProcessor(t)
	stp.newSubtreeChan = make(chan NewSubtreeRequest) // nobody receives: the store queue is full

	set.Finish(chainRoot(stp, 0), StoreResult{Err: errors.NewStorageError("exhausted")}, false)

	done := make(chan struct{})

	go func() {
		stp.maintainStoredPrefix(time.Now())
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the processor goroutine waited for a full store queue")
	}
}

func TestStoredPrefixWatch_FullyStoredChainSendsNothing(t *testing.T) {
	stp, set, requests := newWatchedProcessor(t)

	for i := range stp.chainedSubtrees {
		set.Add(chainRoot(stp, i))
	}

	t0 := time.Now()
	stp.maintainStoredPrefix(t0)
	stp.maintainStoredPrefix(t0.Add(time.Hour))
	requests.requireNoneSent(t)
}

// The tick runs in the processor's select loop, not as a message from a worker.
func TestStoredPrefixWatch_TheProcessorRunsItOnATick(t *testing.T) {
	stp, _, requests := newWatchedProcessor(t, func(stp *SubtreeProcessor) {
		stp.prefixWatch.tickEvery = 10 * time.Millisecond
		stp.prefixWatch.stallFloor = 20 * time.Millisecond
	})

	stp.Start(context.Background())
	t.Cleanup(func() { stp.Stop(context.Background()) })

	require.Eventually(t, func() bool { return requests.has(chainRoot(stp, 0)) }, 5*time.Second, 5*time.Millisecond,
		"a stuck prefix is sent again without anything poking the processor")
}

// A hole at the front stays a hole while later subtrees store: the stall is the first unstored
// root's, not the set's.
func TestStoredPrefixWatch_AHoleAtTheFrontIsSentAgainWhileLaterRootsStore(t *testing.T) {
	stp, set, requests := newWatchedProcessor(t)
	t0 := time.Now()

	stp.maintainStoredPrefix(t0)

	set.Add(chainRoot(stp, 1))
	stp.maintainStoredPrefix(t0.Add(100 * time.Second))

	set.Add(chainRoot(stp, 2))
	stp.maintainStoredPrefix(t0.Add(149 * time.Second))
	requests.requireNoneSent(t)

	stp.maintainStoredPrefix(t0.Add(151 * time.Second))
	requests.requireSent(t, chainRoot(stp, 0))
}

func TestStoredPrefixWatch_ARetryOfAnotherRootDoesNotHoldTheHoleBack(t *testing.T) {
	stp, set, requests := newWatchedProcessor(t)
	t0 := time.Now()

	set.RetryStarted(chainRoot(stp, 3))
	stp.maintainStoredPrefix(t0)
	stp.maintainStoredPrefix(t0.Add(151 * time.Second))
	requests.requireSent(t, chainRoot(stp, 0))
}

// In an outage every send holds bytes in the retry channel for about 100 s; a root is not sent
// again until its last send has settled.
func TestStoredPrefixWatch_ARootIsNotSentAgainWhileItsLastSendIsOutstanding(t *testing.T) {
	stp, set, requests := newWatchedProcessor(t)
	t0 := time.Now()

	failed := StoreResult{Err: errors.NewStorageError("outage")}
	set.Finish(chainRoot(stp, 2), failed, false)

	for i := range 12 { // 6 minutes of ticks, the writes blocked: nothing settles
		stp.maintainStoredPrefix(t0.Add(time.Duration(i) * 31 * time.Second))
	}

	require.Eventually(t, func() bool { return len(requests.snapshot()) == 2 }, 5*time.Second, 10*time.Millisecond)

	first := requests.snapshot()
	require.Equal(t, []chainhash.Hash{chainRoot(stp, 2), chainRoot(stp, 0)}, first)

	requests.mu.Lock()
	last := requests.results[0]
	requests.mu.Unlock()
	require.NotNil(t, last, "a re-send asks for its result, to know when it settles")
	requests.reset()

	// the send settles failed: now it may be sent again
	last <- failed
	set.Finish(chainRoot(stp, 2), failed, false)
	stp.maintainStoredPrefix(t0.Add(400 * time.Second))
	requests.requireSent(t, chainRoot(stp, 2))
}

// A full store queue is exactly when the prefix is stuck: the next tick tries again, not a floor later.
func TestStoredPrefixWatch_AFullQueueIsTriedAgainOnTheNextTick(t *testing.T) {
	stp, _, requests := newWatchedProcessor(t)
	t0 := time.Now()

	stp.maintainStoredPrefix(t0)

	drained := stp.newSubtreeChan
	stp.newSubtreeChan = make(chan NewSubtreeRequest) // nobody receives: every send takes default

	stp.maintainStoredPrefix(t0.Add(151 * time.Second))
	requests.requireNoneSent(t)

	stp.newSubtreeChan = drained
	stp.maintainStoredPrefix(t0.Add(156 * time.Second))
	requests.requireSent(t, chainRoot(stp, 0))
}

// In a long outage every failed root is a send that holds its bytes in the retry channel.
func TestStoredPrefixWatch_FailedRootsAreSentAgainOnePerTick(t *testing.T) {
	stp, set, requests := newWatchedProcessor(t)
	t0 := time.Now()
	failed := StoreResult{Err: errors.NewStorageError("outage")}

	for i := 1; i <= 3; i++ {
		set.Finish(chainRoot(stp, i), failed, false)
	}

	stp.maintainStoredPrefix(t0)
	requests.requireSent(t, chainRoot(stp, 1))

	stp.maintainStoredPrefix(t0.Add(time.Second))
	requests.requireSent(t, chainRoot(stp, 2))
}

// The first request may still read DeletedTxs for its meta build: only its own completion cleans.
func TestStoredPrefixWatch_AResendDoesNotCleanUpTheDeletedTxs(t *testing.T) {
	stp, set, requests := newWatchedProcessor(t)

	set.Finish(chainRoot(stp, 1), StoreResult{Err: errors.NewStorageError("x")}, false)
	stp.maintainStoredPrefix(time.Now())
	requests.requireSent(t, chainRoot(stp, 1))

	requests.mu.Lock()
	defer requests.mu.Unlock()

	require.Len(t, requests.cleanup, 1)
	require.Nil(t, requests.cleanup[0])
}
