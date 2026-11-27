package subtreeprocessor

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/require"
)

// Issue 2269, plan PR 2b: a fork reorg stores the subtrees it built before it writes any
// longest-chain mark, and announces them only after the marks. With blockassembly_reorgStoreDeadline
// set, a store that fails or does not finish in time fails the reorg before the marks; a reset
// follows (failedForkReorgNeedsReset). With the deadline at 0 the reorg waits as it always did and a
// write that failed and went to the retry path does not fail it.

// storeRecorder answers store requests and records, for each request, whether it asked to
// announce and how many longest-chain marks had been written when it arrived.
type storeRecorder struct {
	mu        sync.Mutex
	sc        atomic.Pointer[reorgScenario]
	awaited   []int // marks written when each request with a Result arrived
	announced []int // marks written when each request that announces arrived
	answer    func(req NewSubtreeRequest)
}

func (r *storeRecorder) consume(req NewSubtreeRequest) {
	marks := 0
	if sc := r.sc.Load(); sc != nil {
		marks = sc.utxo.markCount(true) + sc.utxo.markCount(false)
	}

	r.mu.Lock()
	if req.Result != nil {
		r.awaited = append(r.awaited, marks)
	}

	if !req.SkipNotification {
		r.announced = append(r.announced, marks)
	}
	r.mu.Unlock()

	r.answer(req)
}

func newAwaitedStoreScenario(t *testing.T, deadline time.Duration, answer func(req NewSubtreeRequest)) (*reorgScenario, *storeRecorder, *phaseLogger) {
	t.Helper()

	r := &storeRecorder{answer: answer}
	cfg := smallScenarioConfig()
	cfg.reorgStoreDeadline = deadline
	cfg.storeConsumer = r.consume

	sc := newReorgScenario(t, cfg)
	r.sc.Store(sc)

	logger := &phaseLogger{}
	sc.stp.logger = logger

	return sc, r, logger
}

// failAwaited answers a store the reorg waits for as failed, and every other one as stored.
func failAwaited(req NewSubtreeRequest) {
	if req.Result != nil {
		req.Result <- StoreResult{Err: errors.NewStorageError("injected store failure")}

		if req.ErrChan != nil {
			req.ErrChan <- nil
		}

		return
	}

	answerStored(req)
}

func TestReorgScenario_AwaitedStore_StoresBeforeTheMarksAndAnnouncesAfter(t *testing.T) {
	sc, r, _ := newAwaitedStoreScenario(t, 5*time.Second, answerStored)

	require.NoError(t, sc.run(t))

	r.mu.Lock()
	defer r.mu.Unlock()

	require.NotEmpty(t, r.awaited, "the reorg waits for the stores of the subtrees it built")

	for _, marks := range r.awaited {
		require.Zero(t, marks, "every awaited store is sent before the first longest-chain mark")
	}

	r.mu.Unlock()

	// nothing waits for the announcing send, so the listener may take it after the reorg returns
	require.Eventually(t, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()

		return len(r.announced) > 0
	}, 5*time.Second, 10*time.Millisecond, "the reorg announces its subtrees")

	r.mu.Lock()

	for _, marks := range r.announced {
		require.Positive(t, marks, "nothing is announced before the marks")
	}
}

func TestReorgScenario_AwaitedStore_FailedStoreFailsTheReorgBeforeTheMarks(t *testing.T) {
	sc, r, logger := newAwaitedStoreScenario(t, 5*time.Second, failAwaited)

	require.Error(t, sc.run(t))

	require.Zero(t, sc.utxo.markCount(true)+sc.utxo.markCount(false), "no longest-chain mark is written")
	require.True(t, sc.stp.failedForkReorgNeedsReset.Load(), "only a reset may follow")
	require.NotContains(t, logger.phases("reorgBlocks"), "processed_at", "no forward block is marked processed")

	r.mu.Lock()
	defer r.mu.Unlock()

	require.Empty(t, r.announced, "nothing is announced")
}

func TestReorgScenario_AwaitedStore_StoreThatDoesNotFinishInTimeFailsTheReorg(t *testing.T) {
	never := func(req NewSubtreeRequest) {
		if req.Result != nil {
			// the write never settles: answer ErrChan only, as a store whose write hangs does not
			if req.ErrChan != nil {
				req.ErrChan <- nil
			}

			return
		}

		answerStored(req)
	}

	sc, _, _ := newAwaitedStoreScenario(t, 200*time.Millisecond, never)

	err := sc.run(t)
	require.ErrorContains(t, err, "did not finish")
	require.Zero(t, sc.utxo.markCount(true)+sc.utxo.markCount(false))
}

// At deadline 0 no request carries a Result, so a write that failed and went to the retry path is
// invisible to the reorg, as on main: the wait is on ErrChan, which such a write answers with nil.
func TestReorgScenario_AwaitedStore_NoDeadlineWaitsOnErrChanAsBefore(t *testing.T) {
	sc, r, _ := newAwaitedStoreScenario(t, 0, failAwaited)

	require.NoError(t, sc.run(t))
	require.Positive(t, sc.utxo.markCount(true)+sc.utxo.markCount(false))

	r.mu.Lock()
	defer r.mu.Unlock()

	require.Empty(t, r.awaited, "with the deadline at 0 the reorg waits on ErrChan as before")

	for _, marks := range r.announced {
		require.Positive(t, marks, "nothing is announced before the marks")
	}
}

// Issue 2269, plan PR 7: with the stored-prefix cut on, a fork reorg waits for the stores of the first
// K0 chained subtrees only (blockassembly_reorgAwaitedSubtrees). The rest store behind the template,
// which the cut keeps valid. With the cut off the wait is for every subtree, as it was.

const awaitedK0 = 3

// gatedStores are workers that take requests from a StoreQueue the way the block assembly listener
// does. A request for one of the first k0 subtrees of the chain is answered at once; any other waits
// for a token on tail, so a test lets the tail store one subtree at a time. result decides what a
// store reports, from the subtree's index in the chain and the number of times it was taken.
type gatedStores struct {
	stp    *SubtreeProcessor
	set    *StoredSet
	k0     int
	tail   chan struct{}
	result func(i, attempt int) StoreResult

	mu        sync.Mutex
	index     map[chainhash.Hash]int
	attempts  map[chainhash.Hash]int
	announced map[chainhash.Hash]int
	taken     []chainhash.Hash // roots of the stores taken, in order
	cuts      []int            // subtrees in the template after each store settled
}

// startGatedStores starts workers for the chain stp holds now.
func startGatedStores(t testing.TB, stp *SubtreeProcessor, q *StoreQueue, set *StoredSet, k0, workers int, result func(i, attempt int) StoreResult) *gatedStores {
	t.Helper()

	g := &gatedStores{stp: stp, set: set, k0: k0, tail: make(chan struct{}, 1<<16), result: result,
		index: make(map[chainhash.Hash]int), attempts: make(map[chainhash.Hash]int), announced: make(map[chainhash.Hash]int)}

	for i, st := range stp.chainedSubtrees {
		g.index[*st.RootHash()] = i
	}

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup

	for range workers {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for {
				req, _, ok := q.Take(ctx)
				if !ok {
					return
				}

				g.serve(ctx, req)
			}
		}()
	}

	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	return g
}

func (g *gatedStores) serve(ctx context.Context, req NewSubtreeRequest) {
	if req.AnnounceOnly || req.Class == StoreClassSnapshot {
		if req.AnnounceOnly {
			g.noteAnnounced(*req.Subtree.RootHash())
		}

		answerFromQueue(req, g.set, StoreResult{SubtreeStored: true, MetaNotApplicable: true})

		return
	}

	root := *req.Subtree.RootHash()

	g.mu.Lock()
	i, inChain := g.index[root]
	g.attempts[root]++
	attempt := g.attempts[root]
	g.taken = append(g.taken, root)
	g.mu.Unlock()

	if inChain && i >= g.k0 {
		select {
		case <-g.tail:
		case <-ctx.Done():
			return
		}
	}

	res := StoreResult{SubtreeStored: true, MetaNotApplicable: true}
	if inChain && g.result != nil {
		res = g.result(i, attempt)
	}

	// the way the block assembly worker settles: with the result, and announcing when the queue says so
	if !res.OK() {
		req.Failing()
	}

	g.set.Finish(root, res, false)

	if req.Result != nil {
		req.Result <- res
	}

	req.ErrChan <- nil
	req.Answered(nil)

	if req.SettledFunc()(res) || (res.OK() && !req.SkipNotification) {
		g.noteAnnounced(root)
	}

	cut := 0
	if data := g.stp.GetPrecomputedMiningData(); data != nil {
		cut = len(data.Subtrees)
	}

	g.mu.Lock()
	g.cuts = append(g.cuts, cut)
	g.mu.Unlock()
}

func (g *gatedStores) noteAnnounced(root chainhash.Hash) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.announced[root]++
}

// announcements is how many times the subtree was announced: by an announce-only request, by a store
// that announces, or by a store the queue told to announce when it settled.
func (g *gatedStores) announcements(root chainhash.Hash) int {
	g.mu.Lock()
	defer g.mu.Unlock()

	return g.announced[root]
}

func (g *gatedStores) takes(root chainhash.Hash) int {
	g.mu.Lock()
	defer g.mu.Unlock()

	return g.attempts[root]
}

func (g *gatedStores) templateSizes() []int {
	g.mu.Lock()
	defer g.mu.Unlock()

	return append([]int(nil), g.cuts...)
}

func (g *gatedStores) takenRoots() []chainhash.Hash {
	g.mu.Lock()
	defer g.mu.Unlock()

	return append([]chainhash.Hash(nil), g.taken...)
}

// releaseTail lets n stores of the tail finish.
func (g *gatedStores) releaseTail(n int) {
	for range n {
		g.tail <- struct{}{}
	}
}

// awaitedScenario is a scenario with the cut on: a store queue, a stored set and K0 awaited
// subtrees. Nothing takes a request until the reorg has sent its subtrees; start does that.
type awaitedScenario struct {
	sc     *reorgScenario
	q      *StoreQueue
	set    *StoredSet
	logger *phaseLogger

	// reorged is how many of the scenario's forward blocks the reorg moves forward
	reorged int
}

func newAwaitedScenario(t *testing.T, k0 int, deadline time.Duration, cut bool, mutate ...func(*reorgScenarioConfig)) *awaitedScenario {
	t.Helper()

	a := &awaitedScenario{q: NewStoreQueue(1 << 40), set: NewStoredSet(), logger: &phaseLogger{}}
	cfg := smallScenarioConfig()

	for _, m := range mutate {
		m(&cfg)
	}

	cfg.storeQueue, cfg.reorgAwaitedSubtrees, cfg.reorgStoreDeadline = a.q, k0, deadline

	if cut {
		cfg.storedSet = a.set
	}

	a.sc = newReorgScenario(t, cfg)
	a.sc.stp.logger = a.logger
	a.reorged = len(a.sc.forward)

	return a
}

// start runs the reorg and returns its result channel and the workers, which start once the reorg has
// built its chain and is about to wait for the stores.
func (a *awaitedScenario) start(t *testing.T, workers int, result func(i, attempt int) StoreResult) (<-chan error, *gatedStores) {
	t.Helper()

	done := make(chan error, 1)

	go func() { done <- a.sc.stp.reorgBlocks(t.Context(), []*model.Block{a.sc.lost}, a.sc.forward[:a.reorged]) }()

	require.Eventually(t, func() bool {
		for _, phase := range a.logger.phases("reorgBlocks") {
			if phase == "mark_sets" {
				return true
			}
		}

		return false
	}, 60*time.Second, 2*time.Millisecond)

	g := startGatedStores(t, a.sc.stp, a.q, a.set, awaitedK0, workers, result)

	return done, g
}

func requireStillRunning(t *testing.T, done <-chan error, d time.Duration, msg string) {
	t.Helper()

	select {
	case err := <-done:
		t.Fatalf("%s: the reorg returned (%v)", msg, err)
	case <-time.After(d):
	}
}

func requireDone(t *testing.T, done <-chan error) error {
	t.Helper()

	select {
	case err := <-done:
		return err
	case <-time.After(60 * time.Second):
		t.Fatal("the reorg never finished")

		return nil
	}
}

// With the cut off the reorg waits for every subtree whatever the setting says: a template published
// over subtrees that are not stored would name files nothing can read.
func TestReorgScenario_AwaitedSubtrees_CutOffWaitsForEverySubtree(t *testing.T) {
	a := newAwaitedScenario(t, awaitedK0, 0, false)

	done, g := a.start(t, 4, nil)

	requireStillRunning(t, done, 500*time.Millisecond, "the tail is held and the cut is off, so the reorg waits for it")
	require.Greater(t, len(a.sc.stp.chainedSubtrees), awaitedK0)

	g.releaseTail(len(a.sc.stp.chainedSubtrees))
	require.NoError(t, requireDone(t, done))
}

func TestReorgScenario_AwaitedSubtrees_AwaitedSubtreesCoupledToTheCut(t *testing.T) {
	chain := func(sizes ...uint64) []*subtreepkg.Subtree {
		out := make([]*subtreepkg.Subtree, len(sizes))

		for i, size := range sizes {
			out[i] = testQueueSubtree(t, byte(i))
			out[i].SizeInBytes = size
		}

		return out
	}

	const header = 80 // model.BlockHeaderSize

	tests := []struct {
		name     string
		k0       int
		cut      bool
		queue    bool
		maxBlock int
		chain    []*subtreepkg.Subtree
		want     int
	}{
		{"cut off", 3, false, false, 0, chain(1, 1, 1, 1, 1, 1), 6},
		{"queue without the stored set", 3, false, true, 0, chain(1, 1, 1, 1, 1, 1), 6},
		{"setting 0", 0, true, true, 0, chain(1, 1, 1, 1, 1, 1), 6},
		{"the setting", 3, true, true, 0, chain(1, 1, 1, 1, 1, 1), 3},
		{"the chain is shorter", 8, true, true, 0, chain(1, 1, 1), 3},
		{"the block-size prefix is shorter", 5, true, true, header + 250, chain(100, 100, 100, 100, 100, 100), 2},
		{"the prefix fills the budget exactly", 5, true, true, header + 200, chain(100, 100, 100, 100, 100, 100), 2},
		{"no subtree fits a block: the first is still awaited", 5, true, true, header + 50, chain(100, 100, 100), 1},
		{"an empty chain", 3, true, true, 0, nil, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tSettings := settings.NewSettings()
			tSettings.BlockAssembly.ReorgAwaitedSubtrees = tt.k0
			tSettings.Policy.BlockMaxSize = tt.maxBlock

			stp := &SubtreeProcessor{settings: tSettings, chainedSubtrees: tt.chain}
			if tt.cut {
				stp.storedSet = NewStoredSet()
			}

			if tt.queue {
				stp.storeQueue = NewStoreQueue(1 << 30)
			}

			require.Equal(t, tt.want, stp.reorgAwaitedSubtrees())
		})
	}
}

// With the cut on the reorg commits when the first K0 subtrees are stored: the marks are written, the
// reorg returns, and the template is those K0 subtrees. The rest store behind it, one finished store
// at a time, and the template only grows, always as a prefix of the chain.
func TestReorgScenario_AwaitedSubtrees_CutOnAwaitsOnlyK0AndTheTemplateGrows(t *testing.T) {
	for _, fast := range []bool{false, true} {
		t.Run(map[bool]string{false: "slow", true: "fast"}[fast], func(t *testing.T) { requireK0WaitAndGrowth(t, fast) })
	}
}

func requireK0WaitAndGrowth(t *testing.T, fast bool) {
	t.Helper()

	a := newAwaitedScenario(t, awaitedK0, 0, true)
	a.sc.stp.settings.BlockAssembly.ReorgFastPath = fast

	before := readFastPathCounts()

	done, g := a.start(t, 1, nil)
	require.NoError(t, requireDone(t, done), "the tail is held and the reorg still commits")

	if fast {
		after := readFastPathCounts()
		require.Greater(t, after.applied, before.applied, "the fast path applied: %v", after.declinedSince(before))
	}

	chain := a.sc.stp.chainedSubtrees
	require.Greater(t, len(chain), awaitedK0+4, "the chain has a tail")
	require.Equal(t, awaitedK0, a.set.PrefixLen(chain), "only the awaited subtrees are stored")
	require.Positive(t, a.sc.utxo.markCount(true)+a.sc.utxo.markCount(false), "the marks were written")

	data := a.sc.stp.GetPrecomputedMiningData()
	require.NotNil(t, data)
	require.Len(t, data.Subtrees, awaitedK0, "the template is the awaited subtrees")

	last := awaitedK0

	for i := awaitedK0; i < len(chain); i++ {
		g.releaseTail(1)

		require.Eventually(t, func() bool { return a.set.PrefixLen(chain) > i }, 10*time.Second, time.Millisecond)

		data = a.sc.stp.GetPrecomputedMiningData()
		require.NotNil(t, data)
		require.GreaterOrEqual(t, len(data.Subtrees), last, "the template never shrinks")

		for j, st := range data.Subtrees {
			require.Same(t, chain[j], st, "the template is a prefix of the chain")
		}

		last = len(data.Subtrees)
	}

	require.Len(t, data.Subtrees, len(chain), "every store done, the template is the whole chain")

	sizes := g.templateSizes()
	for i := 1; i < len(sizes); i++ {
		require.GreaterOrEqual(t, sizes[i], sizes[i-1], "no store ever saw a smaller template than the one before")
	}
}

// An awaited store that fails fails the reorg before any mark, as it does for the full wait.
func TestReorgScenario_AwaitedSubtrees_AFailureWithinK0FailsTheReorg(t *testing.T) {
	a := newAwaitedScenario(t, awaitedK0, 30*time.Second, true)

	done, _ := a.start(t, 1, func(i, _ int) StoreResult {
		if i == awaitedK0-1 {
			return StoreResult{Err: context.DeadlineExceeded}
		}

		return StoreResult{SubtreeStored: true, MetaNotApplicable: true}
	})

	require.Error(t, requireDone(t, done), "the store within K0 failed")
	require.Zero(t, a.sc.utxo.markCount(true)+a.sc.utxo.markCount(false), "before any longest-chain mark")
	require.True(t, a.sc.stp.failedForkReorgNeedsReset.Load(), "a failed fork reorg waits for the reset")
}

// A store beyond K0 that fails after the commit does not fail the reorg and never resets: the chain
// stays, the template stays at the stored prefix, and the watch sends the subtree to the store again,
// as many times as it fails, and the store that succeeds announces it once.
func TestReorgScenario_AwaitedSubtrees_AFailureBeyondK0ReEnqueuesAndNeverResets(t *testing.T) {
	for _, failures := range []int{1, 2} {
		t.Run(fmt.Sprintf("fails %d times", failures), func(t *testing.T) {
			a := newAwaitedScenario(t, awaitedK0, 30*time.Second, true)

			const failing = awaitedK0 + 1

			done, g := a.start(t, 1, func(i, attempt int) StoreResult {
				if i == failing && attempt <= failures {
					return StoreResult{Err: context.DeadlineExceeded}
				}

				return StoreResult{SubtreeStored: true, MetaNotApplicable: true}
			})
			require.NoError(t, requireDone(t, done), "a store beyond K0 is not awaited")

			stp := a.sc.stp
			chain := append([]*subtreepkg.Subtree(nil), stp.chainedSubtrees...)
			root := *chain[failing].RootHash()

			g.releaseTail(failing - awaitedK0 + 1)

			require.Eventually(t, func() bool { return a.set.HasFailed(root) }, 10*time.Second, time.Millisecond, "the store beyond K0 failed")
			require.False(t, stp.failedForkReorgNeedsReset.Load(), "no reset")
			require.NotNil(t, stp.GetPrecomputedMiningData(), "the template is still served")
			require.Equal(t, chain, stp.chainedSubtrees, "the chain was not rebuilt")
			require.Len(t, stp.GetPrecomputedMiningData().Subtrees, failing, "the template stops before the failed subtree")

			stp.prefixWatch.reenqueueGap = 0

			for resend := 1; resend <= failures; resend++ {
				g.releaseTail(len(chain))

				require.Eventually(t, func() bool {
					stp.maintainStoredPrefix(time.Now())

					return g.takes(root) > resend
				}, 10*time.Second, time.Millisecond, "the subtree was sent again")
			}

			g.releaseTail(len(chain))

			require.Eventually(t, func() bool { return a.set.PrefixLen(chain) == len(chain) }, 10*time.Second, time.Millisecond, "the subtree is stored by its last request")
			require.Equal(t, failures+1, g.takes(root), "the failed subtree was sent again after each failure")
			require.Eventually(t, func() bool { return g.announcements(root) >= 1 }, 10*time.Second, time.Millisecond, "the subtree is announced when it is stored again")
			time.Sleep(50 * time.Millisecond)
			require.Equal(t, 1, g.announcements(root), "and announced once")
			require.False(t, stp.failedForkReorgNeedsReset.Load(), "still no reset")
		})
	}
}

// The next block, while the tail of a reorg is still in the queue, is stored ahead of the tail's
// stale requests: its subtrees are what the template needs, and nothing of the old chain's tail is
// stored for it.
func TestReorgScenario_AwaitedSubtrees_TheNextBlockIsNotStarvedByTheBacklog(t *testing.T) {
	a := newAwaitedScenario(t, awaitedK0, 0, true, func(cfg *reorgScenarioConfig) {
		cfg.forwardBlocks, cfg.hitsAtFront = 3, true
	})

	// the reorg moves forward over all but the last block, which is the next block
	next := a.sc.forward[len(a.sc.forward)-1]
	a.reorged--

	done, g := a.start(t, 2, nil)
	require.NoError(t, requireDone(t, done))

	stp := a.sc.stp
	old := append([]*subtreepkg.Subtree(nil), stp.chainedSubtrees...)
	require.Greater(t, len(old), awaitedK0+4)

	// both workers hold a tail request; the rest of the tail waits in the queue
	require.Eventually(t, func() bool { return len(g.takenRoots()) >= awaitedK0+2 }, 10*time.Second, time.Millisecond)

	_, _, err := stp.moveForwardBlock(t.Context(), next, false, map[chainhash.Hash]struct{}{}, true, false)
	require.NoError(t, err)

	fresh := make(map[chainhash.Hash]struct{}, len(stp.chainedSubtrees))
	for _, st := range stp.chainedSubtrees {
		fresh[*st.RootHash()] = struct{}{}
	}

	stale := 0

	for _, st := range old {
		if _, kept := fresh[*st.RootHash()]; !kept {
			stale++
		}
	}

	require.Greater(t, stale, awaitedK0, "the block rebuilt the chain, so the queued tail is stale")

	before := len(g.takenRoots())

	g.releaseTail(len(old) + len(fresh))

	require.Eventually(t, func() bool { return a.set.PrefixLen(stp.chainedSubtrees) == len(stp.chainedSubtrees) }, 30*time.Second, 5*time.Millisecond,
		"the new chain is stored while the old tail is still queued")

	for _, root := range g.takenRoots()[before:] {
		_, ok := fresh[root]
		require.True(t, ok, "after the block a worker took a request of the old chain's tail")
	}
}
