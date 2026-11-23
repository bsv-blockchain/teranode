package subtreeprocessor

import (
	"context"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Issue 2269, plan PR 4b: with blockassembly_storedPrefixTemplate on, store requests go to a
// store-side queue that never blocks the processor, enforces a byte limit, serves the newest
// chain first and never drops a snapshot.

// takenStore is one request a worker took, in the order of its sequence number.
type takenStore struct {
	seq          uint64
	root         chainhash.Hash
	announceOnly bool
	snapshot     bool
	announces    bool // the request would announce its subtree
	marks        int  // longest-chain marks written when it was taken
}

// queueWorkers are n workers that take requests from a StoreQueue the way the block assembly
// listener does, answer each as stored, and record what they took.
type queueWorkers struct {
	mu    sync.Mutex
	taken []takenStore
}

// startQueueWorkers runs n workers on q until the test ends. hold, when not nil, is waited for after
// a request is taken and before it is answered.
func startQueueWorkers(t testing.TB, q *StoreQueue, set *StoredSet, n int, hold <-chan struct{}, marks ...func() int) *queueWorkers {
	t.Helper()

	w := &queueWorkers{}
	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup

	for range n {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for {
				req, seq, ok := q.Take(ctx)
				if !ok {
					return
				}

				root := *req.Subtree.RootHash()

				written := 0
				if len(marks) > 0 {
					written = marks[0]()
				}

				w.mu.Lock()
				w.taken = append(w.taken, takenStore{seq: seq, root: root, announceOnly: req.AnnounceOnly, snapshot: req.Class == StoreClassSnapshot,
					announces: req.AnnounceOnly || !req.SkipNotification, marks: written})
				w.mu.Unlock()

				if hold != nil {
					select {
					case <-hold:
					case <-ctx.Done():
						return
					}
				}

				answerFromQueue(req, set, StoreResult{SubtreeStored: true, MetaNotApplicable: true})
			}
		}()
	}

	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	return w
}

// answerFromQueue answers a taken request the way the block assembly worker does.
func answerFromQueue(req NewSubtreeRequest, set *StoredSet, res StoreResult) {
	if req.AnnounceOnly {
		req.Settled()

		return
	}

	if set != nil {
		set.Finish(*req.Subtree.RootHash(), res, false, 0)
	}

	if req.Result != nil {
		select {
		case req.Result <- res:
		default:
		}
	}

	if req.ErrChan != nil {
		select {
		case req.ErrChan <- nil:
		default:
		}
	}

	req.Answered(nil)
	req.Settled()
}

// bySeq is what the workers took, in sequence order.
func (w *queueWorkers) bySeq() []takenStore {
	w.mu.Lock()
	defer w.mu.Unlock()

	out := append([]takenStore(nil), w.taken...)
	sort.Slice(out, func(i, j int) bool { return out[i].seq < out[j].seq })

	return out
}

func (w *queueWorkers) stores(root chainhash.Hash) int {
	n := 0

	for _, tk := range w.bySeq() {
		if tk.root == root && !tk.announceOnly {
			n++
		}
	}

	return n
}

func chainRoots(stp *SubtreeProcessor) map[chainhash.Hash]struct{} {
	roots := make(map[chainhash.Hash]struct{}, len(stp.chainedSubtrees))
	for _, st := range stp.chainedSubtrees {
		roots[*st.RootHash()] = struct{}{}
	}

	return roots
}

func TestReorgScenario_StoreQueue_EachChainedSubtreeIsStoredOnce(t *testing.T) {
	q := NewStoreQueue(1 << 40)
	set := NewStoredSet()
	cfg := smallScenarioConfig()
	cfg.storeQueue, cfg.storedSet = q, set
	sc := newReorgScenario(t, cfg)

	workers := startQueueWorkers(t, q, set, 4, nil)

	require.NoError(t, sc.run(t))

	roots := chainRoots(sc.stp)
	require.NotEmpty(t, roots)

	for root := range roots {
		require.Equal(t, 1, workers.stores(root), "a subtree is enqueued once for the reorg, whatever the forward steps and the announce do")
	}

	// the workers take the announcements after the reorg returned
	require.Eventually(t, func() bool {
		announces := 0

		for _, tk := range workers.bySeq() {
			if tk.announceOnly {
				announces++
			}
		}

		return announces == len(roots)
	}, 10*time.Second, 5*time.Millisecond, "the announce after the marks stores nothing")
}

// A failed awaited store fails the reorg before the marks, as it does without a queue, and the
// request that failed is the one the forward step sent.
func TestReorgScenario_StoreQueue_AFailedAwaitedStoreFailsTheReorg(t *testing.T) {
	q := NewStoreQueue(1 << 40)
	cfg := smallScenarioConfig()
	cfg.storeQueue, cfg.storedSet = q, NewStoredSet()
	cfg.reorgStoreDeadline = 30 * time.Second
	sc := newReorgScenario(t, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		for {
			req, _, ok := q.Take(ctx)
			if !ok {
				return
			}

			answerFromQueue(req, nil, StoreResult{Err: context.DeadlineExceeded})
		}
	}()

	require.Error(t, sc.run(t), "the store failed")
	require.Zero(t, sc.utxo.markCount(true)+sc.utxo.markCount(false), "before any longest-chain mark")
}

func TestStoreQueue_ASnapshotIsServedAheadOfABacklog(t *testing.T) {
	q := NewStoreQueue(1 << 40)

	for i := range 1100 {
		q.Push(chainRequest(t, byte(i)))
	}

	snap := chainRequest(t, 250)
	snap.Class = StoreClassSnapshot
	q.Push(snap)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	got, _, ok := q.Take(ctx)
	require.True(t, ok)
	require.Same(t, snap.Subtree, got.Subtree)
}

// A characterization test, not a red one: it pins the blocking send that blockassembly_storedPrefixTemplate
// off keeps, and passes on the base too.
func TestStoreQueue_CharacterizationWithoutAQueueTheSendBlocksOnAFullChannel(t *testing.T) {
	stp := &SubtreeProcessor{newSubtreeChan: make(chan NewSubtreeRequest, 1)}

	require.NoError(t, stp.sendNewSubtree(context.Background(), NewSubtreeRequest{}))

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	require.ErrorIs(t, stp.sendNewSubtree(ctx, NewSubtreeRequest{}), context.DeadlineExceeded, "the full channel holds the sender until its context ends")
}

func TestReorgScenario_StoreQueue_StoresAheadOfTheBacklog(t *testing.T) {
	q := NewStoreQueue(1 << 40)
	set := NewStoredSet()
	cfg := smallScenarioConfig()
	cfg.pending, cfg.lost, cfg.subtreeSize = 5000, 500, 4
	cfg.storeQueue, cfg.storedSet = q, set
	sc := newReorgScenario(t, cfg)

	backlog := q.Len()
	require.Greater(t, backlog, 1100, "the pending set built a backlog nothing has taken")

	logger := &phaseLogger{}
	sc.stp.logger = logger

	done := make(chan error, 1)

	go func() { done <- sc.run(t) }()

	// after the marks are partitioned the reorg has sent every subtree and goes on to wait for them
	require.Eventually(t, func() bool { return slices.Contains(logger.phases("reorgBlocks"), "mark_sets") }, 60*time.Second, 5*time.Millisecond)

	workers := startQueueWorkers(t, q, set, 4, nil)

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(60 * time.Second):
		t.Fatal("the reorg never finished")
	}

	roots := chainRoots(sc.stp)

	var firstK []takenStore

	for _, tk := range workers.bySeq() {
		if !tk.announceOnly {
			firstK = append(firstK, tk)
		}

		if len(firstK) == len(roots) {
			break
		}
	}

	require.Len(t, firstK, len(roots))

	for _, tk := range firstK {
		_, inChain := roots[tk.root]
		require.True(t, inChain, "the first %d stores are the reorg's, not the backlog's", len(roots))
	}
}

func TestStoreQueue_ProcessorServesRemoveAndTheNextBlockWhileTheQueueIsFull(t *testing.T) {
	q := NewStoreQueue(1) // every request but the first waits, and nothing takes one
	stp, _ := newStoredPrefixProcessor(t, 8, NewStoredSet(), func(stp *SubtreeProcessor) { stp.storeQueue = q })

	added := make(chan struct{})

	go func() {
		defer close(added)

		addStoredPrefixTxs(t, stp, 8*1100)
	}()

	select {
	case <-added:
	case <-time.After(60 * time.Second):
		t.Fatal("the processor blocked on a full store queue")
	}

	require.Greater(t, len(stp.chainedSubtrees), 1013, "more requests than newSubtreeChan and its listener can hold")

	mockClient := &blockchain.Mock{}
	mockClient.On("SetBlockProcessedAt", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockClient.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(true, nil)
	stp.blockchainClient = mockClient

	stp.Start(t.Context())
	t.Cleanup(func() { stp.Stop(context.Background()) })

	removed := chainhash.HashH([]byte{5, 0, 's', 'p'}) // a transaction of the first subtree
	_, held := stp.currentTxMap.Get(removed)
	require.True(t, held)
	require.NoError(t, stp.Remove(context.Background(), removed))

	require.Eventually(t, func() bool { _, held := stp.currentTxMap.Get(removed); return !held }, 30*time.Second, 10*time.Millisecond,
		"the processor served Remove while the store queue was full")

	next := make(chan error, 1)

	go func() { next <- stp.MoveForwardBlock(emptyNextBlock(t, stp)) }()

	select {
	case err := <-next:
		require.NoError(t, err)
	case <-time.After(60 * time.Second):
		t.Fatal("the processor did not serve the next block while the store queue was full")
	}
}

func testQueueSubtree(t testing.TB, seed byte) *subtreepkg.Subtree {
	t.Helper()

	st, err := subtreepkg.NewTreeByLeafCount(8)
	require.NoError(t, err)

	for i := range 4 {
		require.NoError(t, st.AddNode(chainhash.HashH([]byte{seed, byte(i)}), 1, 100))
	}

	return st
}

func chainRequest(t testing.TB, seed byte) NewSubtreeRequest {
	t.Helper()

	return NewSubtreeRequest{Subtree: testQueueSubtree(t, seed), ErrChan: make(chan error, 1)}
}

func TestStoreQueue_SequenceIsAssignedAtPickupSoARemovedRequestLeavesNoGap(t *testing.T) {
	one := chainRequest(t, 1).Subtree
	_ = one
	q := NewStoreQueue(StoreRequestBytes(chainRequest(t, 1))) // room for one request
	a, b, c, d := chainRequest(t, 1), chainRequest(t, 2), chainRequest(t, 3), chainRequest(t, 4)

	q.Push(a)
	q.Push(b)
	q.Push(c)
	q.Stale(b.Subtree, c.Subtree) // their generation left the chain
	q.Push(d)                     // at the limit with stale requests: they are dropped

	for _, dropped := range []NewSubtreeRequest{b, c} {
		select {
		case <-dropped.ErrChan:
		case <-time.After(time.Second):
			t.Fatal("a dropped request must be answered")
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	first, seq0, ok := q.Take(ctx)
	require.True(t, ok)
	require.Same(t, a.Subtree, first.Subtree)
	require.Zero(t, seq0)

	first.Settled() // makes room for d

	second, seq1, ok := q.Take(ctx)
	require.True(t, ok)
	require.Same(t, d.Subtree, second.Subtree)
	require.Equal(t, uint64(1), seq1, "the dropped requests took no sequence number")
}

func TestStoreQueue_ASnapshotIsNeverDroppedAndIsTakenFirst(t *testing.T) {
	q := NewStoreQueue(StoreRequestBytes(chainRequest(t, 1)))
	a, b := chainRequest(t, 1), chainRequest(t, 2)
	snap := chainRequest(t, 9)
	snap.Class = StoreClassSnapshot
	snap.Result = make(chan StoreResult, 1)

	q.Push(a)
	q.Push(b)
	q.Push(snap)
	q.Stale(a.Subtree, b.Subtree, snap.Subtree)
	q.Push(chainRequest(t, 3)) // at the limit: every stale chain request goes

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	got, _, ok := q.Take(ctx)
	require.True(t, ok)
	require.Same(t, snap.Subtree, got.Subtree, "the snapshot is served ahead of the chain")

	select {
	case r := <-snap.Result:
		t.Fatalf("a snapshot is not settled by the queue: %+v", r)
	default:
	}
}

func TestStoreQueue_HeldBytesNeverPassTheLimitUnlessOneRequestIsLarger(t *testing.T) {
	per := StoreRequestBytes(chainRequest(t, 1))
	q := NewStoreQueue(3 * per)

	for i := range 20 {
		q.Push(chainRequest(t, byte(i)))
		require.LessOrEqual(t, q.Held(), 3*per)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, _, ok := q.Take(ctx)
	require.True(t, ok)
	require.LessOrEqual(t, q.Held(), 3*per, "a taken request stays charged until it settles")

	req.Settled()
	require.LessOrEqual(t, q.Held(), 3*per)
}

// A snapshot waiter must not serve a subtree whose write failed: ErrChan only says that the first
// attempt ended, so it reads the typed result.
func TestStoreQueue_ASnapshotWhoseWriteFailedIsNotServedAsStored(t *testing.T) {
	for name, tc := range map[string]struct {
		res    *StoreResult // nil: the result is not posted yet, as while a failed write is retried
		stored bool
	}{
		"stored":        {&StoreResult{SubtreeStored: true, MetaNotApplicable: true}, true},
		"failed":        {&StoreResult{Err: context.DeadlineExceeded}, false},
		"retrying":      {nil, false},
		"file not read": {&StoreResult{}, false},
	} {
		t.Run(name, func(t *testing.T) {
			q := NewStoreQueue(1 << 40)
			stp, _ := newStoredPrefixProcessor(t, 8, NewStoredSet(), func(stp *SubtreeProcessor) { stp.storeQueue = q })

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				req, _, ok := q.Take(ctx)
				if !ok {
					return
				}

				if tc.res != nil {
					req.Result <- *tc.res
				}

				req.ErrChan <- nil
				req.Answered(nil)
				req.Settled()
			}()

			st := testQueueSubtree(t, 7)
			require.Equal(t, tc.stored, stp.storeIncompleteSnapshot(ctx, st, nil))

			snapshot := &afterBlockSnapshot{data: &PrecomputedMiningData{Subtrees: []*subtreepkg.Subtree{st}}}

			go func() {
				req, _, ok := q.Take(ctx)
				if !ok {
					return
				}

				if tc.res != nil {
					req.Result <- *tc.res
				}

				req.ErrChan <- nil
				req.Answered(nil)
				req.Settled()
			}()

			// a result that is not posted yet is waited for until the caller's deadline
			waitCtx, waitCancel := context.WithTimeout(ctx, 300*time.Millisecond)
			defer waitCancel()

			require.Equal(t, tc.stored, snapshot.storeOnce(waitCtx, stp))
			require.Equal(t, tc.stored, snapshot.stored)
		})
	}
}

// A block movement that failed puts its chain back after retiring the one it built on. At the byte
// limit the retire dropped the requests of that chain, which is wanted again: they are sent again.
func TestStoreQueue_ARestoredChainIsWantedAgain(t *testing.T) {
	q := NewStoreQueue(StoreRequestBytes(chainRequest(t, 1))) // room for one request
	stp, _ := newStoredPrefixProcessor(t, 8, NewStoredSet(), func(stp *SubtreeProcessor) { stp.storeQueue = q })
	addStoredPrefixTxs(t, stp, 8*5)

	chained := append([]*subtreepkg.Subtree(nil), stp.chainedSubtrees...)
	current := stp.currentSubtree.Load()
	require.GreaterOrEqual(t, len(chained), 4)

	stp.retireChainedSubtrees()
	require.Less(t, q.Len(), len(chained), "retiring the chain at the limit dropped its waiting requests")

	stp.restoreSubtrees(chained, current)

	for i, st := range chained {
		require.NotNil(t, q.entry(st), "restored subtree %d has a request again", i)
		require.False(t, q.entry(st).stale, "and it is wanted")
	}
}

// ErrChan can be answered after the request settled: a store that aborted settles first.
func TestStoreQueue_ARequestThatAbortedStillReportsItsError(t *testing.T) {
	q := NewStoreQueue(1 << 30)
	req := chainRequest(t, 1)
	q.Push(req)

	it := q.entry(req.Subtree)
	require.NotNil(t, it)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	taken, _, ok := q.Take(ctx)
	require.True(t, ok)

	taken.Settled()
	require.Nil(t, q.entry(req.Subtree), "settled: no longer in the queue")

	taken.Answered(context.DeadlineExceeded)

	<-it.first
	require.ErrorIs(t, it.firstErr, context.DeadlineExceeded)
}

// While a snapshot's write is retried, every caller of an outage waits for that request; none sends
// another.
func TestStoreQueue_AFailedAfterBlockSnapshotIsNotSentAgainByEveryCaller(t *testing.T) {
	q := NewStoreQueue(1 << 30)
	stp, _ := newStoredPrefixProcessor(t, 8, NewStoredSet(), func(stp *SubtreeProcessor) { stp.storeQueue = q })
	snapshot := &afterBlockSnapshot{data: &PrecomputedMiningData{Subtrees: []*subtreepkg.Subtree{testQueueSubtree(t, 7)}}}

	var first NewSubtreeRequest

	for range 5 {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		require.False(t, snapshot.storeOnce(ctx, stp))
		cancel()

		if first.Subtree == nil {
			takeCtx, takeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			var ok bool
			first, _, ok = q.Take(takeCtx)
			takeCancel()
			require.True(t, ok)

			first.ErrChan <- nil // the first attempt ended; the write failed and is being retried
			first.Answered(nil)
		}
	}

	require.Zero(t, q.Len(), "one request, however many callers")

	first.Result <- StoreResult{Err: context.DeadlineExceeded} // the retries ran out
	first.Settled()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	require.False(t, snapshot.storeOnce(ctx, stp), "the request failed")
	require.Zero(t, q.Len())

	ctx2, cancel2 := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel2()

	require.False(t, snapshot.storeOnce(ctx2, stp))
	require.Equal(t, 1, q.Len(), "the next caller sends it again")
}

// Nothing is announced before the reorg's marks: the forward steps send silently and the announce
// comes after the marks.
func TestReorgScenario_StoreQueue_NothingIsAnnouncedBeforeTheMarks(t *testing.T) {
	q := NewStoreQueue(1 << 40)
	set := NewStoredSet()
	cfg := smallScenarioConfig()
	cfg.storeQueue, cfg.storedSet = q, set
	sc := newReorgScenario(t, cfg)

	workers := startQueueWorkers(t, q, set, 4, nil, func() int { return sc.utxo.markCount(true) + sc.utxo.markCount(false) })

	require.NoError(t, sc.run(t))

	require.Eventually(t, func() bool {
		for _, tk := range workers.bySeq() {
			if tk.announceOnly {
				return true
			}
		}

		return false
	}, 10*time.Second, 5*time.Millisecond)

	for _, tk := range workers.bySeq() {
		if tk.announces {
			require.Positive(t, tk.marks, "an announcement was taken before the marks")
		}
	}
}

// Rolled back to the chain a catch-up started from, after the first block retired it: the requests of
// that chain must not be taken last or dropped at the limit.
func TestStoreQueue_ACatchUpThatFailedLeavesItsRestoredChainWanted(t *testing.T) {
	q := NewStoreQueue(StoreRequestBytes(chainRequest(t, 1))) // room for one request
	f := newCatchUpQueueFixture(t, false, WithStoreQueue(q), WithStoredSet(NewStoredSet()))

	small, err := f.stp.newSubtree(8)
	require.NoError(t, err)
	f.stp.currentSubtree.Store(small)
	addStoredPrefixTxs(t, f.stp, 8*5)

	chained := append([]*subtreepkg.Subtree(nil), f.stp.chainedSubtrees...)
	require.GreaterOrEqual(t, len(chained), 4)

	require.Error(t, f.stp.reorgBlocks(t.Context(), []*model.Block{}, []*model.Block{f.block1, f.block2}), "block2's subtree is missing")

	for i, st := range chained {
		it := q.entry(st)
		require.NotNil(t, it, "restored subtree %d has a request", i)
		require.False(t, it.stale, "and it is wanted")
	}
}

// A dropped request never stores, so its callback never runs; the processor cleans up for it, before
// the subtree can be unmapped.
func TestStoreQueue_ADroppedRequestsDeletedTxsAreCleanedUp(t *testing.T) {
	q := NewStoreQueue(StoreRequestBytes(chainRequest(t, 1))) // room for one request
	stp, _ := newStoredPrefixProcessor(t, 8, NewStoredSet(), func(stp *SubtreeProcessor) { stp.storeQueue = q })
	addStoredPrefixTxs(t, stp, 8*5)
	require.GreaterOrEqual(t, len(stp.chainedSubtrees), 4)

	hash := stp.chainedSubtrees[3].Nodes[1].Hash
	stp.deletedTxs.Set(hash, subtreepkg.TxInpoints{})

	before := q.Len()
	stp.retireChainedSubtrees() // at the limit: the waiting requests of the retired chain are dropped
	require.Less(t, q.Len(), before)

	stp.closeRetiredSubtrees()

	_, held := stp.deletedTxs.Get(hash)
	require.False(t, held, "the dropped request's deletedTxs entry is gone")
}

func TestStoreQueue_AZeroByteRequestDoesNotWaitBehindOneThatDoesNotFit(t *testing.T) {
	q := NewStoreQueue(StoreRequestBytes(chainRequest(t, 1)))
	a, b := chainRequest(t, 1), chainRequest(t, 2)
	q.Push(a)
	q.Push(b) // does not fit: waits
	announce := NewSubtreeRequest{Subtree: b.Subtree, AnnounceOnly: true}
	q.Push(announce)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	first, _, ok := q.Take(ctx)
	require.True(t, ok)
	require.Same(t, a.Subtree, first.Subtree)

	second, _, ok := q.Take(ctx)
	require.True(t, ok)
	require.True(t, second.AnnounceOnly, "the announcement holds nothing, so it did not wait for room")
}

// With a queue enforceBlockMaxSize waits for nothing: in an outage the first attempt of its new
// subtrees would not end for as long as the retries hold the limit.
func TestStoreQueue_EnforceBlockMaxSizeDoesNotWaitForTheStores(t *testing.T) {
	q := NewStoreQueue(1 << 40) // nothing takes a request
	stp, _ := newBlockMaxSizeProcessor(t, 64, model.BlockHeaderSize+blockMaxSizeTestBudget, WithStoreQueue(q))
	addBlockMaxSizeTestTxs(t, stp, sizes(200, blockMaxSizeTestTxSize)...)
	require.Greater(t, stp.chainedSubtrees[0].SizeInBytes, uint64(blockMaxSizeTestBudget), "precondition: first subtree must be oversized")

	done := make(chan error, 1)

	go func() { done <- stp.enforceBlockMaxSize(t.Context()) }()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(20 * time.Second):
		t.Fatal("enforceBlockMaxSize waited for a store nothing performs")
	}
}

// A subtree whose request is still queued or being retried is not sent again, by the announce after
// the marks or by the watch.
func TestStoreQueue_ASubtreeStillBeingStoredIsNotSentAgain(t *testing.T) {
	q := NewStoreQueue(1 << 40)
	stp, _ := newStoredPrefixProcessor(t, 8, NewStoredSet(), func(stp *SubtreeProcessor) { stp.storeQueue = q })
	addStoredPrefixTxs(t, stp, 8*5)
	require.GreaterOrEqual(t, len(stp.chainedSubtrees), 4)

	before := q.Len()

	require.NoError(t, stp.announceChainedSubtrees(context.Background(), "test"))
	require.Equal(t, before, q.Len(), "the announce did not send a second store")

	stp.sendChainedSubtreeAgain(0, chainRoot(stp, 0), time.Now())
	require.Equal(t, before, q.Len(), "the watch did not send it again either")
}

// A request dropped while the reorg waits for it did not store: with no deadline the reorg used to
// count its ErrChan answer as stored.
func TestStoreQueue_AReorgWaitingForADroppedRequestReportsItNotStored(t *testing.T) {
	q := NewStoreQueue(StoreRequestBytes(chainRequest(t, 1)))
	stp, _ := newStoredPrefixProcessor(t, 8, NewStoredSet(), func(stp *SubtreeProcessor) { stp.storeQueue = q })
	addStoredPrefixTxs(t, stp, 8*5)
	require.GreaterOrEqual(t, len(stp.chainedSubtrees), 4)

	done := make(chan error, 1)

	go func() { done <- stp.awaitQueuedStores(context.Background(), "test", 0) }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	time.Sleep(200 * time.Millisecond) // the reorg is waiting for the first request

	// the first request stores; the second is admitted when it settles, and is then dropped
	first, _, ok := q.Take(ctx)
	require.True(t, ok)
	first.Answered(nil)
	first.Settled()

	require.Eventually(t, func() bool { return q.Len() > 0 && q.Held() > 0 }, 5*time.Second, 5*time.Millisecond)
	q.Stale(stp.chainedSubtrees[1])

	select {
	case err := <-done:
		require.ErrorContains(t, err, "dropped")
	case <-time.After(10 * time.Second):
		t.Fatal("the reorg kept waiting for a dropped request")
	}
}
