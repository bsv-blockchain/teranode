package netsync

import (
	"context"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
)

const (
	// windowBytesPerWireByte is what one serialized block byte is charged against the
	// window budget: the decoded transactions, the transaction map, the subtree data
	// and block validation's own copy all live at once while a block is in flight.
	windowBytesPerWireByte = 4

	// defaultWindowBudget is the fallback byte budget when the operator set none and
	// the process runs with no Go memory limit.
	defaultWindowBudget = 512 << 20

	// baStateCacheTTL bounds how often the dispatcher asks block assembly for its
	// height. The answer only steers the effective depth, so a quarter of a second
	// of staleness is free while a per-block RPC would not be.
	baStateCacheTTL = 250 * time.Millisecond
)

// frontierEntry is the dispatcher's per-block bookkeeping record for a below-checkpoint
// block whose UTXO store work may still be in flight. HandleBlockDirect's ordering
// hand-shake (the wait immediately before sm.ProcessBlock) reads a predecessor's entry
// to decide whether it is safe to start its own RPC; the dispatcher writes hash/height
// once and closes rpcStarted/settled and sets failed/aborted/err as the block's work
// progresses and finishes.
type frontierEntry struct {
	hash   chainhash.Hash
	height uint32

	// rpcStarted closes exactly once, when this block's own ProcessBlock RPC begins.
	// A child waiting on its parent treats a closed rpcStarted as "safe to start
	// mine": from that point the parent's spends are already queued behind its own
	// create in commit order, so a spend of a coin the parent creates can never
	// land ahead of that create.
	rpcStarted chan struct{}

	// rpcStartedOnce guards rpcStarted so a retry or a racing caller can never
	// double-close it.
	rpcStartedOnce sync.Once

	// settled closes exactly once, when this block's outcome (success or failure)
	// is fully known. A child that observes settled before rpcStarted learns the
	// parent never reached its own RPC, so the ordering guarantee rpcStarted would
	// have given never held — see the hand-shake in HandleBlockDirect for why that
	// case still lets the child proceed rather than blocking forever.
	settled chan struct{}

	// settleOnce guards settled the way rpcStartedOnce guards rpcStarted: the worker
	// settles its own entry and the dispatcher settles it again defensively when the
	// completion is processed.
	settleOnce sync.Once

	failed  atomic.Bool
	aborted atomic.Bool
	err     error

	// d is the dispatch this entry tracks, so complete can run its tail and release
	// its budget charge without a second lookup.
	d *blockDispatch
}

// markRPCStarted closes rpcStarted exactly once, no matter how many times or from how
// many goroutines it is called.
func (e *frontierEntry) markRPCStarted() {
	e.rpcStartedOnce.Do(func() {
		close(e.rpcStarted)
	})
}

// settle records this block's outcome and closes settled, exactly once. The worker
// calls it after run returns and before it hands the completion to the dispatcher, so
// in program order rpcStarted (closed inside HandleBlockDirect) always closes strictly
// before settled — which is what makes the hand-shake's rpcStarted-wins pre-check
// sound: a successor can never see settled without also seeing a closed rpcStarted for
// a predecessor that did reach its RPC.
func (e *frontierEntry) settle(err error) {
	e.settleOnce.Do(func() {
		e.err = err

		if err != nil {
			e.failed.Store(true)
		}

		close(e.settled)
	})
}

// frontierEntryContextKey is the unexported key type under which a block's own
// frontierEntry travels on its processing context, so HandleBlockDirect's ordering
// hand-shake can mark it started without threading an extra parameter through every
// call between the dispatcher and ProcessBlock.
type frontierEntryContextKey struct{}

// contextWithFrontierEntry returns a copy of ctx carrying e as the current block's own
// frontier entry.
func contextWithFrontierEntry(ctx context.Context, e *frontierEntry) context.Context {
	return context.WithValue(ctx, frontierEntryContextKey{}, e)
}

// frontierEntryFromContext returns the frontierEntry stashed by contextWithFrontierEntry,
// or nil if none was stashed — the normal case for every route except the dispatcher's.
func frontierEntryFromContext(ctx context.Context) *frontierEntry {
	e, _ := ctx.Value(frontierEntryContextKey{}).(*frontierEntry)
	return e
}

// inflightParent is what the head resolved for a block's parent: the parent's
// height, and its frontier entry when the parent is still in the window. A nil
// entry with a height is a parent the head found in the chain, so HandleBlockDirect
// takes the height and skips the lookup the head has just made; the ordering
// hand-shake is guarded on the entry and never waits on a stored parent. A nil
// inflightParent means "look the parent up in the blockchain store" — the
// pre-window behaviour, and what the park drain and every caller other than the
// head still pass.
type inflightParent struct {
	height uint32
	entry  *frontierEntry
}

// blockDispatch is what handleBlockMsgHead produced: one queued block that passed every
// pre-check, with its parent resolved as stored or in flight and its route decided. It
// carries the state the chain-order tail needs so the tail can run long after the head
// did, and none of that state is the decoded block: msgBlock is released the moment the
// worker returns, and the tail must never want it. A block whose parent is neither
// stored nor in flight never becomes a dispatch at all; the head parks it while it
// still holds the bytes.
type blockDispatch struct {
	msg            *blockQueueMsg
	peer           *peerpkg.Peer
	state          *peerSyncState
	msgBlock       *wire.MsgBlock
	prevHash       chainhash.Hash
	catchingBlocks bool
	isCheckpoint   bool
	height         uint32
	parent         *inflightParent
	windowed       bool
	bytes          int64

	// removedFront is the header node this block's arrival took off the front of
	// the headers-first list, or nil when it was not the front. The tail needs it
	// to put the block back into the download walk when the block fails or is
	// aborted: by then the header is gone from both the list and the index, so
	// nothing else can find it. It is an 80-byte header, not the decoded block.
	removedFront *headerNode

	// parked is the park entry this dispatch commits, and nil for a block that
	// arrived on the wire. A parked dispatch carries no queue message worth
	// replying to and no peer obligation to settle: its blob is read by the
	// worker, its outcome is classified by the park's disposition table, and its
	// bytes were charged to the park rather than to the prefetch budget. It is
	// the field that selects run and tail below, so the two kinds of dispatch
	// cannot share a code path by accident.
	parked *parkedBlock

	// parkedIsCheckpoint is what advanceHeaderListFor answered for a parked
	// dispatch when its header node was taken off the front, which happens at
	// dispatch rather than at commit. By the time the tail runs the front has
	// moved on and the question can no longer be asked.
	parkedIsCheckpoint bool

	// readErr is a parked dispatch's blob-read failure, kept apart from the
	// completion error because the two are classified by opposite defaults: a
	// read failure keeps the block, a commit failure judges it and blames a
	// peer. Collapsing them loses fully downloaded blocks under ordinary store
	// load.
	readErr error

	// aborted is set by complete before the tail runs when this block was never at
	// fault — a predecessor failed. The tail reads it to skip the failure backoff.
	// Written and read on the consumer goroutine only.
	aborted bool
}

// blockCompletion is one worker's outcome, handed back to the consumer goroutine.
type blockCompletion struct {
	d     *blockDispatch
	entry *frontierEntry
	err   error
}

// cachedBAState is block assembly's last observed height and when it was observed.
type cachedBAState struct {
	at     time.Time
	height uint32
	ok     bool
}

// blockDispatcher turns legacy sync's single block-queue consumer into a dispatcher:
// up to K consecutive below-checkpoint blocks have their UTXO store work in flight at
// once, while every chain-order step stays in dispatch order on the consumer goroutine.
//
// Everything except completions is owned by that one goroutine — frontier, inflight,
// barrier and baState are never touched from a worker, so the dispatcher needs no lock.
type blockDispatcher struct {
	sm          *SyncManager
	depth       int
	budget      int64
	frontier    []*frontierEntry
	inflight    int64
	barrier     bool
	completions chan *blockCompletion
	baState     cachedBAState

	// run does one block's work (HandleBlockDirect by default) and tail runs the
	// chain-order bookkeeping (handleBlockMsgTail plus the backlog/reply pairing).
	// Both are fields so the dispatcher tests can drive it without a peer or a store.
	run  func(ctx context.Context, d *blockDispatch, parent *inflightParent) error
	tail func(d *blockDispatch, err error) error

	// parkedRun and parkedTail are the same two steps for a dispatch that commits
	// a parked block off disk. They are separate fields rather than branches
	// inside run and tail because a test that swaps run must not silently take
	// over the blob read as well, and because a parked dispatch must never be
	// able to reach handleBlockMsgTail or finishBlockMsg: it has no queue
	// message, so a reply would send on a nil channel and a backlog decrement
	// would underflow the counter that suppresses the sync-peer stall check.
	parkedRun  func(ctx context.Context, d *blockDispatch) error
	parkedTail func(d *blockDispatch, err error) error
}

// runFor picks the work step for one dispatch, and tailFor picks its bookkeeping
// step. The parked field is the only discriminator, in one place, so the two kinds
// of dispatch cannot drift apart.
func (bd *blockDispatcher) runFor(d *blockDispatch) func(context.Context, *blockDispatch, *inflightParent) error {
	if d.parked != nil {
		return func(ctx context.Context, d *blockDispatch, _ *inflightParent) error {
			return bd.parkedRun(ctx, d)
		}
	}

	return bd.run
}

func (bd *blockDispatcher) tailFor(d *blockDispatch) func(*blockDispatch, error) error {
	if d.parked != nil {
		return bd.parkedTail
	}

	return bd.tail
}

func newBlockDispatcher(sm *SyncManager) *blockDispatcher {
	bd := &blockDispatcher{
		sm:          sm,
		depth:       1,
		budget:      windowBudgetBytes(0),
		completions: make(chan *blockCompletion, 64),
	}

	// Depth 1 with the default budget is the pre-window behaviour, and it is what a
	// SyncManager built as a struct literal in a test gets.
	if sm.settings != nil {
		bd.budget = windowBudgetBytes(sm.settings.BlockValidation.QuickWindowBudgetMiB)

		// One rule for both services: block validation's quickWindowDepth resolves the
		// same setting through this same helper, so legacy can never overlap blocks
		// block validation would refuse to admit.
		depth, reasons := sm.settings.BlockValidation.QuickWindowConfiguredDepth()

		// One startup line, the mirror of block validation's, so a mismatch between the
		// two services is visible in the log rather than only in a diverted block.
		switch {
		case depth == 0:
			// The dispatcher is built but never fed: dispatchBlocks hands the queue to the
			// pre-window consumer instead. bd.depth is left at 1 so nothing can read a 0
			// here as a window that admits nothing.
			sm.logger.Infof("[blockDispatcher] blockvalidation_quick_window_blocks=0: the quick window is off and the block queue is consumed the pre-window way, one block head to tail")
		case len(reasons) > 0:
			bd.depth = depth

			sm.logger.Warnf("[blockDispatcher] blockvalidation_quick_window_blocks=%d resolved to depth %d: %s", sm.settings.BlockValidation.QuickWindowBlocks, bd.depth, strings.Join(reasons, "; "))
		default:
			bd.depth = depth

			sm.logger.Infof("[blockDispatcher] blockvalidation_quick_window_blocks=%d resolved to depth %d", sm.settings.BlockValidation.QuickWindowBlocks, bd.depth)
		}
	}

	// d.msg.peer, not d.peer: HandleBlockDirect has always been handed the peer that
	// delivered the block (a stream sub-peer on an association), and its tracing tags
	// and log lines name it. d.peer is that peer resolved to its association's primary,
	// which is what the tail's own bookkeeping needs — the two must not be swapped.
	bd.run = func(ctx context.Context, d *blockDispatch, parent *inflightParent) error {
		return sm.HandleBlockDirect(ctx, d.msg.peer, d.msg.blockHash, d.msgBlock, parent)
	}

	// The default tail is the pre-window consumer's whole turn after the block's own
	// work: the chain-order tail, then the drain of whatever was parked behind a block
	// that committed, then the backlog accounting and the reply through finishBlockMsg,
	// so both consumers settle a block by the same rule. The committed guard is
	// load-bearing: the tail returns nil from paths that did NOT put the block in the
	// chain, and draining after one of those would try to commit the children of a
	// block that is not there.
	//
	// scheduleDrain rather than drainParkedDescendants, and that is the whole of
	// this change from the consumer's point of view: with a loop to admit into,
	// the parked children become dispatches of their own, and the reply below
	// lands microseconds after this block's own tail work instead of after every
	// block parked behind it has been read off disk and validated.
	bd.tail = func(d *blockDispatch, err error) error {
		terr := sm.handleBlockMsgTail(d, err)

		if terr == nil && d.msg.committed {
			sm.scheduleDrain(d.msg.blockHash, d.height)
		}

		sm.finishBlockMsg(d.msg, terr)

		return terr
	}

	// The parked run: read the blob on the worker, then the same call the serial
	// drain makes. The decoded block lives in this worker's local, so the consumer
	// never holds one.
	//
	// A nil in-flight parent, always. The parent of a parked block is in the chain
	// by the time anything commits it, so HandleBlockDirect looks it up there, and
	// that lookup is what enforces "never hand block validation a parentless
	// block" in the worker rather than on a promise from the consumer. Handing it
	// a resolved parent instead would skip the lookup and is the single most
	// dangerous edit anyone can make here.
	bd.parkedRun = func(ctx context.Context, d *blockDispatch) error {
		msgBlock, err := sm.blockPark.Read(ctx, d.parked.hash)
		if err != nil {
			// Recorded apart from the returned error so the tail cannot classify a
			// read failure by the commit table, which judges the block.
			d.readErr = err

			return err
		}

		return sm.HandleBlockDirect(ctx, d.parked.peer, d.parked.hash, msgBlock, nil)
	}

	// The parked tail: classify in one place, on the consumer, in admission order,
	// out of the same three helpers the serial drain uses. It never touches the
	// backlog and never replies, because a parked dispatch has no queue message.
	bd.parkedTail = func(d *blockDispatch, err error) error {
		entry := *d.parked

		switch {
		case d.readErr != nil:
			sm.parkedReadFailed(entry, d.readErr)

		case err != nil:
			sm.parkedBlockFailed(entry, err)

		default:
			sm.parkedBlockCommitted(entry, d.parkedIsCheckpoint)

			// The chain continues: whatever was parked behind this block is now
			// committable. Scheduled here rather than inside parkedBlockCommitted,
			// because the serial path calls that too and would turn its explicit
			// stack walk back into recursion, one frame set per link of a chain
			// that can be thousands long, each frame holding a decoded block.
			sm.scheduleDrain(entry.hash, d.height)
		}

		return err
	}

	return bd
}

// windowBudgetBytes resolves the configured budget: the operator's MiB when set, else a
// tenth of the Go memory limit, else a fixed fallback.
func windowBudgetBytes(mib int) int64 {
	if mib > 0 {
		return int64(mib) << 20
	}

	// SetMemoryLimit(-1) reads the limit without changing it; math.MaxInt64 is what
	// the runtime reports when no limit is set, so anything near it means "unset".
	if limit := debug.SetMemoryLimit(-1); limit > 0 && limit < 1<<62 {
		return limit / 10
	}

	return defaultWindowBudget
}

// effectiveDepth is the configured depth, reduced by block assembly's observed lag so a
// window block never parks in the block-assembly gate, and by the download-side dynamic
// in-flight limit so a fat-block era collapses the window as it collapses the fetch depth.
func (bd *blockDispatcher) effectiveDepth() int {
	depth := bd.depth

	if bd.sm.blockSizeTracker != nil {
		if fetch := bd.sm.blockSizeTracker.calculateMaxInFlightBlocks(); fetch >= 1 && fetch < depth {
			depth = fetch
		}
	}

	if depth > 1 && bd.sm.blockAssembly != nil {
		if baHeight, ok := bd.blockAssemblyHeight(); ok {
			lag := 0
			if tip := bd.tailHeight(); tip > baHeight {
				lag = int(tip - baHeight)
			}

			// Two blocks of slack: the gate compares against the block being admitted,
			// not the frontier tail, and a rounded-down lag must not put the last
			// admitted block on the gate's threshold.
			if room := bd.sm.settings.BlockValidation.MaxBlocksBehindBlockAssembly - lag - 2; room < depth {
				depth = room
			}
		}
	}

	if depth < 1 {
		depth = 1
	}

	return depth
}

// blockAssemblyHeight returns block assembly's chain tip, cached for baStateCacheTTL.
// A failed or empty answer is cached too, so a block assembly that is down does not
// cost one timing-out RPC per admission.
func (bd *blockDispatcher) blockAssemblyHeight() (uint32, bool) {
	if !bd.baState.at.IsZero() && time.Since(bd.baState.at) < baStateCacheTTL {
		return bd.baState.height, bd.baState.ok
	}

	ctx, cancel := context.WithTimeout(bd.sm.ctx, time.Second)
	defer cancel()

	state, err := bd.sm.blockAssembly.GetBlockAssemblyState(ctx)

	bd.baState = cachedBAState{at: time.Now()}

	if err != nil || state == nil {
		return 0, false
	}

	bd.baState.height = state.CurrentHeight
	bd.baState.ok = true

	return bd.baState.height, true
}

// frontierEmpty reports whether nothing is in flight. Nil-safe: tests build SyncManager
// as a struct literal that bypasses New(), so sm.dispatcher can be nil.
func (bd *blockDispatcher) frontierEmpty() bool { return bd == nil || len(bd.frontier) == 0 }

// tailHeight is the height of the last block admitted, or 0 when nothing is in flight.
// The zero means the block-assembly lag arm of effectiveDepth sees a lag of 0 until the
// first admission, so at most one block can be admitted on a stale reading of the lag;
// that block then parks in the block-assembly gate exactly as it would have before the
// window existed, and every later admission sees the real frontier tail.
//
// A frontier entry whose height was never resolved reads as that same zero, which is the
// right answer for it and needs no special case: such an entry is dispatched un-windowed,
// so canDispatch admits it only into an empty frontier and it is the only entry there.
func (bd *blockDispatcher) tailHeight() uint32 {
	if n := len(bd.frontier); n > 0 {
		return bd.frontier[n-1].height
	}

	return 0
}

// parentFor returns the frontier tail as the in-flight parent for a block whose prevHash
// matches it, else nil. Only the tail can be a parent: the frontier is a chain, so any
// earlier entry is an ancestor of a block already admitted.
//
// A tail whose height is 0 is refused. Height 0 means "not resolved", not "the genesis
// block": two arms of resolveParent dispatch a block without resolving its height, the
// one for a block already in the chain and the one for a block whose parent is in the
// frontier but is not its tail, and dispatch stamps that zero onto the entry. Handing it
// over as a parent gives the child height 1, and nothing downstream on this side catches
// it, because HandleBlockDirect's mismatch guard needs a height the block claims and a
// legacy wire block claims none. Block validation does catch it, in deriveBlockHeight,
// and answers BlockInvalidError, which is not a transient fault: the block is rejected to
// the peer, the peer's whole association is evicted as misbehaving, and every descendant
// is suppressed for the cascade TTL. So the answer here is no parent, which sends the
// child down the nil-parent route where the worker looks the parent up in the chain.
func (bd *blockDispatcher) parentFor(prevHash *chainhash.Hash) *inflightParent {
	if bd == nil {
		return nil
	}

	if n := len(bd.frontier); n > 0 && bd.frontier[n-1].hash.IsEqual(prevHash) {
		e := bd.frontier[n-1]

		if e.height == 0 {
			bd.sm.logger.Warnf("[blockDispatcher][%s] the block in flight has no resolved height, so it is not offered as a parent; its child will look the parent up in the chain", e.hash.String())

			return nil
		}

		return &inflightParent{height: e.height, entry: e}
	}

	return nil
}

// inFlight reports whether this hash is a block the dispatcher is working on right now.
// The recently-failed-parent check consults it first: a parent that is being retried is
// not a failed parent, so its child must not be short-circuited as part of a cascade.
func (bd *blockDispatcher) inFlight(hash chainhash.Hash) bool {
	if bd == nil {
		return false
	}

	for _, e := range bd.frontier {
		if e.hash.IsEqual(&hash) {
			return true
		}
	}

	return false
}

// canDispatch reports whether this block can start now.
func (bd *blockDispatcher) canDispatch(d *blockDispatch) bool {
	// A checkpoint block's tail switches headers-first state over to the next
	// checkpoint, so nothing may be admitted behind it until that tail has run.
	if bd.barrier {
		return false
	}

	// Anything not on the window route runs exactly as it did before the window
	// existed: one block at a time, into an empty frontier.
	if !d.windowed {
		return bd.frontierEmpty()
	}

	if len(bd.frontier) >= bd.effectiveDepth() {
		return false
	}

	// A block that alone exceeds what is left of the budget is admitted only into an
	// empty frontier, so the window collapses to one block in a fat-block era rather
	// than refusing the block outright.
	if d.bytes*windowBytesPerWireByte > bd.budget-bd.inflight {
		return bd.frontierEmpty()
	}

	return true
}

// msgHash is the hash of the block a dispatch is for, from whichever of its two
// sources the dispatch has: the queue message for a block off the wire, the park
// entry for a block being committed off disk. A parked dispatch has no queue
// message at all, so every read of d.msg on a path both kinds reach goes through
// here.
func (d *blockDispatch) msgHash() chainhash.Hash {
	if d.parked != nil {
		return d.parked.hash
	}

	return d.msg.blockHash
}

// dispatch charges the budget, appends the frontier entry and starts the worker.
//
// A parked dispatch must arrive in exactly one shape, and the guard is here rather
// than in a comment because the two ways of getting it wrong are both silent. A
// resolved parent would skip the worker's own parent lookup, which is what
// enforces the never-hand-over-a-parentless-block rule. A non-empty frontier
// would mean the server-side window already holds a legacy entry, so an
// unwindowed parked block would be refused admission there. Failing closed costs
// one restored park entry and one ERROR line; failing open costs a lost block.
func (bd *blockDispatcher) dispatch(d *blockDispatch) {
	// A parked dispatch must still arrive with no resolved parent: that is what
	// leaves the worker's own parent lookup in place, which enforces the
	// never-hand-over-a-parentless-block rule.
	//
	// It may now be windowed, and if it is it may arrive alongside another block.
	// The condition that used to demand an empty window was a consequence of
	// never being windowed rather than a rule of its own, and it is what kept the
	// validator idle between every block drained from the park.
	if d.parked != nil && (d.parent != nil || (!d.windowed && !bd.frontierEmpty())) {
		bd.sm.logger.Errorf("[blockDispatcher][%s] refusing a parked dispatch in the wrong shape: parent=%v windowed=%v frontier=%d", d.parked.hash.String(), d.parent != nil, d.windowed, len(bd.frontier))
		bd.sm.blockPark.Restore(*d.parked)

		return
	}

	// The parked kind carries no queue message, so its hash comes off the entry.
	// Reading d.msg unconditionally would dereference nil for exactly the dispatch
	// this function was just taught to accept.
	hash := d.msgHash()

	e := &frontierEntry{
		hash:       hash,
		height:     d.height,
		rpcStarted: make(chan struct{}),
		settled:    make(chan struct{}),
		d:          d,
	}

	bd.frontier = append(bd.frontier, e)
	bd.inflight += d.bytes * windowBytesPerWireByte

	// The window now accounts for this block's memory, so the download budget
	// does not have to. A parked dispatch has no queue message and never held
	// download bytes in the first place.
	noteHandedOff(d.msg)

	if d.isCheckpoint {
		bd.barrier = true
	}

	// A block that waited for capacity while its in-flight parent failed is aborted
	// before it starts: its own attempt could only fail in the ordering hand-shake,
	// and the abort path replies with a service error and records no failure backoff.
	// An in-frontier successor gets the same mark from failFrom instead.
	if d.parent != nil && d.parent.entry != nil && d.parent.entry.failed.Load() {
		e.aborted.Store(true)
	}

	run := bd.runFor(d)
	ctx := contextWithFrontierEntry(bd.sm.ctx, e)

	go func() {
		var err error

		if e.aborted.Load() {
			err = errors.NewServiceError("[blockDispatcher][%s] not started at height %d: a predecessor failed", e.hash.String(), e.height)
		} else {
			err = run(ctx, d, d.parent)
		}

		// Nothing after this point reads the decoded block — the tail works from the
		// dispatch's own fields — and this entry can outlive the frontier as a child's
		// resolved parent, so drop the reference here rather than pinning a multi-GB
		// block (and its decode arena) until that child settles. Safe to write from
		// this goroutine: the consumer never touches msgBlock after dispatch, and the
		// completion send below is the happens-before edge for everything that does.
		d.msgBlock = nil

		// settle after run returns, so rpcStarted (closed inside HandleBlockDirect)
		// always closes strictly before settled in program order.
		e.settle(err)

		select {
		case bd.completions <- &blockCompletion{d: d, entry: e, err: err}:
		case <-bd.sm.quit:
			// Shutdown already replied for every in-flight block; dropping the
			// completion here only keeps this goroutine from parking forever.
		}
	}()
}

// complete records a worker's outcome and runs the ordered tail for every settled entry
// at the head of the frontier. Entries behind a failed one are aborted with a service
// error, so they are never rejected to the peer and never earn a failure backoff.
func (bd *blockDispatcher) complete(c *blockCompletion) {
	c.entry.settle(c.err)

	for len(bd.frontier) > 0 {
		head := bd.frontier[0]

		select {
		case <-head.settled:
		default:
			return
		}

		err := head.err
		head.d.aborted = head.aborted.Load()

		switch {
		case head.d.aborted:
			// The abort is the verdict that reaches the peer, but a successor that had a
			// failure of its own is worth seeing: without this line the only record of it
			// is gone the moment the service error replaces it.
			if head.err != nil {
				bd.sm.logger.Debugf("[blockDispatcher] aborted successor %s had its own error: %v", head.hash.String(), head.err)
			}

			err = errors.NewServiceError("[blockDispatcher][%s] aborted at height %d: a predecessor failed", head.hash.String(), head.height)
		case err != nil && (errors.Is(err, context.Canceled) || errors.IsContextError(err)):
			// Substituted, and deliberately neither wrapping the cause nor quoting its
			// text: handleBlockMsgTail's context branch replies nil and records the
			// block as accepted, which an abandoned block is not, and errors.Is
			// recognises a context error by its message as well as by its code.
			bd.sm.logger.Warnf("[blockDispatcher][%s] block at height %d abandoned mid-flight: %v", head.hash.String(), head.height, err)

			err = errors.NewServiceError("[blockDispatcher][%s] block at height %d abandoned before it finished", head.hash.String(), head.height)
		}

		if err != nil {
			bd.failFrom(head)
		}

		// Clear the slot before resliding: the backing array outlives the reslice, so
		// leaving the pointer there would pin this entry (and everything it reaches)
		// until a later append reallocates — long after the reply released the block's
		// prefetch budget.
		bd.frontier[0] = nil
		bd.frontier = bd.frontier[1:]
		bd.inflight -= head.d.bytes * windowBytesPerWireByte

		if head.d.isCheckpoint {
			bd.barrier = false
		}

		_ = bd.tailFor(head.d)(head.d, err)
	}
}

// failFrom marks every other entry in the frontier aborted: they are all descendants of
// e, so none of them can store, and none of them was ever at fault.
//
// A successor whose own run had already returned nil is still reported failed, because
// complete reads aborted at pop time and lets it win over the recorded error. That is
// only sound because block validation's window never commits a successor after its
// predecessor failed mid-RPC: a run that returned nil under a failed predecessor did not
// commit a block, so calling it failed here matches what the store actually holds.
func (bd *blockDispatcher) failFrom(e *frontierEntry) {
	for _, x := range bd.frontier {
		if x == e {
			continue
		}

		x.aborted.Store(true)
	}
}
