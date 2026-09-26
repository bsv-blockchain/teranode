package netsync

import (
	"context"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
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

// frontierEntry is the dispatcher's per-block bookkeeping record for a parked block
// whose commit may still be in flight. The dispatcher writes hash/height once, and the
// worker closes settled and sets failed/aborted/err as the block's work finishes.
type frontierEntry struct {
	hash   chainhash.Hash
	height uint32

	// settled closes exactly once, when this block's outcome (success or failure)
	// is fully known.
	settled chan struct{}

	// settleOnce guards settled: the worker settles its own entry and the dispatcher
	// settles it again defensively when the completion is processed.
	settleOnce sync.Once

	failed  atomic.Bool
	aborted atomic.Bool
	err     error

	// d is the dispatch this entry tracks, so complete can run its tail and release
	// its budget charge without a second lookup.
	d *blockDispatch
}

// settle records this block's outcome and closes settled, exactly once. The worker
// calls it after run returns and before it hands the completion to the dispatcher.
func (e *frontierEntry) settle(err error) {
	e.settleOnce.Do(func() {
		e.err = err

		if err != nil {
			e.failed.Store(true)
		}

		close(e.settled)
	})
}

// blockDispatch is what handleBlockMsgHead produced: one queued block that passed every
// pre-check, with its parent resolved as stored or in flight and its route decided. It
// carries the state the chain-order tail needs so the tail can run long after the head
// did, and none of that state is the decoded block: msgBlock is released the moment the
// worker returns, and the tail must never want it. A block whose parent is neither
// stored nor in flight never becomes a dispatch at all; the head parks it while it
// still holds the bytes.
type blockDispatch struct {
	isCheckpoint bool
	height       uint32

	windowed bool
	bytes    int64

	// parked is the park entry this dispatch commits, and nil for a block that
	// arrived on the wire. A parked dispatch carries no queue message worth
	// replying to and no peer obligation to settle: its blob is read by the
	// worker, its outcome is classified by the park's disposition table, and its
	// bytes were charged to the park rather than to the prefetch budget. It is
	// the field that selects run and tail below, so the two kinds of dispatch
	// cannot share a code path by accident.
	parked *parkedBlock

	// parkedIsCheckpoint is what isCheckpointHash answered for a parked
	// dispatch's own hash, computed at dispatch rather than at commit and
	// passed through to parkedBlockCommitted's tail.
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
// inflight, barrier and baState are owned by that one goroutine and never touched from
// a worker, so they need no lock. frontier used to be the same, until the wanted-range
// pass gained a second caller: the park sweep's own goroutine now calls fetchHeaderBlocks
// directly (runParkSweep, block_park_drain.go), which reaches inFlight below to check
// whether a candidate's parent is being retried right now. That is a read of frontier
// from a goroutine that is not the consumer, running concurrently with the consumer's own
// dispatch and complete, which append to and pop from the same slice — a data race,
// caught by go test -race on TestBlockHandler_TheSweepGoroutinePostsAndTheConsumerCommits
// and TestSyncManager_TheBlockHandlerRunsTheParkSweep. frontierMu is the fix: every read
// and write of frontier takes it, for exactly as long as the read or write itself, and
// never across a call back into tail code (handleBlockMsgTail, by way of fetchHeaderBlocks,
// can itself call back into inFlight, so complete must release the lock before invoking a
// dispatch's tail or it would deadlock against itself on the same goroutine).
type blockDispatcher struct {
	sm     *SyncManager
	depth  int
	budget int64

	// frontierMu guards frontier alone. See the struct comment for why it exists and
	// the reentrancy rule complete() observes to avoid deadlocking on its own tail call.
	frontierMu  sync.Mutex
	frontier    []*frontierEntry
	inflight    int64
	barrier     bool
	completions chan *blockCompletion
	baState     cachedBAState

	// parkedRun does one parked block's work and parkedTail runs its chain-order
	// bookkeeping. They are fields so the dispatcher tests can drive it without a
	// peer or a store.
	parkedRun  func(ctx context.Context, d *blockDispatch) error
	parkedTail func(d *blockDispatch, err error) error
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

	// The parked run: read the converted record on the worker, then the same call
	// the serial drain makes.
	//
	// A nil in-flight parent, always. The parent of a parked block is in the chain
	// by the time anything commits it, so HandleConvertedBlock looks it up there,
	// and that lookup is what enforces "never hand block validation a parentless
	// block" in the worker rather than on a promise from the consumer.
	bd.parkedRun = func(ctx context.Context, d *blockDispatch) error {
		record, err := sm.blockPark.ReadConverted(ctx, d.parked.hash)
		if err != nil {
			// Recorded apart from the returned error so the tail cannot classify a
			// read failure by the commit table, which judges the block.
			d.readErr = err

			return err
		}

		return sm.HandleConvertedBlock(ctx, d.parked.peer, d.parked.hash, record)
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

// drainFrontier empties frontier under frontierMu and returns what was in it. It exists
// for dispatchBlocks' shutdown drain, the one place outside this file that used to read
// and clear bd.frontier directly, racing the same way dispatch and complete did against
// a concurrent inFlight call from the park sweep's goroutine. The lock is released before
// the caller does anything with the returned entries: nothing here needs it held that
// long, and holding it across a callback is how complete's own lock earned its comment
// about never doing that.
func (bd *blockDispatcher) drainFrontier() []*frontierEntry {
	bd.frontierMu.Lock()
	defer bd.frontierMu.Unlock()

	entries := bd.frontier
	bd.frontier = nil

	return entries
}

// frontierEmpty reports whether nothing is in flight. Nil-safe: tests build SyncManager
// as a struct literal that bypasses New(), so sm.dispatcher can be nil.
func (bd *blockDispatcher) frontierEmpty() bool {
	if bd == nil {
		return true
	}

	bd.frontierMu.Lock()
	defer bd.frontierMu.Unlock()

	return len(bd.frontier) == 0
}

// frontierLen reports how many entries are in flight, under frontierMu. A plain
// len(bd.frontier) anywhere else in this file is the race this exists to close.
func (bd *blockDispatcher) frontierLen() int {
	bd.frontierMu.Lock()
	defer bd.frontierMu.Unlock()

	return len(bd.frontier)
}

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
	bd.frontierMu.Lock()
	defer bd.frontierMu.Unlock()

	if n := len(bd.frontier); n > 0 {
		return bd.frontier[n-1].height
	}

	return 0
}

// inFlight reports whether this hash is a block the dispatcher is working on right now.
// The recently-failed-parent check consults it first: a parent that is being retried is
// not a failed parent, so its child must not be short-circuited as part of a cascade.
//
// Called from two goroutines: the consumer, indirectly through the live and drain
// paths, and the park sweep's own goroutine, through fetchHeaderBlocks. frontierMu is
// what makes that safe against dispatch and complete, which append to and pop from the
// same slice on the consumer.
func (bd *blockDispatcher) inFlight(hash chainhash.Hash) bool {
	if bd == nil {
		return false
	}

	bd.frontierMu.Lock()
	defer bd.frontierMu.Unlock()

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

	if bd.frontierLen() >= bd.effectiveDepth() {
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
	return d.parked.hash
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
	if !d.windowed && !bd.frontierEmpty() {
		bd.sm.logger.Errorf("[blockDispatcher][%s] refusing a parked dispatch in the wrong shape: windowed=%v frontier=%d", d.parked.hash.String(), d.windowed, bd.frontierLen())
		bd.sm.blockPark.Restore(*d.parked)

		return
	}

	hash := d.msgHash()

	e := &frontierEntry{
		hash:    hash,
		height:  d.height,
		settled: make(chan struct{}),
		d:       d,
	}

	bd.frontierMu.Lock()
	bd.frontier = append(bd.frontier, e)
	bd.frontierMu.Unlock()

	bd.inflight += d.bytes * windowBytesPerWireByte

	if d.isCheckpoint {
		bd.barrier = true
	}

	go func() {
		var err error

		if e.aborted.Load() {
			err = errors.NewServiceError("[blockDispatcher][%s] not started at height %d: a predecessor failed", e.hash.String(), e.height)
		} else {
			err = bd.parkedRun(bd.sm.ctx, d)
		}

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
//
// Each turn of the loop takes frontierMu only for the peek-classify-pop that touches
// frontier, and releases it before calling the dispatch's own tail. That release is load-
// bearing, not tidiness: the tail can call back into fetchHeaderBlocks (a checkpoint or an
// ordinary top-up both can), which reaches inFlight below, and inFlight takes frontierMu
// itself. Holding the lock across the tail call would deadlock complete against itself on
// this same goroutine the first time that path fired.
func (bd *blockDispatcher) complete(c *blockCompletion) {
	c.entry.settle(c.err)

	for {
		bd.frontierMu.Lock()

		if len(bd.frontier) == 0 {
			bd.frontierMu.Unlock()

			return
		}

		head := bd.frontier[0]

		select {
		case <-head.settled:
		default:
			bd.frontierMu.Unlock()

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
			// Caller (this function) holds frontierMu; failFrom reads frontier
			// directly on that strength.
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

		bd.frontierMu.Unlock()

		_ = bd.parkedTail(head.d, err)
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
//
// The caller must hold frontierMu; this is complete's only caller and it always does.
func (bd *blockDispatcher) failFrom(e *frontierEntry) {
	for _, x := range bd.frontier {
		if x == e {
			continue
		}

		x.aborted.Store(true)
	}
}
