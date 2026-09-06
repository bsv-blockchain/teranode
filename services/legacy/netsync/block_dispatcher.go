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

// inflightParent is what the dispatcher resolved for a block whose parent is still in
// the window: the parent's height and its frontier entry. nil means "look the parent
// up in the blockchain store as before" — the pre-window behaviour, and what every
// caller other than the dispatcher itself still passes.
type inflightParent struct {
	height uint32
	entry  *frontierEntry
}

// blockDispatch is what handleBlockMsgHead produced: one queued block that passed every
// pre-check, with its parent resolved and its route decided. It carries the state the
// chain-order tail needs so the tail can run long after the head did.
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

	// The default tail keeps the backlog decrement, its progress stamp and the reply
	// paired on every completion path, in that order, exactly as the pre-window
	// consumer did.
	bd.tail = func(d *blockDispatch, err error) error {
		terr := sm.handleBlockMsgTail(d, err)

		sm.blockBacklog.Add(-1)
		sm.noteBacklogProgress()

		if d.msg.reply != nil {
			d.msg.reply <- terr
		}

		return terr
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
func (bd *blockDispatcher) tailHeight() uint32 {
	if n := len(bd.frontier); n > 0 {
		return bd.frontier[n-1].height
	}

	return 0
}

// parentFor returns the frontier tail as the in-flight parent for a block whose prevHash
// matches it, else nil. Only the tail can be a parent: the frontier is a chain, so any
// earlier entry is an ancestor of a block already admitted.
func (bd *blockDispatcher) parentFor(prevHash *chainhash.Hash) *inflightParent {
	if bd == nil {
		return nil
	}

	if n := len(bd.frontier); n > 0 && bd.frontier[n-1].hash.IsEqual(prevHash) {
		e := bd.frontier[n-1]

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

// dispatch charges the budget, appends the frontier entry and starts the worker.
func (bd *blockDispatcher) dispatch(d *blockDispatch) {
	e := &frontierEntry{
		hash:       d.msg.blockHash,
		height:     d.height,
		rpcStarted: make(chan struct{}),
		settled:    make(chan struct{}),
		d:          d,
	}

	bd.frontier = append(bd.frontier, e)
	bd.inflight += d.bytes * windowBytesPerWireByte

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

	run := bd.run
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

		_ = bd.tail(head.d, err)
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
