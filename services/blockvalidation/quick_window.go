package blockvalidation

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"golang.org/x/sync/semaphore"
)

// quickWindow lets up to depth consecutive below-checkpoint blocks have their UTXO store work
// in flight at once while every chain-order step still runs in height order.
//
// Three rules make the overlap safe, and each has one home here:
//
//  1. A create never waits and a spend waits for the create that made its coin. Spends of an
//     in-block parent already wait on the two-phase path in quick_validate.go; spends of a coin
//     created by an in-flight PREDECESSOR wait on that predecessor's batchGate, looked up in
//     the open-gate map by parent transaction id. A gate closes when its batch's create side has
//     returned, and every store caller blocks until its statement commits, so a closed gate
//     means a committed create.
//  2. Commits run in admission order, one committer goroutine, and admission accepts only the
//     tail's child, so the chain store never sees height h+1 before h.
//  3. Fail closed: when an entry fails, every later entry is marked aborted with a service
//     error recorded on the entry before anything is signalled, so the block validation call
//     for an aborted block returns that error and legacy sync sees a local fault, never a peer
//     fault. Nothing is committed for the failed block or anything behind it; the store's own
//     create and spend guards make the re-delivery converge on the serial result.
//
// Nothing here is sized by the chain: the open-gate and retained maps hold the transaction ids
// of in-flight blocks only, and legacy sync's byte budget bounds how many blocks that is.
type quickWindow struct {
	logger ulogger.Logger
	depth  int
	commit func(context.Context, *windowEntry) error

	mu      sync.Mutex
	cond    *sync.Cond // signalled when an entry leaves or is admitted
	shut    bool       // set once the committer has stopped; refuses every further Admit
	entries []*windowEntry
	byHash  map[chainhash.Hash]*windowEntry
	// open holds every registered id whose gate has not closed; retained holds every id
	// registered by an entry still in the window, closed or not, for the miss backstop.
	open     map[chainhash.Hash]*batchGate
	retained map[chainhash.Hash]*windowEntry

	callers *semaphore.Weighted // shared store-caller budget across every in-flight block

	wake chan struct{}
}

type windowEntry struct {
	w      *quickWindow
	block  *model.Block
	hash   chainhash.Hash
	height uint32
	parent *windowEntry // nil when the parent was already stored at admission
	added  time.Time

	// peerID identifies which sync peer delivered this block. Set at admission by the caller
	// (task 4); the window itself attaches no behaviour to it.
	peerID string

	ctx    context.Context
	cancel context.CancelFunc

	registered chan struct{} // every batch registered
	idAssigned chan struct{} // block.ID set
	storeDone  chan struct{} // pipeline returned without error
	committed  chan struct{} // ordered tail ran, or the entry failed

	regOnce   sync.Once
	idOnce    sync.Once
	storeOnce sync.Once

	commitOnce sync.Once
	leaveOnce  sync.Once

	failed atomic.Bool
	errMu  sync.Mutex
	err    error

	gatesMu sync.Mutex
	gates   []*batchGate
	txids   []chainhash.Hash
}

type batchGate struct {
	entry  *windowEntry
	txids  []chainhash.Hash
	done   chan struct{}
	failed atomic.Bool
	once   sync.Once
}

// quickWindowDepth is the effective depth for this service. The arithmetic itself lives in
// settings.QuickWindowConfiguredDepth, which legacy sync's dispatcher calls too, so the two
// services cannot drift apart on how many blocks may be in flight; all this adds is the one
// startup line that makes the resolved depth (and any clamp) visible in the log next to
// legacy's own line.
func quickWindowDepth(s *settings.Settings, logger ulogger.Logger) int {
	depth, reasons := s.BlockValidation.QuickWindowConfiguredDepth()

	if len(reasons) > 0 {
		logger.Warnf("[quickWindow] blockvalidation_quick_window_blocks=%d resolved to depth %d: %s", s.BlockValidation.QuickWindowBlocks, depth, strings.Join(reasons, "; "))
	} else {
		logger.Infof("[quickWindow] blockvalidation_quick_window_blocks=%d resolved to depth %d", s.BlockValidation.QuickWindowBlocks, depth)
	}

	return depth
}

func newQuickWindow(logger ulogger.Logger, depth int, callerLimit int, commit func(context.Context, *windowEntry) error) *quickWindow {
	if depth < 0 {
		depth = 0
	}

	if callerLimit < 1 {
		callerLimit = 1
	}

	w := &quickWindow{
		logger:   logger,
		depth:    depth,
		commit:   commit,
		byHash:   make(map[chainhash.Hash]*windowEntry),
		open:     make(map[chainhash.Hash]*batchGate),
		retained: make(map[chainhash.Hash]*windowEntry),
		callers:  semaphore.NewWeighted(int64(callerLimit)),
		wake:     make(chan struct{}, 1),
	}
	w.cond = sync.NewCond(&w.mu)

	return w
}

// quickWindowParentWait bounds how long a window-route block waits for its parent to be
// admitted before deciding the parent is neither stored nor in flight. Legacy sync hands
// block N+1 over as soon as N's own call has started, so the gap between the two is normally
// microseconds; the ten seconds is there for a badly stalled predecessor, not for the common
// case. It is a var rather than a const only so tests can shrink it: nothing writes it in
// production.
var quickWindowParentWait = 10 * time.Second

func (w *quickWindow) Enabled() bool { return w != nil && w.depth >= 1 }

func (w *quickWindow) Depth() int {
	if w == nil {
		return 0
	}

	return w.depth
}

// Start runs the committer until ctx is cancelled.
func (w *quickWindow) Start(ctx context.Context) {
	go w.run(ctx)
}

func (w *quickWindow) AcquireCaller(ctx context.Context) error { return w.callers.Acquire(ctx, 1) }
func (w *quickWindow) ReleaseCaller()                          { w.callers.Release(1) }

// Admit adds block to the window once there is room. The block's parent must be the last
// admitted entry, or the window must be empty (the caller has then confirmed the parent is
// stored). A block already in flight is returned with duplicate=true and no new entry.
func (w *quickWindow) Admit(ctx context.Context, block *model.Block) (*windowEntry, bool, error) {
	hash := *block.Hash()

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.shut {
		return nil, false, errors.NewServiceError("[quickWindow] window closed")
	}

	if e, ok := w.byHash[hash]; ok {
		return e, true, nil
	}

	if w.depth < 1 {
		return nil, false, errors.NewServiceError("[quickWindow][%s] window depth is %d; quick window is disabled", hash.String(), w.depth)
	}

	for len(w.entries) >= w.depth {
		if err := w.waitLocked(ctx); err != nil {
			return nil, false, err
		}

		if w.shut {
			return nil, false, errors.NewServiceError("[quickWindow] window closed")
		}

		if e, ok := w.byHash[hash]; ok {
			return e, true, nil
		}
	}

	var parent *windowEntry

	if n := len(w.entries); n > 0 {
		tail := w.entries[n-1]
		if !tail.hash.IsEqual(block.Header.HashPrevBlock) {
			w.logger.Warnf("[quickWindow][%s] admission refused: parent %s is not the window tail %s at height %d", hash.String(), block.Header.HashPrevBlock.String(), tail.hash.String(), tail.height)
			return nil, false, errors.NewServiceError("[quickWindow][%s] parent %s is not the window tail %s at height %d", hash.String(), block.Header.HashPrevBlock.String(), tail.hash.String(), tail.height)
		}

		parent = tail
	}

	// WithoutCancel, not Background: the entry keeps the caller's context VALUES — the trace
	// span and its baggage — so a windowed block's spans still hang off the call that admitted
	// it instead of each becoming a root, and the log correlation that goes with them survives.
	// What it deliberately does not inherit is the caller's cancellation or deadline (Go's
	// WithoutCancel drops both): the committer owns this block from here, and a caller that
	// gives up, or whose RPC is torn down, must not cancel store work that is still going to be
	// committed. The entry's own cancel, called by failLocked, is the only thing that tears the
	// block down.
	ectx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	e := &windowEntry{
		w:          w,
		block:      block,
		hash:       hash,
		height:     block.Height,
		parent:     parent,
		added:      time.Now(),
		ctx:        ectx,
		cancel:     cancel,
		registered: make(chan struct{}),
		idAssigned: make(chan struct{}),
		storeDone:  make(chan struct{}),
		committed:  make(chan struct{}),
	}

	w.entries = append(w.entries, e)
	w.byHash[hash] = e
	// The gauge means "admitted and not yet left", which is len(byHash): an entry that has
	// committed or aborted stays there, holding its dedup slot, until its owner calls Leave.
	// It can legitimately read above the configured depth (the admission bound, len(entries))
	// by however many entries are in exactly that committed-or-aborted-but-not-yet-left state.
	prometheusBlockValidationQuickWindowDepth.Set(float64(len(w.byHash)))
	w.cond.Broadcast()
	w.kick()

	return e, false, nil
}

// waitLocked waits on the condition variable with ctx cancellation. Called with w.mu held.
func (w *quickWindow) waitLocked(ctx context.Context) error {
	stop := context.AfterFunc(ctx, func() {
		w.mu.Lock()
		w.cond.Broadcast()
		w.mu.Unlock()
	})
	defer stop()

	w.cond.Wait()

	return ctx.Err()
}

func (w *quickWindow) Lookup(hash *chainhash.Hash) *windowEntry {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.byHash[*hash]
}

// AwaitParent returns the in-flight entry for parent, waiting up to timeout for it to be
// admitted. nil means the parent is not in flight; the caller decides what that means.
func (w *quickWindow) AwaitParent(ctx context.Context, parent *chainhash.Hash, timeout time.Duration) *windowEntry {
	deadline := time.Now().Add(timeout)

	w.mu.Lock()
	defer w.mu.Unlock()

	for {
		if e, ok := w.byHash[*parent]; ok {
			return e
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil
		}

		tctx, cancel := context.WithTimeout(ctx, remaining)
		err := w.waitLocked(tctx)
		cancel()

		if err != nil {
			if e, ok := w.byHash[*parent]; ok {
				return e
			}

			return nil
		}
	}
}

// GateFor returns the open gate that will release txid, unless it belongs to owner: an entry
// never waits on its own gates (in-block parents are handled by the two-phase path).
//
// A gate returned here always belongs to a PREDECESSOR of owner, never to a successor: a batch
// registers only the ids of its own block, and admission accepts only the current tail's child,
// so every other entry holding an id is either ahead of owner in the chain or has already left.
// That one-way shape is what makes the shared caller budget deadlock-free — waiting always runs
// backwards along the chain, and a block parked on a gate holds no caller slot.
func (w *quickWindow) GateFor(owner *windowEntry, txid *chainhash.Hash) *batchGate {
	w.mu.Lock()
	defer w.mu.Unlock()

	g, ok := w.open[*txid]
	if !ok || g.entry == owner {
		return nil
	}

	return g
}

// Registered reports whether txid is claimed by an in-flight block OTHER than owner. The miss
// backstop asks it before reclassifying a store failure as a gate miss, and the owner filter is
// what keeps that reclassification honest: an id the calling block registered itself was never
// gated (in-block parents take the two-phase path), so a not-found or anonymous-spent answer
// for one of them is a real failure of this block, not our bookkeeping. Without the filter it
// would come back as a local service error — an endless local re-request of a block that will
// never succeed — and would move quick_validate_window_miss_total, whose ship gate is zero.
//
// owner nil means "no calling block", which is every id in the window.
func (w *quickWindow) Registered(owner *windowEntry, txid *chainhash.Hash) bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	e, ok := w.retained[*txid]

	return ok && e != owner
}

func (w *quickWindow) kick() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

// run is the committer: it owns the ordered tail and the abort cascade.
func (w *quickWindow) run(ctx context.Context) {
	// The oldest-in-flight gauge is what a hung block shows up as, so it cannot only be written
	// on the way past: parking on the head's channels would freeze it at the microseconds it
	// read when the block was admitted, which is the reading that hides exactly the incident it
	// exists for. This ticker refreshes it from whatever the head is now, and zeroes it when the
	// window is empty so a drained window does not keep reporting its last block's age forever.
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		head := w.head()
		if head == nil {
			prometheusBlockValidationQuickWindowOldestAgeSeconds.Set(0)

			select {
			case <-ctx.Done():
				w.shutdown()
				return
			case <-ticker.C:
				continue
			case <-w.wake:
				continue
			}
		}

		prometheusBlockValidationQuickWindowOldestAgeSeconds.Set(time.Since(head.added).Seconds())

		select {
		case <-ctx.Done():
			w.shutdown()
			return
		case <-ticker.C:
			// Re-read the head and refresh the gauge. Nothing is lost by looping: a storeDone or
			// committed channel that was ready alongside the tick stays closed, so the next pass
			// through this select takes it.
			continue
		case <-head.storeDone:
			if head.failed.Load() {
				w.abortFrom(head, "head_failed") // cause unused: failLocked already ran once
				continue
			}

			if err := w.commit(ctx, head); err != nil {
				// Record the commit error and let abortFrom's failLocked call do the rest
				// (CAS the failed flag, cancel, fail the head's own gates, close committed).
				// Setting head.failed here directly would make that later CompareAndSwap a
				// no-op and leave head.committed unclosed forever.
				w.logger.Warnf("[quickWindow][%s] commit failed at height %d: %v", head.hash.String(), head.height, err)
				head.recordErr(err)
				w.abortFrom(head, "commit_failed")

				continue
			}

			w.pop(head)
			head.commitOnce.Do(func() { close(head.committed) })
		case <-head.committed:
			// Closed by Fail before store work finished; cause unused, already recorded there.
			w.abortFrom(head, "head_failed")
		}
	}
}

// shutdown marks the window closed to new admissions and aborts every entry still resident.
// It loops rather than aborting a single captured head, because a concurrent Leave's own abort
// cascade (see leave) can have already invalidated whatever entry a single read of head would
// have returned; re-reading head each time always finds whatever is actually still there. shut
// is set before the loop starts, under w.mu, so no Admit racing in around this moment can add
// an entry after the point this loop stops looking: it either lands in w.entries before shut is
// observed true (and this loop catches it) or observes shut and refuses.
func (w *quickWindow) shutdown() {
	w.mu.Lock()
	w.shut = true
	w.mu.Unlock()

	w.logger.Warnf("[quickWindow] shutting down: aborting the in-flight window and refusing further admissions")

	for e := w.head(); e != nil; e = w.head() {
		w.abortFrom(e, "shutdown")
	}
}

func (w *quickWindow) head() *windowEntry {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.entries) == 0 {
		return nil
	}

	return w.entries[0]
}

// pop removes head from the ordered chain (not from byHash: the entry stays in the window,
// holding its dedup slot and its retained ids, until its owner calls Leave).
func (w *quickWindow) pop(e *windowEntry) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for i, x := range w.entries {
		if x == e {
			w.entries = append(w.entries[:i], w.entries[i+1:]...)
			break
		}
	}

	// A committed entry can free a slot below depth for an Admit parked on w.cond.Wait.
	w.cond.Broadcast()
}

// abortFrom fails every entry from failed to the tail, records the abort before signalling,
// and pops them all from the ordered chain. The head keeps its own error and class; headCause
// labels the abort metric for the head's own failure (ignored if failLocked already ran for it,
// since the CAS it guards its one-time actions with will already be spent).
func (w *quickWindow) abortFrom(failed *windowEntry, headCause string) {
	w.mu.Lock()

	idx := -1

	for i, x := range w.entries {
		if x == failed {
			idx = i
			break
		}
	}

	if idx < 0 {
		w.mu.Unlock()
		return
	}

	doomed := append([]*windowEntry(nil), w.entries[idx:]...)
	w.entries = w.entries[:idx]
	// Freed slots (and, for the tail entries, an emptied window) can unblock an Admit parked
	// on w.cond.Wait below depth.
	w.cond.Broadcast()
	w.mu.Unlock()

	for i, e := range doomed {
		if i == 0 {
			w.logger.Warnf("[quickWindow][%s] head failed at height %d, cause=%s, err=%v; aborting window", e.hash.String(), e.height, headCause, e.recordedErr())
			e.failLocked(nil, headCause)
			continue
		}

		w.logger.Debugf("[quickWindow][%s] aborting at height %d: predecessor %s at height %d failed", e.hash.String(), e.height, failed.hash.String(), failed.height)
		e.failLocked(errors.NewServiceError("[quickWindow][%s] aborted at height %d: predecessor %s at height %d failed", e.hash.String(), e.height, failed.hash.String(), failed.height), "predecessor_failed")
	}
}

// leave removes e from the window entirely, frees its retained ids and its dedup slot, drops
// the parent link any still-resident child holds to it, and releases the block, txids and
// gates it pinned.
//
// An entry still queued for commit or abort when its owner calls Leave has not gone through
// the normal StoreDone-then-commit or Fail path (a bug, or a caller giving up early instead of
// calling Fail). Erasing it from the ordered chain in that state would leave the committer, if
// it is parked on this entry as head, waiting on channels nothing will ever close again — the
// whole window would wedge. So it is fail-closed first, exactly as a genuine failure would be.
func (w *quickWindow) leave(e *windowEntry) {
	w.mu.Lock()
	stillQueued := false

	for _, x := range w.entries {
		if x == e {
			stillQueued = true
			break
		}
	}
	w.mu.Unlock()

	if stillQueued {
		w.logger.Warnf("[quickWindow][%s] left before commit at height %d; aborting window from here", e.hash.String(), e.height)

		// recordErr is first-wins: only offer this generic cause if nothing more specific
		// (a real commit failure, a gate failure) got there first. failLocked still does the
		// authoritative failed-flag CAS below, this just avoids clobbering a better message.
		if !e.failed.Load() {
			e.recordErr(errors.NewServiceError("[quickWindow][%s] left before commit at height %d", e.hash.String(), e.height))
		}

		w.abortFrom(e, "left_before_commit")
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	for i, x := range w.entries {
		if x == e {
			w.entries = append(w.entries[:i], w.entries[i+1:]...)
			break
		}
	}

	delete(w.byHash, e.hash)

	// A predecessor that has left has committed closed (either it resolved normally before
	// Leave was called, or the stillQueued branch above just fail-closed it), and on the
	// failure path abortFrom has already fail-closed every successor still pointing to it
	// before this loop runs. So no live successor can wrongly read a cleared link as "there
	// was never a predecessor" — it has already observed the real outcome, or is about to via
	// its own committed channel. Without clearing the link at all, every entry would keep
	// pinning the one before it back to the last time the window was empty, which under
	// continuous IBD is never.
	for _, x := range w.byHash {
		if x.parent == e {
			x.parent = nil
		}
	}

	e.gatesMu.Lock()
	for _, id := range e.txids {
		if w.retained[id] == e {
			delete(w.retained, id)
		}

		if g, ok := w.open[id]; ok && g.entry == e {
			delete(w.open, id)
		}
	}
	// txids and gates are no longer reachable from anyone (children no longer point at e, and
	// its own gates are already closed), so free them here. e.block is left alone: on the
	// leave-before-commit path the committer can still be inside commit(ctx, e), reading
	// e.Block() through its unlocked getter, at the very moment this runs — nil-ing it here
	// would race that read and could hand the commit callback a nil block. The struct, block
	// included, is reclaimed once nothing (including the committer's own stack) references it.
	e.txids = nil
	e.gates = nil
	e.gatesMu.Unlock()

	// "Admitted and not yet left": this can legitimately read above the configured depth by
	// however many entries have committed or aborted (freeing their w.entries slot) but whose
	// owner hasn't called Leave yet.
	prometheusBlockValidationQuickWindowDepth.Set(float64(len(w.byHash)))
	w.cond.Broadcast()
	w.kick()
}

// ---- windowEntry ----

// closed reports whether ch is already closed, without blocking.
func closed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// wrapCtxErr turns a context error into a local service fault. A caller waiting on this window
// must always see a transient local error, never a bare context.Canceled/context.DeadlineExceeded
// (errors.IsTransientLocalError does not recognise either as one).
func wrapCtxErr(hash chainhash.Hash, ctx context.Context) error {
	return errors.NewServiceError("[quickWindow][%s] wait cancelled: %v", hash.String(), ctx.Err())
}

// windowOf returns the window this entry belongs to, nil for a block running outside the
// window. Nil-safe so a caller can hand its (possibly nil) entry straight through.
func (e *windowEntry) windowOf() *quickWindow {
	if e == nil {
		return nil
	}

	return e.w
}

func (e *windowEntry) Block() *model.Block      { return e.block }
func (e *windowEntry) Height() uint32           { return e.height }
func (e *windowEntry) Context() context.Context { return e.ctx }

// parentEntry reads the current predecessor link under the window's lock: leave clears a
// child's link to its parent the moment the parent itself leaves, so this is the only safe way
// to read it. A predecessor that has left has committed closed, and on the failure path every
// successor still pointing to it was already fail-closed by abortFrom before the link was
// cleared, so treating a cleared link as "no predecessor" never loses a live signal.
func (e *windowEntry) parentEntry() *windowEntry {
	e.w.mu.Lock()
	defer e.w.mu.Unlock()

	return e.parent
}

func (e *windowEntry) recordErr(err error) {
	e.errMu.Lock()
	defer e.errMu.Unlock()

	if e.err == nil {
		e.err = err
	}
}

func (e *windowEntry) recordedErr() error {
	e.errMu.Lock()
	defer e.errMu.Unlock()

	return e.err
}

// RegisterBatch records the transaction ids this batch will create and returns the gate that
// releases spends of them. A txid already held by another live entry is a fail-closed error.
func (e *windowEntry) RegisterBatch(txids []chainhash.Hash) (*batchGate, error) {
	g := &batchGate{entry: e, txids: txids, done: make(chan struct{})}

	e.w.mu.Lock()
	for _, id := range txids {
		if other, ok := e.w.retained[id]; ok && other != e {
			e.w.mu.Unlock()
			// A fault in our own bookkeeping (two in-flight entries claiming the same txid)
			// must never be charged to the peer that delivered either block.
			return nil, errors.NewServiceError("[quickWindow][%s] transaction %s is already claimed by in-flight block %s at height %d", e.hash.String(), id.String(), other.hash.String(), other.height)
		}
	}

	for _, id := range txids {
		e.w.open[id] = g
		e.w.retained[id] = e
	}
	e.w.mu.Unlock()

	e.gatesMu.Lock()
	e.gates = append(e.gates, g)
	e.txids = append(e.txids, txids...)
	e.gatesMu.Unlock()

	if e.failed.Load() {
		g.fail()
	}

	return g, nil
}

func (e *windowEntry) RegistrationComplete() {
	e.regOnce.Do(func() { close(e.registered) })
}

// WaitPredecessorsRegistered blocks until every in-flight predecessor has registered all of
// its batches, so the open-gate map is complete before the first dependency check. The parent
// link is re-read through parentEntry on each step, so the walk only ever visits entries still
// resident in the window (at most depth of them), never the whole history behind them.
func (e *windowEntry) WaitPredecessorsRegistered(ctx context.Context) error {
	for p := e.parentEntry(); p != nil; p = p.parentEntry() {
		resolved := func() (bool, error) {
			if closed(p.registered) {
				return true, nil
			}

			if closed(p.committed) {
				if p.failed.Load() {
					return true, errors.NewServiceError("[quickWindow][%s] predecessor %s failed before registering", e.hash.String(), p.hash.String())
				}

				return true, nil
			}

			return false, nil
		}

		if done, err := resolved(); done {
			if err != nil {
				return err
			}

			continue
		}

		select {
		case <-p.registered:
		case <-p.committed:
		case <-ctx.Done():
		}

		// A signal that lands in the same instant ctx is cancelled must still win: re-check
		// non-blockingly before treating this as a cancellation.
		if done, err := resolved(); done {
			if err != nil {
				return err
			}

			continue
		}

		return wrapCtxErr(e.hash, ctx)
	}

	return nil
}

// WaitPredecessorIDAssigned blocks until the direct predecessor has assigned its block ID (or
// has itself resolved without doing so), re-checking both signal channels non-blockingly after
// ctx fires so a ready signal always wins over a bare context error.
func (e *windowEntry) WaitPredecessorIDAssigned(ctx context.Context) error {
	p := e.parentEntry()
	if p == nil {
		return nil
	}

	resolved := func() (bool, error) {
		if closed(p.idAssigned) {
			return true, nil
		}

		if closed(p.committed) {
			if p.failed.Load() {
				return true, errors.NewServiceError("[quickWindow][%s] predecessor %s failed before assigning its id", e.hash.String(), p.hash.String())
			}

			return true, nil
		}

		return false, nil
	}

	if done, err := resolved(); done {
		return err
	}

	select {
	case <-p.idAssigned:
	case <-p.committed:
	case <-ctx.Done():
	}

	if done, err := resolved(); done {
		return err
	}

	return wrapCtxErr(e.hash, ctx)
}

func (e *windowEntry) IDAssigned() {
	e.idOnce.Do(func() { close(e.idAssigned) })
}

// StoreDone tells the committer this entry's store work returned without error.
func (e *windowEntry) StoreDone() {
	e.storeOnce.Do(func() { close(e.storeDone) })
}

// Fail records err as this entry's outcome, fails its gates, cancels its context and wakes
// the committer, which aborts every later entry.
func (e *windowEntry) Fail(err error) {
	e.failLocked(err, "gate_failed")
	e.w.kick()
}

func (e *windowEntry) failLocked(err error, cause string) {
	if err != nil {
		e.recordErr(err)
	}

	if !e.failed.CompareAndSwap(false, true) {
		return
	}

	prometheusBlockValidationQuickWindowAbortsTotal.WithLabelValues(cause).Inc()
	e.cancel()

	e.gatesMu.Lock()
	gates := append([]*batchGate(nil), e.gates...)
	e.gatesMu.Unlock()

	for _, g := range gates {
		g.fail()
	}

	e.commitOnce.Do(func() { close(e.committed) })
}

// WaitCommitted returns nil once the ordered tail has run for this entry, or the recorded
// error once it failed or was aborted. failLocked cancels the entry's context before closing
// committed, so a caller waiting on e.Context() would otherwise race a select between the two
// and see a bare context.Canceled about half the time; resolved is checked non-blockingly both
// before waiting and again after ctx fires, so a closed committed always wins.
func (e *windowEntry) WaitCommitted(ctx context.Context) error {
	resolved := func() (bool, error) {
		if !closed(e.committed) {
			return false, nil
		}

		if e.failed.Load() {
			if err := e.recordedErr(); err != nil {
				return true, err
			}

			return true, errors.NewServiceError("[quickWindow][%s] aborted", e.hash.String())
		}

		return true, nil
	}

	if done, err := resolved(); done {
		return err
	}

	select {
	case <-e.committed:
	case <-ctx.Done():
	}

	if done, err := resolved(); done {
		return err
	}

	return wrapCtxErr(e.hash, ctx)
}

// Leave removes the entry from the window. The owner calls it exactly once, after its pipeline
// has returned, so no store call of this attempt can still be in flight.
func (e *windowEntry) Leave() {
	e.leaveOnce.Do(func() { e.w.leave(e) })
}

// ---- batchGate ----

// Close marks the batch's create side committed: waiters proceed and the ids leave the open map
// (they stay in the retained map until the entry leaves).
func (g *batchGate) Close() {
	g.once.Do(func() {
		g.entry.w.mu.Lock()
		for _, id := range g.txids {
			if cur, ok := g.entry.w.open[id]; ok && cur == g {
				delete(g.entry.w.open, id)
			}
		}
		g.entry.w.mu.Unlock()

		close(g.done)
	})
}

func (g *batchGate) fail() {
	g.failed.Store(true)
	g.Close()
}

// Wait blocks until the gate closes. A gate closed by failure returns a service error, so the
// waiting block fails as a local fault. resolved is checked non-blockingly both before waiting
// and again once ctx fires, so a gate that closes at the same instant ctx is cancelled is never
// mistaken for a bare context error.
func (g *batchGate) Wait(ctx context.Context) error {
	resolved := func() (bool, error) {
		if !closed(g.done) {
			return false, nil
		}

		if g.failed.Load() {
			return true, errors.NewServiceError("[quickWindow][%s] parent block %s at height %d failed; its outputs cannot be spent", g.entry.hash.String(), g.entry.hash.String(), g.entry.height)
		}

		return true, nil
	}

	if done, err := resolved(); done {
		return err
	}

	select {
	case <-g.done:
	case <-ctx.Done():
	}

	if done, err := resolved(); done {
		return err
	}

	return wrapCtxErr(g.entry.hash, ctx)
}
