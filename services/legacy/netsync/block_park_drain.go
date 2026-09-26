package netsync

import (
	"context"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/teranode/errors"
	teranodeblockchain "github.com/bsv-blockchain/teranode/services/blockchain"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
)

// parkSweepInterval is how often the sweep looks over the park for blocks that
// have been waiting too long, or whose parent turned up without a commit event
// this node saw. The sweep has a goroutine of its own (runParkSweep); what it
// finds worth committing it posts to the block-queue consumer, which is the one
// goroutine that commits blocks in order.
//
// A var rather than a const so the two tests that prove this ticker is really
// wired up can run in milliseconds instead of half a minute. Nothing in
// production writes it, and the only reader takes its value once, when the block
// handler starts.
var parkSweepInterval = 30 * time.Second

// parkSweepTimeBudget is how long one sweep tick may spend before it stops and
// leaves the rest for the next one.
//
// It is the bound the count cap beside it cannot give. Every lookup the sweep
// makes waits on the blockchain service, with a deadline of its own around ten
// seconds, and they are handled one after another. Bounded in count alone is a
// long tick in the worst case; bounded in time is a tick that finishes inside
// its interval, so a block that becomes stuck is looked at on the next tick
// rather than after a backlog of somebody else's lookups.
//
// A sixth of parkSweepInterval, and normal ticks are far under it: a full
// 128-lookup burst against a service that answers promptly is milliseconds.
// Nothing is lost by stopping, only deferred by parkSweepInterval, and every item
// the sweep defers is one already past its own deadline, so the only question is
// rate.
var parkSweepTimeBudget = 5 * time.Second

// parkSweepClock reads the clock the sweep measures its own tick against. The
// tick's `now` argument is the time the sweep is judging the park AT, which
// tests move by hours; this is how long the tick itself has been running, which
// is a different question and needs a real clock.
func (sm *SyncManager) parkSweepClock() time.Time {
	if sm.parkSweepNow != nil {
		return sm.parkSweepNow()
	}

	return time.Now()
}

// chainCtx puts a deadline on one blockchain lookup made from the block-commit
// goroutine.
//
// The park already says why, for the other half of its I/O: "Park, read-back and
// delete all run on the single goroutine that commits blocks in order, so an
// undeadlined one is head-of-line blocking for every block queued behind it"
// (blockPark.storeCtx). The chain lookups on that same goroutine had no
// equivalent, and the sweep makes up to parkSweepRPCBudget of them per tick,
// sequentially, on sm.ctx, which carries no deadline. Capped in count is not
// capped in time, and time is the quantity that comment identifies as the one
// that matters.
//
// Same order as legacy_parkStoreTimeout, because it is the same goroutine and
// the same argument. parkMinStoreTimeout is the fallback for a manager built
// without a park, which is how most of this package's tests build one.
func (sm *SyncManager) chainCtx() (context.Context, context.CancelFunc) {
	timeout := parkMinStoreTimeout
	if sm.blockPark != nil && sm.blockPark.storeTimeout > 0 {
		timeout = sm.blockPark.storeTimeout
	}

	return context.WithTimeout(sm.ctx, timeout)
}

// blockExistsWithDeadline is the sweep's parent lookup, in a function of its own
// so the deadline is released at the end of each iteration rather than piling up
// until the end of the tick.
func (sm *SyncManager) blockExistsWithDeadline(hash chainhash.Hash) (bool, error) {
	ctx, cancel := sm.chainCtx()
	defer cancel()

	return sm.blockchainClient.GetBlockExists(ctx, &hash)
}

// parentChainState answers every question the sweep has about a parent in one
// round trip: is it stored, is it usable, and how high is it.
//
// Asking only whether it exists was a real hole. Invalidation is a flag on the
// row, not a delete, so a parent this node has REJECTED still exists, and the
// sweep would commit its descendant on the strength of that. The pair is the
// same one haveInventory already uses for the same reason.
//
// The height comes back because this lookup has already paid for it, and it is
// the only chain-derived height a sweep-posted drain can have. Zero when the
// parent is absent or its row carries no height, which every reader must take as
// "not known" rather than "genesis".
func (sm *SyncManager) parentChainState(hash chainhash.Hash) (exists bool, invalid bool, height uint32, err error) {
	ctx, cancel := sm.chainCtx()
	defer cancel()

	_, meta, err := sm.blockchainClient.GetBlockHeader(ctx, &hash)
	if err != nil {
		if errors.Is(err, errors.ErrBlockNotFound) || errors.Is(err, errors.ErrNotFound) {
			return false, false, 0, nil
		}

		return false, false, 0, err
	}

	if meta == nil {
		return false, false, 0, nil
	}

	return true, meta.Invalid, meta.Height, nil
}

// drainParkedDescendants commits everything parked behind a block that has just
// been committed, and then everything parked behind those, and so on.
//
// It walks an explicit stack rather than recursing: a chain of parked blocks can
// run to however many the read-ahead depth admits, and recursion would nest
// that many frames, each one holding a decoded block. Exactly one block is
// decoded at a time and it is released before the next is read.
func (sm *SyncManager) drainParkedDescendants(committed chainhash.Hash) {
	stack := []chainhash.Hash{committed}

	for len(stack) > 0 {
		parent := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		for _, entry := range sm.blockPark.TakeChildren(parent) {
			if sm.commitParkedBlock(entry) {
				stack = append(stack, entry.hash)
			}
		}
	}
}

// commitParkedBlock reads one parked block back off disk and commits it,
// reporting whether it went in — which is what tells the drain whether to look
// for blocks parked behind it in turn.
//
// It deliberately does NOT go back through handleBlockMsg. That function's front
// half is wrong for a block committed from disk and would break it twice over:
// the peer lookup reports "unknown peer" for a peer that has since been evicted,
// which fails the whole block, and the ownership check sees an obligation that
// was released when the block first arrived, which disconnects a peer for
// delivering a block we asked for.
//
// The entry has already been taken out of the park index by the caller. Its blob
// is still on disk and still charged against the budget, so every path out of
// here goes through applyParkDisposition, which is what settles that.
//
// It is composed from three helpers rather than written out, because a second
// scheduler commits parked blocks through a worker and a tail and has to apply
// exactly the same policy. Two copies of this classification would drift, and the
// two defaults point opposite ways: a read failure keeps the block, a commit
// failure judges it.
func (sm *SyncManager) commitParkedBlock(entry parkedBlock) bool {
	record, err := sm.blockPark.ReadConverted(sm.ctx, entry.hash)
	if err != nil {
		return sm.parkedReadFailed(entry, err)
	}

	// A nil in-flight parent: see HandleConvertedBlock's own doc comment for why it
	// is never called with anything else.
	if err = sm.HandleConvertedBlock(sm.ctx, entry.peer, entry.hash, record); err != nil {
		return sm.parkedBlockFailed(entry, err)
	}

	// isCheckpointHash is a direct hash comparison against the configured
	// checkpoints, so it needs no header-list bookkeeping to answer this for a
	// block committed from disk rather than off the wire.
	isCheckpointBlock := sm.isCheckpointHash(entry.hash)

	sm.parkedBlockCommitted(entry, isCheckpointBlock)

	return true
}

// parkedReadFailed decides what to do with a parked block whose blob would not
// read back, and reports false so the drain stops walking that branch.
//
// A read can fail because the blob is bad, but it can equally fail because the
// store had no permit free inside the park's deadline or because the node is
// shutting down, and neither of those says anything about the block.
// parkReadFailure tells them apart; treating them alike destroys fully
// downloaded blocks under ordinary load. That is why this is a function of its
// own rather than an arm of a shared failure path: the commit failure beside it
// defaults the other way, to judging the block.
func (sm *SyncManager) parkedReadFailed(entry parkedBlock, err error) bool {
	d := parkReadFailure(err)

	sm.logger.Warnf("[commitParkedBlock][%s] parked block could not be read back (%s): %v", entry.hash, d.reason, err)
	sm.applyParkDisposition(entry, d)

	return false
}

// parkedBlockCommitted is everything owed after a parked block has gone into
// the chain: the progress stamp, the disposition that deletes the blob and
// gives its bytes back, the backoff and cascade clears, the peer bookkeeping,
// the possible exit from headers-first mode, and the pipeline top-up.
//
// isCheckpointBlock is isCheckpointHash's answer for this block, passed in
// rather than recomputed here purely so a stall investigation can see from the
// log whether the block that just unstuck a drain was a checkpoint; the
// decision this function makes no longer depends on it; see
// maybeLeaveHeadersFirstMode.
//
// It deliberately does not drain the blocks parked behind this one. The caller
// owns that: the serial path walks an explicit stack in drainParkedDescendants,
// and a stack is what stops a chain of parked blocks nesting one frame per link,
// each holding a decoded block.
func (sm *SyncManager) parkedBlockCommitted(entry parkedBlock, isCheckpointBlock bool) {
	// A parked block committing is a block joining the chain, and it is the one
	// commit that never passes through the block queue. Without this a node
	// working purely off its park looks, to the stall check, like a node that
	// has stopped.
	sm.noteChainProgress()

	sm.applyParkDisposition(entry, parkDispositionCommitted)

	if sm.blockFailureBackoff != nil {
		sm.blockFailureBackoff.Delete(entry.hash)
	}

	if sm.recentlyFailedBlocks != nil {
		sm.recentlyFailedBlocks.Delete(entry.hash)
	}

	sm.noteCommittedParkedBlock(entry)

	if isCheckpointBlock {
		sm.logger.Infof("[commitParkedBlock][%s] committed a checkpoint block from the park", entry.hash)
	}

	sm.maybeLeaveHeadersFirstMode(entry.hash.String())

	sm.fetchMoreHeaderBlocks(sm.livePeer(entry.peer))
}

// replayingHistory reports whether the node is catching blocks rather than
// judging a peer's tip. It is the same question handleBlockMsg asks before it
// suppresses a reject, asked from the paths that commit a block off disk.
//
// An FSM state that cannot be read counts as replaying, because that is what the
// live path does with it too: it fails the block before it ever reaches a reject.
func (sm *SyncManager) replayingHistory() bool {
	if sm.blockchainClient == nil {
		return false
	}

	ctx, cancel := sm.chainCtx()
	defer cancel()

	state, err := sm.blockchainClient.GetFSMCurrentState(ctx)
	if err != nil {
		sm.logger.Warnf("[replayingHistory] could not read the FSM state, so no peer is blamed for a block that would not commit: %v", err)

		return true
	}

	return state != nil && *state == teranodeblockchain.FSMStateCATCHINGBLOCKS
}

// parkedBlockFailed decides what to do with a parked block that would not
// commit, and reports false so the drain stops walking that branch. The decision
// itself is parkCommitFailure's; all this does is log it and carry it out.
func (sm *SyncManager) parkedBlockFailed(entry parkedBlock, err error) bool {
	d := parkCommitFailure(err)

	// The same suppression the live path applies. While the node is catching
	// blocks handleBlockMsg sends no reject for a block that would not commit,
	// because we are replaying history rather than judging a peer's tip — and
	// during initial sync this drain is the MAIN commit path, so without this a
	// parked block earns its peer a reject that the same block delivered live
	// would not. Committing from disk must judge a peer exactly as the wire does.
	if d.blamePeer && sm.replayingHistory() {
		d = d.withoutBlame()
	}

	if d.blob == parkBlobKeep {
		// Stamped before the disposition is carried out, so the entry that goes
		// back into the park carries it. Without this the parent stays queued
		// for a drain and the very next turn picks this same block again: 1,494
		// of 3,000 log lines on mainnet on 2026-09-10, about seven a second,
		// while a block whose parent was the tip waited behind it.
		if d.reason == parkDispositionParentGone.reason {
			entry.parentMissingAt = time.Now()
		}

		sm.logger.Infof("[commitParkedBlock][%s] leaving the block parked (%s), parent %s: %v", entry.hash, d.reason, entry.prevBlock, err)
	} else {
		sm.logger.Errorf("[commitParkedBlock][%s] giving the block up (%s): %v", entry.hash, d.reason, err)
	}

	sm.applyParkDisposition(entry, d)

	return false
}

// noteCommittedParkedBlock does the peer bookkeeping for a block committed from
// disk. It is applied only when the peer that delivered the block is still
// registered, because a departed peer's height is not news and a nil peer
// (every block recovered from disk after a restart) has none.
func (sm *SyncManager) noteCommittedParkedBlock(entry parkedBlock) {
	height := entry.height

	if height <= 0 {
		ctx, cancel := sm.chainCtx()
		defer cancel()

		_, meta, err := sm.blockchainClient.GetBlockHeader(ctx, &entry.hash)
		if err != nil {
			sm.logger.Warnf("[commitParkedBlock][%s] could not read back the committed height: %v", entry.hash, err)
		} else if h, convErr := safeconversion.Uint32ToInt32(meta.Height); convErr != nil {
			sm.logger.Errorf(failedToConvertBlockHeightInt32Msg, convErr)
		} else {
			height = h
		}
	}

	sm.logger.Infof("accepted block %v at height %d from the park", entry.hash, height)

	sm.rejectedTxns.Clear()

	if entry.peer == nil || height <= 0 {
		return
	}

	state, ok := sm.peerStates.Get(entry.peer)
	if !ok {
		return
	}

	if sps, ok := sm.syncPeerStateFor(entry.peer); ok {
		sps.updateLastBlockTime()
	}

	entry.peer.UpdateLastBlockHeight(height)
	state.noteBestKnownHeight(height)
	// Same fact as the direct-delivery path, discovered later: this peer sent us
	// this block, and the height is the one we committed it at.
	state.noteProvenClaim(entry.hash, height)
}

// livePeer returns the peer a post-commit action should be aimed at: the one
// that delivered the block while it is still connected, and otherwise the
// current sync peer, which may be nil. Used for the actions that keep sync
// moving — never for misbehaviour signals, which must not be redirected.
func (sm *SyncManager) livePeer(recorded *peerpkg.Peer) *peerpkg.Peer {
	if recorded != nil && recorded.Connected() {
		return recorded
	}

	return sm.loadSyncPeer()
}

// runParkSweep drives the park sweep and the periodic top-up from a goroutine of
// their own until the manager stops.
//
// The sweep used to be a ticker arm on the goroutine that committed blocks in
// order, and that was the whole justification for its per-tick time budget: a
// slow tick there held up commits. Under the dispatcher that goroutine admits
// blocks, and the ordering guarantee comes from block validation's own single
// committer and its admission rule, so a slow tick there would only delay the
// admission of blocks that have nothing to do with the park. Everything that
// needs no ordering therefore runs here: deciding, the parent lookups, the
// evictions and the store deletes. The one thing that may not is committing,
// and sweepParkedBlocks posts those back to the consumer through parkCommits.
func (sm *SyncManager) runParkSweep() {
	ticker := time.NewTicker(parkSweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-sm.quit:
			return

		case <-ticker.C:
			sm.sweepParkedBlocks(time.Now())

			// A block given up on since the last tick has nothing put back for
			// it: the wanted-range pass recomputes what it wants and who owes
			// it from the committed tip on every call, so this periodic pass is
			// what re-asks for it once it is genuinely unowed again, without
			// waiting for a block, a headers message or a peer top-up to
			// trigger one first.
			sm.fetchHeaderBlocks()
		}
	}
}

// submitParkCommit hands a parked block whose parent is in the chain to the
// block-queue consumer to commit, or commits it here when there is no consumer
// to hand it to.
//
// The sweep must not commit from its own goroutine. A commit admits the block
// into block validation's quick window, and the dispatcher is admitting blocks
// there at the same time from the consumer goroutine: two admitters race for the
// window's tail, and the loser is refused for a parent that is in fact stored
// and judged a local fault. Posting the commit to the consumer puts it behind
// the dispatcher's own admissions, which is the same route the parking workers
// use for their outcomes.
//
// The inline path is for a manager that has no channel, which is how most of
// this package's tests build one, and it is what the sweep did before it had a
// goroutine of its own. A send on a nil channel would block forever.
func (sm *SyncManager) submitParkCommit(commit parkCommit) {
	if sm.parkCommits == nil {
		// Put back, then drained here and now, for the reason the consumers give: an
		// entry the on-disk handler posts is still in the index, and committing it
		// directly left it there. The drain walks synchronously, because the caller
		// may be the sweep's own goroutine, and the consumer's queue is not its to touch.
		sm.blockPark.Restore(commit.entry)
		sm.drainParkedDescendants(commit.entry.prevBlock)

		return
	}

	// Checked before the select, not as an arm of it. Both arms can be ready at
	// once and select picks uniformly, so after quit this would still post into a
	// channel nobody drains, and the entry would be lost rather than restored.
	select {
	case <-sm.quit:
		sm.blockPark.Restore(commit.entry)

		return
	default:
	}

	select {
	case sm.parkCommits <- commit:

	case <-sm.quit:
		// Nobody will commit it now. The caller took it out of the index, so put
		// it back: its blob stays charged and Recover finds it on the next start.
		sm.blockPark.Restore(commit.entry)
	}
}

// parkCommit is one parked block the sweep found a stored parent for, with the
// height of that parent, which the sweep's own lookup already fetched. The height
// travels because a drained dispatch's frontier entry must not read height zero:
// a zero there is refused as a parent, so the block behind it waits for an empty
// frontier instead of chaining on.
type parkCommit struct {
	entry        parkedBlock
	parentHeight uint32
}

// scheduleDrain records that a block has committed and blocks parked behind it
// may now be committable. It is the one entry point for that, and it has two
// behaviours.
//
// With the drain running asynchronously it merges a request into the consumer's
// own queue and returns at once, so the commit that discovered the work is not
// held up by it. The queue holds parent hashes and heights, never claimed park
// entries, which is what keeps a queued request's block visible to the sweep's
// eviction and stuck-candidate passes while it waits.
//
// Otherwise it walks the stack synchronously, exactly as before. That is the
// pre-window path, where there is no consumer loop to admit anything, and every
// manager a test builds as a struct literal. Keeping the old call here rather
// than routing everything through the queue is what makes
// blockvalidation_quick_window_blocks = 0 a true rollback.
//
// Every caller runs on the goroutine that owns the queue, so there is no lock and
// no channel: a channel would only add a send that can block the one goroutine
// this whole change exists to keep free.
func (sm *SyncManager) scheduleDrain(parent chainhash.Hash, parentHeight uint32) {
	if !sm.drainAsync.Load() {
		sm.drainParkedDescendants(parent)

		return
	}

	for i := range sm.drainQueue {
		if sm.drainQueue[i].parent.IsEqual(&parent) {
			// Deduped by parent, keeping the better height. The same parent can be
			// discovered twice, by a commit and by a worker whose write finished
			// after that commit, and draining it twice is wasted lookups.
			if parentHeight > sm.drainQueue[i].parentHeight {
				sm.drainQueue[i].parentHeight = parentHeight
			}

			return
		}
	}

	// Bounded by the number of distinct parents of parked blocks, and nothing
	// caps that on its own account any more: the park itself no longer has an
	// entry ceiling to cap this against, and the number of parents that can ever
	// be distinct is already bounded upstream by the download walk's read-ahead
	// depth, the same bound that now does the park's own job.
	sm.drainQueue = append(sm.drainQueue, drainRequest{parent: parent, parentHeight: parentHeight})
}

// drainRequest is one committed block whose parked children may now be
// committable, held by hash rather than by claimed entry.
type drainRequest struct {
	parent       chainhash.Hash
	parentHeight uint32
}

// drainStep turns at most one parked block into a dispatch, and reports whether
// it did. It runs on the consumer goroutine, in the same loop turn as the
// admission test, so the answer cannot go stale between them.
//
// The order is peek, test, claim, check the checkpoint hash, dispatch. Peeking
// first is what stops a refused candidate being stranded out of the index, and it
// is also where the size comes from, which the byte arm of the admission test
// needs.
func (sm *SyncManager) drainStep(bd *blockDispatcher) bool {
	for len(sm.drainQueue) > 0 {
		req := sm.drainQueue[0]

		peeked, ok := sm.blockPark.FirstChildFor(req.parent)
		if !ok {
			// Nothing left behind this parent that can be committed now. A child
			// still being written stays queued under its own parent and is asked
			// for by whoever finishes the write.
			sm.drainQueue = sm.drainQueue[1:]

			continue
		}

		d := &blockDispatch{parked: &peeked, bytes: peeked.size}

		if req.parentHeight > 0 {
			d.height = req.parentHeight + 1

			// On the window route with a known height, a drained block may run
			// alongside another rather than waiting for the window to empty.
			//
			// This is the fix for a node that sits idle with a hundred blocks
			// already on disk. A dispatch was marked windowed in exactly one
			// place, when its parent was still being validated, and every block
			// drained from the park has a parent that is already committed. So
			// no parked block was ever windowed, an unwindowed one is admitted
			// only into a completely empty window, and during catch-up 91% of
			// blocks arrive out of order and go through the park. The window's
			// depth and byte budget exist to keep more than one block in flight
			// and the path carrying most blocks could not use them.
			//
			// A drained block is a safer candidate than the live one this was
			// built for. Its parent is in the chain rather than merely in
			// flight, and its height comes from the parent's own committed
			// height, which the sweep carries for exactly this purpose.
			//
			// Height zero is left alone deliberately. That is what a block
			// recovered from disk after a restart carries, and a zero in the
			// window is refused as a parent, so such a block keeps the old
			// one-at-a-time rule rather than being admitted next to something it
			// cannot chain to.
			d.windowed = sm.windowRoute(d.height)
		}

		if !bd.canDispatch(d) {
			// Left in the queue, entry untouched, to be offered again next turn.
			return false
		}

		entry, ok := sm.blockPark.Take(peeked.hash)
		if !ok {
			// The sweep took it between the peek and the claim. Its own path will
			// commit it, so this turn has nothing to do.
			return false
		}

		isCheckpointBlock := sm.isCheckpointHash(entry.hash)

		d.parked = &entry
		d.parkedIsCheckpoint = isCheckpointBlock
		d.isCheckpoint = isCheckpointBlock

		bd.dispatch(d)

		return true
	}

	return false
}

// reconcileRecoveredParents asks the chain about the parent of every block the
// restart scan adopted, once, and hands on the ones that can already commit. It
// returns how many it handed on.
//
// A block recovered from disk is a case the commit-driven drain (the primary
// mechanism: a commit is asked for the blocks parked directly behind it, and
// those drain at once) cannot serve on its own: the event that would have woken
// it fired, if at all, in a process that no longer exists. That condition holds
// once, over a set of blocks known once, everything Recover just adopted, so
// it is answered once here rather than by asking about the same blocks again
// every thirty seconds for the life of the node, which is what the sweep
// (sweepParkedBlocks) used to be the only thing doing about it.
//
// Cost: bounded by AllParked's own bound, which is the park's contents at the
// moment recovery finishes. That is NOT legacy_maxBlocksInTransitPerPeer times
// the peer count: a peer's in-flight slot is freed the moment its block
// finishes streaming, not when that block commits, so a stalled frontier does
// not stop peers cycling through slot after slot, parking one block per
// height as each arrives. What actually stops the download reading past a
// point is positional, not a count of requests in flight at once:
// wantedBlocks (wanted_range_assign.go) never asks for a height more than
// legacy_blockDownloadWindow above the committed height, 1024 by default.
// TestWantedRange_TheParkNeverExceedsTheReadAheadDepth is what actually proves
// the park's population bound is that window, not this.
//
// So the real worst case is on the order of legacy_blockDownloadWindow
// recovered blocks needing a lookup, never per park entry per tick: one chain
// lookup per recovered block, once.
//
// Deliberately NOT called from Start(), where Recover runs: at that point
// nothing is reading sm.parkCommits yet (dispatchBlocks, the consumer, and
// runParkSweep both start later, from blockHandler), so handing on more than
// parkSweepRPCBudget blocks there would block this call forever on a channel
// nobody drains. Called instead from blockHandler, immediately after the
// consumer goroutine is started and before the sweep's own goroutine begins,
// so a hand-off here always has somewhere to go.
func (sm *SyncManager) reconcileRecoveredParents(ctx context.Context) int {
	handed := 0

	for _, candidate := range sm.blockPark.AllParked() {
		if ctx.Err() != nil {
			sm.logger.Warnf("[reconcileRecoveredParents] stopping early (%v) after handing on %d block(s); the rest stay parked for the commit-driven drain or the sweep", ctx.Err(), handed)

			break
		}

		exists, invalid, parentHeight, err := sm.parentChainState(candidate.prevBlock)
		if err != nil {
			sm.logger.Warnf("[reconcileRecoveredParents][%s] could not check parent %s: %v", candidate.hash, candidate.prevBlock, err)

			continue
		}

		if !exists {
			// The ordinary case: the parent really has not arrived yet, and the
			// commit-driven drain will pick this block up the moment it does.
			continue
		}

		entry, ok := sm.blockPark.Take(candidate.hash)
		if !ok {
			// Already gone: a commit that raced this pass took it first.
			continue
		}

		if invalid {
			// The parent is stored and rejected, so this block can never be
			// committed, the same judgment sweepParkedBlocks makes for the
			// identical condition, and parkDispositionParentInvalid (drop the
			// blob, mark failed) rather than parkDispositionParentGone (keep
			// and retry) is what belongs here: nothing about this parent is
			// going to change.
			sm.logger.Warnf("[reconcileRecoveredParents][%s] %s (%s), dropping it", entry.hash, parkDispositionParentInvalid.reason, entry.prevBlock)
			sm.applyParkDisposition(entry, parkDispositionParentInvalid)

			continue
		}

		sm.submitParkCommit(parkCommit{entry: entry, parentHeight: parentHeight})

		handed++
	}

	if handed > 0 {
		sm.logger.Infof("[reconcileRecoveredParents] handed on %d recovered block(s) whose parent was already in the chain", handed)
	}

	return handed
}

// sweepParkedBlocks is the safety net for blocks whose parent never arrives
// through a commit this node saw.
//
// One thing needs it now that reconcileRecoveredParents has taken the restart
// case: a block can be parked for a reason other than a genuinely absent
// parent, because a missing parent is not the only thing that surfaces as
// ErrBlockNotFound. A second, likelier than hypothetical, is what
// reconcileRecoveredParents' own doc comment does not cover either: a parent
// committed by something other than legacy sync fires no event legacy sync
// listens for, so neither the commit-driven drain nor the one-off startup pass
// ever sees it. This sweep is deliberately left running to catch that case, and
// its commit branch below now logs with its own distinct prefix when it does,
// so a soak can show whether that third case is real rather than a reasoned
// guess about it.
//
// It runs on the sweep's own goroutine (runParkSweep) and commits nothing
// itself: a parked block whose parent turns out to be stored is posted to the
// block-queue consumer through submitParkCommit. A parent that is still
// genuinely absent after parkAbandonAfter is judged orphaned rather than
// merely slow, and dropped here directly — this is the only reclaim path an
// entry in that state has, since neither Delete nor the restart scan nor the
// store's own retention will ever touch it (see parkAbandonAfter). The lookups are capped per tick
// by parkSweepRPCBudget, so a restart with a large park can never turn one tick
// into a pass over the whole thing in one go.
//
// A count cap is not a bound on the tick, so the tick also has its own
// elapsed-time budget, parkSweepTimeBudget. Each lookup carries chainCtx's
// deadline and waits on a resource the rest of the process is competing for,
// the blockchain service, so a bounded NUMBER of them each allowed ten seconds
// is still a long tick in the worst case, during which nothing newly stuck is
// looked at and every commit this tick has already posted waits behind it.
// Stopping there is free: a lookup the tick does not reach simply leaves the
// block parked for the next one.
func (sm *SyncManager) sweepParkedBlocks(now time.Time) {
	deadline := sm.parkSweepClock().Add(parkSweepTimeBudget)

	for i, candidate := range sm.blockPark.StuckCandidates(now, parkSweepRPCBudget) {
		if i > 0 && !sm.parkSweepClock().Before(deadline) {
			sm.logger.Warnf("[sweepParkedBlocks] out of time after %d parent lookups, the rest wait for the next tick", i)

			break
		}

		exists, invalid, parentHeight, err := sm.parentChainState(candidate.prevBlock)
		if err != nil {
			sm.logger.Warnf("[sweepParkedBlocks][%s] could not check whether parent %s is usable: %v", candidate.hash, candidate.prevBlock, err)
			continue
		}

		if !exists {
			// A missing parent is the ordinary case: keep waiting, unless this
			// entry has been waiting so long that "still syncing" no longer
			// explains it. See parkAbandonAfter for why that threshold is what
			// it is and not something tighter.
			if now.Sub(candidate.parkedAt) < parkAbandonAfter {
				continue
			}

			entry, ok := sm.blockPark.Take(candidate.hash)
			if !ok {
				continue
			}

			sm.logger.Warnf("[sweepParkedBlocks][%s] %s (parked %s ago), dropping it: parent %s", entry.hash, parkDispositionAbandoned.reason, now.Sub(entry.parkedAt).Round(time.Second), entry.prevBlock)
			sm.applyParkDisposition(entry, parkDispositionAbandoned)

			continue
		}

		entry, ok := sm.blockPark.Take(candidate.hash)
		if !ok {
			continue
		}

		if invalid {
			// The parent is stored and rejected, so this block can never be
			// committed. Committing it on the strength of the parent merely
			// EXISTING is the bug this closes.
			sm.logger.Warnf("[sweepParkedBlocks][%s] %s (%s), dropping it", entry.hash, parkDispositionParentInvalid.reason, entry.prevBlock)
			sm.applyParkDisposition(entry, parkDispositionParentInvalid)

			continue
		}

		// This is the evidence line the third case earns its keep with: by now
		// both the commit-driven drain and reconcileRecoveredParents have had
		// their chance at this block, so the sweep finding it committable means
		// something neither of them covers actually happened, most likely a
		// parent committed by something other than legacy sync. If a soak never
		// prints this, the polling this sweep still does has no job left.
		sm.logger.Infof("[sweepParkedBlocks][%s] parent %s was in the chain after all; the commit-driven drain and the startup pass both missed it", entry.hash, entry.prevBlock)

		sm.submitParkCommit(parkCommit{entry: entry, parentHeight: parentHeight})
	}
}
