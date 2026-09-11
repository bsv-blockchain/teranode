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
// It is the bound the two count caps cannot give. Every item the sweep handles
// waits on something outside this process, a blob-store write permit for a
// delete and the blockchain service for a lookup, each with a deadline of its
// own around ten seconds, and they are handled one after another. Bounded in
// count is a twenty-minute tick in the worst case; bounded in time is a tick
// that finishes inside its interval, so a block that becomes stuck is looked at
// on the next tick rather than after a backlog of somebody else's deletes.
//
// A sixth of parkSweepInterval, and normal ticks are far under it: a full
// 128-entry expiry burst against a store with permits free is milliseconds.
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

// parkEvictionFloor is the height below which a parked block can never be
// needed, or 0 when the node cannot say.
//
// It is the highest block this node has actually committed, and eviction is
// strictly below it. Nothing approximate: a block parked below a block we have
// already put in the chain cannot be a link in any chain we are building, and
// its parent is missing so it cannot be a sibling either.
//
// It is NOT the front of the header list, which was the obvious reading and is
// wrong. An arriving front block has its header removed from the list before the
// park sees it, so the front sits one height above the block being waited on, and
// a sweep judging by the front evicts exactly the block it needs.
//
// Zero until the first commit, which switches eviction off on a node that has
// not committed anything yet. That is the right way round: a node still finding
// its feet should keep what it has downloaded.
func (sm *SyncManager) parkEvictionFloor() int32 {
	return sm.lastCommittedHeight.Load()
}

// drainParkedDescendants commits everything parked behind a block that has just
// been committed, and then everything parked behind those, and so on.
//
// It walks an explicit stack rather than recursing: a chain of parked blocks can
// be maxParkedEntries long, and recursion would nest that many frames, each one
// holding a decoded block. Exactly one block is decoded at a time and it is
// released before the next is read.
func (sm *SyncManager) drainParkedDescendants(committed chainhash.Hash) {
	if !sm.blockPark.Enabled() {
		return
	}

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
	msgBlock, err := sm.blockPark.Read(sm.ctx, entry.hash)
	if err != nil {
		return sm.parkedReadFailed(entry, err)
	}

	// A nil in-flight parent: the parent of a parked block is in the chain by the
	// time anything commits it, so HandleBlockDirect looks it up there.
	if err = sm.HandleBlockDirect(sm.ctx, entry.peer, entry.hash, msgBlock, nil); err != nil {
		return sm.parkedBlockFailed(entry, err)
	}

	// The header list is only ever advanced by an arriving block that matches
	// its front. A block committed from disk never passes that code, so without
	// this the front sticks on a block that is already in the chain, the next
	// block never matches it, the frontier is never republished and the
	// checkpoint transition never fires — headers-first sync would wedge one
	// block after the first successful drain.
	isCheckpointBlock, _ := sm.advanceHeaderListFor(entry.hash)

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

// parkedBlockCommitted is everything owed after a parked block has gone into the
// chain and its header node has been taken off the front: the progress stamp, the
// disposition that deletes the blob and gives its bytes back, the backoff and
// cascade clears, the peer bookkeeping, and either the checkpoint transition or
// the pipeline top-up.
//
// isCheckpointBlock is the answer advanceHeaderListFor gave for this block, passed
// in rather than recomputed, because by the time this runs the front has moved and
// the question can no longer be asked.
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
		// A parked block CAN be the checkpoint block, and if the next round of
		// headers is never asked for, headers-first sync stops here for good. So
		// this one falls back to the current sync peer when the peer that
		// delivered the block has gone.
		if err := sm.checkpointBlockCommitted(sm.livePeer(entry.peer), entry.hash); err != nil {
			sm.logger.Errorf("[commitParkedBlock][%s] failed to move past the checkpoint: %v", entry.hash, err)
		}

		return
	}

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

// resumeHeaderWalk sends the download walk out again from wherever the cursor
// now is.
//
// Every rewind moves the cursor back and sends nothing. What actually issues a
// getdata is fetchHeaderBlocks, and its only callers are a block arriving, a
// headers message arriving, and the pipeline top-up after a block is committed —
// all of which are things that happen because sync is moving. In the regime the
// rewinds exist for, sync is not moving: the block that was given up on was the
// one everything else was queued behind, so no later block is coming to carry
// the rewound cursor out with it, and a node would sit on a perfectly good
// cursor until the stall detector rotated the peer and threw the cursor away.
//
// It used to check that the cursor was sitting on the front of the list before
// sending anything, and to call that check the whole of its safety. The rule it
// was reaching for — nothing may fetch while the round's anchor is still the
// front — now lives in topUpHeaderBlocks, which is the one place every one of
// the top-up callers passes through, and it is stated there as a fact about the
// list rather than about the cursor. What is left of the old check is an
// accident: "the cursor is on the front" is also false during an ordinary
// forward walk, where the cursor is deliberately ahead of the front, so the
// ticker declined to top the pipeline up in exactly the state the top-up exists
// for. Keeping it would have meant keeping a condition no test could hold to
// account, next to a comment claiming it was load-bearing.
//
// It is gated on the node having somewhere to put a block, not on the sync peer
// having room. The gate it used to take, the sync peer's own count against the
// block-size ladder, is right for topping a peer's queue back up after that
// peer's block stopped being outstanding, and wrong here: at the ladder's lowest
// rung the cap is one block, so a sync peer mid-transfer on a multi-gigabyte
// block holds the resume shut for hours while the assigner would have handed the
// rewound front block to an idle peer. The frontier race cannot cover it either,
// because publishFrontierLocked clears the frontier for a front block nobody has
// asked for, which is exactly what a rewound front is.
//
// svnode schedules per peer, in each peer's own send pass, with each peer
// checking only its own in-flight count and no sync peer involved in block
// bodies at all (FindNextBlocksToDownload, src/net/net_processing.cpp:5522).
// Letting the assigner decide is that shape: it spreads over every eligible peer
// with budget and refuses the pass when the node-wide download window is spent
// or every peer is at its per-peer cap. With legacy_multiPeerBlockDownload off
// it collapses to the sync peer at the ladder's budget, which is the behaviour
// this had before.
//
// Called from the park sweep's ticker, on the sweep's own goroutine. Everything
// it touches is under headerMu or is a peer send, which handleHeadersMsg already
// does from a goroutine of its own.
func (sm *SyncManager) resumeHeaderWalk() {
	sm.topUpHeaderBlocks(nil)
}

// runParkSweep drives the park sweep and the rewound-cursor resume from a
// goroutine of their own until the manager stops.
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
			// A rewind — from the sweep just above, or from a block given up
			// on since the last tick — moves the download cursor back and
			// sends nothing. This is what carries it out. See
			// resumeHeaderWalk.
			sm.resumeHeaderWalk()
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
		sm.commitParkedBlockAndDrain(commit.entry)

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

// commitParkedBlockAndDrain commits one parked block and then everything parked
// behind it, both on the calling goroutine. It is what a manager with no
// dispatcher does, which is the pre-window path and every struct-literal test
// manager.
func (sm *SyncManager) commitParkedBlockAndDrain(entry parkedBlock) {
	if sm.commitParkedBlock(entry) {
		sm.drainParkedDescendants(entry.hash)
	}
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

	if !sm.blockPark.Enabled() {
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

	// Bounded by the number of distinct parents of parked blocks, and capped at
	// the park's own entry limit besides. A dropped request is not a lost block:
	// the sweep finds it within its interval, because a parked block whose parent
	// is stored is exactly what StuckCandidates hands over.
	if len(sm.drainQueue) >= maxParkedEntries {
		sm.logger.Warnf("[scheduleDrain][%s] the drain queue is full at %d parents; this one waits for the sweep", parent, len(sm.drainQueue))

		return
	}

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
// The order is peek, test, claim, advance the header front, dispatch. Peeking
// first is what stops a refused candidate being stranded out of the index, and it
// is also where the size comes from, which the byte arm of the admission test
// needs. The header front is advanced here rather than in the tail because the
// front has to be past this block before the next arriving block is
// head-processed, and freeing the consumer is precisely what makes that not
// automatic any more: a live successor examined while the front still sits on an
// in-flight parked block matches nothing, never removes its own node, never
// learns it is the checkpoint block, and wedges the walk on a block already
// committed.
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

		isCheckpointBlock, removedFront := sm.advanceHeaderListFor(entry.hash)

		// Merged onto the entry the dispatch owns, because every path that gives
		// the block up rewinds from it, and by then the node is gone from both the
		// list and the index.
		if removedFront != nil {
			entry.removedFront = removedFront
		}

		d.parked = &entry
		d.parkedIsCheckpoint = isCheckpointBlock
		d.isCheckpoint = isCheckpointBlock

		bd.dispatch(d)

		return true
	}

	return false
}

// sweepParkedBlocks is the safety net for blocks whose parent never arrives
// through a commit this node saw.
//
// Two things need it. A block can be parked for a reason other than a genuinely
// absent parent, because a missing parent is not the only thing that surfaces as
// ErrBlockNotFound. And a block recovered from disk after a restart never sees a
// commit event for a parent that was already in the chain when the node started,
// so nothing would ever drain it.
//
// It runs on the sweep's own goroutine (runParkSweep) and commits nothing
// itself: a parked block whose parent turns out to be stored is posted to the
// block-queue consumer through submitParkCommit. BOTH halves are still capped
// per tick and neither can turn into a pass over the whole park in one go: the
// chain lookups by parkSweepRPCBudget, and the blocks it gives up on by
// parkSweepExpiryBudget. The second cap is the one that is easy to miss, and it
// is the more expensive item — a store delete and a cursor rewind rather than a
// lookup — and the one that arrives in bursts, because blocks parked together
// age out together.
//
// Both of those cap a COUNT, and a count is not a bound on the tick. Each item
// carries its own deadline, chainCtx for a lookup and the park's store timeout
// for a delete, and both of those wait on resources the rest of the process is
// competing for: the blockchain service, and the blob store's process-wide write
// permits. A bounded number of sequential items each allowed ten seconds is a
// twenty-minute tick, during which nothing newly stuck is looked at and every
// commit this tick has already posted waits behind it. So the tick has its own
// elapsed-time budget, parkSweepTimeBudget, and both halves stop at it.
//
// Stopping is free for the lookups, which leave the block parked for the next
// tick anyway. It is not free for the expiries, because Expire has already taken
// those entries out of the index: an entry the tick does not reach is put back,
// or its blob is left charged against the budget with nothing tracking it and
// its cursor is never rewound.
func (sm *SyncManager) sweepParkedBlocks(now time.Time) {
	if !sm.blockPark.Enabled() {
		return
	}

	deadline := sm.parkSweepClock().Add(parkSweepTimeBudget)

	expired := sm.blockPark.EvictBelow(sm.parkEvictionFloor(), parkSweepExpiryBudget)

	for i, entry := range expired {
		// Always one, however long the tick has already run: a budget that can
		// refuse every item is a sweep that never sweeps.
		if i > 0 && !sm.parkSweepClock().Before(deadline) {
			sm.blockPark.RestoreAll(expired[i:])
			sm.logger.Warnf("[sweepParkedBlocks] out of time after giving up %d blocks, %d put back for the next tick", i, len(expired)-i)

			break
		}

		sm.logger.Infof("[sweepParkedBlocks][%s] %s (height %d), dropping it: parent %s", entry.hash, parkDispositionOvertaken.reason, entry.height, entry.prevBlock)
		sm.applyParkDisposition(entry, parkDispositionOvertaken)
	}

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

		sm.logger.Infof("[sweepParkedBlocks][%s] parent %s is in the chain after all, committing the parked block", entry.hash, entry.prevBlock)

		sm.submitParkCommit(parkCommit{entry: entry, parentHeight: parentHeight})
	}
}
