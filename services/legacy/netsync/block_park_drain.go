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

// parkSweepInterval is how often the block-queue consumer looks over the park
// for blocks that have been waiting too long, or whose parent turned up without
// a commit event this node saw. It runs on that goroutine and not on the outer
// message handler because a commit is minutes of work, and the outer handler
// dispatches disconnects, invs and headers for every peer.
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
// own around ten seconds, and they are handled one after another on the goroutine
// that commits blocks in order. Bounded in count is a twenty-minute tick in the
// worst case; bounded in time is a tick that gets out of the way.
//
// A sixth of parkSweepInterval, so the sweep never holds the commit goroutine
// for a meaningful share of the interval it runs on, and normal ticks are far
// under it: a full 128-entry expiry burst against a store with permits free is
// milliseconds. Nothing is lost by stopping, only deferred by parkSweepInterval,
// and every item the sweep defers is one already past its own deadline, so the
// only question is rate.
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

// parentChainState answers both questions the sweep has about a parent in one
// round trip: is it stored, and is it usable.
//
// Asking only whether it exists was a real hole. Invalidation is a flag on the
// row, not a delete, so a parent this node has REJECTED still exists, and the
// sweep would commit its descendant on the strength of that. The pair is the
// same one haveInventory already uses for the same reason.
func (sm *SyncManager) parentChainState(hash chainhash.Hash) (exists bool, invalid bool, err error) {
	ctx, cancel := sm.chainCtx()
	defer cancel()

	_, meta, err := sm.blockchainClient.GetBlockHeader(ctx, &hash)
	if err != nil {
		if errors.Is(err, errors.ErrBlockNotFound) || errors.Is(err, errors.ErrNotFound) {
			return false, false, nil
		}

		return false, false, err
	}

	if meta == nil {
		return false, false, nil
	}

	return true, meta.Invalid, nil
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
func (sm *SyncManager) commitParkedBlock(entry parkedBlock) bool {
	msgBlock, err := sm.blockPark.Read(sm.ctx, entry.hash)
	if err != nil {
		// A read can fail because the blob is bad, but it can equally fail
		// because the store had no permit free inside the park's deadline or
		// because the node is shutting down — and neither of those says anything
		// about the block. parkReadFailure tells them apart; treating them alike
		// destroys fully downloaded blocks under ordinary load.
		d := parkReadFailure(err)

		sm.logger.Warnf("[commitParkedBlock][%s] parked block could not be read back (%s): %v", entry.hash, d.reason, err)
		sm.applyParkDisposition(entry, d)

		return false
	}

	if err = sm.HandleBlockDirect(sm.ctx, entry.peer, entry.hash, msgBlock); err != nil {
		return sm.parkedBlockFailed(entry, err)
	}

	// The header list is only ever advanced by an arriving block that matches
	// its front. A block committed from disk never passes that code, so without
	// this the front sticks on a block that is already in the chain, the next
	// block never matches it, the frontier is never republished and the
	// checkpoint transition never fires — headers-first sync would wedge one
	// block after the first successful drain.
	isCheckpointBlock, _ := sm.advanceHeaderListFor(entry.hash)

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
		if err = sm.checkpointBlockCommitted(sm.livePeer(entry.peer), entry.hash); err != nil {
			sm.logger.Errorf("[commitParkedBlock][%s] failed to move past the checkpoint: %v", entry.hash, err)
		}

		return true
	}

	sm.fetchMoreHeaderBlocks(sm.livePeer(entry.peer))

	return true
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
// Called from the park sweep's ticker, on the block-queue consumer.
func (sm *SyncManager) resumeHeaderWalk() {
	sm.topUpHeaderBlocks(nil)
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
// It runs on the block-queue consumer, the same goroutine that commits blocks in
// order, so BOTH halves are capped per tick and neither can turn into a pass over
// the whole park in one go: the chain lookups by parkSweepRPCBudget, and the
// blocks it gives up on by parkSweepExpiryBudget. The second cap is the one that
// is easy to miss, and it is the more expensive item — a store delete and a
// cursor rewind rather than a lookup — and the one that arrives in bursts,
// because blocks parked together age out together.
//
// Both of those cap a COUNT, and a count is not a bound on the tick. Each item
// carries its own deadline, chainCtx for a lookup and the park's store timeout
// for a delete, and both of those wait on resources the rest of the process is
// competing for: the blockchain service, and the blob store's process-wide write
// permits. A bounded number of sequential items each allowed ten seconds is a
// twenty-minute tick, on the goroutine every queued block is waiting behind, and
// then the block queue fills and dispatch backs up for every peer, which is the
// failure the count caps were introduced to prevent. So the tick has its own
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

		exists, invalid, err := sm.parentChainState(candidate.prevBlock)
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

		if sm.commitParkedBlock(entry) {
			sm.drainParkedDescendants(entry.hash)
		}
	}
}
