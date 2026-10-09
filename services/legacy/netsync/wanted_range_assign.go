package netsync

import (
	"strconv"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
)

// slowPassThreshold is how long a download pass may take before it is logged.
const slowPassThreshold = 5 * time.Second

// assignWantedBlocks asks peers for the blocks this node wants next.
//
// There is no loop and no cursor. The pass computes the wanted range from the
// best block processed, drops what is already owed, hands the rest out, and
// returns. Everything it needs is recomputed each time, so there is no position
// to strand, nothing to rewind, and no state that can disagree with the chain.
//
// It replaces a walk that kept a cursor into the header list. That cursor
// answered three different questions at once — where to resume, whether a round
// was still valid, and whether the front had been asked for — and each fix for
// one broke another. In one day on mainnet that walk produced a read-ahead limit
// that raised itself as downloads arrived, a park filling to its 4,096-entry cap
// and refusing the one block that would extend the tip, a node moving 1.8 blocks
// a minute against a 23ms commit path, and a cursor stranded above the limit
// with 955,208 headers queued and nothing requested.
//
// fetchHeaderBlocks is now just a dispatch to this: the cursor walk it used to
// choose between itself and this pass is gone, so this is the only way blocks
// are chosen.
//
// The wanted range is read once, at the top, and used twice: maybeRequestMoreHeaders
// reads the whole thing to decide whether the header round itself has run dry,
// and only after that is it capped to what the assigner can still place, before
// unownedBlocks spends anything on a candidate — a blob-store existence check, a
// blockchain round trip, two map lookups and a ledger write. The assigner's cap
// answers "is there download budget for a block right now", which is not the
// question a header refill needs answered, so the refill check must see the
// range before that cap is applied and must not be skipped by a nil assigner (no
// budget, or no peer to place a block with) — a node with nothing to place a
// block on can still have every reason to ask for more headers.
func (sm *SyncManager) assignWantedBlocks() {
	// One pass at a time: two concurrent passes could both find a block unowned and both ask
	// for it, since the ledger lets a block have several owners. Five duplicate copies on
	// mainnet on 2026-09-24 had no other cause.
	sm.assignMu.Lock()
	defer sm.assignMu.Unlock()

	// A pass holds assignMu, so a slow pass holds up every pass behind it. On 2026-10-09 from
	// 06:36 to 06:46 no block was asked for while 1,900 were wanted, and nothing in the log said
	// why.
	passStart := time.Now()
	defer func() {
		if took := time.Since(passStart); took > slowPassThreshold {
			sm.logger.Warnf("[assignWantedBlocks] a download pass took %s, holding the passes behind it", took.Round(time.Millisecond))
		}
	}()

	// Read here, before wantedBlocks takes headerMu: committedTip makes a
	// blocking blockchain call, and this package's lock rule has no exception
	// for one made while the header lock is held. A failed read ends the pass:
	// its zero height reported to the header cache below would be taken as a
	// reorg to genesis, moving the floor down, clearing every branch's
	// diverged mark and unhooking every branch from the tip until the next good
	// pass, and the range would be named from height 1.
	best, _, ok := sm.committedTip()
	if !ok {
		sm.logger.Debugf("[assignWantedBlocks] the committed tip could not be read; no download pass this time")

		return
	}

	// The header cache is told the committed height on every pass, not only
	// after a fill, because most passes here are driven by a commit, not a
	// headers reply: nothing at or below it is named from then on, a branch
	// whose tip it has reached stops being a candidate, and the active branch is
	// chosen again (headerCache.Prune). The check that a branch still connects
	// to the tip's hash is made with the hash a fill reads, not here, because a
	// branch that forked below the tip can only have been built by a fill.
	// Dropping the tip's own height is safe because a parent at the tip resolves
	// through pipelineParentHeight's blockchain fallback.
	sm.headerCache.Prune(best)

	wanted := sm.wantedBlocks(best)

	// Ahead of the assigner and its budget cap on purpose: whether the cache has
	// run out is a question about the header round, not about whether there is
	// download budget free this instant, so it must be asked on every call this
	// function makes, not only the ones that go on to place a block. It gates
	// itself on headers-first mode, so it is safe to call outside it.
	sm.maybeRequestMoreHeaders(wanted)

	// Above the last checkpoint the cache names nothing new, and the ledger is
	// the only record of a block some peer was asked for. Appended to the cache
	// range rather than replacing it: when the committed tip crosses the final
	// checkpoint and the mode switches off, the cache can still name up to two
	// thousand heights above it, and this pass is what keeps placing them.
	if !sm.headersFirstMode.Load() {
		wanted = sm.appendOutstandingAtTip(wanted)
	}

	// Nobody with budget is not yet nobody to assign to. A peer that has gone
	// quiet holding a full slice keeps every one of those slots against its
	// per-peer cap, and against the node-wide window, until its blocks are
	// forgiven, and forgiveness otherwise happens only in unownedBlocksUpTo,
	// below this return. So when no peer has room, the quiet owners are
	// forgiven first and the budgets read again; without that nothing is
	// re-asked until a stall backstop fires.
	assigner := sm.newDownloadAssigner()
	if assigner == nil && !sm.headersFirstMode.Load() && sm.forgiveQuietOwners(wanted) > 0 {
		assigner = sm.newDownloadAssigner()
	}

	if assigner == nil {
		return
	}

	assigner.tip = best

	// Stop once there are as many candidates as the assigner can place, not after
	// that many heights: the range starts at the block after the tip and runs
	// through blocks already parked or owed. Trimming the range itself left only
	// held blocks to look at, and on 2026-09-24 three peers sat idle with seven
	// blocks parked and five owed.
	candidates := sm.unownedBlocksUpTo(wanted, assigner.remaining)

	// The far blocks for peers with no rate, from the top of the full window (placeUnmeasured).
	if n := assigner.unmeasuredWithRoom(); n > 0 {
		top := make([]wantedBlock, 0, len(wanted))
		for i := len(wanted) - 1; i >= 0; i-- {
			top = append(top, wanted[i])
		}

		assigner.far = sm.unownedBlocksUpTo(top, n)
	}

	if len(candidates) == 0 && len(assigner.far) == 0 {
		return
	}

	sm.requestBlocks(assigner, candidates, sm.highestHeld(wanted))

	// One send per peer that got work, with headerMu released. This is the only
	// place in the pass that talks to a peer at all.
	assigner.send(sm)
}

// wantedBlocks is the locked half of the pass: the range the node wants next,
// bounded by the read-ahead depth that governs the header walk, and named from
// the header cache rather than the header list.
//
// best is the committed height, read by the caller before headerMu is taken:
// committedTip makes a blocking blockchain call, and this package's lock rule
// has no exception for one made while the header lock is held.
//
// Split out so the lock has a body with no way to reach a peer or another
// service from inside it. The cache that names the range holds its own lock
// and is read with headerMu up, since wantedBlocksFromCache makes no peer send
// and no blocking client call either.
func (sm *SyncManager) wantedBlocks(best int32) []wantedBlock {
	sm.headerMu.Lock()
	defer sm.headerMu.Unlock()

	// legacy_blockDownloadWindow is the read-ahead depth as well as the
	// node-wide request budget: the range reaches the node-wide window, and
	// newDownloadAssigner's own budget decides how much of it is asked for
	// this pass. A nil settings is possible in a bare-struct test harness, and
	// Legacy sits past the 4 KB guard page, where an unguarded dereference is
	// a hardware fault rather than a recoverable panic.
	depth := int32(1)
	if sm.settings != nil {
		depth = int32(max(1, sm.settings.Legacy.BlockDownloadWindow)) //nolint:gosec // a block count, not a size
	}

	return sm.wantedBlocksFromCache(best, depth)
}

// appendOutstandingAtTip adds to the pass's range the blocks the ledger names
// above the last checkpoint (blockDownloadTracker.OutstandingAtTip: each quiet
// owner's head-of-queue block, and every block whose owners were all let off),
// skipping any the cache already names. Only the sweep reaches this: the
// per-arrival and headers-reply triggers of the pass are headers-first only
// (topUpHeaderBlocks, handleHeadersMsg), so a quiet owner's block is re-asked
// at the first sweep after blockRequestRetryInterval, 60 to 90 seconds after
// the owner went quiet.
//
// Heights are not known on this route and are left at zero. canServe(0) is
// true, highestHeld's extends test is false and the backstop never stops a
// height of zero, so nothing downstream vetoes on them; the two log lines that
// print a height render zero as not known (describeWantedHeight).
//
// Every entry then goes through unownedBlocksUpTo's checks like a cache-named
// one: blockHeldLocally (the step-9 seam the inv path shares), the retry
// window, disk, chain, then the busy-owner test and ForgiveOwners. A block an
// owner is still delivering is left alone; a quiet owner's block is forgiven
// and goes to the fastest other peer, full queue or not (fastestAvoiding). With
// one eligible peer that already owes it, requestBlocks reasserts rather than
// asking twice, so a one-peer node re-asks nothing here, and newDownloadAssigner
// answers nil when every eligible peer is at its depth, so a sync peer
// saturated by a getblocks burst and nobody else with room means no re-ask
// that pass.
//
// Known and accepted: this walk and drainRequestQueue (the inv path) both go
// RequestedWithin then Add with no lock in common, so an inv landing in the same
// instant as a sweep can send two getdatas for one never-started block. That
// could not happen in headers-first mode, where processInvMsg returns before
// queuing. Bounded to one extra copy: the second is drained or discarded at the
// sink (dupDrained, dupConverted) and blockHeldLocally keeps a third from being
// asked for.
func (sm *SyncManager) appendOutstandingAtTip(wanted []wantedBlock) []wantedBlock {
	hashes := sm.blockDownloads.OutstandingAtTip(blockRequestRetryInterval)
	if len(hashes) == 0 {
		return wanted
	}

	named := make(map[chainhash.Hash]struct{}, len(wanted))
	for _, block := range wanted {
		named[block.hash] = struct{}{}
	}

	for _, h := range hashes {
		if _, ok := named[h]; ok {
			continue
		}

		wanted = append(wanted, wantedBlock{hash: h})
	}

	return wanted
}

// describeWantedHeight renders a wanted block's height for a log line. The
// ledger-named range above the last checkpoint carries no height, and a line
// that said "height 0" for a mainnet block would send a reader to the wrong
// place.
func describeWantedHeight(height int32) string {
	if height <= 0 {
		return "height not known above the last checkpoint"
	}

	return "height " + strconv.Itoa(int(height))
}

// unownedBlocks keeps the wanted blocks this node neither already holds nor
// currently owes, is not given up on, and is not stalled behind a recent
// failure, and lets a quiet owner off the hook on the way past.
//
// wanted must already be capped to the assigner's remaining budget by the
// caller: every check below costs something, several of them a disk read or a
// blockchain round trip, and none of it is worth spending on a candidate this
// pass could not place anyway.
//
// blockHeldLocally runs first: a block parked, being validated, arriving or
// converting is in hand and is never asked for again, whatever the ledger says.
// It is the one answer the inv path shares (step 9 of the 2026-10-02 re-review).
//
// The recently-failed check runs next, and it only skips the one entry it
// names, not everything above it: a failed parent's own children earn no mark
// of their own, so a pass after a failure still names them, and they wait in
// the park behind the missing parent once downloaded. What this check bounds is
// the marked hash itself: it is not requested again for the life of the mark,
// rather than being downloaded and refused afresh every single pass for as long
// as the parent stays failed.
//
// The disk check runs after the ledger's retry window, because a block already
// on disk should not spend a peer's budget nor be forgiven on a peer's behalf:
// it is simply done. holdsBlock answers from the filesystem alone.
//
// haveInventory is the fallback for the one question none of the checks
// above can answer: whether the chain already has this block by some route
// that never touched legacy's own commit path — the block persister, another
// service entirely — since disk and the park's index know only about the
// filesystem, and the counter wantedBlocks reads only ever advances from
// legacy's own commits. Placed last among the per-candidate checks, after
// the budget cap the caller already applied and after every cheaper check
// here, so its one round trip is spent only on what nothing free could
// already answer.
//
// Of what is left, the RequestedWithin test comes next, and that order is the
// rule rather than an accident: a block inside its retry window has an owner
// who may still deliver, and asking for it again spends a peer slot that a
// block nobody owes could have used. Forgiving it as well would be worse
// still, because forgiveness frees the owner's budget and the pass would then
// hand the same peer the same block it is already carrying.
//
// The budgets are read before this runs, so a pass whose every peer is at its
// cap would never get here: a peer that has gone quiet holding a full slice
// keeps every one of those slots until somebody forgives it. assignWantedBlocks
// covers that case by running forgiveQuietOwners when no peer has room and
// reading the budgets again, so the stall this rule exists to break cannot
// form. ForgiveOwners keeps the ownership and drops only the obligation, so a
// copy still on the wire from the quiet peer is admitted when it lands.
//
// Takes no lock of its own beyond the download ledger's and the park store's,
// neither of which is headerMu, so it is safe with headerMu released and must
// not be called with it held.
func (sm *SyncManager) unownedBlocks(wanted []wantedBlock) []wantedBlock {
	return sm.unownedBlocksUpTo(wanted, len(wanted))
}

// unownedBlocksUpTo is unownedBlocks stopping at limit candidates.
func (sm *SyncManager) unownedBlocksUpTo(wanted []wantedBlock, limit int) []wantedBlock {
	candidates := make([]wantedBlock, 0, min(limit, len(wanted)))

	for _, block := range wanted {
		if len(candidates) >= limit {
			break
		}

		// A block in hand is never asked for again: parked (the park's index is
		// written synchronously at admission, before the blob write lands, so a
		// pass run from inside the admission itself sees it), taken off the park
		// to validate, its bytes arriving, or being converted, whatever the
		// ledger says about who owes it. The ledger can let every peer off a
		// block that is still arriving, and on 2026-09-24 that asked for block
		// 734,077 seventeen times in thirteen seconds. SV Node never asks the
		// same peer again for a block in mapBlocksInFlight and asks another peer
		// only through its 30-second parallel fetch (net_processing.cpp:462-507).
		if sm.blockHeldLocally(block.hash) {
			continue
		}

		// #1333: a block that recently failed to store or validate, judged or
		// merely unlucky, is not requested again while the mark stands. Its
		// descendants are not marked themselves; once downloaded they wait in
		// the park behind the missing parent rather than being asked for again.
		// A parent being retried right now is in the dispatcher's window and
		// was skipped above, so the mark is read only for a block nothing holds.
		if sm.recentlyFailedBlocks != nil {
			if _, failed := sm.recentlyFailedBlocks.Get(block.hash); failed {
				continue
			}
		}

		// Already asked of a peer within the retry window: skipped from the ledger,
		// a map read. Ahead of the disk and chain checks below, because in steady
		// state almost every block in the range is parked or requested, and those
		// checks read a file or make a round trip for each one: on mainnet the disk
		// check alone was 4.2 s of a 54 s profile, on the serial path behind every
		// commit. Every check before ForgiveOwners only skips, so the order changes
		// the cost and never the answer; the one rule it keeps is that the disk and
		// chain checks still come before ForgiveOwners, so a block already held is
		// never handed to another peer.
		if sm.blockDownloads.RequestedWithin(block.hash, blockRequestRetryInterval) {
			continue
		}

		// Below the last checkpoint a block with one active owner stays with it: the watcher
		// judges it when it will be late, and a quiet peer's rate decays, so it gets no more
		// blocks. Letting a quiet owner off here released 15 blocks of a busy peer on 2026-10-07,
		// and each was downloaded two times. Skipped here, ahead of the disk and chain checks,
		// which each pass otherwise made for each such block. A block with two or more active
		// owners has had the watcher's one extra copy; when none of them sends, the quiet-owner
		// rule below asks another peer. A block whose owners were let off already, a demoted
		// sync peer's (demoteSyncPeer), has no active owner and is asked of another peer. Above
		// the checkpoint, at the tip, the quiet-owner rule applies to each block.
		if sm.headersFirstMode.Load() {
			if active, _ := sm.blockDownloads.ActiveOwners(block.hash); len(active) == 1 {
				continue
			}
		}

		// A block already on disk is not wanted, whatever any index says. This
		// is what makes a restart free: the files survive it, so a node comes
		// back and asks only for what it genuinely lacks.
		if sm.holdsBlock(sm.ctx, block.hash) {
			// Held on disk but not in the park is a record nothing announced: its
			// peer was dropped between the write and the on-disk message. Left like
			// that it is skipped here forever and never drained, which stopped
			// mainnet at 650,021 on 2026-09-23. Adopt it so the park sweep commits it.
			// A block being committed has left the park but not the disk: that is in
			// flight, not stranded, and putting it back would have it read again after
			// its files are gone.
			if !sm.blockPark.Has(block.hash) && !sm.blockCommitting(block.hash) &&
				sm.blockPark.adoptStranded(sm.ctx, block.hash, sm.subtreeStore) {
				sm.logger.Warnf("[unownedBlocks][%s] adopted a complete record at %s that was on disk but not in the park", block.hash, describeWantedHeight(block.height))
			}

			continue
		}

		// The blockchain fallback. Disk knows nothing about the chain, so a
		// block that joined it through some route other than legacy's own
		// commit path — the block persister, another service entirely — is
		// invisible to both disk checks above, and the counter wantedBlocks
		// reads only ever advances from legacy's own commits. haveInventory
		// answers that question in one round trip, checking the park again
		// first (a second, cheap check, not a second cost) and then the
		// blockchain client. Run last among the per-candidate checks, after
		// every check that answers from memory alone, so the round trip is
		// never spent on a block one of the cheap checks above was already
		// going to skip. Guarded on blockchainClient itself: haveInventory
		// dereferences it with no nil check of its own, and Legacy sits past
		// the 4 KB guard page, where that would be a hardware fault rather
		// than a recoverable panic, in the many tests that build a
		// SyncManager as a struct literal with no client at all.
		if sm.blockchainClient != nil {
			hash := block.hash
			if haveInv, err := sm.haveInventory(wire.NewInvVect(wire.InvTypeBlock, &hash)); err != nil {
				sm.logger.Warnf("[assignWantedBlocks][%s] could not check whether the chain already has this block, asking for it: %v", hash, err)
			} else if haveInv {
				continue
			}
		}

		// An owner still sending block bytes has not gone quiet, however long ago
		// the block was asked for. With large blocks a peer can spend minutes on
		// what was queued ahead of this one, and asking another peer then downloads
		// it twice: 346 blocks in 11 hours at height 705,000, one of them 447 MB.
		// SV Node does not re-ask a block from a peer that is still delivering.
		if sm.ownerStillSending(block.hash) {
			continue
		}

		if sm.forgiveQuietOwnersOf(block) {
			block.reAsked = true
		}

		candidates = append(candidates, block)
	}

	return candidates
}

// forgiveQuietOwners forgives every block in wanted whose owners have all gone
// quiet past the retry window, and returns how many it forgave. It is the
// forgiveness half of unownedBlocksUpTo for a pass that found no peer with
// room: only blocks somebody owes are looked at, and only the checks that
// answer from memory run, because this collects no candidate. The disk and
// chain checks unownedBlocksUpTo makes before forgiving keep a held block from
// being handed to another peer, and that pass still makes them before
// anything forgiven here is asked for again.
func (sm *SyncManager) forgiveQuietOwners(wanted []wantedBlock) int {
	forgiven := 0

	for _, block := range wanted {
		if !sm.blockDownloads.Requested(block.hash) || sm.blockHeldLocally(block.hash) {
			continue
		}

		if sm.blockDownloads.RequestedWithin(block.hash, blockRequestRetryInterval) || sm.ownerStillSending(block.hash) {
			continue
		}

		// Not logged or counted here: the block is logged and counted as re-asked
		// when unownedBlocksUpTo forgives it again and makes it a candidate,
		// which is when it may actually go to another peer.
		if len(sm.blockDownloads.ForgiveOwners(block.hash, blockRequestRetryInterval)) > 0 {
			forgiven++
		}
	}

	return forgiven
}

// ownerStillSending reports whether any owner of hash has sent block bytes
// within the retry window.
func (sm *SyncManager) ownerStillSending(hash chainhash.Hash) bool {
	return sm.blockDownloads.AnyOwner(hash, func(p *peerpkg.Peer) bool {
		return time.Since(sm.streams.lastBlockBytes(p)) < blockRequestRetryInterval
	})
}

// forgiveQuietOwnersOf lets block's quiet owners off it, logs each, and reports
// whether there was anybody to forgive.
func (sm *SyncManager) forgiveQuietOwnersOf(block wantedBlock) bool {
	quiet := sm.blockDownloads.ForgiveOwners(block.hash, blockRequestRetryInterval)
	if len(quiet) == 0 {
		return false
	}

	// Logged per block because it is what lets a block be asked of a second peer, and
	// a duplicate copy can then only be traced back through this line.
	sm.waste.reAskedQuiet.Add(1)

	for _, p := range quiet {
		last := sm.streams.lastBlockBytes(p)
		since := "never"

		if !last.IsZero() {
			since = time.Since(last).Round(time.Second).String()
		}

		sm.logger.Infof("[reRequest][%s] %s: %s owed it and last sent block bytes %s ago; it may be asked of another peer", block.hash, describeWantedHeight(block.height), p, since)
	}

	return true
}

// requestBlocks places each candidate with a peer that has budget for it and
// builds that peer's getdata. It stops at the first block no peer can take,
// because the candidates ascend and a peer that cannot serve one cannot serve
// anything above it.
//
// Stopping is the whole of the termination argument: the range is bounded before
// this is reached, every iteration consumes one entry of it, and nothing here
// re-reads the range or asks for another round. A block left unplaced is simply
// picked up by the next pass, which recomputes the range from the best block
// processed — there is no position to lose it from.
//
// A block whose quiet owner has just been forgiven is never re-asked of that
// same owner. The assigner is told to prefer any other peer with budget, and
// when the owner is the only peer left the block is reasserted rather than
// requested a second time. Sending it twice would have the peer answer twice,
// and the second copy arrives after the first discharged the obligation
// (handleBlockOnDiskMsg calls RemoveOwner on the answering peer), so it looks
// unrequested and is thrown away, a wasted download.
//
// On a node with one peer that means the block is not re-asked at all, which is
// the right answer rather than a gap: there is nobody to help, so the only thing
// a second getdata could achieve is the disconnect above. Recovery is the peer's
// own stall detection and the ledger's expiry.
func (sm *SyncManager) requestBlocks(assigner *downloadAssigner, candidates []wantedBlock, highestHeld int32) {
	candidates = sm.placeUnmeasured(assigner, candidates, highestHeld)

	for _, block := range candidates {
		// The disk backstop bounds how far ahead the node reaches, never a gap below
		// blocks it already holds: on 2026-09-24 parked blocks over the budget
		// stopped the one missing block above the tip being asked for, and the
		// chain stopped with them.
		extends := block.height > highestHeld
		if extends && assigner.overBackstop() {
			return
		}

		owes := func(p *peerpkg.Peer) bool {
			return sm.blockDownloads.HasOwner(p, block.hash)
		}

		// A re-asked block goes to the fastest peer, full queue or not: the chain may be waiting
		// on it, and the peers with room are the slow ones. On 2026-09-25 the 4 GB block 760,331
		// was re-asked of a peer at 2.7 MB/s and took 22 minutes while peers at 40 to 50 MB/s
		// were busy.
		var (
			target *assignerPeer
			ok     bool
		)

		if block.reAsked {
			target, ok = assigner.fastestAvoiding(block.height, owes)
		} else {
			target, ok = assigner.schedulePick(block.height, owes)
		}

		if !ok {
			return
		}

		// A re-ask placed on a peer whose queue was already full goes over that peer's depth, and
		// must not use up the pass's room, which belongs to the peers that have some.
		overDepth := block.reAsked && target.budget <= 0

		// The assigner had nobody but the peer that already holds our request
		// for this block. Re-arm what we hold instead of asking twice: this
		// refreshes the retry window, so the next pass waits another interval
		// before considering the block again, and sends nothing.
		if sm.blockDownloads.ReassertOwner(target.peer, block.hash) {
			// Charged like a request, because that is what it is to the two
			// caps. ReassertOwner clears the forgiven flag, so the block is back
			// in CountForPeer and back in Len from here on, while both budgets
			// were computed with the forgiven records excluded.
			target.charge()

			if !overDepth {
				assigner.remaining--
			}

			continue
		}

		// Record the request before it goes out. A block the ledger will not
		// take is a block we must not ask for: the reply would arrive with
		// nothing vouching for it and cost an honest peer its connection.
		if !sm.blockDownloads.Add(target.peer, block.hash) {
			sm.logger.Warnf("[assignWantedBlocks] block download ledger full at %d blocks, holding off on %s", maxTrackedBlockDownloads, block.hash)

			return
		}

		hash := block.hash
		if err := assigner.recordRequest(target, &hash, overDepth); err != nil {
			// The ledger was told about a request that is not going to be sent,
			// so take it back. Left in place the hash is owned by a peer that
			// was never asked, which answers RequestedWithin for the whole
			// ownership ceiling and quietly holds every later pass off it.
			sm.blockDownloads.RemoveOwner(target.peer, block.hash)

			sm.logger.Warnf(unexpectedFailureAddingInventoryMsg, err)

			return
		}

	}
}

// highestHeld is the height of the highest wanted block that is parked or arriving, or zero if
// none is. A block below it is a gap, which the disk backstop never stops.
func (sm *SyncManager) highestHeld(wanted []wantedBlock) int32 {
	var (
		highest   int32
		lowest    int32
		probeHeld bool
	)

	probes := sm.farProbes.within(wanted)

	for _, block := range wanted {
		if block.height > 0 && (lowest == 0 || block.height < lowest) {
			lowest = block.height
		}

		// Parked or arriving only. A block only asked for holds no bytes, and on 2026-10-08 a
		// request for the top block of the window made each block below it a gap for an hour, so
		// the backstop never applied.
		if !sm.blockPark.Has(block.hash) && !sm.streams.arriving(block.hash) {
			continue
		}

		// A far block given to a peer with no rate (placeUnmeasured) is not counted: it sits at
		// the top of the window, so once it arrives or parks each block below it would be a gap
		// and the backstop would stop nothing in the window. Its bytes still count toward the
		// backstop.
		if _, probe := probes[block.hash]; probe {
			probeHeld = true

			continue
		}

		highest = max(highest, block.height)
	}

	// A held probe still needs the lowest wanted block before the chain can reach it. Were probes
	// alone over the backstop, nothing else held, that block would extend and never be asked: the
	// chain would stop with the bytes that stop it waiting on the chain.
	if probeHeld {
		highest = max(highest, lowest)
	}

	return highest
}
