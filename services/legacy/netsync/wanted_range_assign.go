package netsync

import (
	"time"

	"github.com/bsv-blockchain/go-wire"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
)

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

	// Read here, before wantedBlocks takes headerMu: committedTip makes a
	// blocking blockchain call, and this package's lock rule has no exception
	// for one made while the header lock is held.
	best, _, _ := sm.committedTip()

	// Below the last checkpoint a fill only ever appends (headerCache.Fill,
	// extendLocked), so nothing else shrinks the list as the tip advances past
	// what it already names. Pruned on every pass, not only after a fill,
	// because most passes here are driven by a commit, not a headers reply —
	// see headerCache.Prune for why height itself, not merely below it, is
	// safe to drop.
	sm.headerCache.Prune(best)

	wanted := sm.wantedBlocks(best)

	// Ahead of the assigner and its budget cap on purpose: whether the cache has
	// run out is a question about the header round, not about whether there is
	// download budget free this instant, so it must be asked on every call this
	// function makes, not only the ones that go on to place a block.
	sm.maybeRequestMoreHeaders(wanted)

	assigner := sm.newDownloadAssigner()
	if assigner == nil {
		return
	}

	// Stop once there are as many candidates as the assigner can place, not after
	// that many heights: the range starts at the block after the tip and runs
	// through blocks already parked or owed. Trimming the range itself left only
	// held blocks to look at, and on 2026-09-24 three peers sat idle with seven
	// blocks parked and five owed.
	candidates := sm.unownedBlocksUpTo(wanted, assigner.remaining)
	if len(candidates) == 0 {
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

// unownedBlocks keeps the wanted blocks this node neither already holds nor
// currently owes, is not given up on, and is not stalled behind a recent
// failure, and lets a quiet owner off the hook on the way past.
//
// wanted must already be capped to the assigner's remaining budget by the
// caller: every check below costs something, several of them a disk read or a
// blockchain round trip, and none of it is worth spending on a candidate this
// pass could not place anyway.
//
// The recently-failed check runs first, and it only skips the one entry it
// names, not everything above it: a failed parent's own children earn no mark
// of their own until each has actually been downloaded once and refused on
// arrival (handleBlockMsgHead's delivery-side check, keyed on the arriving
// block's own parent hash), so the first pass after a failure still names
// them. What this check bounds is every pass after that one: once a hash is
// marked, whether the parent itself or a child the cascade has since caught,
// it is not requested again for the life of the mark, rather than being
// downloaded and refused afresh every single pass for as long as the parent
// stays failed. Honouring dispatcher.inFlight is what stops a parent that is
// being retried right now from being misread as a dead one: it was
// re-admitted, so its children must not be held back on the strength of an
// attempt that may yet succeed.
//
// The disk checks run next, ahead of the ledger, because a block already on
// disk should not spend a peer's budget nor be forgiven on a peer's behalf:
// it is simply done. holdsBlock and the park's own index answer from the
// filesystem alone.
//
// blockGivenUpOn is checked ahead of the transient-failure backoff, not
// folded into the same map read: a block past its attempt ceiling must never
// be requested again in this process, where a block merely inside its backoff
// window is asked for again once that window passes, and conflating the two
// would either request a given-up block early or never retry a merely
// backed-off one. Both are map reads, and both run ahead of the blockchain
// round trip below for exactly that reason: the cheap checks come first, so
// the round trip is only ever spent on a candidate none of them already
// rejected.
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
// Forgiveness runs before the budgets are read, not after. A peer that has gone
// quiet holding a full slice keeps every one of those slots against its
// per-peer cap until somebody releases them, so a pass that read the budgets
// first would find that peer at zero and hand out nothing — the stall this rule
// exists to break. ForgiveOwners keeps the ownership and drops only the
// obligation, so a copy still on the wire from the quiet peer is admitted when
// it lands.
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

		// A block in flight is never asked for again, as SV Node never asks for a block
		// in mapBlocksInFlight: its bytes are arriving or it is being converted, whatever
		// the ledger says about who owes it. The ledger can let every peer off a block
		// that is still arriving, and on 2026-09-24 that asked for block 734,077
		// seventeen times in thirteen seconds.
		if sm.streams.arriving(block.hash) || sm.conversionInFlight(block.hash) {
			continue
		}

		// #1333: a block that recently failed to store or validate, judged or
		// merely unlucky, is not requested again while the mark stands. This is
		// what actually bounds the cascade review measured: the descendants of
		// a failed parent are not individually marked until each has been
		// downloaded once and refused on arrival by the delivery-side check in
		// handleBlockMsgHead, so the first pass after a failure still names all
		// of them — but every pass after that skips whichever ones the cascade
		// has already caught, rather than re-downloading the same read-ahead
		// depth of blocks for as long as the parent stays written off.
		// dispatcher.inFlight is honoured for the same reason the delivery-side
		// check honours it: a parent being retried right now was re-admitted,
		// so it is not a failed parent and must not hold its children back.
		if sm.recentlyFailedBlocks != nil && !sm.dispatcher.inFlight(block.hash) {
			if _, failed := sm.recentlyFailedBlocks.Get(block.hash); failed {
				continue
			}
		}

		// The park's own in-memory index, checked separately from holdsBlock:
		// Admit registers a block there synchronously, before its blob write
		// is handed to a worker, so a pass that lands in the gap between
		// admission and the write landing on disk must still see it as held.
		// Without this, a pass triggered from inside the admission itself —
		// parkOrphanBlock's own top-up call among them — re-requests the block
		// it is that same instant holding, spending a peer's slot on a block
		// already safely on its way to disk.
		if sm.blockPark.Has(block.hash) {
			continue
		}

		// blockGivenUpOn: past its attempt ceiling, this block must not be
		// requested again in this process at all, not merely throttled. This
		// is the check handleBlockMsg's delivery-side blockGivenUpOn guards
		// against ever being reached in the first place — without it here, the
		// only thing stopping a re-request was ever the transient backoff
		// below, which forgets the block once its own window passes and lets
		// the whole attempt count restart from a peer that answers nothing new.
		// Checked ahead of the blockchain round trip below: it is a map read,
		// not a network call, so a block this cheap check would already
		// reject is never charged the round trip's cost first.
		if sm.blockGivenUpOn(block.hash) {
			continue
		}

		// The #1187 transient-failure backoff. A block that just failed with a
		// local, non-judgemental fault (dropBlockFromWalk records it) must wait
		// out its backoff window before the next pass asks for it again, or the
		// throttle that exists to stop a re-decorate storm never actually
		// throttles anything: without this check the backoff map fills but
		// nothing here ever reads it, and a block that cannot be stored yet is
		// downloaded again on every pass instead of once per window. Also
		// ahead of the round trip below for the same reason blockGivenUpOn is:
		// it is the cheaper check, so it runs first.
		if sm.blockFailureBackoff != nil {
			if fs, backedOff := sm.blockFailureBackoff.Get(block.hash); backedOff && time.Now().Before(fs.nextRetry) {
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
			if !sm.blockPark.Has(block.hash) && !sm.dispatcher.inFlight(block.hash) &&
				sm.blockPark.adoptStranded(sm.ctx, block.hash, sm.subtreeStore) {
				sm.logger.Warnf("[unownedBlocks][%s] adopted a complete record at height %d that was on disk but not in the park", block.hash, block.height)
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
		if sm.blockDownloads.AnyOwner(block.hash, func(p *peerpkg.Peer) bool {
			return time.Since(sm.streams.lastBlockBytes(p)) < blockRequestRetryInterval
		}) {
			continue
		}

		if quiet := sm.blockDownloads.ForgiveOwners(block.hash, blockRequestRetryInterval); len(quiet) > 0 {
			// Logged per block because it is what lets a block be asked of a second peer, and
			// a duplicate copy can then only be traced back through this line.
			sm.waste.reAskedQuiet.Add(1)

			for _, p := range quiet {
				last := sm.streams.lastBlockBytes(p)
				since := "never"

				if !last.IsZero() {
					since = time.Since(last).Round(time.Second).String()
				}

				sm.logger.Infof("[reRequest][%s] height %d: %s owed it and last sent block bytes %s ago; it may be asked of another peer", block.hash, block.height, p, since)
			}

			block.reAsked = true
		}

		candidates = append(candidates, block)
	}

	return candidates
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
// (handleBlockMsg calls RemoveOwner on the answering peer), so it looks
// unrequested and costs an honest peer its whole association.
//
// On a node with one peer that means the block is not re-asked at all, which is
// the right answer rather than a gap: there is nobody to help, so the only thing
// a second getdata could achieve is the disconnect above. Recovery is the peer's
// own stall detection and the ledger's expiry.
func (sm *SyncManager) requestBlocks(assigner *downloadAssigner, candidates []wantedBlock, highestHeld int32) {
	for _, block := range candidates {
		// The disk backstop bounds how far ahead the node reaches, never a gap below
		// blocks it already holds: on 2026-09-24 parked blocks over the budget
		// stopped the one missing block above the tip being asked for, and the
		// chain stopped with them.
		extends := block.height > highestHeld
		if assigner.overBackstop && extends {
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
			target, ok = assigner.takeAvoiding(block.height, owes)
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

// highestHeld is the height of the highest wanted block that is parked or already asked for, or
// zero if none is.
func (sm *SyncManager) highestHeld(wanted []wantedBlock) int32 {
	var highest int32

	for _, block := range wanted {
		if sm.blockPark.Has(block.hash) || sm.blockDownloads.RequestedWithin(block.hash, blockRequestAssignmentTTL) {
			highest = max(highest, block.height)
		}
	}

	return highest
}
