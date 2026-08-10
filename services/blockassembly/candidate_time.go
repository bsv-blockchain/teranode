package blockassembly

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
)

// maxFutureBlockTime mirrors the two-hours-in-the-future ceiling that
// model.Block CheckHeaderContextual applies to every header unconditionally —
// unlike the median-time rule, that check needs no currentChain, so the local
// submit path does enforce it (model/Block.go).
//
// Deliberately a second copy rather than a shared constant, for now. Declaring
// it once is issue #1528, which settles on `settings` as the home — the same
// place PR #1481 had to put the eleven-block window, since `settings` cannot
// import `model` without an import cycle. Doing it here would edit
// model/Block.go while #1481 is changing that same consensus-critical function,
// which is a merge conflict worth not having.
const maxFutureBlockTime = 2 * time.Hour

// mtpFloorEntry is the median-time-past floor for one parent block. The median
// of the 11 headers ending at a given hash can never change — headers are
// immutable once stored — so it is computed once per parent and reused for
// every miner poll on that tip. The two flags latch their log lines so each
// fires once per parent rather than once per poll: both conditions stay engaged
// for as long as the underlying clock problem lasts, on a path miners poll
// several times a second.
type mtpFloorEntry struct {
	parent        chainhash.Hash
	minTime       int64
	warnedFloor   atomic.Bool
	warnedCeiling atomic.Bool
}

// candidateTime returns the timestamp to stamp on a mining candidate built on
// parentHeader: the local wall clock, floored at median-time-past+1 of the
// parent chain. Block validation rejects any block whose timestamp is not
// strictly greater than the median timestamp of the previous 11 blocks
// (model.Block CheckHeaderContextual), so without the floor a lagging local
// clock — or a run of forward-stamped blocks dragging the median above
// wall-clock time — makes the assembler hand miners a candidate that the
// network refuses once mined. SV Node applies the same floor in UpdateTime, as
// max(median-time-past+1, GetAdjustedTime()); note it floors against
// network-adjusted time where this floors against the raw local clock, so a
// skewed node here has nothing pulling it back towards its peers. Consulting
// the MedianTimeSource in services/legacy/blockchain would close that gap.
//
// The two consensus bounds on a block timestamp are a pair, and this function
// honours both. The median rule is the floor; the two-hours-in-the-future rule
// is the ceiling. A local clock lagging far enough behind the network puts
// median-time-past+1 above now+2h, leaving no timestamp that satisfies both.
// Flooring anyway would hand miners a candidate whose every solution fails
// block.Valid on our own submit path, and that failure is treated as a
// subtree-processor fault: it deletes the job and calls Reset, so each solution
// would trigger a full block-assembly reset. Rather than emit unmineable work,
// candidateTime fails the poll and names the clock skew. Chains with
// GenerateSupported downgrade the median rule to a warning, so there the
// wall-clock candidate is still valid and is served instead of failing.
//
// Availability is preserved everywhere else. The floor only changes the outcome
// when the wall clock is at or below median-time-past, which is rare, whereas
// the lookup it needs is a blockchain round-trip that can fail at any time. A
// lookup failure therefore degrades to the bare wall clock with an error log —
// exactly the pre-PR behaviour — rather than failing the poll. That matters
// most for generateEmptyBlockCandidate, whose whole purpose is to keep handing
// miners work while a block is being processed.
//
// Headers are fetched by parent hash rather than height so a candidate built on
// a side chain gets the median of its own chain.
func (b *BlockAssembler) candidateTime(ctx context.Context, parentHeader *model.BlockHeader) (int64, error) {
	if parentHeader == nil {
		return 0, errors.NewProcessingError("[candidateTime] nil parent header")
	}

	parentHash := parentHeader.Hash()
	timeNow := time.Now().Unix()

	entry := b.mtpFloor(ctx, parentHeader, parentHash)
	if entry == nil {
		// mtpFloor has already logged the cause. Serve the wall clock rather
		// than stopping mining on a blockchain hiccup.
		return timeNow, nil
	}

	// Check the ceiling before applying the floor: when the two cross there is
	// no valid timestamp at all and flooring makes things worse, not better.
	if maxTime := timeNow + int64(maxFutureBlockTime/time.Second); entry.minTime > maxTime {
		if b.generateSupported() {
			// Latched per parent like the floor warning below: this branch stays
			// engaged for as long as the clock condition lasts, and it sits on a
			// path polled several times a second per connected miner.
			if entry.warnedCeiling.CompareAndSwap(false, true) {
				b.logger.Warnf("[candidateTime] parent %s median-time-past floor %d exceeds the two-hour future bound %d; serving the wall clock %d because this chain treats the median-time rule as advisory", parentHash.String(), entry.minTime, maxTime, timeNow)
			}

			return timeNow, nil
		}

		// This is the only new branch that takes mining output to zero, and the
		// error it returns is never logged locally: GetMiningCandidate and
		// generateEmptyBlockCandidate propagate it verbatim, and the gRPC handler
		// returns errors.WrapGRPC(err) without a log line. Unlogged, an operator
		// whose node has stopped producing blocks has to reconstruct the cause
		// from the miners' error strings. Latched on the same flag as the
		// carve-out above, which is mutually exclusive with this branch.
		//
		// Both messages name the two causes rather than only the local clock.
		// The condition is a gap between this node's clock and the parent
		// chain's timestamps, and it is silent about which of the two moved: a
		// slow local clock produces it, and so does a parent chain stamped into
		// the future by a misconfigured or hostile upstream miner. An operator
		// reading only "fix your clock" chases NTP while the real cause is
		// upstream.
		if entry.warnedCeiling.CompareAndSwap(false, true) {
			b.logger.Errorf("[candidateTime] block production has stopped for parent %s: its median-time-past floor %d is above the two-hour future bound %d, so no timestamp satisfies both consensus rules; either the local clock is at least %d seconds behind the network and must be corrected, or the parent chain is future-stamped by an upstream miner", parentHash.String(), entry.minTime, maxTime, entry.minTime-maxTime)
		}

		prometheusBlockAssemblerCandidateTimeClockSkew.Inc()

		return 0, errors.NewProcessingError("[candidateTime] parent %s median-time-past floor %d exceeds the two-hour future bound %d, so no timestamp satisfies both consensus rules; the local clock is at least %d seconds behind the parent chain, which is either local clock skew or a future-stamped parent chain", parentHash.String(), entry.minTime, maxTime, entry.minTime-maxTime)
	}

	if timeNow < entry.minTime {
		if entry.warnedFloor.CompareAndSwap(false, true) {
			b.logger.Warnf("[candidateTime] local clock %d is at or below the parent chain's median-time-past; flooring candidate time to %d for blocks on parent %s", timeNow, entry.minTime, parentHash.String())
		}

		timeNow = entry.minTime
	}

	return timeNow, nil
}

// MinCandidateTime returns the median-time-past floor candidateTime computed for
// parentHash, and whether it is known. It is a pure memo read: the entry is warm
// whenever a candidate was just built on that parent, so the submit path can
// reject a miner-supplied nTime below the consensus floor without a round-trip.
// A false second return means there is no floor for the submit path to enforce —
// the candidate was served on the degraded wall-clock path, the tip has since
// moved twice, or this chain treats the median rule as advisory — in which case
// the submit path behaves as it did before the floor existed.
//
// That is a property of the moment the solution arrives, not of the job it was
// built from, and the two can disagree. A degraded poll memoizes nothing, so a
// later poll on the same parent can succeed and memoize a floor that a solution
// against the earlier job then fails; within a job's TTL the guard can therefore
// reject a candidate this node itself served unfloored. The rejection is right —
// that block is peer-invalid either way — and submitMiningSolution logs that
// case as ours rather than as a miner's bad timestamp.
func (b *BlockAssembler) MinCandidateTime(parentHash *chainhash.Hash) (int64, bool) {
	// The "this chain treats the median rule as advisory" decision has to live in
	// one place or the producer and the guard drift apart. candidateTime's ceiling
	// branch deliberately serves an unfloored wall-clock candidate on chains with
	// GenerateSupported, while the memo entry it read still carries a minTime above
	// now+2h. mining.Mine copies the candidate's own time into the solution
	// unconditionally, so enforcing that floor here would reject every solution
	// against a candidate this package chose to hand out — turning the carve-out
	// that keeps rapid generation working into the thing that stops it.
	if parentHash == nil || b.generateSupported() {
		return 0, false
	}

	if entry := b.memoizedMtpFloor(parentHash); entry != nil {
		return entry.minTime, true
	}

	return 0, false
}

// mtpFloor returns the memoized median-time-past floor for parentHash,
// computing it on a miss. It returns nil — having logged the cause — when the
// floor cannot be established, leaving the caller to decide how to degrade.
//
// The memo has two slots because the two candidate paths key on different
// parents: GetMiningCandidate's busy branch uses the blockchain service's tip
// while the main path uses the subtree processor's precomputed parent. A single
// slot would thrash whenever those two alternate, refetching on every poll and
// re-firing the floor warning each time.
func (b *BlockAssembler) mtpFloor(ctx context.Context, parentHeader *model.BlockHeader, parentHash *chainhash.Hash) *mtpFloorEntry {
	if entry := b.memoizedMtpFloor(parentHash); entry != nil {
		return entry
	}

	// Single-flight the miss. Neither GetMiningCandidate entry point holds a lock
	// and every miner polls several times a second, so a cold tip is entered
	// concurrently as a matter of course — the check-then-compute-then-store
	// below is not atomic. Unflighted, each racer fetches the same eleven
	// headers, each wins its own CompareAndSwap and warns, and each stores into
	// its own round-robin slot, so one parent occupies both and evicts the other
	// candidate path's entry. Sharing one flight per parent makes the entry a
	// single object again, which is what the per-entry log latches assume.
	//
	// DoChan rather than Do so that sharing the flight does not also share the
	// leader's deadline: Do blocks every follower until the leader's fetch
	// returns, however long after their own contexts died. Each caller therefore
	// waits on its own context and degrades to the wall clock when it expires,
	// which is the same degradation a failed lookup already takes. A caller that
	// gives up memoizes nothing, so the next poll re-flights.
	//
	// The flight itself still runs on whichever context entered it first, so a
	// leader that is cancelled mid-fetch returns nil to the followers waiting on
	// it and costs them a floor for that one poll. Detaching the flight from its
	// leader would fix that too, but it would leave the fetch with no deadline at
	// all and nothing to cancel it; one self-healing poll is the cheaper problem.
	ch := b.mtpFloorFlight.DoChan(parentHash.String(), func() (interface{}, error) {
		// Re-check inside the flight: a previous flight for this parent may have
		// completed between the read above and getting here.
		if entry := b.memoizedMtpFloor(parentHash); entry != nil {
			return entry, nil
		}

		return b.computeMtpFloor(ctx, parentHeader, parentHash), nil
	})

	select {
	case res := <-ch:
		entry, _ := res.Val.(*mtpFloorEntry)

		return entry
	case <-ctx.Done():
		// No log line: the caller is abandoning this poll, so the candidate this
		// feeds is already on its way to being discarded. computeMtpFloor logs
		// the causes that matter.
		return nil
	}
}

// memoizedMtpFloor returns the memo entry for parentHash, or nil on a miss.
// Reads stay lock-free: the slots are atomic pointers and entries are immutable
// apart from their own atomic log latches.
func (b *BlockAssembler) memoizedMtpFloor(parentHash *chainhash.Hash) *mtpFloorEntry {
	for i := range b.mtpFloorMemo {
		if entry := b.mtpFloorMemo[i].Load(); entry != nil && entry.parent.IsEqual(parentHash) {
			return entry
		}
	}

	return nil
}

// placeMtpFloor stores entry, reusing the slot its parent already occupies so a
// single parent can never hold both and evict the other candidate path's entry.
// The round-robin victim is therefore always a different parent.
func (b *BlockAssembler) placeMtpFloor(entry *mtpFloorEntry) {
	for i := range b.mtpFloorMemo {
		if cur := b.mtpFloorMemo[i].Load(); cur != nil && cur.parent.IsEqual(&entry.parent) {
			b.mtpFloorMemo[i].Store(entry)

			return
		}
	}

	slot := b.mtpFloorMemoNext.Add(1) % uint64(len(b.mtpFloorMemo))
	b.mtpFloorMemo[slot].Store(entry)
}

// computeMtpFloor does the actual lookup and median calculation for one parent.
// It runs inside a single flight, so at most one caller per parent is here at a
// time. It returns nil — having logged the cause — when the floor cannot be
// established, leaving the caller to decide how to degrade.
func (b *BlockAssembler) computeMtpFloor(ctx context.Context, parentHeader *model.BlockHeader, parentHash *chainhash.Hash) *mtpFloorEntry {
	// A transport failure and an unusable result call for opposite responses, so
	// the fetch and the verification are separate steps. If the call itself fails
	// the blockchain service is unhealthy, and walking it once per header would
	// add MedianTimeBlocks-1 more sequential reads without adding any information —
	// exactly the amplification to avoid, since a saturated service is what caused
	// the failure. Only a result that arrives but cannot be trusted (mis-anchored,
	// unlinked, or short) is worth the walk, because that is the reorg race the
	// walk exists to recover from and its per-hash reads cannot hit it.
	headers, fetchErr := b.fetchParentChainHeaders(ctx, parentHash)
	if fetchErr != nil {
		b.logFloorLookupFailure(parentHash, "the blockchain service did not answer the batched header fetch (%v); not attempting the %d-read parent-chain walk against a service that is already failing", fetchErr, blockchain.MedianTimeBlocks-1)

		return nil
	}

	run := headers

	if verifyErr := verifyParentChainRun(parentHash, headers); verifyErr != nil {
		walked, walkErr := b.walkParentChain(ctx, parentHeader, blockchain.MedianTimeBlocks)
		if walkErr != nil {
			b.logFloorLookupFailure(parentHash, "the batched header fetch returned an unusable run (%v) and the hash-keyed parent-chain walk also failed (%v)", verifyErr, walkErr)

			return nil
		}

		// Name the cause of the slower path rather than discarding the error
		// silently: the walk costs one round-trip per header, so an operator
		// seeing the extra latency needs to know why it engaged.
		b.logger.Warnf("[candidateTime] batched header fetch for parent %s was unusable (%v); fell back to the hash-keyed parent-chain walk", parentHash.String(), verifyErr)

		run = walked
	}

	timestamps := make([]time.Time, len(run))
	for i, header := range run {
		timestamps[i] = time.Unix(int64(header.Timestamp), 0)
	}

	// Defensive only: CalculateMedianTimestamp errors on an empty slice, and both
	// producers above guarantee at least one header.
	medianTimestamp, err := model.CalculateMedianTimestamp(timestamps)
	if err != nil {
		b.logFloorLookupFailure(parentHash, "the median timestamp over %d headers could not be calculated (%v)", len(run), err)

		return nil
	}

	entry := &mtpFloorEntry{parent: *parentHash, minTime: medianTimestamp.Unix() + 1}

	b.placeMtpFloor(entry)

	return entry
}

// generateSupported reports whether this chain lets blocks be generated quickly,
// in which case model.Block CheckHeaderContextual downgrades a median-time
// violation from an error to a warning. Guarded because a nil Settings deref
// past the struct's guard page is an unrecoverable fault.
func (b *BlockAssembler) generateSupported() bool {
	return b.settings != nil && b.settings.ChainCfgParams != nil && b.settings.ChainCfgParams.GenerateSupported
}

// logFloorLookupFailure reports that the median-time-past floor could not be
// established, latched to the last parent it fired for. The floor-lookup failure
// modes persist while the underlying problem does, and this sits on a path polled
// several times a second per connected miner, so an unlatched line floods the log
// during precisely the incident an operator is trying to read it through. The
// latch is a single slot rather than per-entry because these are the paths where
// no memo entry exists to hang a flag on.
func (b *BlockAssembler) logFloorLookupFailure(parentHash *chainhash.Hash, format string, args ...interface{}) {
	if prev := b.mtpFloorFailureLoggedTip.Swap(parentHash); prev != nil && prev.IsEqual(parentHash) {
		return
	}

	b.logger.Errorf("[candidateTime] cannot establish the median-time-past floor for parent %s: "+format+"; serving an unfloored candidate time", append([]interface{}{parentHash.String()}, args...)...)
}

// fetchParentChainHeaders performs the batched header fetch and nothing else, so
// its caller can tell a transport failure apart from a result that arrived but
// cannot be trusted. The two want opposite responses: see mtpFloor.
func (b *BlockAssembler) fetchParentChainHeaders(ctx context.Context, parentHash *chainhash.Hash) ([]*model.BlockHeader, error) {
	headers, _, err := b.blockchainClient.GetBlockHeaders(ctx, parentHash, blockchain.MedianTimeBlocks)
	if err != nil {
		return nil, errors.NewProcessingError("failed to fetch parent-chain headers for %s", parentHash.String(), err)
	}

	return headers, nil
}

// verifyParentChainRun checks that a batched run is complete, anchored at the
// parent and correctly linked. Any mismatch means the batched lookup cannot be
// trusted — its fast path is a height-range scan over main-chain flags and its
// CTE stops early on an unresolved parent_id, neither of which is an atomic
// parent-chain walk — and the caller falls back to walkParentChain.
func verifyParentChainRun(parentHash *chainhash.Hash, headers []*model.BlockHeader) error {
	if len(headers) == 0 || headers[0] == nil || !headers[0].Hash().IsEqual(parentHash) {
		return errors.NewProcessingError("returned chain head does not match requested parent %s", parentHash.String())
	}

	if uint64(len(headers)) > blockchain.MedianTimeBlocks {
		return errors.NewProcessingError("batched run below parent %s returned %d headers, more than the %d requested", parentHash.String(), len(headers), blockchain.MedianTimeBlocks)
	}

	for i := 1; i < len(headers); i++ {
		if headers[i] == nil {
			return errors.NewProcessingError("nil header at depth %d below parent %s", i, parentHash.String())
		}

		if !headers[i-1].HashPrevBlock.IsEqual(headers[i].Hash()) {
			return errors.NewProcessingError("parent-chain link broken at depth %d below parent %s", i, parentHash.String())
		}
	}

	// The anchor and link checks cannot see a run truncated at its oldest end:
	// such a run is still anchored at the parent and still correctly linked, but
	// its median is taken over a narrower window and is biased high, which feeds
	// straight into the two-hour ceiling above. A short run is only legitimate
	// when it reaches the start of the chain.
	if uint64(len(headers)) < blockchain.MedianTimeBlocks && !isChainStart(headers[len(headers)-1]) {
		return errors.NewProcessingError("short parent-chain run (%d of %d) below parent %s does not reach the chain start", len(headers), blockchain.MedianTimeBlocks, parentHash.String())
	}

	return nil
}

// walkParentChain returns up to depth headers ending at startHeader by following
// HashPrevBlock one header at a time. Each read is keyed by hash — immutable once
// stored — so the result cannot be poisoned by a reorg happening between reads,
// at the cost of one round-trip per header. Unlike the equivalents in
// subtreevalidation and legacy/netsync it tolerates reaching the start of the
// chain (returning fewer than depth headers), because block assembly runs from
// genesis on fresh networks; the validator applies the median rule over however
// many headers exist in the same way.
//
// The run is seeded with startHeader rather than re-fetching it: the caller
// already holds the parent header, so fetching it again would spend one of the
// depth round-trips re-reading data in hand — on the path that engages precisely
// when the blockchain service is struggling.
func (b *BlockAssembler) walkParentChain(ctx context.Context, startHeader *model.BlockHeader, depth uint64) ([]*model.BlockHeader, error) {
	if startHeader == nil {
		return nil, errors.NewProcessingError("nil start header")
	}

	headers := make([]*model.BlockHeader, 0, depth)
	headers = append(headers, startHeader)

	cur := startHeader

	for uint64(len(headers)) < depth {
		if isChainStart(cur) {
			break
		}

		header, _, err := b.blockchainClient.GetBlockHeader(ctx, cur.HashPrevBlock)
		if err != nil {
			return nil, errors.NewProcessingError("failed to fetch header %s at depth %d", cur.HashPrevBlock.String(), len(headers), err)
		}

		if header == nil {
			return nil, errors.NewProcessingError("nil header for %s at depth %d", cur.HashPrevBlock.String(), len(headers))
		}

		headers = append(headers, header)
		cur = header
	}

	return headers, nil
}

// isChainStart reports whether header is the start of the chain: the genesis
// block's HashPrevBlock is all zeroes, so nothing links below it.
func isChainStart(header *model.BlockHeader) bool {
	return header.HashPrevBlock == nil || header.HashPrevBlock.IsEqual(&chainhash.Hash{})
}
