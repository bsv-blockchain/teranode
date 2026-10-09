package netsync

import (
	"sort"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
)

// THE WATCHER (PR 1699, 2026-10-08). The
// schedule gives blocks by rate and queue with no block size, because a size is not known before
// the download. The watcher corrects the blocks that turn out late. Each tick it examines up to
// watchBlocks owed blocks from the tip, in height sequence, and estimates when each lands
// (ownerArrival):
//   - Arriving: the declared size is known from the first bytes, so the time is its remaining
//     bytes at the copy's rate. Under raceStallRate the block is the race's.
//   - Queued: the bytes its owner sends first and the block, a recent mean block for each block of
//     unknown size, at the owner's rate.
// The chain applies a block only after each block below it, so a block that lands after each
// block below it adds to the chain's wait. When it adds watchMinETA or more, its request is
// watchMinAge old, and it has one owner, one more peer is asked:
//   - Arriving: the peer that lands a full copy soonest, if in half the owner's time or less. A
//     copy with more than half its bytes gets no helper while it will land within
//     watchPastHalfWait.
//   - Queued: the fastest peer with rescueFasterBy times the owner's rate that lands the block
//     at the recent mean size in half the owner's time.
// A block whose every copy is stalled is the race's; no block above it counts as late until it is
// resolved, since the chain waits on it whatever lands above.
// The owner keeps its request and its connection; the first complete copy converts and the other
// drains at the sink. One block gets one more getdata at the maximum. Headers-first mode only: the
// heights come from the header cache, which is empty above the last checkpoint.
//
// It replaces the rescue rule, which examined only the lowest owed block, 30 s after its getdata,
// against a deadline from the chain's pace.

const (
	// rescueFasterBy is how many times sooner, or faster, a helper must be.
	rescueFasterBy = 2
	// watchBlocks is how many owed blocks from the tip the watcher examines.
	watchBlocks = 64
	// watchMinAge is how long a request runs before the watcher judges it.
	watchMinAge = 10 * time.Second
	// watchMinETA is the least time a block must add to the chain's wait, landing after each block
	// below it, to get a helper: a copy for less costs more than the wait. On 2026-10-08 a helper
	// for each block that landed after the blocks below it, by any margin, discarded 8% of the
	// bytes received.
	watchMinETA = 30 * time.Second
	// watchPastHalfWait is the longest a copy with more than half its bytes may still take and get
	// no helper. Below it the bytes a helper discards cost more than the wait it saves; above it
	// the chain waits on one slow copy for longer than any helper costs.
	watchPastHalfWait = 5 * time.Minute
)

// arrivalVerdict is how ownerArrival judged one owner's copy of a block.
type arrivalVerdict int

const (
	// arrivalJudged: the estimate holds.
	arrivalJudged arrivalVerdict = iota
	// arrivalTooEarly: the request or its bytes are younger than watchMinAge, or the copy waits for
	// an admission slot. The block is left for a later tick.
	arrivalTooEarly
	// arrivalStalled: the copy arrives under raceStallRate, so it is the race's.
	arrivalStalled
)

// maxEstimate is the longest arrival estimate the watcher uses: 1,000 hours. A stopped copy counts
// at 1 byte a second, and with gigabytes to come the conversion to a duration went past the int64
// limit and turned negative, so the stalled peer looked like the fastest one. The bound leaves room
// to double an estimate.
const maxEstimate = 1000 * time.Hour

// estimate is the time to send bytes at rate, at most maxEstimate.
func estimate(bytes, rate float64) time.Duration {
	if rate <= 0 {
		return maxEstimate
	}

	secs := bytes / rate
	if secs >= maxEstimate.Seconds() {
		return maxEstimate
	}

	return time.Duration(secs * float64(time.Second))
}

// watchedBlock is an owed block the watcher examines.
type watchedBlock struct {
	rec    queuedBlock
	height int32
}

// watchOwedBlocks runs on the race's ticker: while the chain waits nothing else starts a download
// pass. The committed tip is read here, outside each lock.
func (sm *SyncManager) watchOwedBlocks(now time.Time) {
	if sm.streams == nil || sm.blockDownloads == nil || sm.headerCache == nil || sm.blockSizeTracker == nil || !sm.headersFirstMode.Load() {
		return
	}

	typical := sm.blockSizeTracker.meanRecentSize()
	if typical <= 0 {
		return
	}

	tip, _, ok := sm.committedTip()
	if !ok {
		return
	}

	queues := sm.blockDownloads.Queues()
	used := make(map[*peerpkg.Peer]bool)

	var latest time.Duration

	for _, b := range sm.owedBlocksFromTip(queues, tip, watchBlocks) {
		owners := sm.blockDownloads.OwnersOf(b.rec.hash)
		if len(owners) == 0 {
			continue
		}

		eta, size, state := farOff, int64(0), ""
		judged, anyJudged, stalled := true, false, false

		for _, o := range owners {
			e, sz, st, v := sm.ownerArrival(o, b.rec.hash, queues[o], typical, now)

			switch v {
			case arrivalStalled:
				// The race's copy. Another owner's copy may still judge the block.
				stalled = true

				continue
			case arrivalTooEarly:
				judged = false
			case arrivalJudged:
				anyJudged = true

				if e < eta {
					eta, size, state = e, sz, st
				}
			}

			if !judged {
				break
			}
		}

		if !judged {
			continue
		}

		if !anyJudged {
			// Every copy is stalled: the block is the race's, and the chain waits on it whatever
			// lands above it, so no block above it counts as late until it is resolved. A helper
			// for one of those would not move the chain.
			if stalled {
				latest = farOff
			}

			continue
		}

		// The time this block adds to the chain's wait: how much later it lands than each block
		// below it. A few seconds is noise between peers, not a block that stops the chain.
		adds := eta - latest
		latest = max(latest, eta)

		if adds < watchMinETA || len(owners) != 1 || sm.streams.wasRaced(b.rec.hash, now) {
			continue
		}

		owner := owners[0]

		// A copy with more than half its bytes gets no helper while it lands within
		// watchPastHalfWait: the helper must send the full block, and the owner's nearly complete
		// copy is discarded if the helper wins. On 2026-10-08 from 14:31 to 14:49, 8% of the bytes
		// received were discarded. A copy past half that will take longer is helped all the same:
		// at 150 KB/s with 900 MB to come the chain would wait 100 minutes on it.
		if read, total, _, arriving := sm.streams.arrivingFrom(b.rec.hash, owner); arriving && total > 0 && read*2 > total && eta <= watchPastHalfWait {
			continue
		}

		helper, helperETA := sm.watchHelper(queues, owner, b, size, typical, eta, used)
		if helper == nil || !sm.askRacer(helper, b.rec.hash, now) {
			continue
		}

		used[helper] = true

		sm.waste.rescued.Add(1)
		sm.logger.Infof("[watch][%s] block %d, asked %s ago, %s %s: estimated %s there against %s at %s, so %s was asked too and the owner keeps its request",
			b.rec.hash, b.height, now.Sub(b.rec.at).Round(time.Second), state, owner, fmtETA(eta), helperETA.Round(time.Second), helper, helper)
	}
}

// owedBlocksFromTip is up to limit blocks above tip that a peer owes, not let off, and that this
// node does not hold, in height sequence, each with its record at the owner holding it earliest.
func (sm *SyncManager) owedBlocksFromTip(queues map[*peerpkg.Peer][]queuedBlock, tip int32, limit int) []watchedBlock {
	byHash := make(map[chainhash.Hash]watchedBlock)

	for _, queue := range queues {
		for _, b := range queue {
			height, ok := sm.headerCache.HeightOf(b.hash)
			if !ok || height <= tip {
				continue
			}

			// Held: parked, committing, or converting a complete copy. A block converting as it
			// arrives is not held yet; it is one to judge.
			if sm.blockPark.Has(b.hash) || sm.blockCommitting(b.hash) || (sm.conversionInFlight(b.hash) && !sm.streams.arriving(b.hash)) {
				continue
			}

			if cur, seen := byHash[b.hash]; !seen || b.seq < cur.rec.seq {
				byHash[b.hash] = watchedBlock{rec: b, height: height}
			}
		}
	}

	out := make([]watchedBlock, 0, len(byHash))
	for _, b := range byHash {
		out = append(out, b)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].height < out[j].height })

	if len(out) > limit {
		out = out[:limit]
	}

	return out
}

// watchHelper is the peer to ask for a late block, and when it would land the block; nil when no
// peer qualifies. size is the block's declared size when its bytes are arriving, zero when it is
// queued and its size is not known. A peer with no rate, a peer used this tick, and a peer whose
// advertised height is below the block are not helpers.
func (sm *SyncManager) watchHelper(queues map[*peerpkg.Peer][]queuedBlock, owner *peerpkg.Peer, b watchedBlock, size, typical int64, ownerETA time.Duration, used map[*peerpkg.Peer]bool) (*peerpkg.Peer, time.Duration) {
	ownerRate := sm.streams.peerRate(owner)

	var (
		best      *peerpkg.Peer
		bestRate  float64
		bestStart time.Duration
		bestETA   time.Duration
	)

	for _, bp := range sm.eligibleBlockPeers() {
		if bp.peer == owner || used[bp.peer] {
			continue
		}

		if bp.state != nil && bp.state.BestKnownHeight() > 0 && bp.state.BestKnownHeight() < b.height {
			continue
		}

		rate := sm.streams.peerRate(bp.peer)
		if rate <= 0 {
			continue
		}

		start := sm.queuedArrival(bp.peer, queues[bp.peer], 0, typical, 0, rate)

		if size > 0 {
			// The size is known: the peer that lands a full copy soonest, in half the owner's time.
			eta := start + estimate(float64(size), rate)
			if eta*rescueFasterBy <= ownerETA && (best == nil || eta < bestETA) {
				best, bestETA = bp.peer, eta
			}

			continue
		}

		// The size is not known: the fastest peer with rescueFasterBy times the owner's rate
		// that lands the block at the recent mean size in half the owner's time. Without the
		// time test, helpers with long queues of their own were asked for small gains, and on
		// 2026-10-08 from 23:30 the share of discarded bytes rose from 4.0% to 7.5%. There is
		// no test that the helper starts before the owner: a silent owner's first block starts
		// at 0 s by the estimate, and that test refused every helper for it.
		eta := start + estimate(float64(typical), rate)
		if eta*rescueFasterBy > ownerETA {
			continue
		}

		if ownerRate <= 0 || rate >= rescueFasterBy*ownerRate {
			if best == nil || rate > bestRate || (rate == bestRate && start < bestStart) {
				best, bestRate, bestStart = bp.peer, rate, start
				bestETA = start + estimate(float64(typical), rate)
			}
		}
	}

	return best, bestETA
}

// fmtETA prints an arrival estimate. farOff equals maxEstimate, so a capped real estimate and an
// owner that will not deliver print the same; the log line's state names which it is.
func fmtETA(d time.Duration) string {
	if d >= maxEstimate {
		return "over " + maxEstimate.String()
	}

	return d.Round(time.Second).String()
}

// farOff is an arrival estimate for an owner that will not deliver: a forgiven owner sending no
// copy, or a copy that has stopped.
const farOff = maxEstimate

// ownerArrival estimates when block h lands at owner o, and the block's declared size when its
// bytes are arriving from o, zero otherwise. The verdict is arrivalTooEarly when the copy must be
// given more time (its request or its bytes are younger than watchMinAge, or it waits for an
// admission slot), and arrivalStalled when its bytes arrive under raceStallRate, so it is the
// race's.
//
// A copy arriving from o is judged on its own rate: its remaining bytes at that rate, and a fresh
// copy must fetch the whole block. A copy not yet started is judged by o's queue: the bytes o is
// still sending, a typical block for each block ahead of it and its own typical size, at o's rate.
// An owner let off the block (forgiven, so not in its queue) and sending no copy will not deliver.
func (sm *SyncManager) ownerArrival(o *peerpkg.Peer, h chainhash.Hash, queue []queuedBlock, typical int64, now time.Time) (eta time.Duration, ownSize int64, state string, verdict arrivalVerdict) {
	// A copy waiting for an admission slot is this node's delay, not the owner's.
	if sm.streams.awaitingAdmission(h, o) {
		return 0, 0, "", arrivalTooEarly
	}

	if read, total, start, arriving := sm.streams.arrivingFrom(h, o); arriving {
		elapsed := now.Sub(start)
		if elapsed < watchMinAge {
			return 0, 0, "", arrivalTooEarly
		}

		if read <= 0 || float64(read)/elapsed.Seconds() < raceStallRate {
			return 0, 0, "", arrivalStalled
		}

		rate := float64(read) / elapsed.Seconds()

		return estimate(float64(total-read), rate), total, "arrives slowly from", arrivalJudged
	}

	var (
		rec    queuedBlock
		queued bool
	)

	for _, b := range queue {
		if b.hash == h {
			rec, queued = b, true

			break
		}
	}

	if !queued {
		return farOff, 0, "was let off by", arrivalJudged
	}

	if now.Sub(rec.at) < watchMinAge {
		return 0, 0, "", arrivalTooEarly
	}

	// An owner with no rate has sent no block bytes: it lands the block very late. The median rate
	// made it look as fast as the fastest peer.
	rate := sm.streams.peerRate(o)
	if rate <= 0 {
		return farOff, 0, "has no rate at", arrivalJudged
	}

	// The queued block's own size is not known: it counts at the recent mean, as each block ahead
	// of it does.
	return sm.queuedArrival(o, queue, rec.seq, typical, typical, rate), 0, "waits behind", arrivalJudged
}

// queuedArrival estimates when a block lands at p: the bytes still to come on every copy p is
// sending now (pending), one typical block for each block queued ahead of it (before seq) and not
// arriving from p, and the block's own typical size, at rate. A queued block arriving from another
// peer still counts: p sends its own copy all the same. A seq of zero puts the block at the back of the
// queue, as a new request is.
func (sm *SyncManager) queuedArrival(p *peerpkg.Peer, queue []queuedBlock, seq uint64, typical, ownSize int64, rate float64) time.Duration {
	sending, _ := sm.streams.pending(p)

	var queued int64

	for _, b := range queue {
		if seq != 0 && b.seq >= seq {
			continue
		}

		if _, _, _, from := sm.streams.arrivingFrom(b.hash, p); from {
			continue
		}

		queued++
	}

	bytes := float64(sending + queued*typical + ownSize)

	return estimate(bytes, rate)
}
