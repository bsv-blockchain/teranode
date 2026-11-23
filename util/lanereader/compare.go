package lanereader

import (
	"context"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/util/lane"
)

// Readers: the checks that ask whether a transaction is mined, each answered from the index beside
// today's answer.
const (
	// ReaderValidator is the validator's parent read (ParentOutputsForValidation): is each parent
	// mined, and at what height. Today's answer is the store's.
	ReaderValidator = "validator"
	// ReaderSubtreePrefetch is the same check on parents subtree validation read in bulk for the
	// validator (validator.Options.PrefetchedParents). Today's answer is the parent's mined block
	// list: the check itself uses the lowest mined height, but a height may be a fork block's, and
	// the record carries the list.
	ReaderSubtreePrefetch = "subtree_prefetch"
	// ReaderSubtreeAlreadyMined is subtree validation's check that a transaction it blesses is not
	// already mined on our chain. Today's answer is the record's mined block list.
	ReaderSubtreeAlreadyMined = "subtree_already_mined"
	// ReaderBlockParent is block validation's check that each parent outside the block is mined on
	// the block's chain. Today's answer is the parent record's mined block list.
	ReaderBlockParent = "block_parent"
	// ReaderBlockDuplicate is block validation's check that the block holds no transaction already
	// mined on its chain, per block. Today's answer is setMined's verdict.
	ReaderBlockDuplicate = "block_duplicate"
)

var readers = []string{ReaderValidator, ReaderSubtreePrefetch, ReaderSubtreeAlreadyMined, ReaderBlockParent, ReaderBlockDuplicate}

// Kinds of comparison between the lanes answer and today's.
const (
	// KindAgree: both say not mined, or both say mined in the same block. A floor answer, mined at
	// least the settle depth deep in a block it does not name, agrees with today's mined block
	// when that block is at or below the floor's anchor.
	KindAgree = "agree"
	// KindLanesOnly: lanes says mined, today says not.
	KindLanesOnly = "lanes_only"
	// KindTodayOnly: today says mined in a block the index loaded with rows, lanes says not.
	KindTodayOnly = "today_only"
	// KindDifferentBlock: both say mined, in different blocks.
	KindDifferentBlock = "different_block"
	// KindNoPosition: the transaction has no lane position, so lanes cannot answer.
	KindNoPosition = "no_position"
	// KindNoRows: today says mined in a block the map has no rows for (quick validation, or our own
	// block until block assembly writes rows), and lanes says not. Not a disagreement.
	KindNoRows = "no_rows"
	// KindNotLoaded: the index cannot judge the check. The block today names is outside what the
	// index loaded or has been forgotten, a height names a fork block whose rows are gone, the
	// chain's older ancestry is not held, or the duplicate check cannot place the earlier copy.
	// Counted whether or not today says mined. Not a disagreement.
	KindNotLoaded = "not_loaded"
	// KindNotReady: the index had not loaded the chain to judge against yet.
	KindNotReady = "not_ready"
	// KindOffChain: today's answer is a height, and a loaded block at that height off the main
	// chain took the position, such as one a reorg replaced: the store's height is the lowest it
	// recorded, from any chain. Not a disagreement.
	KindOffChain = "off_chain"
)

var kinds = []string{KindAgree, KindLanesOnly, KindTodayOnly, KindDifferentBlock, KindNoPosition, KindNoRows, KindNotLoaded, KindNotReady, KindOffChain}

// isDisagreement reports whether kind is one the soak must explain.
func isDisagreement(kind string) bool {
	return kind == KindLanesOnly || kind == KindTodayOnly || kind == KindDifferentBlock
}

// lookup answers pos on v, timing one lookup in lookupSampleEvery, chosen at random so the hot
// path shares no counter between goroutines.
func (r *Reader) lookup(v *lane.ChainView, pos uint64) lane.Answer {
	if rand.Uint64()%lookupSampleEvery != 0 {
		return v.Lookup(pos)
	}

	start := time.Now()
	a := v.Lookup(pos)
	prometheusLaneReaderLookupSeconds.Observe(time.Since(start).Seconds())

	return a
}

// lookupSampleEvery is how often a lookup is timed: two clock reads and a histogram observation
// cost about what a lookup does.
const lookupSampleEvery = 64

// CompareHeight compares the lanes answer for a transaction on the main chain with today's answer
// given as a height: mined at height, or not mined. The validator's parent read answers so.
func (r *Reader) CompareHeight(reader string, hash chainhash.Hash, pos uint64, todayMined bool, todayHeight uint32) {
	if r == nil {
		return
	}

	s := r.Snapshot()
	if s == nil {
		count(reader, KindNotReady, lane.ViaNone)

		return
	}

	if !lane.Valid(pos) {
		count(reader, KindNoPosition, lane.ViaNone)

		return
	}

	a := r.lookup(s.Main(), pos)
	kind := classifyHeight(s, pos, a, todayMined, todayHeight)
	count(reader, kind, a.Via)

	if isDisagreement(kind) {
		r.disagree.log(r, reader, kind, hash, pos, a, s, "height", todayMined, todayHeight, nil)
	}
}

func classifyHeight(s *lane.IndexSnapshot, pos uint64, a lane.Answer, todayMined bool, todayHeight uint32) string {
	if !todayMined {
		if a.Mined {
			return KindLanesOnly
		}

		return KindAgree
	}

	if a.Via == lane.ViaRow && a.Height == todayHeight {
		return KindAgree
	}

	// The height alone does not name a block: a fork block at it may be the one today means.
	if s.TakenOffMain(pos, todayHeight) {
		return KindOffChain
	}

	// A fork block at that height the index cannot judge, one forgotten, such as a reorg's loser
	// the floors have passed, or one with no rows, may be the one today means.
	if s.ForkAt(todayHeight) && (a.Via != lane.ViaFloor || todayHeight > s.AnchorHeight()) {
		return KindNotLoaded
	}

	switch a.Via {
	case lane.ViaRow:
		return KindDifferentBlock
	case lane.ViaFloor:
		if todayHeight <= s.AnchorHeight() {
			return KindAgree
		}

		return KindDifferentBlock
	}

	id, ok := s.MainBlockAt(todayHeight)
	if !ok {
		return KindNotLoaded
	}

	if state, _ := s.Block(id); state == lane.BlockNoRows {
		return KindNoRows
	}

	return KindTodayOnly
}

// CompareBlocks compares the lanes answer for a transaction on chain v with today's answer given
// as the record's mined block list: today's answer is the listed blocks on v, so both answers are
// judged against the same chain.
func (r *Reader) CompareBlocks(reader string, v *lane.ChainView, hash chainhash.Hash, pos uint64, todayBlockIDs []uint32) {
	r.compareBlocks(reader, v, KindNotReady, hash, pos, todayBlockIDs)
}

// compareBlocks is CompareBlocks counting noView when v is nil.
func (r *Reader) compareBlocks(reader string, v *lane.ChainView, noView string, hash chainhash.Hash, pos uint64, todayBlockIDs []uint32) {
	if r == nil {
		return
	}

	if v == nil {
		count(reader, noView, lane.ViaNone)

		return
	}

	if !lane.Valid(pos) {
		count(reader, KindNoPosition, lane.ViaNone)

		return
	}

	a := r.lookup(v, pos)
	kind := classifyBlocks(v, a, todayBlockIDs)
	count(reader, kind, a.Via)

	if isDisagreement(kind) {
		r.disagree.log(r, reader, kind, hash, pos, a, v.Snapshot(), "blocks", false, 0, todayBlockIDs)
	}
}

func classifyBlocks(v *lane.ChainView, a lane.Answer, todayBlockIDs []uint32) string {
	s := v.Snapshot()

	var (
		// onChain is whether today names a block on the chain, firstOnChain the first such,
		// rowBlockOnChain whether a.BlockID is one of them, and lowestOnChain the lowest height.
		onChain         bool
		firstOnChain    uint32
		rowBlockOnChain bool
		// unknown is a block today names that may be on the chain: one the index never loaded,
		// or a known one the view cannot place. foldedOutside is the second kind for a folded
		// block the floor's claims cover.
		unknown       bool
		foldedOutside bool
		lowestOnChain uint32 = ^uint32(0)
	)

	for _, id := range todayBlockIDs {
		state, _ := s.Block(id)
		if state == lane.BlockNotLoaded {
			unknown = true

			continue
		}

		h, ok := v.Height(id)
		if !ok {
			// A loaded block missing from the view may still be an ancestor on a fork whose older
			// ancestry the index no longer holds, or, on a chain given as a window of block IDs,
			// when the block is folded and so may be older than the window. An unfolded block
			// missing from the window is treated as off the chain. At the defaults the window
			// (blockchain retention times two, 576 blocks) is deeper than the settle depth (100),
			// so such a block is a fork; a node configured with a window shallower than the settle
			// depth can count an older main-chain block this way, as different_block or lanes_only.
			if v.IDSet() && s.Folded(id) {
				unknown, foldedOutside = true, true
			}

			unknown = unknown || v.Disconnected()

			continue
		}

		if !onChain {
			onChain, firstOnChain = true, id
		}

		rowBlockOnChain = rowBlockOnChain || id == a.BlockID
		lowestOnChain = min(lowestOnChain, h)
	}

	switch {
	case onChain:
		switch a.Via {
		case lane.ViaRow:
			if rowBlockOnChain {
				return KindAgree
			}

			return KindDifferentBlock
		case lane.ViaFloor:
			if lowestOnChain <= s.AnchorHeight() {
				return KindAgree
			}

			return KindDifferentBlock
		}

		if state, _ := s.Block(firstOnChain); state == lane.BlockNoRows {
			return KindNoRows
		}

		// On a chain the floors do not hold, a folded block's rows are no longer in memory.
		if !v.FloorValid() && s.Folded(firstOnChain) {
			return KindNotLoaded
		}

		return KindTodayOnly
	case unknown:
		// Today names a block the index cannot place on the chain. A floor answer agrees only with
		// a folded block the index knows, outside the caller's window of IDs: a block it never
		// loaded may be an orphan, so the floor's settled block need not be the one today names.
		switch {
		case a.Via == lane.ViaFloor && foldedOutside:
			return KindAgree
		case a.Via == lane.ViaRow:
			return KindDifferentBlock
		}

		return KindNotLoaded
	default:
		if a.Mined {
			return KindLanesOnly
		}

		return KindAgree
	}
}

// ShadowDuplicates loads the ancestry of the stored block hash, as ID blockID, and compares the
// lane index's answer to the duplicate check with setMined's verdict. A block below the index's
// start counts not_loaded, without a walk back.
func (r *Reader) ShadowDuplicates(ctx context.Context, hash chainhash.Hash, blockID uint32, todayDuplicate bool) {
	if r == nil {
		return
	}

	v, noView := r.prepare(ctx, hash)
	r.compareDuplicates(v, noView, hash, blockID, todayDuplicate)
}

// compareDuplicates compares the lanes answer to block validation's duplicate check for block
// blockID on chain v, which must include it, with today's verdict: whether setMined found a
// transaction of the block already mined on its chain. With v nil it counts noView.
func (r *Reader) compareDuplicates(v *lane.ChainView, noView string, hash chainhash.Hash, blockID uint32, todayDuplicate bool) {
	if r == nil {
		return
	}

	if v == nil {
		count(ReaderBlockDuplicate, noView, lane.ViaNone)

		return
	}

	byRow, byFloor, hasRows := v.TakenBefore(blockID)
	taken := byRow + byFloor

	// A block already folded into the floors, as setMined's backlog can reach during catch-up,
	// has rows whose bitmaps have left memory: the index cannot say.
	if state, _ := v.Snapshot().Block(blockID); state == lane.BlockHasRows && v.Snapshot().Folded(blockID) {
		count(ReaderBlockDuplicate, KindNotLoaded, lane.ViaNone)

		return
	}

	via := lane.ViaNone

	switch {
	case byRow > 0:
		via = lane.ViaRow
	case byFloor > 0:
		via = lane.ViaFloor
	}

	var kind string

	switch {
	case !hasRows:
		kind = KindNoRows
	case taken == 0 && v.Disconnected():
		// The ancestors the view does not hold may hold the earlier copy.
		kind = KindNotLoaded
	case (taken > 0) == todayDuplicate:
		kind = KindAgree
	case taken > 0:
		kind = KindLanesOnly
	default:
		// setMined checks every ancestor; the index holds only what it loaded, so the earlier
		// block may be older than its start, and the verdict does not say which block it was.
		kind = KindNotLoaded
	}

	count(ReaderBlockDuplicate, kind, via)

	if isDisagreement(kind) {
		r.disagree.log(r, ReaderBlockDuplicate, kind, hash, 0, lane.Answer{Mined: taken > 0, BlockID: blockID}, v.Snapshot(), "duplicate", todayDuplicate, uint32(min(taken, uint64(^uint32(0)))), nil)
	}
}

// disagreementLog logs the first disagreementsLogged disagreements of each reader and kind, then
// one a minute, however many parent checks run at once. It takes no lock on the path that decides
// not to log, so a burst of disagreements from many parent checks at once does not serialise them.
type disagreementLog struct {
	gates sync.Map // reader/kind to *logGate
}

// logGate is one reader and kind's count and the time of its last log line, in Unix nanoseconds.
type logGate struct {
	seen atomic.Int64
	last atomic.Int64
}

const disagreementsLogged = 5

func (d *disagreementLog) log(r *Reader, reader, kind string, hash chainhash.Hash, pos uint64, a lane.Answer, s *lane.IndexSnapshot,
	todayForm string, todayMined bool, todayValue uint32, todayBlockIDs []uint32) {
	key := reader + "/" + kind

	g, ok := d.gates.Load(key)
	if !ok {
		// The gate starts its minute when made, so a check that finds the first lines taken cannot
		// log again before a minute has passed.
		fresh := &logGate{}
		fresh.last.Store(time.Now().UnixNano())
		g, _ = d.gates.LoadOrStore(key, fresh)
	}

	gate := g.(*logGate)

	// seen counts the lines written; the totals are in teranode_lane_reader_answers_total. Each of
	// the first lines is reserved by a compare-and-swap, so checks at once cannot write more.
	// Past them, a disagreement inside the minute only reads the gate, so a run of them from many
	// goroutines shares the cache line without writing it.
	var n int64

	for n == 0 {
		seen := gate.seen.Load()
		if seen >= disagreementsLogged {
			break
		}

		if gate.seen.CompareAndSwap(seen, seen+1) {
			n = seen + 1
			gate.last.Store(time.Now().UnixNano())
		}
	}

	if n == 0 {
		now := time.Now().UnixNano()
		last := gate.last.Load()

		if now-last < int64(time.Minute) || !gate.last.CompareAndSwap(last, now) {
			return
		}

		n = gate.seen.Add(1)
	}

	laneNum, p := lane.Unpack(pos)

	switch todayForm {
	case "height":
		r.logger.Warnf("[lanereader] %s %s (log line %d): tx %s lane %d position %d: lanes mined=%t via %s block %d height %d; today mined=%t height %d; index tip %d anchor %d start %d",
			reader, kind, n, hash.String(), laneNum, p, a.Mined, a.Via, a.BlockID, a.Height, todayMined, todayValue, s.TipHeight(), s.AnchorHeight(), s.StartHeight())
	case "duplicate":
		r.logger.Warnf("[lanereader] %s %s (log line %d): block %s (ID %d): lanes counts %d positions taken before it; today duplicate=%t; index tip %d anchor %d start %d",
			reader, kind, n, hash.String(), a.BlockID, todayValue, todayMined, s.TipHeight(), s.AnchorHeight(), s.StartHeight())
	default:
		r.logger.Warnf("[lanereader] %s %s (log line %d): tx %s lane %d position %d: lanes mined=%t via %s block %d height %d; today block IDs %v; index tip %d anchor %d start %d",
			reader, kind, n, hash.String(), laneNum, p, a.Mined, a.Via, a.BlockID, a.Height, todayBlockIDs, s.TipHeight(), s.AnchorHeight(), s.StartHeight())
	}
}
