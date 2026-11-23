package lanereader

import (
	"sync"
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
	// validator (validator.Options.PrefetchedParents). Today's answer is the parent's mined
	// heights.
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
	// KindNotLoaded: today says mined in a block the index never loaded, older than its start, and
	// lanes says not. Not a disagreement.
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

// lookup answers pos on v, timing one lookup in lookupSampleEvery.
func (r *Reader) lookup(v *lane.ChainView, pos uint64) lane.Answer {
	if r.lookups.Add(1)%lookupSampleEvery != 0 {
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
	if r == nil {
		return
	}

	if v == nil {
		count(reader, KindNotReady, lane.ViaNone)

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
		onChain       []uint32
		unknown       bool
		lowestOnChain uint32 = ^uint32(0)
	)

	for _, id := range todayBlockIDs {
		state, _ := s.Block(id)
		if state == lane.BlockNotLoaded {
			unknown = true

			continue
		}

		if h, ok := v.Height(id); ok {
			onChain = append(onChain, id)
			lowestOnChain = min(lowestOnChain, h)
		}
	}

	switch {
	case len(onChain) > 0:
		switch a.Via {
		case lane.ViaRow:
			for _, id := range onChain {
				if id == a.BlockID {
					return KindAgree
				}
			}

			return KindDifferentBlock
		case lane.ViaFloor:
			if lowestOnChain <= s.AnchorHeight() {
				return KindAgree
			}

			return KindDifferentBlock
		}

		if state, _ := s.Block(onChain[0]); state == lane.BlockNoRows {
			return KindNoRows
		}

		// On a chain the floors do not hold, a folded block's rows are no longer in memory.
		if !v.FloorValid() && s.Folded(onChain[0]) {
			return KindNotLoaded
		}

		return KindTodayOnly
	case unknown:
		// Today names a block the index never loaded: older than its start, or forgotten.
		switch a.Via {
		case lane.ViaFloor:
			return KindAgree
		case lane.ViaRow:
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

// CompareDuplicates compares the lanes answer to block validation's duplicate check for block
// blockID on chain v, which must include it, with today's verdict: whether setMined found a
// transaction of the block already mined on its chain.
func (r *Reader) CompareDuplicates(v *lane.ChainView, hash chainhash.Hash, blockID uint32, todayDuplicate bool) {
	if r == nil {
		return
	}

	if v == nil {
		count(ReaderBlockDuplicate, KindNotReady, lane.ViaNone)

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
// one a minute.
type disagreementLog struct {
	mu   sync.Mutex
	seen map[string]int
	last map[string]time.Time
}

const disagreementsLogged = 5

func (d *disagreementLog) log(r *Reader, reader, kind string, hash chainhash.Hash, pos uint64, a lane.Answer, s *lane.IndexSnapshot,
	todayForm string, todayMined bool, todayValue uint32, todayBlockIDs []uint32) {
	key := reader + "/" + kind

	d.mu.Lock()

	if d.seen == nil {
		d.seen, d.last = map[string]int{}, map[string]time.Time{}
	}

	d.seen[key]++
	n := d.seen[key]

	if n > disagreementsLogged && time.Since(d.last[key]) < time.Minute {
		d.mu.Unlock()

		return
	}

	d.last[key] = time.Now()
	d.mu.Unlock()

	laneNum, p := lane.Unpack(pos)

	switch todayForm {
	case "height":
		r.logger.Warnf("[lanereader] %s %s (%d so far): tx %s lane %d position %d: lanes mined=%t via %s block %d height %d; today mined=%t height %d; index tip %d anchor %d start %d",
			reader, kind, n, hash.String(), laneNum, p, a.Mined, a.Via, a.BlockID, a.Height, todayMined, todayValue, s.TipHeight(), s.AnchorHeight(), s.StartHeight())
	case "duplicate":
		r.logger.Warnf("[lanereader] %s %s (%d so far): block %s (ID %d): lanes counts %d positions taken before it; today duplicate=%t; index tip %d anchor %d start %d",
			reader, kind, n, hash.String(), a.BlockID, todayValue, todayMined, s.TipHeight(), s.AnchorHeight(), s.StartHeight())
	default:
		r.logger.Warnf("[lanereader] %s %s (%d so far): tx %s lane %d position %d: lanes mined=%t via %s block %d height %d; today block IDs %v; index tip %d anchor %d start %d",
			reader, kind, n, hash.String(), laneNum, p, a.Mined, a.Via, a.BlockID, a.Height, todayBlockIDs, s.TipHeight(), s.AnchorHeight(), s.StartHeight())
	}
}
