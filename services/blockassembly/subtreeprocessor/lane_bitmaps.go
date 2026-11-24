package subtreeprocessor

import (
	"bytes"
	"encoding/binary"
	"runtime"
	"slices"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/util/lane"
	"golang.org/x/sync/errgroup"
)

// At utxostore_laneMigration shadow the processor builds the lane bitmaps (util/lane) of each
// subtree it chains before a mining candidate can list the subtree, and hands them to the keeper,
// which block assembly reads when it stores a block it mined. A transaction's position is
// recorded as it is added to the open subtree, so sealing the subtree builds its bitmaps without
// reading the pool. A subtree whose recorded positions do not account for every member, such as
// one a bulk rebuild filled, has its bitmaps built from the positions the pool keeps instead.

// LaneBitmapsKeeper keeps the lane bitmaps of the subtrees the processor chains, for the lane
// rows of the blocks block assembly mines from them.
type LaneBitmapsKeeper interface {
	// KeepLaneBitmaps keeps a subtree's lane bitmaps; unpositioned counts its members that have
	// no lane position and are left out of them.
	KeepLaneBitmaps(bitmaps lane.SubtreeBitmaps, unpositioned int)

	// HasLaneBitmaps reports whether the lane bitmaps of subtree root are kept.
	HasLaneBitmaps(root chainhash.Hash) bool
}

// laneFill holds the lane positions recorded for the members of one open subtree.
type laneFill struct {
	subtree *subtreepkg.Subtree
	// lanes holds the recorded positions within each lane, indexed by lane, so recording one is
	// an append with no map lookup.
	lanes *[lane.MaxLane + 1][]uint64
	// unpositioned counts the members recorded without a valid position.
	unpositioned int
	// added counts the members recorded, with a position or without one.
	added int
	// removed holds the positions of recorded members taken out of the subtree again.
	removed []uint64
}

// newLaneFill starts an empty record for st.
func newLaneFill(st *subtreepkg.Subtree) laneFill {
	return laneFill{subtree: st, lanes: new([lane.MaxLane + 1][]uint64)}
}

// startLaneFill starts an empty record for st on the per-lane lists the last sealed record left,
// emptied, when there are some.
func (stp *SubtreeProcessor) startLaneFill(st *subtreepkg.Subtree) laneFill {
	if lanes := stp.laneSpare; lanes != nil {
		stp.laneSpare = nil
		return laneFill{subtree: st, lanes: lanes}
	}

	return newLaneFill(st)
}

// add records one member's position, 0 or another invalid one for none.
func (f *laneFill) add(position uint64) {
	f.added++

	if !lane.Valid(position) {
		f.unpositioned++
		return
	}

	l, p := lane.Unpack(position)
	f.lanes[l] = append(f.lanes[l], p)
}

// keepsLaneBitmaps reports whether the processor builds lane bitmaps for its subtrees: at shadow,
// with a keeper to hand them to.
func (stp *SubtreeProcessor) keepsLaneBitmaps() bool {
	return stp.laneKeeper != nil
}

// recordLanePosition records position, 0 for none, for a member just added to st. A subtree other
// than the one being recorded starts a new record.
func (stp *SubtreeProcessor) recordLanePosition(st *subtreepkg.Subtree, position uint64) {
	if stp.laneKeeper == nil {
		return
	}

	if stp.laneFill.subtree != st || stp.laneFill.lanes == nil {
		stp.laneFill = stp.startLaneFill(st)
	}

	stp.laneFill.add(position)
}

// recordLaneMemberRemoved notes that to replaced from as the open subtree without the member whose
// position is position. When the position is not known the record is dropped, and the subtree's
// bitmaps are built from the pool when it is sealed.
func (stp *SubtreeProcessor) recordLaneMemberRemoved(from, to *subtreepkg.Subtree, position uint64, known bool) {
	if stp.laneKeeper == nil || stp.laneFill.subtree != from {
		return
	}

	if !known {
		stp.laneFill = laneFill{}
		return
	}

	stp.laneFill.subtree = to
	stp.laneFill.removed = append(stp.laneFill.removed, position)
}

// matches reports whether the record holds a position, or none, for each member of st.
func (f *laneFill) matches(st *subtreepkg.Subtree) bool {
	return f.subtree == st && f.lanes != nil && f.added-len(f.removed) == laneMembers(st)
}

// laneMembers is the number of st's members, leaving out the coinbase placeholder.
func laneMembers(st *subtreepkg.Subtree) int {
	n := len(st.Nodes)
	if n > 0 && st.Nodes[0].Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
		n--
	}

	return n
}

// bitmaps builds the record's lane bitmaps for subtree root, and the count of its members
// without a position.
func (f *laneFill) bitmaps(root chainhash.Hash) (lane.SubtreeBitmaps, int) {
	bitmaps := lane.SubtreeBitmaps{SubtreeHash: root, Lanes: lane.LaneBitmaps{}}
	unpositioned := f.unpositioned

	for l, positions := range f.lanes {
		if len(positions) == 0 {
			continue
		}

		bitmaps.Lanes[uint8(l)] = laneBitmap(positions) //nolint:gosec // l is at most MaxLane
	}

	for _, position := range f.removed {
		if !lane.Valid(position) {
			unpositioned--
			continue
		}

		l, p := lane.Unpack(position)
		if bm := bitmaps.Lanes[l]; bm != nil {
			bm.Remove(p)

			if bm.IsEmpty() {
				delete(bitmaps.Lanes, l)
			}
		}
	}

	return bitmaps, unpositioned
}

// laneDenseFactor bounds the span of a lane's positions laneBitmap builds from a bitset: at most
// this many times the number of positions, so the bitset costs at most this many bits a position.
const laneDenseFactor = 32

// laneRunFactor is the fewest positions a run must average for laneBitmap to add a lane's
// positions as ranges, one per run of positions that follow one another.
const laneRunFactor = 64

// laneBitmap builds one lane's bitmap from positions within the lane, in any order. One pass finds
// their lowest, highest and how many runs of consecutive positions they come in. Few long runs are
// added as ranges. Otherwise, when the positions fit in 32 bits and span at most laneDenseFactor
// times their number within one 2^32 window, they are set in a bitset, whatever their order, and the
// bitmap is built from its words; failing that they are sorted, in a copy when out of order, and
// added with AddMany.
func laneBitmap(positions []uint64) *roaring64.Bitmap {
	bm := roaring64.New()
	if len(positions) == 0 {
		return bm
	}

	lowest, highest, runs := positions[0], positions[0], 1

	for i := 1; i < len(positions); i++ {
		p := positions[i]
		lowest = min(lowest, p)
		highest = max(highest, p)

		if p != positions[i-1]+1 {
			runs++
		}
	}

	switch {
	case runs*laneRunFactor <= len(positions):
		start := 0

		for i := 1; i <= len(positions); i++ {
			if i == len(positions) || positions[i] != positions[i-1]+1 {
				bm.AddRange(positions[start], positions[i-1]+1)
				start = i
			}
		}
	case lowest>>32 == highest>>32 && highest-(lowest&^0xFFFF) < uint64(len(positions))*laneDenseFactor:
		return denseLaneBitmap(positions, lowest, highest)
	default:
		if !slices.IsSorted(positions) {
			positions = slices.Clone(positions)
			slices.Sort(positions)
		}

		bm.AddMany(positions)
	}

	bm.RunOptimize()

	return bm
}

// denseLaneBitmap builds the bitmap of positions, which run from lowest to highest inside one 2^32
// window, from a bitset that starts on the container boundary at or below lowest.
func denseLaneBitmap(positions []uint64, lowest, highest uint64) *roaring64.Bitmap {
	window := lowest &^ 0xFFFFFFFF
	base := lowest &^ 0xFFFF

	words := make([]uint64, (highest-base+64)/64)
	for _, p := range positions {
		off := p - base
		words[off>>6] |= 1 << (off & 63)
	}

	bm32 := roaring.FromDense(words, false)
	if base != window {
		bm32 = roaring.AddOffset(bm32, uint32(base-window)) //nolint:gosec // base-window < 2^32
	}

	bm32.RunOptimize()

	if window == 0 {
		return roaring64.Roaring32AsRoaring64(bm32)
	}

	return roaring32InWindow(bm32, uint32(window>>32)) //nolint:gosec // window>>32 < 2^32
}

// roaring32InWindow returns bm32 as the 64-bit bitmap whose values carry high as their upper 32
// bits, through roaring64's serialized form: a bucket count, then each bucket's key and its 32-bit
// bitmap.
func roaring32InWindow(bm32 *roaring.Bitmap, high uint32) *roaring64.Bitmap {
	var buf bytes.Buffer

	var head [12]byte

	binary.LittleEndian.PutUint64(head[:8], 1)
	binary.LittleEndian.PutUint32(head[8:], high)
	buf.Write(head[:])

	if _, err := bm32.WriteTo(&buf); err != nil {
		panic(err) // a bytes.Buffer write does not fail
	}

	bm := roaring64.New()
	if _, err := bm.ReadFrom(&buf); err != nil {
		panic(err) // the bytes were written just above
	}

	return bm
}

// laneKey is the bucket bitmapsFromPositions groups position v into: 0 for none, or its lane + 1.
func laneKey(v uint64) int {
	if !lane.Valid(v) {
		return 0
	}

	return int(lane.Lane(v)) + 1
}

// bitmapsFromPositions builds the lane bitmaps of subtree root from positions, one per member, 0
// or another invalid one for none, and counts the members without one. It groups positions by lane
// in place, in one counting pass and one swapping pass, so the caller must not read positions
// afterwards; it allocates the bitmaps, and a sorted copy of a lane laneBitmap adds with AddMany.
func bitmapsFromPositions(root chainhash.Hash, positions []uint64) (lane.SubtreeBitmaps, int) {
	const keys = lane.MaxLane + 2

	var count, next, end [keys]int

	for _, v := range positions {
		count[laneKey(v)]++
	}

	offset := 0
	for k := range count {
		next[k] = offset
		offset += count[k]
		end[k] = offset
	}

	// each position is swapped straight into its lane's bucket
	for k := range next {
		for next[k] < end[k] {
			v := positions[next[k]]

			kv := laneKey(v)
			if kv == k {
				next[k]++
				continue
			}

			positions[next[k]], positions[next[kv]] = positions[next[kv]], v
			next[kv]++
		}
	}

	bitmaps := lane.SubtreeBitmaps{SubtreeHash: root, Lanes: lane.LaneBitmaps{}}

	start := count[0]

	for k := 1; k < keys; k++ {
		if count[k] == 0 {
			continue
		}

		segment := positions[start : start+count[k]]
		for i := range segment {
			segment[i] &= lane.MaxPosition
		}

		bitmaps.Lanes[uint8(k-1)] = laneBitmap(segment) //nolint:gosec // k-1 is at most MaxLane
		start += count[k]
	}

	return bitmaps, count[0]
}

// sealLaneBitmaps keeps the lane bitmaps of st, the open subtree being sealed, and ends its record.
func (stp *SubtreeProcessor) sealLaneBitmaps(st *subtreepkg.Subtree) {
	if stp.laneKeeper == nil {
		return
	}

	fill := stp.laneFill
	stp.laneFill = laneFill{}

	stp.keepLaneBitmapsOf(st, &fill)

	// the bitmaps hold no reference to the record's lists, which the next record reuses, emptied
	if fill.lanes != nil {
		for l := range fill.lanes {
			fill.lanes[l] = fill.lanes[l][:0]
		}

		stp.laneSpare = fill.lanes
	}
}

// keepLaneBitmapsOf keeps st's lane bitmaps from fill when it accounts for every member, or else
// from the positions the pool keeps.
func (stp *SubtreeProcessor) keepLaneBitmapsOf(st *subtreepkg.Subtree, fill *laneFill) {
	root := *st.RootHash()

	if fill != nil && fill.matches(st) {
		bitmaps, unpositioned := fill.bitmaps(root)
		stp.laneKeeper.KeepLaneBitmaps(bitmaps, unpositioned)

		return
	}

	stp.keepLaneBitmapsFromPool(st)
}

// keepLaneBitmapsFromPool keeps st's lane bitmaps built from the positions the pool keeps for its
// members. A member the pool does not hold, or whose position cannot be read, leaves st without
// bitmaps, rather than with bitmaps that miss it.
func (stp *SubtreeProcessor) keepLaneBitmapsFromPool(st *subtreepkg.Subtree) {
	root := *st.RootHash()

	if bitmaps, unpositioned, ok := stp.laneBitmapsFromPool(st, root); ok {
		stp.laneKeeper.KeepLaneBitmaps(bitmaps, unpositioned)
		return
	}

	stp.markLaneBitmapsUnbuilt(root)
	stp.logger.Warnf("[SubtreeProcessor][%s] the subtree has no lane bitmaps: the pool does not hold the position of each of its members", root.String())
}

// laneBitmapsFromPool builds st's lane bitmaps from the positions the pool keeps, and reports
// false when a member's position cannot be read.
func (stp *SubtreeProcessor) laneBitmapsFromPool(st *subtreepkg.Subtree, root chainhash.Hash) (lane.SubtreeBitmaps, int, bool) {
	builder := lane.NewBitmapsBuilder()

	for _, node := range st.Nodes {
		if node.Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
			continue
		}

		position, ok := stp.poolLanePosition(node.Hash)
		if !ok {
			return lane.SubtreeBitmaps{}, 0, false
		}

		builder.Add(position)
	}

	return builder.Subtree(root), builder.Unpositioned(), true
}

// poolLanePosition returns the lane position the pool keeps for hash, 0 for none, and whether the
// pool holds hash and its position could be read.
func (stp *SubtreeProcessor) poolLanePosition(hash chainhash.Hash) (uint64, bool) {
	m := stp.currentTxMap

	if lookup, ok := m.(interface {
		LookupPosition(hash chainhash.Hash) (uint64, bool, error)
	}); ok {
		position, found, err := lookup.LookupPosition(hash)

		return position, found && err == nil
	}

	if !m.Exists(hash) {
		return 0, false
	}

	return m.Position(hash), true
}

// markLaneBitmapsUnbuilt records that root's lane bitmaps could not be built, so they are not
// built again for each later mining candidate.
func (stp *SubtreeProcessor) markLaneBitmapsUnbuilt(root chainhash.Hash) {
	if stp.laneUnbuilt == nil {
		stp.laneUnbuilt = make(map[chainhash.Hash]struct{})
	}

	stp.laneUnbuilt[root] = struct{}{}
}

// ensureLaneBitmaps keeps the lane bitmaps of each subtree in subtrees that has none kept, built
// from the pool, in parallel. It runs before chained subtrees reach a mining candidate or the
// subtree store, so a subtree a bulk rebuild chained has its bitmaps too.
func (stp *SubtreeProcessor) ensureLaneBitmaps(subtrees []*subtreepkg.Subtree) {
	if stp.laneKeeper == nil || len(subtrees) == 0 {
		return
	}

	var missing []*subtreepkg.Subtree

	chained := make(map[chainhash.Hash]struct{}, len(subtrees))

	for _, st := range subtrees {
		root := *st.RootHash()
		chained[root] = struct{}{}

		if _, unbuilt := stp.laneUnbuilt[root]; unbuilt {
			continue
		}

		if !stp.laneKeeper.HasLaneBitmaps(root) {
			missing = append(missing, st)
		}
	}

	// the record of unbuilt subtrees keeps only subtrees still chained
	for root := range stp.laneUnbuilt {
		if _, ok := chained[root]; !ok {
			delete(stp.laneUnbuilt, root)
		}
	}

	if len(missing) == 0 {
		return
	}

	type built struct {
		bitmaps      lane.SubtreeBitmaps
		unpositioned int
		ok           bool
	}

	results := make([]built, len(missing))

	var g errgroup.Group

	g.SetLimit(runtime.GOMAXPROCS(0))

	for i, st := range missing {
		g.Go(func() error {
			bitmaps, unpositioned, ok := stp.laneBitmapsFromPool(st, *st.RootHash())
			results[i] = built{bitmaps: bitmaps, unpositioned: unpositioned, ok: ok}

			return nil
		})
	}

	_ = g.Wait()

	for i, st := range missing {
		if results[i].ok {
			stp.laneKeeper.KeepLaneBitmaps(results[i].bitmaps, results[i].unpositioned)
			continue
		}

		root := *st.RootHash()
		stp.markLaneBitmapsUnbuilt(root)
		stp.logger.Warnf("[SubtreeProcessor][%s] the subtree has no lane bitmaps: the pool does not hold the position of each of its members", root.String())
	}
}

// keepIncompleteLaneBitmaps keeps the lane bitmaps of copied, a copy of the open subtree, from the
// open subtree's record when it accounts for every member, or else from the pool. The record is
// left as it is, for the open subtree.
func (stp *SubtreeProcessor) keepIncompleteLaneBitmaps(open, copied *subtreepkg.Subtree) {
	if stp.laneKeeper == nil {
		return
	}

	if stp.laneFill.matches(open) && laneMembers(open) == laneMembers(copied) {
		bitmaps, unpositioned := stp.laneFill.bitmaps(*copied.RootHash())
		stp.laneKeeper.KeepLaneBitmaps(bitmaps, unpositioned)

		return
	}

	stp.keepLaneBitmapsFromPool(copied)
}
