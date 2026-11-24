package subtreeprocessor

import (
	"runtime"

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
		stp.laneFill = newLaneFill(st)
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

		bm := roaring64.New()
		bm.AddMany(positions)
		bm.RunOptimize()
		bitmaps.Lanes[uint8(l)] = bm //nolint:gosec // l is at most MaxLane
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

// sealLaneBitmaps keeps the lane bitmaps of st, the open subtree being sealed, and ends its record.
func (stp *SubtreeProcessor) sealLaneBitmaps(st *subtreepkg.Subtree) {
	if stp.laneKeeper == nil {
		return
	}

	fill := stp.laneFill
	stp.laneFill = laneFill{}

	stp.keepLaneBitmapsOf(st, &fill)
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
