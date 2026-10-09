// Package chainancestry answers one question as a value: which block sits at each height on
// one branch of the chain.
//
// An Ancestry is a proven run of block ids, one per height over [lo, hi], every one an ancestor
// of the anchor block or the anchor itself. It is built by walking parent links back from the
// anchor and proving every step by hash linkage, so a stale main-chain flag cannot steer it.
// Once built it holds no client and no method takes a context, so it can do no I/O: a store
// that receives one reads it like an array and makes no chain call of its own.
//
// Two callers share it. The pruner's stamp asks BlockID at each height of a containment
// window, anchored on the best tip. The validator asks Contains for a parent's candidate
// blocks, anchored on the parent of the block being validated.
package chainancestry

import (
	"sort"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

// Ancestry is a proven run of block ids, one per height over [lo, hi]. Inside the range
// membership is final: present means ancestor, absent means not. Outside the range it says
// nothing, and Covers is how a caller asks.
type Ancestry struct {
	lo, hi   uint32
	anchor   chainhash.Hash
	byHeight []uint32 // index = height - lo
	sorted   []uint32 // the same ids, sorted, for Contains
	notMined []uint32 // heights whose block row had mined_set false at build time, ascending
}

// New assembles an Ancestry from ids already proven, one per height from lo upward. It is the
// constructor Build ends with and the one the test helper uses; production code that is not
// the builder has no business calling it, because an unproven run is a chain answer nobody
// checked.
func New(lo uint32, anchor chainhash.Hash, ids []uint32, notMined []uint32) (*Ancestry, error) {
	if len(ids) == 0 {
		return nil, errors.NewProcessingError("[chainancestry] an ancestry needs at least one height")
	}

	sorted := make([]uint32, len(ids))
	copy(sorted, ids)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	nm := make([]uint32, len(notMined))
	copy(nm, notMined)
	sort.Slice(nm, func(i, j int) bool { return nm[i] < nm[j] })

	byHeight := make([]uint32, len(ids))
	copy(byHeight, ids)

	return &Ancestry{
		lo:       lo,
		hi:       lo + uint32(len(ids)) - 1, //nolint:gosec // bounded by the span asked for
		anchor:   anchor,
		byHeight: byHeight,
		sorted:   sorted,
		notMined: nm,
	}, nil
}

// Lo is the lowest height the ancestry speaks for.
func (a *Ancestry) Lo() uint32 { return a.lo }

// Hi is the highest height the ancestry speaks for, which is the anchor's height.
func (a *Ancestry) Hi() uint32 { return a.hi }

// Anchor is the hash the walk started from. Every id in the ancestry is an ancestor of it, or
// the anchor itself.
func (a *Ancestry) Anchor() chainhash.Hash { return a.anchor }

// Covers reports whether the ancestry can speak for height h.
func (a *Ancestry) Covers(h uint32) bool { return h >= a.lo && h <= a.hi }

// BlockID is the id of the block on this branch at height h. ok is false outside the range.
func (a *Ancestry) BlockID(h uint32) (uint32, bool) {
	if !a.Covers(h) {
		return 0, false
	}

	return a.byHeight[h-a.lo], true
}

// Contains reports whether block id is on this branch inside the range.
func (a *Ancestry) Contains(id uint32) bool {
	i := sort.Search(len(a.sorted), func(i int) bool { return a.sorted[i] >= id })

	return i < len(a.sorted) && a.sorted[i] == id
}

// NotMined returns the lowest height in [lo, hi] whose block had mined_set false when the
// ancestry was built, and whether there was one. mined_set is the blocks-table flag that says
// every transaction of the block has had its containment recorded, so a false inside a window
// means that window's containment may still be incomplete.
func (a *Ancestry) NotMined(lo, hi uint32) (uint32, bool) {
	i := sort.Search(len(a.notMined), func(i int) bool { return a.notMined[i] >= lo })
	if i < len(a.notMined) && a.notMined[i] <= hi {
		return a.notMined[i], true
	}

	return 0, false
}

// Pairs returns the (height, id) pairs for [lo, hi] as two parallel int32 slices, the shape a
// SQL statement binds as int[] arrays. Heights outside the ancestry's range are left out, so
// the caller must check Covers first if it needs the whole span.
func (a *Ancestry) Pairs(lo, hi uint32) (heights, ids []int32) {
	if lo < a.lo {
		lo = a.lo
	}

	if hi > a.hi {
		hi = a.hi
	}

	if lo > hi {
		return nil, nil
	}

	n := int(hi - lo + 1)
	heights = make([]int32, 0, n)
	ids = make([]int32, 0, n)

	for h := lo; ; h++ {
		heights = append(heights, int32(h))          //nolint:gosec // a height fits int32
		ids = append(ids, int32(a.byHeight[h-a.lo])) //nolint:gosec // a block id fits int32

		if h == hi {
			break
		}
	}

	return heights, ids
}
