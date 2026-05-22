// Package blockvalidation: per-subtree []Node pool.
//
// Block validation deserialises every subtree into a Subtree.Nodes slice (48
// bytes/node). For a 654M-tx block split across ~624 subtrees of ~1M nodes
// each, that is ~29 GB of fresh heap allocation per block — churned through
// the GC the moment the block leaves the lastValidatedBlocks cache.
//
// The pool below buckets these slices into log2 size classes (1K..4M) so:
//
//  1. Each class has its own sync.Pool — slices for one class do not get
//     swapped in for another. Network-load shifts (subtree size 1K <-> 1M+
//     under heavy load) drain the unused class via sync.Pool's per-GC eviction
//     and let the active class fill up, without manual aging.
//  2. Odd-cap slices are not Put back, so the pool never accumulates
//     pathologically sized slices.
//  3. Requests larger than the maximum class fall through to plain make so the
//     pool never holds onto >4M-node allocations unintentionally.

package blockvalidation

import (
	"sync"

	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
)

// nodePoolClasses are the size-class capacities for the per-subtree Node pool,
// in ascending order. Production subtree size starts at 1024 and rises to 1M+
// under load (governed by `initial_merkle_items_per_subtree`). A request larger
// than the last class skips the pool entirely.
var nodePoolClasses = [...]int{
	1 << 10, // 1K
	1 << 12, // 4K
	1 << 14, // 16K
	1 << 16, // 64K
	1 << 18, // 256K
	1 << 20, // 1M
	1 << 22, // 4M
}

// nodePools holds one sync.Pool per size class. Slices stored here have
// cap == nodePoolClasses[i] exactly; we use the capacity to classify on
// Put so we never mix sizes.
var nodePools [len(nodePoolClasses)]sync.Pool

// classIndexForCap returns the index into nodePoolClasses whose capacity
// exactly matches c, or -1 if c is not a pooled class capacity.
func classIndexForCap(c int) int {
	for i, class := range nodePoolClasses {
		if class == c {
			return i
		}
	}
	return -1
}

// classIndexForWant returns the smallest class index whose capacity is >= want,
// or -1 if want exceeds the largest class.
func classIndexForWant(want int) int {
	for i, class := range nodePoolClasses {
		if class >= want {
			return i
		}
	}
	return -1
}

// GetNodeSlice returns a []subtreepkg.Node with capacity at least `want`,
// drawn from the pool when possible. The returned slice has len == 0 so the
// caller can append or reslice as needed; for the deserializer use case the
// caller (DeserializeFromReaderWithAllocator) reslices to [:numLeaves].
//
// Requests larger than the largest pooled class allocate a fresh slice with
// make so we never grow the pool to pathological sizes; such slices are
// discarded on Put (PutNodeSlice classifies by exact cap).
func GetNodeSlice(want int) []subtreepkg.Node {
	if want <= 0 {
		return nil
	}

	idx := classIndexForWant(want)
	if idx < 0 {
		// Larger than the largest class — caller-owned, never pooled.
		return make([]subtreepkg.Node, 0, want)
	}

	if v := nodePools[idx].Get(); v != nil {
		s, _ := v.([]subtreepkg.Node)
		// Defensive: only return pooled slices that actually match the class.
		if cap(s) == nodePoolClasses[idx] {
			return s[:0]
		}
	}

	return make([]subtreepkg.Node, 0, nodePoolClasses[idx])
}

// PutNodeSlice returns a slice to the pool. Slices whose capacity does not
// exactly match a class capacity are discarded — this is what keeps the pool
// from accumulating odd sizes (e.g. >max-class fall-through allocations).
func PutNodeSlice(s []subtreepkg.Node) {
	if s == nil {
		return
	}

	idx := classIndexForCap(cap(s))
	if idx < 0 {
		return
	}

	// Zero only when needed: Node contains a chainhash.Hash and two uint64s,
	// none of which hold pointers, so we skip the wipe. Callers should not
	// rely on returned-and-reused slices being zeroed.
	nodePools[idx].Put(s[:cap(s)]) //nolint:staticcheck // SA6002: storing a slice header is what we want
}

// NodeAllocFromPool is the subtreepkg.NodeAllocator adapter that
// model.Block.GetAndValidateSubtrees passes into the subtree deserializer.
// It is a plain function (not a method) so it can be installed on a Block
// without capturing any state.
func NodeAllocFromPool(numLeaves int) []subtreepkg.Node {
	return GetNodeSlice(numLeaves)
}

// releaseBlockNodes walks a block's SubtreeSlices and returns each pooled
// Nodes backing slice to the pool. Subtrees with no Nodes (already released or
// mmap-backed) are skipped. Safe to call multiple times — ReleaseNodes is
// idempotent. Intended to be invoked from cache eviction and validation
// failure paths.
func releaseBlockNodes(b *model.Block) {
	if b == nil {
		return
	}

	for _, st := range b.SubtreeSlices {
		if st == nil {
			continue
		}

		// mmap-backed subtrees never produce a heap-pooled slice; ReleaseNodes
		// hands back the slice exactly as allocated, and PutNodeSlice's
		// cap-match check discards anything not produced by GetNodeSlice.
		nodes := st.ReleaseNodes()
		if nodes != nil {
			PutNodeSlice(nodes)
		}
	}
}
