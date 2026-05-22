package subtreevalidation

import (
	"sync"

	"github.com/bsv-blockchain/go-bt/v2"
)

const (
	subtreeArenaInitialCap = 2 << 20  // 2 MiB
	subtreeArenaShrinkCap  = 64 << 20 // 64 MiB
)

// subtreeArenaPool holds bt.Arena instances reused across subtree decode
// operations. The contract: callers Get an arena before decoding a subtree,
// Put it back when the decoded txs are fully consumed (validation +
// metadata emission complete). Put runs ResetAndShrink(subtreeArenaShrinkCap)
// so a one-off oversized decode doesn't bloat the pool's idle footprint.
var subtreeArenaPool = sync.Pool{
	New: func() any { return bt.NewArena(subtreeArenaInitialCap) },
}

// getSubtreeArena returns an arena ready for use. The arena's cursor is at
// zero on entry.
func getSubtreeArena() *bt.Arena {
	return subtreeArenaPool.Get().(*bt.Arena)
}

// putSubtreeArena returns the arena to the pool after Reset+Shrink. The
// caller must release all *bt.Tx pointers obtained from arena-backed
// decode calls before invoking putSubtreeArena — script bytes will be
// reused or freed by subsequent pool consumers.
func putSubtreeArena(a *bt.Arena) {
	a.ResetAndShrink(subtreeArenaShrinkCap)
	subtreeArenaPool.Put(a)
}
