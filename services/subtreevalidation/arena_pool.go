package subtreevalidation

import (
	"sync"
	"sync/atomic"

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

// arenaGets / arenaPuts count lifetime Get/Put calls on the arena pool. They
// exist to catch arena leaks: every getSubtreeArena must be balanced by a
// putSubtreeArena. The CheckBlockSubtrees batch pipeline loads batches ahead
// of processing, so a load-ahead batch whose processing never runs (e.g. an
// abort mid-block) must still release its arenas — these counters let tests
// assert that balance, and are cheap enough (one atomic add per subtree decode,
// not per tx) to leave on in production.
var (
	arenaGets atomic.Int64
	arenaPuts atomic.Int64
)

// getSubtreeArena returns an arena ready for use. The arena's cursor is at
// zero on entry.
func getSubtreeArena() *bt.Arena {
	arenaGets.Add(1)
	return subtreeArenaPool.Get().(*bt.Arena)
}

// putSubtreeArena returns the arena to the pool after Reset+Shrink. The
// caller must release all *bt.Tx pointers obtained from arena-backed
// decode calls before invoking putSubtreeArena — script bytes will be
// reused or freed by subsequent pool consumers.
func putSubtreeArena(a *bt.Arena) {
	arenaPuts.Add(1)
	a.ResetAndShrink(subtreeArenaShrinkCap)
	subtreeArenaPool.Put(a)
}
