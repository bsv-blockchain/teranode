package blockpersister

import (
	"sync"

	"github.com/bsv-blockchain/go-bt/v2"
)

const (
	blockpersisterArenaInitialCap = 2 << 20  // 2 MiB
	blockpersisterArenaShrinkCap  = 64 << 20 // 64 MiB
)

// blockpersisterArenaPool holds bt.Arena instances reused by the subtree
// UTXO streaming loop and by existing-subtreeData validation, which phase 1
// runs for several subtrees at once, each with its own arena. The contract:
// callers Get an arena before the decode loop, Put it back when the loop
// returns. Put runs ResetAndShrink so a one-off oversized decode doesn't
// bloat the pool's idle footprint. That footprint still scales with phase 1's
// concurrency: each arena keeps a slab of up to blockpersisterArenaShrinkCap, so
// at blockpersister_concurrency 8 the pool can hold about 512 MiB of idle slabs
// after checking subtrees with 32-64 MiB transactions, until two GC cycles
// drain it.
var blockpersisterArenaPool = sync.Pool{
	New: func() any { return bt.NewArena(blockpersisterArenaInitialCap) },
}

func getBlockpersisterArena() *bt.Arena {
	return blockpersisterArenaPool.Get().(*bt.Arena)
}

func putBlockpersisterArena(a *bt.Arena) {
	a.ResetAndShrink(blockpersisterArenaShrinkCap)
	blockpersisterArenaPool.Put(a)
}
