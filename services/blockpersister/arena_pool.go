package blockpersister

import (
	"sync"

	"github.com/bsv-blockchain/go-bt/v2"
)

const (
	blockpersisterArenaInitialCap = 2 << 20  // 2 MiB
	blockpersisterArenaShrinkCap  = 64 << 20 // 64 MiB
)

// blockpersisterArenaPool holds bt.Arena instances reused across subtree
// UTXO streaming. The contract: callers Get an arena before the decode
// loop, Put it back when the loop returns. Put runs ResetAndShrink so a
// one-off oversized decode doesn't bloat the pool's idle footprint.
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
