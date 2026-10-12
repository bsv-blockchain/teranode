package aerospike

import (
	"sync"

	"github.com/bsv-blockchain/go-bt/v2"
)

const (
	createArenaInitialCap = 1 << 20  // 1 MiB
	createArenaShrinkCap  = 32 << 20 // 32 MiB
)

// createArenaPool holds bt.Arena instances reused across sendStoreBatch calls.
// Contract: get an arena at the top of sendStoreBatch, put it back after
// BatchOperate has returned (all arena-backed bin bytes have been packed into
// the wire buffer). Bins handed to goroutines that outlive sendStoreBatch must
// NOT reference arena memory — see the escape rebuild in sendStoreBatch.
var createArenaPool = sync.Pool{
	New: func() any { return bt.NewArena(createArenaInitialCap) },
}

// getCreateArena returns an arena ready for use. The arena's cursor is at
// zero on entry.
func getCreateArena() *bt.Arena {
	return createArenaPool.Get().(*bt.Arena)
}

// putCreateArena returns the arena to the pool after Reset+Shrink. The
// caller must release all bin slices backed by arena memory before invoking
// putCreateArena — those bytes will be reused or freed by subsequent pool
// consumers.
func putCreateArena(a *bt.Arena) {
	a.ResetAndShrink(createArenaShrinkCap)
	createArenaPool.Put(a)
}
