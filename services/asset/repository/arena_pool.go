package repository

import (
	"sync"

	"github.com/bsv-blockchain/go-bt/v2"
)

const (
	assetArenaInitialCap = 4 << 20  // 4 MiB
	assetArenaShrinkCap  = 64 << 20 // 64 MiB
)

// assetArenaPool holds bt.Arena instances reused across legacy-block
// reconstruction streams. The caller Gets an arena once per block and Puts
// it on goroutine exit; the per-tx arena.Reset between WriteTo calls
// bounds peak memory by the largest single tx.
var assetArenaPool = sync.Pool{
	New: func() any { return bt.NewArena(assetArenaInitialCap) },
}

func getAssetArena() *bt.Arena {
	return assetArenaPool.Get().(*bt.Arena)
}

func putAssetArena(a *bt.Arena) {
	a.ResetAndShrink(assetArenaShrinkCap)
	assetArenaPool.Put(a)
}
