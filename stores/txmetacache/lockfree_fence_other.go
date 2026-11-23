//go:build !amd64 && !arm64

package txmetacache

import "sync/atomic"

var fenceWord atomic.Uint64

// storeFence and loadFence fall back to a sequentially consistent atomic
// read-modify-write, a full barrier on every architecture Go supports.
func storeFence() { fenceWord.Add(0) }

func loadFence() { fenceWord.Add(0) }
