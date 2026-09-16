package netsync

import "github.com/bsv-blockchain/go-bt/v2/chainhash"

// wantedBlock is one block the node wants next, named by the header cache.
type wantedBlock struct {
	height int32
	hash   chainhash.Hash
}

// wantedBlocksFromCache returns the blocks the node wants next: the contiguous
// run above best that the header cache can name, up to depth blocks.
//
// This is the whole of "what do I want". It is derived from the best block
// processed every time it is asked, so it cannot go stale, cannot be left
// behind and cannot be pointed at the wrong place. The design it replaces kept
// a cursor into a header list, which on 2026-09-12 came to rest above the
// read-ahead limit and left the node holding 955,208 headers while requesting
// nothing.
//
// It takes no lock of its own, because the cache holds its own and nothing
// else is consulted here.
//
// The run stops at the first height the cache cannot name rather than skipping
// it. A block whose parent never arrives cannot commit, so blocks fetched
// beyond a hole only wait in the park, and a park that fills starts refusing
// the very block that would close the hole.
func (sm *SyncManager) wantedBlocksFromCache(best int32, depth int32) []wantedBlock {
	if depth < 1 {
		// A depth of zero asks for nothing for ever, which is a stall wearing a
		// setting's clothes.
		depth = 1
	}

	wanted := make([]wantedBlock, 0, depth)

	for i := int32(1); i <= depth; i++ {
		hash, ok := sm.headerCache.At(best + i)
		if !ok {
			break
		}

		wanted = append(wanted, wantedBlock{height: best + i, hash: hash})
	}

	return wanted
}
