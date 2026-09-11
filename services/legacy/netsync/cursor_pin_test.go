package netsync

import (
	"container/list"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// TestFetchHeaderBlocks_CursorStopsAtTheFirstBlockWeLack is the fix for holes that
// never close.
//
// The download walk kept one pointer for two jobs: where this pass has reached,
// and where the next pass begins. Stepping over a block somebody was asked for and
// has not delivered wrote that skip into the pointer, so the next pass started
// ABOVE the gap and nothing ever asked for the block again. The function's own
// comment lists the only four events that could put the cursor back in front of
// it, and one of them, a notfound reply, cannot fire for a block at all.
//
// SV Node keeps the two jobs apart. Its walk runs ahead and steps over in-flight
// blocks exactly as this one does, while pindexLastCommonBlock advances only past
// blocks it has data for and stops at the first it does not. That is the whole
// difference, and it is why SV Node does not accumulate holes.
//
// Measured on mainnet on 2026-09-10: fifteen distinct holes open at once beneath
// 115 parked blocks, the node idle with 4.9 GB of committable work on disk.
func TestFetchHeaderBlocks_CursorStopsAtTheFirstBlockWeLack(t *testing.T) {
	// Three headers in a row. The middle one has been asked for and has not
	// arrived, which is exactly the case that used to make a permanent hole.
	seed := func(t *testing.T) (*SyncManager, []chainhash.Hash, []*list.Element) {
		t.Helper()

		// The fuller harness, because the walk asks the blockchain and the park
		// whether each header is already held, and a bare manager has neither.
		h := newParkWiringHarness(t, true)
		sm := h.sm
		sm.blockSizeTracker = newBlockSizeTracker(10)

		sm.headerMu.Lock()
		sm.headerList = list.New()
		sm.headerIndex = map[chainhash.Hash]*list.Element{}
		sm.headerMu.Unlock()

		hashes := []chainhash.Hash{{0xa1}, {0xa2}, {0xa3}}
		elems := make([]*list.Element, 0, len(hashes))

		for i := range hashes {
			h := hashes[i]
			e := sm.headerList.PushBack(&headerNode{height: int32(100 + i), hash: &h})
			sm.indexHeaderLocked(e, h)
			elems = append(elems, e)
		}

		sm.startHeader = elems[0]

		return sm, hashes, elems
	}

	t.Run("the cursor stays on a block nobody has delivered", func(t *testing.T) {
		sm, hashes, elems := seed(t)

		// Somebody was asked for the FIRST header recently and it has not come.
		peer, _, _ := connectRacePeer(t, 91, 1000)
		registerRacePeer(sm, peer)
		require.True(t, sm.blockDownloads.Add(peer, hashes[0]))

		sm.fetchHeaderBlocks()

		require.Equal(t, elems[0], sm.startHeader,
			"the next pass must begin at the block we still lack, or nothing ever asks for it again")
	})

	t.Run("the pass still runs ahead past the gap", func(t *testing.T) {
		sm, hashes, _ := seed(t)

		peer, _, _ := connectRacePeer(t, 92, 1000)
		registerRacePeer(sm, peer)
		require.True(t, sm.blockDownloads.Add(peer, hashes[0]))

		sm.fetchHeaderBlocks()

		// Pinning the cursor must not stop the pass requesting higher blocks;
		// that would trade a hole for a stall and lose the fan-out entirely.
		require.True(t, sm.blockDownloads.RequestedWithin(hashes[1], blockRequestRetryInterval) ||
			sm.blockDownloads.RequestedWithin(hashes[2], blockRequestRetryInterval),
			"the walk must keep asking for blocks above the gap")
	})

	t.Run("with nothing outstanding the cursor advances as before", func(t *testing.T) {
		sm, _, elems := seed(t)

		peer, _, _ := connectRacePeer(t, 93, 1000)
		registerRacePeer(sm, peer)

		sm.fetchHeaderBlocks()

		require.NotEqual(t, elems[0], sm.startHeader,
			"an ordinary forward walk must still move the cursor, or every pass would redo the same work")
	})
}
