package netsync

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestSyncManager_CommittingAParkedBlockMidHeaderRoundDoesNotWedgeTheCheckpoint
// is the same rule as TestSyncManager_ATickBetweenHeaderBatchesDoesNotWedgeTheCheckpointTransition,
// asked of the other way into the same window.
//
// A checkpoint round runs in two halves. While the headers are still coming in,
// batch after batch, the front of the list is the previous round's anchor — a
// block already in this node's chain, kept only so the next header can prove it
// links, and one no peer will ever deliver again. The anchor comes off exactly
// once, taken by the batch that reaches the checkpoint, and only then may blocks
// be asked for. Anything that fetches in the first half wedges the second: the
// blocks arrive, none of them matches the front, none comes off the list, and
// when the anchor is finally trimmed the front becomes a block that has already
// been delivered — which nothing after it matches either. The checkpoint block
// is never recognised as the checkpoint, the next round of headers is never
// asked for, and sync sits until the 180-second stall detector rotates the peer.
//
// The park reaches that window through the sweep, and it is the sweep's designed
// case that takes it there. A node that restarts mid-sync with blocks in the
// park adopts them on the way up; their parents are already in the chain, so no
// commit event ever drains them and the sweep is the only thing that will ever
// look at them. Two minutes later a sweep tick commits one — and a successful
// commit's last act is to top the download pipeline back up. With mainnet
// checkpoint gaps running to 50,000 blocks, about 25 sequential header
// round-trips, a tick lands in the first half of a round routinely.
//
// Blocks are delivered only if they were actually asked for, which is what a
// peer does, so the test cannot heal a walk that asked for a block at the wrong
// moment.
func TestSyncManager_CommittingAParkedBlockMidHeaderRoundDoesNotWedgeTheCheckpoint(t *testing.T) {
	h := newParkWiringHarness(t, true)

	// A block arrives before its parent and is parked. This is the state a
	// restart leaves behind, reached here through the ordinary arrival path.
	child := h.blocks[1].MsgBlock().BlockHash()

	h.client.On("GetBlockExists", mock.Anything, &child).Return(false, nil).Once()

	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 1, h.sm.blockPark.Len(), "the sweep needs something parked to commit")

	// Now the node starts a headers round: an anchor, four headers in the first
	// batch and two more in the second, with the checkpoint on the last of them.
	var nonce uint32

	anchor := chainhash.Hash{0xa7}
	first, firstHashes := linkedHeaders(anchor, 4, &nonce)
	second, secondHashes := linkedHeaders(firstHashes[3], 2, &nonce)

	round := append(append([]chainhash.Hash{}, firstHashes...), secondHashes...)
	checkpoint := round[len(round)-1]

	h.sm.resetHeaderState(&anchor, 10)

	// resetHeaderState now derives the checkpoint from the height it is given,
	// so the synthetic one this test needs is put back after it.
	h.sm.headerMu.Lock()
	h.sm.nextCheckpoint = &chaincfg.Checkpoint{Height: 16, Hash: &checkpoint}
	h.sm.headerMu.Unlock()

	h.sm.headersFirstMode.Store(true)

	// The first batch lands. It does not reach the checkpoint, so the anchor is
	// still the front of the list.
	h.sm.handleHeadersMsg(&headersMsg{headers: first, peer: h.peer})

	h.sm.headerMu.Lock()
	front := h.sm.headerList.Front().Value.(*headerNode)
	h.sm.headerMu.Unlock()

	require.Equal(t, anchor.String(), front.hash.String(),
		"between header batches the front of the list is still the previous round's anchor")

	// Every block this node is asked for is delivered exactly once, in chain
	// order, the way a sync peer answers a getdata.
	delivered := make(map[chainhash.Hash]bool, len(round))
	sawCheckpoint := false

	deliverWhatWasAskedFor := func() int {
		count := 0

		for _, hash := range round {
			if delivered[hash] || !h.sm.blockDownloads.RequestedWithin(hash, time.Minute) {
				continue
			}

			delivered[hash] = true
			count++

			if isCheckpoint, _ := h.sm.advanceHeaderListFor(hash); isCheckpoint {
				sawCheckpoint = true
			}
		}

		return count
	}

	// The sweep fires here, in the middle of the header round. The parent is in
	// the chain, but nothing in this node committed it, so no drain was ever
	// triggered — exactly the state a restart leaves behind.
	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(true, nil)

	h.sm.sweepParkedBlocks(time.Now().Add(parkStuckThreshold + time.Second))

	require.Zero(t, h.sm.blockPark.Len(), "the sweep must have committed the parked block")

	_, failed := h.sm.recentlyFailedBlocks.Get(child)
	require.False(t, failed, "the sweep must have committed the block, not given up on it; a commit is what runs the top-up under test")

	t.Logf("blocks the mid-round commit asked for: %d", deliverWhatWasAskedFor())

	// The second batch reaches the checkpoint, so handleHeadersMsg trims the
	// anchor and starts fetching blocks.
	h.sm.handleHeadersMsg(&headersMsg{headers: second, peer: h.peer})
	deliverWhatWasAskedFor()

	require.True(t, sawCheckpoint,
		"the checkpoint block must still be recognised as the checkpoint, or the round never ends and no further headers are ever asked for")

	h.sm.headerMu.Lock()
	remaining := h.sm.headerList.Len()
	last := h.sm.headerList.Front().Value.(*headerNode)
	h.sm.headerMu.Unlock()

	require.Equal(t, 1, remaining, "every block of the round must have come off the front of the list")
	require.Equal(t, checkpoint.String(), last.hash.String(),
		"what is left is the checkpoint node, kept to anchor the next round")
}
