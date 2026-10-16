package netsync

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/stretchr/testify/require"
)

// newReAskHarness is the park harness above the last checkpoint, where a dropped record is
// asked for again by the disposition itself.
func newReAskHarness(t *testing.T) (*parkWiringHarness, chainhash.Hash) {
	t.Helper()

	h := newParkWiringHarnessInState(t, true, blockchain2.FSMStateRUNNING, withTransactions(1))
	h.sm.headersFirstMode.Store(false)

	return h, h.blocks[1].MsgBlock().BlockHash()
}

// External review of e4698b219, P2. The re-ask for a dropped record was a getblocks from the
// best block with a zero stop hash. The getblocks that fetched the block in the first place
// had the same locator and stop hash whenever the tip had not moved since, and
// PushGetBlocksMsg drops a repeat of the last (locator[0], stop) pair, so the re-ask never left
// the node. Every row that re-asks is checked: the block must actually be asked for.
func TestApplyParkDisposition_AReAskIsSentEvenWhenTheTipHasNotMoved(t *testing.T) {
	for _, d := range parkDispositionRows {
		if !d.reAsk {
			continue
		}

		t.Run(d.name, func(t *testing.T) {
			h, child := newReAskHarness(t)

			// The getblocks that announced the block, at the tip the re-ask will see.
			h.sm.requestMissingBlocks(h.peer, child)
			require.True(t, WaitUntil(func() bool { return h.rec.getBlocksCount() == 1 }, 5*time.Second))

			before := h.rec.getDataCount()

			h.sm.applyParkDisposition(parkedBlock{hash: child, peer: h.peer}, d)

			require.True(t, WaitUntil(func() bool { return h.rec.askedForSince(before, child) }, 5*time.Second),
				"the dropped block must be asked for again with the tip unmoved")
			require.True(t, h.sm.blockDownloads.HasOwner(h.peer, child), "the re-ask is recorded, so the body passes the gate when it comes")
		})
	}
}

// Suppression of a real duplicate stays. A dropped block that another peer has been asked for
// within the retry window, or that is already arriving, converting or parked again, is not
// asked for a second time.
func TestApplyParkDisposition_AReAskIsNotADuplicateRequest(t *testing.T) {
	for _, tc := range []struct {
		name string
		arm  func(t *testing.T, h *parkWiringHarness, child chainhash.Hash)
	}{
		{
			name: "requested from another peer",
			arm: func(t *testing.T, h *parkWiringHarness, child chainhash.Hash) {
				other, _, _ := connectRecordingPeer(t, 72, 1000)
				registerRacePeer(h.sm, other)
				require.True(t, h.sm.blockDownloads.Add(other, child))
			},
		},
		{
			name: "converting",
			arm: func(_ *testing.T, h *parkWiringHarness, child chainhash.Hash) {
				h.sm.inFlightBlocksMu.Lock()
				if h.sm.inFlightBlocks == nil {
					h.sm.inFlightBlocks = make(map[chainhash.Hash]*inFlightBlock)
				}

				h.sm.inFlightBlocks[child] = &inFlightBlock{}
				h.sm.inFlightBlocksMu.Unlock()
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, child := newReAskHarness(t)

			tc.arm(t, h, child)

			before := h.rec.getDataCount()

			h.sm.applyParkDisposition(parkedBlock{hash: child, peer: h.peer}, parkDispositionFilesGone)

			require.False(t, WaitUntil(func() bool { return h.rec.askedForSince(before, child) }, 300*time.Millisecond),
				"a block already on its way must not be asked for again")
		})
	}
}

// The ordinary duplicate filter on getblocks is untouched: two requests at the same tip send one.
func TestRequestMissingBlocks_ARepeatAtTheSameTipIsStillSuppressed(t *testing.T) {
	h, child := newReAskHarness(t)

	h.sm.requestMissingBlocks(h.peer, child)
	h.sm.requestMissingBlocks(h.peer, child)

	require.True(t, WaitUntil(func() bool { return h.rec.getBlocksCount() >= 1 }, 5*time.Second))
	require.False(t, WaitUntil(func() bool { return h.rec.getBlocksCount() > 1 }, 300*time.Millisecond),
		"a repeat getblocks at an unmoved tip is still filtered")
}
