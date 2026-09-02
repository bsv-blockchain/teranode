package netsync

import (
	"testing"
	"time"

	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestSyncManager_AParkedBlockIsNotDownloadedAllOverAgain is what makes the park
// worth having outside headers-first mode, which is where every mainnet node
// lives.
//
// Parking a block does not end the conversation with the peer: the same drop
// path also sends a getblocks, because in the legacy protocol that is the only
// thing that fetches the missing parent and the only thing that makes the peer
// send anything more. The peer answers with an inv covering the gap — including
// the block we are already holding. Nothing in the inventory path knew that, so
// the block was requested and downloaded again, and again, for as long as it
// stayed parked: the park saved the decode and none of the bandwidth.
//
// The end state asserted here is that the node does not ask for a block it is
// already holding.
func TestSyncManager_AParkedBlockIsNotDownloadedAllOverAgain(t *testing.T) {
	h := newParkWiringHarness(t, true)

	// Past the final checkpoint: headers-first mode is over and the inventory
	// path is what fetches blocks.
	h.sm.headersFirstMode.Store(false)

	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)

	// The queue handleInvMsg drains into a getdata, which the real constructor
	// builds and this struct-literal harness does not.
	state, ok := h.sm.peerStates.Get(h.peer)
	require.True(t, ok)

	state.requestQueue = txmap.NewSyncedSlice[wire.InvVect](maxRequestedBlocks)

	child := h.blocks[1].MsgBlock().BlockHash()

	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 1, h.sm.blockPark.Len(), "the block is on disk, waiting for its parent")
	require.False(t, h.sm.blockDownloads.RequestedWithin(child, time.Minute),
		"nothing is outstanding for it: it has already been delivered")

	// The peer answers the park's getblocks with an inv that covers the gap, and
	// the gap includes the block we are holding.
	inv := wire.NewMsgInv()
	require.NoError(t, inv.AddInvVect(wire.NewInvVect(wire.InvTypeBlock, &child)))

	h.sm.handleInvMsg(&invMsg{inv: inv, peer: h.peer})

	// A block is recorded in the download ledger before its getdata is queued,
	// so the ledger says what this inv asked for without waiting on the wire.
	require.False(t, h.sm.blockDownloads.RequestedWithin(child, time.Minute),
		"a block we are already holding on disk must not be downloaded again")
}
