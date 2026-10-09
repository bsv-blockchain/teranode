package netsync

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/stretchr/testify/require"
)

// The real gate over a real connection: a block body whose header nobody did
// the work for costs the sender its connection whatever hash it carries, and a
// block with an honest header that this node never asked for is drained with
// the connection kept (SV Node's rule for an unrequested block). The forged
// header's hash is one the ledger never recorded, so a gate that asked "was
// this requested" first would have drained it and kept the peer.
// TestPeer_AForgedBlockStillDisconnects in the peer package pins the read
// loop's half with a stub gate; this pins the gate's own order.
func TestPeer_AForgedHeaderDisconnectsThroughTheRealGate(t *testing.T) {
	newManager := func(t *testing.T) (*SyncManager, *wire.MsgBlock) {
		t.Helper()

		sm := newPipelineParkManager(t, memory.New(), 8)
		sm.blockDownloads = newBlockDownloadTracker(time.Hour)

		msgBlock, _ := regtestStreamedBlock(t, sm, 4)

		// regtestStreamedBlock records a request to get its header through
		// the gate; nothing is requested from here on.
		sm.blockDownloads = newBlockDownloadTracker(time.Hour)

		return sm, msgBlock
	}

	t.Run("a target easier than the chain limit disconnects", func(t *testing.T) {
		sm, msgBlock := newManager(t)

		// 0x2100ffff is a larger target than regtest's limit 0x207fffff.
		msgBlock.Header.Bits = 0x2100ffff
		require.False(t, sm.blockDownloads.Requested(msgBlock.Header.BlockHash()), "sanity: the forged hash was never asked for")

		sender, receiver, _ := punishmentPeerPair(t, sm, 33)
		sender.QueueMessage(msgBlock, nil)

		require.True(t, disconnectsWithin(receiver, 5*time.Second), "a forged header must cost the sender its connection")
	})

	t.Run("a header that does not meet its own target disconnects", func(t *testing.T) {
		sm, msgBlock := newManager(t)

		// Mainnet's limit, far harder than the regtest work this header carries.
		msgBlock.Header.Bits = 0x1d00ffff
		require.False(t, sm.blockDownloads.Requested(msgBlock.Header.BlockHash()))

		sender, receiver, _ := punishmentPeerPair(t, sm, 34)
		sender.QueueMessage(msgBlock, nil)

		require.True(t, disconnectsWithin(receiver, 5*time.Second), "a header without its work must cost the sender its connection")
	})

	t.Run("an honest block nobody asked for is drained and the peer kept", func(t *testing.T) {
		sm, msgBlock := newManager(t)
		hash := msgBlock.Header.BlockHash()
		require.False(t, sm.blockDownloads.Requested(hash))

		sender, receiver, capture := punishmentPeerPair(t, sm, 35)
		sender.QueueMessage(msgBlock, nil)

		require.False(t, disconnectsWithin(receiver, 2*time.Second), "an unrequested block is not the peer's fault")
		require.True(t, receiver.Connected())

		capture.mu.Lock()
		defer capture.mu.Unlock()

		require.Empty(t, capture.rejected, "nothing was rejected")
		require.Empty(t, capture.rejects, "the sender was sent no reject")

		_, err := sm.blockPark.ReadConverted(t.Context(), hash)
		require.Error(t, err, "nothing of the unrequested block was kept")
	})
}
