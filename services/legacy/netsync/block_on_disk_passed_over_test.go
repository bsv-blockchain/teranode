package netsync

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/stretchr/testify/require"
)

// A block the peer delivers is discharged through Delivered, so a block asked of the same peer
// earlier and not sent reads as passed over. Through RemoveOwner the ledger forgot where in the
// peer's queue the delivered request was, and a withheld block was never seen as one.
func TestADeliveredBlockMarksAnEarlierRequestPassedOver(t *testing.T) {
	h := newParkWiringHarness(t, true)
	h.sm.drainAsync.Store(true)
	h.sm.parkCommits = make(chan parkCommit, 4)

	header := wire.BlockHeader{Version: 1, PrevBlock: h.blocks[1].MsgBlock().BlockHash()}
	body := peerpkg.BlockBody{Header: header, TxCount: 3, Size: 4096, Hash: header.BlockHash(), Converted: true}

	withheld := chainhash.Hash{0x77}

	require.True(t, h.sm.blockDownloads.Add(h.peer, withheld))
	require.True(t, h.sm.blockDownloads.Add(h.peer, body.Hash))

	h.sm.handleBlockOnDiskMsg(&blockOnDiskMsg{body: body, peer: h.peer})

	require.False(t, h.sm.blockDownloads.HasOwner(h.peer, body.Hash), "the delivered block is discharged")
	require.True(t, h.sm.blockDownloads.PassedOver(h.peer, withheld), "the block asked before it and not sent was passed over")
}
