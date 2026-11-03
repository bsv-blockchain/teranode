package netsync

import (
	"bytes"
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/model"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/stretchr/testify/require"
)

// A block the dispatcher has taken off the park to validate is in neither the park nor the
// chain until it commits. An inventory check that consults only those two answered "not held"
// for it, so another peer's inv for the same block fetched it again in full: on mainnet from
// 2026-09-28 about 8% of blocks were downloaded and converted twice, and the second copy's park
// entry later found its record deleted by the first copy's commit and logged "parked block could
// not be read back ... NOT_FOUND".
func TestHaveInventory_ABlockBeingValidatedIsHeld(t *testing.T) {
	h := newParkWiringHarness(t, true)

	inFlight := chainhash.Hash{0xd1}

	inv := &wire.InvVect{Type: wire.InvTypeBlock, Hash: inFlight}

	held, err := h.sm.haveInventory(inv)
	require.NoError(t, err)
	require.False(t, held, "precondition: the block is in no park and not in the chain")

	h.sm.dispatcher = newBlockDispatcher(h.sm)
	h.sm.dispatcher.frontier = append(h.sm.dispatcher.frontier, &frontierEntry{hash: inFlight, height: 1})

	held, err = h.sm.haveInventory(inv)
	require.NoError(t, err)
	require.True(t, held, "a block being validated is held; asking for it again downloads it twice")
}

// A second copy that finishes streaming while the first is being validated must not get its own
// park entry. That entry would be dispatched after the first copy commits and deletes the shared
// record, and would only find NOT_FOUND. It is counted as a duplicate and left alone: deleting it
// would take the record the first copy's validation may still be reading.
//
// The record is really on disk before the duplicate arrives. The first version of this test
// never wrote one, so its "not deleted" claim had nothing to check against.
func TestHandleBlockOnDiskMsg_ACopyOfABlockBeingValidatedIsNotParked(t *testing.T) {
	h := newParkWiringHarness(t, true)
	h.sm.drainAsync.Store(true)
	h.sm.parkCommits = make(chan parkCommit, 4)

	parent := chainhash.Hash{0xd2}
	header := wire.BlockHeader{Version: 1, PrevBlock: parent}

	// The record must hash to the key it is stored under or ReadConverted refuses it, so the
	// model header is built from the wire header's own bytes, as streamingBlockGate builds it.
	var hb bytes.Buffer
	require.NoError(t, header.Serialize(&hb))

	modelHeader, err := model.NewBlockHeaderFromBytes(hb.Bytes())
	require.NoError(t, err)

	blk, err := model.NewBlock(modelHeader, coinbaseTx(t), []*chainhash.Hash{{0x50, 0xd2}}, 1, 183, 2, 0)
	require.NoError(t, err)

	hash := *blk.Header.Hash()
	require.Equal(t, header.BlockHash(), hash)
	require.NoError(t, h.sm.blockPark.WriteConvertedBlock(context.Background(), hash, blk))

	body := peerpkg.BlockBody{Header: header, TxCount: 1, Size: 183, Hash: hash, Converted: true}

	h.sm.dispatcher = newBlockDispatcher(h.sm)
	h.sm.dispatcher.frontier = append(h.sm.dispatcher.frontier,
		&frontierEntry{hash: parent, height: 1}, &frontierEntry{hash: hash, height: 2})

	before := h.sm.waste.dupConverted.Load()

	h.sm.handleBlockOnDiskMsg(&blockOnDiskMsg{body: body, peer: h.peer})

	require.False(t, h.sm.blockPark.Has(hash), "the copy being validated is the only one the park may hand out")
	require.Equal(t, before+1, h.sm.waste.dupConverted.Load(), "a copy converted for a block already in flight is wasted work and is counted")
	require.Empty(t, h.sm.parkCommits)

	got, err := h.sm.blockPark.ReadConverted(context.Background(), hash)
	require.NoError(t, err, "the record the first copy's validation may still be reading must not be deleted")
	require.Equal(t, hash, *got.Header.Hash())
}
