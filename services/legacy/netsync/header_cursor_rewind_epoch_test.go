package netsync

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestSyncManager_ARewindOntoARebuiltHeaderListDoesNotReRequestAStoredBlock is
// about what a rewind may assume.
//
// The rewind puts a dropped block's header node back on the front of the header
// list, on the strength of one rule: headers only ever leave the list from the
// front, so a node that was the front when it went has nothing that belongs in
// front of it. That rule holds only while the list is the list the node left.
//
// A sync-peer rotation throws the list away and starts a new one whose first
// entry is the opposite of a block still wanted: it is the best block already in
// the database, kept only so the next round of headers can prove it links, and
// removed on the strength of being exactly that. A block parked before the
// rotation still carries its old header node, and when its time runs out the
// sweep hands that node back — into a list it never belonged to.
//
// The end state this asserts is the one an operator would notice: after the
// rotation the node must never ask a peer for the block it already has.
func TestSyncManager_ARewindOntoARebuiltHeaderListDoesNotReRequestAStoredBlock(t *testing.T) {
	h := newParkWiringHarness(t, true)

	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)

	// A block that was the FRONT of the header list parks, so its header node is
	// removed and unindexed and travels with the parked entry.
	h.parkFrontBlock(t)

	// The sync peer stalls and is rotated. The header list is rebuilt around the
	// best stored block, and startSync turns headers-first mode back on.
	stored := chainhash.Hash{0xa9}

	h.sm.resetHeaderState(&stored, 100)
	h.sm.headersFirstMode.Store(true)

	// The next round of headers, linking off the stored block, up to a
	// checkpoint three headers along.
	var nonce uint32

	msg, hashes := linkedHeaders(stored, 3, &nonce)
	h.sm.nextCheckpoint = &chaincfg.Checkpoint{Height: 103, Hash: &hashes[2]}

	getDataBefore := h.rec.getDataCount()

	// The parked block's time runs out. The sweep gives it up and rewinds the
	// walk onto it, using the header node from the list that no longer exists.
	h.sm.sweepParkedBlocks(time.Now().Add(parkEntryTTL + time.Second))

	// The headers land, reach the checkpoint, and the walk goes out for blocks.
	h.sm.handleHeadersMsg(&headersMsg{headers: msg, peer: h.peer})
	h.sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return h.rec.askedForSince(getDataBefore, hashes[0]) }, 5*time.Second),
		"the fresh round of headers must be fetched")

	require.False(t, h.rec.askedForSince(getDataBefore, stored),
		"the block the header list is anchored on is already in the database; a stale rewind must not make the node download it again")
}

// blocksAskedFor is every block hash this peer has been sent a getdata for, in
// the order they went out.
func (r *peerMsgRecorder) blocksAskedFor() []chainhash.Hash {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]chainhash.Hash(nil), r.getData...)
}

// TestSyncManager_TwoGivenUpBlocksAreAskedForInTheOrderTheyAreNeeded pins the
// other half of the same rule.
//
// Blocks are committed strictly in order, so the download walk has to ask for
// them in order. Each rewind on its own puts a header back on the front, which
// is right for one block and wrong for two: the second rewind would put the
// higher block in front of the lower one, and the walk would then ask for the
// child before the parent — and the child arrives as an orphan of a block
// nobody has asked for yet.
//
// The order the two blocks are given up in is the whole test, and the sweep does
// not choose it: Expire ranges over a map, so one run in two took the order that
// hides the bug and the test only caught its mutation about ten times in twelve.
// So the expiry is driven one block at a time here, parent first, which is the
// order a naive rewind gets wrong — with a plain push-to-the-front the child
// then goes in AHEAD of the parent, and because the cursor only ever moves
// backwards it is left on the parent, so the walk from there never reaches the
// child at all.
func TestSyncManager_TwoGivenUpBlocksAreAskedForInTheOrderTheyAreNeeded(t *testing.T) {
	h := newParkWiringHarness(t, true)

	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)

	// The first block parks as the front; once its header is gone the second one
	// is the front, and it parks too. Both carry a header node home with them.
	first := h.blocks[0].MsgBlock().BlockHash()
	second := h.blocks[1].MsgBlock().BlockHash()

	require.NoError(t, h.deliver(t, 0))
	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 2, h.sm.blockPark.Len(), "both blocks arrived before their parents")

	getDataBefore := h.rec.getDataCount()

	// Neither parent ever turns up, so both are given up on and both rewind —
	// the parent on this tick and the child on the next. Holding the child's
	// clock at the sweep time is what keeps it: an entry expires on how long it
	// has been parked, so a child parked "now" is not yet old enough.
	parentGivenUpAt := time.Now().Add(parkEntryTTL + time.Second)

	h.sm.blockPark.mu.Lock()
	h.sm.blockPark.entries[second].parkedAt = parentGivenUpAt
	h.sm.blockPark.mu.Unlock()

	h.sm.sweepParkedBlocks(parentGivenUpAt)

	require.Equal(t, 1, h.sm.blockPark.Len(), "only the parent may have been given up on so far")

	h.sm.sweepParkedBlocks(parentGivenUpAt.Add(parkEntryTTL + time.Second))

	require.Zero(t, h.sm.blockPark.Len(), "the child must have been given up on too")

	h.sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool {
		return h.rec.askedForSince(getDataBefore, first) && h.rec.askedForSince(getDataBefore, second)
	}, 5*time.Second), "both blocks given up on must be asked for again")

	asked := h.rec.blocksAskedFor()[getDataBefore:]

	firstAt, secondAt := -1, -1

	for i, got := range asked {
		if got.IsEqual(&first) && firstAt < 0 {
			firstAt = i
		}

		if got.IsEqual(&second) && secondAt < 0 {
			secondAt = i
		}
	}

	require.Less(t, firstAt, secondAt,
		"the parent must be asked for before the child, or the child comes back as an orphan of a block nobody has asked for")
}

// TestSyncManager_PastTheFinalCheckpointAGivenUpBlockIsNotRewound pins the
// other limit on the rewind, and it is what makes the park switch honest about
// what it does and does not change.
//
// The rewind exists for one reason: inside headers-first mode the header list is
// the only thing that fetches blocks, and the getblocks a drop path sends is
// thrown away by processInvMsg. Past the final checkpoint — every mainnet node
// today — that is the other way round: the getblocks is the whole of the
// recovery and the header list drives nothing. Rewinding there builds a header
// list out of a block that was just given up on, and fetchHeaderBlocks has no
// headers-first guard of its own, so it would go on to issue a getdata from a
// list that exists only because of the rewind.
func TestSyncManager_PastTheFinalCheckpointAGivenUpBlockIsNotRewound(t *testing.T) {
	h := newParkWiringHarness(t, true)

	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)

	// The block parks while headers-first sync is still on, so it carries the
	// header its arrival took off the front.
	front := h.parkFrontBlock(t)

	// The final checkpoint is passed and the node goes back to asking for blocks
	// by inventory.
	h.sm.headersFirstMode.Store(false)

	// The parked block's time runs out and it is given up on.
	h.sm.sweepParkedBlocks(time.Now().Add(parkEntryTTL + time.Second))

	// A walk, synchronously: a block is recorded in the download ledger before
	// its getdata is queued.
	h.sm.fetchHeaderBlocks()

	require.False(t, h.sm.blockDownloads.RequestedWithin(front, time.Minute),
		"past the final checkpoint the header walk drives nothing, so a given-up block must not be fetched from it")
}
