package netsync

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestHandleBlockOnDiskMsg covers the consumer's side of the streaming path.
//
// A streamed block's bytes reach the park's store before the park has any record
// of them, so this is what gives it one and then asks for a drain. It runs on the
// consumer goroutine for the same reason every other park and drain decision
// does: the drain queue is that goroutine's own state, and five separate stalls
// this week came from touching this machinery from somewhere else.
func TestHandleBlockOnDiskMsg(t *testing.T) {
	bodyFor := func(prev chainhash.Hash, size int64) peerpkg.BlockBody {
		header := wire.BlockHeader{Version: 1, PrevBlock: prev}

		return peerpkg.BlockBody{
			Header:  header,
			TxCount: 3,
			Size:    size,
			Hash:    header.BlockHash(),
		}
	}

	// drainAsync is what the consumer loop sets while it is running, and it is
	// the production setting. Left false, scheduleDrain walks the drain stack
	// synchronously instead of queueing, which is correct for a manager with no
	// consumer but exercises the whole drain rather than this hand-off.
	withConsumer := func(t *testing.T) *parkWiringHarness {
		t.Helper()

		h := newParkWiringHarness(t, true)
		h.sm.drainAsync.Store(true)

		return h
	}

	t.Run("the block is adopted and a drain is asked for", func(t *testing.T) {
		h := withConsumer(t)
		h.sm.parkCommits = make(chan parkCommit, 4)

		// A parent the harness has in its header list, because an invented one is
		// now correctly refused as unreachable.
		parent := h.blocks[1].MsgBlock().BlockHash()
		body := bodyFor(parent, 1<<20)

		h.sm.handleBlockOnDiskMsg(&blockOnDiskMsg{body: body, peer: h.peer})

		require.True(t, h.sm.blockPark.Has(body.Hash),
			"the bytes are already on disk, so the park must know about them or nothing will ever read them")
		require.Equal(t, int64(1<<20), h.sm.blockPark.Bytes(),
			"an adopted block must be charged, or the park's accounting drifts from its disk")

		// The drain request goes to the consumer through parkCommits rather than
		// onto drainQueue directly: this handler runs on blockHandler and the
		// queue belongs to the dispatchBlocks consumer with no lock. Queueing it
		// here raced that queue and could not wake a sleeping consumer.
		require.Empty(t, h.sm.drainQueue,
			"this goroutine must not touch the consumer's queue")

		// The harness answers not-found for every header, so this parent is NOT in
		// the chain and no drain should be asked for: the block is worth keeping
		// but is not committable yet, and its parent's own commit will schedule
		// the drain when it lands.
		require.Empty(t, h.sm.parkCommits,
			"asking for a drain on an uncommitted parent sends the consumer after a block that cannot commit, and each attempt costs a store lookup on the goroutine that commits blocks")
	})

	t.Run("the delivering peer is recorded", func(t *testing.T) {
		h := withConsumer(t)
		h.sm.parkCommits = make(chan parkCommit, 4)

		body := bodyFor(h.blocks[1].MsgBlock().BlockHash(), 4096)
		h.sm.handleBlockOnDiskMsg(&blockOnDiskMsg{body: body, peer: h.peer})

		entry, ok := h.sm.blockPark.Take(body.Hash)
		require.True(t, ok)
		require.Equal(t, h.peer, entry.peer,
			"post-commit actions ask the delivering peer for more, so losing it costs a peer's worth of progress")
	})

	t.Run("a re-delivered body is not adopted twice", func(t *testing.T) {
		h := withConsumer(t)
		h.sm.parkCommits = make(chan parkCommit, 4)

		body := bodyFor(h.blocks[1].MsgBlock().BlockHash(), 2048)

		h.sm.handleBlockOnDiskMsg(&blockOnDiskMsg{body: body, peer: h.peer})
		h.sm.handleBlockOnDiskMsg(&blockOnDiskMsg{body: body, peer: h.peer})

		require.Equal(t, 1, h.sm.blockPark.Len())
		require.Equal(t, int64(2048), h.sm.blockPark.Bytes(),
			"charging a duplicate twice would have the park believe it holds bytes it does not")
	})

	t.Run("a park that refuses the entry has the orphaned body deleted", func(t *testing.T) {
		h := withConsumer(t)
		h.sm.parkCommits = make(chan parkCommit, 4)

		// Fill to the ceiling so adoption is refused for a reason other than
		// already holding it.
		h.sm.blockPark.mu.Lock()
		for i := 0; i < maxParkedEntries; i++ {
			var hash chainhash.Hash
			hash[0] = byte(i)
			hash[1] = byte(i >> 8)
			hash[2] = 0xfe
			stored := parkedBlock{hash: hash}
			h.sm.blockPark.entries[hash] = &stored
		}
		h.sm.blockPark.mu.Unlock()

		body := bodyFor(h.blocks[1].MsgBlock().BlockHash(), 512)
		h.sm.handleBlockOnDiskMsg(&blockOnDiskMsg{body: body, peer: h.peer})

		require.False(t, h.sm.blockPark.Has(body.Hash))
		require.Empty(t, h.sm.drainQueue,
			"nothing was adopted, so asking for a drain would send the consumer looking for work that is not there")
	})

	t.Run("no park means the message is dropped rather than panicking", func(t *testing.T) {
		h := newParkWiringHarness(t, true)
		h.sm.blockPark = nil

		require.NotPanics(t, func() {
			h.sm.handleBlockOnDiskMsg(&blockOnDiskMsg{body: bodyFor(h.blocks[1].MsgBlock().BlockHash(), 8), peer: h.peer})
		}, "a nil park is how every other call site reads the park being switched off")
	})
}

// TestHandleBlockOnDiskMsg_RefusesAnUnreachableParent is the regression test for
// a fault this path caused on mainnet on 2026-09-10.
//
// Three streamed blocks whose parents were never in the header list produced
// 23,111 "the parent is missing again" retries in two hours, against zero on each
// of the three preceding days. Each retry is a store lookup on the goroutine that
// commits blocks, so an unreachable block competes with the work the operator is
// waiting for rather than merely sitting there.
//
// The decoded path never had to ask this, because a block only reaches its park
// call after the header list has been walked for it. Streaming skips that walk by
// design, so the question moves here.
func TestHandleBlockOnDiskMsg_RefusesAnUnreachableParent(t *testing.T) {
	bodyFor := func(prev chainhash.Hash) peerpkg.BlockBody {
		header := wire.BlockHeader{Version: 1, PrevBlock: prev}

		return peerpkg.BlockBody{Header: header, TxCount: 1, Size: 4096, Hash: header.BlockHash()}
	}

	t.Run("a parent in neither the chain nor the header list is refused", func(t *testing.T) {
		h := newParkWiringHarness(t, true)
		h.sm.drainAsync.Store(true)

		// The harness answers "no such block" for every header lookup, and the
		// header list holds only the harness's own three blocks, so this parent
		// is in neither.
		body := bodyFor(chainhash.Hash{0x9e})

		h.sm.handleBlockOnDiskMsg(&blockOnDiskMsg{body: body, peer: h.peer})

		require.False(t, h.sm.blockPark.Has(body.Hash),
			"an unreachable block must not be held, or the drain offers it to the chain forever")
		require.Empty(t, h.sm.parkCommits,
			"and nothing should be handed to the consumer on its behalf")
	})

	t.Run("a parent still ahead of us in the header list is accepted", func(t *testing.T) {
		h := newParkWiringHarness(t, true)
		h.sm.drainAsync.Store(true)

		// The harness seeds the header list with its three blocks, so any of
		// them is a parent we have asked for and will get.
		parent := h.blocks[1].MsgBlock().BlockHash()
		body := bodyFor(parent)

		h.sm.handleBlockOnDiskMsg(&blockOnDiskMsg{body: body, peer: h.peer})

		require.True(t, h.sm.blockPark.Has(body.Hash),
			"a block waiting on a parent we asked for is exactly what the park is for")
	})

	t.Run("a store fault reads as reachable rather than throwing the download away", func(t *testing.T) {
		h := newParkWiringHarness(t, true)
		h.sm.drainAsync.Store(true)

		h.noSuchBlock.Unset()
		h.client.On("GetBlockHeader", mock.Anything, mock.Anything).
			Return(nil, nil, errors.NewStorageError("the store is not answering"))

		body := bodyFor(chainhash.Hash{0x9f})

		h.sm.handleBlockOnDiskMsg(&blockOnDiskMsg{body: body, peer: h.peer})

		require.True(t, h.sm.blockPark.Has(body.Hash),
			"our own storage being briefly unwell says nothing about the block, and discarding it would pay for the download twice")
	})
}

// TestParentIsReachable_ParkedParentCounts is the regression test for a fault
// this function caused on the day it shipped.
//
// A parked parent is a block already on disk waiting for its own parent, so a
// child of it is exactly as reachable as a child of a committed block. Leaving the
// park out of the test meant discarding bodies whose parents were sitting a few
// inches away, which manufactured the very holes the node was stalling on.
//
// Measured on mainnet on 2026-09-10: sixteen of seventeen holes had their parent
// in the park at that moment, no discard appears in four days of log before this
// deployed, and it looped with the frontier race, which re-requested each missing
// block only to have the arriving copy discarded again. Fourteen of the 28.7 GB
// streamed in the log went round that loop.
func TestParentIsReachable_ParkedParentCounts(t *testing.T) {
	t.Run("a parent in the park is reachable", func(t *testing.T) {
		h := newParkWiringHarness(t, true)

		parent := parkedBlock{hash: chainhash.Hash{0xa1}, prevBlock: chainhash.Hash{0xa0}, size: 512}
		require.True(t, h.sm.blockPark.AdoptWritten(parent))

		require.True(t, h.sm.parentIsReachable(parent.hash),
			"the parent is on this node's own disk; discarding its child manufactures the hole the node then stalls on")
	})

	t.Run("a parent nowhere at all is still unreachable", func(t *testing.T) {
		h := newParkWiringHarness(t, true)

		require.False(t, h.sm.parentIsReachable(chainhash.Hash{0x9e}),
			"the gate must still refuse a body whose parent is in neither the park, the header list nor the chain")
	})
}

// TestHandleBlockOnDiskMsg_DrainsOnlyWhenTheParentIsCommitted separates the two
// questions this path has to ask, which were briefly answered by one predicate.
//
// Whether the body is worth keeping is the looser question, and a parked parent
// counts. Whether a drain is worth asking for is the stricter one, and only a
// committed parent counts. Conflating them sent the consumer after blocks that
// could not commit: measured on mainnet on 2026-09-10, five blocks streamed and
// six parent-missing failures inside one 45-second window with nothing committing.
func TestHandleBlockOnDiskMsg_DrainsOnlyWhenTheParentIsCommitted(t *testing.T) {
	header := wire.BlockHeader{Version: 1, PrevBlock: chainhash.Hash{0xc1}}
	body := peerpkg.BlockBody{Header: header, TxCount: 1, Size: 2048, Hash: header.BlockHash()}

	t.Run("a committed parent asks for a drain", func(t *testing.T) {
		h := newParkWiringHarness(t, true)
		h.sm.drainAsync.Store(true)
		h.sm.parkCommits = make(chan parkCommit, 4)

		h.chainHolds(t, header.PrevBlock)

		h.sm.handleBlockOnDiskMsg(&blockOnDiskMsg{body: body, peer: h.peer})

		require.Len(t, h.sm.parkCommits, 1,
			"the parent is committed, so this block can be committed now and the consumer must be told")
	})

	t.Run("an uncommitted parent is kept but not drained", func(t *testing.T) {
		h := newParkWiringHarness(t, true)
		h.sm.drainAsync.Store(true)
		h.sm.parkCommits = make(chan parkCommit, 4)

		// A parent that is parked rather than committed: worth keeping the child,
		// not worth a drain.
		parent := parkedBlock{hash: header.PrevBlock, prevBlock: chainhash.Hash{0xc0}, size: 64}
		require.True(t, h.sm.blockPark.AdoptWritten(parent))

		h.sm.handleBlockOnDiskMsg(&blockOnDiskMsg{body: body, peer: h.peer})

		require.True(t, h.sm.blockPark.Has(body.Hash),
			"a parked parent still makes the body worth keeping")
		require.Empty(t, h.sm.parkCommits,
			"but it is not committable yet, and its parent's own commit will schedule the drain")
	})
}
