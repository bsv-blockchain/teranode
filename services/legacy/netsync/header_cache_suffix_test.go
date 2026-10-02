package netsync

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/stretchr/testify/require"
)

// The tests in this file cover one rule: a getheaders reply is judged on
// whether it reaches above the committed tip, not on whether it starts there.
//
// The bug they pin was measured on Hetzner mainnet on 2026-09-14. The node
// commits around 18 blocks a second during a catch-up run, so between asking a
// peer for headers from height n and that peer's 2,000-header reply arriving,
// the committed tip has moved perhaps 20 blocks into the reply. The cache
// refused the whole batch on the strength of those first 20 entries and threw
// away the 1,980 that were exactly what the node wanted next. Eleven getheaders
// went out between 13:11:36 and 13:12:26 across eight peers and not one reply
// was kept; the last block was accepted at 13:12:30 and the twelfth reply
// landed at 13:12:40, in the same second as its own request. The round trip was
// never the problem — the node could only refill once it had stopped moving.
//
// tipWithin is the fixture that expresses "the tip has moved into the batch":
// it hands back the hash of headers[i], which is what the chain's committed tip
// is for a node that has just committed that header's block.
func tipWithin(headers []*wire.BlockHeader, i int) chainhash.Hash {
	return headers[i].BlockHash()
}

// TestHeaderCache_KeepsTheSuffixAboveAnAdvancedTip is the live bug. The batch
// answers a question asked from height 100, the node has since committed to
// 103, and the four entries at 101 to 103 plus the one naming 103 itself are
// behind us. Everything from 104 up is still precisely what the node wants.
//
// Before the suffix rule this Fill returned false and the node cached nothing.
func TestHeaderCache_KeepsTheSuffixAboveAnAdvancedTip(t *testing.T) {
	anchor := chainhash.Hash{0xaa}
	headers := chainOfHeaders(anchor, 10)

	// The reply was asked for from height 100, so headers[0] would sit at 101.
	// By the time it lands the chain's tip is height 103, which is headers[2].
	tip := tipWithin(headers, 2)

	c := newHeaderCache()
	require.True(t, c.Fill(tip, 104, headers),
		"a batch the committed tip has moved into is still 1,980 usable heights, not a batch to throw away")

	require.Equal(t, 7, c.Len(), "heights 104 to 110 are what is left above the tip")

	first, ok := c.At(104)
	require.True(t, ok, "the first height above the committed tip must be named")
	require.Equal(t, headers[3].BlockHash(), first)

	last, ok := c.At(110)
	require.True(t, ok, "and so must the last")
	require.Equal(t, headers[9].BlockHash(), last)

	top, ok := c.Top()
	require.True(t, ok)
	require.Equal(t, int32(110), top)
}

// TestHeaderCache_SuffixBoundaryIsExact pins the off-by-one that would be the
// worst thing this structure could do. Every height in the cache is a promise
// that a specific hash belongs at a specific number; caching the suffix one
// place out would name every single height with its neighbour's hash, and the
// node would download a whole run of blocks that each fail their parent check.
//
// The tip is deep inside the batch on purpose, so an error of one in either
// direction lands on a real, differently-hashed header rather than off the end
// where a bounds check would catch it anyway.
func TestHeaderCache_SuffixBoundaryIsExact(t *testing.T) {
	anchor := chainhash.Hash{0xab}
	headers := chainOfHeaders(anchor, 12)

	const (
		meetIndex  = 5
		baseHeight = int32(507)
	)

	// headers[0] sits at 501, so headers[meetIndex] sits at 506 and the first
	// usable entry, headers[meetIndex+1], sits at 507.
	c := newHeaderCache()
	require.True(t, c.Fill(tipWithin(headers, meetIndex), baseHeight, headers))

	_, ok := c.At(baseHeight - 1)
	require.False(t, ok, "the committed tip's own height must not be named: the node already has that block")

	got, ok := c.At(baseHeight)
	require.True(t, ok)
	require.Equal(t, headers[meetIndex+1].BlockHash(), got,
		"the lowest named height must carry the hash of the header directly after the tip")

	// And the whole run behind it, so a shift of one anywhere is caught rather
	// than only a shift at the boundary.
	for i := meetIndex + 1; i < len(headers); i++ {
		height := baseHeight + int32(i-(meetIndex+1)) //nolint:gosec // a batch index, bounded by the fixture
		got, ok := c.At(height)
		require.True(t, ok, "height %d must be named", height)
		require.Equal(t, headers[i].BlockHash(), got, "height %d must carry headers[%d]", height, i)
	}

	require.Equal(t, len(headers)-(meetIndex+1), c.Len(), "and nothing beyond the suffix may be named")
}

// TestHeaderCache_RefusesABatchWhoseLastHeaderIsTheTip covers the boundary at
// the other end: the reply is honest and connects, but the node has committed
// every block in it, so there is no usable suffix. The refusal has to be a
// clean one. Writing an empty map here would be a "success" that discarded a
// perfectly good previous batch in exchange for nothing, and the node would
// then have no heights to ask for until the next reply arrived.
func TestHeaderCache_RefusesABatchWhoseLastHeaderIsTheTip(t *testing.T) {
	first := chainhash.Hash{0xac}
	held := chainOfHeaders(first, 5)

	c := newHeaderCache()
	require.True(t, c.Fill(first, 101, held))

	anchor := chainhash.Hash{0xad}
	stale := chainOfHeaders(anchor, 4)

	require.False(t, c.Fill(tipWithin(stale, len(stale)-1), 900, stale),
		"a batch the tip has run clean past has no usable suffix and must be refused")

	require.Equal(t, 5, c.Len(), "and the batch already held must survive it")

	for i := 0; i < 5; i++ {
		got, ok := c.At(int32(101 + i))
		require.True(t, ok, "height %d must still be named", 101+i)
		require.Equal(t, held[i].BlockHash(), got)
	}
}

// TestHeaderCache_RefusesABatchThatNeverMeetsTheTip is the case the suffix rule
// must not widen: a run that does not contain the committed tip anywhere is a
// run from a chain this node is not on, or one it has passed entirely, and
// caching it would name heights on our chain with another chain's hashes.
func TestHeaderCache_RefusesABatchThatNeverMeetsTheTip(t *testing.T) {
	first := chainhash.Hash{0xae}
	held := chainOfHeaders(first, 5)

	c := newHeaderCache()
	require.True(t, c.Fill(first, 101, held))

	// An honest, internally linked run off a different ancestor. The tip hash
	// is not its parent and appears nowhere inside it.
	other := chainOfHeaders(chainhash.Hash{0xbb}, 6)

	require.False(t, c.Fill(chainhash.Hash{0xaf}, 900, other),
		"a run that never meets the committed tip describes a chain this node is not on")

	require.Equal(t, 5, c.Len(), "and it must leave the batch already held completely intact")

	for i := 0; i < 5; i++ {
		got, ok := c.At(int32(101 + i))
		require.True(t, ok, "height %d must still be named", 101+i)
		require.Equal(t, held[i].BlockHash(), got)
	}
}

// TestHeaderCache_RefusesABreakBeforeWhereTheRunMeetsTheTip and its sibling
// below are the two halves of the internal-linkage guard, which the suffix rule
// makes it possible to get wrong in a new way: a walk that stopped checking
// once it had found the tip, or one that only started checking there, would
// each pass one of these and fail the other.
//
// A break before the meeting point matters because the meeting point is only
// meaningful if the hash it matched was computed from a header that genuinely
// follows the one before it. Without the check the "tip" could be matched
// against a header spliced in from anywhere.
func TestHeaderCache_RefusesABreakBeforeWhereTheRunMeetsTheTip(t *testing.T) {
	first := chainhash.Hash{0xb0}
	held := chainOfHeaders(first, 5)

	c := newHeaderCache()
	require.True(t, c.Fill(first, 101, held))

	anchor := chainhash.Hash{0xb1}
	headers := chainOfHeaders(anchor, 10)

	// The tip sits at index 5; the break is at index 2, ahead of it.
	tip := tipWithin(headers, 5)
	headers[2].PrevBlock = chainhash.Hash{0xcc}

	require.False(t, c.Fill(tip, 107, headers),
		"a break anywhere in the run leaves every height after it a guess, even a break below the tip")

	require.Equal(t, 5, c.Len(), "and a refused fill must not disturb what is held")
}

// A break after the meeting point is the more obvious half: those are the
// heights actually being cached, and every one past the break is a guess.
func TestHeaderCache_RefusesABreakAfterWhereTheRunMeetsTheTip(t *testing.T) {
	first := chainhash.Hash{0xb2}
	held := chainOfHeaders(first, 5)

	c := newHeaderCache()
	require.True(t, c.Fill(first, 101, held))

	anchor := chainhash.Hash{0xb3}
	headers := chainOfHeaders(anchor, 10)

	// The tip sits at index 2; the break is at index 6, inside the suffix.
	tip := tipWithin(headers, 2)
	headers[6].PrevBlock = chainhash.Hash{0xcc}

	require.False(t, c.Fill(tip, 104, headers),
		"a break inside the suffix must refuse the whole batch, not cache the part before it")

	require.Equal(t, 5, c.Len(), "and a refused fill must not disturb what is held")
}

// TestHeaderCache_StillAcceptsABatchThatLinksAtTheFront is the no-regression
// case. A node that has just started, or one whose tip has not moved while the
// reply was in flight, gets a batch whose first header names the committed tip
// as its parent, and the whole batch is usable from baseHeight. That is what
// the rule the suffix rule replaces did, and the suffix search must not have
// shifted it by one or dropped its first entry.
func TestHeaderCache_StillAcceptsABatchThatLinksAtTheFront(t *testing.T) {
	tip := chainhash.Hash{0xb4}
	headers := chainOfHeaders(tip, 6)

	c := newHeaderCache()
	require.True(t, c.Fill(tip, 101, headers))

	require.Equal(t, 6, c.Len(), "the whole batch is above the tip, so all of it is cached")

	for i, h := range headers {
		height := int32(101 + i) //nolint:gosec // a batch index, bounded by the fixture
		got, ok := c.At(height)
		require.True(t, ok, "height %d must be named", height)
		require.Equal(t, h.BlockHash(), got)
	}

	top, ok := c.Top()
	require.True(t, ok)
	require.Equal(t, int32(106), top)
}

// TestHandleHeadersMsg_ASuccessfulFillRunsAnAssignmentPass is the second half
// of the mainnet gap, and it is the larger half.
//
// Nothing used to ask for blocks when a batch of headings landed. An assignment
// pass runs on a commit, on a block arriving, on a peer connecting, or on the
// park sweep's ticker, and at a cache boundary the first three are all quiet by
// definition — the node has run out of heights to ask for, which is why it was
// asking for headers. That left parkSweepInterval, 30 seconds, as the only
// thing that would notice the fill. Measured on mainnet on 2026-09-14: the fill
// landed at 13:12:40 and the next block was accepted at 13:13:10, exactly one
// sweep interval later, and the 13:10:02 boundary shows the same 30 seconds.
//
// This drives the real handleHeadersMsg, not fillHeaderCache, because the wire
// between the two is the thing under test. Before the fix the getdata never
// arrives and this times out.
func TestHandleHeadersMsg_ASuccessfulFillRunsAnAssignmentPass(t *testing.T) {
	sm := newRaceManager(t)
	sm.ctx = context.Background()
	sm.blockSizeTracker = newBlockSizeTracker(10)

	// mockCommittedTip provisions the blockchain mock this pass needs: a best
	// block header at height 100, a GetBlockHeader that answers not-found so
	// haveInventory never claims the chain already holds these blocks, and a
	// GetBlockLocator for the refill maybeRequestMoreHeaders will also run.
	tip := mockCommittedTip(t, sm, 100, 0)

	peer, rec := schedulerPeer(t, sm, 60, 2000)

	var nonce uint32

	msg, hashes := linkedHeaders(tip, 12, &nonce)

	sm.handleHeadersMsg(&headersMsg{headers: msg, peer: peer})

	// A wait, not an immediate sample: the getdata is written to the peer's
	// socket asynchronously, so reading the recorder straight after the call
	// would be reading a race and would pass or fail on timing rather than on
	// behaviour.
	require.True(t, WaitUntil(func() bool { return rec.count() > 0 }, 5*time.Second),
		"a batch of headers that filled the cache must be followed by a request for the blocks it names")

	got := rec.all()
	require.Equal(t, hashes[0], got[0],
		"and the first block asked for must be the one directly above the committed tip")
}
