package netsync

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestMaybeRequestMoreHeaders_AsksForMoreOnceTheCacheReachesItsTop drives the
// real path: fill the header cache with one getheaders batch's worth of
// headers, commit the node all the way up to the top of that batch, run the
// download pass, and check a fresh getheaders went out.
//
// This is the state a node reaches the moment it finishes what its last
// getheaders reply named. Before this fix, nothing was left to notice that
// moment: wantedBlocksFromCache came back empty, assignWantedBlocks requested
// nothing, and the node sat there — the exact stall this whole rewrite exists
// to remove, reached simply by making progress.
//
// The locator itself is asserted twice, from two directions. GetBlockLocator
// must be called with the committed tip's own hash and height as its
// arguments — never anything derived from the cache or from a header merely
// downloaded, which is what had a peer answer from genesis and lose its
// connection for answering honestly on 2026-09-11. And the getheaders that
// goes out on the wire must carry exactly what that call returned, proving
// there is no second, competing way this path could have built a locator.
func TestMaybeRequestMoreHeaders_AsksForMoreOnceTheCacheReachesItsTop(t *testing.T) {
	fromChain := []*chainhash.Hash{{0x99}}

	client := &blockchain2.Mock{}
	client.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).Return(fromChain, nil)

	sm := newRaceManager(t)
	sm.ctx = context.Background()
	sm.blockchainClient = client

	_, _, headers := demotionPeer(t, sm, 220, 1000)

	var nonce uint32

	anchor := chainhash.Hash{0x70}
	msg, hashes := linkedHeaders(anchor, 5, &nonce)

	sm.headerCache = newHeaderCache()
	require.True(t, sm.headerCache.Fill(anchor, 1, msg.Headers))

	// The committed tip has caught all the way up to the last header the cache
	// names: nothing is left in the cache above it. mockCommittedTipAtHash makes
	// the chain mock answer with that exact header, re-encoded, so its hash is
	// provably hashes[len(hashes)-1] rather than merely standing in for it.
	tip := mockCommittedTipAtHash(t, sm, uint32(len(hashes)), msg.Headers[len(msg.Headers)-1]) //nolint:gosec // a small test count
	require.Equal(t, hashes[len(hashes)-1], tip, "sanity: the re-encoded header must hash to what linkedHeaders already computed")

	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return headers.count() == 1 }, 5*time.Second),
		"a cache with nothing left above the committed tip must produce a getheaders")

	client.AssertCalled(t, "GetBlockLocator", mock.Anything, &tip, uint32(len(hashes))) //nolint:gosec // a small test count

	sent := headers.last()
	require.NotNil(t, sent)
	require.Equal(t, *fromChain[0], *sent.BlockLocatorHashes[0],
		"the getheaders sent must carry exactly the locator the chain's own GetBlockLocator returned")
}

// TestMaybeRequestMoreHeaders_DoesNotAskTwiceWithinTheRateLimit pins the floor
// that keeps a commit-driven pass from turning into a getheaders storm. Every
// commit can call assignWantedBlocks, and a cache that has run dry stays dry
// for as long as the reply is still in flight — without a floor, every one of
// those calls would send its own request to whichever peer it happened to
// pick.
func TestMaybeRequestMoreHeaders_DoesNotAskTwiceWithinTheRateLimit(t *testing.T) {
	client := &blockchain2.Mock{}
	client.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
		Return([]*chainhash.Hash{{0x99}}, nil)

	sm := newRaceManager(t)
	sm.ctx = context.Background()
	sm.blockchainClient = client

	_, _, headers := demotionPeer(t, sm, 221, 1000)

	var nonce uint32

	anchor := chainhash.Hash{0x71}
	msg, hashes := linkedHeaders(anchor, 5, &nonce)

	sm.headerCache = newHeaderCache()
	require.True(t, sm.headerCache.Fill(anchor, 1, msg.Headers))
	mockCommittedTip(t, sm, uint32(len(hashes)), 0) //nolint:gosec // a small test count

	// Twenty passes in a row, the way twenty rapid commits would each call
	// fetchHeaderBlocks while the first reply is still on the wire.
	for i := 0; i < 20; i++ {
		sm.fetchHeaderBlocks()
	}

	require.True(t, WaitUntil(func() bool { return headers.count() > 0 }, 5*time.Second),
		"sanity: at least the first pass must have asked")
	require.Equal(t, 1, headers.count(),
		"twenty passes inside the rate limit's window must produce exactly one getheaders")
	client.AssertNumberOfCalls(t, "GetBlockLocator", 1)
}

// TestMaybeRequestMoreHeaders_RotatesAcrossEligiblePeers pins the fix for the
// single-usable-peer stall: PushGetHeadersMsg drops a repeat of the same
// (locator, stop hash) pair from one peer, and while the header cache is
// empty neither half of that pair can move — the stop hash is always the
// zero hash, and the locator is built from the committed tip, which cannot
// advance until a block commits. Sending to peers[0] on every call therefore
// sends exactly once and is filtered forever after: with one usable peer, the
// node never asks again once that first reply is unusable.
//
// This drives the refill repeatedly against three connected, eligible peers
// and requires that more than one of them actually received a request. Before
// the fix, every one of these passes would have named the same peer and every
// send after the first would have been silently dropped by that peer's own
// dedup filter.
func TestMaybeRequestMoreHeaders_RotatesAcrossEligiblePeers(t *testing.T) {
	client := &blockchain2.Mock{}
	client.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
		Return([]*chainhash.Hash{{0x99}}, nil)

	sm := newRaceManager(t)
	sm.ctx = context.Background()
	sm.blockchainClient = client

	_, _, headersA := demotionPeer(t, sm, 240, 1000)
	_, _, headersB := demotionPeer(t, sm, 241, 1000)
	_, _, headersC := demotionPeer(t, sm, 242, 1000)

	var nonce uint32

	anchor := chainhash.Hash{0x72}
	msg, hashes := linkedHeaders(anchor, 5, &nonce)

	sm.headerCache = newHeaderCache()
	require.True(t, sm.headerCache.Fill(anchor, 1, msg.Headers))
	mockCommittedTip(t, sm, uint32(len(hashes)), 0) //nolint:gosec // a small test count

	// Repeated refills, as repeated commit-driven passes would produce while a
	// single unusable reply leaves the cache dry. The rate limit's floor is
	// reset before each pass so every one of them actually reaches a peer,
	// standing in for headerCacheRefillInterval having elapsed on a live node.
	for i := 0; i < 3; i++ {
		want := i + 1

		sm.lastHeaderRequestAt.Store(0)
		sm.fetchHeaderBlocks()

		require.True(t, WaitUntil(func() bool {
			return headersA.count()+headersB.count()+headersC.count() >= want
		}, 5*time.Second), "pass %d must have sent a getheaders", i)
	}

	distinct := 0

	for _, r := range []*getHeadersRecorder{headersA, headersB, headersC} {
		if r.count() > 0 {
			distinct++
		}
	}

	require.Greater(t, distinct, 1,
		"three refills against three connected, eligible peers must not all land on the same one")
}

// TestMaybeRequestMoreHeaders_SinglePeerAsksAgainAfterRateLimitLapses pins the
// half of the stall the peer rotation above does not reach. Rotation only
// ever changes which peer is asked; with exactly one eligible peer every
// retry names that same peer again, and PushGetHeadersMsg's own dedup filter
// on that peer sees the same (locator, stop hash) pair every time — the stop
// hash is always the zero hash, and the locator cannot move until a block
// commits — so it drops every retry after the first while logging success.
// Before the fix this test would see exactly one getheaders reach the wire
// no matter how many times the refill ran.
func TestMaybeRequestMoreHeaders_SinglePeerAsksAgainAfterRateLimitLapses(t *testing.T) {
	client := &blockchain2.Mock{}
	client.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
		Return([]*chainhash.Hash{{0x99}}, nil)

	sm := newRaceManager(t)
	sm.ctx = context.Background()
	sm.blockchainClient = client

	_, _, headers := demotionPeer(t, sm, 250, 1000)

	var nonce uint32

	anchor := chainhash.Hash{0x73}
	msg, hashes := linkedHeaders(anchor, 5, &nonce)

	sm.headerCache = newHeaderCache()
	require.True(t, sm.headerCache.Fill(anchor, 1, msg.Headers))
	mockCommittedTip(t, sm, uint32(len(hashes)), 0) //nolint:gosec // a small test count

	// Repeated refills against the one eligible peer, as repeated
	// commit-driven passes would produce while a single unusable reply
	// leaves the cache dry. The rate limit's floor is reset before each pass
	// to stand in for headerCacheRefillInterval having elapsed on a live
	// node — the actual gate under test is the peer's own dedup filter, not
	// this one.
	for i := 0; i < 3; i++ {
		want := i + 1

		sm.lastHeaderRequestAt.Store(0)
		sm.fetchHeaderBlocks()

		require.True(t, WaitUntil(func() bool {
			return headers.count() >= want
		}, 5*time.Second), "pass %d must have sent a getheaders", i)
	}

	require.Greater(t, headers.count(), 1,
		"a single eligible peer must still be asked again once the rate limit lapses")
}

// TestMaybeRequestMoreHeaders_AsksEarlyWhenRemainingCacheIsBelowThreshold pins
// the fix itself: on a live Hetzner mainnet node, maybeRequestMoreHeaders only
// ever asked once wantedBlocksFromCache's depth-limited pass ran clean off the
// end of the header cache, which happens only once the cache is fully drained.
// Measured on that node: a 2,000-header batch commits in about 100 seconds,
// and a refill at a boundary can cost multiple rate-limited attempts before
// a peer answers (three attempts, ~40 seconds, observed at the 8,000
// boundary) — dead time repeating every 2,000 blocks for the whole sync.
//
// This cache holds far more heights above the committed tip (300) than the
// read-ahead depth that limits a single pass (legacy_blockDownloadLowerWindow,
// 128 by default), so the backstop alone — "does the cache still name
// last+1?" — would find it does and stay quiet. The fix must still ask,
// because 300 remaining heights is under headerCacheRefillThreshold (1,000).
func TestMaybeRequestMoreHeaders_AsksEarlyWhenRemainingCacheIsBelowThreshold(t *testing.T) {
	client := &blockchain2.Mock{}
	client.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
		Return([]*chainhash.Hash{{0x99}}, nil)

	sm := newRaceManager(t)
	sm.ctx = context.Background()
	sm.blockchainClient = client

	_, _, headers := demotionPeer(t, sm, 251, 1000)

	var nonce uint32

	anchor := chainhash.Hash{0x74}
	// 1,300 heights above the tip (1,000 to 2,300 named by the cache, tip at
	// 1,000): comfortably more than the 128-height read-ahead depth, so the
	// backstop alone sees plenty of cache left, but the 1,300 remaining above
	// the tip is what headerCacheRefillThreshold judges, and it is not being
	// tested here directly — the committed tip is placed 300 below the
	// cache's top instead, so remaining (300) sits under the 1,000 threshold.
	msg, _ := linkedHeaders(anchor, 1300, &nonce)

	sm.headerCache = newHeaderCache()
	require.True(t, sm.headerCache.Fill(anchor, 1, msg.Headers))

	// Committed tip at height 1,000: the cache names up to height 1,300, so
	// only 300 heights remain above the tip, under headerCacheRefillThreshold
	// (1,000), even though the read-ahead depth (128) leaves last+1 = 1,129
	// well short of the cache's own end at 1,300.
	mockCommittedTip(t, sm, 1000, 0)

	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return headers.count() == 1 }, 5*time.Second),
		"a cache with only 300 heights left above the tip, under the 1,000-height threshold, must trigger an early refill")
}

// TestMaybeRequestMoreHeaders_DoesNotAskWhenCacheIsComfortablyFull is the other
// direction of the same threshold check: a cache that still names well over
// headerCacheRefillThreshold heights above the committed tip must not trigger
// a refill, or the fix would just burn the rate-limit slot on every pass while
// there is nothing yet to gain from asking.
func TestMaybeRequestMoreHeaders_DoesNotAskWhenCacheIsComfortablyFull(t *testing.T) {
	client := &blockchain2.Mock{}
	client.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
		Return([]*chainhash.Hash{{0x99}}, nil)

	sm := newRaceManager(t)
	sm.ctx = context.Background()
	sm.blockchainClient = client

	_, _, headers := demotionPeer(t, sm, 252, 1000)

	var nonce uint32

	anchor := chainhash.Hash{0x75}
	// The cache names up to height 2,500 and the tip sits at 1,000, so 1,500
	// heights remain above the tip: over headerCacheRefillThreshold (1,000),
	// with headroom to spare.
	msg, _ := linkedHeaders(anchor, 2500, &nonce)

	sm.headerCache = newHeaderCache()
	require.True(t, sm.headerCache.Fill(anchor, 1, msg.Headers))

	mockCommittedTip(t, sm, 1000, 0)

	// Several passes, the way repeated commits would drive this in practice:
	// none of them may ask, not just the first.
	for i := 0; i < 5; i++ {
		sm.fetchHeaderBlocks()
	}

	require.Never(t, func() bool { return headers.count() > 0 }, 200*time.Millisecond, 20*time.Millisecond,
		"a cache with 1,500 heights left above the tip, comfortably over the 1,000-height threshold, must not trigger a refill")
	client.AssertNotCalled(t, "GetBlockLocator", mock.Anything, mock.Anything, mock.Anything)
}
