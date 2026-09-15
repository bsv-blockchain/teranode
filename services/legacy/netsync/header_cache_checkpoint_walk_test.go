package netsync

import (
	"bytes"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// This file covers the below-checkpoint header walk this branch adds on top of
// headerCache: below the last checkpoint a fill EXTENDS the list instead of
// replacing it, and a request short of the checkpoint is followed immediately
// rather than on the ordinary 5-second cadence. See
// docs/superpowers/sdd/2026-09-13-legacy-sync-simple/checkpoint-header-list.md
// for the design this pins.
//
// Every test builds a real, internally linked run and drives it through Fill
// or fillHeaderCache — never headerCache.provenTo or .Proven's return value set
// directly — following header_provenance_flow_test.go's own rule.

// headersMsgOf wraps headers in a wire.MsgHeaders, the shape fillHeaderCache
// and handleHeadersMsg actually take.
func headersMsgOf(t *testing.T, headers []*wire.BlockHeader) *wire.MsgHeaders {
	t.Helper()

	msg := wire.NewMsgHeaders()
	for _, h := range headers {
		require.NoError(t, msg.AddBlockHeader(h))
	}

	return msg
}

// Test 1: below a checkpoint, successive replies extend the list up to and
// including the checkpoint. No entry is proven before the checkpoint hash is
// matched, and every entry is proven after.
func TestCheckpointWalk_ExtendProvesOnlyAfterMatchingTheCheckpoint(t *testing.T) {
	genesis := chainhash.Hash{0x01}
	full, hashes := linkedRun(genesis, 12)

	// One continuous chain of 12; the checkpoint sits at height 10 (the 10th
	// header), inside the second reply.
	cache := newHeaderCache().WithCheckpoints([]chaincfg.Checkpoint{{Height: 10, Hash: &hashes[9]}})

	require.True(t, cache.Fill(genesis, 1, full[:6]), "the first reply: heights 1 to 6, cold start")

	for i := 0; i < 6; i++ {
		require.False(t, cache.Proven(hashes[i]), "height %d must not be proven before the walk reaches the checkpoint", i+1)
	}

	require.True(t, cache.Fill(hashes[5], 1, full[6:]),
		"the second reply must EXTEND onto the list's own top (hashes[5]), not replace it")

	top, ok := cache.Top()
	require.True(t, ok)
	require.Equal(t, int32(12), top, "the list must hold the whole walk, not just the second reply")
	require.Equal(t, int32(10), cache.ProvenTo())

	for i := 0; i < 10; i++ {
		require.True(t, cache.Proven(hashes[i]), "height %d must be proven once the walk has matched the checkpoint at height 10", i+1)
	}

	for i := 10; i < 12; i++ {
		require.False(t, cache.Proven(hashes[i]), "height %d, above the matched checkpoint, must not be proven", i+1)
	}
}

// Test 2: a reply reaching the checkpoint height with the wrong hash drops the
// WHOLE list, not merely this batch, leaves nothing proven, and disconnects
// the sender.
func TestCheckpointWalk_WrongHashAtCheckpointDropsWholeListAndDisconnects(t *testing.T) {
	sm := newRaceManager(t)

	tipHash := mockCommittedTip(t, sm, 0, 0)
	good, goodHashes := linkedRun(tipHash, 12)

	params := chaincfg.RegressionNetParams
	params.Checkpoints = []chaincfg.Checkpoint{{Height: 10, Hash: &goodHashes[9]}}
	sm.chainParams = &params
	sm.headerCache = newHeaderCache().WithCheckpoints(params.Checkpoints)

	peer, _, headers := demotionPeer(t, sm, 230, 1000)
	require.True(t, peer.Connected())

	// First reply: cold start, heights 1 to 6, short of the checkpoint. This
	// itself is short of the checkpoint at 10, so it legitimately triggers the
	// walk's own immediate continuation — recorded here, not asserted away,
	// because what this test pins is the SECOND reply's contradiction, not
	// whether the first one asks for more.
	require.True(t, sm.fillHeaderCache(peer, headersMsgOf(t, good[:6])))
	require.False(t, sm.headerCache.Proven(goodHashes[0]), "sanity: nothing is proven yet")

	// The continuation lands on the peer's outbound queue asynchronously, so
	// wait for it before taking the baseline. Sampling straight away read zero
	// whenever the send had not landed yet, most often under -race, and then
	// counted that same legitimate request against the disconnected sender.
	require.Eventually(t, func() bool { return headers.count() == 1 }, 5*time.Second, 5*time.Millisecond,
		"a successful fill short of the checkpoint must ask for the next batch")

	sentBeforeContradiction := headers.count()

	// Second reply: an honest, internally linked run extending from the list's
	// own top (goodHashes[5]) that carries the WRONG hash at the checkpoint
	// height. forgedRun is header_provenance_test.go's fixture for exactly
	// this: a valid linked run from the same anchor that hashes differently at
	// every height.
	forged, forgedHashes := forgedRun(goodHashes[5], 6)

	require.False(t, sm.fillHeaderCache(peer, headersMsgOf(t, forged)),
		"a run reaching the checkpoint height with the wrong hash must be refused")

	require.Zero(t, sm.headerCache.Len(), "the WHOLE list must be dropped, not merely this batch")
	require.Zero(t, sm.headerCache.ProvenTo())
	require.False(t, sm.headerCache.Proven(goodHashes[0]),
		"even the earlier, never-in-question prefix must be gone: it was one linked chain with the batch that just failed")
	require.False(t, sm.headerCache.Proven(forgedHashes[0]))

	require.False(t, peer.Connected(), "a peer whose run contradicts a pinned checkpoint hash must be disconnected")
	require.Never(t, func() bool { return headers.count() != sentBeforeContradiction }, 300*time.Millisecond, 10*time.Millisecond,
		"a dropped, disconnected sender is not asked for anything more")
}

// Test 3: a reply connecting to neither the list top nor the committed tip is
// refused, the list is unchanged, and the sender is not disconnected.
func TestCheckpointWalk_ReplyMeetingNeitherAnchorIsRefusedWithoutDisconnect(t *testing.T) {
	sm := newRaceManager(t)

	tipHash := mockCommittedTip(t, sm, 0, 0)
	good, goodHashes := linkedRun(tipHash, 6)

	params := chaincfg.RegressionNetParams
	params.Checkpoints = []chaincfg.Checkpoint{{Height: 20, Hash: &chainhash.Hash{0x99}}}
	sm.chainParams = &params
	sm.headerCache = newHeaderCache().WithCheckpoints(params.Checkpoints)

	peer, _, headers := demotionPeer(t, sm, 231, 1000)

	require.True(t, sm.fillHeaderCache(peer, headersMsgOf(t, good)))
	require.Equal(t, 6, sm.headerCache.Len())

	// That fill was a success short of the checkpoint at height 20, so it
	// correctly sends the walk's next request to this peer. The send lands on
	// the peer's outbound queue asynchronously, so wait for it and take it as
	// the baseline. Asserting zero at the end instead counted this legitimate
	// request whenever it happened to land first, which made the test fail
	// about one run in six for a reason that had nothing to do with refusal.
	require.Eventually(t, func() bool { return headers.count() == 1 }, 5*time.Second, 5*time.Millisecond,
		"a successful fill short of the checkpoint must ask for the next batch")

	baseline := headers.count()

	// Neither the committed tip (still height 0) nor the list's own top
	// (goodHashes[5]) is this batch's parent, and neither hash appears inside
	// it anywhere: an honest reply about a point this node has already moved
	// past, from a peer that did nothing wrong.
	stray, _ := linkedRun(chainhash.Hash{0x55}, 4)

	require.False(t, sm.fillHeaderCache(peer, headersMsgOf(t, stray)))

	require.Equal(t, 6, sm.headerCache.Len(), "the list must be unchanged")

	top, ok := sm.headerCache.Top()
	require.True(t, ok)
	require.Equal(t, int32(6), top)

	for i, h := range goodHashes {
		got, ok := sm.headerCache.At(int32(1 + i)) //nolint:gosec // a small test count
		require.True(t, ok)
		require.Equal(t, h, got)
	}

	require.True(t, peer.Connected(), "an honest reply about a point already passed must not cost the connection")
	// "Must not happen" needs a real window to happen in, not a single sample
	// taken before an asynchronous send could have landed.
	require.Never(t, func() bool { return headers.count() != baseline }, 300*time.Millisecond, 10*time.Millisecond,
		"a refusal that names no contradiction sends nothing back")
}

// Test 4: an extending reply whose front overlaps the list top keeps exactly
// the part above the top, at the right heights.
func TestCheckpointWalk_ExtendKeepsExactlyTheSuffixAboveTheListTop(t *testing.T) {
	genesis := chainhash.Hash{0x31}
	full, hashes := linkedRun(genesis, 10)

	cache := newHeaderCache().WithCheckpoints([]chaincfg.Checkpoint{{Height: 20, Hash: &chainhash.Hash{0x99}}})

	require.True(t, cache.Fill(genesis, 1, full[:6]), "heights 1 to 6")

	// The second reply's front overlaps the list: it repeats the real headers
	// for heights 5 and 6 (the list's own top) before continuing with four new
	// ones for heights 7 to 10.
	overlap := append([]*wire.BlockHeader{}, full[4], full[5])
	overlap = append(overlap, full[6:]...)

	require.True(t, cache.Fill(chainhash.Hash{}, 1, overlap))

	top, ok := cache.Top()
	require.True(t, ok)
	require.Equal(t, int32(10), top)
	require.Equal(t, 10, cache.Len(), "6 already held plus exactly the 4 new heights, not 6")

	first, ok := cache.At(7)
	require.True(t, ok, "the first height above the overlap must be named")
	require.Equal(t, hashes[6], first, "height 7 must carry headers[6]'s hash, not its neighbour's")

	last, ok := cache.At(10)
	require.True(t, ok, "the last height must be named")
	require.Equal(t, hashes[9], last, "height 10 must carry headers[9]'s hash, not its neighbour's")
}

// newExtendingWalkManager builds a manager with a header cache already filled
// below a checkpoint far above what a single reply reaches, a mocked
// GetBlockLocator answering with one recognisable placeholder entry, and a
// real, connected peer recording every getheaders it is sent. The initial fill
// itself is below the checkpoint and short of it, so it triggers the walk's
// own immediate continuation — every caller sees headers.count() == 1 already
// by the time this returns.
func newExtendingWalkManager(t *testing.T, checkpointHeight int32) (sm *SyncManager, peer *peerpkg.Peer, headers *getHeadersRecorder, hashes []chainhash.Hash) {
	t.Helper()

	sm = newRaceManager(t)

	tipHash := mockCommittedTip(t, sm, 0, 0)

	client, ok := sm.blockchainClient.(*blockchain2.Mock)
	require.True(t, ok, "harness check: mockCommittedTip must install a *blockchain2.Mock")

	client.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).Unset()
	client.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
		Return([]*chainhash.Hash{{0x77}}, nil)

	full, hashes := linkedRun(tipHash, 6)

	params := chaincfg.RegressionNetParams
	params.Checkpoints = []chaincfg.Checkpoint{{Height: checkpointHeight, Hash: &chainhash.Hash{0x99}}}
	sm.chainParams = &params
	sm.headerCache = newHeaderCache().WithCheckpoints(params.Checkpoints)

	peer, _, headers = demotionPeer(t, sm, 232, 1000)

	require.True(t, sm.fillHeaderCache(peer, headersMsgOf(t, full)))

	// Make the promise in this helper's doc comment true rather than assumed:
	// the continuation send is asynchronous.
	require.True(t, WaitUntil(func() bool { return headers.count() == 1 }, 5*time.Second),
		"harness check: the initial fill short of the checkpoint must send the walk's first continuation")

	return sm, peer, headers, hashes
}

// Test 5: an extending request's locator has the list-top hash first, the
// committed-tip locator entries after it, and a zero stop hash.
func TestCheckpointWalk_ExtendingLocatorHasTopHashFirstThenTipLocatorThenZeroStop(t *testing.T) {
	_, _, headers, hashes := newExtendingWalkManager(t, 5000)

	require.True(t, WaitUntil(func() bool { return headers.count() > 0 }, 5*time.Second),
		"a fill short of the checkpoint must continue the walk immediately")

	sent := headers.last()
	require.NotNil(t, sent)
	require.Len(t, sent.BlockLocatorHashes, 2, "the list's own top hash, then the tip locator's one placeholder entry")
	require.Equal(t, hashes[5], *sent.BlockLocatorHashes[0], "the list's own top hash must be first")
	require.Equal(t, chainhash.Hash{0x77}, *sent.BlockLocatorHashes[1], "the committed-tip locator's own entries must follow it, unchanged")
	require.True(t, sent.HashStop.IsEqual(&chainhash.Hash{}),
		"the stop hash must stay the zero hash: docs/superpowers/specs/2026-09-11-legacy-sync-stall-800128.md")
}

// Test 6: a successful extending fill short of the checkpoint sends the next
// request immediately, without waiting for the 5-second interval.
func TestCheckpointWalk_ExtendingFillContinuesImmediatelyWithoutWaitingTheInterval(t *testing.T) {
	sm, peer, headers, hashes := newExtendingWalkManager(t, 5000)

	require.True(t, WaitUntil(func() bool { return headers.count() == 1 }, 5*time.Second))

	// Force the interval to look freshly consumed, the state a below-checkpoint
	// walk is normally in between two fast replies: allowedToRequestMoreHeadersNow
	// would refuse a call made through the ordinary rate-limited path right now.
	sm.lastHeaderRequestAt.Store(time.Now().UnixNano())
	require.False(t, sm.allowedToRequestMoreHeadersNow(time.Now()),
		"sanity: the interval must genuinely be blocking the rate-limited path at this instant")

	more, _ := linkedRun(hashes[5], 6)
	require.True(t, sm.fillHeaderCache(peer, headersMsgOf(t, more)))

	require.True(t, WaitUntil(func() bool { return headers.count() == 2 }, 5*time.Second),
		"a successful extending fill short of the checkpoint must send its own next request immediately, not wait out the interval")
}

// Test 7: above the last checkpoint, a reply replaces from the tip exactly as
// before, even when a stale, already-extended list still reaches above the
// new tip — the chain can still reorg up there, and extending would keep a
// branch this node may have left.
func TestCheckpointWalk_PastTheLastCheckpointAlwaysReplaces(t *testing.T) {
	tip0 := chainhash.Hash{0x41}
	full, hashes := linkedRun(tip0, 20)

	cache := newHeaderCache().WithCheckpoints([]chaincfg.Checkpoint{{Height: 2, Hash: &hashes[1]}})

	require.True(t, cache.Fill(tip0, 1, full))
	require.Equal(t, int32(2), cache.ProvenTo(), "sanity: the first fill matched the only checkpoint")

	top, ok := cache.Top()
	require.True(t, ok)
	require.Equal(t, int32(20), top, "sanity: the list already extends well above the tip below")

	// The tip has since advanced, honestly, along the SAME chain, to height 10
	// (hashes[9]) — but height 10 is already past the only checkpoint, so
	// nothing is ahead to walk toward any more.
	next, _ := linkedRun(hashes[9], 5)

	require.True(t, cache.Fill(hashes[9], 11, next),
		"an above-checkpoint fill must still accept a batch anchored on the real tip")

	newTop, ok := cache.Top()
	require.True(t, ok)
	require.Equal(t, int32(15), newTop)
	require.Equal(t, 5, cache.Len(), "a replace, not an extend: the old heights 1..20 must be gone")

	_, ok = cache.At(1)
	require.False(t, ok, "the pre-checkpoint-crossing content must not survive a replace")

	require.Zero(t, cache.ProvenTo(), "nothing is proven once there is no checkpoint left to walk toward")

	for _, h := range next {
		require.False(t, cache.Proven(h.BlockHash()))
	}
}

// Test 8: pruning keeps the list bounded as the tip advances, and
// pipelineParentHeight still resolves a parent at the (now pruned) tip itself,
// through the blockchain client fallback.
func TestCheckpointWalk_PruneKeepsListBoundedAndParentAtTipStillResolves(t *testing.T) {
	sm := newRaceManager(t)

	tipHash := mockCommittedTip(t, sm, 5, 0)

	params := chaincfg.RegressionNetParams
	params.Checkpoints = []chaincfg.Checkpoint{{Height: 5000, Hash: &chainhash.Hash{0x99}}}
	sm.chainParams = &params
	sm.headerCache = newHeaderCache().WithCheckpoints(params.Checkpoints)

	// Heights 6..15, all above the committed tip at 5.
	full, hashes := linkedRun(tipHash, 10)
	require.True(t, sm.headerCache.Fill(tipHash, 6, full))

	// The tip advances, honestly along the same chain, to height 10 —
	// hashes[4], the block the walk itself already named at that height.
	newTip := hashes[4]

	client, ok := sm.blockchainClient.(*blockchain2.Mock)
	require.True(t, ok)
	client.On("GetBlockHeader", mock.Anything, mock.Anything).Unset()
	client.On("GetBlockHeader", mock.Anything, &newTip).
		Return(&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 10}, nil)

	sm.headerCache.Prune(10)

	_, atTip := sm.headerCache.At(6)
	require.False(t, atTip, "everything at or below the new tip must be gone")

	top, ok := sm.headerCache.Top()
	require.True(t, ok)
	require.Equal(t, int32(15), top, "the untouched suffix above the tip must survive")
	require.Equal(t, 5, sm.headerCache.Len())

	// pipelineParentHeight for a parent hash EXACTLY at the pruned tip must
	// still resolve — via the blockchain client fallback, since the cache no
	// longer names it.
	height, resolved := sm.pipelineParentHeight(newTip)
	require.True(t, resolved, "a parent at the committed tip must still resolve once pruned from the cache")
	require.Equal(t, uint32(11), height)
}

// Test 9: after the tip passes checkpoint C, the list keeps extending toward
// the next checkpoint C2, and blocks between C and C2 become proven once C2 is
// matched — not before.
func TestCheckpointWalk_CrossingACheckpointContinuesTowardTheNextOne(t *testing.T) {
	genesis := chainhash.Hash{0x51}
	full, hashes := linkedRun(genesis, 16)

	cache := newHeaderCache().WithCheckpoints([]chaincfg.Checkpoint{
		{Height: 5, Hash: &hashes[4]},
		{Height: 15, Hash: &hashes[14]},
	})

	require.True(t, cache.Fill(genesis, 1, full[:8]), "heights 1 to 8: matches C at height 5")
	require.Equal(t, int32(5), cache.ProvenTo())
	require.False(t, cache.Proven(hashes[7]), "height 8, above C, is not proven until C2 is matched")

	require.True(t, cache.Fill(hashes[7], 1, full[8:12]), "heights 9 to 12, extending onto the list's own top")
	require.Equal(t, int32(5), cache.ProvenTo(), "still only C: C2 has not been reached yet")
	require.False(t, cache.Proven(hashes[11]), "height 12, between C and C2, is not proven until C2 matches")

	require.True(t, cache.Fill(hashes[11], 1, full[12:16]), "heights 13 to 16: matches C2 at height 15")
	require.Equal(t, int32(15), cache.ProvenTo())

	for i := 4; i < 15; i++ {
		require.True(t, cache.Proven(hashes[i]), "height %d, at or between C and C2, must be proven once C2 is matched", i+1)
	}

	require.False(t, cache.Proven(hashes[15]), "height 16, above C2, must still not be proven")
}

// Test 10: end to end through blockOrigin — a below-checkpoint block delivered
// after a genuine EXTENDING walk is header-proven, and the streaming sink
// writes .subtree for it, not .subtreeToCheck.
func TestCheckpointWalk_EndToEnd_BelowCheckpointBlockProvenAfterExtendWritesSubtree(t *testing.T) {
	ctx := t.Context()
	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)

	blk := wireBlockWithTxs(t, 20, false)
	pipelineHeaderFixture(t, sm, blk)

	grandparent := chainhash.HashH([]byte("checkpoint-walk-grandparent"))
	parentHeader := wire.BlockHeader{PrevBlock: grandparent, Timestamp: time.Now()}
	parent := parentHeader.BlockHash()
	blk.MsgBlock().Header.PrevBlock = parent

	hash := *blk.Hash()
	sm.chainParams.Checkpoints = []chaincfg.Checkpoint{{Height: 42, Hash: &hash}}
	sm.headerCache = newHeaderCache().WithCheckpoints(sm.chainParams.Checkpoints)

	// First fill: cold start, reaches only the grandparent-to-parent step at
	// height 41 — short of the checkpoint at 42.
	require.True(t, sm.headerCache.Fill(grandparent, 41, []*wire.BlockHeader{&parentHeader}))
	require.False(t, sm.headerCache.Proven(parent), "sanity: nothing is proven before the walk reaches the checkpoint")

	// Second fill: EXTENDS onto the list's own top (the parent header just
	// cached), reaching the checkpoint at blk's own height. The parent/
	// baseHeight arguments below are deliberately whatever Fill needs to
	// dispatch into extendLocked; extendLocked itself never reads them.
	require.True(t, sm.headerCache.Fill(chainhash.Hash{}, 41, []*wire.BlockHeader{&blk.MsgBlock().Header}))
	require.True(t, sm.headerCache.Proven(hash), "the walk must prove blk once the extending fill reaches the checkpoint")

	body := blockBodyBytes(t, blk)

	converted, err := sm.pipelineBlockSink(*blk.Hash(), &blk.MsgBlock().Header, bytes.NewReader(body), int64(len(body)))
	require.NoError(t, err)
	require.True(t, converted)

	got, err := sm.blockPark.ReadConverted(ctx, *blk.Hash())
	require.NoError(t, err)
	require.NotNil(t, got)
	require.NotEmpty(t, got.Subtrees)

	for _, h := range got.Subtrees {
		exists, existsErr := store.Exists(ctx, h[:], fileformat.FileTypeSubtree)
		require.NoError(t, existsErr)
		require.True(t, exists, "a below-checkpoint block proven via the extending walk must write .subtree")

		toCheck, existsErr := store.Exists(ctx, h[:], fileformat.FileTypeSubtreeToCheck)
		require.NoError(t, existsErr)
		require.False(t, toCheck, "it must never ALSO write .subtreeToCheck")
	}
}

// Test 11: maybeRequestMoreHeaders' backstop still retries a below-checkpoint
// walk that is far short of its checkpoint even when the cache already holds
// comfortably more than headerCacheRefillThreshold heights of download
// runway — the download-driven trigger alone would stay silent here, since
// top-best is nowhere near exhausted.
func TestCheckpointWalk_MaybeRequestMoreHeadersRetriesAsABackstopWithRunwayToSpare(t *testing.T) {
	sm := newRaceManager(t)

	tipHash := mockCommittedTip(t, sm, 0, 0)

	client, ok := sm.blockchainClient.(*blockchain2.Mock)
	require.True(t, ok)
	client.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).Unset()
	client.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
		Return([]*chainhash.Hash{{0x99}}, nil)

	_, _, headers := demotionPeer(t, sm, 253, 1000)

	params := chaincfg.RegressionNetParams
	params.Checkpoints = []chaincfg.Checkpoint{{Height: 50_000, Hash: &chainhash.Hash{0x99}}}
	sm.chainParams = &params
	sm.headerCache = newHeaderCache().WithCheckpoints(params.Checkpoints)

	var nonce uint32

	// 2,000 heights above the tip: comfortably over headerCacheRefillThreshold
	// (1,000), which alone would leave the download-driven trigger silent.
	msg, _ := linkedHeaders(tipHash, 2000, &nonce)
	require.True(t, sm.headerCache.Fill(tipHash, 1, msg.Headers))

	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return headers.count() > 0 }, 5*time.Second),
		"a below-checkpoint walk far short of its checkpoint must still be retried even with plenty of download runway already cached")
}
