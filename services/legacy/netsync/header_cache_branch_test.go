package netsync

import (
	"runtime"
	"testing"
	"time"
	"unsafe"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/stretchr/testify/require"
)

// These tests pin the header cache's per-peer branches against the wedge the
// single-run cache had (docs/superpowers/specs/2026-10-06-pr1699-round2-rereview.md,
// Major 1): one cached run of fake headers above the committed tip, and every
// honest reply that did not build on its top refused. A reply is capped at
// 2,000 headers (wire.MaxBlockHeadersPerMsg), so an attacker that sent 2,000
// fake headers and then 2,000 more on its own top was out of reach of any honest
// reply from the tip, and a fake run as long as an honest one displaced it.

// wedgeCheckpoints pins the honest chain at 5000 and puts a later checkpoint at
// 9000, so every fill in these tests is below the last checkpoint.
func wedgeCheckpoints(honestHashes []chainhash.Hash) []chaincfg.Checkpoint {
	return []chaincfg.Checkpoint{
		{Height: 5000, Hash: &honestHashes[4999]},
		{Height: 9000, Hash: &chainhash.Hash{0x77}},
	}
}

// saltedRun is forgedRun with a salt in the merkle root, so runs from different
// fake peers are different headers.
func saltedRun(parent chainhash.Hash, count int, salt byte, bits uint32) ([]*wire.BlockHeader, []chainhash.Hash) {
	headers := make([]*wire.BlockHeader, 0, count)
	hashes := make([]chainhash.Hash, 0, count)

	prev := parent

	for i := 0; i < count; i++ {
		header := wire.NewBlockHeader(1, &prev, &chainhash.Hash{0x02, salt}, bits, uint32(i)) //nolint:gosec // a small test nonce
		hash := header.BlockHash()

		headers = append(headers, header)
		hashes = append(hashes, hash)
		prev = hash
	}

	return headers, hashes
}

// The first probe, ported: the attacker sends 2,000 fake headers and then 2,000
// more on its own fake top, so its branch reaches tip+4000. The honest peer
// answers from the tip, 2,000 at a time, and each reply builds on its own
// branch. Its branch reaches the checkpoint at 5000, beats the longer-looking
// fake one, and its blocks become wantable.
func TestHeaderBranches_AFakeRunPastOneReplyDoesNotWedgeTheHonestWalk(t *testing.T) {
	tip := chainhash.Hash{0xaa}
	honest, honestHashes := linkedRun(tip, 6000)
	cache := newHeaderCache().WithCheckpoints(wedgeCheckpoints(honestHashes))

	fake1, fakeHashes1 := forgedRun(tip, 2000)
	require.True(t, cache.FillFrom("attacker", tip, 1, fake1).accepted)

	fake2, _ := forgedRun(fakeHashes1[1999], 2000)
	extended := cache.FillFrom("attacker", tip, 1, fake2)
	require.True(t, extended.accepted, "the attacker extends its own fake top")
	require.True(t, extended.extended)
	require.Equal(t, int32(4000), extended.top)

	first := cache.FillFrom("honest", tip, 1, honest[:2000])
	require.True(t, first.accepted, "an honest reply from the tip is held as the honest peer's own branch")
	require.Equal(t, int32(2000), first.top)

	_, ok := cache.Wantable(1)
	require.False(t, ok, "neither branch has matched a checkpoint, so nothing below it is wantable")

	height, topHash, ok := cache.PeerTop("honest")
	require.True(t, ok)
	require.Equal(t, int32(2000), height)
	require.Equal(t, honestHashes[1999], topHash, "the honest walk continues from its own top, not the active fake one")

	require.True(t, cache.FillFrom("honest", tip, 1, honest[2000:4000]).accepted)
	require.True(t, cache.FillFrom("honest", tip, 1, honest[4000:6000]).accepted)

	require.Equal(t, int32(5000), cache.ProvenTo(), "the honest branch matched the checkpoint and is the one read")

	for _, h := range []int32{1, 2000, 4000, 5000} {
		got, ok := cache.Wantable(h)
		require.True(t, ok, "height %d is proven and wantable", h)
		require.Equal(t, honestHashes[h-1], got)
	}

	_, ok = cache.Wantable(5001)
	require.False(t, ok, "above the matched checkpoint nothing is proven yet")

	top, ok := cache.Top()
	require.True(t, ok)
	require.Equal(t, int32(6000), top)
}

// The second probe, ported: an honest branch is held first, and an attacker
// sends a fake run of the same length from the same tip. Equal work keeps the
// branch already read, SV Node's earliest-received tie-break, so the honest
// branch is not displaced; the fake one is held as the attacker's own and
// changes nothing anyone reads.
func TestHeaderBranches_AnEqualLengthFakeDoesNotDisplaceTheHonestBranch(t *testing.T) {
	tip := chainhash.Hash{0xab}
	honest, honestHashes := linkedRun(tip, 6000)
	cache := newHeaderCache().WithCheckpoints(wedgeCheckpoints(honestHashes))

	require.True(t, cache.FillFrom("honest", tip, 1, honest[:2000]).accepted)

	fake, _ := forgedRun(tip, 2000)
	require.True(t, cache.FillFrom("attacker", tip, 1, fake).accepted, "the attacker's branch is its own")

	for _, h := range []int32{1, 1000, 2000} {
		got, ok := cache.At(h)
		require.True(t, ok)
		require.Equal(t, honestHashes[h-1], got, "height %d still names the honest branch", h)
	}

	require.True(t, cache.FillFrom("honest", tip, 1, honest[2000:]).accepted)
	require.Equal(t, int32(5000), cache.ProvenTo())

	got, ok := cache.Wantable(5000)
	require.True(t, ok)
	require.Equal(t, honestHashes[4999], got)
}

// A fake branch with less work than the honest one never becomes the one read,
// and a proven branch beats an unproven one below its checkpoint even when the
// unproven one declares far more work per header.
func TestHeaderBranches_LessWorkNeverWinsAndProofBeatsWork(t *testing.T) {
	tip := chainhash.Hash{0xac}
	honest, honestHashes := linkedRun(tip, 30)
	cache := newHeaderCache().WithCheckpoints([]chaincfg.Checkpoint{
		{Height: 20, Hash: &honestHashes[19]},
		{Height: 100, Hash: &chainhash.Hash{0x77}},
	})

	require.True(t, cache.FillFrom("honest", tip, 1, honest[:10]).accepted)

	shorter, _ := forgedRun(tip, 9)
	require.True(t, cache.FillFrom("attacker", tip, 1, shorter).accepted)

	got, ok := cache.At(1)
	require.True(t, ok)
	require.Equal(t, honestHashes[0], got, "nine fake headers carry less work than ten honest ones")

	top, ok := cache.Top()
	require.True(t, ok)
	require.Equal(t, int32(10), top)

	// 0x1d00ffff declares about 2^223 times the work of 0x207fffff per
	// header: ten of them outweigh the whole honest branch. There is no
	// proof-of-work ceiling here, so they need not be mined. They arrive
	// before any branch holds the checkpoint, so they are held, and read.
	heavy, heavyHashes := saltedRun(tip, 10, 0x01, 0x1d00ffff)
	require.True(t, cache.FillFrom("heavy", tip, 1, heavy).accepted)

	got, ok = cache.At(1)
	require.True(t, ok)
	require.Equal(t, heavyHashes[0], got, "sanity: before any proof, the most work is read")

	require.True(t, cache.FillFrom("honest", tip, 1, honest[10:25]).accepted)
	require.Equal(t, int32(20), cache.ProvenTo())

	for h := int32(1); h <= 25; h++ {
		got, ok := cache.At(h)
		require.True(t, ok)
		require.Equal(t, honestHashes[h-1], got, "height %d names the proven branch, not the heavier unproven one", h)
	}

	require.Equal(t, int32(20), cache.ProvenTo())
}

// A peer's own branch only moves to a header with at least as much work, SV
// Node's UpdateBlockAvailability: a shorter reply from the same peer is not a
// reason to forget the longer branch it already sent.
func TestHeaderBranches_APeersBranchOnlyMovesToMoreWork(t *testing.T) {
	tip := chainhash.Hash{0xad}
	held, heldHashes := linkedRun(tip, 10)
	cache := newHeaderCache().WithCheckpoints([]chaincfg.Checkpoint{{Height: 100, Hash: &chainhash.Hash{0x77}}})

	require.True(t, cache.FillFrom("peer", tip, 1, held).accepted)

	shorter, _ := forgedRun(tip, 9)
	require.False(t, cache.FillFrom("peer", tip, 1, shorter).accepted, "a reply with less work does not move the peer's branch")

	height, hash, ok := cache.PeerTop("peer")
	require.True(t, ok)
	require.Equal(t, int32(10), height)
	require.Equal(t, heldHashes[9], hash)
	require.Equal(t, 10, cache.heldHeaders(), "and the refused headers are not held")
}

// A batch that reaches a checkpoint height with the wrong hash is refused
// whole, as checkpoint mismatch, a reason that costs the sender its connection.
// Nothing any other peer sent is touched: the single-run cache wiped the whole
// list here, so one peer's lie cost every peer's walk.
func TestHeaderBranches_AContradictionFromOnePeerLeavesTheOtherBranchesIntact(t *testing.T) {
	tip := chainhash.Hash{0xae}
	honest, honestHashes := linkedRun(tip, 25)
	cache := newHeaderCache().WithCheckpoints([]chaincfg.Checkpoint{
		{Height: 20, Hash: &honestHashes[19]},
		{Height: 100, Hash: &chainhash.Hash{0x77}},
	})

	require.True(t, cache.FillFrom("honest", tip, 1, honest[:15]).accepted)

	fake, fakeHashes := forgedRun(tip, 15)
	require.True(t, cache.FillFrom("attacker", tip, 1, fake).accepted, "fifteen fake headers stop short of the checkpoint")

	more, _ := forgedRun(fakeHashes[14], 10)
	result := cache.FillFrom("attacker", tip, 1, more)
	require.False(t, result.accepted)
	require.Equal(t, rejectCheckpointMismatch, result.rejection)
	require.Equal(t, int32(20), result.rejectedHeight)
	require.True(t, result.rejection.disconnects())

	require.True(t, cache.FillFrom("honest", tip, 1, honest[15:]).accepted, "the honest walk goes on after another peer's lie")
	require.Equal(t, int32(20), cache.ProvenTo(), "and reaches the checkpoint")

	for h := int32(1); h <= 25; h++ {
		got, ok := cache.At(h)
		require.True(t, ok)
		require.Equal(t, honestHashes[h-1], got)
	}

	height, _, ok := cache.PeerTop("attacker")
	require.True(t, ok, "the cache itself drops nothing; the caller drops the sender (DropPeer) as it disconnects it")
	require.Equal(t, int32(15), height)

	cache.DropPeer("attacker")

	_, _, ok = cache.PeerTop("attacker")
	require.False(t, ok)
	require.Equal(t, 25, cache.heldHeaders(), "the attacker's headers are released, the honest ones kept")
}

// Two peers sending the same chain share its headers: the tree holds each
// header once, and a peer leaving releases only what no other peer holds.
func TestHeaderBranches_PeersShareHeadersAndADepartureReleasesOnlyItsOwn(t *testing.T) {
	tip := chainhash.Hash{0xaf}
	honest, honestHashes := linkedRun(tip, 30)
	cache := newHeaderCache().WithCheckpoints([]chaincfg.Checkpoint{{Height: 100, Hash: &chainhash.Hash{0x77}}})

	require.True(t, cache.FillFrom("a", tip, 1, honest[:20]).accepted)

	same := cache.FillFrom("b", tip, 1, honest[:20])
	require.True(t, same.accepted, "a peer sending headers already held gets them as its best known")
	require.Zero(t, same.added)
	require.Equal(t, int32(20), same.top)
	require.Equal(t, 20, cache.heldHeaders())

	require.True(t, cache.FillFrom("b", tip, 1, honest[20:]).accepted)
	require.Equal(t, 30, cache.heldHeaders())

	cache.DropPeer("b")
	require.Equal(t, 20, cache.heldHeaders(), "the ten only b held are released")

	top, ok := cache.Top()
	require.True(t, ok)
	require.Equal(t, int32(20), top)

	got, ok := cache.At(20)
	require.True(t, ok)
	require.Equal(t, honestHashes[19], got)
}

// A branch that forked from the committed chain below the tip can never be
// committed, so once a fill reports a tip on another chain it is not read
// again, whatever its work. A branch whose tip the committed tip has reached
// is not read either, and the sweep releases both.
func TestHeaderBranches_ABranchTheTipLeftBehindIsNeverRead(t *testing.T) {
	tip := chainhash.Hash{0xb0}
	honest, honestHashes := linkedRun(tip, 4000)
	fork, _ := forgedRun(tip, 5000)

	cache := newHeaderCache().WithCheckpoints([]chaincfg.Checkpoint{{Height: 9000, Hash: &chainhash.Hash{0x77}}})

	for i := 0; i < len(fork); i += wire.MaxBlockHeadersPerMsg {
		require.True(t, cache.FillFrom("fork", tip, 1, fork[i:min(i+wire.MaxBlockHeadersPerMsg, len(fork))]).accepted)
	}

	require.True(t, cache.FillFrom("honest", tip, 1, honest[:2000]).accepted)

	got, ok := cache.At(1)
	require.True(t, ok)
	require.NotEqual(t, honestHashes[0], got, "sanity: the longer fork is read while both connect to the tip")

	// The committed chain advanced along the honest branch to 2000, and the
	// honest peer's next reply says so.
	require.True(t, cache.FillFrom("honest", honestHashes[1999], 2001, honest[2000:]).accepted)

	for _, h := range []int32{2001, 3000, 4000} {
		got, ok := cache.At(h)
		require.True(t, ok)
		require.Equal(t, honestHashes[h-1], got, "height %d names the branch the tip is on, not the fork with more work", h)
	}

	_, ok = cache.At(2000)
	require.False(t, ok, "the committed tip itself is not named")

	cache.PruneTo(4000, honestHashes[3999])

	_, ok = cache.Top()
	require.False(t, ok, "the tip has reached the honest top, and the fork no longer connects")
	require.Zero(t, cache.heldHeaders(), "and the sweep has released both")
}

// The memory bound: twenty peers each send a distinct branch longer than the
// cap, each branch is held only up to the cap, and the tree never holds more
// than peers times cap. The cap is lowered to 5,000 here so the test stays
// small; the real one is the widest checkpoint gap plus one reply, pinned for
// mainnet and testnet below. The test also measures what a held header costs.
func TestHeaderBranches_MemoryStaysBoundedWithTwentyCappedPeers(t *testing.T) {
	const (
		peers     = 20
		branchCap = 5000
	)

	require.Equal(t, int32(52000), capForCheckpoints(chaincfg.MainNetParams.Checkpoints), "mainnet: the 50,000 gaps from 600000, plus one reply")
	require.Equal(t, int32(102010), capForCheckpoints(chaincfg.TestNetParams.Checkpoints), "testnet: its widest gap, 700000 to 800010, plus one reply")

	tip := chainhash.Hash{0xb1}
	cache := newHeaderCache().WithCheckpoints([]chaincfg.Checkpoint{{Height: 100000, Hash: &chainhash.Hash{0x73}}})
	cache.mu.Lock()
	cache.branchCap = branchCap
	cache.mu.Unlock()

	runs := make([][]*wire.BlockHeader, peers)
	for p := range runs {
		runs[p], _ = saltedRun(tip, 6000, byte(p), 0x207fffff)
	}

	var before, after runtime.MemStats

	runtime.GC()
	runtime.ReadMemStats(&before)

	for p := 0; p < peers; p++ {
		for i := 0; i < len(runs[p]); i += wire.MaxBlockHeadersPerMsg {
			cache.FillFrom(p, tip, 1, runs[p][i:min(i+wire.MaxBlockHeadersPerMsg, len(runs[p]))])
		}

		height, _, ok := cache.PeerTop(p)
		require.True(t, ok)
		require.Equal(t, int32(branchCap), height, "peer %d's branch stops at the cap", p)
	}

	runtime.GC()
	runtime.ReadMemStats(&after)

	// The input runs must outlive the second reading, or their collection is
	// subtracted from what the tree costs.
	runtime.KeepAlive(runs)

	held := cache.heldHeaders()
	require.Equal(t, peers*branchCap, held, "twenty capped branches hold twenty times the cap and no more")

	t.Logf("held %d headers for %d peers: %.0f bytes of heap per held header (headerNode is %d bytes)", held, peers, float64(after.HeapAlloc-before.HeapAlloc)/float64(held), unsafe.Sizeof(headerNode{}))

	for p := 1; p < peers; p++ {
		cache.DropPeer(p)
	}

	require.Equal(t, branchCap, cache.heldHeaders(), "nineteen departures release nineteen branches")
}

// Above the last checkpoint the active branch's blocks are wantable only once
// its chain work reaches SV Node's nMinimumChainWork.
func TestHeaderBranches_TheMinimumChainWorkGatesDownloadsAboveTheLastCheckpoint(t *testing.T) {
	tip := chainhash.Hash{0xb2}
	run, runHashes := linkedRun(tip, 10)

	checkpoints := []chaincfg.Checkpoint{{Height: 0, Hash: &tip}}

	high := newHeaderCache().WithCheckpoints(checkpoints).WithMinimumChainWork(minimumChainWork(&chaincfg.MainNetParams))
	require.True(t, high.FillFrom("p", tip, 1, run).accepted)

	_, ok := high.Wantable(1)
	require.False(t, ok, "ten regtest-difficulty headers are far below mainnet's minimum chain work")

	got, ok := high.At(1)
	require.True(t, ok, "the branch is still named, so the walk can go on")
	require.Equal(t, runHashes[0], got)

	none := newHeaderCache().WithCheckpoints(checkpoints)
	require.True(t, none.FillFrom("p", tip, 1, run).accepted)

	got, ok = none.Wantable(1)
	require.True(t, ok, "with no minimum the same branch is wantable above the last checkpoint")
	require.Equal(t, runHashes[0], got)

	require.Nil(t, minimumChainWork(&chaincfg.RegressionNetParams), "regtest's minimum is zero, so no gate")
}

// The wedge end to end through the manager: the attacker's and the honest
// peer's replies go through fillHeaderCache, the honest peer's continuation
// getheaders leads with its own branch's top (not the active fake one, which it
// cannot place), and once the honest branch matches the checkpoint the wanted
// range names its blocks. The committed chain is a real sqlitememory store
// read through the blockchain client.
func TestHeaderBranches_TheWalkContinuesFromEachPeersOwnTopThroughTheManager(t *testing.T) {
	params := chaincfg.RegressionNetParams
	trunk := newRealTrunk(t, &params, 0, nil)

	sm := newHeaderCacheManager(t)
	sm.blockchainClient = trunk.client

	best, tip, ok := sm.committedTip()
	require.True(t, ok)
	require.Zero(t, best)

	honest, honestHashes := linkedRun(tip, 6000)
	params.Checkpoints = wedgeCheckpoints(honestHashes)
	sm.chainParams = &params
	sm.headerCache = newHeaderCache().WithCheckpoints(params.Checkpoints)

	attacker, _, attackerHeaders := demotionPeer(t, sm, 241, 1000)
	honestPeer, _, honestHeaders := demotionPeer(t, sm, 242, 1000)

	fake1, fakeHashes1 := forgedRun(tip, 2000)
	fake2, _ := forgedRun(fakeHashes1[1999], 2000)

	require.True(t, sm.fillHeaderCache(attacker, headersMsgOf(t, fake1)))
	require.True(t, WaitUntil(func() bool { return attackerHeaders.count() == 1 }, 5*time.Second))
	require.True(t, sm.fillHeaderCache(attacker, headersMsgOf(t, fake2)))

	require.True(t, sm.fillHeaderCache(honestPeer, headersMsgOf(t, honest[:2000])))
	require.True(t, WaitUntil(func() bool { return honestHeaders.count() == 1 }, 5*time.Second),
		"a fill short of the checkpoint continues the walk at once")

	sent := honestHeaders.last()
	require.NotNil(t, sent)
	require.Equal(t, honestHashes[1999], *sent.BlockLocatorHashes[0], "the honest peer is asked to continue its own branch")

	require.Empty(t, sm.wantedBlocksFromCache(best, 100), "nothing is wantable before a checkpoint is matched")

	require.True(t, sm.fillHeaderCache(honestPeer, headersMsgOf(t, honest[2000:4000])))
	require.True(t, WaitUntil(func() bool { return honestHeaders.count() == 2 }, 5*time.Second))
	require.Equal(t, honestHashes[3999], *honestHeaders.last().BlockLocatorHashes[0])

	require.True(t, sm.fillHeaderCache(honestPeer, headersMsgOf(t, honest[4000:])))

	wanted := sm.wantedBlocksFromCache(best, 100)
	require.Len(t, wanted, 100)

	for i, w := range wanted {
		require.Equal(t, honestHashes[i], w.hash, "wanted height %d is the honest block", w.height)
	}

	require.True(t, attacker.Connected(), "an unproven fake below the checkpoint is not provably a lie yet")
}

// A peer that leaves takes its branch with it: handleDonePeerMsg drops it, as
// SV Node forgets a departed peer's best known header.
func TestHeaderBranches_APeerThatLeavesTakesItsBranch(t *testing.T) {
	sm := newHeaderCacheManager(t)

	tip := chainhash.Hash{0xb3}
	run, _ := forgedRun(tip, 10)

	peer, _, _ := demotionPeer(t, sm, 243, 1000)
	require.True(t, sm.headerCache.FillFrom(sm.headerOwner(peer), tip, 1, run).accepted)

	_, _, ok := sm.headerCache.PeerTop(peer)
	require.True(t, ok, "sanity: the branch is keyed by the peer")

	sm.handleDonePeerMsg(peer)

	_, _, ok = sm.headerCache.PeerTop(peer)
	require.False(t, ok)
	require.Zero(t, sm.headerCache.heldHeaders())
}
