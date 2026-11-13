package netsync

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/stretchr/testify/require"
)

// The rescue rule: the lowest block owed, held at a peer whose queue or rate will make the chain
// wait for it, is asked of one other peer that would deliver it at least twice as soon. The owner
// keeps its request and its connection. A block arriving under 100 KB/s is the race's. One
// arriving faster is judged on its own rate against a fresh copy of the whole block, so a large
// block coming in at a healthy rate is never doubled.

const reaskTypicalBlock = 300_000_000

// reaskSetup builds a manager with the committed tip at 10, a typical block of 300 MB, and two
// peers: a slow owner at 3.5 MB/s and a fast idle peer at 80 MB/s.
func reaskSetup(t *testing.T) (sm *SyncManager, owner, fast *peerpkg.Peer, fastRec *getDataRecorder) {
	t.Helper()

	sm = assignManager(t, 1, 120)
	sm.streams = newStreamRegistry()
	sm.blockPark = &blockPark{}
	sm.commitRate = newCommitRateTracker()
	mockCommittedTip(t, sm, 10, 0)

	for range 4 {
		sm.blockSizeTracker.addBlockSize(reaskTypicalBlock)
	}

	owner, _ = schedulerPeer(t, sm, 1, 2000)
	fast, fastRec = schedulerPeer(t, sm, 2, 2000)

	sm.streams.rates[owner] = 3_500_000
	sm.streams.rates[fast] = 80_000_000

	return sm, owner, fast, fastRec
}

// heightHash is the header cache's hash at height.
func heightHash(t *testing.T, sm *SyncManager, height int32) chainhash.Hash {
	t.Helper()

	h, ok := sm.headerCache.At(height)
	require.True(t, ok)

	return h
}

// askAt records a request of h from p as made at `at`.
func askAt(t *testing.T, sm *SyncManager, p *peerpkg.Peer, h chainhash.Hash, at time.Time) {
	t.Helper()

	prev := sm.blockDownloads.now
	sm.blockDownloads.now = func() time.Time { return at }
	require.True(t, sm.blockDownloads.Add(p, h))
	sm.blockDownloads.now = prev
}

// The case behind the 20-minute wait at 705,725: the block after the tip sits behind other
// blocks at a peer delivering 3.5 MB/s, which is busy, so it is neither struggling on this block
// nor quiet. A fast idle peer is asked for it too, and the owner keeps its request.
func TestRescueAsksAFastPeerForTheBlockBehindASlowQueue(t *testing.T) {
	sm, owner, fast, fastRec := reaskSetup(t)
	now := time.Now()

	// The owner was asked for 13, 14 and 15 first and 11 last, so it sends 11 last. 13 is
	// arriving now with 250 MB to go.
	for _, h := range []int32{13, 14, 15, 11} {
		askAt(t, sm, owner, heightHash(t, sm, h), now.Add(-2*time.Minute))
	}

	s := sm.streams.start(heightHash(t, sm, 13), 13, owner, reaskTypicalBlock, now.Add(-15*time.Second))
	s.read.Store(50_000_000)

	sm.watchOwedBlocks(now)

	next := heightHash(t, sm, 11)
	require.True(t, sm.blockDownloads.HasOwner(fast, next), "the fast peer is asked for the block the chain needs")
	require.True(t, sm.blockDownloads.HasOwner(owner, next), "and the owner keeps its request")
	require.True(t, WaitUntil(func() bool { return fastRec.count() == 1 }, 5*time.Second))
	require.Equal(t, []chainhash.Hash{next}, fastRec.all())
	require.True(t, owner.Connected(), "the owner is not dropped: it is working, just slowly")
}

// The reason this rule never looks at an arriving block. A 4 GB block at 40 MB/s takes 100 s;
// any timer on it alone fired for every large block and downloaded each one twice.
func TestRescueNeverDoublesALargeBlockThatIsArriving(t *testing.T) {
	sm, owner, fast, _ := reaskSetup(t)
	now := time.Now()

	next := heightHash(t, sm, 11)
	askAt(t, sm, owner, next, now.Add(-90*time.Second))

	sm.streams.rates[owner] = 40_000_000
	s := sm.streams.start(next, 11, owner, 4_000_000_000, now.Add(-60*time.Second))
	s.read.Store(2_400_000_000)

	sm.watchOwedBlocks(now)
	sm.maybeRaceSlowBlock(now)

	require.False(t, sm.blockDownloads.HasOwner(fast, next), "a 4 GB block arriving at 40 MB/s is never asked of a second peer")
}

// A block the owner will deliver before the chain reaches it is left alone.
func TestRescueLeavesABlockThatArrivesInTime(t *testing.T) {
	sm, owner, fast, _ := reaskSetup(t)
	now := time.Now()

	// A pace of one block a second: the chain needs block 32 in 21 s. The owner at 30 MB/s
	// delivers it, with one 300 MB block ahead, in 20 s.
	for i := range 64 {
		sm.commitRate.note(now.Add(time.Duration(i-63) * time.Second))
	}

	sm.streams.rates[owner] = 30_000_000

	for _, h := range []int32{33, 32} {
		askAt(t, sm, owner, heightHash(t, sm, h), now.Add(-time.Minute))
	}

	sm.watchOwedBlocks(now)

	require.False(t, sm.blockDownloads.HasOwner(fast, heightHash(t, sm, 32)))
}

// The watcher gives a request watchMinAge (10 s) before it judges it.
func TestRescueWaitsTenSecondsFromTheRequest(t *testing.T) {
	sm, owner, fast, _ := reaskSetup(t)
	now := time.Now()

	for _, h := range []int32{13, 14, 15, 11} {
		askAt(t, sm, owner, heightHash(t, sm, h), now.Add(-5*time.Second))
	}

	sm.watchOwedBlocks(now)

	require.False(t, sm.blockDownloads.HasOwner(fast, heightHash(t, sm, 11)), "5 s after the request is too soon")
}

// Another copy only helps if it comes clearly sooner. A fast peer with a long queue of its own
// would not deliver the block in half the owner's time, so it is not asked.
func TestRescueNeedsAPeerAtLeastTwiceAsFast(t *testing.T) {
	sm, owner, fast, _ := reaskSetup(t)
	now := time.Now()

	// Owner: 1 block ahead, 600 MB at 3.5 MB/s, about 171 s.
	for _, h := range []int32{13, 11} {
		askAt(t, sm, owner, heightHash(t, sm, h), now.Add(-time.Minute))
	}

	// Fast peer: 30 blocks queued, 9.3 GB at 80 MB/s, about 116 s. Sooner, not twice as soon.
	for h := int32(40); h < 70; h++ {
		askAt(t, sm, fast, heightHash(t, sm, h), now.Add(-time.Minute))
	}

	sm.watchOwedBlocks(now)

	require.False(t, sm.blockDownloads.HasOwner(fast, heightHash(t, sm, 11)))
}

// One extra request per block: a block with two owners is not judged again.
func TestRescueAsksOnce(t *testing.T) {
	sm, owner, fast, _ := reaskSetup(t)
	third, _ := schedulerPeer(t, sm, 3, 2000)
	sm.streams.rates[third] = 70_000_000
	now := time.Now()

	for _, h := range []int32{13, 14, 15, 11} {
		askAt(t, sm, owner, heightHash(t, sm, h), now.Add(-2*time.Minute))
	}

	sm.watchOwedBlocks(now)
	require.True(t, sm.blockDownloads.HasOwner(fast, heightHash(t, sm, 11)))

	sm.watchOwedBlocks(now.Add(10 * time.Second))
	require.False(t, sm.blockDownloads.HasOwner(third, heightHash(t, sm, 11)), "the extra copy has its 30 s")

	sm.watchOwedBlocks(now.Add(time.Minute))
	require.False(t, sm.blockDownloads.HasOwner(third, heightHash(t, sm, 11)), "one more getdata at the maximum")
}

// A block owed by two peers, the owner and the peer the re-ask added, used to stream with no single
// owner, and its bytes counted for nobody. On 2026-10-07 the fast peer sending re-asked block
// 707,857, 2 GB in 64 s, therefore looked silent for a minute, and the quiet-owner rule let it off
// all 15 blocks in its queue; it delivered them anyway and each was downloaded twice. The peer
// sending the copy counts as sending, and the other owner, which sends nothing, does not.
func TestAPeerSendingABlockTwoPeersOweIsNotQuiet(t *testing.T) {
	sm, owner, fast, _ := reaskSetup(t)
	now := time.Now()

	block := heightHash(t, sm, 11)
	askAt(t, sm, owner, block, now.Add(-3*time.Minute))
	askAt(t, sm, fast, block, now.Add(-2*time.Minute))

	queued := heightHash(t, sm, 30)
	askAt(t, sm, fast, queued, now.Add(-2*time.Minute))

	var stream *blockStream

	sink := sm.trackBlockStreams(func(chainhash.Hash, *wire.BlockHeader, io.Reader, int64) (bool, error) {
		sm.streams.mu.Lock()
		for s := range sm.streams.active {
			stream = s
		}
		sm.streams.mu.Unlock()

		stream.lastRead.Store(time.Now().UnixNano())

		require.False(t, sm.streams.lastBlockBytes(fast).IsZero(), "the peer sending a block two peers owe is sending")
		require.True(t, sm.ownerStillSending(queued), "so the rest of its queue is not let off")
		require.True(t, sm.streams.lastBlockBytes(owner).IsZero(), "the other owner sends nothing and does not look busy")

		return true, nil
	})

	_, err := sink(block, &wire.BlockHeader{}, peerpkg.NewDeliveryReader(bytes.NewReader(nil), fast), 0)
	require.NoError(t, err)
}

// A block arriving at a steady rate above the race's 100 KB/s floor is still re-asked when it
// will be late and a fresh copy from another peer, started from the first byte, would land in
// half the time. On 2026-10-07 a peer at 1.9 MB/s had 1,083 MB left of a block while peers at 26
// to 38 MB/s carried the rest. The owner keeps its copy; whichever completes first is converted.
func TestRescueAsksForASlowArrivingBlockThatWillBeLate(t *testing.T) {
	sm, owner, fast, _ := reaskSetup(t)
	now := time.Now()

	next := heightHash(t, sm, 11)
	askAt(t, sm, owner, next, now.Add(-2*time.Minute))

	s := sm.streams.start(next, 11, owner, 1_083_000_000, now.Add(-95*time.Second))
	s.read.Store(180_000_000)

	sm.watchOwedBlocks(now)

	require.True(t, sm.blockDownloads.HasOwner(fast, next), "1.9 MB/s with 903 MB left: 475 s, against about 14 s for a fresh copy")
	require.True(t, sm.blockDownloads.HasOwner(owner, next), "and the owner keeps its copy")
	require.True(t, owner.Connected())
}

// A block arriving at a healthy rate is not re-asked when a fresh copy would not land in half
// the time, however late it is.
func TestRescueLeavesAnArrivingBlockAFreshCopyWouldNotBeat(t *testing.T) {
	sm, owner, fast, _ := reaskSetup(t)
	now := time.Now()

	next := heightHash(t, sm, 11)
	askAt(t, sm, owner, next, now.Add(-2*time.Minute))

	// 2 GB at 30 MB/s, 1.05 GB in after 35 s: 32 s to go. A fresh copy at 80 MB/s needs 25 s,
	// not half of 32.
	sm.streams.rates[owner] = 30_000_000
	s := sm.streams.start(next, 11, owner, 2_000_000_000, now.Add(-10*time.Second))
	s.read.Store(1_050_000_000)

	sm.watchOwedBlocks(now.Add(25 * time.Second))

	require.False(t, sm.blockDownloads.HasOwner(fast, next))
}

// A queued block's size is not known, so the helper is the fastest peer that starts before the
// owner and has at least twice its rate: an idle 12 MB/s peer starts first, but for a 2 GB block
// the 50 MB/s peer behind five blocks lands it far sooner.
func TestWatchAsksTheFastestPeerThatStartsBeforeTheOwner(t *testing.T) {
	sm, owner, fast, _ := reaskSetup(t)
	medium, _ := schedulerPeer(t, sm, 3, 2000)
	now := time.Now()

	sm.blockSizeTracker = newBlockSizeTracker(10)
	sm.blockSizeTracker.addBlockSize(2_000_000_000)
	for range 9 {
		sm.blockSizeTracker.addBlockSize(10_000_000)
	}

	sm.streams.rates[owner] = 3.5 * mb
	sm.streams.rates[fast] = 50 * mb
	sm.streams.rates[medium] = 12 * mb

	for _, h := range []int32{13, 14, 15, 11} {
		askAt(t, sm, owner, heightHash(t, sm, h), now.Add(-2*time.Minute))
	}

	for h := int32(40); h < 45; h++ {
		askAt(t, sm, fast, heightHash(t, sm, h), now.Add(-time.Minute))
	}

	sm.watchOwedBlocks(now)

	next := heightHash(t, sm, 11)
	require.True(t, sm.blockDownloads.HasOwner(fast, next))
	require.False(t, sm.blockDownloads.HasOwner(medium, next))
}

// The rescue judges a block with one owner only. A block with two owners has had its one extra
// getdata, from the rescue or the race.
func TestRescueLeavesABlockWithTwoOwners(t *testing.T) {
	sm, owner, fast, _ := reaskSetup(t)
	other, _ := schedulerPeer(t, sm, 3, 2000)
	sm.streams.rates[other] = 3_500_000
	now := time.Now()

	for _, h := range []int32{13, 14, 15, 11} {
		askAt(t, sm, owner, heightHash(t, sm, h), now.Add(-2*time.Minute))
	}

	askAt(t, sm, other, heightHash(t, sm, 11), now.Add(-2*time.Minute))

	sm.watchOwedBlocks(now)

	require.False(t, sm.blockDownloads.HasOwner(fast, heightHash(t, sm, 11)))
}

// An owner with no rate, 30 s after the getdata, has sent nothing: it lands the block very late,
// and does not borrow the median rate, which made it look as fast as the fastest peer.
func TestRescueTreatsAnUnmeasuredOwnerAsLate(t *testing.T) {
	sm, owner, fast, _ := reaskSetup(t)
	delete(sm.streams.rates, owner)
	now := time.Now()

	askAt(t, sm, owner, heightHash(t, sm, 11), now.Add(-time.Minute))

	sm.watchOwedBlocks(now)

	require.True(t, sm.blockDownloads.HasOwner(fast, heightHash(t, sm, 11)))
}

// Review focus 5: a block nobody owes (its owner left) is the download pass's, not the rescue's.
func TestRescueLeavesABlockWithNoOwner(t *testing.T) {
	sm, _, fast, _ := reaskSetup(t)

	sm.watchOwedBlocks(time.Now())

	require.Zero(t, sm.blockDownloads.CountForPeer(fast))
}

// An unmeasured peer is never the rescue's second peer: its arrival is a guess.
func TestRescueNeverAsksAnUnmeasuredPeer(t *testing.T) {
	sm, owner, fast, _ := reaskSetup(t)
	delete(sm.streams.rates, fast)
	now := time.Now()

	for _, h := range []int32{13, 14, 15, 11} {
		askAt(t, sm, owner, heightHash(t, sm, h), now.Add(-2*time.Minute))
	}

	sm.watchOwedBlocks(now)

	require.False(t, sm.blockDownloads.HasOwner(fast, heightHash(t, sm, 11)))
}

// The watcher examines each owed block near the tip, not only the lowest. Block 11 lands in time
// at a fast peer; block 12 waits behind a 3.5 MB/s queue and will stop the chain after block 11.
// The rescue rule examined only block 11.
func TestWatchHelpsALateBlockThatIsNotTheLowest(t *testing.T) {
	sm, owner, fast, _ := reaskSetup(t)
	third, _ := schedulerPeer(t, sm, 3, 2000)
	sm.streams.rates[third] = 80 * mb
	now := time.Now()

	askAt(t, sm, fast, heightHash(t, sm, 11), now.Add(-time.Minute))

	for _, h := range []int32{13, 14, 15, 12} {
		askAt(t, sm, owner, heightHash(t, sm, h), now.Add(-2*time.Minute))
	}

	sm.watchOwedBlocks(now)

	require.False(t, sm.blockDownloads.HasOwner(third, heightHash(t, sm, 11)), "block 11 lands in about 4 s")
	require.True(t, sm.blockDownloads.HasOwner(third, heightHash(t, sm, 12)), "block 12 waits behind about 1.2 GB at 3.5 MB/s")
}

// A block that will land within watchMinETA (30 s) gets no help: the copy would cost more than
// the wait. The owner at 30 MB/s starts block 11 in 10 s and lands it in 20 s; the 80 MB/s peer
// starts first and is more than twice as fast, so only the floor keeps it off.
func TestWatchLeavesABlockThatLandsWithinThirtySeconds(t *testing.T) {
	sm, owner, fast, _ := reaskSetup(t)
	sm.streams.rates[owner] = 30 * mb
	now := time.Now()

	for _, h := range []int32{12, 11} {
		askAt(t, sm, owner, heightHash(t, sm, h), now.Add(-time.Minute))
	}

	sm.watchOwedBlocks(now)

	require.False(t, sm.blockDownloads.HasOwner(fast, heightHash(t, sm, 11)))
}

// A block gets a helper only when it lands watchMinETA or more after each block below it: that is
// the time it adds to the chain's wait. On mainnet at 13:06 on 2026-10-08 the watcher asked 50
// helpers in two minutes, for blocks that landed a few seconds after the block below them, and
// 8% of the bytes received were discarded. Block 11 lands in about 171 s, behind one 300 MB block
// at 3.5 MB/s; block 12 lands in about 185 s, behind one block at 3.24 MB/s, so 14 s later.
func TestWatchLeavesABlockThatLandsSoonAfterTheBlockBelowIt(t *testing.T) {
	sm, owner, fast, _ := reaskSetup(t)
	other, _ := schedulerPeer(t, sm, 3, 2000)
	fast2, _ := schedulerPeer(t, sm, 4, 2000)
	sm.streams.rates[other] = 3_240_000
	sm.streams.rates[fast2] = 80_000_000
	now := time.Now()

	for _, h := range []int32{20, 11} {
		askAt(t, sm, owner, heightHash(t, sm, h), now.Add(-time.Minute))
	}

	for _, h := range []int32{21, 12} {
		askAt(t, sm, other, heightHash(t, sm, h), now.Add(-time.Minute))
	}

	sm.watchOwedBlocks(now)

	require.True(t, sm.blockDownloads.HasOwner(fast, heightHash(t, sm, 11)) || sm.blockDownloads.HasOwner(fast2, heightHash(t, sm, 11)), "block 11 adds about 171 s to the wait")
	require.False(t, sm.blockDownloads.HasOwner(fast, heightHash(t, sm, 12)) || sm.blockDownloads.HasOwner(fast2, heightHash(t, sm, 12)), "block 12 adds about 14 s more")
}

// A copy that has more than half its bytes gets no helper: the helper must send the full block,
// and the owner's copy, nearly complete, is discarded if the helper wins. On mainnet from 14:31 to
// 14:49 on 2026-10-08, 8% of the bytes received were discarded, over the 5% accepted. Here 600 of
// 1,000 MB are in after 200 s at 3 MB/s: 133 s to go, and a fresh copy at 80 MB/s needs 12.5 s.
func TestWatchLeavesACopyThatHasMoreThanHalfItsBytes(t *testing.T) {
	sm, owner, fast, _ := reaskSetup(t)
	now := time.Now()

	next := heightHash(t, sm, 11)
	askAt(t, sm, owner, next, now.Add(-4*time.Minute))

	s := sm.streams.start(next, 11, owner, 1_000_000_000, now.Add(-200*time.Second))
	s.read.Store(600_000_000)

	sm.watchOwedBlocks(now)

	require.False(t, sm.blockDownloads.HasOwner(fast, next))
}

// A queued block's helper must also land it in half the owner's estimated time, as for an
// arriving block. On mainnet from 23:30 on 2026-10-08 the watcher asked helpers that started
// first at twice the rate but had long queues of their own (8m21s against 5m36s), and the share
// of discarded bytes rose from 4.0% to 7.5%. Here the owner at 3.5 MB/s lands block 11 in about
// 514 s, and the 8 MB/s peer, behind 11 blocks, in about 450 s.
func TestWatchAsksNoHelperThatSavesLessThanHalf(t *testing.T) {
	sm, owner, fast, _ := reaskSetup(t)
	sm.streams.rates[fast] = 8_000_000
	now := time.Now()

	for _, h := range []int32{13, 14, 15, 16, 17, 11} {
		askAt(t, sm, owner, heightHash(t, sm, h), now.Add(-2*time.Minute))
	}

	for h := int32(40); h < 51; h++ {
		askAt(t, sm, fast, heightHash(t, sm, h), now.Add(-time.Minute))
	}

	sm.watchOwedBlocks(now)

	require.False(t, sm.blockDownloads.HasOwner(fast, heightHash(t, sm, 11)))
}

// A silent owner's first queued block gets a helper. Its owner has sent nothing, so its estimated
// start is 0 s, and the rule "the helper starts before the owner" refused every helper; with the
// quiet-owner re-ask off below the checkpoint, the block waited for the peer's download timeout,
// about an hour (review of 2026-10-09). The half-time test already proves the gain.
func TestWatchHelpsTheFirstBlockOfASilentOwner(t *testing.T) {
	sm, owner, fast, _ := reaskSetup(t)
	sm.streams.rates[owner] = 10_000
	now := time.Now()

	askAt(t, sm, owner, heightHash(t, sm, 11), now.Add(-10*time.Minute))

	sm.watchOwedBlocks(now)

	require.True(t, sm.blockDownloads.HasOwner(fast, heightHash(t, sm, 11)))
}

// An estimate stays positive and bounded at any rate. A stopped copy counts at 1 byte a second,
// and with about 21 GB to come the conversion to a duration went past the int64 limit and turned
// negative: the stalled peer then looked like the fastest one (review of 2026-10-09).
func TestAnEstimateAtOneBytePerSecondStaysPositive(t *testing.T) {
	sm, owner, _, _ := reaskSetup(t)
	now := time.Now()

	var queue []queuedBlock
	for h := int32(11); h < 32; h++ {
		askAt(t, sm, owner, heightHash(t, sm, h), now.Add(-time.Minute))
	}

	queue = sm.blockDownloads.Queues()[owner]

	eta := sm.queuedArrival(owner, queue, 0, 1_000_000_000, 1_000_000_000, 1)
	require.Positive(t, eta)
	require.LessOrEqual(t, eta, maxEstimate)
	require.Positive(t, eta*rescueFasterBy, "doubling an estimate does not overflow")
}

// A copy past half its bytes that will still take longer than watchPastHalfWait gets a helper.
// The byte test alone left it to arrive however slowly it came, as long as it stayed above the
// race's 100 KB/s. The review's case was 1.1 of 2 GB in at 150 KB/s, 100 minutes to go; here 110
// of 200 MB are in at 150 KB/s, 10 minutes to go, against under 3 s for a fresh copy at 80 MB/s
// (review of 2026-10-09).
func TestWatchHelpsACopyPastHalfThatWillTakeLong(t *testing.T) {
	sm, owner, fast, _ := reaskSetup(t)
	now := time.Now()

	next := heightHash(t, sm, 11)
	askAt(t, sm, owner, next, now.Add(-13*time.Minute))

	s := sm.streams.start(next, 11, owner, 200_000_000, now.Add(-733*time.Second))
	s.read.Store(110_000_000)

	sm.watchOwedBlocks(now)

	require.True(t, sm.blockDownloads.HasOwner(fast, next), "10 minutes to go is past watchPastHalfWait")
	require.True(t, sm.blockDownloads.HasOwner(owner, next), "and the owner keeps its copy")
}

// A block whose only copy is stalled, under the race's 100 KB/s, is the race's, and the chain
// waits on it whatever lands above it. So a block above it is not late against it: here block 11
// arrives at 50 KB/s and block 12, queued at a 3.5 MB/s peer, lands in about 86 s, and a helper
// for 12 would not move the chain (review of 2026-10-09).
func TestWatchHelpsNoBlockAboveAStalledOne(t *testing.T) {
	sm, owner, fast, _ := reaskSetup(t)
	other, _ := schedulerPeer(t, sm, 3, 2000)
	sm.streams.rates[other] = 3_500_000
	now := time.Now()

	stalled := heightHash(t, sm, 11)
	askAt(t, sm, owner, stalled, now.Add(-2*time.Minute))

	s := sm.streams.start(stalled, 11, owner, reaskTypicalBlock, now.Add(-100*time.Second))
	s.read.Store(5_000_000)

	above := heightHash(t, sm, 12)
	askAt(t, sm, other, above, now.Add(-time.Minute))

	sm.watchOwedBlocks(now)

	require.False(t, sm.blockDownloads.HasOwner(fast, stalled), "the stalled block is the race's")
	require.False(t, sm.blockDownloads.HasOwner(fast, above), "the chain waits on block 11 anyway")
}
