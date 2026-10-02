package netsync

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/stretchr/testify/require"
)

// Design B (docs/superpowers/specs/2026-09-24-byte-bounded-block-download-design.md). At
// height 709,000 download is the limit: a 2 GB block took 265 s to arrive and 1.9 s to process.
// The chain can only move at the peers' combined speed, and it loses time only when peers go
// idle. So, with the park on, the read-ahead limit is a byte budget, a peer holds at most two
// large blocks, and the lowest block goes to the fastest peer with room.

const qMB = int64(1) << 20

// budgetManager has the park on, a stream registry, and two peers a and b.
func budgetManager(t *testing.T) (*SyncManager, *peerpkg.Peer, *getDataRecorder, *peerpkg.Peer, *getDataRecorder) {
	t.Helper()

	sm := schedulerManager(t)
	sm.ctx = context.Background()
	sm.blockPark, _ = newTestPark(t, "")
	sm.streams = newStreamRegistry()
	// Two per peer keeps these tests small; the depth itself follows the setting, which
	// TestAPeerHoldsTheConfiguredDepthWhateverTheBlockSize checks.
	sm.settings.Legacy.MaxBlocksInTransitPerPeer = 2

	a, aRec := schedulerPeer(t, sm, 130, 1000)
	sm.storeSyncPeer(a, &syncPeerState{})

	b, bRec := schedulerPeer(t, sm, 131, 1000)

	return sm, a, aRec, b, bRec
}

func recentBlocks(sm *SyncManager, size int64) {
	sm.blockSizeTracker = newBlockSizeTracker(10)
	sm.blockSizeTracker.addBlockSize(size)
}

func requested(recs ...*getDataRecorder) int {
	n := 0
	for _, r := range recs {
		n += r.count()
	}

	return n
}

// Every peer is kept at two requests while the bytes really held ahead of the chain, parked and
// arriving, are under 100 GiB. The budget is a backstop for the disk, not a pacing rule.
func TestEveryPeerHoldsTwoRequestsUnderTheBackstop(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xe1}
	msg, _ := linkedHeaders(anchor, 10, &nonce)

	sm, a, aRec, _, bRec := budgetManager(t)
	recentBlocks(sm, 1<<30)

	// 99 GiB is arriving from a third peer already: under the backstop.
	sm.streams.start(chainhash.Hash{0xe2}, 0, newTestPeer(t, "10.0.0.9:8333"), 99<<30, time.Now())

	seedFetchHeaders(t, sm, a, anchor, msg)
	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return requested(aRec, bRec) == 4 }, 5*time.Second), "both peers are filled")
	require.Equal(t, 2, aRec.count())
	require.Equal(t, 2, bRec.count())
}

// At 100 GiB really held ahead of the chain, nothing further ahead is asked for.
func TestTheBackstopStopsRequestsAtOneHundredGiBHeld(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xe1}
	msg, _ := linkedHeaders(anchor, 10, &nonce)

	sm, a, aRec, _, bRec := budgetManager(t)
	recentBlocks(sm, 1<<30)

	sm.streams.start(chainhash.Hash{0xe2}, 0, newTestPeer(t, "10.0.0.9:8333"), 101<<30, time.Now())

	seedFetchHeaders(t, sm, a, anchor, msg)
	sm.fetchHeaderBlocks()

	require.False(t, WaitUntil(func() bool { return requested(aRec, bRec) > 0 }, 300*time.Millisecond))
}

// A block asked for but not yet arriving holds no bytes, so it does not count, however large the
// recent blocks were. It used to count at the largest recent block: after a 2.3 GB block on
// 2026-09-25 a few unstarted requests filled the budget with bytes that did not exist, and four
// of eight peers sat idle.
func TestAnUnstartedRequestIsNotCountedAsBytes(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xe7}
	msg, _ := linkedHeaders(anchor, 10, &nonce)

	sm, a, aRec, _, bRec := budgetManager(t)
	recentBlocks(sm, 2300*qMB)

	// 12 GiB parked, and five requests owed by another peer that have not started arriving.
	require.True(t, sm.blockPark.AdoptWritten(parkedBlock{hash: chainhash.Hash{0xe8}, prevBlock: chainhash.Hash{0xe9}, size: 300, wireSize: 12 << 30}))

	other := newTestPeer(t, "10.0.0.8:8333")
	for i := byte(0); i < 5; i++ {
		require.True(t, sm.blockDownloads.Add(other, chainhash.Hash{0xf0, i}))
	}

	seedFetchHeaders(t, sm, a, anchor, msg)
	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return requested(aRec, bRec) == 4 }, 5*time.Second), "12 GiB held is under 100 GiB, so both peers are filled")
}

func TestAParkedBlockCountsAtItsWireSize(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xe3}
	msg, _ := linkedHeaders(anchor, 10, &nonce)

	sm, a, aRec, _, bRec := budgetManager(t)
	recentBlocks(sm, 1<<30)

	// Converted, so the park charges its few-hundred-byte record, but the block is 101 GiB.
	require.True(t, sm.blockPark.AdoptWritten(parkedBlock{hash: chainhash.Hash{0xe4}, prevBlock: chainhash.Hash{0xe5}, size: 300, wireSize: 101 << 30}))

	seedFetchHeaders(t, sm, a, anchor, msg)
	sm.fetchHeaderBlocks()

	require.False(t, WaitUntil(func() bool { return requested(aRec, bRec) > 0 }, 300*time.Millisecond),
		"the parked block's 101 GiB counts, not its record")
}

func TestAPeerHoldsAtMostTwoLargeBlocks(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xe6}
	msg, _ := linkedHeaders(anchor, 10, &nonce)

	sm, a, aRec, _, bRec := budgetManager(t)
	recentBlocks(sm, 200*qMB)

	seedFetchHeaders(t, sm, a, anchor, msg)
	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return requested(aRec, bRec) == 4 }, 5*time.Second))
	require.False(t, WaitUntil(func() bool { return requested(aRec, bRec) > 4 }, 300*time.Millisecond))
	require.Equal(t, 2, aRec.count(), "one sending and one queued")
	require.Equal(t, 2, bRec.count())
}

func TestTheLowestBlockGoesToTheFastestPeerWithRoom(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xe7}
	msg, hashes := linkedHeaders(anchor, 4, &nonce)

	sm, a, aRec, b, bRec := budgetManager(t)
	recentBlocks(sm, 200*qMB)

	sm.streams.rates[a] = float64(10 * qMB)
	sm.streams.rates[b] = float64(50 * qMB)

	seedFetchHeaders(t, sm, a, anchor, msg)
	sm.fetchHeaderBlocks()

	// The slower peer runs at a fifth of the speed, so its queue is a fifth of the depth of two,
	// which rounds to the floor of one.
	require.True(t, WaitUntil(func() bool { return requested(aRec, bRec) == 3 }, 5*time.Second))
	require.Equal(t, hashes[0:2], bRec.all(), "the faster peer takes the lowest blocks until it is full")
	require.Equal(t, hashes[2:3], aRec.all())
}

// At 07:35Z on 2026-09-24 three peers sat idle with 7 blocks parked and 5 owed: the pass trimmed
// the wanted range to the number of requests it could make before skipping what was already
// parked or owed, so the trimmed range held nothing to ask for.
func TestIdlePeersAreAskedForBlocksPastThoseAlreadyParkedOrOwed(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xe8}
	msg, hashes := linkedHeaders(anchor, 10, &nonce)

	sm, a, aRec, _, bRec := budgetManager(t)
	recentBlocks(sm, 200*qMB)

	seedFetchHeaders(t, sm, a, anchor, msg)

	// a is full with the first two; the next four are parked.
	require.True(t, sm.blockDownloads.Add(a, hashes[0]))
	require.True(t, sm.blockDownloads.Add(a, hashes[1]))

	for i := 2; i < 6; i++ {
		require.True(t, sm.blockPark.AdoptWritten(parkedBlock{hash: hashes[i], prevBlock: hashes[i-1], size: 300, wireSize: 200 * qMB}))
	}

	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return bRec.count() == 2 }, 5*time.Second), "the idle peer is asked for the next two nobody holds")
	require.Equal(t, hashes[6:8], bRec.all())
	require.Zero(t, aRec.count())
}

// A peer holds at most two blocks whatever their size. Any limit that depends on block size
// fails when small blocks come first: after the 08:29Z restart on 2026-09-24 the first blocks to
// complete were small, the limit rose to 16, and the sync peer was handed ten blocks up to 1 GB
// while five peers sat idle.
func TestAPeerHoldsAtMostTwoBlocksWhateverTheirSize(t *testing.T) {
	for _, size := range []int64{0, 5 * qMB, 50 * qMB} {
		var nonce uint32

		anchor := chainhash.Hash{0xe9, byte(size >> 20)}
		msg, _ := linkedHeaders(anchor, 10, &nonce)

		sm, a, aRec, _, bRec := budgetManager(t)
		if size > 0 {
			recentBlocks(sm, size)
		}

		seedFetchHeaders(t, sm, a, anchor, msg)
		sm.fetchHeaderBlocks()

		require.True(t, WaitUntil(func() bool { return requested(aRec, bRec) == 4 }, 5*time.Second), "size %d", size)
		require.False(t, WaitUntil(func() bool { return requested(aRec, bRec) > 4 }, 300*time.Millisecond), "size %d", size)
		require.Equal(t, 2, aRec.count())
		require.Equal(t, 2, bRec.count())
	}
}

// The largest recent block is taken over the last largestSizeSamples blocks, not the last ten.
// Small blocks finish quickly, so ten of them pushed a 1.8 GB block out of the window within 90
// seconds on 2026-09-24, the per-peer limit jumped to 16, and the node owed 106 blocks.
func TestOneLargeBlockStaysTheLargestThroughARunOfSmallOnes(t *testing.T) {
	bst := newBlockSizeTracker(10)
	bst.addBlockSize(1800 * qMB)

	for i := 0; i < 50; i++ {
		bst.addBlockSize(5 * qMB)
	}

	require.Equal(t, 1800*qMB, bst.largestRecentSize())

	for i := 0; i < largestSizeSamples; i++ {
		bst.addBlockSize(5 * qMB)
	}

	require.Equal(t, 5*qMB, bst.largestRecentSize(), "until it is further back than the window")
}

// The budget bounds how far ahead the node reaches, never the filling of a gap below blocks it
// already holds. On 2026-09-24 the park held about 42 blocks counted at 1 GB each after a restart,
// bytes ahead read 45 GB against the 20 GiB budget, and nothing was requested: not even the one
// missing block just above the tip that would have let the park drain. The chain stopped.
func TestTheBudgetNeverStopsAGapBelowParkedBlocksBeingFilled(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xea}
	msg, hashes := linkedHeaders(anchor, 10, &nonce)

	sm, a, aRec, _, bRec := budgetManager(t)
	recentBlocks(sm, 1<<30)

	seedFetchHeaders(t, sm, a, anchor, msg)

	// Everything from the second block to the sixth is parked, far over the budget.
	for i := 1; i < 6; i++ {
		require.True(t, sm.blockPark.AdoptWritten(parkedBlock{hash: hashes[i], prevBlock: hashes[i-1], size: 300, wireSize: 21 << 30}))
	}

	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return requested(aRec, bRec) == 1 }, 5*time.Second), "the missing block is asked for")
	require.False(t, WaitUntil(func() bool { return requested(aRec, bRec) > 1 }, 300*time.Millisecond), "and nothing past what is parked")
	require.Equal(t, []chainhash.Hash{hashes[0]}, append(aRec.all(), bRec.all()...))
}

// A peer is refilled the moment its block arrives, not at the next commit. While download is the
// limit blocks arrive out of order and park behind a missing one, and nothing asked the delivering
// peer for more until a later commit or the 30-second sweep: on 2026-09-24 four of eight peers
// were idle in some reports with the byte budget half used.
func TestAPeerIsRefilledWhenItsBlockArrivesAndParks(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xeb}
	msg, hashes := linkedHeaders(anchor, 10, &nonce)

	sm, a, aRec, _, _ := budgetManager(t)
	recentBlocks(sm, 200*qMB)
	sm.settings.Legacy.MultiPeerBlockDownload = true

	seedFetchHeaders(t, sm, a, anchor, msg)

	// a is asked for the second block, which will park: the first is still missing.
	require.True(t, sm.blockDownloads.Add(a, hashes[1]))

	body := peerpkg.BlockBody{Hash: hashes[1], Size: 200 * qMB, TxCount: 1, Converted: false}
	body.Header.PrevBlock = hashes[0]

	sm.handleBlockOnDiskMsg(&blockOnDiskMsg{body: body, peer: a})

	require.True(t, WaitUntil(func() bool { return aRec.count() > 0 }, 5*time.Second), "the delivering peer is asked for more straight away")
}

// A block recovered from the park after a restart counts at its real size, not a guess. Its
// converted record carries the size the block had on the wire. Counting each at the largest recent
// block made about 28 recovered blocks read as 61.7 GB after a restart on 2026-09-25, far over the
// backstop, and three of four peers sat idle until the park drained.
func TestARecoveredBlockCountsAtTheSizeItsRecordCarries(t *testing.T) {
	park, _ := newTestPark(t, "")

	prev := chainhash.Hash{0x51}
	header := &model.BlockHeader{Version: 1, HashPrevBlock: &prev, HashMerkleRoot: &chainhash.Hash{}}
	record := &model.Block{Header: header, SizeInBytes: 300 * uint64(qMB), Height: 753000}

	require.True(t, park.adoptRecord(chainhash.Hash{0x52}, record, 400, time.Now()))

	require.Equal(t, 300*qMB, park.aheadBytes(2<<30), "the record's own size, not the 2 GiB guess")
}

// A peer is asked for legacy_maxBlocksInTransitPerPeer blocks, 16 by default as SV Node's
// MAX_BLOCKS_IN_TRANSIT_PER_PEER, whatever the size of the recent blocks. It was fixed at 2, which
// left a peer idle for a round trip after every pair of small blocks.
func TestAPeerHoldsTheConfiguredDepthWhateverTheBlockSize(t *testing.T) {
	for _, size := range []int64{200 << 10, 2 << 30} {
		var nonce uint32

		anchor := chainhash.Hash{0xec, byte(size >> 20)}
		msg, _ := linkedHeaders(anchor, 40, &nonce)

		sm, a, aRec, b, bRec := budgetManager(t)
		sm.settings.Legacy.MaxBlocksInTransitPerPeer = 16
		recentBlocks(sm, size)

		sm.streams.rates[a] = float64(20 * qMB)
		sm.streams.rates[b] = float64(20 * qMB)

		seedFetchHeaders(t, sm, a, anchor, msg)
		sm.fetchHeaderBlocks()

		require.True(t, WaitUntil(func() bool { return requested(aRec, bRec) == 32 }, 5*time.Second), "size %d", size)
		require.Equal(t, 16, aRec.count(), "size %d", size)
		require.Equal(t, 16, bRec.count(), "size %d", size)
	}
}

// A peer's queue is its share of the depth by speed: 16 times its rate over the fastest peer's,
// never below one. A peer sends its queue in order, so a slow peer with a full queue buries blocks
// the chain will soon need: on 2026-09-25 blocks 755,236 and 755,244 started 11 and 22 minutes
// after they were asked for, behind multi-GB blocks at peers delivering 5 to 10 MB/s, while
// peers at 50 MB/s had fetched 800 blocks further ahead.
func TestASlowPeersQueueIsShorterInProportionToItsSpeed(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xed}
	msg, _ := linkedHeaders(anchor, 40, &nonce)

	sm, a, aRec, b, bRec := budgetManager(t)
	sm.settings.Legacy.MaxBlocksInTransitPerPeer = 16
	recentBlocks(sm, 200*qMB)

	sm.streams.rates[a] = float64(5 * qMB)
	sm.streams.rates[b] = float64(50 * qMB)

	seedFetchHeaders(t, sm, a, anchor, msg)
	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return requested(aRec, bRec) == 18 }, 5*time.Second))
	require.False(t, WaitUntil(func() bool { return requested(aRec, bRec) > 18 }, 300*time.Millisecond))
	require.Equal(t, 16, bRec.count(), "the fastest peer keeps the full depth")
	require.Equal(t, 2, aRec.count(), "a tenth of the speed, a tenth of the depth, rounded")
}

func TestSpeedScaledDepth(t *testing.T) {
	require.Equal(t, 16, speedScaledDepth(16, 50, 50))
	require.Equal(t, 2, speedScaledDepth(16, 5, 50))
	require.Equal(t, 1, speedScaledDepth(16, 1, 50), "never below one")
	require.Equal(t, 16, speedScaledDepth(16, 5, 0), "no measured rates, full depth")
	require.Equal(t, 16, speedScaledDepth(16, 0, 50), "an unmeasured peer is scaled by the caller's fallback, not here")
}

// A peer whose speed is not yet measured, as every peer is straight after a restart, gets at most
// two blocks until it has delivered one. On 2026-09-25 after a restart every peer was given 16
// before any speed was known, and peers at 4 MB/s held 13 blocks against a speed-scaled depth of
// two or three.
func TestAnUnmeasuredPeerGetsAtMostTwoBlocks(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xee}
	msg, _ := linkedHeaders(anchor, 40, &nonce)

	sm, a, aRec, b, bRec := budgetManager(t)
	sm.settings.Legacy.MaxBlocksInTransitPerPeer = 16
	recentBlocks(sm, 200*qMB)

	// b is measured and fast; a has not delivered anything yet.
	sm.streams.rates[b] = float64(50 * qMB)

	seedFetchHeaders(t, sm, a, anchor, msg)
	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return requested(aRec, bRec) == 18 }, 5*time.Second))
	require.False(t, WaitUntil(func() bool { return requested(aRec, bRec) > 18 }, 300*time.Millisecond))
	require.Equal(t, 16, bRec.count())
	require.Equal(t, 2, aRec.count(), "unmeasured, so two until its speed is known")
}

// Straight after a restart no peer is measured, so every peer starts at two.
func TestEveryPeerStartsAtTwoWhenNoneIsMeasured(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xef}
	msg, _ := linkedHeaders(anchor, 40, &nonce)

	sm, a, aRec, _, bRec := budgetManager(t)
	sm.settings.Legacy.MaxBlocksInTransitPerPeer = 16
	recentBlocks(sm, 200*qMB)

	seedFetchHeaders(t, sm, a, anchor, msg)
	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return requested(aRec, bRec) == 4 }, 5*time.Second))
	require.False(t, WaitUntil(func() bool { return requested(aRec, bRec) > 4 }, 300*time.Millisecond))
}
