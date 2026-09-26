package netsync

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A block is handed to another peer only when the peer that owes it has sent no block bytes for
// the retry window, not merely because the window has passed since the request. With large blocks
// a peer spends minutes sending what was queued ahead of a block, and asking a second peer then
// downloads it twice: at height 705,000 mainnet downloaded 346 blocks twice in 11 hours, one of
// them 447 MB. SV Node never re-asks a block from a peer that is still delivering; it only gives
// up on a peer averaging under 100 KB/s of block data, or after tens of minutes.
func TestABlockAtAPeerStillSendingBlocksIsNotAskedOfAnother(t *testing.T) {
	sm := assignManager(t, 1, 120)
	sm.streams = newStreamRegistry()
	mockCommittedTip(t, sm, 10, 0)

	owner, _ := schedulerPeer(t, sm, 1, 2000)
	schedulerPeer(t, sm, 2, 2000)

	far, ok := sm.headerCache.At(60)
	require.True(t, ok)

	past := time.Now().Add(-2 * blockRequestRetryInterval)
	sm.blockDownloads.now = func() time.Time { return past }
	require.True(t, sm.blockDownloads.Add(owner, far))
	sm.blockDownloads.now = time.Now

	// The owner is part way through sending an earlier block.
	earlierHash, ok := sm.headerCache.At(59)
	require.True(t, ok)

	earlier := sm.streams.start(earlierHash, 59, owner, 400<<20, time.Now().Add(-30*time.Second))
	earlier.read.Store(100 << 20)
	earlier.lastRead.Store(time.Now().UnixNano())

	require.Empty(t, sm.unownedBlocks([]wantedBlock{{height: 60, hash: far}}),
		"its peer is still sending block bytes, so the block is not handed to anyone else")
	require.Equal(t, 1, sm.blockDownloads.CountForPeer(owner), "and the peer is not let off it")

	// The stream stops: no block bytes for longer than the retry window.
	earlier.lastRead.Store(time.Now().Add(-2 * blockRequestRetryInterval).UnixNano())

	require.Len(t, sm.unownedBlocks([]wantedBlock{{height: 60, hash: far}}), 1,
		"once the peer has sent no block bytes past the retry window, the block is asked for again")
}

// Only block bytes count. A peer that has dropped our request still sends pings and
// announcements, and counting those would keep its blocks owed for ever.
func TestLastBlockBytesIgnoresAPeerWithNoBlockTraffic(t *testing.T) {
	r := newStreamRegistry()
	p := newTestPeer(t, "10.0.0.9:8333")

	require.True(t, r.lastBlockBytes(p).IsZero(), "no block from this peer, whatever else it sends")

	s := r.start(hashN(1), 100, p, 10, time.Now().Add(-time.Minute))
	s.read.Store(10)
	r.finish(s, time.Now(), true)

	require.WithinDuration(t, time.Now(), r.lastBlockBytes(p), time.Second, "a finished block counts as its last block bytes")

	r.forgetPeer(p)
	require.True(t, r.lastBlockBytes(p).IsZero(), "a departed peer is forgotten")
}
