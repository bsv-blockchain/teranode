package netsync

import (
	"slices"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/stretchr/testify/require"
)

// A block more than one peer owes. Both rules that ask another peer for a block used to skip it:
// the rescue rule returned when a block had more than one owner, and the race skipped a stream
// with no single owner. After one extra copy, the block the chain waited on could wait for the
// peer layer's deadline. Each copy is judged now, as SV Node judges each peer a block is in flight
// from, and a further copy is asked for when every copy is late, up to maxBlockCopies live copies.

// A block with one healthy copy is not raced, whatever its other copy does.
func TestRaceLeavesABlockWhileOneCopyIsHealthy(t *testing.T) {
	r := newStreamRegistry()
	now := time.Now()

	streamAt(r, 1, 1001, newTestPeer(t, "10.0.0.1:8333"), 300_000_000, 1_000_000, now.Add(-40*time.Second))
	streamAt(r, 1, 1001, newTestPeer(t, "10.0.0.2:8333"), 300_000_000, 200_000_000, now.Add(-40*time.Second))

	_, _, _, ok := r.pickRace(now, 1000, 1)
	require.False(t, ok, "the second copy arrives at 5 MB/s")
}

// Two owners each sending the block under 100 KB/s for 40 s: a third peer is asked and both
// stalling owners are dropped. With three owners nobody else is asked.
func TestRaceAsksAThirdPeerWhenEveryCopyStruggles(t *testing.T) {
	sm := assignManager(t, 1, 120)
	sm.streams = newStreamRegistry()
	mockCommittedTip(t, sm, 10, 0)

	a, _ := schedulerPeer(t, sm, 1, 2000)
	b, _ := schedulerPeer(t, sm, 2, 2000)
	_, racerRec := schedulerPeer(t, sm, 3, 2000)

	next := heightHash(t, sm, 11)
	require.True(t, sm.blockDownloads.Add(a, next))
	require.True(t, sm.blockDownloads.Add(b, next))

	now := time.Now()
	sm.streams.markRaced(next, now.Add(-time.Minute))

	for _, p := range []struct {
		s    *blockStream
		read int64
	}{
		{sm.streams.start(next, 11, a, 300_000_000, now.Add(-40*time.Second)), 1_000_000},
		{sm.streams.start(next, 11, b, 300_000_000, now.Add(-40*time.Second)), 2_000_000},
	} {
		p.s.read.Store(p.read)
	}

	sm.maybeRaceSlowBlock(now)

	require.True(t, WaitUntil(func() bool { return racerRec.count() == 1 }, 5*time.Second), "a third peer is asked")
	require.Equal(t, []chainhash.Hash{next}, racerRec.all())
	require.True(t, WaitUntil(func() bool { return !a.Connected() && !b.Connected() }, 5*time.Second), "and both stalling owners are dropped")
}

// At the cap the race asks no fourth peer, but still drops each stalling copy, as SV Node's
// DetectStalling drops a staller whatever its parallel fetch count.
func TestRaceDropsStallingCopiesAtTheCapAndAsksNoFourthPeer(t *testing.T) {
	sm := assignManager(t, 1, 120)
	sm.streams = newStreamRegistry()
	mockCommittedTip(t, sm, 10, 0)

	next := heightHash(t, sm, 11)
	now := time.Now()

	// Three owners, SV Node's DEFAULT_MAX_BLOCK_PARALLEL_FETCH, written out so a changed bound
	// fails here.
	owners := make([]*peerpkg.Peer, 0, 3)

	for i := range 3 {
		p, _ := schedulerPeer(t, sm, uint8(i+1), 2000)
		require.True(t, sm.blockDownloads.Add(p, next))

		s := sm.streams.start(next, 11, p, 300_000_000, now.Add(-40*time.Second))
		s.read.Store(1_000_000)

		owners = append(owners, p)
	}

	_, fourthRec := schedulerPeer(t, sm, 9, 2000)

	sm.maybeRaceSlowBlock(now)

	require.True(t, WaitUntil(func() bool {
		return !owners[0].Connected() && !owners[1].Connected() && !owners[2].Connected()
	}, 5*time.Second), "each stalling copy is dropped at the cap")
	require.False(t, WaitUntil(func() bool { return fourthRec.count() > 0 }, 300*time.Millisecond), "no fourth peer is asked")
}

// Two forgiven owners sending nothing are not copies. The one copy arriving at 50 KB/s is
// raced: its peer is dropped and another peer is asked. The race used to count the forgiven
// owners, find three, and do nothing, and the block waited for the peer layer's deadline.
func TestRaceDoesNotCountForgivenOwnersSendingNothing(t *testing.T) {
	sm := assignManager(t, 1, 120)
	sm.streams = newStreamRegistry()
	mockCommittedTip(t, sm, 10, 0)

	a, _ := schedulerPeer(t, sm, 1, 2000)
	b, _ := schedulerPeer(t, sm, 2, 2000)
	c, _ := schedulerPeer(t, sm, 3, 2000)
	_, racerRec := schedulerPeer(t, sm, 4, 2000)

	next := heightHash(t, sm, 11)
	require.True(t, sm.blockDownloads.Add(a, next))
	require.True(t, sm.blockDownloads.Add(b, next))
	require.Len(t, sm.blockDownloads.ForgiveOwners(next, blockRequestRetryInterval), 2)
	require.True(t, sm.blockDownloads.Add(c, next))
	require.Len(t, sm.blockDownloads.OwnersOf(next), 3)

	now := time.Now()

	s := sm.streams.start(next, 11, c, 300_000_000, now.Add(-40*time.Second))
	s.read.Store(2_000_000)

	sm.maybeRaceSlowBlock(now)

	require.True(t, WaitUntil(func() bool { return racerRec.count() == 1 }, 5*time.Second), "one live copy: another peer is asked")
	require.True(t, WaitUntil(func() bool { return !c.Connected() }, 5*time.Second), "the copy at 50 KB/s is dropped")
	require.True(t, a.Connected())
	require.True(t, b.Connected())
}

// A peer sends its queue in order and cannot drop a request, so a block ahead in its queue counts
// against it while another peer sends a copy of that block. Only a block the peer itself is
// sending counts once, in its pending bytes.
func TestAQueuedBlockArrivingFromAnotherPeerStillCountsAtItsOwner(t *testing.T) {
	sm, owner, fast, _ := reaskSetup(t)
	now := time.Now()

	ahead := heightHash(t, sm, 13)
	next := heightHash(t, sm, 11)

	askAt(t, sm, owner, ahead, now.Add(-2*time.Minute))
	askAt(t, sm, owner, next, now.Add(-2*time.Minute))
	askAt(t, sm, fast, ahead, now.Add(-time.Minute))

	s := sm.streams.start(ahead, 13, fast, reaskTypicalBlock, now)
	s.read.Store(0)

	queue := sm.blockDownloads.Queues()[owner]
	require.Len(t, queue, 2)

	eta := sm.queuedArrival(owner, queue, queue[1].seq, reaskTypicalBlock, reaskTypicalBlock, 3_000_000)
	require.Equal(t, 200*time.Second, eta, "block 13 ahead and block 11 itself, 600 MB at 3 MB/s")
}

// Two forgiven owners sending nothing and one copy arriving at 50 KB/s, with no other peer
// connected. A forgiven owner is not a live copy, so it is the race's racer: it is sent a
// getdata. The race used to skip every owner, find nobody to ask, and leave the block to the peer
// layer's deadline.
//
// The copy at 50 KB/s is the only peer sending the block, so it stays connected. The race used
// to drop it in exchange for a peer already known to be silent: with a peer that takes blocks and
// goes quiet, an honest slow peer was dropped each raceExpiry and a block that needs more than
// that at its rate never arrived. SV Node marks a staller only for a peer with no block in flight
// (net_processing.cpp:5532), and an owner asked again has the block in flight. The second round
// asks the other owner, and the third, at the cap, still drops nobody: both are silent.
func TestRaceAsksAForgivenOwnerWhenNoOtherPeerIsConnected(t *testing.T) {
	sm := assignManager(t, 1, 120)
	sm.streams = newStreamRegistry()
	mockCommittedTip(t, sm, 10, 0)

	a, aRec := schedulerPeer(t, sm, 1, 2000)
	b, bRec := schedulerPeer(t, sm, 2, 2000)
	c, cRec := schedulerPeer(t, sm, 3, 2000)

	next := heightHash(t, sm, 11)
	require.True(t, sm.blockDownloads.Add(a, next))
	require.True(t, sm.blockDownloads.Add(b, next))
	require.Len(t, sm.blockDownloads.ForgiveOwners(next, blockRequestRetryInterval), 2)
	require.True(t, sm.blockDownloads.Add(c, next))

	now := time.Now()

	s := sm.streams.start(next, 11, c, 300_000_000, now.Add(-40*time.Second))
	s.read.Store(2_000_000)

	sm.maybeRaceSlowBlock(now)

	require.True(t, WaitUntil(func() bool { return aRec.count()+bRec.count() == 1 }, 5*time.Second), "one forgiven owner is sent a getdata")
	require.False(t, WaitUntil(func() bool { return !c.Connected() }, 300*time.Millisecond), "the only peer sending the block stays connected")
	require.Zero(t, cRec.count(), "the stalling peer is not asked again")

	asked := a
	if bRec.count() == 1 {
		asked = b
	}

	active, _ := sm.blockDownloads.ActiveOwners(next)
	require.True(t, slices.Contains(active, asked), "the owner asked again owes the block again")

	sm.maybeRaceSlowBlock(now.Add(31 * time.Second))
	require.True(t, WaitUntil(func() bool { return aRec.count() == 1 && bRec.count() == 1 }, 5*time.Second), "the other forgiven owner is asked")

	sm.maybeRaceSlowBlock(now.Add(62 * time.Second))

	asks, _ := sm.streams.raceCost(next, now.Add(62*time.Second))
	require.Equal(t, maxBlockCopies-1, asks, "the third round is at the cap")
	require.False(t, WaitUntil(func() bool { return !c.Connected() }, 300*time.Millisecond), "at the cap, with both owners asked again silent, the copy at 50 KB/s stays connected")
	require.True(t, a.Connected())
	require.True(t, b.Connected())
}

// Once the owners asked again send the block, the race may drop the copies that stall. Each
// round that asks a forgiven owner drops nobody. At the cap both owners asked again are sending,
// each under 100 KB/s, so each stalling copy is dropped, the one at 50 KB/s included.
func TestRaceDropsAStallingCopyOnceTheOwnersAskedAgainSend(t *testing.T) {
	sm := assignManager(t, 1, 120)
	sm.streams = newStreamRegistry()
	mockCommittedTip(t, sm, 10, 0)

	a, aRec := schedulerPeer(t, sm, 1, 2000)
	b, bRec := schedulerPeer(t, sm, 2, 2000)
	c, _ := schedulerPeer(t, sm, 3, 2000)

	next := heightHash(t, sm, 11)
	require.True(t, sm.blockDownloads.Add(a, next))
	require.True(t, sm.blockDownloads.Add(b, next))
	require.Len(t, sm.blockDownloads.ForgiveOwners(next, blockRequestRetryInterval), 2)
	require.True(t, sm.blockDownloads.Add(c, next))

	now := time.Now()

	s := sm.streams.start(next, 11, c, 300_000_000, now.Add(-40*time.Second))
	s.read.Store(2_000_000)

	sm.maybeRaceSlowBlock(now)
	sm.maybeRaceSlowBlock(now.Add(31 * time.Second))

	require.True(t, WaitUntil(func() bool { return aRec.count() == 1 && bRec.count() == 1 }, 5*time.Second), "both forgiven owners are asked")
	require.True(t, c.Connected(), "no round that asks a forgiven owner drops a peer")

	for _, p := range []*peerpkg.Peer{a, b} {
		sp := sm.streams.start(next, 11, p, 300_000_000, now.Add(32*time.Second))
		sp.read.Store(1_000_000)
	}

	sm.maybeRaceSlowBlock(now.Add(70 * time.Second))

	require.True(t, WaitUntil(func() bool { return !c.Connected() }, 5*time.Second), "both owners asked again are sending: the copy at 50 KB/s is dropped")
}
