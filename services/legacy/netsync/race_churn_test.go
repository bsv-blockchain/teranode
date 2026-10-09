package netsync

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/stretchr/testify/require"
)

// When this node's own sink is slow, every copy arrives under 100 KB/s. The race dropped every
// copy, the disconnect cleared each dropped peer from the ledger, so the live owners never
// reached the cap, and one honest peer was dropped about every 30 to 35 s while the disk was slow.

// slowCopy starts a slow copy of h from p at start, for the race to judge.
func slowCopy(sm *SyncManager, h chainhash.Hash, p *peerpkg.Peer, start time.Time) *blockStream {
	s := sm.streams.start(h, 11, p, 300_000_000, start)
	s.read.Store(1_000_000)

	return s
}

// dropped stands in for what the disconnect does to a dropped peer: its copy stops and the ledger
// lets go of it (handleDonePeerMsg, ClearPeer).
func dropped(sm *SyncManager, s *blockStream, p *peerpkg.Peer, now time.Time) {
	sm.streams.finish(s, now, false)
	sm.blockDownloads.ClearPeer(p)
}

// With a slow sink, a block costs at most one disconnect in raceExpiry, and at most
// maxBlockCopies-1 extra copies in raceExpiry, whoever still owes it.
func TestASlowSinkCostsAtMostOneDisconnectForEachBlockInRaceExpiry(t *testing.T) {
	sm := assignManager(t, 1, 120)
	sm.streams = newStreamRegistry()
	mockCommittedTip(t, sm, 10, 0)

	a, _ := schedulerPeer(t, sm, 1, 2000)
	b, bRec := schedulerPeer(t, sm, 2, 2000)
	c, cRec := schedulerPeer(t, sm, 3, 2000)
	d, dRec := schedulerPeer(t, sm, 4, 2000)

	// The racer is the fastest peer: b, then c, then d.
	sm.streams.rates[b] = 30_000_000
	sm.streams.rates[c] = 20_000_000
	sm.streams.rates[d] = 10_000_000

	next := heightHash(t, sm, 11)
	require.True(t, sm.blockDownloads.Add(a, next))

	now := time.Now()
	sa := slowCopy(sm, next, a, now.Add(-40*time.Second))

	// Round one: a is dropped and b is asked.
	sm.maybeRaceSlowBlock(now)
	require.True(t, WaitUntil(func() bool { return bRec.count() == 1 }, 5*time.Second))
	require.True(t, WaitUntil(func() bool { return !a.Connected() }, 5*time.Second))
	dropped(sm, sa, a, now)

	// Round two, 35 s on: b's copy is as slow. c is asked, and b is kept.
	slowCopy(sm, next, b, now)
	sm.maybeRaceSlowBlock(now.Add(35 * time.Second))
	require.True(t, WaitUntil(func() bool { return cRec.count() == 1 }, 5*time.Second))

	// Round three, 70 s on: c's copy is as slow. Two extra copies were asked in raceExpiry, so d
	// is not asked, and nobody is dropped.
	slowCopy(sm, next, c, now.Add(36*time.Second))
	sm.maybeRaceSlowBlock(now.Add(70 * time.Second))
	require.False(t, WaitUntil(func() bool { return dRec.count() > 0 }, 300*time.Millisecond), "no third extra copy in raceExpiry")
	require.True(t, b.Connected(), "one disconnect for this block in raceExpiry")
	require.True(t, c.Connected(), "one disconnect for this block in raceExpiry")

	// After raceExpiry the block may cost one more disconnect.
	sm.maybeRaceSlowBlock(now.Add(raceExpiry + time.Minute))
	require.True(t, WaitUntil(func() bool { return !b.Connected() && !c.Connected() }, 5*time.Second))

}

// While this node is backpressured (a read loop waits for an admission slot), nobody is dropped:
// a slow copy is then this node's delay.
func TestTheRaceDropsNobodyWhileThisNodeIsBackpressured(t *testing.T) {
	sm := assignManager(t, 1, 120)
	sm.streams = newStreamRegistry()
	mockCommittedTip(t, sm, 10, 0)

	a, _ := schedulerPeer(t, sm, 1, 2000)
	schedulerPeer(t, sm, 2, 2000)
	schedulerPeer(t, sm, 3, 2000)

	next := heightHash(t, sm, 11)
	require.True(t, sm.blockDownloads.Add(a, next))

	now := time.Now()
	slowCopy(sm, next, a, now.Add(-40*time.Second))

	// Another peer may be asked while backpressured: a raced copy goes to a side file and needs
	// no admission slot.
	sm.blockPrefetchWaiters.Add(1)
	require.True(t, sm.localReadBackpressured())

	sm.maybeRaceSlowBlock(now)
	require.False(t, WaitUntil(func() bool { return !a.Connected() }, 300*time.Millisecond), "nobody is dropped while this node is backpressured")

	sm.blockPrefetchWaiters.Add(-1)

	sm.maybeRaceSlowBlock(now.Add(35 * time.Second))
	require.True(t, WaitUntil(func() bool { return !a.Connected() }, 5*time.Second), "dropped when the backpressure is gone")
}
