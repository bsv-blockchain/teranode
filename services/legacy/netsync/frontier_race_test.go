package netsync

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/stretchr/testify/require"
)

// The race is SV Node's rule: one extra request for a block, only when the peer sending it is
// struggling, under 100 KB/s after 30 seconds, and the chain reaches the block before it would
// arrive. The struggling peer is dropped, so the extra copy converts.

func hashN(n byte) chainhash.Hash { return chainhash.Hash{n} }

// streamAt registers a stream that started at start, has received read of total bytes, and
// belongs to owner at height.
func streamAt(r *streamRegistry, n byte, height int32, owner *peerpkg.Peer, total, read int64, start time.Time) *blockStream {
	s := r.start(hashN(n), height, owner, total, start)
	s.read.Store(read)

	return s
}

func TestRaceFiresOnAStrugglingPeer(t *testing.T) {
	r := newStreamRegistry()
	now := time.Now()

	// 2 MB of 300 MB in 40 s: 50 KB/s.
	slow := streamAt(r, 1, 1001, newTestPeer(t, "10.0.0.1:8333"), 300_000_000, 2_000_000, now.Add(-40*time.Second))

	got, c, ok := r.pickRace(now, 1000, 1)
	require.True(t, ok)
	require.Same(t, slow, got)
	require.Less(t, c.rate, float64(raceStallRate))
}

// A peer delivering at a healthy rate is never raced, however large the block. On 2026-09-24 a
// 13 MB/s peer on a 2 GB block was raced three times over.
func TestRaceLeavesAHealthyPeerAlone(t *testing.T) {
	r := newStreamRegistry()
	now := time.Now()

	streamAt(r, 1, 1001, newTestPeer(t, "10.0.0.1:8333"), 2_000_000_000, 520_000_000, now.Add(-40*time.Second))

	_, _, ok := r.pickRace(now, 1000, 1)
	require.False(t, ok, "13 MB/s is not struggling")
}

func TestRaceWaitsThirtySeconds(t *testing.T) {
	r := newStreamRegistry()
	now := time.Now()

	streamAt(r, 1, 1001, newTestPeer(t, "10.0.0.1:8333"), 300_000_000, 0, now.Add(-20*time.Second))

	_, _, ok := r.pickRace(now, 1000, 1)
	require.False(t, ok, "SV Node judges a fetch after 30 s")
}

func TestRaceLeavesABlockTheChainDoesNotNeedYet(t *testing.T) {
	r := newStreamRegistry()
	now := time.Now()

	// 50 KB/s with 1 MB left: 20 s to go, and the chain reaches it in 500 s.
	streamAt(r, 1, 1500, newTestPeer(t, "10.0.0.1:8333"), 3_000_000, 2_000_000, now.Add(-40*time.Second))

	_, _, ok := r.pickRace(now, 1000, 1)
	require.False(t, ok)
}

// A block is raced once. Nothing clears its mark before it expires, not a finished copy and not a
// drained one: clearing it on every finished copy is what let the race fire again and again.
func TestABlockIsRacedOnce(t *testing.T) {
	r := newStreamRegistry()
	now := time.Now()

	first := streamAt(r, 1, 1001, newTestPeer(t, "10.0.0.1:8333"), 300_000_000, 2_000_000, now.Add(-40*time.Second))
	r.markRaced(first.hash, now)
	r.finish(first, now, true)

	streamAt(r, 1, 1001, newTestPeer(t, "10.0.0.2:8333"), 300_000_000, 1_000_000, now.Add(-40*time.Second))

	_, _, ok := r.pickRace(now, 1000, 1)
	require.False(t, ok, "the same block is not raced a second time")

	_, _, ok = r.pickRace(now.Add(raceExpiry+time.Second), 1000, 1)
	require.True(t, ok, "until its mark expires")
}

func TestRacingDropsTheStrugglingPeer(t *testing.T) {
	sm := assignManager(t, 1, 120)
	sm.streams = newStreamRegistry()
	mockCommittedTip(t, sm, 10, 0)

	owner, _ := schedulerPeer(t, sm, 1, 2000)
	_, racerRec := schedulerPeer(t, sm, 2, 2000)

	next, ok := sm.headerCache.At(11)
	require.True(t, ok)
	require.True(t, sm.blockDownloads.Add(owner, next))

	s := sm.streams.start(next, 11, owner, 300_000_000, time.Now().Add(-40*time.Second))
	s.read.Store(1_000_000)

	sm.maybeRaceSlowBlock(time.Now())

	require.True(t, WaitUntil(func() bool { return racerRec.count() == 1 }, 5*time.Second), "one other peer is asked")
	require.Equal(t, []chainhash.Hash{next}, racerRec.all())
	require.True(t, WaitUntil(func() bool { return !owner.Connected() }, 5*time.Second), "and the struggling peer is dropped")

	sm.maybeRaceSlowBlock(time.Now())
	require.Equal(t, 1, racerRec.count(), "and nobody is asked again")
}

func TestFinishedStreamRecordsItsPeersRate(t *testing.T) {
	r := newStreamRegistry()
	now := time.Now()
	p := newTestPeer(t, "10.0.0.1:8333")

	s := streamAt(r, 1, 1001, p, 100_000_000, 100_000_000, now.Add(-10*time.Second))
	r.finish(s, now, true)

	require.InDelta(t, 10_000_000.0, r.peerRate(p), 1)

	// An aborted stream says nothing about the peer's bandwidth.
	s = streamAt(r, 2, 1002, p, 100_000_000, 1_000, now.Add(-10*time.Second))
	r.finish(s, now, false)
	require.InDelta(t, 10_000_000.0, r.peerRate(p), 1)
}

func TestCountingReaderCountsWhatTheSinkReads(t *testing.T) {
	r := newStreamRegistry()
	s := r.start(hashN(1), 1001, nil, 11, time.Now())

	got, err := io.ReadAll(countingReader{r: bytes.NewReader([]byte("hello world")), s: s})
	require.NoError(t, err)
	require.Equal(t, "hello world", string(got))
	require.Equal(t, int64(11), s.read.Load())
}

func TestChooseRacerPrefersTheFastestPeerThatDoesNotOwnTheBlock(t *testing.T) {
	r := newStreamRegistry()
	now := time.Now()

	owner := newTestPeer(t, "10.0.0.1:8333")
	slowOther := newTestPeer(t, "10.0.0.2:8333")
	fastOther := newTestPeer(t, "10.0.0.3:8333")
	unknown := newTestPeer(t, "10.0.0.4:8333")

	for p, bps := range map[*peerpkg.Peer]int64{owner: 50_000_000, slowOther: 2_000_000, fastOther: 30_000_000} {
		s := streamAt(r, 9, 900, p, bps*10, bps*10, now.Add(-10*time.Second))
		r.finish(s, now, true)
	}

	queued := func(*peerpkg.Peer) int { return 0 }

	got := r.chooseRacer([]*peerpkg.Peer{owner, slowOther, fastOther, unknown}, []*peerpkg.Peer{owner}, queued)
	require.Same(t, fastOther, got, "the owner is never its own racer, and a measured fast peer beats an unmeasured one")

	require.Nil(t, r.chooseRacer([]*peerpkg.Peer{owner}, []*peerpkg.Peer{owner}, queued), "no one else to ask")

	// With no measurements at all, the peer with the fewest blocks queued goes first.
	fresh := newStreamRegistry()
	counts := map[*peerpkg.Peer]int{slowOther: 9, fastOther: 3, unknown: 5}
	got = fresh.chooseRacer([]*peerpkg.Peer{slowOther, fastOther, unknown}, nil, func(p *peerpkg.Peer) int { return counts[p] })
	require.Same(t, fastOther, got)
}
