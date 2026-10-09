package netsync

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// How a peer's rate is measured. A rate used to change only when the peer completed a block, timed
// from the block's first byte, and never fell while the peer sent nothing. One small fast block, or
// a peer that delivered well and then stopped, could hold the top rate, make up most of the
// measured bandwidth and keep every other peer on standby.

// A peer that owes blocks and sends nothing loses its rate: after rateDecayAfter it halves every
// rateDecayHalfLife, until its next completed block.
func TestAQuietPeerThatOwesBlocksLosesItsRate(t *testing.T) {
	sm, owner, fast, _ := reaskSetup(t)
	busy, _ := schedulerPeer(t, sm, 3, 2000)
	sm.streams.rates[busy] = 50_000_000
	now := time.Now()

	// fast was asked for 11 seventy seconds ago and has sent nothing: 60 s past the 10 s grace,
	// two half-lives.
	askAt(t, sm, fast, heightHash(t, sm, 11), now.Add(-70*time.Second))

	// busy was asked as long ago and is sending a block now.
	askAt(t, sm, busy, heightHash(t, sm, 12), now.Add(-70*time.Second))
	s := sm.streams.start(heightHash(t, sm, 12), 12, busy, 300_000_000, now.Add(-5*time.Second))
	s.lastRead.Store(now.Add(-time.Second).UnixNano())

	sm.decayQuietRates(now)

	require.InDelta(t, 20_000_000, sm.streams.peerRate(fast), 1, "80 MB/s halved twice")
	require.InDelta(t, 3_500_000, sm.streams.peerRate(owner), 0, "a peer that owes nothing keeps its rate")
	require.InDelta(t, 50_000_000, sm.streams.peerRate(busy), 0, "a peer sending block bytes keeps its rate")

	sm.decayQuietRates(now.Add(-30 * time.Second))
	require.InDelta(t, 20_000_000, sm.streams.peerRate(fast), 1, "a later, shorter reading never raises the rate back")

	// fast delivers a block at 40 MB/s: its rate is its next sample, weighed against the decayed
	// rate, not against 80 MB/s.
	done := sm.streams.start(heightHash(t, sm, 13), 13, fast, 80_000_000, now)
	done.read.Store(80_000_000)
	sm.streams.finish(done, now.Add(2*time.Second), true)

	require.InDelta(t, 30_000_000, sm.streams.peerRate(fast), 1, "half of 40 and half of 20")
}

// Blocks that take less than minRateSample are pooled into one sample. A 1 MB block read in 5 ms
// does not make a peer 200 MB/s.
func TestSubSecondBlocksArePooledIntoOneRateSample(t *testing.T) {
	r := newStreamRegistry()
	p := newTestPeer(t, "10.0.0.41:8333")
	t0 := time.Now()

	burst := r.start(chainhash.Hash{0x41}, 1, p, 1_000_000, t0)
	burst.read.Store(1_000_000)
	r.finish(burst, t0.Add(5*time.Millisecond), true)
	require.Zero(t, r.peerRate(p), "one 5 ms block is not a measurement")

	at := t0.Add(5 * time.Millisecond)

	for i := range 4 {
		s := r.start(chainhash.Hash{0x42, byte(i)}, 1, p, 1_000_000, at)
		s.read.Store(1_000_000)
		at = at.Add(250 * time.Millisecond)
		r.finish(s, at, true)

		if i < 3 {
			require.Zero(t, r.peerRate(p), "%d ms of delivery is not a measurement yet", 5+250*(i+1))
		}
	}

	require.InDelta(t, 5_000_000/1.005, r.peerRate(p), 1, "five 1 MB blocks in 1.005 s")
	require.False(t, r.lastBlockBytes(p).IsZero(), "each block still counts as the peer delivering")
}

// A block is timed from when its peer could start it: the later of its request and the end of the
// block before it at the same peer, never after its first byte. A peer that took four seconds to
// start a 4 MB block delivered it at 1 MB/s, not at the speed of its last few bytes.
func TestABlockIsTimedFromWhenItsPeerCouldStartIt(t *testing.T) {
	r := newStreamRegistry()
	p := newTestPeer(t, "10.0.0.42:8333")
	t0 := time.Now()

	require.Equal(t, t0.Add(-4*time.Second), r.couldStart(p, t0.Add(-4*time.Second), t0), "no block before it: from the request")
	require.Equal(t, t0, r.couldStart(p, time.Time{}, t0), "no request known: from the first byte")
	require.Equal(t, t0, r.couldStart(p, t0.Add(time.Second), t0), "never after the first byte")

	r.lastBlock[p] = t0.Add(-time.Second)
	require.Equal(t, t0.Add(-time.Second), r.couldStart(p, t0.Add(-4*time.Second), t0), "asked earlier: from the end of the block ahead of it")
	require.Equal(t, t0.Add(-500*time.Millisecond), r.couldStart(p, t0.Add(-500*time.Millisecond), t0), "asked later: from the request")

	delete(r.lastBlock, p)

	s := r.start(chainhash.Hash{0x43}, 1, p, 4_000_000, t0)
	s.from = t0.Add(-4 * time.Second)
	s.read.Store(4_000_000)
	r.finish(s, t0.Add(10*time.Millisecond), true)

	require.InDelta(t, 4_000_000/4.01, r.peerRate(p), 1)
}
