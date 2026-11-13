package netsync

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/stretchr/testify/require"
)

// A copy was judged on its lifetime average. A burst then a stall stayed above 100 KB/s for
// hours, and a trickle of one byte every 10 s kept its owner from the rate decay. SV Node judges
// a peer on its bandwidth over a recent window (net/stream.cpp:312-346).

// 30 MB in the first 10 s, then nothing: 300 KB/s on average after 100 s, 0 B/s over the last 30 s.
func TestACopyThatBurstsThenStallsIsRaced(t *testing.T) {
	r := newStreamRegistry()
	start := time.Now()

	s := r.start(chainhash.Hash{0x21}, 11, newTestPeer(t, "10.0.0.1:8333"), 300_000_000, start)

	for sec := 0; sec <= 100; sec += 5 {
		if sec <= 10 {
			s.read.Store(int64(sec) * 3_000_000)
		}

		r.sampleStreams(start.Add(time.Duration(sec) * time.Second))
	}

	now := start.Add(100 * time.Second)
	require.GreaterOrEqual(t, s.rate(now), float64(raceStallRate), "the lifetime average")

	_, _, _, raced := r.pickRace(now, 10, 1)
	require.True(t, raced, "no bytes in the last 30 s")
}

// 30 MB in the first 10 s, then one byte every 10 s: the bytes keep coming, but the copy is
// stalling, and its owner's rate decays.
func TestACopyThatTricklesIsRacedAndItsOwnerDecays(t *testing.T) {
	r := newStreamRegistry()
	start := time.Now()
	p := newTestPeer(t, "10.0.0.2:8333")
	r.rates[p] = 10_000_000

	s := r.start(chainhash.Hash{0x22}, 11, p, 300_000_000, start)

	read := int64(0)

	for sec := 0; sec <= 100; sec += 5 {
		at := start.Add(time.Duration(sec) * time.Second)

		switch {
		case sec <= 10:
			read = int64(sec) * 3_000_000
		case sec%10 == 0:
			read++
		}

		s.read.Store(read)
		s.lastRead.Store(at.UnixNano())
		r.sampleStreams(at)
	}

	now := start.Add(100 * time.Second)

	_, _, _, raced := r.pickRace(now, 10, 1)
	require.True(t, raced, "one byte every 10 s is a stall")

	r.decayQuiet(now, map[*peerpkg.Peer]time.Time{p: start})
	require.Less(t, r.peerRate(p), float64(5_000_000), "the trickle does not keep the owner from the decay")
}

// A 4 GB copy that has sent one byte in 100 s arrives at 0.01 B/s: 4e11 s to come, which as
// nanoseconds is past the int64 limit. Unclamped, the estimate turned negative, eta <= need held,
// and the race skipped the staller it exists for.
func TestACopyTricklingUnderOneByteASecondIsRaced(t *testing.T) {
	r := newStreamRegistry()
	start := time.Now()

	s := r.start(chainhash.Hash{0x23}, 11, newTestPeer(t, "10.0.0.3:8333"), 4_000_000_000, start)
	s.read.Store(1)

	now := start.Add(100 * time.Second)
	require.Greater(t, s.rate(now), float64(0), "the copy has a rate")
	require.Less(t, s.rate(now), float64(1), "under one byte a second")

	_, c, _, raced := r.pickRace(now, 10, 1)
	require.True(t, raced, "a copy at 0.01 B/s is a staller")
	// The conversion of an out-of-range float is implementation-defined: on amd64 it wraps to
	// MinInt64, on arm64 it saturates. Bounded by maxEstimate, it is the same on both.
	require.Positive(t, c.eta, "the estimate does not wrap negative")
	require.LessOrEqual(t, c.eta, maxEstimate, "the estimate is bounded")
}
