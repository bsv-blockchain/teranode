package netsync

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// A peer's rate follows what it sends now: 30 s into a transfer, the rate of the last 30 s
// replaces a stored rate, high or low. A 4 GB block at 2 MB/s used to leave the peer unmeasured,
// or measured at an old speed, for half an hour.
func TestPeerRateFollowsTheLast30SecondsOfACopy(t *testing.T) {
	r := newStreamRegistry()
	p := newTestPeer(t, "10.0.0.1:8333")
	r.rates[p] = 50_000_000

	now := time.Now()
	s := streamAt(r, 1, 1001, p, 4_000_000_000, 0, now.Add(-40*time.Second))

	// 2 MB/s for 40 s, sampled every 5 s.
	for i := 0; i <= 8; i++ {
		s.read.Store(int64(i) * 10_000_000)
		r.sampleStreams(now.Add(time.Duration(i-8) * 5 * time.Second))
	}

	require.InDelta(t, 2_000_000, r.peerRate(p), 200_000)
}

// A copy that sends nothing for 30 s gives its peer a very low rate, not no rate: no rate reads
// as unmeasured, and an unmeasured peer is treated as a peer to try.
func TestAPeerThatStopsSendingStaysMeasured(t *testing.T) {
	r := newStreamRegistry()
	p := newTestPeer(t, "10.0.0.1:8333")
	r.rates[p] = 50_000_000

	now := time.Now()
	s := streamAt(r, 1, 1001, p, 4_000_000_000, 0, now.Add(-40*time.Second))
	s.read.Store(1_000_000)

	for i := 0; i <= 8; i++ {
		r.sampleStreams(now.Add(time.Duration(i-8) * 5 * time.Second))
	}

	require.Positive(t, r.peerRate(p))
	require.Less(t, r.peerRate(p), float64(raceStallRate))
}

// After a restart a known peer starts with the rate it had, by address. A peer the file does not
// name has no rate.
func TestARememberedRateIsUsedUntilThePeerIsMeasured(t *testing.T) {
	r := newStreamRegistry()
	known := newTestPeer(t, "10.0.0.1:8333")
	unknown := newTestPeer(t, "10.0.0.2:8333")

	r.remember(map[string]float64{known.Addr(): 40_000_000})

	require.InDelta(t, 40_000_000.0, r.peerRate(known), 1)
	require.Zero(t, r.peerRate(unknown))

	r.rates[known] = 3_000_000
	require.InDelta(t, 3_000_000.0, r.peerRate(known), 1, "a measurement replaces the remembered rate")
}

// A peer that disconnects keeps its rate in the memory, so the rates file names it after a
// restart.
func TestAForgottenPeerIsRemembered(t *testing.T) {
	r := newStreamRegistry()
	p := newTestPeer(t, "10.0.0.1:8333")
	r.rates[p] = 12_000_000

	r.forgetPeer(p)

	require.InDelta(t, 12_000_000.0, r.rememberedRates()[p.Addr()], 1)
}

func TestPeerRatesRoundTripThroughTheFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), peerRatesFile)
	in := map[string]float64{"10.0.0.1:8333": 40_000_000, "[2001:db8::1]:8333": 1_500_000}

	require.NoError(t, savePeerRates(path, in))

	out, err := loadPeerRates(path)
	require.NoError(t, err)
	require.Equal(t, in, out)

	missing, err := loadPeerRates(filepath.Join(t.TempDir(), "absent.json"))
	require.NoError(t, err, "no file is an empty memory, not an error")
	require.Empty(t, missing)
}

// A rate from the rates file decays like a measured rate while the peer owes blocks and sends
// nothing. It did not, and a silent remembered peer held 16 near blocks until the getdata deadline
// (review of 2026-10-08).
func TestARememberedRateDecaysWhileThePeerIsSilent(t *testing.T) {
	r := newStreamRegistry()
	p := newTestPeer(t, "10.0.0.1:8333")
	r.remember(map[string]float64{p.Addr(): 40_000_000})

	require.InDelta(t, 40_000_000.0, r.peerRate(p), 1)

	now := time.Now()
	r.decayQuiet(now, map[*peerpkg.Peer]time.Time{p: now.Add(-30 * time.Minute)})

	require.Less(t, r.peerRate(p), float64(raceStallRate))
}

// An inbound peer's rate is not remembered: it connects from a different port each time, so its
// address never matches again, and each entry stayed in the rates file for good (review of
// 2026-10-09).
func TestAnInboundPeerIsNotRemembered(t *testing.T) {
	r := newStreamRegistry()
	p := peerpkg.NewInboundPeer(ulogger.TestLogger{}, test.CreateBaseTestSettings(t), &peerpkg.Config{})
	require.True(t, p.Inbound())
	r.rates[p] = 12_000_000

	r.forgetPeer(p)

	require.Empty(t, r.rememberedRates())

	out := newTestPeer(t, "10.0.0.7:8333")
	r.rates[out] = 12_000_000

	r.forgetPeer(out)

	require.Contains(t, r.rememberedRates(), out.Addr(), "an outbound peer is remembered")
}

// Two saves at the same time each leave a whole file: the stop and the 10-minute save wrote the
// same temporary file (review of 2026-10-09).
func TestTwoSavesAtOnceLeaveAWholeFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), peerRatesFile)
	rates := map[string]float64{}

	for i := range 2000 {
		rates[fmt.Sprintf("10.0.%d.%d:8333", i/250, i%250)] = float64(i + 1)
	}

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)

		go func() {
			defer wg.Done()
			require.NoError(t, savePeerRates(path, rates))
		}()
	}

	wg.Wait()

	got, err := loadPeerRates(path)
	require.NoError(t, err)
	require.Equal(t, rates, got)
}
