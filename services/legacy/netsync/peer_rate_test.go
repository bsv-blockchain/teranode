package netsync

import (
	"fmt"
	"os"
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

	r.remember(map[string]peerRateRecord{known.Addr(): {BPS: 40_000_000, At: time.Now()}})

	// Relative: the remembered rate is discounted by its age, see the decay test below.
	require.InEpsilon(t, 40_000_000.0, r.peerRate(known), 0.001)
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

	require.InDelta(t, 12_000_000.0, r.rememberedRates(time.Now())[p.Addr()].BPS, 1)
}

func TestPeerRatesRoundTripThroughTheFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), peerRatesFile)
	at := time.Date(2026, 10, 12, 9, 0, 0, 0, time.UTC)
	in := map[string]peerRateRecord{"10.0.0.1:8333": {BPS: 40_000_000, At: at}, "[2001:db8::1]:8333": {BPS: 1_500_000, At: at}}

	require.NoError(t, savePeerRates(path, in))

	out, err := loadPeerRates(path)
	require.NoError(t, err)
	require.Len(t, out, len(in))

	for addr, rec := range in {
		require.InDelta(t, rec.BPS, out[addr].BPS, 0)
		require.True(t, rec.At.Equal(out[addr].At), addr)
	}

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
	r.remember(map[string]peerRateRecord{p.Addr(): {BPS: 40_000_000, At: time.Now()}})

	// Relative, not absolute: the rate is discounted by its age (half-life one hour), so an
	// absolute tolerance of 1 B/s held only if this read came within about 0.13 ms of the
	// record, which a loaded CI runner does not guarantee. 0.1% allows about five seconds.
	require.InEpsilon(t, 40_000_000.0, r.peerRate(p), 0.001)

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

	require.Empty(t, r.rememberedRates(time.Now()))

	out := newTestPeer(t, "10.0.0.7:8333")
	r.rates[out] = 12_000_000

	r.forgetPeer(out)

	require.Contains(t, r.rememberedRates(time.Now()), out.Addr(), "an outbound peer is remembered")
}

// Two saves at the same time each leave a whole file: the stop and the 10-minute save wrote the
// same temporary file (review of 2026-10-09).
func TestTwoSavesAtOnceLeaveAWholeFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), peerRatesFile)
	rates := map[string]peerRateRecord{}
	at := time.Date(2026, 10, 12, 9, 0, 0, 0, time.UTC)

	for i := range 2000 {
		rates[fmt.Sprintf("10.0.%d.%d:8333", i/250, i%250)] = peerRateRecord{BPS: float64(i + 1), At: at}
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
	require.Len(t, got, len(rates))

	for addr, rec := range rates {
		require.InDelta(t, rec.BPS, got[addr].BPS, 0)
	}
}

// The save path leaves out a connected inbound peer, not only forgetPeer. The 10-minute save and
// the stop copied every measured peer, inbound included, so the file grew by one address per
// inbound connection (review of 2026-10-12).
func TestTheSaveLeavesOutAConnectedInboundPeer(t *testing.T) {
	r := newStreamRegistry()
	in := peerpkg.NewInboundPeer(ulogger.TestLogger{}, test.CreateBaseTestSettings(t), &peerpkg.Config{})
	require.True(t, in.Inbound())
	r.rates[in] = 12_000_000

	out := newTestPeer(t, "10.0.0.7:8333")
	r.rates[out] = 9_000_000

	path := filepath.Join(t.TempDir(), peerRatesFile)
	sm := &SyncManager{logger: ulogger.TestLogger{}, streams: r, peerRatesPath: path}
	sm.savePeerRates()

	saved, err := loadPeerRates(path)
	require.NoError(t, err)
	require.Len(t, saved, 1, "only the outbound peer is written")
	require.InDelta(t, 9_000_000.0, saved[out.Addr()].BPS, 1)
}

// Remembered entries older than peerRateMaxAge are dropped on save, from memory as well as the
// file, and the file keeps at most peerRatesMaxEntries, the newest. Entries were never pruned,
// so the file grew across restarts (review of 2026-10-12).
func TestTheRatesFileDropsOldEntriesAndIsBounded(t *testing.T) {
	r := newStreamRegistry()
	now := time.Now()

	r.remember(map[string]peerRateRecord{"10.9.9.9:8333": {BPS: 50_000_000, At: now.Add(-peerRateMaxAge - time.Minute)}})

	fresh := map[string]peerRateRecord{}
	for i := range peerRatesMaxEntries + 10 {
		fresh[fmt.Sprintf("10.1.%d.%d:8333", i/250, i%250)] = peerRateRecord{BPS: 1_000_000, At: now.Add(-time.Duration(i) * time.Second)}
	}

	r.remember(fresh)

	saved := r.rememberedRates(now)

	require.Len(t, saved, peerRatesMaxEntries)
	require.NotContains(t, saved, "10.9.9.9:8333", "an entry older than the max age is dropped")
	require.Contains(t, saved, "10.1.0.0:8333", "the newest entries are kept")
	require.NotContains(t, saved, fmt.Sprintf("10.1.%d.%d:8333", (peerRatesMaxEntries+9)/250, (peerRatesMaxEntries+9)%250), "the oldest entry over the cap is dropped")

	r.mu.Lock()
	inMemory := len(r.remembered)
	r.mu.Unlock()

	require.Equal(t, peerRatesMaxEntries, inMemory, "the memory is pruned too, so it does not grow")
}

// A remembered rate counts for less the older it is. A peer that was fast before a long stop got
// the near blocks at its old speed until decay caught up (review of 2026-10-12).
func TestAnOldRememberedRateIsDiscounted(t *testing.T) {
	r := newStreamRegistry()
	recent := newTestPeer(t, "10.0.0.1:8333")
	old := newTestPeer(t, "10.0.0.2:8333")

	r.remember(map[string]peerRateRecord{
		recent.Addr(): {BPS: 40_000_000, At: time.Now()},
		old.Addr():    {BPS: 40_000_000, At: time.Now().Add(-3 * peerRateHalfLife)},
	})

	require.InDelta(t, 40_000_000.0, r.peerRate(recent), 100_000)
	require.InDelta(t, 5_000_000.0, r.peerRate(old), 100_000, "three half-lives leave an eighth")
}

// A rates file written before rates carried a time still loads, each rate stamped with the
// file's modification time.
func TestARatesFileInTheEarlierFormatStillLoads(t *testing.T) {
	path := filepath.Join(t.TempDir(), peerRatesFile)
	require.NoError(t, os.WriteFile(path, []byte(`{"10.0.0.1:8333":40000000}`), 0o600))

	mtime := time.Now().Add(-2 * time.Hour).Truncate(time.Second)
	require.NoError(t, os.Chtimes(path, mtime, mtime))

	rates, err := loadPeerRates(path)
	require.NoError(t, err)
	require.InDelta(t, 40_000_000.0, rates["10.0.0.1:8333"].BPS, 0)
	require.True(t, mtime.Equal(rates["10.0.0.1:8333"].At))
}
