package netsync

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

// A copy that waits for an admission slot reads nothing, because this node is not reading it. The
// race clock and the rate decay used to run from the first byte, before that wait, so an honest
// owner read as 0 B/s and was dropped, and its rate decayed, for this node's own backpressure.

// A copy waiting 40 s for admission is not raced, and its owner's rate does not decay.
func TestACopyWaitingForAdmissionIsNotJudged(t *testing.T) {
	sm := assignManager(t, 1, 120)
	sm.streams = newStreamRegistry()
	sm.ctx = context.Background()
	sm.inFlightBlocks = make(map[chainhash.Hash]*inFlightBlock)
	sm.blockPrefetchBudgetSlots = 1
	sm.blockPrefetchBudget = semaphore.NewWeighted(1)
	require.True(t, sm.blockPrefetchBudget.TryAcquire(1), "the one slot is taken: the next copy waits")
	mockCommittedTip(t, sm, 10, 0)

	owner, _ := schedulerPeer(t, sm, 1, 2000)
	sm.streams.rates[owner] = 10_000_000

	next := heightHash(t, sm, 11)
	require.True(t, sm.blockDownloads.Add(owner, next))

	sink := sm.trackBlockStreams(sm.admitPipelineSink(func(_ chainhash.Hash, _ *wire.BlockHeader, r io.Reader, _ int64) (bool, error) {
		_, err := io.Copy(io.Discard, r)

		return true, err
	}))

	done := make(chan error, 1)

	go func() {
		_, err := sink(next, &wire.BlockHeader{}, peerpkg.NewDeliveryReader(bytes.NewReader(make([]byte, 1000)), owner), 300_000_000)
		done <- err
	}()

	require.Eventually(t, func() bool { return sm.blockPrefetchWaiters.Load() == 1 }, 5*time.Second, 5*time.Millisecond)

	later := time.Now().Add(40 * time.Second)

	_, _, _, raced := sm.streams.pickRace(later, 10, 1)
	require.False(t, raced, "a copy waiting for admission is not judged")

	sm.decayQuietRates(later)
	require.InDelta(t, 10_000_000, sm.streams.peerRate(owner), 1, "its owner is not quiet: this node is not reading")

	sm.blockPrefetchBudget.Release(1)
	require.NoError(t, <-done)
}

// The race clock starts when the slot is granted. A copy whose first byte came 60 s ago but which
// was admitted 20 s ago has not had its 30 s.
func TestTheRaceClockStartsAtAdmission(t *testing.T) {
	r := newStreamRegistry()
	now := time.Now()

	s := r.start(chainhash.Hash{0x11}, 11, newTestPeer(t, "10.0.0.1:8333"), 300_000_000, now.Add(-60*time.Second))
	r.awaitAdmission(s)
	r.admit(s, now.Add(-20*time.Second))

	_, _, _, raced := r.pickRace(now, 10, 1)
	require.False(t, raced, "20 s since admission")

	_, _, _, raced = r.pickRace(now.Add(15*time.Second), 10, 1)
	require.True(t, raced, "35 s since admission with no bytes")
}
