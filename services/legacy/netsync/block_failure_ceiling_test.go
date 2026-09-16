package netsync

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/stretchr/testify/require"
)

func ceilingManager(t *testing.T, ceiling int) *SyncManager {
	t.Helper()

	sm := newRaceManager(t)
	sm.settings.Legacy.BlockFailureBackoffBase = time.Second
	sm.settings.Legacy.BlockFailureBackoffMaxDuration = 10 * time.Second
	sm.settings.Legacy.BlockFailureAttemptCeiling = ceiling
	sm.blockFailureBackoff = expiringmap.New[chainhash.Hash, *blockFailureState](time.Minute).
		WithMaxSize(blockFailureBackoffMaxTracked)

	t.Cleanup(func() { sm.blockFailureBackoff.Stop() })

	return sm
}

// TestBlockGivenUpOn_StopsAtTheCeiling pins the bound that does not exist today.
// The retry SPACING is capped at 150s but the attempt COUNT is not, so a block
// that can never be accepted is asked for every 150 seconds for ever.
func TestBlockGivenUpOn_StopsAtTheCeiling(t *testing.T) {
	sm := ceilingManager(t, 3)
	hash := chainhash.Hash{0x01}

	for i := 0; i < 3; i++ {
		require.False(t, sm.blockGivenUpOn(hash),
			"attempt %d is inside the ceiling and must still be retried", i+1)
		sm.recordBlockFailureBackoff(hash)
	}

	require.True(t, sm.blockGivenUpOn(hash),
		"past the ceiling the block must be given up on rather than re-requested for ever")
}

func TestBlockGivenUpOn_ACeilingOfZeroNeverGivesUp(t *testing.T) {
	sm := ceilingManager(t, 0)
	hash := chainhash.Hash{0x02}

	for i := 0; i < 50; i++ {
		sm.recordBlockFailureBackoff(hash)
	}

	require.False(t, sm.blockGivenUpOn(hash),
		"zero disables the ceiling, which is the prior behaviour and must stay reachable")
}

func TestBlockGivenUpOn_ASuccessfulBlockStartsFresh(t *testing.T) {
	sm := ceilingManager(t, 3)
	hash := chainhash.Hash{0x03}

	for i := 0; i < 5; i++ {
		sm.recordBlockFailureBackoff(hash)
	}

	require.True(t, sm.blockGivenUpOn(hash))

	// What the success path already does on every commit.
	sm.blockFailureBackoff.Delete(hash)

	require.False(t, sm.blockGivenUpOn(hash),
		"a block that later commits must not stay given up on")
}

func TestBlockGivenUpOn_IsSafeWithNoBackoffMap(t *testing.T) {
	sm := newRaceManager(t)

	require.False(t, sm.blockGivenUpOn(chainhash.Hash{0x04}),
		"with the backoff disabled nothing is ever given up on")
}

// TestFetchHeaderBlocks_NeverAsksAgainForABlockPastItsGivenUpCeiling drives the
// request side rather than calling blockGivenUpOn directly, which is the gap
// review found: ForgiveOwners back-dates the ledger record on the delivery
// side before blockGivenUpOn is ever consulted there, so RequestedWithin
// answers false on the very next pass and nothing stopped unownedBlocks
// handing the block straight back out. A block past the ceiling costs the
// 150 seconds its own backoff window was supposed to buy, then goes back to
// being requested every pass as if it had never failed at all — which is
// worse than the wedge the ceiling exists to prevent, not merely a smaller
// version of it.
func TestFetchHeaderBlocks_NeverAsksAgainForABlockPastItsGivenUpCeiling(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xf7}
	msg, hashes := linkedHeaders(anchor, 4, &nonce)

	sm := schedulerManager(t)
	sm.settings.Legacy.BlockFailureAttemptCeiling = 1
	// Zero, not merely short: the record's nextRetry is then already in the
	// past the instant it is written, so this test asserts the ceiling holds
	// once the backoff window itself has nothing left to say — the exact
	// state review's timeline describes once "roughly three minutes" have
	// passed and the backoff map has moved on but the ceiling has not.
	sm.settings.Legacy.BlockFailureBackoffBase = 0
	sm.settings.Legacy.BlockFailureBackoffMaxDuration = 0
	sm.blockFailureBackoff = expiringmap.New[chainhash.Hash, *blockFailureState](time.Minute).
		WithMaxSize(blockFailureBackoffMaxTracked)
	t.Cleanup(func() { sm.blockFailureBackoff.Stop() })

	syncPeer, rec := schedulerPeer(t, sm, 131, 1000)
	sm.storeSyncPeer(syncPeer, &syncPeerState{})

	seedFetchHeaders(t, sm, syncPeer, anchor, msg)

	// The front header failed once already, past a ceiling of one.
	sm.recordBlockFailureBackoff(hashes[0])
	require.True(t, sm.blockGivenUpOn(hashes[0]), "sanity: past the ceiling")
	require.False(t, sm.blockDownloads.RequestedWithin(hashes[0], blockRequestRetryInterval),
		"sanity: nothing else is holding the block back, so blockGivenUpOn has to be the one doing it")

	sm.fetchHeaderBlocks()

	require.True(t, WaitUntil(func() bool { return rec.count() == len(hashes)-1 }, 5*time.Second),
		"the rest of the run must still be asked for")
	require.NotContains(t, rec.all(), hashes[0],
		"a block given up on must not be requested again in this process")
}
