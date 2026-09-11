package netsync

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// TestFrontierRace_ABusyOwnerMustHaveProvenTheBlock is the fix for the fault that
// dominates the measured dead air.
//
// The busy-owner guard cancels the frontier race whenever a peer that owes the
// stuck block is reading bytes, on the reasoning that a peer part-way through a
// block should not be raced. That reasoning has an unstated premise: that the
// owner has the block. The scheduler breaks it, deliberately, by asking the first
// peer with budget when nobody claims a chain reaching the block. So an owner may
// never have had it, and its traffic is somebody else's blocks.
//
// Measured on Hetzner mainnet on 2026-09-10: the node was dead 39.7% of the day,
// 20 of 27 gaps over two minutes had a frontier published with no headers round
// inside, and 80% of that long-gap time had this guard suppressing the race.
//
// SV Node's guard is the same shape and is sound, because its download run is
// built from the peer's own announced chain, so a busy owner necessarily has the
// block. This restores that premise rather than adding a fourth mechanism.
func TestFrontierRace_ABusyOwnerMustHaveProvenTheBlock(t *testing.T) {
	const frontierHeight = 2

	setUp := func(t *testing.T) (*SyncManager, chainhash.Hash, *peerSyncState) {
		t.Helper()

		sm := newRaceManager(t)
		sm.blockSizeTracker = newBlockSizeTracker(10)

		stuck := chainhash.HashH([]byte("the block everything is waiting for"))

		sm.frontierMu.Lock()
		sm.frontierHash = stuck
		sm.frontierHeight = frontierHeight
		sm.frontierSince = time.Now().Add(-time.Hour)
		sm.frontierRacers = nil
		sm.frontierMu.Unlock()

		// The owner: registered, connected, owes the block, and pulling bytes
		// hard enough to satisfy the rate floor.
		owner, _, _ := connectRacePeer(t, 61, 1000)
		state := registerRacePeer(sm, owner)
		state.assocReadBytesLastTick.Store(0)
		state.assocReadBytes.Store(1 << 30)
		state.throughputTicks.Store(2)
		require.True(t, sm.blockDownloads.Add(owner, stuck), "the owner must owe the block")

		// frontierRaceTarget declines outright with no sync peer, before it ever
		// reaches the owner loop, so the fixture has to elect one.
		sm.storeSyncPeer(owner, &syncPeerState{})

		// A second peer to race to, otherwise there is nobody to pick and the
		// test would pass for the wrong reason.
		other, _, _ := connectRacePeer(t, 62, 1000)
		otherState := registerRacePeer(sm, other)
		otherState.noteProvenClaim(chainhash.Hash{0xbb}, frontierHeight+10)

		return sm, stuck, state
	}

	t.Run("an owner that has proven nothing does not cancel the race", func(t *testing.T) {
		sm, stuck, state := setUp(t)

		require.False(t, state.HasProvenTo(frontierHeight), "precondition: the owner has demonstrated nothing")

		hash, _, target, ok := sm.frontierRaceTarget(time.Now())

		require.True(t, ok,
			"a busy peer that never proved it has this block is not evidence the block is coming, so the race must fire")
		require.Equal(t, stuck, hash)
		require.NotNil(t, target)
	})

	t.Run("an owner that has proven the block still cancels the race", func(t *testing.T) {
		sm, _, state := setUp(t)

		state.noteProvenClaim(chainhash.Hash{0xaa}, frontierHeight)
		require.True(t, state.HasProvenTo(frontierHeight))

		_, _, _, ok := sm.frontierRaceTarget(time.Now())

		require.False(t, ok,
			"a peer that has the block and is mid-transfer is slow rather than stalled, and racing it wastes the bandwidth already spent")
	})

	t.Run("proof below the frontier is not proof of the frontier", func(t *testing.T) {
		sm, _, state := setUp(t)

		state.noteProvenClaim(chainhash.Hash{0xaa}, frontierHeight-1)

		_, _, _, ok := sm.frontierRaceTarget(time.Now())

		require.True(t, ok,
			"a peer proven one block short of the frontier has said nothing about the frontier itself")
	})
}
