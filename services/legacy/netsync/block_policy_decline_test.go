package netsync

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/stretchr/testify/require"
)

func policyDeclineManager(t *testing.T) *SyncManager {
	t.Helper()

	sm := schedulerManager(t)
	sm.recentlyFailedBlocks = expiringmap.New[chainhash.Hash, struct{}](recentlyFailedBlocksTTL)
	t.Cleanup(sm.recentlyFailedBlocks.Stop)

	return sm
}

func requireWrittenOff(t *testing.T, sm *SyncManager, hash chainhash.Hash, want bool, msg string) {
	t.Helper()

	_, failed := sm.recentlyFailedBlocks.Get(hash)
	require.Equal(t, want, failed, msg)
}

// A block whose body the gate declines on this node's own block policy was drained and asked for
// again on every pass that wanted it (review of 76be891d6). One host's declared length is only its
// claim, so one host, however often it declares it, does not stop the block being asked for; two
// different hosts that were asked for the block declaring it does, for recentlyFailedBlocksTTL, as
// the park's policy_declined row would.
func TestNotePolicyDecline_TwoHostsWriteTheBlockOffAndOneDoesNot(t *testing.T) {
	sm := policyDeclineManager(t)

	first, _ := schedulerPeer(t, sm, 1, 2000)
	second, _ := schedulerPeer(t, sm, 2, 2000)

	hash := chainhash.Hash{0x0d}
	require.True(t, sm.blockDownloads.Add(first, hash))
	require.True(t, sm.blockDownloads.Add(second, hash))

	now := time.Now()

	sm.notePolicyDecline(hash, first, now)
	sm.notePolicyDecline(hash, first, now.Add(time.Second))
	requireWrittenOff(t, sm, hash, false, "one host's claim, however often repeated, stops nothing")

	sm.notePolicyDecline(hash, second, now.Add(2*time.Second))
	requireWrittenOff(t, sm, hash, true, "two hosts declaring the block above policy write it off")

	sm.policyDeclinesMu.Lock()
	defer sm.policyDeclinesMu.Unlock()

	require.Empty(t, sm.policyDeclines, "the count is done with once the block is written off")
}

// The gate admits a body for any requested hash whoever sends it, so a peer this node never asked
// for the block could vote, and two connections from one host were two votes (review of
// c1f4e9202). Only owners count, once per host.
func TestNotePolicyDecline_OnlyOwnersCountOncePerHost(t *testing.T) {
	sm := policyDeclineManager(t)

	owner, _ := schedulerPeer(t, sm, 1, 2000)
	sameHost, _ := schedulerPeer(t, sm, 1, 2000)
	stranger, _ := schedulerPeer(t, sm, 3, 2000)

	require.NotSame(t, owner, sameHost)
	require.Equal(t, declineHost(owner), declineHost(sameHost), "harness check: two connections from one host")

	hash := chainhash.Hash{0x0f}
	require.True(t, sm.blockDownloads.Add(owner, hash))
	require.True(t, sm.blockDownloads.Add(sameHost, hash))

	now := time.Now()

	sm.notePolicyDecline(hash, stranger, now)
	sm.notePolicyDecline(hash, owner, now)
	requireWrittenOff(t, sm, hash, false, "a peer never asked for the block has no vote")

	sm.notePolicyDecline(hash, sameHost, now)
	requireWrittenOff(t, sm, hash, false, "a second connection from the same host is the same vote")
}

// Declines further apart than recentlyFailedBlocksTTL do not add up: the count starts again.
func TestNotePolicyDecline_OldDeclinesDoNotCount(t *testing.T) {
	sm := policyDeclineManager(t)

	first, _ := schedulerPeer(t, sm, 1, 2000)
	second, _ := schedulerPeer(t, sm, 2, 2000)

	hash := chainhash.Hash{0x0e}
	require.True(t, sm.blockDownloads.Add(first, hash))
	require.True(t, sm.blockDownloads.Add(second, hash))

	now := time.Now()

	sm.notePolicyDecline(hash, first, now)
	sm.notePolicyDecline(hash, second, now.Add(recentlyFailedBlocksTTL+time.Second))

	requireWrittenOff(t, sm, hash, false, "declines a TTL apart do not add up")
}
