package netsync

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/stretchr/testify/require"
)

// hashFrom builds a distinct, stable hash from a single byte.
func hashFrom(b byte) chainhash.Hash {
	var h chainhash.Hash
	h[0] = b

	return h
}

// Only headers at or below an actually matched checkpoint have provenance,
// even after checkpoint block delivery advances the pending download target.
func TestHeaderNodeProven_DeniesAboveVerifiedCheckpoint(t *testing.T) {
	sm := &SyncManager{
		nextCheckpoint:           &chaincfg.Checkpoint{Height: 33_333},
		verifiedCheckpointHeight: 11_111,
	}

	require.True(t, sm.headerNodeProven(&headerNode{height: 11_111}),
		"the checkpoint height itself is committed by the hash match")
	require.True(t, sm.headerNodeProven(&headerNode{height: 11_110}),
		"heights below the matched checkpoint are committed by linkage to it")
	require.False(t, sm.headerNodeProven(&headerNode{height: 11_112}),
		"a header above the matched checkpoint is committed by nothing and must not be proven")
	require.False(t, sm.headerNodeProven(&headerNode{height: 900_000}),
		"a far-above header must not be proven either")
}

// TestHeaderNodeProven_FailsClosedWithoutCheckpoint — no next checkpoint means
// there is no pinned hash to appeal to, so nothing can be proven.
func TestHeaderNodeProven_FailsClosedWithoutCheckpoint(t *testing.T) {
	sm := &SyncManager{}

	require.False(t, sm.headerNodeProven(&headerNode{height: 1}))
	require.False(t, sm.headerNodeProven(nil))
}

// TestBlockOrigin_SurvivesGlobalMapExpiry is the regression test for the TTL bug
// found in review.
//
// New() builds sm.requestedBlocks with a 60-SECOND expiry, while the per-peer map
// gets 60 MINUTES ("needed for legacy sync/checkpoints"). expiringmap.Get does not
// refresh and reports a miss past expiry, so reading provenance from the global
// map made every block slower than a minute — the common case on mainnet, with
// multi-GB blocks behind a 20-deep FIFO — silently lose its header proof and drop
// to full script validation.
//
// Provenance must come from the same record the unsolicited-block check consults,
// so it lives exactly as long as admission does.
func TestBlockOrigin_SurvivesGlobalMapExpiry(t *testing.T) {
	hash := hashFrom(0x77)

	globalMap := expiringmap.New[chainhash.Hash, blockRequestOrigin](time.Millisecond)
	t.Cleanup(globalMap.Stop)

	perPeerMap := expiringmap.New[chainhash.Hash, blockRequestOrigin](time.Hour)
	t.Cleanup(perPeerMap.Stop)

	sm := &SyncManager{requestedBlocks: globalMap}
	state := &peerSyncState{requestedBlocks: perPeerMap}

	// fetchHeaderBlocks records the proof in both maps.
	globalMap.Set(hash, blockRequestOrigin{headerProven: true})
	perPeerMap.Set(hash, blockRequestOrigin{headerProven: true})

	// Let the short-lived global entry lapse, as it does for any block that takes
	// more than a minute to reach the FIFO consumer.
	require.Eventually(t, func() bool {
		_, ok := globalMap.Get(hash)
		return !ok
	}, time.Second, 5*time.Millisecond, "the global entry must lapse for this test to mean anything")

	require.True(t, sm.blockOrigin(state, hash).headerProven,
		"provenance must survive the global map's short TTL")
}

// TestBlockOrigin_FailsClosed — an unknown hash, or no peer state, yields the
// untrusted zero value rather than a panic.
func TestBlockOrigin_FailsClosed(t *testing.T) {
	perPeerMap := expiringmap.New[chainhash.Hash, blockRequestOrigin](time.Hour)
	t.Cleanup(perPeerMap.Stop)

	sm := &SyncManager{}

	require.False(t, sm.blockOrigin(&peerSyncState{requestedBlocks: perPeerMap}, hashFrom(0x78)).headerProven,
		"a hash that was never requested has no provenance")
	require.False(t, sm.blockOrigin(nil, hashFrom(0x79)).headerProven,
		"no peer state means no provenance")
	require.False(t, sm.blockOrigin(&peerSyncState{}, hashFrom(0x7a)).headerProven,
		"no request map means no provenance")
}

func TestHeaderNodeProven_FailsClosedBeforeMatch(t *testing.T) {
	sm := &SyncManager{nextCheckpoint: &chaincfg.Checkpoint{Height: 11_111}}
	require.False(t, sm.headerNodeProven(&headerNode{height: 1}))
	require.False(t, sm.headerNodeProven(&headerNode{height: 0}))
	require.False(t, sm.headerNodeProven(nil))
}

func TestHeaderProvenance_ResetDiscardsProof(t *testing.T) {
	sm, _, _ := newHeaderProvenanceManager(t)
	hash := hashFrom(0x43)
	sm.nextCheckpoint = &chaincfg.Checkpoint{Height: 33_333, Hash: &hash}
	sm.verifiedCheckpointHeight = 11_111
	sm.resetHeaderState(&hash, 100)
	require.Zero(t, sm.verifiedCheckpointHeight)
	require.Nil(t, sm.startHeader)
	require.False(t, sm.headerNodeProven(&headerNode{height: 100}))
}
