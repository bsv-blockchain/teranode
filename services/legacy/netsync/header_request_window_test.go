package netsync

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/stretchr/testify/require"
)

// windowCheckpoint is the one checkpoint windowManager's chain has: mainnet's
// block 11000, which its committed chain already holds.
const windowCheckpoint = 11000

// windowManager is headerRuleManager's setup over a chain that has already
// committed its last checkpoint while headers-first mode is still on: the
// window between the commit that reaches the last checkpoint and the
// parkedBlockCommitted call that leaves the mode (block_park_drain.go). The
// chain is mainnet's real blocks 1 to 11000 under mainnet's rules, with one
// checkpoint pinned at 11000, so the honest headers from 11001 to 11111 are
// what a peer would send next.
func windowManager(t *testing.T) *SyncManager {
	t.Helper()

	honest := mainnetFirstCheckpointHeaders(t)

	params := chaincfg.MainNetParams
	pinned := honest[windowCheckpoint-1].BlockHash()
	params.Checkpoints = []chaincfg.Checkpoint{{Height: windowCheckpoint, Hash: &pinned}}

	trunk := newRealTrunk(t, &params, 1, honest[:windowCheckpoint])

	sm := newHeaderCacheManager(t)
	sm.blockchainClient = trunk.client
	sm.chainParams = &params
	sm.headerCache = newHeaderCache().
		WithCheckpoints(params.Checkpoints).
		WithHeaderRules(trunk.rules(t, time.Now())).
		WithOwnerLive(sm.headerOwnerLive)
	sm.headersFirstMode.Store(true)

	best, tip, ok := sm.committedTip()
	require.True(t, ok)
	require.Equal(t, int32(windowCheckpoint), best)
	require.Equal(t, pinned, tip)
	require.Nil(t, sm.findNextHeaderCheckpoint(best), "the committed chain has passed its last checkpoint")

	return sm
}

// In the window after the last checkpoint commits and before headers-first
// mode is left, the rule still holds: an outbound peer this node never asked
// and an inbound peer are not read, and an asked outbound peer is. Without
// it, any peer could build a branch there, and nothing bounds the branches
// now that the global header cap is gone.
func TestHeaderRequestRule_TheRuleHoldsUntilHeadersFirstModeIsLeft(t *testing.T) {
	sm := windowManager(t)
	honest := mainnetFirstCheckpointHeaders(t)
	next := honest[windowCheckpoint:]

	best, _, ok := sm.committedTip()
	require.True(t, ok)

	outbound, _, _ := demotionPeer(t, sm, 100, 20000)
	inbound := inboundSyncPeer(t, sm, 101, 20000, false)

	require.False(t, sm.mayAskForHeaders(inbound, best), "an inbound peer is not asked while the mode is on")
	require.True(t, sm.mayAskForHeaders(outbound, best))

	sendHeadersInReplies(t, sm, outbound, next)
	sendHeadersInReplies(t, sm, inbound, next)
	require.Zero(t, sm.headerCache.heldHeaders(), "neither unasked peer is read in the window")
	require.True(t, outbound.Connected())
	require.True(t, inbound.Connected())

	askForHeaders(t, sm, outbound)
	sendHeadersInReplies(t, sm, outbound, next)

	top, _, held := sm.headerCache.PeerTop(outbound)
	require.True(t, held, "the asked outbound peer's branch is read")
	require.Equal(t, int32(11111), top)

	_, _, held = sm.headerCache.PeerTop(inbound)
	require.False(t, held)

	// Once the mode is left the rule no longer applies to asking.
	sm.headersFirstMode.Store(false)
	require.True(t, sm.mayAskForHeaders(inbound, best))
}

// The asked check runs before anything is read from the blockchain, so a batch
// from a peer this node never asked costs no GetBestBlockHeader call.
func TestHeaderRequestRule_AnUnaskedBatchCostsNoTipRead(t *testing.T) {
	sm := headerRuleManager(t)
	honest := mainnetFirstCheckpointHeaders(t)

	inbound := inboundSyncPeer(t, sm, 102, 20000, false)
	client := wrapTipFaults(sm)

	sendHeadersInReplies(t, sm, inbound, honest[:4000])

	require.Zero(t, client.calls.Load(), "an unasked batch is dropped before the tip is read")
	require.Zero(t, sm.headerCache.heldHeaders())
}
