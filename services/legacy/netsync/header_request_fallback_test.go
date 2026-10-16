package netsync

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/stretchr/testify/require"
)

// inboundSyncPeer registers the inbound side of a connected pair as a sync
// candidate at lastBlock, whitelisted or not, as the peer server builds it: the
// whitelist is read into the peer's config before the peer is created.
func inboundSyncPeer(t *testing.T, sm *SyncManager, idx uint8, lastBlock int32, whitelisted bool) *peerpkg.Peer {
	t.Helper()

	inboundCfg := peerpkg.Config{
		UserAgentName:    "btcdtest",
		UserAgentVersion: "1.0",
		ChainParams:      &chaincfg.MainNetParams,
		Whitelisted:      whitelisted,
	}
	remoteCfg := peerpkg.Config{
		UserAgentName:    "btcdtest",
		UserAgentVersion: "1.0",
		ChainParams:      &chaincfg.MainNetParams,
	}

	inbound, remote, err := MakeConnectedPeers(t, inboundCfg, remoteCfg, idx)
	require.NoError(t, err)
	require.True(t, inbound.Inbound())

	inbound.UpdateLastBlockHeight(lastBlock)

	t.Cleanup(func() {
		inbound.DisconnectWithInfo("test over")
		remote.DisconnectWithInfo("test over")
	})

	state := &peerSyncState{
		syncCandidate: true,
		requestedTxns: expiringmap.New[chainhash.Hash, struct{}](10 * time.Second),
	}
	t.Cleanup(state.requestedTxns.Stop)
	state.noteBestKnownHeight(lastBlock)

	sm.peerStates.Set(inbound, state)

	return inbound
}

// electSyncPeer clears the sync peer and runs one election.
func electSyncPeer(sm *SyncManager) *peerpkg.Peer {
	if sp := sm.loadSyncPeer(); sp != nil {
		sp.SetSyncPeer(false)
	}

	sm.storeSyncPeer(nil, nil)
	sm.startSync()

	return sm.loadSyncPeer()
}

// headersAskedRecorded reports whether peer's state records a getheaders from this node.
func headersAskedRecorded(t *testing.T, sm *SyncManager, peer *peerpkg.Peer) bool {
	t.Helper()

	state, ok := sm.peerStates.Get(peer)
	require.True(t, ok)

	return state.headersAsked.Load()
}

// A node with only inbound peers, below the last checkpoint, still syncs. As SV
// Node does when it has no preferred-download peer (net_processing.cpp:5057), it
// elects one inbound peer, asks only that one, and reads only that one's
// headers. Its branch walks to mainnet's first checkpoint and the blocks become
// wanted.
func TestHeaderRequestRule_AnInboundOnlyNodeElectsOneInboundSyncPeerAndReachesTheCheckpoint(t *testing.T) {
	sm := headerRuleManager(t)
	sm.headersFirstMode.Store(false)

	honest := mainnetFirstCheckpointHeaders(t)

	inbound := make([]*peerpkg.Peer, 0, 4)
	for i := 0; i < 4; i++ {
		inbound = append(inbound, inboundSyncPeer(t, sm, uint8(60+i), 20000, false)) //nolint:gosec // a small peer index
	}

	elected := electSyncPeer(sm)
	require.NotNil(t, elected, "an inbound-only node elects a sync peer")
	require.True(t, elected.Inbound())
	require.True(t, sm.headersFirstMode.Load(), "the elected peer was sent the round's getheaders")
	require.True(t, headersAskedRecorded(t, sm, elected))

	best, tip, ok := sm.committedTip()
	require.True(t, ok)

	for _, p := range inbound {
		if p == elected {
			continue
		}

		require.False(t, headersAskedRecorded(t, sm, p), "only the elected inbound peer is asked")
		require.False(t, sm.mayAskForHeaders(p, best))
		require.Error(t, sm.requestHeaders(p, best, []*chainhash.Hash{&tip}, &zeroHash))

		sendHeadersInReplies(t, sm, p, sybilBranch(honest, 7))
		_, _, held := sm.headerCache.PeerTop(p)
		require.False(t, held, "an inbound peer that is not the sync peer holds no branch")
	}

	askable := sm.headerRequestPeers(best)
	require.Len(t, askable, 1, "the refill rotation reaches only the sync peer")
	require.Equal(t, elected, askable[0].peer)

	sendHeadersInReplies(t, sm, elected, honest)

	requireHonestWalkReachedTheCheckpoint(t, sm, elected, honest)
	require.Equal(t, 11111, sm.headerCache.heldHeaders(), "the sync peer's branch is all the cache holds")
}

// When the inbound fallback sync peer is demoted, its branch goes with the role,
// so at most one inbound peer holds a branch at any time, and the next elected
// inbound peer may be asked.
func TestHeaderRequestRule_ADemotedInboundFallbackLosesItsBranch(t *testing.T) {
	sm := headerRuleManager(t)
	sm.headersFirstMode.Store(false)

	honest := mainnetFirstCheckpointHeaders(t)

	first := inboundSyncPeer(t, sm, 70, 20000, false)
	second := inboundSyncPeer(t, sm, 71, 20000, false)

	elected := electSyncPeer(sm)
	require.NotNil(t, elected)

	other := second
	if elected == second {
		other = first
	}

	sendHeadersInReplies(t, sm, elected, honest[:2*wire.MaxBlockHeadersPerMsg])

	_, _, held := sm.headerCache.PeerTop(elected)
	require.True(t, held)

	state, ok := sm.peerStates.Get(elected)
	require.True(t, ok)
	sm.demoteSyncPeer(elected, state)

	_, _, held = sm.headerCache.PeerTop(elected)
	require.False(t, held, "the demoted inbound peer's branch is dropped")
	require.Equal(t, other, sm.loadSyncPeer(), "the other inbound peer is elected")
	require.True(t, headersAskedRecorded(t, sm, other))

	// The demoted peer stays asked, but is no longer read.
	sendHeadersInReplies(t, sm, elected, honest[:wire.MaxBlockHeadersPerMsg])
	_, _, held = sm.headerCache.PeerTop(elected)
	require.False(t, held, "a demoted inbound peer's later batches are not read")

	sendHeadersInReplies(t, sm, other, honest)
	requireHonestWalkReachedTheCheckpoint(t, sm, other, honest)
}

// With an outbound peer ahead, inbound peers are never elected, asked or read
// below the last checkpoint. Election is random, so it is repeated: with four
// inbound candidates and one outbound, a startSync that let inbound peers in
// would elect the outbound peer fifty times running with probability 5^-50.
func TestHeaderRequestRule_WithAnOutboundPeerInboundPeersAreNeverAsked(t *testing.T) {
	sm := headerRuleManager(t)
	sm.headersFirstMode.Store(false)

	honest := mainnetFirstCheckpointHeaders(t)

	outbound, _, _ := demotionPeer(t, sm, 80, 20000)
	require.False(t, outbound.Inbound())

	inbound := make([]*peerpkg.Peer, 0, 4)
	for i := 0; i < 4; i++ {
		inbound = append(inbound, inboundSyncPeer(t, sm, uint8(81+i), 20000, false)) //nolint:gosec // a small peer index
	}

	for round := 0; round < 50; round++ {
		require.Equal(t, outbound, electSyncPeer(sm), "round %d elects the outbound peer", round)
	}

	best, _, ok := sm.committedTip()
	require.True(t, ok)

	askable := sm.headerRequestPeers(best)
	require.Len(t, askable, 1, "the refill rotation holds only the outbound peer")
	require.Equal(t, outbound, askable[0].peer)

	for i, p := range inbound {
		require.False(t, headersAskedRecorded(t, sm, p), "inbound peer %d was never asked", i)

		sendHeadersInReplies(t, sm, p, sybilBranch(honest, byte(i+1))) //nolint:gosec // a small tag
		_, _, held := sm.headerCache.PeerTop(p)
		require.False(t, held, "inbound peer %d holds no branch", i)
	}

	sendHeadersInReplies(t, sm, outbound, honest)
	requireHonestWalkReachedTheCheckpoint(t, sm, outbound, honest)
	require.Equal(t, 11111, sm.headerCache.heldHeaders())
}

// A whitelisted inbound peer is a preferred-download peer, as in SV Node
// (net_processing.cpp:120): it may be asked beside an outbound peer, and with
// only a whitelisted and a plain inbound peer connected the plain one is never
// elected.
func TestHeaderRequestRule_AWhitelistedInboundPeerIsPreferred(t *testing.T) {
	sm := headerRuleManager(t)
	sm.headersFirstMode.Store(false)

	honest := mainnetFirstCheckpointHeaders(t)

	whitelisted := inboundSyncPeer(t, sm, 90, 20000, true)
	plain := make([]*peerpkg.Peer, 0, 4)

	for i := 0; i < 4; i++ {
		plain = append(plain, inboundSyncPeer(t, sm, uint8(91+i), 20000, false)) //nolint:gosec // a small peer index
	}

	for round := 0; round < 50; round++ {
		require.Equal(t, whitelisted, electSyncPeer(sm), "round %d elects the whitelisted peer", round)
	}

	best, _, ok := sm.committedTip()
	require.True(t, ok)

	for _, p := range plain {
		require.False(t, sm.mayAskForHeaders(p, best))
		require.False(t, headersAskedRecorded(t, sm, p))
	}

	outbound, _, _ := demotionPeer(t, sm, 95, 20000)
	require.True(t, sm.mayAskForHeaders(outbound, best))
	require.True(t, sm.mayAskForHeaders(whitelisted, best), "a whitelisted inbound peer is asked beside an outbound one")

	sendHeadersInReplies(t, sm, whitelisted, honest)
	requireHonestWalkReachedTheCheckpoint(t, sm, whitelisted, honest)
}

// A fill that was running when the inbound fallback sync peer was demoted does not give it its
// branch back. The install used to check only that the peer was still connected, so the demoted
// peer could hold a branch beside the next fallback's, and one inbound branch was not a bound.
func TestHeaderRequestRule_AFillAfterDemotionGivesTheInboundFallbackNoBranch(t *testing.T) {
	sm := headerRuleManager(t)
	sm.headersFirstMode.Store(false)

	honest := mainnetFirstCheckpointHeaders(t)

	fallback := inboundSyncPeer(t, sm, 72, 20000, false)
	require.True(t, electSyncPeer(sm) == fallback)

	// A second inbound peer for the role to move to.
	inboundSyncPeer(t, sm, 74, 20000, false)

	_, tip, ok := sm.committedTip()
	require.True(t, ok)

	// The fill passed the read rule while the peer was the sync peer; it installs after the
	// demotion, as a fill holding the cache's locks across a store call can.
	state, ok := sm.peerStates.Get(fallback)
	require.True(t, ok)
	sm.demoteSyncPeer(fallback, state)
	// Compared as pointers: require.NotEqual reads every field of a live peer.
	require.False(t, sm.loadSyncPeer() == fallback, "the role moved on")

	sm.headerCache.FillFrom(sm.headerOwner(fallback), tip, 1, honest[:wire.MaxBlockHeadersPerMsg])

	_, _, held := sm.headerCache.PeerTop(fallback)
	require.False(t, held, "the demoted inbound fallback holds no branch")
	require.Zero(t, sm.headerCache.heldHeaders())

	outbound, _, _ := demotionPeer(t, sm, 73, 20000)
	sm.headerCache.FillFrom(sm.headerOwner(outbound), tip, 1, honest[:wire.MaxBlockHeadersPerMsg])

	_, _, held = sm.headerCache.PeerTop(outbound)
	require.True(t, held, "an outbound peer the node may ask still gets one")
}
