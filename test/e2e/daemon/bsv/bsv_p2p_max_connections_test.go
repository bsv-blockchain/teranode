package bsv

import (
	"strconv"
	"testing"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/test/utils/wirepeer"
	"github.com/stretchr/testify/require"
)

// maxPeersKey is the settings key that feeds legacy config.MaxPeers - Teranode's
// analogue of bitcoin-sv's -maxconnections. settings.conf sets it to 20;
// services/legacy/config.go:32 defaults it to 125.
const maxPeersKey = "legacy_config_MaxPeers"

// maxPeersUnderTest is what the port lowers config.MaxPeers to, and it is
// upstream's inbound allowance rather than upstream's -maxconnections. Upstream
// passes -maxconnections=11 (8 outbound + 2 inbound + 1 feeler) and expects two
// inbound connections to be admitted; Teranode reserves nothing for outbound, so
// the number that governs how many inbound peers are admitted is MaxPeers itself.
// Setting it to upstream's 2 therefore reproduces upstream's observable - two in,
// the third refused - even though the knob means something different.
//
// It has to be below MaxPeersPerIP or the per-IP cap would fire first and this
// port would silently re-test TestBSVP2PMaxConnectionsFromAddr. That is asserted,
// not assumed.
const maxPeersUnderTest = 2

// TestBSVP2PMaxConnections ports bsv-p2p-max-connections.py, which checks that a
// node caps how many P2P connections it accepts in total.
//
// Teranode enforces a total cap immediately after the per-IP one, in
// server.handleAddPeerMsg (services/legacy/peer_server.go:2320): once
// state.Count() reaches config.MaxPeers the arriving peer is disconnected. So the
// shape of upstream's first block ports - fill the cap, offer one more, watch it
// refused, confirm the count did not move - but two things it asserts along the
// way do not, and both are recorded in the max-peers-no-reservation-no-eviction
// gap:
//
//   - Upstream's cap is split. -maxconnections is a total out of which
//     -maxoutboundconnections plus a feeler slot are reserved, so inbound peers
//     can never consume the slots the node needs to reach out. Teranode's MaxPeers
//     is flat and shared, which is why maxPeersUnderTest is set to upstream's
//     inbound allowance rather than to its -maxconnections.
//   - Upstream drops the newcomer only after trying to evict an existing peer:
//     the log line it waits for, "failed to find an eviction candidate -
//     connection dropped (full)", is that attempt reporting failure. Teranode
//     makes no attempt. The final loop here is a tripwire on that.
//
// Upstream's second block - no cap argument, 100 connections from one address
// admitted - is waived outright. Teranode's total cap is always on and its per-IP
// cap would refuse the sixth connection regardless, so satisfying the assertion
// would mean relaxing two defences at once.
func TestBSVP2PMaxConnections(t *testing.T) {
	// The precondition that makes this port about MaxPeers rather than about
	// MaxPeersPerIP. Both caps are checked in handleAddPeerMsg, per-IP first, so
	// the total cap is only observable while it is the lower of the two.
	require.Less(t, maxPeersUnderTest, configuredMaxPeersPerIP(t),
		"MaxPeers must be below MaxPeersPerIP for the total cap to be the one that fires")

	// Lowering the cap tightens it, which is the only safe direction: raising it to
	// admit upstream's 100 connections would relax a defence (GOAL.md rule 3).
	setLegacyConfigForTest(t, maxPeersKey, strconv.Itoa(maxPeersUnderTest))

	// P2P is enabled for speed only: getpeerinfo takes ~10s per call without it.
	// See the getpeerinfo-stalls-without-p2p-service gap.
	td := wirepeer.NewLegacyDaemonWithP2P(t)
	defer td.Stop(t)

	admitted := make([]*wirepeer.Peer, 0, maxPeersUnderTest)

	for range maxPeersUnderTest {
		admitted = append(admitted, connectPeer(t, td))
	}

	// Upstream: assert_equal(len(getpeerinfo()), connections).
	established := peerIdentities(requirePeerCount(t, td, maxPeersUnderTest))

	// Upstream: open one more connection and wait for the node to report it
	// dropped, then assert_equal(len(getpeerinfo()), connections) again.
	//
	// Connect asserts the dial succeeds, so the refusal is a node decision rather
	// than a refused accept. SkipHandshakeWait because the cap is checked from
	// OnVersion before the node writes its own version, so this peer will never
	// see a handshake to wait for.
	over := wirepeer.Connect(t, td, wirepeer.SkipHandshakeWait())
	defer over.Close()

	over.AssertDisconnected(t, rpcSettle, "a connection beyond MaxPeers must be dropped")

	require.Zero(t, over.Count(wire.CmdVersion),
		"an over-cap peer should be dropped before the node writes anything to it, received: %s",
		over.Summary())

	require.Equal(t, established, peerIdentities(requirePeerCount(t, td, maxPeersUnderTest)),
		"the over-cap connection changed the set of connected peers")

	// Tripwire on the missing eviction attempt. bitcoin-sv would have run
	// AttemptToEvictConnection here and, on a node whose peers were all equally
	// good, logged that it found no candidate; Teranode never looks. Asserting the
	// survivors rather than only the count is what distinguishes "the newcomer was
	// refused" from "an existing peer was evicted and the newcomer took its slot",
	// which are indistinguishable by count alone.
	for i, p := range admitted {
		require.True(t, p.Connected(),
			"TRIPWIRE: peer %d was dropped to make room for a connection beyond MaxPeers. If Teranode "+
				"has gained peer eviction, revisit the max-peers-no-reservation-no-eviction gap and the "+
				"waivers on bsv-p2p-max-connections.py", i)
	}
}
