package bsv

import (
	"math/rand"
	"testing"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/test/utils/wirepeer"
	"github.com/stretchr/testify/require"
)

const (
	// connPeers is capped by the node, not chosen: services/legacy/config.go sets
	// defaultMaxPeersPerIP to 5, and peer_server.go enforces it with no bypass for
	// whitelisted or loopback addresses. Every wirepeer dials from 127.0.0.1, so a
	// sixth is disconnected before it sends a version and Connect times out. That
	// is correct DoS protection, so the port scales to it rather than raising the
	// limit - a porting test must not switch off a defence to make itself pass.
	//
	// Upstream meshes 8 nodes, which is why this is a waived assertion rather than
	// a free choice.
	connPeers = 5

	// connRounds and connDropsPerRound are deliberately smaller than upstream's
	// 10 rounds of 10 disconnections. Upstream can afford it because its 8 nodes
	// gossip in parallel; here every round is serialised against one node's RPC,
	// so the full 100 churn events would dominate the suite for no extra signal.
	// Reduced on purpose - see the log line in the churn subtest, which says so
	// at runtime rather than leaving the shortfall implicit.
	connRounds        = 4
	connDropsPerRound = 2
)

// TestBSVP2PConnections ports p2p-connections.py, whose stated purpose is to
// check "the P2P connection handling after moving to use shared_ptrs" - that is,
// that a node's view of who is connected stays correct across churn.
//
// Upstream builds an 8-node full mesh, disconnects random pairs, and asserts each
// node's getconnectioncount matches the expected number. Neither half of that is
// available here:
//
//   - getconnectioncount is handleUnimplemented, so the count is read as
//     len(getpeerinfo) instead (see the net-rpcs-unimplemented gap).
//   - a multi-node mesh cannot be built at all: td.ConnectToPeer produces an
//     empty peer ID under SETTINGS_CONTEXT=test, so no ring or mesh forms (see
//     the invalidblockrequest-port-red gap).
//
// The property under test survives both, because it is a property of one node's
// bookkeeping rather than of the mesh: attach wire peers to a single node, close
// some, reattach, and require the node's count to track every change.
//
// Two things are lost and both are waived rather than glossed over. The symmetry:
// upstream checks that all 8 nodes agree, this checks one. And the scale: the node
// allows 5 connections per IP and every wirepeer is on 127.0.0.1, so this runs 5
// peers against upstream's 8 - see connPeers.
//
// Where upstream picks pairs with an unseeded random.randint, this port seeds
// explicitly. Churn tests fail rarely and matter when they do, and an
// irreproducible failure is close to worthless.
func TestBSVP2PConnections(t *testing.T) {
	// P2P is enabled for speed, not correctness: getpeerinfo takes ~10s per call
	// without it, and this port polls it dozens of times. See the
	// getpeerinfo-stalls-without-p2p-service gap.
	td := wirepeer.NewLegacyDaemonWithP2P(t)
	defer td.Stop(t)

	// A fixed seed, so a failure can be replayed exactly.
	rng := rand.New(rand.NewSource(1))

	peers := make([]*wirepeer.Peer, 0, connPeers)

	connect := func() {
		p := wirepeer.Connect(t, td)
		t.Cleanup(p.Close)
		peers = append(peers, p)
	}

	t.Run("every connected peer is reported", func(t *testing.T) {
		for range connPeers {
			connect()
		}

		reported := requirePeerCount(t, td, connPeers)

		ids := make(map[int32]bool, len(reported))
		for _, info := range reported {
			require.False(t, ids[info.ID], "peer id %d reported twice in %+v", info.ID, reported)
			ids[info.ID] = true
		}
	})

	t.Run("closing peers lowers the count", func(t *testing.T) {
		closing := peers[:connDropsPerRound]
		for _, p := range closing {
			p.Close()
		}

		peers = peers[connDropsPerRound:]

		requirePeerCount(t, td, connPeers-connDropsPerRound)
	})

	t.Run("reconnecting restores the count", func(t *testing.T) {
		for range connDropsPerRound {
			connect()
		}

		requirePeerCount(t, td, connPeers)
	})

	t.Run("the count tracks repeated churn", func(t *testing.T) {
		t.Logf("running %d rounds of %d drops; upstream runs 10 rounds of 10 against 8 nodes",
			connRounds, connDropsPerRound)

		for round := range connRounds {
			// Drop a random subset, as upstream picks random pairs.
			rng.Shuffle(len(peers), func(i, j int) { peers[i], peers[j] = peers[j], peers[i] })

			for _, p := range peers[:connDropsPerRound] {
				p.Close()
			}

			peers = peers[connDropsPerRound:]

			require.Len(t, requirePeerCount(t, td, connPeers-connDropsPerRound),
				connPeers-connDropsPerRound, "round %d: after dropping", round)

			for range connDropsPerRound {
				connect()
			}

			require.Len(t, requirePeerCount(t, td, connPeers), connPeers,
				"round %d: after reconnecting", round)
		}
	})
}

// requirePeerCount waits for getpeerinfo to report exactly want peers and returns
// what it saw. It polls rather than reading once because a closed socket takes a
// moment to leave the node's peer list, which is the same reason upstream wraps
// its getconnectioncount check in wait_until.
func requirePeerCount(t *testing.T, td *daemon.TestDaemon, want int) []peerInfo {
	t.Helper()

	var reported []peerInfo

	require.Eventually(t, func() bool {
		reported = tryPeers(td)
		return len(reported) == want
	}, rpcSettle, rpcPoll, "expected %d connected peers, last saw %d: %+v", want, len(reported), reported)

	return reported
}
