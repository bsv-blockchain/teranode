package bsv

import (
	"testing"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/test/utils/wirepeer"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// maxPeersPerIPKey is the settings key that feeds legacy config.MaxPeersPerIP.
// services/legacy/config.go:33 defaults it to 5 and settings.conf sets it to 5;
// the port reads the configured value rather than restating 5, so that changing
// the setting moves the test's expectations with it instead of breaking it.
const maxPeersPerIPKey = "legacy_config_MaxPeersPerIP"

// legacyWhitelistKey is the settings key that feeds legacy config.Whitelists -
// the nearest thing Teranode has to bitcoin-sv's -whitelist. settings.conf
// defines it in no context, so it is empty in every deployment today.
const legacyWhitelistKey = "legacy_config_Whitelists"

// TestBSVP2PMaxConnectionsFromAddr ports bsv-p2p-max-connections-from-addr.py,
// which checks that a node caps how many P2P connections it accepts from a
// single address.
//
// Teranode has the same knob under a different name: bitcoin-sv's
// -maxconnectionsfromaddr is legacy config.MaxPeersPerIP, enforced in
// server.handleAddPeerMsg (services/legacy/peer_server.go:2312). So upstream's
// first block - fill the cap, offer one more, watch it dropped, confirm nothing
// established was displaced - ports directly.
//
// Upstream's other two blocks do not, and both are waived rather than glossed:
//
//   - Upstream restarts the node with -whitelist=127.0.0.1 and asserts the cap
//     stops applying. Teranode's per-IP check never consults sp.isWhitelisted,
//     and its whitelist cannot be configured at all - see the
//     legacy-whitelist-inert gap. The third subtest here is a tripwire pinning
//     the current behaviour, so the waiver breaks the day either half is fixed.
//   - Upstream restarts with no cap argument and asserts 20 connections from one
//     address are admitted, because bitcoin-sv treats the option as off by
//     default. Teranode's cap is always on, and raising it to 20 to make the
//     assertion pass would relax the very defence the first block asserts, which
//     a porting change must not do.
//
// Each of upstream's blocks is a fresh node, so each subtest here runs its own
// daemon - sequentially, since the whitelist is read once at server start and
// only a restart can change it.
//
// The cap is enforced in handleAddPeerMsg, which runs off the back of OnVersion
// (peer_server.go:689) and therefore before the node writes its own version. An
// over-cap peer consequently receives nothing at all, so it is opened with
// SkipHandshakeWait: waiting for a handshake that will never arrive would fail
// inside Connect rather than assert anything.
func TestBSVP2PMaxConnectionsFromAddr(t *testing.T) {
	limit := configuredMaxPeersPerIP(t)

	// Upstream's first `with run_node_with_connections(...)` block: fill the cap,
	// then probe it against the same node. Deliberately one subtest rather than two
	// nested ones - a nested subtest's t.Cleanup fires when it returns, which would
	// close the peers holding the cap full before the probe that depends on them.
	t.Run("a connection over MaxPeersPerIP is dropped from an address at the cap", func(t *testing.T) {
		// P2P is enabled for speed only: getpeerinfo takes ~10s per call without
		// it. See the getpeerinfo-stalls-without-p2p-service gap.
		td := wirepeer.NewLegacyDaemonWithP2P(t)
		defer td.Stop(t)

		// Every wirepeer dials from 127.0.0.1, which is what makes one node enough
		// to exercise a per-address cap - upstream needs the same property, and
		// passes ip='127.0.0.1' explicitly for it.
		admitted := make([]*wirepeer.Peer, 0, limit)

		for range limit {
			admitted = append(admitted, connectPeer(t, td))
		}

		// Upstream: assert_equal(len(getpeerinfo()), maxconnectionsfromaddr) once
		// run_node_with_connections has opened exactly that many.
		established := peerIdentities(requirePeerCount(t, td, limit))

		// Upstream: open one more connection, wait for "connection from 127.0.0.1
		// dropped: too many connections from the same address" in the log, then
		// assert_equal(len(getpeerinfo()), maxconnectionsfromaddr) again.
		//
		// Connect asserts the dial itself succeeds, which is the framework-implied
		// half: the TCP connection is accepted and the drop is a node decision
		// rather than a refused accept. Upstream relies on the same thing, building
		// the extra Connection without expecting a dial error.
		over := wirepeer.Connect(t, td, wirepeer.SkipHandshakeWait())
		defer over.Close()

		over.AssertDisconnected(t, rpcSettle, "a connection beyond MaxPeersPerIP must be dropped")

		// The cap is checked in handleAddPeerMsg, reached from OnVersion before the
		// node writes its own version, so an over-cap peer never sees one. Worth
		// asserting rather than assuming: a node that replied and only then dropped
		// the peer would still satisfy the check above, and the two are very
		// different from a peer's point of view.
		require.Zero(t, over.Count(wire.CmdVersion),
			"an over-cap peer should be dropped before the node writes anything to it, received: %s",
			over.Summary())

		// The interesting half of upstream's second count check: the drop must not
		// have evicted an established peer to make room. Compared on identity
		// rather than on the whole getpeerinfo row, because the byte counters keep
		// moving between calls - the node pings its peers.
		require.Equal(t, established, peerIdentities(requirePeerCount(t, td, limit)),
			"the over-cap connection displaced an established peer")

		// And the same from the peers' side of the socket. No polling window is
		// needed: AssertDisconnected above has already waited until the node
		// observably finished dealing with the over-cap connection, so if that had
		// cost an established peer its socket, it would have cost it by now.
		for i, p := range admitted {
			require.True(t, p.Connected(), "peer %d was dropped when the over-cap peer arrived", i)
		}
	})

	// Tripwire for the waived half of the script. Upstream's second block runs the
	// node with -whitelist=127.0.0.1 and asserts maxconnectionsfromaddr + 1
	// connections are then admitted from that address. Teranode admits no such
	// thing, for two independent reasons, both recorded in the
	// legacy-whitelist-inert gap:
	//
	//   - config.MaxPeersPerIP is checked in handleAddPeerMsg without ever reading
	//     sp.isWhitelisted, so a whitelist would not exempt an address from the cap
	//     even if one were in force.
	//   - no whitelist can be in force. loadConfig parses config.Whitelists into
	//     the config.whitelists CIDR list that isWhitelisted actually reads
	//     (services/legacy/config.go:523), and setConfigValuesFromSettings
	//     overwrites config.Whitelists from the settings map afterwards
	//     (peer_server.go:3530) without re-deriving it, so setting the only key
	//     there is leaves the parsed list empty.
	//
	// This subtest asserts what Teranode does today, so it goes red the day either
	// reason is addressed - at which point the waiver should become a real port.
	t.Run("whitelisting the address does not exempt it from the cap", func(t *testing.T) {
		// Set the whitelist the way an operator would have to: before the daemon
		// reads it, and taken back out afterwards.
		setLegacyConfigForTest(t, legacyWhitelistKey, "127.0.0.1")

		td := wirepeer.NewLegacyDaemonWithP2P(t)
		defer td.Stop(t)

		for range limit {
			connectPeer(t, td)
		}

		requirePeerCount(t, td, limit)

		// Upstream expects this one to be admitted. Teranode drops it.
		over := wirepeer.Connect(t, td, wirepeer.SkipHandshakeWait())
		defer over.Close()

		over.AssertDisconnected(t, rpcSettle,
			"TRIPWIRE: whitelisting 127.0.0.1 now exempts it from MaxPeersPerIP. If that is intended, "+
				"close the legacy-whitelist-inert gap and turn the whitelist waiver on "+
				"bsv-p2p-max-connections-from-addr.py into a real port")

		requirePeerCount(t, td, limit)
	})
}

// connectPeer attaches one handshaked wire peer to td and closes it when t ends.
// Connect completes version/verack before returning, so these are negotiated
// peers rather than half-open sockets - upstream's P2PHandler waits for the same
// handshake, so the cap counts the same thing on both sides.
func connectPeer(t *testing.T, td *daemon.TestDaemon) *wirepeer.Peer {
	t.Helper()

	p := wirepeer.Connect(t, td)
	t.Cleanup(p.Close)

	return p
}

// peerIdentities reduces a getpeerinfo result to the part of it that identifies
// a connection and does not change while the connection lives. Two calls a
// moment apart differ in bytessent and bytesrecv on every peer, so a whole-row
// comparison cannot express "the same peers are still here".
func peerIdentities(infos []peerInfo) map[int32]string {
	out := make(map[int32]string, len(infos))
	for _, info := range infos {
		out[info.ID] = info.Addr
	}

	return out
}

// configuredMaxPeersPerIP reads the per-IP cap the daemon will run with. It
// requires a cap of at least two so that "fill it, then offer one more" is a
// meaningful sequence, which also makes a setting of 0 - which bars every
// inbound connection, since handleAddPeerMsg compares with >= - fail loudly here
// rather than as a confusing connect timeout.
func configuredMaxPeersPerIP(t *testing.T) int {
	t.Helper()

	limit, found := gocore.Config().GetInt(maxPeersPerIPKey)
	require.True(t, found, "%s is not set in settings.conf for this context", maxPeersPerIPKey)
	require.GreaterOrEqual(t, limit, 2, "%s is too low for this port to say anything", maxPeersPerIPKey)

	return limit
}
