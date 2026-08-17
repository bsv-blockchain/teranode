package bsv

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/test/utils/wirepeer"
	"github.com/stretchr/testify/require"
)

// protoconfViolationSends is how many protoconf messages the port sends after the
// handshake. Upstream's node sees two in total: one its mininode sends
// automatically during the handshake, and one the test sends explicitly to
// trigger the violation. wirepeer sends none during its handshake, so the first
// send below stands in for upstream's automatic one and the second is upstream's
// explicit one - the node sees the same two, in the same order.
//
// The remaining sends have no upstream counterpart and are there because
// bitcoin-sv's rule is "once", so a port that only ever sent two could not tell
// "the second is tolerated" from "some later one is not".
const protoconfViolationSends = 7

// TestBSVProtoconfViolation ports bsv-protoconf-violation.py, which asserts two
// things about protoconf: that the node sends its own after the verack, and that a
// peer sending more than one gets disconnected but not banned.
//
// The ordering half ports exactly, and reads the same way upstream does thanks to
// wirepeer.Peer.MessageIndex, which is the analogue of mininode's msg_index.
//
// The violation half does not port: Teranode has no duplicate-protoconf guard at
// all. serverPeer.OnProtoconf (services/legacy/peer_server.go:695) does not track
// whether it has run for this peer before, so every protoconf is processed as if
// it were the first. Measured: seven on one connection, no disconnect, no reject,
// nothing banned. See the no-duplicate-protoconf-guard gap, which is about what
// OnProtoconf redoes each time rather than about the missing disconnect on its own.
//
// The "not banned" half of upstream's assertion is asserted rather than skipped
// even though Teranode passes it trivially - it currently holds because nothing
// happens at all, and it should keep holding for the right reason if the
// disconnect is ever added. bitcoin-sv is explicit that this is a disconnect and
// not a ban.
func TestBSVProtoconfViolation(t *testing.T) {
	// P2P is enabled because this port reads listbanned twice: with the legacy
	// service alone every ban RPC fails outright. See the setban-address-format and
	// getpeerinfo-stalls-without-p2p-service gaps.
	td := wirepeer.NewLegacyDaemonWithP2P(t)
	defer td.Stop(t)

	p := wirepeer.Connect(t, td)
	defer p.Close()

	// Upstream: test_node.wait_for_protoconf().
	require.Eventually(t, func() bool { return p.Count(wire.CmdProtoconf) > 0 },
		10*time.Second, 100*time.Millisecond,
		"the node should send a protoconf after the handshake, received: %s", p.Summary())

	// Upstream: assert_greater_than(msg_index["protoconf"], msg_index["verack"]).
	// protoconf is a BSV extension a pre-protoconf peer would not understand, so it
	// must not precede the verack that establishes the peer speaks this protocol.
	verackAt := p.MessageIndex(wire.CmdVerAck)
	protoconfAt := p.MessageIndex(wire.CmdProtoconf)

	require.NotEqual(t, -1, verackAt, "no verack recorded, received: %s", p.Summary())
	require.Greater(t, protoconfAt, verackAt,
		"the node's protoconf must arrive after its verack, received: %s", p.Summary())

	// Upstream: assert_equal(len(listbanned()), 0) before the violation.
	require.Empty(t, wirepeer.ListBanned(t, td), "nothing should be banned before the violation")

	// Upstream: send_message(msg_protoconf()) once, then wait_for_disconnect().
	// See protoconfViolationSends for why this is a loop.
	for range protoconfViolationSends {
		p.Send(t, wire.NewMsgProtoconf(0, true))
	}

	p.AssertStillConnected(t, 3*time.Second,
		"TRIPWIRE: Teranode now disconnects a peer that sends protoconf more than once, as bitcoin-sv "+
			"does. Revisit the no-duplicate-protoconf-guard gap and assert upstream's disconnect "+
			"instead of this tolerance")

	require.Empty(t, p.Received(wire.CmdReject),
		"a repeated protoconf draws no reject either")

	// Upstream: assert_equal(len(listbanned()), 0) after. Upstream reaches this
	// having been disconnected; Teranode reaches it still connected. Either way the
	// peer must not be banned - bitcoin-sv's comment is explicit that a repeated
	// protoconf causes "disconnection (but not banning)".
	require.Empty(t, wirepeer.ListBanned(t, td), "a repeated protoconf must not ban the peer")
}
