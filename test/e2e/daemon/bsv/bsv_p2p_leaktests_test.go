package bsv

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/test/utils/wirepeer"
	"github.com/stretchr/testify/require"
)

// TestBSVP2PLeakTests ports p2p-leaktests.py, whose docstring states the rule it
// enforces: "A node should never send anything other than VERSION/VERACK/REJECT
// until it's received a VERACK."
//
// Upstream attaches three peers that each stop short of a full handshake and
// asserts that none of them is sent anything outside that set. Two of the three
// port as clean reproductions. The third shows that Teranode does not apply the
// rule at all: a peer that has sent a version and no verack is treated as fully
// connected, appears in getpeerinfo, and has its requests answered. See the
// pre-handshake-message-leak gap.
//
// Two notes on faithfulness, both about not passing for the wrong reason:
//
//   - Upstream's third peer sends a getaddr and expects no addr back. Against a
//     freshly started Teranode that assertion passes trivially, because the
//     address manager is empty and there is nothing to send - measured. The port
//     therefore seeds the address manager first, via a peer that does complete its
//     handshake, so that the getaddr has something to disclose. Without that step
//     this port would report a pass and hide the finding.
//   - Upstream mines a block and asserts no inv reaches the three peers. That is
//     not reproduced: Teranode does not announce a locally mined block to legacy
//     wire peers at all (the legacy-block-announcement gap), so the assertion
//     cannot fail here for any reason to do with handshake state.
func TestBSVP2PLeakTests(t *testing.T) {
	// P2P is enabled for getpeerinfo speed only. See the
	// getpeerinfo-stalls-without-p2p-service gap.
	td := wirepeer.NewLegacyDaemonWithP2P(t)
	defer td.Stop(t)

	// Give the node addresses to disclose, so the getaddr assertion below tests
	// handshake gating rather than an empty address manager. Uses the same
	// unsolicited-addr route TestBSVUnsolicitedAddr establishes, on a peer that
	// completes its handshake and then goes away.
	seedAddressManager(t, td)

	t.Run("a verack before any version is rejected and the connection closed", func(t *testing.T) {
		// Upstream's CNodeNoVersionBan sends banscore veracks to accumulate
		// bitcoin-sv's ban score. One is enough here: negotiateInboundProtocol
		// rejects the first message that is neither a version nor a createstream and
		// returns, so the connection is gone before a second could matter. Upstream
		// flags its own count as implementation-specific - "Remove if bitcoind ban
		// behavior changes" - so not copying it is following the comment.
		p := wirepeer.DialRaw(t, td)
		defer p.Close()

		p.WriteFrame(t, wire.CmdVerAck, nil)

		// Upstream permits a reject to this peer (its on_reject is overridden to
		// ignore), and asserts only that it ends up disconnected.
		got := p.ReadSome(t, 4096, 10*time.Second)
		require.NotEmpty(t, got, "the node should reject a verack that precedes a version")

		require.GreaterOrEqual(t, len(got), 24, "expected at least a message header, got %d bytes", len(got))
		require.Equal(t, wire.CmdReject, string(trimCommand(got[4:16])),
			"the first message back should be a reject")
		require.Contains(t, string(got), "a version message must precede all others",
			"the reject should say why")

		// Upstream: assert not no_version_bannode.connected.
		p.ExpectClosed(t, 10*time.Second)
	})

	t.Run("a peer that says nothing is sent nothing", func(t *testing.T) {
		// Upstream's CNodeNoVersionIdle: connect and wait. Every message type is
		// unexpected for this one, rejects included.
		p := wirepeer.DialRaw(t, td)
		defer p.Close()

		// Upstream mines a block here to give the node something to leak. Kept for
		// the same reason, though see the doc comment - a locally mined block is not
		// announced to legacy peers regardless.
		td.MineBlocks(t, 1)

		require.Empty(t, p.ReadSome(t, 4096, 5*time.Second),
			"a peer that has sent nothing should be sent nothing")
	})

	t.Run("a peer that sends no verack still has its requests answered", func(t *testing.T) {
		// Upstream's CNodeNoVerackIdle: send a version, wait for the node's version,
		// then send ping and getaddr and never a verack. Its on_version and on_verack
		// are tolerated; a pong or an addr is a leak.
		p := wirepeer.Connect(t, td, wirepeer.SkipHandshakeWait())
		defer p.Close()

		p.Wait(t, 10*time.Second, "version from node", cmdIs(wire.CmdVersion))

		p.Send(t, wire.NewMsgPing(leakTestPingNonce))
		p.Send(t, wire.NewMsgGetAddr())

		// Upstream asserts neither of these arrives. Both do, and each is a direct
		// answer to a request from a peer that has not completed its handshake.
		pong := p.Wait(t, 10*time.Second,
			"TRIPWIRE: pong to a peer that never sent a verack - if this now times out, Teranode gates "+
				"replies on the handshake and the pre-handshake-message-leak gap should be revisited",
			cmdIs(wire.CmdPong))

		if msg, ok := pong.(*wire.MsgPong); ok {
			require.Equal(t, uint64(leakTestPingNonce), msg.Nonce,
				"the pong should echo our nonce, confirming it answers our ping")
		}

		addr := p.Wait(t, 10*time.Second,
			"TRIPWIRE: addr to a peer that never sent a verack - if this now times out, Teranode gates "+
				"replies on the handshake and the pre-handshake-message-leak gap should be revisited",
			cmdIs(wire.CmdAddr))

		msg, ok := addr.(*wire.MsgAddr)
		require.True(t, ok, "the addr reply should decode as MsgAddr")
		require.NotEmpty(t, msg.AddrList,
			"an empty addr would mean the address manager had nothing to disclose, which makes this "+
				"subtest vacuous - seedAddressManager should have prevented that")

		// The reason both leaks happen: the node considers the handshake done once
		// it has our version, without waiting for ours in return.
		require.NotEmpty(t, tryPeers(td),
			"a peer that has sent only a version is counted as connected, which is why its requests "+
				"are answered")
	})
}

// leakTestPingNonce is echoed back in the pong, which is how the port knows the
// pong answers its ping rather than being an unrelated keepalive.
const leakTestPingNonce = 0x5ea1ea11

// seedAddressManager gives the node a set of addresses to hold, so that a later
// getaddr has something to return. It uses a peer that completes its handshake and
// then disconnects, leaving only the addresses behind.
//
// The addresses must be routable or addrmgr.IsRoutable discards them on the way in
// and the seeding silently does nothing.
func seedAddressManager(t *testing.T, td *daemon.TestDaemon) {
	t.Helper()

	seed := wirepeer.Connect(t, td)
	defer seed.Close()

	injectUnsolicitedAddr(t, seed)

	// The address manager is updated synchronously in OnAddr, but the message has
	// to be read first; a getaddr on a later connection is what actually confirms
	// it landed, and the subtest that needs it asserts a non-empty reply.
	time.Sleep(time.Second)
}

// cmdIs matches a message by wire command.
func cmdIs(cmd string) func(wire.Message) bool {
	return func(msg wire.Message) bool { return msg.Command() == cmd }
}

// trimCommand strips the NUL padding from a 12-byte wire command field.
func trimCommand(field []byte) []byte {
	for i, b := range field {
		if b == 0 {
			return field[:i]
		}
	}

	return field
}
