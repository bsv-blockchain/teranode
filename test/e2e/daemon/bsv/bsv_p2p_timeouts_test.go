package bsv

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/test/utils/wirepeer"
	"github.com/stretchr/testify/require"
)

const (
	// negotiateTimeout mirrors services/legacy/peer/peer.go:63, which is the
	// deadline a connection has to complete protocol negotiation. bitcoin-sv's
	// equivalent is 60 seconds, which is why upstream's timeline is twice as long
	// as this one. Measured to fire at 30.0s to the tenth of a second.
	negotiateTimeout = 30 * time.Second

	// beforeTimeout and afterTimeout are the two points either side of it that the
	// port samples. Both are needed: a check only after the deadline could not
	// tell a timeout from an immediate disconnect, which is exactly the difference
	// between how this node treats two of the three peers below.
	beforeTimeout = negotiateTimeout - 2*time.Second
	afterTimeout  = negotiateTimeout + 3*time.Second
)

// TestBSVP2PTimeouts ports p2p-timeouts.py, which attaches three peers that each
// stall the handshake in a different way and checks when the node gives up on
// them. Upstream's timeline is 1s, +30s, +31s against a 60-second deadline;
// Teranode's deadline is 30 seconds, so this runs to about 33 and says so rather
// than silently rescaling.
//
// The three peers get three different answers, and only one of them is upstream's:
//
//   - A peer that sends nothing is dropped when negotiation times out. That is
//     upstream's assertion with a different constant, and it is reproduced.
//   - A peer that sends a ping without a version is dropped at once, not on a
//     deadline: negotiateInboundProtocol rejects the first message that is neither
//     a version nor a createstream. Stricter than bitcoin-sv, which tolerates the
//     ping and waits out the full 60 seconds.
//   - A peer that sends a version and never a verack is not dropped at all. The
//     negotiation deadline does not apply to it, because as far as Teranode is
//     concerned negotiation finished when the version arrived. This is the same
//     root cause as the pre-handshake-message-leak gap, seen from the timeout side:
//     there is no verack deadline because there is no verack requirement.
//
// The wall clock is the point of the script rather than an accident of it, so this
// port costs about 33 seconds and cannot be made much cheaper. All three peers are
// attached at once so one wait covers them, against upstream's 62.
func TestBSVP2PTimeouts(t *testing.T) {
	// P2P is enabled because the last subtest reads getpeerinfo, which costs ~10s
	// per call without it - measured here as exactly that before the switch. See the
	// getpeerinfo-stalls-without-p2p-service gap.
	td := wirepeer.NewLegacyDaemonWithP2P(t)
	defer td.Stop(t)

	start := time.Now()

	// Upstream's no_verack_node: sends a version, ignores the node's, never acks.
	noVerack := wirepeer.Connect(t, td, wirepeer.SkipHandshakeWait())
	defer noVerack.Close()

	// Upstream's no_version_node: never sends a version, only a ping.
	noVersion := wirepeer.DialRaw(t, td)
	defer noVersion.Close()

	// Upstream's no_send_node: never sends anything at all.
	noSend := wirepeer.DialRaw(t, td)
	defer noSend.Close()

	// Upstream: sleep(1) then assert all three are connected, before any ping is
	// sent. Order matters here in a way it does not upstream - the ping is what
	// costs noVersion its connection, so this has to be checked first or the
	// reproduction would be of the wrong thing.
	t.Run("all three connections are accepted", func(t *testing.T) {
		require.False(t, noVersion.ClosedWithin(t, time.Second),
			"a connection that has sent nothing yet should be accepted and held")
		require.False(t, noSend.ClosedWithin(t, time.Second),
			"a connection that has sent nothing yet should be accepted and held")

		noVerack.Wait(t, 10*time.Second, "version from node",
			func(msg wire.Message) bool { return msg.Command() == wire.CmdVersion })
	})

	// Upstream sends a ping to no_verack_node and no_version_node at this point,
	// and expects both to survive another 30 seconds.
	noVerack.Send(t, wire.NewMsgPing(1))
	noVersion.WriteFrame(t, wire.CmdPing, make([]byte, 8))

	t.Run("a ping before any version ends the connection immediately", func(t *testing.T) {
		// Upstream expects this peer to live until the 60-second deadline. Teranode
		// rejects the first message that is not a version and closes, so the ping
		// itself is fatal. Well inside beforeTimeout, so the disconnect is
		// attributable to the ping rather than to any deadline.
		require.True(t, noVersion.ClosedWithin(t, 10*time.Second),
			"a ping sent before any version should be rejected outright")

		require.Less(t, time.Since(start), beforeTimeout,
			"the disconnect should be prompt, not on the negotiation deadline")
	})

	t.Run("a silent connection survives until the negotiation deadline", func(t *testing.T) {
		waitUntil(t, start, beforeTimeout)

		require.False(t, noSend.ClosedWithin(t, time.Second),
			"a silent connection should still be held just before the negotiation deadline")

		waitUntil(t, start, afterTimeout)

		// Upstream's assertion, at Teranode's constant rather than bitcoin-sv's.
		require.True(t, noSend.ClosedWithin(t, 10*time.Second),
			"a silent connection should be dropped once negotiation times out")
	})

	t.Run("a peer that never sends a verack is never dropped", func(t *testing.T) {
		// Upstream's assertion is that this peer is gone by now. It is not, and the
		// negotiation deadline has already passed for the silent peer above, so the
		// deadline demonstrably does not apply once a version has arrived.
		require.Greater(t, time.Since(start), negotiateTimeout,
			"this check is only meaningful past the negotiation deadline")

		require.True(t, noVerack.Connected(),
			"TRIPWIRE: Teranode now drops a peer that never sent a verack. bitcoin-sv drops it on a "+
				"deadline; if Teranode has gained one, revisit the pre-handshake-message-leak gap and "+
				"assert upstream's disconnect instead of this tolerance")

		// And it is still a peer as far as the node is concerned, which is why no
		// deadline applies: negotiation is over, so nothing is waiting on it.
		require.NotEmpty(t, tryPeers(td),
			"the half-handshaked peer should still be counted as connected")
	})
}

// suspendSlack is how far past a sampling point the clock may drift before this
// port treats its own measurement as void rather than failed.
//
// Everything here is a statement about where the node's 30-second deadline falls
// relative to real elapsed time, so anything that moves the clock underneath the
// test destroys the measurement. A suspended machine does exactly that, and it is
// not hypothetical: a suite run during this exercise reported 163s of test time
// inside 78 minutes of wall clock at 0% CPU, having been asleep in between. On the
// far side of that gap every peer has been dropped, for reasons that have nothing
// to do with what is being tested.
//
// Generous, because ordinary scheduling delay on a loaded machine is milliseconds
// and the thing being screened out is minutes.
const suspendSlack = 10 * time.Second

// waitUntil sleeps until the given offset from start has elapsed. Plain sleeping
// rather than polling, because the thing being waited for is the clock.
//
// It skips rather than continues if the clock jumped past the target by more than
// suspendSlack. Skipping is the honest outcome: a failure here would report that
// the node behaved wrongly, when what actually happened is that the test stopped
// running for a while.
func waitUntil(t *testing.T, start time.Time, offset time.Duration) {
	t.Helper()

	if remaining := offset - time.Since(start); remaining > 0 {
		time.Sleep(remaining)
	}

	if overshoot := time.Since(start) - offset; overshoot > suspendSlack {
		t.Skipf("the clock jumped %s past the %s sampling point, most likely because the machine "+
			"suspended; this port measures wall-clock deadlines, so the measurement is void rather "+
			"than failed", overshoot.Round(time.Second), offset)
	}
}
