package bsv

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/test/utils/wirepeer"
	"github.com/stretchr/testify/require"
)

// banWaitTimeout is generous because the legacy server bans asynchronously: the
// version handler hands the peer to server.banPeers and the ban lands on a later
// iteration of the peer handler loop.
const banWaitTimeout = 20 * time.Second

// loopback is the address the node sees a wirepeer connecting from, and so the
// address a resulting ban is recorded against.
const loopback = "127.0.0.1"

// TestBSVBanUserAgents is the Teranode port of bitcoin-sv's
// bsv-ban-useragents.py.
//
// Upstream tests bitcoind's *configurable* client-user-agent ban list
// (-banclientua / -allowclientua) plus the built-in default that bans
// Bitcoin-Cash-flavoured agents. Teranode implements the inverse and
// non-configurable rule: services/legacy/peer_server.go OnVersion admits only
// peers whose user agent contains "Bitcoin SV" or "BSV", and bans everything
// else. The assertions that survive that difference are ported here; the ones
// that depend on the missing configuration are waived in registry.yaml.
//
// Reproduced from upstream:
//   - a peer whose user agent matches the ban rule is banned, and appears in
//     the node's ban list by address (upstream: listbanned contains our IP)
//   - a Bitcoin-Cash-flavoured agent is banned under default settings
//   - a peer whose user agent does not match the ban rule stays connected
//
// Added beyond upstream, because Teranode's ban is worth pinning down: the ban
// is enforced on reconnect, not merely recorded.
//
// The subtests share one daemon and run in order, mirroring upstream's use of
// clearbanned between cases. The order is load-bearing: a ban is recorded
// against the loopback address and so applies to every later connection until
// cleared, which is why the admitted case runs first and each ban case clears up
// after itself. One daemon is also a harness constraint — see PORTING.md, a
// TestDaemon cannot be stopped and replaced within a single process.
func TestBSVBanUserAgents(t *testing.T) {
	td := wirepeer.NewLegacyDaemon(t)
	defer td.Stop(t)

	t.Run("BSV agent is admitted and not banned", func(t *testing.T) {
		// wirepeer's default agent is BSV-flavoured, so this is the compliant
		// case. Connect returns only after verack in both directions.
		p := wirepeer.Connect(t, td)
		defer p.Close()

		require.True(t, p.Connected(), "a BSV user agent should complete the handshake")
		require.Zero(t, p.Count(wire.CmdReject), "a BSV user agent should not be rejected")

		// Assert the ban never arrives, rather than that it has not arrived yet.
		require.Never(t, func() bool {
			return len(wirepeer.ListBanned(t, td)) > 0
		}, 3*time.Second, 250*time.Millisecond, "a compliant peer must not be banned")

		require.True(t, p.Connected(), "a BSV user agent should stay connected")
	})

	t.Run("non-BSV agent is rejected and banned", func(t *testing.T) {
		// Clear on the way out even if an assertion fails, or the ban leaks into
		// the next subtest and fails it for the wrong reason.
		t.Cleanup(func() { wirepeer.ClearBanned(t, td) })

		// SkipHandshakeWait: the node answers this version with a reject and a
		// disconnect, so there will never be a verack to wait for.
		p := wirepeer.Connect(t, td,
			wirepeer.WithUserAgent("ClientA", "1.0"),
			wirepeer.SkipHandshakeWait())
		defer p.Close()

		reject := p.WaitForReject(t, 30*time.Second)
		require.Equal(t, wire.CmdVersion, reject.Cmd, "reject should name the version message")
		require.Contains(t, reject.Reason, "BSV", "reject reason should explain the BSV-only rule")

		banned := wirepeer.WaitForBan(t, td, loopback, banWaitTimeout)
		t.Logf("ban list after rejection: %v", banned)

		// The ban is by address (banlist.IsBanned strips the port), so even a
		// compliant agent from the same address must now be refused service:
		// peer_server.go handleAddPeerMsg drops peers in the ban list before the
		// handshake can proceed.
		//
		// Asserted as silence rather than as a closed socket, deliberately. An
		// admitted peer gets version and verack within milliseconds; a banned one
		// was observed to get nothing at all, but the node does not close the
		// connection promptly either, so asserting on closure would be asserting
		// something that was not observed.
		again := wirepeer.Connect(t, td, wirepeer.SkipHandshakeWait())
		defer again.Close()

		again.AssertNotReceived(t, 5*time.Second, "version from a banned address", func(m wire.Message) bool {
			return m.Command() == wire.CmdVersion
		})
	})

	t.Run("BCH-flavoured agent is banned under default settings", func(t *testing.T) {
		t.Cleanup(func() { wirepeer.ClearBanned(t, td) })

		// The upstream case: an agent naming a Bitcoin Cash implementation.
		// Teranode reaches the same outcome through the allowlist rather than a
		// BCH-specific pattern, which is the point of porting it.
		p := wirepeer.Connect(t, td,
			wirepeer.WithUserAgent("ThisIsAnABCClient", "0.1"),
			wirepeer.SkipHandshakeWait())
		defer p.Close()

		reject := p.WaitForReject(t, 30*time.Second)
		require.Equal(t, wire.CmdVersion, reject.Cmd, "reject should name the version message")

		banned := wirepeer.WaitForBan(t, td, loopback, banWaitTimeout)
		t.Logf("ban list after rejection: %v", banned)
	})
}
