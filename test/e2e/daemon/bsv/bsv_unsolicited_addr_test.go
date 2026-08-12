package bsv

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/test/utils/wirepeer"
	"github.com/stretchr/testify/require"
)

const (
	// unsolicitedAddrIP and the port range are upstream's exactly:
	// 104.20.31.65:10000 through :10009. Kept verbatim so the two files line up,
	// and because the address needs to be routable for the address manager to
	// keep it - addrmgr.IsRoutable discards private ranges, so a 192.168 address
	// would make this test pass by being thrown away for the wrong reason.
	unsolicitedAddrIP        = "104.20.31.65"
	unsolicitedAddrFirstPort = 10000
	unsolicitedAddrCount     = 10

	// getAddrAttempts bounds how many fresh peers ask for addresses before the
	// port gives up. More than one is needed because AddressCache samples rather
	// than returning everything: addrmgr returns len(known) * getAddrPercent / 100
	// entries, chosen by a Fisher-Yates shuffle, which with getAddrPercent = 23
	// and ten known addresses is two of them - and which two is random. One
	// attempt would therefore be a coin toss if the node knew addresses beyond
	// the ten injected here.
	getAddrAttempts = 3
)

// TestBSVUnsolicitedAddr ports p2p-unsolicited_addr.py, whose docstring says it
// checks that unsolicited ADDR messages are neither accepted nor relayed.
//
// Upstream meshes two real nodes, feeds an unsolicited addr to the first, waits
// 60 seconds, and asserts that neither of its two test connections ever received
// an addr - the reasoning being that if node0 had accepted the addresses it would
// have relayed them onward and they would have come back.
//
// This port reaches a more interesting result than a straight translation would.
// Upstream's assertion holds against Teranode - and it holds for a reason that has
// nothing to do with the property being tested, while the property itself is
// violated:
//
//   - No addr is ever relayed unsolicited, so upstream's observation is vacuously
//     satisfied. serverPeer.pushAddrMsg has exactly two callers: OnGetAddr, which
//     answers a request, and the OnVersion path at peer_server.go:642, which is
//     guarded by !isInbound and so never fires for a peer that dialled us. There
//     is no Poisson-delayed relay to wait 60 seconds for, which is also why the
//     windows below are seconds rather than a minute - the absence is structural,
//     not probabilistic.
//   - The addresses ARE accepted. OnAddr (peer_server.go:1779) adds every address
//     to the address manager with no check that any were asked for, and a
//     subsequent getaddr from an unrelated peer hands them back out. That is the
//     half upstream names in its own docstring and cannot observe with its method.
//
// So the port asserts upstream's assertion, and then asserts the acceptance that
// upstream's technique misses, as a tripwire. See the unsolicited-addr-accepted
// gap.
//
// Upstream also passes -whitelist=127.0.0.1 to both nodes. That is not reproduced
// and does not need to be: the legacy whitelist cannot be configured at all (see
// the legacy-whitelist-inert gap), and nothing in this test depends on it.
func TestBSVUnsolicitedAddr(t *testing.T) {
	td := wirepeer.NewLegacyDaemon(t)
	defer td.Stop(t)

	sender := wirepeer.Connect(t, td)
	defer sender.Close()

	// Upstream: node.wait_for_verack() for each connection, then assert gotAddr is
	// False at the end. Checked before the injection as well as after, so that a
	// node which volunteers addresses to every inbound peer is distinguished from
	// one which relays these particular addresses.
	sender.AssertNotReceived(t, 3*time.Second, "an unsolicited addr before we sent one",
		func(msg wire.Message) bool { return msg.Command() == wire.CmdAddr })

	injected := injectUnsolicitedAddr(t, sender)

	// Upstream's implicit assertion: feeding an addr does not cost the sender its
	// connection. Teranode does disconnect a peer that sends an addr with an EMPTY
	// address list (OnAddr calls DisconnectWithWarning for that), so this is worth
	// pinning rather than assuming.
	sender.AssertStillConnected(t, 2*time.Second,
		"sending an addr with addresses in it must not cost the peer its connection")

	t.Run("no peer is sent an addr it did not ask for", func(t *testing.T) {
		// Upstream's actual assertion, for both of its connections. Reproduced on
		// the sender, and on a second peer that stands in for upstream's node1
		// connection - the one that would have received a relayed addr.
		observer := wirepeer.Connect(t, td)
		defer observer.Close()

		matchAddr := func(msg wire.Message) bool { return msg.Command() == wire.CmdAddr }

		observer.AssertNotReceived(t, 3*time.Second, "a relayed addr on a peer that never asked", matchAddr)
		sender.AssertNotReceived(t, time.Second, "an addr echoed back to the sender", matchAddr)
	})

	t.Run("but the addresses were accepted, and getaddr hands them back", func(t *testing.T) {
		// The half upstream names and cannot see. bitcoin-sv does not accept
		// unsolicited addresses at all, so a getaddr there would return nothing of
		// what was injected.
		found := findInjectedAddresses(t, td, injected)

		require.NotEmpty(t, found,
			"TRIPWIRE: the node no longer hands back addresses it was given unsolicited, which is "+
				"bitcoin-sv's behaviour and the point of this upstream test. Revisit the "+
				"unsolicited-addr-accepted gap and assert the rejection instead")

		t.Logf("getaddr returned %d of the %d unsolicited addresses: %v",
			len(found), len(injected), found)
	})
}

// injectUnsolicitedAddr sends upstream's addr message and returns the set of
// "ip:port" strings it announced.
func injectUnsolicitedAddr(t *testing.T, p *wirepeer.Peer) map[string]bool {
	t.Helper()

	msg := wire.NewMsgAddr()
	announced := make(map[string]bool, unsolicitedAddrCount)

	ip := net.ParseIP(unsolicitedAddrIP)
	require.NotNil(t, ip, "parse %s", unsolicitedAddrIP)

	for i := range unsolicitedAddrCount {
		port := uint16(unsolicitedAddrFirstPort + i)

		require.NoError(t, msg.AddAddress(wire.NewNetAddressIPPort(ip, port, wire.SFNodeNetwork)),
			"add %s:%d to the addr message", unsolicitedAddrIP, port)

		announced[fmt.Sprintf("%s:%d", unsolicitedAddrIP, port)] = true
	}

	p.Send(t, msg)

	return announced
}

// findInjectedAddresses asks the node for addresses and returns whichever of the
// injected ones it hands back.
//
// A fresh peer per attempt, because OnGetAddr answers at most one getaddr per
// connection (the sentAddrs flag, there to discourage address stamping), so
// retrying on the same peer would silently do nothing. Each peer is closed before
// the next connects, keeping well clear of MaxPeersPerIP.
func findInjectedAddresses(t *testing.T, td *daemon.TestDaemon, injected map[string]bool) []string {
	t.Helper()

	for attempt := range getAddrAttempts {
		found := getAddrOnce(t, td, injected, attempt)
		if len(found) > 0 {
			return found
		}
	}

	return nil
}

// getAddrOnce runs a single getaddr exchange on its own connection.
func getAddrOnce(t *testing.T, td *daemon.TestDaemon, injected map[string]bool, attempt int) []string {
	t.Helper()

	p := wirepeer.Connect(t, td)
	defer p.Close()

	p.Send(t, wire.NewMsgGetAddr())

	// A getaddr that returns nothing at all is a different failure from one that
	// returns other addresses, so wait for the reply rather than polling the
	// contents.
	reply := p.Wait(t, 15*time.Second, fmt.Sprintf("addr in reply to getaddr (attempt %d)", attempt),
		func(msg wire.Message) bool { return msg.Command() == wire.CmdAddr })

	msg, ok := reply.(*wire.MsgAddr)
	require.True(t, ok, "the reply to getaddr should decode as MsgAddr")

	var found []string

	for _, na := range msg.AddrList {
		key := fmt.Sprintf("%s:%d", na.IP, na.Port)
		if injected[key] {
			found = append(found, key)
		}
	}

	return found
}
