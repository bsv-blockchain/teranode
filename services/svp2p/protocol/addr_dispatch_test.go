package protocol

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// newAddrTestManager is newTestManager with an address table wired in, which
// is what turns getaddr and addr handling on (manager.go SetAddrMan).
// Persistence is off — an empty AddrManOptions.Path is the disabled state
// (addrman_persist.go) and is also the production default, since
// legacy_savePeers is false.
func newAddrTestManager(t *testing.T) (*PeerManager, *AddrMan) {
	t.Helper()

	m := newTestManager(t, nil)

	addrMan := NewAddrMan(ulogger.TestLogger{}, AddrManOptions{})
	m.SetAddrMan(addrMan)

	return m, addrMan
}

// seedAddrMan puts n routable addresses in the table, each on its own /16 so
// no two share a network group. n has to be comfortably above 5: GetAddr
// returns ADDRMAN_GETADDR_MAX_PCT (23) percent of the table, so a table of
// four yields nothing at all.
func seedAddrMan(t *testing.T, addrMan *AddrMan, n int) {
	t.Helper()

	now := time.Now().Unix()

	for i := 0; i < n; i++ {
		addrMan.Add(NewAddressAtTime(net.ParseIP(addrListIP(i)), 8333, wire.SFNodeNetwork, now), net.ParseIP("45.250.0.1"), 0)
	}

	// The exact count is deliberately NOT asserted: CAddrMan is a stochastic
	// structure and addOne drops an entry whose bucket slot is already taken
	// (addrman.go addOne), so a seed of n reliably lands somewhat fewer than n.
	// What the callers actually need is that GetAddr has something to return,
	// which is a stronger and more relevant statement than any size.
	require.NotEmpty(t, addrMan.GetAddr(), "the seeded table yields no addresses")
}

// addrReadWindow is how long readOrNothing waits for a message that may never
// come. It cannot be a message fence instead of a clock: the transport writes
// on TWO lanes, a priority one for handshake replies and a droppable one for
// machine output (transport/conn.go Send/SendPriority), so a ping written after
// the message under test can be answered with a pong that OVERTAKES the addr
// still queued on the other lane. A pong fence would therefore report "nothing
// was sent" for a message that was merely still in flight — the exact
// false negative this window avoids. Localhost with no artificial delay
// answers well inside it.
const addrReadWindow = 3 * time.Second

// readOrNothing reads until want arrives or the read window expires, and
// returns nil in the second case. Pings and pongs on the way are skipped
// rather than being treated as an answer, for the lane reason above.
func (s *scriptedPeer) readOrNothing(t *testing.T, want string) wire.Message {
	t.Helper()

	deadline := time.Now().Add(addrReadWindow)

	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil
		}

		require.NoError(t, s.nc.SetReadDeadline(time.Now().Add(remaining)))

		_, msg, _, err := wire.ReadMessageWithEncodingN(s.nc, wire.ProtocolVersion, wire.MainNet, wire.BaseEncoding)
		if err != nil {
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				return nil
			}

			require.NoError(t, err)
		}

		if msg.Command() == want {
			return msg
		}
	}
}

// TestManagerAnswersGetAddrForInboundPeerOnce is ProcessGetAddrMessage's
// fSentAddr gate (net_processing.cpp:4111-4119) and legacy's sentAddrs
// (services/legacy/peer_server.go:1753-1758), end to end over a socket.
func TestManagerAnswersGetAddrForInboundPeerOnce(t *testing.T) {
	m, addrMan := newAddrTestManager(t)
	seedAddrMan(t, addrMan, 40)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, m.Start(ctx, []string{"127.0.0.1:0"}))

	defer func() { require.NoError(t, m.Stop()) }()

	far := dialScripted(t, m.ListenAddrs()[0])
	far.completeOutboundHandshake(t)

	far.write(t, wire.NewMsgGetAddr())

	first := far.readOrNothing(t, wire.CmdAddr)
	require.NotNil(t, first, "an inbound peer's first getaddr must be answered")
	require.NotEmpty(t, first.(*wire.MsgAddr).AddrList)

	// "Only send one GetAddr response per connection to reduce resource waste
	// and discourage addr stamping of INV announcements."
	far.write(t, wire.NewMsgGetAddr())
	require.Nil(t, far.readOrNothing(t, wire.CmdAddr), "a repeated getaddr must be ignored")

	require.NoError(t, far.nc.Close())
}

// TestManagerIgnoresGetAddrFromOutboundPeer is the fingerprinting defense
// (net_processing.cpp:4102-4109, legacy peer_server.go:1748-1752), and in the
// same run proves the outbound side ASKS for addresses once its handshake
// completes (net_processing.cpp:1867-1871).
func TestManagerIgnoresGetAddrFromOutboundPeer(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	defer func() { _ = ln.Close() }()

	tSettings := managerSettings()
	tSettings.Legacy.ConnectPeers = []string{ln.Addr().String()}

	banList, err := NewBanList("")
	require.NoError(t, err)

	m := NewPeerManager(ulogger.TestLogger{}, tSettings, banList)

	addrMan := NewAddrMan(ulogger.TestLogger{}, AddrManOptions{})
	m.SetAddrMan(addrMan)
	seedAddrMan(t, addrMan, 40)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, m.Start(ctx, nil))

	defer func() { require.NoError(t, m.Stop()) }()

	nc, err := ln.Accept()
	require.NoError(t, err)

	far := &scriptedPeer{nc: nc}

	// The manager dialled us, so it is the outbound side: it sends version
	// first, and our verack must draw its getaddr.
	require.IsType(t, &wire.MsgVersion{}, far.read(t))
	far.write(t, remoteVersion(4321))

	sawVerack := false
	for !sawVerack {
		if _, ok := far.read(t).(*wire.MsgVerAck); ok {
			sawVerack = true
		}
	}

	far.write(t, wire.NewMsgVerAck())

	require.IsType(t, &wire.MsgGetAddr{}, far.readUntil(t, wire.CmdGetAddr))

	// Now the other direction: this connection is OUTBOUND from the manager's
	// point of view, so our getaddr must be ignored.
	far.write(t, wire.NewMsgGetAddr())
	require.Nil(t, far.readOrNothing(t, wire.CmdAddr), "a getaddr from an outbound peer must be ignored")

	require.NoError(t, nc.Close())
}

// TestManagerDisconnectsOnEmptyAddr is legacy's OnAddr empty-list rule
// (services/legacy/peer_server.go:1795-1801).
func TestManagerDisconnectsOnEmptyAddr(t *testing.T) {
	m, _ := newAddrTestManager(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, m.Start(ctx, []string{"127.0.0.1:0"}))

	defer func() { require.NoError(t, m.Stop()) }()

	far := dialScripted(t, m.ListenAddrs()[0])
	far.completeOutboundHandshake(t)

	require.Eventually(t, func() bool { return establishedCount(m) == 1 }, 5*time.Second, 20*time.Millisecond)

	far.write(t, wire.NewMsgAddr())

	require.Eventually(t, func() bool { return m.ConnectedCount() == 0 }, 5*time.Second, 20*time.Millisecond)

	require.NoError(t, far.nc.Close())
}

// TestManagerStoresSolicitedAddrEntries drives the whole addr path over a
// socket for a connection that WAS asked for addresses, so the
// unsolicited-addr fence (net_processing.cpp:2302-2332) is lifted and the
// entries reach the address table with the two-hour source penalty
// (:2362).
func TestManagerStoresSolicitedAddrEntries(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	defer func() { _ = ln.Close() }()

	tSettings := managerSettings()
	tSettings.Legacy.ConnectPeers = []string{ln.Addr().String()}

	banList, err := NewBanList("")
	require.NoError(t, err)

	m := NewPeerManager(ulogger.TestLogger{}, tSettings, banList)

	addrMan := NewAddrMan(ulogger.TestLogger{}, AddrManOptions{})
	m.SetAddrMan(addrMan)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, m.Start(ctx, nil))

	defer func() { require.NoError(t, m.Stop()) }()

	nc, err := ln.Accept()
	require.NoError(t, err)

	far := &scriptedPeer{nc: nc}

	require.IsType(t, &wire.MsgVersion{}, far.read(t))
	far.write(t, remoteVersion(4321))

	sawVerack := false
	for !sawVerack {
		if _, ok := far.read(t).(*wire.MsgVerAck); ok {
			sawVerack = true
		}
	}

	far.write(t, wire.NewMsgVerAck())
	require.IsType(t, &wire.MsgGetAddr{}, far.readUntil(t, wire.CmdGetAddr))

	now := time.Now().Unix()

	reply := wire.NewMsgAddr()
	require.NoError(t, reply.AddAddresses(
		addrAt("45.60.0.1", 8333, wire.SFNodeNetwork, now),
		addrAt("45.61.0.1", 8333, wire.SFNodeNetwork, now),
		// Skipped: no NODE_NETWORK (net_processing.cpp:2344).
		addrAt("45.62.0.1", 8333, 0, now),
		// Marked known and then dropped: RFC1918 is unroutable
		// (net_processing.cpp:2357-2359).
		addrAt("10.1.2.3", 8333, wire.SFNodeNetwork, now),
	))

	far.write(t, reply)

	require.Eventually(t, func() bool { return addrMan.Size() == 2 }, 5*time.Second, 20*time.Millisecond)

	// A second addr on the same connection is unsolicited: fGetAddr was
	// cleared by the exchange (net_processing.cpp:2290-2291). This peer is
	// outbound from our side, so the inbound-only fence does not apply and the
	// entry is still processed — which is what makes the count grow to three.
	second := wire.NewMsgAddr()
	require.NoError(t, second.AddAddresses(addrAt("45.63.0.1", 8333, wire.SFNodeNetwork, now)))
	far.write(t, second)

	require.Eventually(t, func() bool { return addrMan.Size() == 3 }, 5*time.Second, 20*time.Millisecond)

	require.NoError(t, nc.Close())
}

// TestManagerDropsUnsolicitedInboundAddr is the anti-flood fence
// (net_processing.cpp:2302-2332): an inbound peer that was never asked may
// only tell us about itself, so an addr full of other hosts stores nothing.
func TestManagerDropsUnsolicitedInboundAddr(t *testing.T) {
	m, addrMan := newAddrTestManager(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, m.Start(ctx, []string{"127.0.0.1:0"}))

	defer func() { require.NoError(t, m.Stop()) }()

	far := dialScripted(t, m.ListenAddrs()[0])
	far.completeOutboundHandshake(t)

	require.Eventually(t, func() bool { return establishedCount(m) == 1 }, 5*time.Second, 20*time.Millisecond)

	now := time.Now().Unix()

	flood := wire.NewMsgAddr()
	require.NoError(t, flood.AddAddresses(
		addrAt("45.70.0.1", 8333, wire.SFNodeNetwork, now),
		addrAt("45.71.0.1", 8333, wire.SFNodeNetwork, now),
	))

	far.write(t, flood)

	// The fence is not an error, so the peer stays: a pong round trip proves
	// it, and by the time it comes back the addr has been handled.
	far.write(t, wire.NewMsgPing(0x1234))
	pong := far.readUntil(t, wire.CmdPong)
	require.Equal(t, uint64(0x1234), pong.(*wire.MsgPong).Nonce)

	require.Equal(t, 0, addrMan.Size())
	require.Equal(t, int32(1), m.ConnectedCount())

	require.NoError(t, far.nc.Close())
}

// TestManagerForwardsAddrToInboundPeers is RelayAddress
// (net_processing.cpp:998-1041). SVNode is the only source of truth here: the
// legacy service dropped addr forwarding entirely.
//
// The source connection is OUTBOUND (the manager dials the sender), so the
// unsolicited fence does not filter the batch; the two forwarding targets are
// INBOUND, which is the only kind RelayAddress considers (:1017).
func TestManagerForwardsAddrToInboundPeers(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	defer func() { _ = ln.Close() }()

	tSettings := managerSettings()
	tSettings.Legacy.ConnectPeers = []string{ln.Addr().String()}

	banList, err := NewBanList("")
	require.NoError(t, err)

	m := NewPeerManager(ulogger.TestLogger{}, tSettings, banList)

	addrMan := NewAddrMan(ulogger.TestLogger{}, AddrManOptions{})
	m.SetAddrMan(addrMan)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, m.Start(ctx, []string{"127.0.0.1:0"}))

	defer func() { require.NoError(t, m.Stop()) }()

	// Three inbound peers, so the reachable width of two has something to
	// choose between and one peer is provably left out.
	const inboundPeers = 3

	inbound := make([]*scriptedPeer, 0, inboundPeers)

	for i := 0; i < inboundPeers; i++ {
		far := dialScripted(t, m.ListenAddrs()[0])
		far.completeOutboundHandshakeAs(t, remoteVersion(uint64(9000+i))) //nolint:gosec // small loop bound

		inbound = append(inbound, far)
	}

	// Only the three inbound peers can be established yet: the connection the
	// manager dialled is registered but has not been handshaken, because this
	// test has not accepted it.
	require.Eventually(t, func() bool { return establishedCount(m) == inboundPeers }, 10*time.Second, 20*time.Millisecond)

	// The outbound connection the manager dialled: it is the addr source.
	nc, err := ln.Accept()
	require.NoError(t, err)

	defer func() { _ = nc.Close() }()

	source := &scriptedPeer{nc: nc}

	require.IsType(t, &wire.MsgVersion{}, source.read(t))
	source.write(t, remoteVersion(4321))

	sawVerack := false
	for !sawVerack {
		if _, ok := source.read(t).(*wire.MsgVerAck); ok {
			sawVerack = true
		}
	}

	source.write(t, wire.NewMsgVerAck())
	require.IsType(t, &wire.MsgGetAddr{}, source.readUntil(t, wire.CmdGetAddr))

	relayed := addrAt("45.80.0.1", 8333, wire.SFNodeNetwork, time.Now().Unix())

	msg := wire.NewMsgAddr()
	require.NoError(t, msg.AddAddresses(relayed))
	source.write(t, msg)

	// Exactly nRelayNodes of the inbound peers receive it — two, because the
	// address is routable (net_processing.cpp:1000-1001).
	got := 0

	for _, far := range inbound {
		if out := far.readOrNothing(t, wire.CmdAddr); out != nil {
			require.Len(t, out.(*wire.MsgAddr).AddrList, 1)
			require.Equal(t, relayed.IP.String(), out.(*wire.MsgAddr).AddrList[0].IP.String())

			got++
		}
	}

	require.Equal(t, addrRelayNodesReachable, got)

	// The source is never handed its own address back: its addrKnown was
	// marked before the relay pass chose targets (net_processing.cpp:2350
	// before :2355).
	require.Nil(t, source.readOrNothing(t, wire.CmdAddr))

	for _, far := range inbound {
		require.NoError(t, far.nc.Close())
	}
}
