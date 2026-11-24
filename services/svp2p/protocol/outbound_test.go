package protocol

import (
	"context"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// errTestNoRoute is what the harness dial seam answers for an address no test
// peer is listening on — the harness stand-in for a refused TCP connection.
var errTestNoRoute = errors.New(errors.ERR_ERROR, "svp2p test: no route")

// outboundTestPeer is one listening node the dialer can reach.
//
// Its address in the table is a ROUTABLE test address, never a loopback one,
// because CAddrMan refuses to store an unroutable address at all
// (CAddrMan::Add_, addrman.cpp:255 — addrman.go addOne carries it). SVNode has
// exactly the same refusal, which is why a loopback peer is reached through
// `-connect` there and through legacy_connect_peers here, and never through
// the address table. So the harness maps the routable address onto a loopback
// listener at the dial seam: the network is the one dependency a test may
// stand in for, and everything above the socket — the candidate walk, the
// netgroup accounting, Attempt/Good, the real handshake over a real TCP
// connection — runs as production runs it.
type outboundTestPeer struct {
	addr Address
	ln   net.Listener

	// bitcoinNet is the wire magic this peer frames with. It has to be the
	// manager's own network, which is settings-driven and is NOT mainnet under
	// every context (regtest under a plain dev context), or every frame is
	// discarded before it is ever parsed.
	bitcoinNet wire.BitcoinNet

	mu         sync.Mutex
	handshakes int
	getAddrs   int
}

// nextTestPeerNonce keeps every scripted version message's nonce distinct, so
// no session can be mistaken for a self-connection (handshake.go's
// CheckIncomingNonce).
var nextTestPeerNonce atomic.Uint64

func newOutboundTestPeer(t *testing.T, ip string) *outboundTestPeer {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	t.Cleanup(func() { _ = ln.Close() })

	p := &outboundTestPeer{
		addr:       NewAddressAtTime(net.ParseIP(ip), 8333, wire.SFNodeNetwork, time.Now().Unix()),
		ln:         ln,
		bitcoinNet: wire.MainNet,
	}

	go p.serve()

	return p
}

func (p *outboundTestPeer) serve() {
	for {
		nc, err := p.ln.Accept()
		if err != nil {
			return
		}

		go p.session(nc)
	}
}

// session is the INBOUND half of the handshake: the manager dialled us, so it
// speaks version first. Errors end the session rather than failing the test,
// because this runs off the test goroutine; every assertion is made by the
// test itself against the counters below.
func (p *outboundTestPeer) session(nc net.Conn) {
	defer func() { _ = nc.Close() }()

	msg, err := readTestMsg(nc, p.bitcoinNet)
	if err != nil {
		return
	}

	if _, ok := msg.(*wire.MsgVersion); !ok {
		return
	}

	if err := writeTestMsg(nc, p.bitcoinNet, remoteVersion(nextTestPeerNonce.Add(1))); err != nil {
		return
	}

	for {
		msg, err := readTestMsg(nc, p.bitcoinNet)
		if err != nil {
			return
		}

		switch m := msg.(type) {
		case *wire.MsgVerAck:
			if err := writeTestMsg(nc, p.bitcoinNet, wire.NewMsgVerAck()); err != nil {
				return
			}

			p.mu.Lock()
			p.handshakes++
			p.mu.Unlock()

		case *wire.MsgPing:
			if err := writeTestMsg(nc, p.bitcoinNet, wire.NewMsgPong(m.Nonce)); err != nil {
				return
			}

		case *wire.MsgGetAddr:
			p.mu.Lock()
			p.getAddrs++
			p.mu.Unlock()
		}
	}
}

func (p *outboundTestPeer) counts() (handshakes, getAddrs int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.handshakes, p.getAddrs
}

// handshakedAndAsked reports whether this peer has completed exactly one
// handshake and been asked for addresses exactly once.
//
// It exists to be POLLED, never to be read once off the back of an
// establishment check on our own side. The two events are not ordered: session
// above writes its verack and increments handshakes afterwards, and our
// Established() closes on receiving that verack, so our manager can be
// established while the peer's own counter is still zero. getAddrs is later
// again, since the manager only sends getaddr once it has established. Reading
// these counters without waiting for them is a race that passes on an idle box
// and fails under a full package run.
func (p *outboundTestPeer) handshakedAndAsked() bool {
	handshakes, getAddrs := p.counts()

	return handshakes == 1 && getAddrs == 1
}

// routedConn makes a loopback socket report the address the dialer asked for as
// its remote address. Without it the manager would see 127.0.0.1:<ephemeral>
// where production sees the address it dialed, and every address-keyed decision
// downstream — the outbound slot accounting, and the Good mark that
// GetAddrRequest makes against the peer's own address — would be reading a
// harness artifact instead of the real value.
type routedConn struct {
	net.Conn

	remote net.Addr
}

func (c routedConn) RemoteAddr() net.Addr { return c.remote }

func readTestMsg(nc net.Conn, bitcoinNet wire.BitcoinNet) (wire.Message, error) {
	if err := nc.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
		return nil, err
	}

	_, msg, _, err := wire.ReadMessageWithEncodingN(nc, wire.ProtocolVersion, bitcoinNet, wire.BaseEncoding)

	return msg, err
}

func writeTestMsg(nc net.Conn, bitcoinNet wire.BitcoinNet, msg wire.Message) error {
	_, err := wire.WriteMessageWithEncodingN(nc, msg, wire.ProtocolVersion, bitcoinNet, wire.BaseEncoding)

	return err
}

// outboundHarness wires a manager whose dial seam redirects the test peers'
// routable addresses to their loopback listeners, and counts every dial the
// dialer starts so a test can assert that one never happened.
type outboundHarness struct {
	m       *PeerManager
	addrMan *AddrMan
	peers   []*outboundTestPeer
	dials   atomic.Int32
}

// outboundTestTick keeps the dialer's cadence out of the test's runtime.
// Production keeps SVNode's own 500ms (net.cpp:1846).
const outboundTestTick = 10 * time.Millisecond

type outboundHarnessOptions struct {
	target      int
	connectTo   []string
	banned      []string
	noAddrMan   bool
	seedPeers   bool
	extraSeeded []Address
}

func newOutboundHarness(t *testing.T, opts outboundHarnessOptions, peers ...*outboundTestPeer) *outboundHarness {
	t.Helper()

	tSettings := managerSettings()
	tSettings.Legacy.TargetOutboundPeers = opts.target
	tSettings.Legacy.ConnectPeers = opts.connectTo

	banList, err := NewBanList("")
	require.NoError(t, err)

	for _, host := range opts.banned {
		require.NoError(t, banList.Add(host, time.Now().Add(time.Hour)))
	}

	h := &outboundHarness{
		m:     NewPeerManager(ulogger.TestLogger{}, tSettings, banList),
		peers: peers,
	}

	redirect := make(map[string]string, len(peers))
	for _, p := range peers {
		redirect[p.addr.String()] = p.ln.Addr().String()
	}

	h.m.outboundTick = outboundTestTick

	// No real DNS in the harness: the manager's chain params are mainnet's,
	// whose seed would otherwise be resolved for real on an empty table and
	// flood it with live addresses (and block the fixed-seed fallback, which
	// only fires on an EMPTY table). Tests that want seeding install their own.
	h.m.dnsLookup = func(context.Context, string) ([]net.IP, error) { return nil, nil }

	h.m.dialTCP = func(addr string) (net.Conn, error) {
		h.dials.Add(1)

		target, ok := redirect[addr]
		if !ok {
			return nil, errTestNoRoute
		}

		nc, err := net.DialTimeout("tcp", target, 5*time.Second)
		if err != nil {
			return nil, err
		}

		remote, err := net.ResolveTCPAddr("tcp", addr)
		if err != nil {
			return nil, err
		}

		return routedConn{Conn: nc, remote: remote}, nil
	}

	if !opts.noAddrMan {
		h.addrMan = NewAddrMan(ulogger.TestLogger{}, AddrManOptions{})
		h.addrMan.makeDeterministic()
		h.m.SetAddrMan(h.addrMan)

		seeded := make([]Address, 0, len(peers)+len(opts.extraSeeded))
		if opts.seedPeers {
			for _, p := range peers {
				seeded = append(seeded, p.addr)
			}
		}

		seeded = append(seeded, opts.extraSeeded...)

		source := net.ParseIP("45.250.0.1")
		for _, a := range seeded {
			require.True(t, h.addrMan.Add(a, source, 0), "seeding %s must land in the table", a)
		}

		require.Equal(t, len(seeded), h.addrMan.Size())
	}

	return h
}

func (h *outboundHarness) start(t *testing.T) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	require.NoError(t, h.m.Start(ctx, nil))
	t.Cleanup(func() { require.NoError(t, h.m.Stop()) })
}

// triedCount reads CAddrMan::nTried, which is what "marked Good" means: good_
// moves the entry out of the new table and into the tried one (addrman.cpp:203,
// makeTried).
func triedCount(a *AddrMan) int {
	a.cs.Lock()
	defer a.cs.Unlock()

	return a.nTried
}

// attemptsOf reads CAddrInfo::nLastTry and nAttempts for one address.
func attemptsOf(a *AddrMan, addr Address) (nLastTry int64, nAttempts int) {
	a.cs.Lock()
	defer a.cs.Unlock()

	info := a.find(newNetAddr(addr.IP()))
	if info == nil {
		return 0, 0
	}

	return info.nLastTry, info.nAttempts
}

// getAddrArmed counts the peers whose fGetAddr flag is set — CNode::fGetAddr,
// which ProcessVersionMessage arms on the outbound side
// (net_processing.cpp:1869).
func getAddrArmed(m *PeerManager) int {
	m.mu.Lock()
	syncPeers := make([]*SyncPeer, 0, len(m.peers))

	for _, sp := range m.peers {
		syncPeers = append(syncPeers, sp)
	}
	m.mu.Unlock()

	m.syncMu.Lock()
	defer m.syncMu.Unlock()

	n := 0

	for _, sp := range syncPeers {
		if sp.fGetAddr {
			n++
		}
	}

	return n
}

// TestOutboundDialerReachesTargetAndMarksGood is Task 19's Step 1 leg: the
// addrman-fed loop (net.cpp ThreadOpenConnections:1839-1991) dials up to the
// target, and each established handshake marks its address Good
// (net_processing.cpp:1872, connman.MarkAddressGood). It also carries the
// folded-in Task 18 leg: an outbound peer sends getaddr after verack and arms
// fGetAddr (net_processing.cpp:1867-1871).
func TestOutboundDialerReachesTargetAndMarksGood(t *testing.T) {
	a := newOutboundTestPeer(t, "45.1.0.1")
	b := newOutboundTestPeer(t, "45.2.0.1")

	h := newOutboundHarness(t, outboundHarnessOptions{target: 2, seedPeers: true}, a, b)
	h.start(t)

	// Both sides of every connection are in the polled condition: ours
	// (establishedCount) and each peer's own bookkeeping. See
	// handshakedAndAsked for why the far side cannot be read off the back of
	// our own establishment check.
	require.Eventually(t, func() bool {
		if establishedCount(h.m) != 2 {
			return false
		}

		for _, p := range h.peers {
			if !p.handshakedAndAsked() {
				return false
			}
		}

		return true
	}, 15*time.Second, 20*time.Millisecond)

	// Re-read exactly, to catch an OVERSHOOT the condition above would have
	// waited through: a second handshake on the same peer.
	for _, p := range h.peers {
		handshakes, getAddrs := p.counts()
		require.Equal(t, 1, handshakes, "peer %s must be handshaked exactly once", p.addr)
		require.Equal(t, 1, getAddrs, "peer %s must be asked for addresses once", p.addr)
	}

	// Good moves both entries into the tried table.
	require.Eventually(t, func() bool { return triedCount(h.addrMan) == 2 }, 5*time.Second, 20*time.Millisecond)

	require.Equal(t, 2, getAddrArmed(h.m))
}

// TestOutboundDialerRespectsTargetCap is the nOutbound >= nMaxOutbound gate
// (net.cpp:1910). With feelers not carried, reaching the target stops the loop
// dialing entirely.
func TestOutboundDialerRespectsTargetCap(t *testing.T) {
	a := newOutboundTestPeer(t, "45.1.0.1")
	b := newOutboundTestPeer(t, "45.2.0.1")

	h := newOutboundHarness(t, outboundHarnessOptions{target: 1, seedPeers: true}, a, b)
	h.start(t)

	require.Eventually(t, func() bool { return establishedCount(h.m) == 1 }, 15*time.Second, 20*time.Millisecond)

	// Many ticks later the cap still holds and no second dial was started.
	time.Sleep(20 * outboundTestTick)

	require.Equal(t, 1, establishedCount(h.m))
	require.Equal(t, int32(1), h.dials.Load())
}

// TestOutboundDialerOnePeerPerNetGroup is setConnected (net.cpp:1878-1895,
// "Only connect out to one peer per network group (/16 for IPv4)"): two
// addresses in one /16 fill one outbound slot between them.
func TestOutboundDialerOnePeerPerNetGroup(t *testing.T) {
	a := newOutboundTestPeer(t, "45.1.0.1")
	b := newOutboundTestPeer(t, "45.1.0.2")

	h := newOutboundHarness(t, outboundHarnessOptions{target: 2, seedPeers: true}, a, b)
	h.start(t)

	require.Eventually(t, func() bool { return establishedCount(h.m) == 1 }, 15*time.Second, 20*time.Millisecond)

	time.Sleep(20 * outboundTestTick)

	require.Equal(t, 1, establishedCount(h.m))
	require.Equal(t, int32(1), h.dials.Load())
}

// TestOutboundDialerNeverDuplicatesAPeer is OpenNetworkConnection's FindNode
// fence (net.cpp:2163-2166): the one address we already hold is not dialed a
// second time, even though the target is not reached.
func TestOutboundDialerNeverDuplicatesAPeer(t *testing.T) {
	a := newOutboundTestPeer(t, "45.1.0.1")

	h := newOutboundHarness(t, outboundHarnessOptions{target: 8, seedPeers: true}, a)
	h.start(t)

	require.Eventually(t, func() bool { return establishedCount(h.m) == 1 }, 15*time.Second, 20*time.Millisecond)

	time.Sleep(20 * outboundTestTick)

	require.Equal(t, 1, establishedCount(h.m))
	require.Equal(t, int32(1), h.dials.Load())
}

// TestOutboundDialerMarksAttemptOnFailedDial is ConnectNode's failure leg
// (net.cpp:435-438): a dial that never connects still stamps nLastTry.
// nAttempts stays at zero because fCountFailure is
// `setConnected.size() >= min(nMaxConnections - 1, 2)` (net.cpp:1988) and this
// node holds no outbound peer at all.
func TestOutboundDialerMarksAttemptOnFailedDial(t *testing.T) {
	dead := NewAddressAtTime(net.ParseIP("45.3.0.1"), 8333, wire.SFNodeNetwork, time.Now().Unix())

	h := newOutboundHarness(t, outboundHarnessOptions{target: 8, extraSeeded: []Address{dead}})
	h.start(t)

	require.Eventually(t, func() bool { return h.dials.Load() > 0 }, 15*time.Second, 20*time.Millisecond)

	require.Eventually(t, func() bool {
		nLastTry, _ := attemptsOf(h.addrMan, dead)
		return nLastTry > 0
	}, 5*time.Second, 20*time.Millisecond)

	_, nAttempts := attemptsOf(h.addrMan, dead)
	require.Equal(t, 0, nAttempts)

	require.Equal(t, 0, establishedCount(h.m))
	require.Equal(t, 0, triedCount(h.addrMan))
}

// TestOutboundDialerSkipsBannedAddress is OpenNetworkConnection's IsBanned
// fence (net.cpp:2157).
func TestOutboundDialerSkipsBannedAddress(t *testing.T) {
	a := newOutboundTestPeer(t, "45.1.0.1")

	h := newOutboundHarness(t, outboundHarnessOptions{target: 8, seedPeers: true, banned: []string{"45.1.0.1"}}, a)
	h.start(t)

	time.Sleep(30 * outboundTestTick)

	require.Equal(t, int32(0), h.dials.Load())
	require.Equal(t, 0, establishedCount(h.m))

	handshakes, _ := a.counts()
	require.Equal(t, 0, handshakes)
}

// TestOutboundDialerNotRunWithConnectPeers is ThreadOpenConnections' `-connect`
// branch (net.cpp:1817-1836): when specific peers are configured the addrman
// loop is never reached at all, so legacy_connect_peers stays dominant.
func TestOutboundDialerNotRunWithConnectPeers(t *testing.T) {
	a := newOutboundTestPeer(t, "45.1.0.1")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	defer func() { _ = ln.Close() }()

	h := newOutboundHarness(t, outboundHarnessOptions{
		target:    8,
		seedPeers: true,
		connectTo: []string{ln.Addr().String()},
	}, a)
	h.start(t)

	// The configured peer is dialed by dialLoop, which shares the seam.
	require.Eventually(t, func() bool { return h.dials.Load() > 0 }, 5*time.Second, 20*time.Millisecond)

	time.Sleep(30 * outboundTestTick)

	handshakes, _ := a.counts()
	require.Equal(t, 0, handshakes, "the addrman loop must not run while legacy_connect_peers is set")
}

// TestOutboundDialerDisabled covers the two states that leave the loop off:
// no address table at all, and a target of zero.
func TestOutboundDialerDisabled(t *testing.T) {
	tests := []struct {
		name string
		opts outboundHarnessOptions
	}{
		{name: "no addrman", opts: outboundHarnessOptions{target: 8, noAddrMan: true}},
		{name: "zero target", opts: outboundHarnessOptions{target: 0, seedPeers: true}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := newOutboundTestPeer(t, "45.1.0.1")

			opts := tc.opts
			h := newOutboundHarness(t, opts, a)
			h.start(t)

			time.Sleep(30 * outboundTestTick)

			require.Equal(t, int32(0), h.dials.Load())
			require.Equal(t, 0, establishedCount(h.m))
		})
	}
}

// bulkAddresses builds n routable full-node addresses, each on its own /16 so
// no two share a network group. A walk over a table this size costs a
// reasonable number of steps; a table holding one entry makes CAddrMan's
// occupied-slot search (addrman.cpp:388-397) walk most of 1024x64 slots per
// Select, which is what makes a one-entry vector slow rather than wrong.
func bulkAddresses(n int, port uint16, services wire.ServiceFlag, nTime int64) []Address {
	out := make([]Address, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, NewAddressAtTime(net.ParseIP(addrListIP(i)), port, services, nTime))
	}

	return out
}

// TestSelectOutboundCandidate is the candidate walk itself
// (net.cpp:1919-1972), one clause per case.
//
// The two soft clauses are covered on BOTH sides, because SVNode defers rather
// than refuses them: a deferred address loses to any address that is not
// deferred, and is taken anyway once nTries passes the clause's threshold.
// Reading either clause as a flat refusal would be wrong.
func TestSelectOutboundCandidate(t *testing.T) {
	now := time.Now().Unix()

	const bulk = 40

	clean := NewAddressAtTime(net.ParseIP("45.251.0.1"), 8333, wire.SFNodeNetwork, now)
	held := NewAddressAtTime(net.ParseIP("45.252.0.1"), 8333, wire.SFNodeNetwork, now)

	noServices := bulkAddresses(bulk, 8333, 0, now)
	oddPort := bulkAddresses(bulk, 9999, wire.SFNodeNetwork, now)
	recent := bulkAddresses(bulk, 8333, wire.SFNodeNetwork, now)

	tests := []struct {
		name    string
		seeded  []Address
		banned  []string
		prepare func(*AddrMan)
		slots   func() outboundSlots
		want    string
		wantOK  bool
	}{
		{
			name:   "an empty table yields nothing",
			wantOK: false,
		},
		{
			name:   "a routable full node is selected",
			seeded: []Address{clean},
			want:   clean.String(),
			wantOK: true,
		},
		{
			// net.cpp:1946-1950, "only connect to full nodes". This clause has
			// no escape hatch, so a table of nothing but these yields nothing
			// at all once the 100-try cap is reached.
			name:   "addresses without NODE_NETWORK are refused outright",
			seeded: noServices,
			wantOK: false,
		},
		{
			// net.cpp:1963-1968, first half: "do not allow non-default ports".
			name:   "a non-default port loses to a default-port address",
			seeded: append([]Address{clean}, oddPort...),
			want:   clean.String(),
			wantOK: true,
		},
		{
			// net.cpp:1963-1968, second half: "unless after 50 invalid
			// addresses selected already".
			name:   "a non-default port is taken once nothing else is left",
			seeded: oddPort,
			wantOK: true,
		},
		{
			// net.cpp:1954-1957, first half: nLastTry inside the ten-minute
			// window. Attempt is what writes nLastTry (net.cpp:410).
			name:    "a recently tried address loses to an untried one",
			seeded:  append([]Address{clean}, recent...),
			prepare: func(a *AddrMan) { attemptAll(a, recent, now) },
			want:    clean.String(),
			wantOK:  true,
		},
		{
			// net.cpp:1954-1957, second half: "only consider very recently
			// tried nodes after 30 failed attempts".
			name:    "a recently tried address is taken once nothing else is left",
			seeded:  recent,
			prepare: func(a *AddrMan) { attemptAll(a, recent, now) },
			wantOK:  true,
		},
		{
			// OpenNetworkConnection's IsBanned fence (net.cpp:2157).
			name:   "a banned address is never chosen",
			seeded: []Address{clean, held},
			banned: []string{"45.252.0.1"},
			want:   clean.String(),
			wantOK: true,
		},
		{
			// IsLocal (net.cpp:1926, :285). Like setConnected below, this is
			// one of the three tests that BREAK the walk rather than
			// continuing it, so the table holds nothing else: a second
			// address would be returned and hide the break.
			name:   "one of our own listen addresses ends the walk",
			seeded: []Address{held},
			slots: func() outboundSlots {
				return outboundSlots{
					connected:    map[string]struct{}{},
					setConnected: map[string]struct{}{},
					local:        map[string]struct{}{held.String(): {}},
				}
			},
			wantOK: false,
		},
		{
			// FindNode's duplicate-connection fence (net.cpp:2163-2166). The
			// held address is deliberately NOT in setConnected here, so the
			// duplicate test is the only thing that can refuse it.
			name:   "an address we already hold is never chosen",
			seeded: []Address{clean, held},
			slots: func() outboundSlots {
				return outboundSlots{
					connected:    map[string]struct{}{held.String(): {}},
					setConnected: map[string]struct{}{},
					local:        map[string]struct{}{},
				}
			},
			want:   clean.String(),
			wantOK: true,
		},
		{
			// setConnected (net.cpp:1924-1927). This one BREAKS the walk
			// rather than continuing it, which is why the table holds nothing
			// else: a second address would be returned and hide the break.
			name:   "an address in a group we already hold ends the walk",
			seeded: []Address{clean},
			slots: func() outboundSlots {
				return outboundSlots{
					connected:    map[string]struct{}{},
					setConnected: map[string]struct{}{string(newNetAddr(clean.IP()).group()): {}},
					local:        map[string]struct{}{},
				}
			},
			wantOK: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			banList, err := NewBanList("")
			require.NoError(t, err)

			for _, host := range tc.banned {
				require.NoError(t, banList.Add(host, time.Now().Add(time.Hour)))
			}

			m := NewPeerManager(ulogger.TestLogger{}, managerSettings(), banList)

			addrMan := NewAddrMan(ulogger.TestLogger{}, AddrManOptions{})
			addrMan.makeDeterministic()

			for _, a := range tc.seeded {
				require.True(t, addrMan.Add(a, net.ParseIP("45.250.0.1"), 0))
			}

			if tc.prepare != nil {
				tc.prepare(addrMan)
			}

			slots := outboundSlots{
				connected:    map[string]struct{}{},
				setConnected: map[string]struct{}{},
				local:        map[string]struct{}{},
			}

			if tc.slots != nil {
				slots = tc.slots()
			}

			got, ok := m.selectOutboundCandidate(addrMan, slots, now)

			require.Equal(t, tc.wantOK, ok)

			if !tc.wantOK {
				return
			}

			if tc.want != "" {
				require.Equal(t, tc.want, got.String())

				return
			}

			// The open cases assert the property the clause is about rather
			// than one address: which of the equivalent entries the stochastic
			// walk lands on is not part of the contract.
			require.Contains(t, addrStrings(tc.seeded), got.String())
		})
	}
}

func attemptAll(a *AddrMan, addrs []Address, nTime int64) {
	for _, addr := range addrs {
		a.Attempt(addr, false, nTime)
	}
}

func addrStrings(addrs []Address) []string {
	out := make([]string, 0, len(addrs))
	for _, a := range addrs {
		out = append(out, a.String())
	}

	return out
}

// TestGetAddrRequest is ProcessVersionMessage's outbound block
// (net_processing.cpp:1867-1872): getaddr out, fGetAddr armed, address marked
// Good — and none of it for an inbound connection.
func TestGetAddrRequest(t *testing.T) {
	peerAddr := NewAddressAtTime(net.ParseIP("45.1.0.1"), 8333, wire.SFNodeNetwork, time.Now().Unix())

	tests := []struct {
		name      string
		inbound   bool
		wantMsgs  int
		wantArmed bool
		wantTried int
	}{
		{name: "outbound asks and marks good", inbound: false, wantMsgs: 1, wantArmed: true, wantTried: 1},
		{name: "inbound is never asked", inbound: true, wantMsgs: 0, wantArmed: false, wantTried: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := newTestManager(t, nil)

			addrMan := NewAddrMan(ulogger.TestLogger{}, AddrManOptions{})
			addrMan.makeDeterministic()
			require.True(t, addrMan.Add(peerAddr, net.ParseIP("45.250.0.1"), 0))
			m.SetAddrMan(addrMan)

			sp := NewSyncPeer(peerAddr.String(), 0, newPeerSyncState())

			msgs := m.GetAddrRequest(sp, tc.inbound, peerAddr.NetAddress())

			require.Len(t, msgs, tc.wantMsgs)

			if tc.wantMsgs > 0 {
				require.IsType(t, &wire.MsgGetAddr{}, msgs[0])
			}

			m.syncMu.Lock()
			armed := sp.fGetAddr
			m.syncMu.Unlock()

			require.Equal(t, tc.wantArmed, armed)
			require.Equal(t, tc.wantTried, triedCount(addrMan))
		})
	}

	t.Run("a nil sync peer is ignored", func(t *testing.T) {
		m := newTestManager(t, nil)
		require.Nil(t, m.GetAddrRequest(nil, false, peerAddr.NetAddress()))
	})
}

// TestOutboundPeerSendsNoGetAddrWithoutAddrMan is the suppression leg: with no
// address table wired, runPeer leaves PeerConfig.Addrs nil (manager.go) and no
// getaddr is sent at all — there would be nowhere to put the reply.
func TestOutboundPeerSendsNoGetAddrWithoutAddrMan(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	defer func() { _ = ln.Close() }()

	tSettings := managerSettings()
	tSettings.Legacy.ConnectPeers = []string{ln.Addr().String()}

	banList, err := NewBanList("")
	require.NoError(t, err)

	m := NewPeerManager(ulogger.TestLogger{}, tSettings, banList)

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

	require.Nil(t, far.readOrNothing(t, wire.CmdGetAddr),
		"no address table means no getaddr")

	m.mu.Lock()
	syncPeers := make([]*SyncPeer, 0, len(m.peers))

	for _, sp := range m.peers {
		syncPeers = append(syncPeers, sp)
	}
	m.mu.Unlock()

	require.Len(t, syncPeers, 1)
	require.Equal(t, 0, getAddrArmed(m), "fGetAddr must stay disarmed")

	require.NoError(t, nc.Close())
}

// TestOutboundDialerUsesLoadedSetting closes the settings path end to end: the
// manager is built from settings.NewSettings(), so legacy_targetOutboundPeers
// reaches the dialer the way it does in production and nothing in this test
// sets the target by hand.
//
// It is the second half of the loader guard in
// settings/target_outbound_peers_test.go. That test proves the key is READ;
// this one proves the read value is USED. Both are needed: PeerManager holds
// the whole *settings.Settings (manager.go) and reads Legacy.ConnectPeers,
// Legacy.PeerIdleTimeout and now Legacy.TargetOutboundPeers off it directly, so
// there is no per-key config struct in between to forget — which is exactly why
// a silent zero would otherwise be invisible.
func TestOutboundDialerUsesLoadedSetting(t *testing.T) {
	tSettings := settings.NewSettings()
	require.Positive(t, tSettings.Legacy.TargetOutboundPeers,
		"the loaded target must be non-zero or the dialer can never run")

	// The candidate walk refuses a non-default port until 50 tries
	// (net.cpp:1963), so the test peer listens on whatever the loaded network's
	// default port is rather than a hardcoded 8333.
	port, err := strconv.ParseUint(tSettings.ChainCfgParams.DefaultPort, 10, 16)
	require.NoError(t, err)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	t.Cleanup(func() { _ = ln.Close() })

	peer := &outboundTestPeer{
		addr:       NewAddressAtTime(net.ParseIP("45.1.0.1"), uint16(port), wire.SFNodeNetwork, time.Now().Unix()),
		ln:         ln,
		bitcoinNet: tSettings.ChainCfgParams.Net,
	}

	go peer.serve()

	banList, err := NewBanList("")
	require.NoError(t, err)

	// Legacy.ConnectPeers must be empty for the addrman loop to run at all
	// (net.cpp:1817-1836); a settings_local.conf that names peers would
	// otherwise turn this test into a no-op without saying so.
	tSettings.Legacy.ConnectPeers = nil

	m := NewPeerManager(ulogger.TestLogger{}, tSettings, banList)
	m.outboundTick = outboundTestTick
	m.dialTCP = func(addr string) (net.Conn, error) {
		if addr != peer.addr.String() {
			return nil, errTestNoRoute
		}

		nc, err := net.DialTimeout("tcp", ln.Addr().String(), 5*time.Second)
		if err != nil {
			return nil, err
		}

		remote, err := net.ResolveTCPAddr("tcp", addr)
		if err != nil {
			return nil, err
		}

		return routedConn{Conn: nc, remote: remote}, nil
	}

	addrMan := NewAddrMan(ulogger.TestLogger{}, AddrManOptions{})
	addrMan.makeDeterministic()
	require.True(t, addrMan.Add(peer.addr, net.ParseIP("45.250.0.1"), 0))
	m.SetAddrMan(addrMan)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	require.NoError(t, m.Start(ctx, nil))
	t.Cleanup(func() { require.NoError(t, m.Stop()) })

	require.Eventually(t, func() bool {
		return establishedCount(m) == 1 && peer.handshakedAndAsked()
	}, 15*time.Second, 20*time.Millisecond)

	handshakes, getAddrs := peer.counts()
	require.Equal(t, 1, handshakes)
	require.Equal(t, 1, getAddrs)
}
