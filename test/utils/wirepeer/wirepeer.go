// Package wirepeer provides a BSV-wire test peer for driving Teranode's legacy
// service from Go tests. It is the analogue of mininode.py / NodeConnCB in the
// bitcoin-sv Python functional suite, and exists so that suite's P2P tests can
// be ported with their assertions intact.
//
// Message encoding and decoding come entirely from
// github.com/bsv-blockchain/go-wire. What this package adds is a minimal client
// connection: the version/verack handshake, a read loop that records every
// message the node sends, and the ability to wait for — or assert the absence
// of — a specific message.
//
// # Why this does not reuse services/legacy/peer
//
// The obvious implementation is to wrap peer.NewOutboundPeer, and that does not
// work. peer.go keeps the nonces it has sent in a package-level MRU map
// (sentNonces) and treats an inbound version carrying a known nonce as a
// self-connection. A TestDaemon runs in the same process as the test, so a
// peer.Peer client shares that map with the node under test: the node sees its
// own registry hit and disconnects with "disconnecting peer connected to self".
// The flag that bypasses the check (TstAllowSelfConnection) lives on the
// receiving side, which the test does not control, and disabling the node's
// self-connection defence to accommodate a test harness would be the wrong
// trade. So the client side is implemented directly on go-wire — the same choice
// mininode.py makes, and the reason it is a standalone implementation rather
// than a reuse of bitcoind's peer class.
//
// Typical use:
//
//	td := daemon.NewTestDaemon(t, daemon.TestOptions{
//	    EnableLegacy:         true,
//	    EnableValidator:      true,
//	    SettingsOverrideFunc: wirepeer.LegacySettings(t),
//	})
//	defer td.Stop(t)
//
//	p := wirepeer.Connect(t, td)
//	defer p.Close()
//
//	p.Send(t, blockMsg)
//	reject := p.WaitForRejectReason(t, 10*time.Second, "bad-cb-amount")
//
// For malformed-message tests that must put bytes on the wire go-wire would
// refuse to encode, use DialRaw instead of Connect.
package wirepeer

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/require"
)

// DefaultTimeout is the wait used by the WaitFor* helpers when a test does not
// specify one. It is deliberately generous: a Teranode block validation round
// trip involves several services.
const DefaultTimeout = 30 * time.Second

// The legacy service bans any peer whose user agent contains neither
// "Bitcoin SV" nor "BSV", to keep BCH/BTC forks out (see
// services/legacy/peer_server.go, OnVersion). A test peer must therefore
// present a BSV-looking agent by default, so these values satisfy that check
// while keeping the harness identifiable in node logs as
// "/Bitcoin SV:1.2.2(teranode-wirepeer)/".
//
// Tests that deliberately probe the ban rule (upstream bsv-ban-useragents.py)
// override this with WithUserAgent.
const (
	defaultUserAgentName    = "Bitcoin SV"
	defaultUserAgentVersion = "1.2.2"
	defaultUserAgentComment = "teranode-wirepeer"
)

// FreePort asks the kernel for an unused TCP port. Ported P2P tests must not
// share a fixed port, or they cannot run in parallel with each other.
func FreePort(t *testing.T) int {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "reserve a free port")

	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())

	return port
}

// LegacySettings returns a SettingsOverrideFunc that makes a TestDaemon
// reachable by this harness: it binds the legacy BSV-wire listener to a free
// port and allows loopback peers to be sync candidates, which is off by default
// (see Legacy.AllowSyncCandidateFromLocalPeers) and would otherwise make the
// node ignore a test peer for block download.
func LegacySettings(t *testing.T, extra ...func(*settings.Settings)) func(*settings.Settings) {
	t.Helper()

	port := FreePort(t)

	return func(s *settings.Settings) {
		s.Legacy.ListenAddresses = []string{fmt.Sprintf("127.0.0.1:%d", port)}
		s.Legacy.AllowSyncCandidateFromLocalPeers = true

		for _, fn := range extra {
			fn(s)
		}
	}
}

// listenAddr recovers the legacy wire listen address from a running daemon's
// settings, so tests never restate the port they configured.
func listenAddr(t *testing.T, td *daemon.TestDaemon) string {
	t.Helper()

	require.NotNil(t, td.Settings, "daemon has no settings")
	require.NotEmpty(t, td.Settings.Legacy.ListenAddresses,
		"daemon has no legacy listen address; start it with wirepeer.LegacySettings and EnableLegacy: true")

	return td.Settings.Legacy.ListenAddresses[0]
}

// Peer is a connected BSV-wire test peer. It records every message received
// from the node under test. All methods are safe for concurrent use.
type Peer struct {
	conn    net.Conn
	pver    uint32
	bsvnet  wire.BitcoinNet
	logf    func(string, ...any)
	nonce   uint64
	writeMu sync.Mutex

	mu        sync.Mutex
	received  []wire.Message
	waiters   []*waiter
	closed    bool
	readEnded bool
	readErr   error
	remoteVer *wire.MsgVersion
}

// waiter is a pending Wait call: the first recorded message satisfying match is
// delivered on ch.
type waiter struct {
	match func(wire.Message) bool
	ch    chan wire.Message
	done  bool
}

// Connect dials the daemon's legacy wire listener, completes the version/verack
// handshake, and returns a Peer recording everything the node sends.
func Connect(t *testing.T, td *daemon.TestDaemon, opts ...Option) *Peer {
	t.Helper()

	cfg := config{
		protocolVersion:   wire.ProtocolVersion,
		services:          wire.SFNodeNetwork,
		userAgentName:     defaultUserAgentName,
		userAgentVersion:  defaultUserAgentVersion,
		userAgentComments: []string{defaultUserAgentComment},
	}

	for _, o := range opts {
		o(&cfg)
	}

	addr := listenAddr(t, td)

	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	require.NoError(t, err, "dial legacy listener at %s", addr)

	p := &Peer{
		conn:   conn,
		pver:   cfg.protocolVersion,
		bsvnet: td.Settings.ChainCfgParams.Net,
		logf:   t.Logf,
		nonce:  randomNonce(t),
	}

	// Start reading before the handshake so the node's version and verack are
	// recorded by the same path as everything else.
	go p.readLoop()

	p.sendVersion(t, &cfg, conn)

	if cfg.skipHandshake {
		return p
	}

	// The node replies version then verack; we ack its version in between.
	p.Wait(t, DefaultTimeout, "version from node", cmdMatcher(wire.CmdVersion))
	p.Send(t, wire.NewMsgVerAck())
	p.Wait(t, DefaultTimeout, "verack from node", cmdMatcher(wire.CmdVerAck))

	return p
}

func randomNonce(t *testing.T) uint64 {
	t.Helper()

	var b [8]byte

	_, err := rand.Read(b[:])
	require.NoError(t, err, "generate version nonce")

	return binary.LittleEndian.Uint64(b[:])
}

// sendVersion writes our version message. The nonce is generated here and never
// registered in services/legacy/peer's package-level sentNonces map, which is
// exactly what keeps the node from mistaking us for itself.
func (p *Peer) sendVersion(t *testing.T, cfg *config, conn net.Conn) {
	t.Helper()

	local, ok := conn.LocalAddr().(*net.TCPAddr)
	require.True(t, ok, "expected a TCP local address")

	remote, ok := conn.RemoteAddr().(*net.TCPAddr)
	require.True(t, ok, "expected a TCP remote address")

	me := wire.NewNetAddress(local, cfg.services)
	you := wire.NewNetAddress(remote, wire.SFNodeNetwork)

	msg := wire.NewMsgVersion(me, you, p.nonce, 0)
	msg.ProtocolVersion = int32(cfg.protocolVersion)
	msg.Services = cfg.services
	msg.UserAgent = ""

	require.NoError(t, msg.AddUserAgent(cfg.userAgentName, cfg.userAgentVersion, cfg.userAgentComments...),
		"set user agent %s:%s", cfg.userAgentName, cfg.userAgentVersion)

	p.Send(t, msg)
}

// readLoop records inbound messages until the connection fails or is closed.
func (p *Peer) readLoop() {
	for {
		_, msg, _, err := wire.ReadMessageWithEncodingN(p.conn, p.pver, p.bsvnet, wire.LatestEncoding)
		if err != nil {
			p.mu.Lock()
			closed := p.closed
			// readEnded is set for every exit, EOF included: a clean EOF is the
			// node hanging up on us, which is precisely what the malformed-message
			// ports need to be able to see. readErr stays reserved for the
			// unexpected kind of failure, so it keeps its diagnostic value.
			p.readEnded = true

			if !closed && !errors.Is(err, io.EOF) {
				p.readErr = err
			}
			p.mu.Unlock()

			return
		}

		p.record(msg)
	}
}

// record appends a message and wakes any waiter it satisfies.
func (p *Peer) record(msg wire.Message) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.received = append(p.received, msg)

	if v, ok := msg.(*wire.MsgVersion); ok {
		p.remoteVer = v
		// Negotiate down to the lower of the two protocol versions, as the
		// protocol requires, so later reads decode the way the node encodes.
		if uint32(v.ProtocolVersion) < p.pver {
			p.pver = uint32(v.ProtocolVersion)
		}
	}

	for _, w := range p.waiters {
		if w.done || !w.match(msg) {
			continue
		}

		w.done = true
		w.ch <- msg
	}
}

// Send writes a message to the node. Encoding failures are the test's bug, so
// they surface through t rather than an error return.
func (p *Peer) Send(t *testing.T, msg wire.Message) {
	t.Helper()

	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	require.NoError(t, p.conn.SetWriteDeadline(time.Now().Add(DefaultTimeout)),
		"set write deadline")

	_, err := wire.WriteMessageWithEncodingN(p.conn, msg, p.pver, p.bsvnet, wire.LatestEncoding)
	require.NoError(t, err, "write %s", msg.Command())
}

// SendRawFrame writes a well-framed message with an arbitrary command name and
// payload body — bytes a conforming encoder would never produce — over the
// peer's already-negotiated connection.
//
// RawConn does the same thing on a bare socket, and is the right tool when the
// malformed input is meant to arrive before any handshake. This is for the
// other case, which is the one upstream's malformed-message tests actually
// exercise: bsv-empty-payload.py and bsv-empty-msg-cmd.py both connect through
// run_node_with_connections, so the node is past negotiation and in its main
// read loop when the bad frame lands. That distinction is not cosmetic in
// Teranode — negotiation and the main loop are different readers
// (peer.readMessage vs peer.readMessageStreaming) reached by different error
// paths, so sending pre-handshake would measure the wrong one.
func (p *Peer) SendRawFrame(t *testing.T, command string, payload []byte) {
	t.Helper()

	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	require.NoError(t, p.conn.SetWriteDeadline(time.Now().Add(DefaultTimeout)),
		"set write deadline")

	_, err := p.conn.Write(frame(t, uint32(p.bsvnet), command, payload, uint32(len(payload))))
	require.NoError(t, err, "write raw %q frame", command)
}

// Wait blocks until a message satisfying match is received, considering messages
// already recorded before the call. It fails the test on timeout.
func (p *Peer) Wait(t *testing.T, timeout time.Duration, what string, match func(wire.Message) bool) wire.Message {
	t.Helper()

	msg, ok := p.tryWait(timeout, match)
	if !ok {
		t.Fatalf("wirepeer: no %s within %s; received: %s%s%s",
			what, timeout, p.Summary(), p.rejectSuffix(), p.readErrSuffix())
	}

	return msg
}

// rejectSuffix spells out any reject messages received. A reject is the single
// most common reason a wait times out, and its reason string is the whole
// diagnosis, so it is worth surfacing rather than leaving as "reject x1".
func (p *Peer) rejectSuffix() string {
	rejects := p.Received(wire.CmdReject)
	if len(rejects) == 0 {
		return ""
	}

	reasons := make([]string, 0, len(rejects))

	for _, msg := range rejects {
		r, ok := msg.(*wire.MsgReject)
		if !ok {
			continue
		}

		reasons = append(reasons, fmt.Sprintf("%s/%s: %q", r.Cmd, r.Code, r.Reason))
	}

	return "; rejects: " + strings.Join(reasons, ", ")
}

// readErrSuffix reports a read-loop failure, which is usually the real reason a
// wait timed out.
func (p *Peer) readErrSuffix() string {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.readErr == nil {
		return ""
	}

	return fmt.Sprintf(" (read loop failed: %v)", p.readErr)
}

// tryWait is the non-fatal core of Wait, shared with the negative assertions.
func (p *Peer) tryWait(timeout time.Duration, match func(wire.Message) bool) (wire.Message, bool) {
	p.mu.Lock()

	// Satisfy from history first, so a fast node that replied before the test
	// started waiting does not cause a spurious timeout.
	for _, msg := range p.received {
		if match(msg) {
			p.mu.Unlock()
			return msg, true
		}
	}

	w := &waiter{match: match, ch: make(chan wire.Message, 1)}
	p.waiters = append(p.waiters, w)
	p.mu.Unlock()

	defer p.removeWaiter(w)

	select {
	case msg := <-w.ch:
		return msg, true
	case <-time.After(timeout):
		return nil, false
	}
}

func (p *Peer) removeWaiter(target *waiter) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for i, w := range p.waiters {
		if w == target {
			p.waiters = append(p.waiters[:i], p.waiters[i+1:]...)
			return
		}
	}
}

// AssertNotReceived fails if a matching message arrives within the window. This
// is the shape several upstream tests need — "rejected and NOT re-requested" —
// and it necessarily costs the full timeout, so keep the window tight.
func (p *Peer) AssertNotReceived(t *testing.T, timeout time.Duration, what string, match func(wire.Message) bool) {
	t.Helper()

	if msg, ok := p.tryWait(timeout, match); ok {
		t.Fatalf("wirepeer: expected no %s, but received %s", what, msg.Command())
	}
}

// Received returns every recorded message with the given wire command.
func (p *Peer) Received(cmd string) []wire.Message {
	p.mu.Lock()
	defer p.mu.Unlock()

	var out []wire.Message

	for _, msg := range p.received {
		if msg.Command() == cmd {
			out = append(out, msg)
		}
	}

	return out
}

// Count returns how many messages with the given command were received.
func (p *Peer) Count(cmd string) int {
	return len(p.Received(cmd))
}

// MessageIndex returns the position of the first message with the given command
// in the order the node sent them, or -1 if none arrived. It is the analogue of
// bitcoin-sv's mininode msg_index, and exists for the same reason: some upstream
// tests assert on the ORDER of two different messages, which neither Received nor
// Count can express.
//
// Positions are indices into everything recorded on this connection, so they are
// only meaningful when compared with each other, never against a constant.
func (p *Peer) MessageIndex(cmd string) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	for i, msg := range p.received {
		if msg.Command() == cmd {
			return i
		}
	}

	return -1
}

// RemoteVersion returns the node's version message, or nil before it arrives.
func (p *Peer) RemoteVersion() *wire.MsgVersion {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.remoteVer
}

// Connected reports whether the connection is still up: the read loop running,
// neither side having closed it. A node that hangs up answers false here, with
// no distinction drawn between a clean EOF and a reset — for a test peer they
// mean the same thing.
func (p *Peer) Connected() bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	return !p.closed && !p.readEnded && p.readErr == nil
}

// AssertDisconnected waits for the node to close the connection, failing if it
// is still up after timeout.
func (p *Peer) AssertDisconnected(t *testing.T, timeout time.Duration, why string) {
	t.Helper()

	require.Eventually(t, func() bool {
		return !p.Connected()
	}, timeout, 50*time.Millisecond,
		"expected the node to disconnect (%s); received: %s%s", why, p.Summary(), p.rejectSuffix())
}

// AssertStillConnected asserts the connection stays up for the whole of
// timeout. Use it where the point of the test is that some input did NOT cost
// the peer its connection; a single Connected() read after the fact would pass
// simply by being early.
func (p *Peer) AssertStillConnected(t *testing.T, timeout time.Duration, why string) {
	t.Helper()

	require.Never(t, func() bool {
		return !p.Connected()
	}, timeout, 50*time.Millisecond,
		"the node should not have disconnected (%s); received: %s%s", why, p.Summary(), p.rejectSuffix())
}

// Summary describes what has been received, for failure messages.
func (p *Peer) Summary() string {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.received) == 0 {
		return "(nothing)"
	}

	counts := make(map[string]int)
	order := make([]string, 0, len(p.received))

	for _, msg := range p.received {
		cmd := msg.Command()
		if counts[cmd] == 0 {
			order = append(order, cmd)
		}

		counts[cmd]++
	}

	parts := make([]string, 0, len(order))
	for _, cmd := range order {
		parts = append(parts, fmt.Sprintf("%s x%d", cmd, counts[cmd]))
	}

	return strings.Join(parts, ", ")
}

// Close disconnects the peer. It is safe to call more than once.
func (p *Peer) Close() {
	p.mu.Lock()

	if p.closed {
		p.mu.Unlock()
		return
	}

	p.closed = true
	p.mu.Unlock()

	_ = p.conn.Close()
}

// --- typed waiters ---------------------------------------------------------

func cmdMatcher(cmd string) func(wire.Message) bool {
	return func(msg wire.Message) bool { return msg.Command() == cmd }
}

// WaitForReject waits for any reject message.
func (p *Peer) WaitForReject(t *testing.T, timeout time.Duration) *wire.MsgReject {
	t.Helper()

	return p.Wait(t, timeout, "reject", cmdMatcher(wire.CmdReject)).(*wire.MsgReject)
}

// WaitForRejectReason waits for a reject whose Reason contains want. Upstream
// tests assert on specific reasons ("bad-cb-amount", "bad-txns-duplicate"), and
// matching the reason is what makes those ports faithful rather than nominal.
func (p *Peer) WaitForRejectReason(t *testing.T, timeout time.Duration, want string) *wire.MsgReject {
	t.Helper()

	match := func(msg wire.Message) bool {
		r, ok := msg.(*wire.MsgReject)
		return ok && strings.Contains(r.Reason, want)
	}

	return p.Wait(t, timeout, fmt.Sprintf("reject with reason containing %q", want), match).(*wire.MsgReject)
}

// WaitForInv waits for an inv message.
func (p *Peer) WaitForInv(t *testing.T, timeout time.Duration) *wire.MsgInv {
	t.Helper()

	return p.Wait(t, timeout, "inv", cmdMatcher(wire.CmdInv)).(*wire.MsgInv)
}

// WaitForGetData waits for a getdata message.
func (p *Peer) WaitForGetData(t *testing.T, timeout time.Duration) *wire.MsgGetData {
	t.Helper()

	return p.Wait(t, timeout, "getdata", cmdMatcher(wire.CmdGetData)).(*wire.MsgGetData)
}

// WaitForGetDataOf waits for a getdata that requests the given inventory hash.
func (p *Peer) WaitForGetDataOf(t *testing.T, timeout time.Duration, hash *chainhash.Hash) *wire.MsgGetData {
	t.Helper()

	return p.Wait(t, timeout, fmt.Sprintf("getdata for %s", hash), invMatcher(wire.CmdGetData, hash)).(*wire.MsgGetData)
}

// WaitForInvOf waits for an inv announcing the given hash.
func (p *Peer) WaitForInvOf(t *testing.T, timeout time.Duration, hash *chainhash.Hash) *wire.MsgInv {
	t.Helper()

	return p.Wait(t, timeout, fmt.Sprintf("inv for %s", hash), invMatcher(wire.CmdInv, hash)).(*wire.MsgInv)
}

// invMatcher matches an inv or getdata message carrying the given hash.
func invMatcher(cmd string, hash *chainhash.Hash) func(wire.Message) bool {
	return func(msg wire.Message) bool {
		if msg.Command() != cmd {
			return false
		}

		var invList []*wire.InvVect

		switch m := msg.(type) {
		case *wire.MsgInv:
			invList = m.InvList
		case *wire.MsgGetData:
			invList = m.InvList
		default:
			return false
		}

		for _, inv := range invList {
			if inv.Hash.IsEqual(hash) {
				return true
			}
		}

		return false
	}
}

// WaitForHeaders waits for a headers message.
func (p *Peer) WaitForHeaders(t *testing.T, timeout time.Duration) *wire.MsgHeaders {
	t.Helper()

	return p.Wait(t, timeout, "headers", cmdMatcher(wire.CmdHeaders)).(*wire.MsgHeaders)
}

// WaitForBlock waits for a block message.
func (p *Peer) WaitForBlock(t *testing.T, timeout time.Duration) *wire.MsgBlock {
	t.Helper()

	return p.Wait(t, timeout, "block", cmdMatcher(wire.CmdBlock)).(*wire.MsgBlock)
}

// WaitForNotFound waits for a notfound message.
func (p *Peer) WaitForNotFound(t *testing.T, timeout time.Duration) *wire.MsgNotFound {
	t.Helper()

	return p.Wait(t, timeout, "notfound", cmdMatcher(wire.CmdNotFound)).(*wire.MsgNotFound)
}
