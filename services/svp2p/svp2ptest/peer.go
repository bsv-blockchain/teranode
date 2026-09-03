package svp2ptest

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"io"
	"math"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// Direction says which way a transcript entry travelled, from the peer's point
// of view.
type Direction int

const (
	// In is a message the peer received from the node.
	In Direction = iota
	// Out is a message the peer sent to the node.
	Out
)

// Entry is one message in a Transcript. Conn is the connection it travelled
// on, so a multi-stream scenario can tell the association's GENERAL stream
// from its DATA1 stream. Seq is the entry's position in transcript order,
// assigned under Transcript.mu — the ordering a scenario should compare on,
// since At is wall-clock time and two entries can tie on a coarse clock.
type Entry struct {
	At   time.Time
	Seq  int
	Dir  Direction
	Cmd  string
	Msg  wire.Message
	Conn net.Conn

	// ReplyTo is the Seq of the inbound entry this outbound one answers, or
	// -1 when it answers nothing (unsolicited sends, handshake messages).
	// Lets a test judge a reply by when its request arrived, not by when the
	// serve goroutine got round to writing it.
	ReplyTo int
}

// Transcript records every message a ScriptedPeer exchanged and who ended the
// connection. It is the harness's evidence.
type Transcript struct {
	mu       sync.Mutex
	entries  []Entry
	closedBy string
}

func (tr *Transcript) add(conn net.Conn, dir Direction, msg wire.Message) int {
	return tr.addReply(conn, dir, msg, -1)
}

func (tr *Transcript) addReply(conn net.Conn, dir Direction, msg wire.Message, replyTo int) int {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	seq := len(tr.entries)
	tr.entries = append(tr.entries, Entry{At: time.Now(), Seq: seq, Dir: dir, Cmd: msg.Command(), Msg: msg, Conn: conn, ReplyTo: replyTo})

	return seq
}

// Count returns how many entries travelled in dir with the given command.
func (tr *Transcript) Count(dir Direction, cmd string) int {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	n := 0

	for _, e := range tr.entries {
		if e.Dir == dir && e.Cmd == cmd {
			n++
		}
	}

	return n
}

// CountOn is Count restricted to one connection.
func (tr *Transcript) CountOn(conn net.Conn, dir Direction, cmd string) int {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	n := 0

	for _, e := range tr.entries {
		if e.Conn == conn && e.Dir == dir && e.Cmd == cmd {
			n++
		}
	}

	return n
}

// FirstOn returns the first entry travelling in dir with the given command,
// and whether there was one.
func (tr *Transcript) FirstOn(dir Direction, cmd string) (Entry, bool) {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	for _, e := range tr.entries {
		if e.Dir == dir && e.Cmd == cmd {
			return e, true
		}
	}

	return Entry{}, false
}

// Snapshot copies the entries recorded so far.
func (tr *Transcript) Snapshot() []Entry {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	out := make([]Entry, len(tr.entries))
	copy(out, tr.entries)

	return out
}

// ClosedBy is "node" when the node ended the connection, "peer" when the
// script or Close did, and "" while a connection is still open or none was made.
func (tr *Transcript) ClosedBy() string {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	return tr.closedBy
}

func (tr *Transcript) setClosedBy(who string) {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	if tr.closedBy == "" {
		tr.closedBy = who
	}
}

// Script is the set of reaction rules a ScriptedPeer follows. A nil hook means
// the honest default: serve the fixture chain.
type Script struct {
	// OnGetHeaders answers a getheaders. The default serves from the locator.
	OnGetHeaders func(p *ScriptedPeer, m *wire.MsgGetHeaders) []wire.Message
	// OnGetData answers a getdata. The default serves every known block.
	// Returning nil sends nothing, which is how a peer withholds blocks.
	OnGetData func(p *ScriptedPeer, m *wire.MsgGetData) []wire.Message
	// OnGetBlocks answers a getblocks. The default sends the inv of blocks
	// after the locator, which is how legacy netsync syncs a chain without
	// checkpoints (regtest).
	OnGetBlocks func(p *ScriptedPeer, m *wire.MsgGetBlocks) []wire.Message
	// OnGetAddr answers a getaddr. The default sends nothing.
	OnGetAddr func(p *ScriptedPeer, m *wire.MsgGetAddr) []wire.Message
	// WriteDelay is waited before each outbound message; nil means none.
	WriteDelay func(msg wire.Message, size int) time.Duration
	// Version may mutate the peer's version message before it is sent.
	Version func(v *wire.MsgVersion)
	// OnConnect is sent right after the peer's verack.
	OnConnect []wire.Message
	// BeforeVersion is sent when the node's version arrives, BEFORE the peer's
	// own version — the "missing-version" offence.
	BeforeVersion []wire.Message
	// StreamPolicies is what the peer's protoconf advertises. Nil means the
	// honest default of a node that allows block priority.
	StreamPolicies []string
	// OnCreateStream answers the node's createstream on a second connection.
	// The default acks it on the same connection, which is what
	// net_processing.cpp:1569-1571 does.
	OnCreateStream func(p *ScriptedPeer, conn net.Conn, m *wire.MsgCreateStream) []wire.Message
	// OnSendCmpct answers the node's sendcmpct. The default sends nothing,
	// which is what SVNode does: ProcessSendCompactMessage
	// (net_processing.cpp:2390-2411) records the peer's preference and
	// answers no message. A peer's own sendcmpct is announced from
	// Script.OnConnect, not from here.
	OnSendCmpct func(p *ScriptedPeer, conn net.Conn, m *wire.MsgSendcmpct) []wire.Message
	// OnGetBlockTxn answers the node's getblocktxn. The default serves the
	// requested indexes from the fixture chain (BlockTxnFor).
	OnGetBlockTxn func(p *ScriptedPeer, conn net.Conn, m *wire.MsgGetBlockTxn) []wire.Message
}

// Raw is a wire message with an arbitrary command and payload, for sending
// what go-wire's typed messages refuse to build (an addr above MaxAddrPerMsg,
// a headers batch above MaxBlockHeadersPerMsg).
type Raw struct {
	Cmd     string
	Payload []byte
}

// Bsvdecode is unused; Raw is only ever sent.
func (r *Raw) Bsvdecode(_ io.Reader, _ uint32, _ wire.MessageEncoding) error {
	return errors.NewProcessingError("svp2ptest.Raw is write-only")
}

// BsvEncode writes the payload verbatim.
func (r *Raw) BsvEncode(w io.Writer, _ uint32, _ wire.MessageEncoding) error {
	_, err := w.Write(r.Payload)

	return err
}

// Command is the wire command name.
func (r *Raw) Command() string { return r.Cmd }

// MaxPayloadLength is the payload's own length.
func (r *Raw) MaxPayloadLength(_ uint32) uint64 { return uint64(len(r.Payload)) }

// ExtendedFrame is one extmsg-framed message this peer received: the command
// the extension header named, the payload length it declared, and how many
// payload bytes actually arrived. The payload itself is discarded as it is
// counted, so a peer can measure a multi-gigabyte block without holding it.
type ExtendedFrame struct {
	Command  string
	Declared uint64
	Received uint64
}

// extendedHeaderBytes is the whole extended header: the 24 byte basic header
// plus the 12 byte real command and the 64 bit length SVNode appends
// (protocol.cpp:220-237).
const extendedHeaderBytes = wire.MessageHeaderSize + wire.CommandSize + 8

// FrameHeader is one wire message header read off a socket, before any payload
// byte. Raw holds the bytes consumed, so a caller that decides not to handle
// the message itself can replay them into go-wire's own reader.
type FrameHeader struct {
	Command  string
	Length   uint64
	Extended bool
	Raw      []byte
}

// ReadFrameHeader reads one message header from r, including the 20 byte
// extension SVNode appends when a payload cannot fit a uint32 length
// (protocol.cpp:220-237: command "extmsg", length 0xffffffff, zero checksum,
// then the real command and a 64 bit length).
//
// go-wire cannot read that extension: readMessageHeader (message.go:246,
// v1.2.11) reads MessageHeaderSize = 24 bytes into a buffer and then parses the
// extension out of that SAME 24 byte buffer, which is already exhausted. The
// error is discarded, so an extmsg frame comes back with an empty command and a
// zero extended length, and the payload is left on the socket. This reads the
// header by hand instead. It is deliberately transport-free: svp2ptest stands
// in for a peer, so it must not share the code under test.
func ReadFrameHeader(r io.Reader) (FrameHeader, error) {
	// Sized for the extension from the start: a basic header keeps the slice
	// at its own length, and an extended one appends into the spare capacity
	// rather than reallocating.
	raw := make([]byte, wire.MessageHeaderSize, extendedHeaderBytes)

	if _, err := io.ReadFull(r, raw); err != nil {
		return FrameHeader{}, err
	}

	hdr := FrameHeader{
		Command: trimCommand(raw[4 : 4+wire.CommandSize]),
		Length:  uint64(binary.LittleEndian.Uint32(raw[16:20])),
		Raw:     raw,
	}

	// The three conditions together are go-wire's own extmsg test
	// (message.go:270) and SVNode's frame: any one of them alone would
	// misread an ordinary message.
	if hdr.Command != wire.CmdExtMsg || hdr.Length != math.MaxUint32 || !bytes.Equal(raw[20:24], []byte{0, 0, 0, 0}) {
		return hdr, nil
	}

	ext := make([]byte, extendedHeaderBytes-wire.MessageHeaderSize)

	if _, err := io.ReadFull(r, ext); err != nil {
		return FrameHeader{}, err
	}

	hdr.Command = trimCommand(ext[:wire.CommandSize])
	hdr.Length = binary.LittleEndian.Uint64(ext[wire.CommandSize:])
	hdr.Extended = true
	hdr.Raw = append(raw, ext...)

	return hdr, nil
}

// trimCommand reads a zero padded command field.
func trimCommand(b []byte) string { return string(bytes.TrimRight(b, "\x00")) }

// ServeLimit is the script of a peer that serves the first n blocks requested
// of it and withholds the rest. n == 0 withholds everything.
func ServeLimit(n int) Script {
	var (
		mu     sync.Mutex
		served int
	)

	return Script{OnGetData: func(p *ScriptedPeer, m *wire.MsgGetData) []wire.Message {
		var out []wire.Message

		for _, inv := range m.InvList {
			if inv == nil || inv.Type != wire.InvTypeBlock {
				continue
			}

			block, known := p.Chain.Block(inv.Hash)
			if !known {
				continue
			}

			mu.Lock()
			allowed := served < n
			if allowed {
				served++
			}
			mu.Unlock()

			if allowed {
				out = append(out, block)
			}
		}

		return out
	}}
}

// ScriptedPeer is a wire-level peer that listens on loopback, completes the
// handshake, and answers the node according to its Script while recording a
// Transcript. It stands in for a real node in integration and parity tests.
type ScriptedPeer struct {
	Addr       string
	Chain      *FixtureChain
	Net        wire.BitcoinNet
	Script     Script
	Transcript *Transcript

	t *testing.T

	mu          sync.Mutex
	ln          net.Listener
	conns       []net.Conn
	closed      bool
	served      int
	conns_      int
	dialled     map[net.Conn]bool
	requested   map[chainhash.Hash]int
	assocOfConn map[net.Conn]string
	data1Conns  map[string]net.Conn
	extFrames   []ExtendedFrame

	// writeMu serialises writes per connection. See writeLockFor.
	writeMu map[net.Conn]*sync.Mutex
}

// Connections is how many times the node connected to this peer. With
// legacy_connect_peers a dropped peer is redialed, so a scenario that bounds
// what a peer was handed must bound it per connection.
func (p *ScriptedPeer) Connections() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.conns_
}

// NewScriptedPeer reserves a loopback address for the peer. With listen true it
// accepts connections at once; otherwise call Listen when the scenario wants
// the peer to appear.
func NewScriptedPeer(t *testing.T, chain *FixtureChain, netMagic wire.BitcoinNet, script Script, listen bool) *ScriptedPeer {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	p := &ScriptedPeer{
		Addr:        ln.Addr().String(),
		Chain:       chain,
		Net:         netMagic,
		Script:      script,
		Transcript:  &Transcript{},
		t:           t,
		dialled:     make(map[net.Conn]bool),
		requested:   make(map[chainhash.Hash]int),
		assocOfConn: make(map[net.Conn]string),
		data1Conns:  make(map[string]net.Conn),
		writeMu:     make(map[net.Conn]*sync.Mutex),
	}

	require.NoError(t, ln.Close())

	if listen {
		p.Listen()
	}

	t.Cleanup(p.Close)

	return p
}

// Listen starts accepting connections on the reserved address.
func (p *ScriptedPeer) Listen() {
	p.t.Helper()

	ln, err := net.Listen("tcp", p.Addr)
	require.NoError(p.t, err)

	p.mu.Lock()
	p.ln = ln
	p.mu.Unlock()

	go p.acceptLoop(ln)
}

func (p *ScriptedPeer) acceptLoop(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}

		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			_ = conn.Close()

			return
		}

		p.conns = append(p.conns, conn)
		p.conns_++
		p.mu.Unlock()

		go p.serve(conn)
	}
}

// Dial connects to the node as an INBOUND peer (from the node's point of view)
// and serves the connection with the same script. The peer keeps its
// reserved Addr for reporting; the node sees an ephemeral source port.
func (p *ScriptedPeer) Dial(nodeAddr string) error {
	conn, err := net.Dial("tcp", nodeAddr)
	if err != nil {
		return err
	}

	p.mu.Lock()
	p.conns = append(p.conns, conn)
	p.conns_++
	p.dialled[conn] = true
	p.mu.Unlock()

	// An inbound peer speaks first.
	local := wire.NewNetAddressIPPort(net.ParseIP("127.0.0.1"), 0, wire.SFNodeNetwork)
	version := wire.NewMsgVersion(local, local, uint64(time.Now().UnixNano()), int32(p.Chain.Len())) //nolint:gosec // fixture height is small
	version.UserAgent = "/Bitcoin SV:1.0.16/"
	version.Services = wire.SFNodeNetwork

	if p.Script.Version != nil {
		p.Script.Version(version)
	}

	if err := p.write(conn, version); err != nil {
		return err
	}

	go p.serve(conn)

	return nil
}

// DialRaw opens a bare TCP connection to the node and sends nothing. The
// connection is tracked so Close ends it, but no serve loop runs on it: the
// caller drives it message by message with Write and ReadOne.
func (p *ScriptedPeer) DialRaw(nodeAddr string) (net.Conn, error) {
	conn, err := net.Dial("tcp", nodeAddr)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	p.conns = append(p.conns, conn)
	p.mu.Unlock()

	return conn, nil
}

// Write sends one message on conn and records it in the Transcript.
func (p *ScriptedPeer) Write(conn net.Conn, msg wire.Message) error {
	return p.write(conn, msg)
}

// ReadOne reads a single message off conn within timeout.
func ReadOne(t *testing.T, conn net.Conn, netMagic wire.BitcoinNet, timeout time.Duration) wire.Message {
	t.Helper()

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(timeout)))

	_, msg, _, err := wire.ReadMessageWithEncodingN(conn, wire.ProtocolVersion, netMagic, wire.BaseEncoding)
	require.NoError(t, err)

	return msg
}

// Conns copies the connections this peer holds, in the order they appeared.
func (p *ScriptedPeer) Conns() []net.Conn {
	p.mu.Lock()
	defer p.mu.Unlock()

	return append([]net.Conn(nil), p.conns...)
}

// protoconf is the peer's protocol configuration announcement. The stream
// policies it carries are the ones the node intersects with its own
// (net.cpp:904-923 CNode::SetSupportedStreamPolicies) before picking the first
// of its prioritised list that is in common
// (net.cpp:945-963 CNode::GetPreferredStreamPolicyName).
func (p *ScriptedPeer) protoconf() *wire.MsgProtoconf {
	policies := p.Script.StreamPolicies
	if policies == nil {
		policies = []string{wire.BlockPriorityStreamPolicy, wire.DefaultStreamPolicy}
	}

	return &wire.MsgProtoconf{
		NumberOfFields:       2,
		MaxRecvPayloadLength: wire.DefaultMaxRecvPayloadLength,
		StreamPolicies:       policies,
	}
}

// Close ends every connection and stops listening.
func (p *ScriptedPeer) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return
	}

	p.closed = true
	p.Transcript.setClosedBy("peer")

	if p.ln != nil {
		_ = p.ln.Close()
	}

	for _, c := range p.conns {
		_ = c.Close()
	}
}

// Send writes msg to every live GENERAL connection, skipping any connection
// this peer has acked a DATA1 createstream on.
//
// A DATA1 stream is not a second peer. It is one more socket of the SAME
// association, and SVNode never originates unsolicited traffic on it: the send
// router (stream_policy.cpp:161-184 BlockPriorityStreamPolicy::PushMessage)
// puts a message on DATA1 only when IsHighPriorityMsg says so
// (stream_policy.cpp:25-31), and everything else takes GENERAL. Writing a
// scripted message to both sockets would deliver it to the node TWICE, and the
// node would answer, or score, twice — which is exactly what happened to
// TestParity_GetHeadersFlood, TestParity_InvGetHeadersAmplification and
// TestParity_MisbehaviourScores once Tasks 9/10 gave the node a second
// connection: every count doubled.
//
// A scenario that deliberately wants a message on the DATA1 socket addresses
// it by hand with Write.
func (p *ScriptedPeer) Send(msg wire.Message) {
	for _, c := range p.generalConns() {
		_ = p.write(c, msg)
	}
}

// generalConns copies the live connections that are NOT a recorded DATA1
// stream. A connection whose createstream this peer has not acked is GENERAL
// by definition, which keeps a single-connection peer behaving exactly as it
// did before multistreams existed.
func (p *ScriptedPeer) generalConns() []net.Conn {
	p.mu.Lock()
	defer p.mu.Unlock()

	data1 := make(map[net.Conn]struct{}, len(p.data1Conns))
	for _, c := range p.data1Conns {
		data1[c] = struct{}{}
	}

	out := make([]net.Conn, 0, len(p.conns))

	for _, c := range p.conns {
		if _, isData1 := data1[c]; isData1 {
			continue
		}

		out = append(out, c)
	}

	return out
}

// Requested reports whether the node ever asked this peer for the block.
func (p *ScriptedPeer) Requested(hash chainhash.Hash) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.requested[hash] > 0
}

// RequestedCount is the number of block getdata items the node sent this peer.
func (p *ScriptedPeer) RequestedCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	n := 0
	for _, c := range p.requested {
		n += c
	}

	return n
}

// ExtendedFrames copies the extmsg-framed messages this peer received, in
// arrival order.
func (p *ScriptedPeer) ExtendedFrames() []ExtendedFrame {
	p.mu.Lock()
	defer p.mu.Unlock()

	return append([]ExtendedFrame(nil), p.extFrames...)
}

// ServedBlocks is the number of block messages this peer sent.
func (p *ScriptedPeer) ServedBlocks() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.served
}

func (p *ScriptedPeer) recordRequest(hash chainhash.Hash) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.requested[hash]++
}

// recordAssociation remembers which association a GENERAL connection belongs
// to, read off the outbound side's own version message
// (net_processing.cpp:210 CreateAssociationID: the dialling side names the
// association it just generated). A version carrying no association ID
// leaves the connection unmapped, which is the multistreams-off case.
func (p *ScriptedPeer) recordAssociation(conn net.Conn, associationID []byte) {
	if len(associationID) == 0 {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.assocOfConn[conn] = hex.EncodeToString(associationID)
}

// recordData1Conn remembers conn as the DATA1 stream of the named association,
// once this peer has acked the node's createstream on it
// (association.cpp:137-160 MoveStream). A later getdata answered on the
// association's GENERAL connection can then route its blocks here instead.
func (p *ScriptedPeer) recordData1Conn(conn net.Conn, associationID []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.data1Conns[hex.EncodeToString(associationID)] = conn
}

// data1ConnFor returns the DATA1 connection recorded for the association
// conn's GENERAL connection belongs to, and whether one has been recorded.
func (p *ScriptedPeer) data1ConnFor(conn net.Conn) (net.Conn, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	id, ok := p.assocOfConn[conn]
	if !ok {
		return nil, false
	}

	dc, ok := p.data1Conns[id]

	return dc, ok
}

// forgetConn drops conn from the association-routing tables when its serve
// loop exits, whichever role it held: a GENERAL connection's own entry in
// assocOfConn, or a DATA1 connection's entry in data1Conns. Without this a
// long-lived or reused ScriptedPeer would grow both maps by one entry per
// connection for the rest of the test.
func (p *ScriptedPeer) forgetConn(conn net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()

	delete(p.assocOfConn, conn)
	delete(p.writeMu, conn)

	for id, dc := range p.data1Conns {
		if dc == conn {
			delete(p.data1Conns, id)
		}
	}
}

func (p *ScriptedPeer) write(conn net.Conn, msg wire.Message) error {
	return p.writeReply(conn, msg, -1)
}

// writeLockFor returns the mutex that serialises writes on conn, creating it on
// first use.
//
// The bytes of one message cannot interleave with another's without it: go-wire
// writes header and payload in a single w.Write (message.go, "Write header and
// payload in 1 go"), and net.TCPConn.Write holds the connection's own write lock
// for the whole call, however many syscalls the payload takes.
//
// The TRANSCRIPT is what needs the lock. Recording happens after the write
// returns, so two goroutines writing on one connection can record in the
// opposite order to the order their bytes reached the socket. That breaks the
// promise Entry.Seq makes in its own doc comment — "the ordering a scenario
// should compare on". Holding this across both the write and the record keeps
// transcript order equal to wire order.
//
// The write delay is applied OUTSIDE the lock by writeReply, so a scenario that
// slows one message down does not stall every other writer on that connection.
func (p *ScriptedPeer) writeLockFor(conn net.Conn) *sync.Mutex {
	p.mu.Lock()
	defer p.mu.Unlock()

	mu, ok := p.writeMu[conn]
	if !ok {
		mu = &sync.Mutex{}
		p.writeMu[conn] = mu
	}

	return mu
}

// writeReply is write for a message that answers the inbound entry replyTo.
func (p *ScriptedPeer) writeReply(conn net.Conn, msg wire.Message, replyTo int) error {
	if p.Script.WriteDelay != nil {
		if d := p.Script.WriteDelay(msg, int(msg.MaxPayloadLength(wire.ProtocolVersion))); d > 0 { //nolint:gosec // bounded by the wire limit
			time.Sleep(d)
		}
	}

	mu := p.writeLockFor(conn)

	mu.Lock()
	defer mu.Unlock()

	if err := wire.WriteMessage(conn, msg, wire.ProtocolVersion, p.Net); err != nil {
		return err
	}

	p.Transcript.addReply(conn, Out, msg, replyTo)

	if msg.Command() == "block" {
		p.mu.Lock()
		p.served++
		p.mu.Unlock()
	}

	return nil
}

func (p *ScriptedPeer) writeAll(conn net.Conn, msgs []wire.Message) bool {
	for _, m := range msgs {
		if p.write(conn, m) != nil {
			return false
		}
	}

	return true
}

// writeAllReply is writeAll for messages that answer the inbound entry
// requestSeq, so the transcript records which request each one answered.
func (p *ScriptedPeer) writeAllReply(conn net.Conn, requestSeq int, msgs []wire.Message) bool {
	for _, m := range msgs {
		if p.writeReply(conn, m, requestSeq) != nil {
			return false
		}
	}

	return true
}

// writeGetDataReply writes a getdata answer, routing each block message onto
// the requesting connection's DATA1 stream when one has been recorded and
// leaving every other message (notfound, and so on) on conn, which is the
// connection getdata itself always arrives on. A connection with no recorded
// DATA1 stream behaves exactly like writeAll, which keeps every scripted-peer
// test written before this routing existed unaffected.
func (p *ScriptedPeer) writeGetDataReply(conn net.Conn, requestSeq int, msgs []wire.Message) bool {
	for _, m := range msgs {
		target := conn

		if m.Command() == wire.CmdBlock {
			if dc, ok := p.data1ConnFor(conn); ok {
				target = dc
			}
		}

		if p.writeReply(target, m, requestSeq) != nil {
			return false
		}
	}

	return true
}

// noteClosed records that the connection ended, attributing it to the node
// unless this peer is the side that shut down.
func (p *ScriptedPeer) noteClosed() {
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()

	if !closed {
		p.Transcript.setClosedBy("node")
	}
}

// drainExtendedFrame counts an extmsg payload straight into io.Discard and
// records what the header declared beside what actually arrived. It reports
// whether the connection is still byte aligned: a short payload leaves the
// socket mid-message and the serve loop must give up on it.
//
// A block is also added to the Transcript, so a scenario can assert on it with
// the same Count/FirstOn vocabulary it uses for every other message. The value
// carried is an empty wire.MsgBlock: the payload was never decoded, and a
// scenario that wants the byte counts reads ExtendedFrames.
func (p *ScriptedPeer) drainExtendedFrame(conn net.Conn, hdr FrameHeader) bool {
	n, err := io.Copy(io.Discard, io.LimitReader(conn, int64(hdr.Length))) //nolint:gosec // an extended length above MaxInt64 cannot be framed

	p.mu.Lock()
	p.extFrames = append(p.extFrames, ExtendedFrame{Command: hdr.Command, Declared: hdr.Length, Received: uint64(n)}) //nolint:gosec // io.Copy never returns a negative count
	p.mu.Unlock()

	if hdr.Command == wire.CmdBlock {
		p.Transcript.add(conn, In, &wire.MsgBlock{})
	}

	return err == nil && uint64(n) == hdr.Length //nolint:gosec // io.Copy never returns a negative count
}

func (p *ScriptedPeer) serve(conn net.Conn) {
	defer func() {
		_ = conn.Close()
		p.forgetConn(conn)
	}()

	local := wire.NewNetAddressIPPort(net.ParseIP("127.0.0.1"), 0, wire.SFNodeNetwork)
	remote := wire.NewNetAddressIPPort(net.ParseIP("127.0.0.1"), 0, wire.SFNodeNetwork)

	for {
		// The header is read by hand first so an extmsg frame is recognised
		// before go-wire is asked for anything: go-wire misparses that header
		// (ReadFrameHeader) and, for a block above the 4 GiB envelope, would
		// try to buffer the whole payload. An extended block is counted and
		// discarded instead; every other frame is replayed into go-wire
		// unchanged, header bytes and all.
		hdr, err := ReadFrameHeader(conn)
		if err != nil {
			p.noteClosed()

			return
		}

		if hdr.Extended {
			if !p.drainExtendedFrame(conn, hdr) {
				p.noteClosed()

				return
			}

			continue
		}

		_, msg, _, err := wire.ReadMessageWithEncodingN(io.MultiReader(bytes.NewReader(hdr.Raw), conn),
			wire.ProtocolVersion, p.Net, wire.BaseEncoding)
		if err != nil {
			p.noteClosed()

			return
		}

		inSeq := p.Transcript.add(conn, In, msg)

		switch m := msg.(type) {
		case *wire.MsgVersion:
			p.mu.Lock()
			weDialled := p.dialled[conn]
			p.mu.Unlock()

			if weDialled {
				// We dialled this connection and already sent our version:
				// answer the node's version with a verack and carry on.
				if !p.writeAll(conn, append([]wire.Message{wire.NewMsgVerAck()}, p.Script.OnConnect...)) {
					return
				}

				continue
			}

			// The node dialled us, so it is the outbound side of this
			// connection and, when multistreams are on, named the
			// association it just generated in its own version message.
			p.recordAssociation(conn, m.AssociationID)

			version := wire.NewMsgVersion(local, remote, uint64(time.Now().UnixNano()), int32(p.Chain.Len())) //nolint:gosec // fixture height is small
			version.UserAgent = "/Bitcoin SV:1.0.16/"
			version.Services = wire.SFNodeNetwork

			if p.Script.Version != nil {
				p.Script.Version(version)
			}

			if !p.writeAll(conn, p.Script.BeforeVersion) {
				return
			}

			if !p.writeAll(conn, []wire.Message{version, wire.NewMsgVerAck(), p.protoconf()}) {
				return
			}

			if !p.writeAll(conn, p.Script.OnConnect) {
				return
			}

		case *wire.MsgPing:
			if p.write(conn, wire.NewMsgPong(m.Nonce)) != nil {
				return
			}

		case *wire.MsgGetHeaders:
			var out []wire.Message
			if p.Script.OnGetHeaders != nil {
				out = p.Script.OnGetHeaders(p, m)
			} else {
				out = []wire.Message{p.HeadersFor(m)}
			}

			if !p.writeAll(conn, out) {
				return
			}

		case *wire.MsgGetData:
			for _, inv := range m.InvList {
				if inv != nil && inv.Type == wire.InvTypeBlock {
					p.recordRequest(inv.Hash)
				}
			}

			var out []wire.Message
			if p.Script.OnGetData != nil {
				out = p.Script.OnGetData(p, m)
			} else {
				out = p.blocksFor(m)
			}

			if !p.writeGetDataReply(conn, inSeq, out) {
				return
			}

		case *wire.MsgGetBlocks:
			var out []wire.Message
			if p.Script.OnGetBlocks != nil {
				out = p.Script.OnGetBlocks(p, m)
			} else {
				out = []wire.Message{p.InvFor(m)}
			}

			if !p.writeAll(conn, out) {
				return
			}

		case *wire.MsgCreateStream:
			var out []wire.Message
			if p.Script.OnCreateStream != nil {
				out = p.Script.OnCreateStream(p, conn, m)
			} else {
				out = []wire.Message{wire.NewMsgStreamAck(m.AssociationID, m.StreamType)}
			}

			// Recorded BEFORE the ack goes out: a client that has read the ack
			// may rely on this connection being the association's DATA1 stream,
			// and getdata answered on GENERAL routes its blocks here
			// (stream_policy.cpp:161-184).
			if m.StreamType == wire.StreamTypeData1 && len(out) > 0 {
				p.recordData1Conn(conn, m.AssociationID)
			}

			if !p.writeAll(conn, out) {
				return
			}

		case *wire.MsgSendcmpct:
			if p.Script.OnSendCmpct != nil && !p.writeAllReply(conn, inSeq, p.Script.OnSendCmpct(p, conn, m)) {
				return
			}

		case *wire.MsgGetBlockTxn:
			var out []wire.Message

			if p.Script.OnGetBlockTxn != nil {
				out = p.Script.OnGetBlockTxn(p, conn, m)
			} else if reply := p.BlockTxnFor(m); reply != nil {
				out = []wire.Message{reply}
			}

			// blocktxn stays on the connection its getblocktxn arrived on. Only
			// a block message is routed onto an association's DATA1 stream
			// (stream_policy.cpp:25-31, IsHighPriorityMsg), which is why
			// writeGetDataReply routes CmdBlock and nothing else.
			if !p.writeAllReply(conn, inSeq, out) {
				return
			}

		case *wire.MsgGetAddr:
			if p.Script.OnGetAddr != nil && !p.writeAll(conn, p.Script.OnGetAddr(p, m)) {
				return
			}
		}
	}
}

func (p *ScriptedPeer) blocksFor(m *wire.MsgGetData) []wire.Message {
	var out []wire.Message

	for _, inv := range m.InvList {
		if inv == nil || inv.Type != wire.InvTypeBlock {
			continue
		}

		if block, known := p.Chain.Block(inv.Hash); known {
			out = append(out, block)
		}
	}

	return out
}

// HeadersFor is the honest getheaders answer: up to MaxBlockHeadersPerMsg
// headers after the first locator hash the chain knows, stopping at HashStop.
func (p *ScriptedPeer) HeadersFor(msg *wire.MsgGetHeaders) *wire.MsgHeaders {
	start := int32(0)

	for _, hash := range msg.BlockLocatorHashes {
		if hash == nil {
			continue
		}

		if height, known := p.Chain.Height(*hash); known {
			start = height
			break
		}
	}

	headers := wire.NewMsgHeaders()

	for i := start; i < int32(p.Chain.Len()); i++ { //nolint:gosec // fixture height is small
		if len(headers.Headers) == wire.MaxBlockHeadersPerMsg {
			break
		}

		header := p.Chain.HeaderAt(int(i))
		if header == nil {
			break
		}

		_ = headers.AddBlockHeader(header)

		if msg.HashStop != (chainhash.Hash{}) && header.BlockHash() == msg.HashStop {
			break
		}
	}

	return headers
}

// InvFor is the honest getblocks answer: the inventory of up to MaxBlocksPerMsg
// blocks after the first locator hash the chain knows, stopping at HashStop.
func (p *ScriptedPeer) InvFor(msg *wire.MsgGetBlocks) *wire.MsgInv {
	start := int32(0)

	for _, hash := range msg.BlockLocatorHashes {
		if hash == nil {
			continue
		}

		if height, known := p.Chain.Height(*hash); known {
			start = height
			break
		}
	}

	inv := wire.NewMsgInv()

	for i := start; i < int32(p.Chain.Len()); i++ { //nolint:gosec // fixture height is small
		if len(inv.InvList) == wire.MaxBlocksPerMsg {
			break
		}

		header := p.Chain.HeaderAt(int(i))
		if header == nil {
			break
		}

		hash := header.BlockHash()

		_ = inv.AddInvVect(wire.NewInvVect(wire.InvTypeBlock, &hash))

		if msg.HashStop != (chainhash.Hash{}) && hash == msg.HashStop {
			break
		}
	}

	return inv
}
