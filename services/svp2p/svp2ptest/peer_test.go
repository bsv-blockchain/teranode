package svp2ptest

import (
	"net"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// rawClient is the node side of a ScriptedPeer test: a bare TCP client that
// completes the handshake and then exchanges wire messages.
type rawClient struct {
	t    *testing.T
	conn net.Conn
	net  wire.BitcoinNet
}

func dialScripted(t *testing.T, p *ScriptedPeer) *rawClient {
	t.Helper()

	conn, err := net.Dial("tcp", p.Addr)
	require.NoError(t, err)

	t.Cleanup(func() { _ = conn.Close() })

	c := &rawClient{t: t, conn: conn, net: p.Net}

	me := wire.NewNetAddressIPPort(net.ParseIP("127.0.0.1"), 0, 0)
	c.write(wire.NewMsgVersion(me, me, 1, 0))

	for {
		msg := c.read(5 * time.Second)
		if _, ok := msg.(*wire.MsgVerAck); ok {
			break
		}
	}

	return c
}

func (c *rawClient) write(msg wire.Message) {
	c.t.Helper()
	require.NoError(c.t, wire.WriteMessage(c.conn, msg, wire.ProtocolVersion, c.net))
}

func (c *rawClient) read(timeout time.Duration) wire.Message {
	c.t.Helper()
	require.NoError(c.t, c.conn.SetReadDeadline(time.Now().Add(timeout)))

	msg, _, err := wire.ReadMessage(c.conn, wire.ProtocolVersion, c.net)
	require.NoError(c.t, err)

	return msg
}

// readUntil returns the first message of the wanted command, answering pings
// and skipping anything else.
func (c *rawClient) readUntil(cmd string, timeout time.Duration) wire.Message {
	c.t.Helper()

	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		msg := c.read(time.Until(deadline))
		if msg.Command() == cmd {
			return msg
		}
	}

	c.t.Fatalf("no %s within %s", cmd, timeout)

	return nil
}

func newTestPeer(t *testing.T, height int, script Script) (*ScriptedPeer, *FixtureChain) {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	chain := BuildFixtureChain(t, tSettings, height)
	peer := NewScriptedPeer(t, chain, tSettings.ChainCfgParams.Net, script, true)

	return peer, chain
}

func getHeadersFrom(locator chainhash.Hash) *wire.MsgGetHeaders {
	m := wire.NewMsgGetHeaders()
	m.ProtocolVersion = wire.ProtocolVersion
	_ = m.AddBlockLocatorHash(&locator)

	return m
}

func getDataFor(hash chainhash.Hash) *wire.MsgGetData {
	m := wire.NewMsgGetData()
	_ = m.AddInvVect(wire.NewInvVect(wire.InvTypeBlock, &hash))

	return m
}

func TestScriptedPeer_HonestGetHeadersServesFromLocator(t *testing.T) {
	peer, chain := newTestPeer(t, 30, Script{})
	c := dialScripted(t, peer)

	genesis := chain.Headers[0].PrevBlock
	c.write(getHeadersFrom(genesis))

	headers := c.readUntil("headers", 5*time.Second).(*wire.MsgHeaders)
	require.Len(t, headers.Headers, 30)
	require.Equal(t, chain.Tip(), headers.Headers[29].BlockHash())

	require.Equal(t, 1, peer.Transcript.Count(In, "getheaders"))
	require.Equal(t, 1, peer.Transcript.Count(Out, "headers"))
}

func TestScriptedPeer_ScriptOverridesGetHeaders(t *testing.T) {
	var first *wire.MsgHeaders

	replay := Script{OnGetHeaders: func(p *ScriptedPeer, m *wire.MsgGetHeaders) []wire.Message {
		if first == nil {
			first = p.HeadersFor(m)
		}

		return []wire.Message{first}
	}}

	peer, chain := newTestPeer(t, 10, replay)
	c := dialScripted(t, peer)

	genesis := chain.Headers[0].PrevBlock
	c.write(getHeadersFrom(genesis))
	a := c.readUntil("headers", 5*time.Second).(*wire.MsgHeaders)

	c.write(getHeadersFrom(chain.Tip()))
	b := c.readUntil("headers", 5*time.Second).(*wire.MsgHeaders)

	require.Len(t, a.Headers, 10)
	require.Len(t, b.Headers, 10, "the script replays the first batch regardless of the locator")
	require.Equal(t, a.Headers[0].BlockHash(), b.Headers[0].BlockHash())
}

func TestScriptedPeer_WithholdBlocks(t *testing.T) {
	withhold := Script{OnGetData: func(*ScriptedPeer, *wire.MsgGetData) []wire.Message { return nil }}

	peer, chain := newTestPeer(t, 3, withhold)
	c := dialScripted(t, peer)

	hash := chain.Headers[0].BlockHash()
	c.write(getDataFor(hash))

	// Nothing comes back but pings/pongs; give it a moment and check the record.
	time.Sleep(300 * time.Millisecond)

	require.True(t, peer.Requested(hash))
	require.Equal(t, 0, peer.ServedBlocks())
	require.Equal(t, 0, peer.Transcript.Count(Out, "block"))
}

func TestScriptedPeer_WriteDelayThrottles(t *testing.T) {
	slow := Script{WriteDelay: func(msg wire.Message, _ int) time.Duration {
		if msg.Command() == "block" {
			return 50 * time.Millisecond
		}

		return 0
	}}

	peer, chain := newTestPeer(t, 5, slow)
	c := dialScripted(t, peer)

	m := wire.NewMsgGetData()
	for _, h := range chain.Headers {
		hash := h.BlockHash()
		require.NoError(t, m.AddInvVect(wire.NewInvVect(wire.InvTypeBlock, &hash)))
	}

	start := time.Now()
	c.write(m)

	for i := 0; i < 5; i++ {
		c.readUntil("block", 5*time.Second)
	}

	require.GreaterOrEqual(t, time.Since(start), 200*time.Millisecond)
	require.Equal(t, 5, peer.ServedBlocks())
}

func TestScriptedPeer_TranscriptRecordsWhoClosed(t *testing.T) {
	peer, _ := newTestPeer(t, 1, Script{})
	c := dialScripted(t, peer)

	require.NoError(t, c.conn.Close())

	require.Eventually(t, func() bool { return peer.Transcript.ClosedBy() == "node" }, 5*time.Second, 20*time.Millisecond)

	other, _ := newTestPeer(t, 1, Script{})
	_ = dialScripted(t, other)
	other.Close()

	require.Eventually(t, func() bool { return other.Transcript.ClosedBy() == "peer" }, 5*time.Second, 20*time.Millisecond)
}

func TestScriptedPeer_ServeLimitScript(t *testing.T) {
	peer, chain := newTestPeer(t, 4, ServeLimit(2))
	c := dialScripted(t, peer)

	m := wire.NewMsgGetData()
	for _, h := range chain.Headers {
		hash := h.BlockHash()
		require.NoError(t, m.AddInvVect(wire.NewInvVect(wire.InvTypeBlock, &hash)))
	}

	c.write(m)
	c.readUntil("block", 5*time.Second)
	c.readUntil("block", 5*time.Second)

	time.Sleep(200 * time.Millisecond)
	require.Equal(t, 2, peer.ServedBlocks())
	require.Equal(t, 4, peer.RequestedCount())
}

// Legacy netsync on a chain without checkpoints (regtest) syncs by getblocks →
// inv → getdata, so the peer must answer getblocks with the block inventory
// after the locator, capped at MaxBlocksPerMsg.
func TestScriptedPeer_HonestGetBlocksAnswersInv(t *testing.T) {
	peer, chain := newTestPeer(t, 12, Script{})
	c := dialScripted(t, peer)

	m := wire.NewMsgGetBlocks(&chainhash.Hash{})
	m.ProtocolVersion = wire.ProtocolVersion
	locator := chain.Headers[2].BlockHash() // height 3
	require.NoError(t, m.AddBlockLocatorHash(&locator))
	c.write(m)

	inv := c.readUntil("inv", 3*time.Second).(*wire.MsgInv)
	require.Len(t, inv.InvList, 9, "heights 4..12")
	require.Equal(t, wire.InvTypeBlock, inv.InvList[0].Type)
	require.Equal(t, chain.Headers[3].BlockHash(), inv.InvList[0].Hash)
	require.Equal(t, chain.Tip(), inv.InvList[8].Hash)
}

func TestRaw_EncodesPayloadVerbatimUnderItsCommand(t *testing.T) {
	peer, _ := newTestPeer(t, 1, Script{OnConnect: []wire.Message{&Raw{Cmd: "addr", Payload: []byte{0x00}}}})
	c := dialScripted(t, peer)

	msg := c.readUntil("addr", 3*time.Second)
	require.Equal(t, "addr", msg.Command())
	require.Equal(t, 1, peer.Transcript.Count(Out, "addr"))
}

func TestScriptedPeer_BeforeVersionIsSentAheadOfTheVersion(t *testing.T) {
	peer, _ := newTestPeer(t, 1, Script{BeforeVersion: []wire.Message{wire.NewMsgPing(7)}})

	conn, err := net.Dial("tcp", peer.Addr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	c := &rawClient{t: t, conn: conn, net: peer.Net}
	me := wire.NewNetAddressIPPort(net.ParseIP("127.0.0.1"), 0, 0)
	c.write(wire.NewMsgVersion(me, me, 1, 0))

	first := c.read(3 * time.Second)
	require.Equal(t, "ping", first.Command(), "the scripted message precedes the peer's version")
	require.Equal(t, "version", c.read(3*time.Second).Command())
}

// TestScriptedPeer_DialActsAsAnInboundPeer: the peer connects to a listener,
// sends its version first, and answers the listener's version with a verack.
func TestScriptedPeer_DialActsAsAnInboundPeer(t *testing.T) {
	peer, _ := newTestPeer(t, 3, Script{})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	require.NoError(t, peer.Dial(ln.Addr().String()))

	conn, err := ln.Accept()
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	c := &rawClient{t: t, conn: conn, net: peer.Net}
	require.Equal(t, "version", c.read(3*time.Second).Command(), "an inbound peer speaks first")

	me := wire.NewNetAddressIPPort(net.ParseIP("127.0.0.1"), 0, 0)
	c.write(wire.NewMsgVersion(me, me, 1, 0))
	require.Equal(t, "verack", c.read(3*time.Second).Command())
	require.Equal(t, 1, peer.Connections())
}

// TestScriptedPeer_SendSkipsTheData1Stream pins the rule Send follows once an
// association holds two sockets: unsolicited scripted traffic goes out on
// GENERAL only.
//
// SVNode's send router puts a message on DATA1 only when IsHighPriorityMsg
// says so (stream_policy.cpp:25-31, used by
// BlockPriorityStreamPolicy::PushMessage at :161-184); everything else takes
// GENERAL. Before this rule Send wrote to EVERY live connection, so a scenario
// that sent one inv delivered it to the node twice and every count the
// scenario made came back doubled.
//
// Both sockets are driven the way the node drives them: a version on the
// first, a DATA1 createstream on the second, which is what makes the peer
// record it (association.cpp:137-160 MoveStream).
func TestScriptedPeer_SendSkipsTheData1Stream(t *testing.T) {
	peer, chain := newTestPeer(t, 3, Script{})

	general := dialScripted(t, peer)

	associationID := []byte{0x01, 0x02, 0x03, 0x04}

	data1, err := net.Dial("tcp", peer.Addr)
	require.NoError(t, err)

	t.Cleanup(func() { _ = data1.Close() })

	data1Client := &rawClient{t: t, conn: data1, net: peer.Net}
	data1Client.write(wire.NewMsgCreateStream(associationID, wire.StreamTypeData1, wire.BlockPriorityStreamPolicy))

	ack, ok := data1Client.readUntil(wire.CmdStreamAck, 5*time.Second).(*wire.MsgStreamAck)
	require.True(t, ok, "the peer must ack a DATA1 createstream")
	require.Equal(t, wire.StreamTypeData1, ack.StreamType)

	// The ack is what records the socket as DATA1, so only now is Send's rule
	// under test at all.
	inv := wire.NewMsgInv()
	tip := chain.Tip()
	require.NoError(t, inv.AddInvVect(wire.NewInvVect(wire.InvTypeBlock, &tip)))

	peer.Send(inv)

	got := general.readUntil(wire.CmdInv, 5*time.Second)
	require.Equal(t, wire.CmdInv, got.Command(), "the GENERAL socket must receive the sent message")

	// Nothing may arrive on DATA1. A read deadline is the only way to assert an
	// absence on a socket, so the window is explicit.
	require.NoError(t, data1.SetReadDeadline(time.Now().Add(200*time.Millisecond)))

	_, _, readErr := wire.ReadMessage(data1, wire.ProtocolVersion, peer.Net)
	require.Error(t, readErr, "nothing may be sent unsolicited on the DATA1 stream")

	var netErr net.Error
	require.ErrorAs(t, readErr, &netErr)
	require.True(t, netErr.Timeout(), "the DATA1 read must end in a timeout, not in a message or a closed socket: %v", readErr)

	// The transcript agrees: exactly one inv went out, on the GENERAL socket.
	require.Equal(t, 1, peer.Transcript.Count(Out, wire.CmdInv), "Send must write the message once, not once per socket")
}
