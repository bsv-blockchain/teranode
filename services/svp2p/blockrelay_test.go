package svp2p

import (
	"bytes"
	"context"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/model"
	inmemorykafka "github.com/bsv-blockchain/teranode/util/kafka/in_memory_kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// relayScriptedPeer is a minimal wire client for this file's Server-level
// wiring tests: dial, complete the handshake, optionally negotiate
// sendheaders, and read what the relay sends. It is deliberately separate
// from protocol package's own scriptedPeer (unexported there, and this file
// tests the wiring ABOVE that package: Kafka -> Server -> PeerManager, not
// the relay's selection rules, which protocol/relay_test.go already covers).
type relayScriptedPeer struct {
	nc  net.Conn
	net wire.BitcoinNet
}

func dialRelayPeer(t *testing.T, addr string, netMagic wire.BitcoinNet) *relayScriptedPeer {
	t.Helper()

	nc, err := net.Dial("tcp", addr)
	require.NoError(t, err)

	t.Cleanup(func() { _ = nc.Close() })

	return &relayScriptedPeer{nc: nc, net: netMagic}
}

func (s *relayScriptedPeer) write(t *testing.T, msg wire.Message) {
	t.Helper()

	_, err := wire.WriteMessageWithEncodingN(s.nc, msg, wire.ProtocolVersion, s.net, wire.BaseEncoding)
	require.NoError(t, err)
}

func (s *relayScriptedPeer) read(t *testing.T) wire.Message {
	t.Helper()

	require.NoError(t, s.nc.SetReadDeadline(time.Now().Add(5*time.Second)))

	_, msg, _, err := wire.ReadMessageWithEncodingN(s.nc, wire.ProtocolVersion, s.net, wire.BaseEncoding)
	require.NoError(t, err)

	return msg
}

func (s *relayScriptedPeer) readUntil(t *testing.T, want string) wire.Message {
	t.Helper()

	for i := 0; i < 64; i++ {
		msg := s.read(t)
		if msg.Command() == want {
			return msg
		}
	}

	t.Fatalf("no %s message received", want)

	return nil
}

// completeHandshake runs the outbound side of the version/verack exchange
// against a live Server, optionally negotiating sendheaders, and barriers
// the negotiation with a ping/pong round trip: each peer's inbound messages
// are processed in order on its own goroutine, so the pong only comes back
// after sendheaders has already been applied.
func (s *relayScriptedPeer) completeHandshake(t *testing.T, sendHeaders bool) {
	t.Helper()

	local := wire.NewNetAddressIPPort(net.ParseIP("127.0.0.1"), 0, wire.SFNodeNetwork)
	remote := wire.NewNetAddressIPPort(net.ParseIP("127.0.0.1"), 0, wire.SFNodeNetwork)

	version := wire.NewMsgVersion(local, remote, uint64(time.Now().UnixNano()), 0) //nolint:gosec // test-only nonce
	version.UserAgent = "/svp2p-relay-test:1.0/"
	version.Services = wire.SFNodeNetwork
	s.write(t, version)

	sawVerack := false
	for !sawVerack {
		switch s.read(t).(type) {
		case *wire.MsgVerAck:
			sawVerack = true
		case *wire.MsgVersion, *wire.MsgProtoconf:
		}
	}

	s.write(t, wire.NewMsgVerAck())

	if sendHeaders {
		s.write(t, wire.NewMsgSendHeaders())
	}

	nonce := uint64(time.Now().UnixNano()) //nolint:gosec // test-only nonce
	s.write(t, wire.NewMsgPing(nonce))

	pong, ok := s.readUntil(t, wire.CmdPong).(*wire.MsgPong)
	require.True(t, ok)
	require.Equal(t, nonce, pong.Nonce)
}

// regtestGenesisAsModelHeader rebuilds the server's own genesis header as a
// model.BlockHeader (what the blocks-final producer actually serializes,
// services/blockchain/Server.go sendKafkaBlockFinalNotification) by
// round-tripping the wire header's own 80 raw bytes, so this file's wiring
// tests have a real, self-consistent header to relay without depending on
// model.GenesisBlockHeader (a different chain's genesis).
func regtestGenesisAsModelHeader(t *testing.T, srv *Server) *model.BlockHeader {
	t.Helper()

	genesis := srv.settings.ChainCfgParams.GenesisBlock.Header

	var raw bytes.Buffer
	require.NoError(t, genesis.Serialize(&raw))

	header, err := model.NewBlockHeaderFromBytes(raw.Bytes())
	require.NoError(t, err)

	return header
}

func produceBlocksFinal(t *testing.T, ctx context.Context, topic string, header *model.BlockHeader) {
	t.Helper()

	value, err := proto.Marshal(&kafkamessage.KafkaBlocksFinalTopicMessage{Header: header.Bytes()})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return inmemorykafka.GetSharedBroker().HasConsumer(topic)
	}, 5*time.Second, 10*time.Millisecond, "the blocks-final consumer never subscribed")

	require.NoError(t, inmemorykafka.GetSharedBroker().Produce(ctx, topic, []byte(header.Hash().String()), value))
}

// TestServerRelaysBlocksFinalToPeers is the plan's own "Kafka leg" scenario
// (task-12-brief.md Step 1): a message produced onto the blocks-final topic
// reaches two real, connected peers in the right form — headers for the one
// that negotiated sendheaders, inv for the one that did not — proving the
// whole chain from bridge/kafka.go's decode through Server.Start's wiring to
// PeerManager.RelayBlock's real socket sends.
func TestServerRelaysBlocksFinalToPeers(t *testing.T) {
	srv, _ := newTestServer(t)

	const topic = "blocks-final-server-wiring-test"

	kafkaURL, err := url.Parse("memory://localhost:9092/" + topic)
	require.NoError(t, err)
	srv.settings.Kafka.BlocksFinalConfig = kafkaURL

	t.Cleanup(func() { inmemorykafka.GetSharedBroker().DropTopic(topic) })

	cancel := startServer(t, srv)
	defer cancel()

	defer func() { require.NoError(t, srv.Stop(context.Background())) }()

	netMagic := srv.settings.ChainCfgParams.Net

	headersPeer := dialRelayPeer(t, srv.manager.ListenAddrs()[0], netMagic)
	headersPeer.completeHandshake(t, true)

	invPeer := dialRelayPeer(t, srv.manager.ListenAddrs()[0], netMagic)
	invPeer.completeHandshake(t, false)

	// The chain's own genesis header, reconstructed as a model.BlockHeader
	// (what the blocks-final producer actually serializes). This test
	// exercises the WIRING — Kafka decode through to a real socket send —
	// not chain validity: RelayBlock never checks proof of work or lineage,
	// so relaying genesis again is a fine stand-in for "a block".
	block := regtestGenesisAsModelHeader(t, srv)

	produceBlocksFinal(t, context.Background(), topic, block)

	headersMsg, ok := headersPeer.readUntil(t, wire.CmdHeaders).(*wire.MsgHeaders)
	require.True(t, ok)
	require.Len(t, headersMsg.Headers, 1)
	require.Equal(t, block.Hash().String(), headersMsg.Headers[0].BlockHash().String())

	invMsg, ok := invPeer.readUntil(t, wire.CmdInv).(*wire.MsgInv)
	require.True(t, ok)
	require.Len(t, invMsg.InvList, 1)
	require.Equal(t, wire.InvTypeBlock, invMsg.InvList[0].Type)
	require.Equal(t, block.Hash().String(), invMsg.InvList[0].Hash.String())
}

// TestServerStopReturnsPromptlyWithMessageInFlight is what this test can
// actually establish about the D7 consumer-lifecycle contract: Stop returns
// (the daemon assumes it does not hang) and clears the consumer handle, even
// with a blocks-final message already in flight when shutdown begins. It
// does NOT prove the underlying consumer goroutine actually drained — fix
// round 1, review finding I3 — see blocksFinalConsumer's field comment in
// Server.go for why that is a real limitation of the in-memory Kafka fake
// this test necessarily runs against, not of the production Close path.
func TestServerStopReturnsPromptlyWithMessageInFlight(t *testing.T) {
	srv, _ := newTestServer(t)

	const topic = "blocks-final-server-shutdown-test"

	kafkaURL, err := url.Parse("memory://localhost:9092/" + topic)
	require.NoError(t, err)
	srv.settings.Kafka.BlocksFinalConfig = kafkaURL

	t.Cleanup(func() { inmemorykafka.GetSharedBroker().DropTopic(topic) })

	cancel := startServer(t, srv)
	defer cancel()

	block := regtestGenesisAsModelHeader(t, srv)

	produceBlocksFinal(t, context.Background(), topic, block)

	done := make(chan error, 1)
	go func() { done <- srv.Stop(context.Background()) }()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Stop never returned with a blocks-final message in flight")
	}

	require.Nil(t, srv.blocksFinalConsumer, "Stop must clear the consumer handle it closed")
}
