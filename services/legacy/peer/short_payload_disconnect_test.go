package peer_test

import (
	"encoding/binary"
	"io"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// A well-framed reject with no payload is a malformed message. The peer that sent it is told so
// with a "malformed" reject, instead of the node dropping the connection and saying nothing as it
// did when the decoder's end-of-data error was taken for the peer hanging up.
func TestAShortRejectIsAnsweredWithAMalformedReject(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	verack := make(chan struct{}, 4)
	rejected := make(chan *wire.MsgReject, 1)

	cfg := func(listeners peer.MessageListeners) *peer.Config {
		listeners.OnVerAck = func(*peer.Peer, *wire.MsgVerAck) { verack <- struct{}{} }
		listeners.OnWrite = func(_ *peer.Peer, _ int, msg wire.Message, _ error) {
			if _, ok := msg.(*wire.MsgVerAck); ok {
				verack <- struct{}{}
			}
		}

		return &peer.Config{
			Listeners:              listeners,
			UserAgentName:          "peer",
			UserAgentVersion:       "1.0",
			ChainParams:            &chaincfg.MainNetParams,
			TrickleInterval:        time.Second * 10,
			TstAllowSelfConnection: true,
		}
	}

	inR, outW := io.Pipe()
	outR, inW := io.Pipe()

	inConn := &conn{raddr: "10.0.0.1:8333", Reader: inR, Writer: inW, Closer: inW}
	outConn := &conn{raddr: "10.0.0.2:8333", Reader: outR, Writer: outW, Closer: outW}

	// the sender: it sees whatever reject the node answers with
	sender := peer.NewInboundPeer(ulogger.TestLogger{}, tSettings, cfg(peer.MessageListeners{
		OnReject: func(_ *peer.Peer, msg *wire.MsgReject) { rejected <- msg },
	}))
	sender.AssociateConnection(inConn)

	node, err := peer.NewOutboundPeer(ulogger.TestLogger{}, tSettings, cfg(peer.MessageListeners{}), "10.0.0.2:8333")
	require.NoError(t, err)
	node.AssociateConnection(outConn)

	for i := 0; i < 4; i++ {
		select {
		case <-verack:
		case <-time.After(2 * time.Second):
			t.Fatal("verack timeout")
		}
	}

	t.Cleanup(func() {
		sender.DisconnectWithInfo("test cleanup")
		sender.WaitForDisconnect()
	})

	// a reject frame with a declared length of 0 and the right checksum for it
	header := make([]byte, 24)
	binary.LittleEndian.PutUint32(header[0:4], uint32(chaincfg.MainNetParams.Net))
	copy(header[4:16], wire.CmdReject)
	copy(header[20:24], chainhash.DoubleHashB(nil)[:4])

	_, err = inW.Write(header)
	require.NoError(t, err)

	select {
	case msg := <-rejected:
		require.Equal(t, "malformed", msg.Cmd)
		require.Equal(t, wire.RejectMalformed, msg.Code)
		require.Contains(t, msg.Reason, "shorter than its command requires")
	case <-time.After(5 * time.Second):
		t.Fatal("the sender was not told its message was malformed")
	}

	node.WaitForDisconnect()
}
