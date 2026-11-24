package protocol

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/services/svp2p/transport"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// newLoggingTestPeer is newTestPeer with a caller-supplied logger, so a test
// can assert on the log-only branch of the unsupported-message policy.
func newLoggingTestPeer(t *testing.T, logger ulogger.Logger) (*Peer, *scriptedPeer) {
	t.Helper()

	a, b := net.Pipe()
	conn := transport.New(a, transport.Config{
		Net: wire.MainNet, ProtocolVersion: wire.ProtocolVersion,
		SendBudgetBytes: 1 << 20, RecvQueueLen: 32, WriteTimeout: 5 * time.Second,
	})

	cfg := PeerConfig{
		Handshake: HandshakeConfig{
			Inbound: false, Nonce: 7777, UserAgent: "/teranode-svp2p:0.1.0/",
			StartingHeight: 0, MaxRecvPayloadLength: wire.DefaultMaxRecvPayloadLength,
			AllowBlockPriority: true,
			LocalAddr:          wire.NewNetAddressIPPort(nil, 8333, 0),
			RemoteAddr:         wire.NewNetAddressIPPort(nil, 8333, 0),
		},
		Conn: conn, Logger: logger,
		IdleTimeout: 30 * time.Second, PingInterval: 30 * time.Second, BanThreshold: 100,
	}

	return NewPeer(cfg), &scriptedPeer{nc: b}
}

// TestPeerDisconnectsUnsupportedMessages covers the disconnect half of spec
// §6's last paragraph ("disconnect on mempool and the filter/cfilter
// families"), which is the legacy service's behavior at OnMemPool
// (services/legacy/peer_server.go:878-887) and the three filter handlers
// (:1715-1734). Before this task each of these fell through dispatchSync's
// switch and was silently ignored.
func TestPeerDisconnectsUnsupportedMessages(t *testing.T) {
	var stopHash chainhash.Hash

	// go-wire frames only a subset of the cfilter family: makeEmptyMessage
	// (go-wire message.go:193-200, v1.2.10) knows getcfilters, getcfcheckpt
	// and cfilter, and rejects getcfheaders, cfheaders and cfcheckpt as
	// "unhandled command" at decode time — which fails the connection inside
	// transport before any dispatch runs. Those three are covered by
	// TestDispatchUnsupportedCoversTheWholeFamily instead, which drives the
	// dispatch directly; putting them here would only assert go-wire's
	// decoder.
	tests := []struct {
		name string
		msg  wire.Message
	}{
		{name: wire.CmdMemPool, msg: wire.NewMsgMemPool()},
		{name: wire.CmdFilterLoad, msg: wire.NewMsgFilterLoad([]byte{0x01, 0x02}, 1, 0, wire.BloomUpdateNone)},
		{name: wire.CmdFilterAdd, msg: wire.NewMsgFilterAdd([]byte{0x01})},
		{name: wire.CmdFilterClear, msg: wire.NewMsgFilterClear()},
		{name: wire.CmdGetCFilters, msg: wire.NewMsgGetCFilters(wire.GCSFilterRegular, 0, &stopHash)},
		{name: wire.CmdGetCFCheckpt, msg: wire.NewMsgGetCFCheckpt(wire.GCSFilterRegular, &stopHash)},
		{name: wire.CmdCFilter, msg: wire.NewMsgCFilter(wire.GCSFilterRegular, &stopHash, []byte{0x01})},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, far := newTestPeer(t, 30*time.Second, 30*time.Second)

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			errCh := make(chan error, 1)
			go func() { errCh <- p.Run(ctx) }()

			completeHandshake(t, far)
			far.write(t, tt.msg)

			select {
			case err := <-errCh:
				require.Error(t, err)
				require.ErrorIs(t, err, ErrUnsupportedMessage)
				require.Contains(t, err.Error(), tt.name)
			case <-time.After(5 * time.Second):
				t.Fatalf("peer was not disconnected for %s", tt.name)
			}
		})
	}
}

// TestDispatchUnsupportedCoversTheWholeFamily drives dispatchUnsupported
// directly, so it covers every message spec §6 names — including the three
// cfilter commands go-wire cannot frame, which therefore cannot reach the
// dispatch over a real socket. It also pins the two gates that are easy to
// lose: nothing is refused before the handshake completes, and a message the
// policy does not name is left alone.
func TestDispatchUnsupportedCoversTheWholeFamily(t *testing.T) {
	var stopHash chainhash.Hash

	refused := []wire.Message{
		wire.NewMsgMemPool(),
		wire.NewMsgFilterLoad([]byte{0x01, 0x02}, 1, 0, wire.BloomUpdateNone),
		wire.NewMsgFilterAdd([]byte{0x01}),
		wire.NewMsgFilterClear(),
		wire.NewMsgGetCFilters(wire.GCSFilterRegular, 0, &stopHash),
		wire.NewMsgGetCFHeaders(wire.GCSFilterRegular, 0, &stopHash),
		wire.NewMsgGetCFCheckpt(wire.GCSFilterRegular, &stopHash),
		wire.NewMsgCFilter(wire.GCSFilterRegular, &stopHash, []byte{0x01}),
		wire.NewMsgCFHeaders(),
		wire.NewMsgCFCheckpt(wire.GCSFilterRegular, &stopHash, 0),
	}

	for _, msg := range refused {
		t.Run("refused/"+msg.Command(), func(t *testing.T) {
			p, _ := newLoggingTestPeer(t, ulogger.TestLogger{})

			require.ErrorIs(t, p.dispatchUnsupported(msg, true), ErrUnsupportedMessage)

			// net_processing.cpp ProcessMessage judges nothing before the
			// handshake completes; the handshake machine already scores those
			// messages (handshake.go scoreMissingVersion).
			require.NoError(t, p.dispatchUnsupported(msg, false))
		})
	}

	allowed := []wire.Message{
		wire.NewMsgReject(wire.CmdTx, wire.RejectInvalid, "no thanks"),
		wire.NewMsgNotFound(),
		wire.NewMsgPing(1),
		wire.NewMsgGetAddr(),
		wire.NewMsgAddr(),
		wire.NewMsgInv(),
	}

	for _, msg := range allowed {
		t.Run("allowed/"+msg.Command(), func(t *testing.T) {
			p, _ := newLoggingTestPeer(t, ulogger.TestLogger{})

			require.NoError(t, p.dispatchUnsupported(msg, true))
		})
	}
}

// TestPeerLogsRejectAndNotFoundWithoutDisconnecting is the other half of spec
// §6's last paragraph: reject and notfound are log-only. The legacy service
// logs both and does nothing else (OnReject, peer_server.go:1828-1835;
// OnNotFound, :1836-1843), and SVNode ignores an inbound notfound outright
// (net_processing.cpp:4847-4850).
func TestPeerLogsRejectAndNotFoundWithoutDisconnecting(t *testing.T) {
	tests := []struct {
		name string
		msg  wire.Message
		want string
	}{
		{
			name: wire.CmdReject,
			msg:  wire.NewMsgReject(wire.CmdTx, wire.RejectInvalid, "no thanks"),
			want: "reject",
		},
		{
			name: wire.CmdNotFound,
			msg:  wire.NewMsgNotFound(),
			want: "notfound",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := &debugCaptureLogger{}
			p, far := newLoggingTestPeer(t, logger)

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			errCh := make(chan error, 1)
			go func() { errCh <- p.Run(ctx) }()

			completeHandshake(t, far)
			far.write(t, tt.msg)

			// The peer must still be answering: a ping round trip after the
			// message proves the loop neither disconnected nor stalled.
			far.write(t, wire.NewMsgPing(9191))
			pong := far.readUntil(t, wire.CmdPong)
			require.Equal(t, uint64(9191), pong.(*wire.MsgPong).Nonce)

			select {
			case err := <-errCh:
				t.Fatalf("peer disconnected on %s: %v", tt.name, err)
			default:
			}

			require.True(t, logger.contains(tt.want), "no log line mentioning %s", tt.want)

			p.Disconnect("test done")

			select {
			case <-errCh:
			case <-time.After(5 * time.Second):
				t.Fatal("Run did not return after disconnect")
			}
		})
	}
}
